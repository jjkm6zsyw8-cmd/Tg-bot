"""Аналитика реального времени: кластеры → профили → скоринг → алерты (разд. 3, 4, 8, 9, 12 ТЗ).

Читает новые строки tokens/trades из PostgreSQL по курсору (NOTIFY 'events' от приёмника),
держит в памяти состояние монет моложе окна сопровождения, кладёт сообщения в outbox.
"""
from __future__ import annotations

import asyncio
import heapq
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ..alerts import format as fmt
from ..config import Env, Settings
from ..db.pool import Database
from ..ingest.rpc import RpcError, SolanaRpc
from . import clusters as cl
from .copytrade import TIER_NAMES, refresh_traders_adaptive
from .curve import Costs, entry_cost_fraction, exit_cost_fraction, trade_net_profit
from .features import launch_summary
from .filters import check as check_filters
from .newcoins import evaluate_new
from .trends import Trends, Tracked
from .paper import PaperTrader
from ..external import ExternalHub
from .loader import attach_cluster, cluster_profile, load_state, save_profile, state_from_token
from .price import SolPrice, measured_fee_bps
from .profiles import Profile, similar_to_reference
from .scoring import LEVELS, Snapshot, bump, evaluate_dev
from .state import TokenState, TradeRow

log = logging.getLogger(__name__)


@dataclass
class TokenInfo:
    resolution: cl.Resolution | None = None
    profile: Profile | None = None
    cluster_row: Any = None
    last_reason: str = ""
    last_decision: dict[str, Any] = field(default_factory=dict)
    resolving: bool = False
    needs_rpc: bool = False        # свежий деплоер: проверка финансирования через RPC ещё не сделана
    rpc_started: bool = False
    created_at: Any = None


@dataclass
class AlertEntry:
    """Объединённый алерт по монете: одна монета — одно сообщение."""
    mint: str
    created_mono: float
    created_wall: datetime
    modes: dict[str, str] = field(default_factory=dict)      # режим → уровень
    alert_ids: dict[str, int] = field(default_factory=dict)  # режим → alerts.id
    data: dict[str, Any] = field(default_factory=dict)
    level: str = "normal"
    topic: str = "dev"
    has_message: bool = False
    last_edit: float = 0.0
    last_text: str = ""
    follow_until: float = 0.0
    flags: set[str] = field(default_factory=set)
    trigger_traders: dict[str, float] = field(default_factory=dict)  # трейдер → позиция токенов
    trader_sold: dict[str, float] = field(default_factory=dict)
    manual: bool = False  # /watch без алерта
    alert_ts: float = 0.0  # unix-время алерта

    def combined_level(self) -> str:
        lvl = max(self.modes.values(), key=lambda x: LEVELS[x]) if self.modes else "normal"
        real_modes = [m for m in self.modes if m != "new_dev_wallet"]
        if len(set(real_modes)) >= 2:
            lvl = bump(lvl)
        return lvl


class Engine:
    def __init__(self, env: Env, settings: Settings, db: Database):
        self.env = env
        self.settings = settings
        self.db = db
        self.rpc = SolanaRpc(env.rpc_endpoint, rps=env.rpc_rps, fallback=env.rpc_fallback_url or None)
        rel = settings.reliability()
        self.ext = ExternalHub(env, db)
        self.linker = cl.Linker(db, self.rpc, settings.exchanges(), int(rel.get("exchange_tx_heuristic", 1000)),
                                ext=self.ext)
        self.price = SolPrice(db, ext=self.ext)
        self.ext_pending: dict[str, list[tuple[Any, ...]]] = {}
        self.ext_blocked: dict[str, float] = {}
        self.fee_bps: float | None = None
        self.states: dict[str, TokenState] = {}
        self.info: dict[str, TokenInfo] = {}
        self.alerts: dict[str, AlertEntry] = {}
        self.traders: dict[str, Any] = {}
        self.smart_buys: dict[str, list[tuple[float, str, float]]] = {}
        self.references: list[dict[str, Any]] = []
        self.cur_token = 0
        self.cur_trade = 0
        self.wake = asyncio.Event()
        self.slots_dirty = asyncio.Event()
        self.timers: list[tuple[float, int, str, str]] = []
        self._tseq = 0
        self.sent_times: list[float] = []
        self.summary: dict[str, list[str]] = {}
        self.latencies: list[float] = []
        self.watch_tokens: set[str] = set()
        self.new_eval_at: dict[str, float] = {}
        self.trends = Trends(self)
        self.paper = PaperTrader(self)
        from .pricecheck import PriceCheck
        self.pricecheck = PriceCheck(self)
        self._ls_cache: set[str] = set()
        self._ls_loaded = 0.0
        self._cl_day: dict[int, tuple[Any, int]] = {}
        from collections import deque
        self._feed_ts: deque[float] = deque(maxlen=5000)
        self.watch_ok = False          # слежка Helius за выбранными кошельками и монетами подключена
        self.my_devs: dict[str, dict[str, Any]] = {}
        from .copyguard import CopyGuard
        self.copyguard = CopyGuard(self)

    # ------------------------------------------------------------------ утилиты
    @property
    def cfg(self) -> dict[str, Any]:
        return self.settings.effective()

    def costs(self) -> Costs:
        return Costs.from_config(self.settings.costs(), self.fee_bps)

    def schedule(self, delay_s: float, mint: str, what: str) -> None:
        self._tseq += 1
        heapq.heappush(self.timers, (time.monotonic() + delay_s, self._tseq, mint, what))

    async def reload_settings(self) -> None:
        self.settings.overrides = await self.db.get_overrides()

    def rpc_over_budget(self) -> bool:
        """Бесплатный Helius — ~1 млн запросов в месяц на ВСЕ процессы (~33 тыс. в сутки), приёмнику нужно ~15 тыс.
        Аналитике по умолчанию — 12 тыс. в сутки на фоновые задачи; сверх лимита они ждут следующих суток (UTC)."""
        self.rpc.budget = int(self.cfg.get("rpc_daily_budget_analytics", 12_000))
        return self.rpc.over_budget()

    # ------------------------------------------------------------------ запуск
    async def start(self) -> None:
        await self.reload_settings()
        cfg = self.cfg
        window = int(cfg.get("follow_minutes", 60))
        self.cur_token = await self.db.fetchval("SELECT COALESCE(max(id), 0) FROM tokens")
        self.cur_trade = await self.db.fetchval("SELECT COALESCE(max(id), 0) FROM trades")
        rows = await self.db.fetch(
            "SELECT * FROM tokens WHERE created_at > now() - make_interval(mins => $1) AND id <= $2 ORDER BY id",
            window + 5, self.cur_token)
        for r in rows:
            st = await load_state(self.db, r["mint"], cfg)
            if st:
                self.states[st.mint] = st
                self.info[st.mint] = TokenInfo()
                if r["cluster_id"]:
                    await self._load_profile(st.mint, r["cluster_id"])
        # незавершённые сопровождения после рестарта
        for a in await self.db.fetch(
                "SELECT * FROM alerts WHERE follow_until > now() AND closed_at IS NULL ORDER BY id"):
            e = self.alerts.get(a["mint"])
            if e is None:
                e = AlertEntry(a["mint"], time.monotonic(), a["created_at"], alert_ts=a["created_at"].timestamp())
                e.follow_until = time.monotonic() + (a["follow_until"] - datetime.now(timezone.utc)).total_seconds()
                e.data = dict(a["payload"] or {})
                e.has_message = True
                self.alerts[a["mint"]] = e
            e.modes[a["mode"]] = a["level"]
            e.alert_ids[a["mode"]] = a["id"]
            e.level = e.combined_level()
            if a["trader"]:
                e.trigger_traders.setdefault(a["trader"], 0.0)
            if a["mint"] not in self.states:
                st = await load_state(self.db, a["mint"], cfg)
                if st:
                    self.states[a["mint"]] = st
                    self.info[a["mint"]] = TokenInfo()
        await self._load_traders()
        await self._load_references()
        await self.paper.start()
        await self.copyguard.load()
        for p in self.paper.positions.values():   # монеты открытых бумажных позиций держим в памяти
            if p.mint not in self.states:
                st = await load_state(self.db, p.mint, cfg)
                if st:
                    self.states[p.mint] = st
                    self.info[p.mint] = TokenInfo()
        log.info("engine: %d монет в памяти, %d сопровождений, %d бумажных позиций", len(self.states),
                 len(self.alerts), len(self.paper.positions))

    async def _listen(self) -> None:
        conn = await self.db.p.acquire()
        try:
            await conn.add_listener("events", lambda *_: self.wake.set())
            await conn.add_listener("slots", lambda *_: self.slots_dirty.set())
            await conn.add_listener("meta", lambda _c, _p, _ch, payload: asyncio.create_task(self._on_meta(payload)))
            await conn.add_listener("settings", lambda *_: asyncio.create_task(self.reload_settings()))
            await conn.add_listener("jobs", lambda *_: self.wake.set())
            while True:
                await asyncio.sleep(3600)
        finally:
            await self.db.p.release(conn)

    async def run(self) -> None:
        await self.start()
        tasks = [self._listen(), self._consume_loop(), self._timer_loop(), self._slots_loop(),
                 self._housekeeping_loop(), self._summary_loop(), self.price.loop(), self._jobs_loop(),
                 self._daily_loop(), self._withdrawal_loop(), self._traders_loop(), self.trends.run(), self.trends.fast_follow_loop(), self.paper.tick_loop(),
                 self.pricecheck.loop()]
        await asyncio.gather(*(asyncio.create_task(t) for t in tasks))

    # ------------------------------------------------------------------ поток событий
    async def _consume_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            self.wake.clear()
            try:
                await self._consume()
            except Exception:  # noqa: BLE001
                log.exception("consume")
                await asyncio.sleep(1)

    async def _consume(self) -> None:
        cfg = self.cfg
        toks = await self.db.fetch("SELECT * FROM tokens WHERE id > $1 ORDER BY id LIMIT 5000", self.cur_token)
        for r in toks:
            self.cur_token = max(self.cur_token, r["id"])
            if r["mint"] in self.states:
                continue
            st = state_from_token(r, cfg)
            self.states[st.mint] = st
            self.info[st.mint] = TokenInfo()
            asyncio.create_task(self._on_new_token(st, r))
        trades = await self.db.fetch("SELECT * FROM trades WHERE id > $1 ORDER BY id LIMIT 20000", self.cur_trade)
        for r in trades:
            self.cur_trade = max(self.cur_trade, r["id"])
            await self._on_trade(r)
        if len(trades) == 20000 or len(toks) == 5000:
            self.wake.set()

    async def _on_new_token(self, st: TokenState, rec: Any) -> None:
        cfg = self.cfg
        info = self.info[st.mint]
        info.resolving = True
        info.created_at = rec["created_at"]
        mine = self.my_devs.get(st.deployer) or (self.my_devs.get(st.creator) if st.creator else None)
        if mine is not None:
            asyncio.create_task(self._my_dev_launch(st, mine))
        try:
            if not cfg.get("modes", {}).get("dev", True):
                return
            # 0) личность: если монету создал сервис запуска за другого создателя — кластер по создателю
            ident = st.deployer
            if st.creator and st.creator != st.deployer and st.deployer in await self._launch_services():
                ident = st.creator
                st.add_cluster_wallet(st.creator, "создатель (запуск через сервис)", "high")
            await self.db.execute("UPDATE tokens SET identity=$2 WHERE mint=$1", st.mint, ident)
            # 1) только БД: известный дев, кошелёк из цепочки вывода SOL или новый кластер
            known = await cl.cluster_of(self.db, ident)
            watch = await self.db.fetchrow(
                "SELECT cluster_id, hop FROM withdrawal_watch WHERE wallet=$1 AND stopped IS NULL", ident)
            if watch and not known:
                cid = watch["cluster_id"]
                await cl.add_link(self.db, cid, ident,
                                  f"получил SOL от кластера после дампа ({watch['hop']} переход)", "high")
                info.resolution = cl.Resolution(cid, "high", "SOL после дампа пришли на этот кошелёк",
                                                known_dev_wallet=True)
                await self._bind_cluster(st, cid)
                await self._new_dev_wallet_alert(st, info.resolution)
            elif known:
                info.resolution = cl.Resolution(known[0], known[1], known[2])
                await self._bind_cluster(st, known[0])
            else:
                cid = await cl.new_cluster(self.db, ident)
                info.resolution = cl.Resolution(cid, "high", "новый кластер", new_cluster=True)
                await self._bind_cluster(st, cid)
                info.needs_rpc = True
            await self._count_cluster_launch(info.resolution.cluster_id if info.resolution else None)
            await self.evaluate_dev(st.mint, "create")
            # 2) свежий кошелёк: связь через финансирование (RPC). В экономном режиме — только когда
            #    у монеты появились покупатели, иначе бесплатного лимита RPC не хватит.
            if info.needs_rpc and not cfg.get("rpc_economy", True):
                await self._resolve_rpc(st)
        finally:
            info.resolving = False
            for d in (3, 10, 30, 60, 120):
                self.schedule(d, st.mint, "eval")

    async def _launch_services(self) -> set[str]:
        now = time.time()
        if now - self._ls_loaded > 300:
            from .services import launch_services
            self._ls_cache = await launch_services(self.db)
            self._ls_loaded = now
        return self._ls_cache

    async def _count_cluster_launch(self, cluster_id: int | None) -> None:
        """50+ запусков кластера в сутки — подозрительный кластер (склейка или массовый запуск)."""
        if cluster_id is None:
            return
        day = datetime.now(timezone.utc).date()
        d, n = self._cl_day.get(cluster_id, (day, 0))
        n = n + 1 if d == day else 1
        self._cl_day[cluster_id] = (day, n)
        if n == 50:
            await self.db.execute(
                "UPDATE clusters SET suspicious=true, suspicious_reason=$2 WHERE id=$1 AND NOT suspicious",
                cluster_id, "50+ запусков за сутки")
        if len(self._cl_day) > 200_000:
            self._cl_day = {k: v for k, v in self._cl_day.items() if v[0] == day}

    async def _resolve_rpc(self, st: TokenState) -> None:
        info = self.info.get(st.mint)
        if info is None or info.rpc_started or self.rpc_over_budget():
            return
        info.rpc_started = True
        cfg = self.cfg
        fresh = info.resolution.cluster_id if info.resolution and info.resolution.new_cluster else None
        created = info.created_at or datetime.fromtimestamp(st.created_ts, tz=timezone.utc)
        try:
            res, funded = await self.linker.resolve_deployer(
                st.deployer, created, float(cfg.get("funding_window_min", 30)), st.meta.get("image"),
                fresh_cluster=fresh, economy=bool(cfg.get("rpc_economy", True)))
        except RpcError as exc:
            log.debug("resolve %s: %s", st.mint, exc)
            return
        target = res.cluster_id if res and not res.new_cluster else (fresh or st.cluster_id)
        for w, why in funded:
            st.add_cluster_wallet(w, why, "high")
            if target:
                await cl.add_link(self.db, target, w, why, "high")
        if res and not res.new_cluster and res.cluster_id != st.cluster_id:
            info.resolution = res
            await self._bind_cluster(st, res.cluster_id)
            if res.known_dev_wallet:
                await self._new_dev_wallet_alert(st, res)
        info.needs_rpc = False
        await self.evaluate_dev(st.mint, "resolved")

    async def _bind_cluster(self, st: TokenState, cluster_id: int | None) -> None:
        if not cluster_id:
            return
        await attach_cluster(self.db, st, cluster_id)
        await self.db.execute("UPDATE tokens SET cluster_id=$2 WHERE mint=$1", st.mint, cluster_id)
        await self._load_profile(st.mint, cluster_id)

    async def _load_profile(self, mint: str, cluster_id: int) -> None:
        row = await self.db.fetchrow("SELECT * FROM clusters WHERE id=$1", cluster_id)
        info = self.info.setdefault(mint, TokenInfo())
        info.cluster_row = row
        prof = Profile.from_json(row["profile"]) if row else None
        if row and prof is None:
            prof, _ = await cluster_profile(self.db, cluster_id, self.cfg)
        info.profile = prof

    async def _on_meta(self, mint: str) -> None:
        st = self.states.get(mint)
        if not st:
            return
        row = await self.db.fetchrow("SELECT image, description, twitter, telegram, website FROM tokens WHERE mint=$1",
                                     mint)
        if not row:
            return
        st.meta = {k: v for k, v in dict(row).items() if v}
        info = self.info.get(mint)
        if st.cluster_id and info and info.resolution:
            for cid, reason in await self.linker.link_by_metadata(st.cluster_id, mint, st.meta):
                # повтор метаданных: предлагаем связь с известным кластером (средняя уверенность)
                if not await self.db.fetchval("SELECT qualified_at IS NOT NULL OR priority FROM clusters WHERE id=$1",
                                              cid):
                    continue
                exists = await self.db.fetchval(
                    "SELECT status FROM cluster_links WHERE cluster_id=$1 AND wallet=$2", cid, st.deployer)
                if exists:
                    continue
                lid = await cl.add_link(self.db, cid, st.deployer, reason, "medium", status="proposed")
                await self.db.enqueue("text", {
                    "topic": "dev",
                    "text": (f"🔗 <b>Возможная связь с кластером #{cid}</b>\nДеплоер <code>{st.deployer}</code> "
                             f"(${fmt.esc(st.symbol)})\nПричина: {fmt.esc(reason)}\nУверенность: средняя"),
                    "keyboard": [[{"text": "Подтвердить", "callback": f"lk:{lid}:y", "style": "success"},
                                  {"text": "Отклонить", "callback": f"lk:{lid}:n", "style": "danger"}]],
                }, priority=6)

    def trade_feed_ok(self, mint: str | None = None) -> bool:
        """Есть ли живой поток сделок: за 5 минут пришло 20+ сделок помимо покупок дева в транзакции создания.
        Без него алерты «Девы» и «Новые монеты» вслепую: нет цены после алерта и не видно продажи дева.
        Монеты девов-кандидатов идут через слежку Helius (сделки именно этой монеты) — им общий поток не нужен."""
        if mint is not None and self.watch_ok and self._candidate_coin(mint):
            return True
        cut = time.monotonic() - 300
        while self._feed_ts and self._feed_ts[0] < cut:
            self._feed_ts.popleft()
        return len(self._feed_ts) >= 20

    async def _on_trade(self, r: Any) -> None:
        mint = r["mint"]
        st = self.states.get(mint)
        if st is None or r["trader"] != st.deployer or r["ts"].timestamp() - st.created_ts > 2:
            self._feed_ts.append(time.monotonic())
        trader = r["trader"]
        tracked = trader in self.traders
        if st is None and (tracked or mint in self.alerts or mint in self.watch_tokens):
            st = await load_state(self.db, mint, self.cfg)
            if st is None and tracked:
                # монета появилась до запуска бота (или во время обрыва): заводим заглушку по сделке трейдера,
                # иначе покупку отслеживаемого кошелька пропустим
                await self._stub_token(mint, r)
                st = await load_state(self.db, mint, self.cfg)
            if st is None:
                return
            self.states[mint] = st
            self.info.setdefault(mint, TokenInfo())
            if st.cluster_id:
                await self._load_profile(mint, st.cluster_id)
            if any(t.signature == r["signature"] for t in st.trades):
                eff = None
            else:
                eff = st.add_trade(TradeRow.from_record(r))
        elif st is None:
            return
        else:
            eff = st.add_trade(TradeRow.from_record(r))
        if r["pool"] == "pump-amm" and not st.migrated:
            st.migrated = True
        if tracked:
            await self._on_smart_trade(st, r)
        info = self.info.get(mint)
        if info is not None and info.needs_rpc and not info.rpc_started and r["is_buy"]:
            n = len({t.trader for t in st.trades if t.is_buy and t.trader != st.deployer})
            if n >= int(self.cfg.get("resolve_min_buyers", 8)):
                asyncio.create_task(self._resolve_rpc(st))
        await self._maybe_new_coin(st)
        if eff is not None and self.paper.positions:
            await self.paper.on_trade(st, eff, trader, r["is_buy"])
        entry = self.alerts.get(mint)
        if entry and eff is not None and time.monotonic() < entry.follow_until:
            await self._follow(st, entry, eff, r)
        # до алерта: продажа кластера меняет решение
        if eff is not None and (eff.cluster_sell or eff.big_sell) and mint not in self.alerts:
            self.info.get(mint, TokenInfo()).last_reason = "dev_sold"

    # ------------------------------------------------------------------ режим 1: девы
    def _snapshot(self, st: TokenState, info: TokenInfo) -> Snapshot:
        now = time.time()
        res = info.resolution
        row = info.cluster_row
        deployer_sold = any(not t.is_buy and t.trader == st.deployer for t in st.trades)
        return Snapshot(
            mc=st.mc, age_s=max(0.0, now - st.created_ts),
            cluster_sold=deployer_sold or st.big_sell_ts is not None,
            cluster_share=st.share(st.cluster_wallets()),
            link_confidence=res.confidence if res else "low", link_reason=res.reason if res else "",
            blacklisted=bool(row and row["blacklisted"]),
            suspicious=bool(row and row.get("suspicious")),
            observed_growth_rate=st.growth_rate_per_s(now),
            threshold_override=row["threshold_override"] if row and row["threshold_override"] is not None else None,
        )

    async def evaluate_dev(self, mint: str, why: str) -> None:
        cfg = self.cfg
        if not cfg.get("modes", {}).get("dev", True):
            return
        st, info = self.states.get(mint), self.info.get(mint)
        if not st or not info:
            return
        if not self.trade_feed_ok(mint):
            info.last_reason = "no_trade_data"
            info.last_decision.update({"reason": "no_trade_data"})
            return
        snap = self._snapshot(st, info)
        dec = evaluate_dev(info.profile, snap, cfg, self.costs())
        info.last_reason = dec.reason if not dec.alert else "alerted"
        info.last_decision.update({"reason": dec.reason, "net": dec.net, "potential": dec.potential,
                                   "at_age": snap.age_s})
        if not dec.alert:
            if cfg.get("watch_level_enabled") and snap.age_s >= 25 and mint not in self.alerts:
                await self._maybe_watch(st, info)
            return
        incomplete = not st.slots_known() or not st.meta
        if incomplete and cfg.get("wait_full_data") and snap.age_s < float(cfg.get("full_data_wait_s", 3)):
            return  # подождём следующую переоценку
        metrics = st.metrics()
        filt = check_filters(metrics, self.settings.filters(), "dev")
        prof = info.profile
        data = {
            "symbol": st.symbol, "mc": st.mc, "age_s": snap.age_s, "potential": dec.potential, "net": dec.net,
            "entry_mc": dec.entry_mc, "entry_max_mc": dec.entry_max_mc, "target_mc": dec.target_mc,
            "dump_mc": dec.dump_mc, "profile": prof.to_json() if prof else None,
            "link_confidence": snap.link_confidence, "link_reason": snap.link_reason,
            "metrics": metrics, "violations": filt.violations, "incomplete": incomplete,
            "cluster_id": st.cluster_id, "stage": self._stage(st),
            "exit_rule": "Выход сразу при первой продаже кластера",
            "buy_within_s": buy_window_s(prof, snap.age_s),
            "smart_in_coin": sum(1 for a in self.traders if st.balances.get(a, 0) > 0),
        }
        await self.compose("dev", mint, dec.level or "normal", data)

    async def _maybe_watch(self, st: TokenState, info: TokenInfo) -> None:
        if not self.references:
            return
        cur = launch_summary(st, self.cfg, self.costs())
        for ref in self.references:
            ok, reasons = similar_to_reference(cur, ref, self.cfg)
            if ok:
                metrics = st.metrics()
                await self.compose("dev", st.mint, "watch", {
                    "symbol": st.symbol, "mc": st.mc, "age_s": time.time() - st.created_ts,
                    "entry_max_mc": st.mc * 1.1, "target_mc": ref.get("dump_mc") and 0.8 * ref["dump_mc"],
                    "metrics": metrics, "violations": [], "link_confidence": "low",
                    "note": f"🔍 похоже на эталон {ref['mint'][:6]}…: " + ", ".join(reasons),
                    "cluster_id": st.cluster_id, "stage": self._stage(st),
                })
                return

    async def _new_dev_wallet_alert(self, st: TokenState, res: cl.Resolution) -> None:
        await self.compose("new_dev_wallet", st.mint, "normal", {
            "symbol": st.symbol, "mc": st.mc, "age_s": time.time() - st.created_ts,
            "entry_max_mc": st.mc * 1.2, "target_mc": None, "link_confidence": "high",
            "link_reason": res.reason, "cluster_id": res.cluster_id,
            "note": f"🔗 новый кошелёк известного дева · кластер #{res.cluster_id}",
            "metrics": st.metrics(), "violations": [], "stage": self._stage(st),
        })

    @staticmethod
    def _stage(st: TokenState) -> str:
        if st.migrated:
            return "мигрировала"
        p = st.progress()
        return "скоро миграция" if p is not None and p >= 0.7 else "новая"

    # ------------------------------------------------------------------ режим «Новые монеты»
    async def _maybe_new_coin(self, st: TokenState) -> None:
        """Проверка свежей монеты при каждой сделке (не чаще раза в секунду на монету)."""
        cfg = self.cfg
        if not cfg.get("modes", {}).get("new", True):
            return
        e = self.alerts.get(st.mint)
        if e and "new" in e.modes:
            return
        now = time.time()
        if now - self.new_eval_at.get(st.mint, 0) < 1.0 or not self.trade_feed_ok():
            return
        self.new_eval_at[st.mint] = now
        info = self.info.get(st.mint)
        blacklisted = bool(info and info.cluster_row and info.cluster_row["blacklisted"])
        chk = evaluate_new(st, now, cfg, self.settings.filters(), blacklisted)
        if info is not None and not chk.ok:
            info.last_decision["new_reason"] = chk.reason
        if not chk.ok:
            return
        costs = self.costs()
        pos = float(cfg.get("position_sol", 0.5))
        thr = float(cfg.get("min_net_profit", 0.3))
        entry_mc = st.mc * (1 + max(0.0, st.growth_rate_per_s(now)) * float(cfg.get("reaction_s", 5)))
        f_in = entry_cost_fraction(entry_mc, pos, costs)
        target = entry_mc * (1 + thr) / max(0.05, (1 - f_in) * (1 - exit_cost_fraction(entry_mc * 1.3, pos, costs)))
        prof = info.profile if info else None
        dump = prof.dump_mc if prof and prof.launches >= int(cfg.get("min_launches", 3)) else None
        if dump and 0.8 * dump > target:
            target = 0.8 * dump
        data = {
            "symbol": st.symbol, "mc": st.mc, "age_s": now - st.created_ts, "entry_mc": entry_mc,
            "entry_max_mc": st.mc * 1.1, "target_mc": target, "dump_mc": dump,
            "metrics": chk.metrics, "violations": chk.violations, "cluster_id": st.cluster_id,
            "stage": self._stage(st), "exit_rule": "Выход сразу при первой продаже дева",
            "buy_within_s": 20.0, "smart_in_coin": sum(1 for a in self.traders if st.balances.get(a, 0) > 0),
            "newcoin": {"buyers": chk.buyers, "volume": chk.volume, "buy_share": chk.buy_share},
        }
        if prof and prof.launches:
            data["profile"] = prof.to_json()
            data["link_confidence"] = info.resolution.confidence if info and info.resolution else "low"
            data["link_reason"] = info.resolution.reason if info and info.resolution else ""
        await self.compose("new", st.mint, "normal", data)

    # ------------------------------------------------------------------ режим 2: смарт-мани
    async def _my_dev_launch(self, st: TokenState, dev: dict[str, Any]) -> None:
        """Дев из своего списка запустил монету — алерт сразу, без внешних проверок (каждая секунда — это MC).
        Вход и цель — по его истории: цель чуть ниже нижней четверти его точек первой продажи."""
        cfg = self.cfg
        if not cfg.get("modes", {}).get("copy", True):
            return
        dev_addr = dev.get("address") or st.deployer
        g = self.copyguard
        skip = g.dev_gap_reason(dev_addr)
        if skip is None and dev.get("once") and dev.get("once_done"):
            skip = "режим «один раз»: к этому деву уже заходили"
        if skip:
            await g.log(dev_addr, st.mint, st.symbol, "launch", None, st.mc, "skipped", skip)
            return
        if dev.get("once"):
            dev["once_done"] = True
            await self.db.execute("UPDATE my_devs SET once_done=true WHERE address=$1", dev_addr)
        await g.log(dev_addr, st.mint, st.symbol, "launch", None, st.mc, "copied", "дев запустил монету")
        start = st.mc
        p25, p75, med = dev.get("p25_sell_mc"), dev.get("p75_sell_mc"), dev.get("med_sell_mc")
        sol = self.price.value or 150
        label = dev.get("label") or f"{st.deployer[:4]}…"
        if med and med > start * 1.2:
            target = max(start * 1.25, 0.9 * (p25 or med))
            entry_max = max(start * 1.05, min(0.6 * med, target / 1.25))
            note = (f"👻 {label}: продаёт на {fmt.mc_usd(p25, sol)}–{fmt.mc_usd(p75, sol)} через "
                    f"~{fmt.ago(dev.get('med_sell_s'))} · запусков {dev.get('launches') or 0}, "
                    f"×1.5+ в {float(dev.get('hit_rate') or 0):.0%}")
        else:
            target, entry_max = start * 1.5, start * 1.2
            note = f"👻 {label}: почерка ещё нет — цель ×1.5 по умолчанию, выход при первой продаже дева"
        strong = int(dev.get("sold") or 0) >= 3 and float(dev.get("hit_rate") or 0) >= 0.6
        data = {
            "symbol": st.symbol, "mc": st.mc, "age_s": time.time() - st.created_ts, "entry_mc": st.mc,
            "entry_max_mc": entry_max, "target_mc": target, "dump_mc": med, "note": note,
            "signal": "мой дев", "metrics": {}, "violations": [], "cluster_id": st.cluster_id,
            "exit_rule": "Выход при первой продаже дева", "stage": "на кривой", "source_wallet": dev_addr,
        }
        await self._compose_inner("mydev", st.mint, "strong" if strong else "normal", data, signal="my_dev")

    async def _price_glitch(self, mint: str, mc_sol: float) -> bool:
        """Наш MC выше реального больше чем в 5 раз (по Jupiter сейчас, pump.fun: 1 млрд токенов)?
        Нет цены у Jupiter — не судим (False)."""
        ext = getattr(self, "ext", None)
        jp = await ext.jup_price(mint) if ext is not None else None
        if not jp:
            return False
        ours = mc_sol * (self.price.value or 150) / 1e9
        return ours > 5 * jp

    def _candidate_coin(self, mint: str) -> bool:
        """Монета дева-кандидата (кластер прошёл пороги профиля или отмечен «Успешно»): приёмник подписал её сделки
        через слежку Helius сразу при создании — те же условия, что в Receiver.watch_targets."""
        info = self.info.get(mint)
        row = info.cluster_row if info is not None else None
        return bool(row and (row["priority"] or row["qualified_at"]) and not row["blacklisted"]
                    and not row.get("suspicious"))

    async def _stub_token(self, mint: str, r: Any) -> None:
        symbol = ""
        pair = await self.ext.dex_pair(mint, timeout=2.0) if getattr(self, "ext", None) is not None else None
        if pair:
            symbol = str(pair.get("symbol") or "")
        await self.db.execute(
            """INSERT INTO tokens(mint, signature, deployer, current_owner, symbol, created_at, initial_mc_sol, source)
               VALUES ($1, $2, '', '', $3, $4, $5, 'stub') ON CONFLICT (mint) DO NOTHING""",
            mint, f"stub:{mint}", symbol, r["ts"], r["mc_sol"])

    async def _load_traders(self) -> None:
        rows = await self.db.fetch("SELECT * FROM traders WHERE active")
        self.traders = {r["address"]: r for r in rows}
        for r in await self.db.fetch("SELECT address, label FROM watchlist WHERE kind='trader'"):
            self.traders.setdefault(r["address"], {"address": r["address"], "label": r["label"], "tier": None,
                                                   "copy_30d": None, "win_rate": None, "manual": True})
        self.watch_tokens = {r["address"] for r in await self.db.fetch(
            "SELECT address FROM watchlist WHERE kind='token' AND (until IS NULL OR until > now())")}
        # девы из своего списка: алерт в момент запуска, цели — по их почерку (analytics/walletcheck.py)
        self.my_devs = {r["address"]: dict(r) for r in await self.db.fetch(
            """SELECT w.address, w.label, m.launches, m.sold, m.med_sell_mc, m.p25_sell_mc, m.p75_sell_mc,
                      m.med_sell_s, m.hit_rate, COALESCE(m.once, false) once, COALESCE(m.once_done, false) once_done
               FROM watchlist w LEFT JOIN my_devs m ON m.address = w.address WHERE w.kind = 'dev'""")}
        # девы из своего списка — заодно и трейдеры: их покупки чужих монет тоже копируем
        # (покупку своей монеты при запуске пропускает _on_smart_trade — это алерт «мой дев»)
        for addr, d in self.my_devs.items():
            self.traders.setdefault(addr, {"address": addr, "label": d.get("label"), "tier": None,
                                           "copy_30d": None, "win_rate": None, "manual": True})

    async def _load_references(self) -> None:
        rows = await self.db.fetch(
            "SELECT * FROM launches WHERE is_reference ORDER BY created_at DESC LIMIT 200")
        self.references = [dict(r) for r in rows]

    async def _on_smart_trade(self, st: TokenState, r: Any) -> None:
        cfg = self.cfg
        trader = self.traders[r["trader"]]
        own = bool(trader.get("manual"))      # свой список → раздел «Копитрейд», включается отдельно
        if not cfg.get("modes", {}).get("copy" if own else "smart", True):
            return
        line = {"is_buy": r["is_buy"], "label": trader["label"], "trader": r["trader"], "sol": r["sol_amount"],
                "symbol": st.symbol, "mc": st.mc, "mint": st.mint}
        if not cfg.get("smart_filtered", True) or cfg.get("smart_full_feed"):
            await self.db.enqueue("full", {"text": fmt.render_full_line(line, self.price.value)}, priority=7)
        entry = self.alerts.get(st.mint)
        g = self.copyguard
        ts = r["ts"].timestamp()
        if own and r["trader"] != st.deployer:
            if r["is_buy"]:
                g.on_buy(r["trader"], st.mint, ts)
            else:
                hold = await g.on_sell(r["trader"], st.mint, ts)
                await g.log(r["trader"], st.mint, st.symbol, "sell", r["sol_amount"], r["mc_sol"], "info",
                            f"держал {fmt.ago(hold)}" if hold is not None else None)
        if not r["is_buy"]:
            if entry and r["trader"] in entry.trigger_traders:
                entry.trader_sold[r["trader"]] = entry.trader_sold.get(r["trader"], 0.0) + r["token_amount"]
                pos = entry.trigger_traders[r["trader"]] or 0.0
                if pos and entry.trader_sold[r["trader"]] > 0.5 * pos and f"tsell:{r['trader']}" not in entry.flags:
                    entry.flags.add(f"tsell:{r['trader']}")
                    await self._follow_msg(entry, "trader_sell", st, emergency=True,
                                           extra=f"{trader['label'] or r['trader'][:6]} продал")
            return
        if not own and not cfg.get("smart_filtered", True):
            return
        now = time.time()
        buys = [b for b in self.smart_buys.get(st.mint, [])
                if now - b[0] <= 60 * float(cfg.get("consensus_window_min", 10))]
        buys.append((now, r["trader"], r["sol_amount"]))
        self.smart_buys[st.mint] = buys
        # свой список (импорт из Axiom, /addwallet) — раздел «Копитрейд»: копируем и небольшие покупки,
        # без рамок MC и роста за минуту (вы сами выбрали этих трейдеров)
        if own and r["trader"] == st.deployer:
            return                       # дев покупает свою монету при запуске — это алерт «мой дев», не копия

        async def skip(reason: str) -> None:
            if own:
                await g.log(r["trader"], st.mint, st.symbol, "buy", r["sol_amount"], r["mc_sol"], "skipped", reason)
        if own:
            why = g.pause_reason(r["trader"]) or g.delta_reason(r["trader"])
            if why:
                return await skip(why)
        min_buy = float(cfg.get("copy_min_buy_sol", 0.1)) if own else float(cfg.get("smart_min_buy_sol", 0.5))
        if r["sol_amount"] < min_buy:
            return await skip(f"мелкая покупка {r['sol_amount']:.3f} SOL < {min_buy:g}")
        if not own and not (float(cfg.get("smart_mc_min_sol", 0)) <= st.mc <= float(cfg.get("smart_mc_max_sol", 1e9))):
            return
        if not own and st.growth(now) > float(cfg.get("smart_max_growth_1m", 1.0)):
            return
        blacklisted = bool(self.info.get(st.mint) and self.info[st.mint].cluster_row
                           and self.info[st.mint].cluster_row["blacklisted"])
        if own:
            # свой список: по монете мы видим только сделки своих кошельков — доли холдеров, свежих кошельков,
            # бандлов по таким данным ложные (твинк = «100% свежих»). Отсекаем только чёрный список;
            # мусор и раги ловят внешние проверки (RugCheck: mint/freeze, раг) перед алертом
            metrics = st.metrics(None)
            filt = check_filters({}, self.settings.filters(), "smart", blacklisted)
        else:
            fresh = await self._fresh_share(st)
            metrics = st.metrics(fresh)
            filt = check_filters(metrics, self.settings.filters(), "smart", blacklisted)
        if not filt.passed:
            return await skip("фильтры безопасности: " + ", ".join(filt.violations))
        distinct = {b[1] for b in buys}
        signal, level = "single", "normal"
        if len(distinct) >= 2:
            signal, level = "consensus", "strong"
        if entry and "dev" in entry.modes:
            signal, level = "dev_match", "strong"
        costs = self.costs()
        pos = float(cfg.get("position_sol", 0.5))
        thr = float(cfg.get("min_net_profit", 0.3))
        entry_mc = st.mc * (1 + max(0.0, st.growth_rate_per_s(now)) * float(cfg.get("reaction_s", 5)))
        f_in = entry_cost_fraction(entry_mc, pos, costs)
        target = entry_mc * (1 + thr) / max(0.05, (1 - f_in) * (1 - exit_cost_fraction(entry_mc * 1.3, pos, costs)))
        traders = []
        for addr in distinct:
            t = self.traders.get(addr) or {}
            traders.append({"address": addr, "label": t.get("label"), "tier_name": TIER_NAMES.get(t.get("tier"), ""),
                            "copy_30d": t.get("copy_30d"), "win_rate": t.get("win_rate"),
                            "tags": (t.get("tags") if hasattr(t, "get") else None) or None,
                            "buy_sol": sum(b[2] for b in buys if b[1] == addr)})
        data = {
            "symbol": st.symbol, "mc": st.mc, "age_s": now - st.created_ts, "signal": signal,
            "entry_max_mc": st.mc * 1.15, "target_mc": target, "entry_mc": entry_mc,
            "metrics": metrics, "violations": filt.violations, "traders": traders,
            "smart_in_coin": sum(1 for a in self.traders if st.balances.get(a, 0) > 0),
            "exit_rule": "Выход при продаже трейдера или кластера", "trader": r["trader"],
            "cluster_id": st.cluster_id, "stage": self._stage(st),
        }
        if own:
            # для бумаги: размер копии от суммы цели (Ratio) и дельта входа относительно её покупки
            data.update(copy_wallet=r["trader"], copy_sol=r["sol_amount"], copy_ref_mc=r["mc_sol"])
            await g.log(r["trader"], st.mint, st.symbol, "buy", r["sol_amount"], r["mc_sol"], "copied",
                        "консенсус: второй кошелёк" if signal == "consensus" else None, ref_mc=r["mc_sol"])
        await self.compose("copy" if own else "smart", st.mint, level, data, signal=signal, trader=r["trader"])
        e = self.alerts.get(st.mint)
        if e:
            e.trigger_traders[r["trader"]] = e.trigger_traders.get(r["trader"], 0.0) + r["token_amount"]

    async def _fresh_share(self, st: TokenState, top: int = 15) -> float | None:
        """Доля холдеров (из топа), созданных за последние 24 часа."""
        holders = sorted(st.holders().items(), key=lambda x: -x[1])[:top]
        if not holders or self.rpc_over_budget():
            return None
        addrs = [a for a, _ in holders]
        known = {r["address"]: r["created_est"] for r in await self.db.fetch(
            "SELECT address, created_est FROM wallets WHERE address = ANY($1) AND created_est IS NOT NULL", addrs)}
        missing = [a for a in addrs if a not in known]

        async def one(a: str) -> None:
            try:
                sigs = await self.rpc.signatures_for(a, limit=1000)
            except RpcError:
                return
            if not sigs:
                return
            if len(sigs) >= 1000:
                est = datetime(2000, 1, 1, tzinfo=timezone.utc)  # старый активный кошелёк
            else:
                bt = sigs[-1].get("blockTime")
                if not bt:
                    return
                est = datetime.fromtimestamp(bt, tz=timezone.utc)
            known[a] = est
            await self.db.execute(
                "INSERT INTO wallets(address, created_est) VALUES ($1, $2) "
                "ON CONFLICT (address) DO UPDATE SET created_est = EXCLUDED.created_est", a, est)

        try:
            await asyncio.wait_for(asyncio.gather(*(one(a) for a in missing)), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        if not known:
            return None
        cut = datetime.now(timezone.utc) - timedelta(hours=24)
        vals = [known[a] for a in addrs if a in known]
        return sum(1 for v in vals if v > cut) / len(vals)

    # ------------------------------------------------------------------ сборка алерта
    def _topic_for(self, level: str, modes: dict[str, str]) -> str:
        if "copy" in modes or "mydev" in modes:
            return "copy"            # свой список — отдельный раздел «Копитрейд»
        if level == "strong":
            return "strong"
        if level == "watch":
            return "full"
        if "dev" in modes or "new_dev_wallet" in modes:
            return "dev"
        if "smart" in modes:
            return "smart"
        if "new" in modes:
            return "new"
        return "trend"

    async def compose(self, mode: str, mint: str, level: str, data: dict[str, Any],
                      signal: str | None = None, trader: str | None = None) -> None:
        """Первый алерт по монете проходит проверку внешних источников (≤1 с, в фоне, не тормозя поток)."""
        args = (mode, mint, level, data, signal, trader)
        if mint in self.alerts or not self.ext.any_token_source():
            await self._compose_inner(*args)
            return
        if time.time() - self.ext_blocked.get(mint, 0) < 600:
            return
        if mint in self.ext_pending:
            self.ext_pending[mint].append(args)
            return
        self.ext_pending[mint] = [args]
        asyncio.create_task(self._compose_checked(mint))

    async def _compose_checked(self, mint: str) -> None:
        st = self.states.get(mint)
        onchain = {"top10": st.top10_share()} if st is not None and st.trades else {}
        wallets = [a[5] for a in self.ext_pending.get(mint, []) if a[5]]
        try:
            chk = await self.ext.token_checks(mint, onchain=onchain, wallets=wallets)
        except Exception:  # noqa: BLE001
            log.exception("external checks %s", mint)
            chk = None
        queue = self.ext_pending.pop(mint, [])
        if chk is not None and chk.block:
            self.ext_blocked[mint] = time.time()
            info = self.info.get(mint)
            if info is not None:
                info.last_reason = "external_filters"
                info.last_decision["external_block"] = chk.block
            log.info("алерт %s отсечён внешними фильтрами: %s", mint, "; ".join(chk.block))
            return
        for mode, m, level, data, signal, trader in queue:
            if chk is not None:
                data = {**data, "ext_line": chk.short(), "ext_missing": chk.missing}
                if chk.smart is not None:
                    data["smart_in_coin_ext"] = chk.smart
                if chk.wallets and data.get("traders"):
                    for t in data["traders"]:
                        info = chk.wallets.get(t.get("address"))
                        if info:
                            t["tags"] = info.get("tags")
                            t["twitter"] = info.get("twitter")
            await self._compose_inner(mode, m, level, data, signal, trader)

    async def _compose_inner(self, mode: str, mint: str, level: str, data: dict[str, Any],
                             signal: str | None = None, trader: str | None = None) -> None:
        cfg = self.cfg
        st = self.states.get(mint)
        e = self.alerts.get(mint)
        if e is None:
            e = AlertEntry(mint, time.monotonic(), datetime.now(timezone.utc), alert_ts=time.time())
            e.follow_until = time.monotonic() + 60 * float(cfg.get("follow_minutes", 60))
            self.alerts[mint] = e
        prev_mode_level = e.modes.get(mode)
        if prev_mode_level is not None and LEVELS[level] <= LEVELS[prev_mode_level]:
            # ключ алерта — токен + режим: повтор только при повышении уровня
            e.data.update({k: v for k, v in data.items() if k in ("mc", "age_s", "metrics", "traders",
                                                                    "smart_in_coin", "incomplete", "violations")})
            return
        prev_level = e.level if e.modes else None
        e.modes[mode] = level
        # данные режима «дев» приоритетнее для целей и входа
        if mode == "dev" or "dev" not in e.modes or not e.data:
            merged = {**e.data, **data}
        else:
            merged = {**data, **{k: v for k, v in e.data.items() if v is not None}}
            merged["traders"] = data.get("traders")
        if e.data.get("extra_reasons") and data.get("extra_reasons"):
            merged["extra_reasons"] = list(dict.fromkeys(e.data["extra_reasons"] + data["extra_reasons"]))
        e.data = merged
        e.level = e.combined_level()
        e.data["mint"] = mint
        e.data["level"] = e.level
        e.data["modes"] = list(e.modes)
        e.data["reaction_s"] = cfg.get("reaction_s", 5)
        latency = time.time() - st.created_ts if st and mode == "dev" else None
        if latency is not None and prev_mode_level is None:
            e.data["latency_s"] = latency
            self.latencies.append(latency)
            self.latencies = self.latencies[-500:]
        aid = await self.db.fetchval(
            """INSERT INTO alerts(mint, mode, level, signal, trader, cluster_id, mc_alert, entry_mc, target_mc,
                   dump_mc, potential, net_profit, reasons, payload, incomplete, latency_ms, follow_until)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16, now() + make_interval(mins => $17))
               ON CONFLICT (mint, mode) DO UPDATE SET level=EXCLUDED.level, payload=EXCLUDED.payload,
                   signal=COALESCE(EXCLUDED.signal, alerts.signal)
               RETURNING id""",
            mint, mode, level, signal, trader, data.get("cluster_id"), data.get("mc"), data.get("entry_mc"),
            data.get("target_mc"), data.get("dump_mc"), data.get("potential"), data.get("net"),
            data.get("extra_reasons") or [], _jsonable(e.data), bool(data.get("incomplete")),
            int(latency * 1000) if latency is not None else None, int(cfg.get("follow_minutes", 60)))
        e.alert_ids[mode] = aid
        if prev_mode_level is None and not e.manual:
            # бумажная торговля входит по каждому алерту выбранных уровней (одна позиция на монету)
            asyncio.create_task(self.paper.on_alert(mint, e.level, aid, dict(e.data), trader or e.data.get("trader")))
        if e.level == "watch" and not e.has_message and not cfg.get("send_watch_alerts", False):
            # «Наблюдение» — ~4000 в сутки: Telegram их не пропускал, очередь сообщений росла до 900+ и копии
            # приходили с опозданием. Бумага (лотерея D/E) их покупает и так; в чат — только отчёт бумаги
            return
        text = fmt.render_alert(e.data, self.price.value, bool(cfg.get("compact")))
        profile_kind, profile_ref = ("d", str(e.data.get("cluster_id"))) if e.data.get("cluster_id") else \
            ("t", trader or e.data.get("trader") or "")
        wallet = (st.deployer if st and profile_kind == "d" else (trader or e.data.get("trader"))) or None
        kb = fmt.alert_keyboard(mint, self.settings.links(), self.env.redirect_base, profile_kind, profile_ref,
                                wallet=wallet)
        topic = self._topic_for(e.level, e.modes)
        upgraded = prev_level is not None and LEVELS[e.level] > LEVELS[prev_level]
        if e.has_message and not upgraded:
            await self._edit(e, text, kb, force=True)
            return
        # всплеск: второстепенные — в сводку
        now = time.monotonic()
        self.sent_times = [t for t in self.sent_times if now - t < 60]
        if e.level != "strong" and len(self.sent_times) >= int(cfg.get("burst_per_minute", 8)) and not e.has_message:
            self.summary.setdefault(topic, []).append(
                f"{fmt.ICONS[e.level]} ${fmt.esc(e.data.get('symbol') or '?')} · "
                f"MC {fmt.mc_usd(e.data.get('mc'), self.price.value)} · {'+'.join(e.modes)} · "
                f"<code>{mint}</code>")
            return
        self.sent_times.append(now)
        e.has_message = True
        e.topic = topic
        e.last_text = text
        e.last_edit = now
        prio = 1 if e.level == "strong" else 2 if e.level == "normal" else 4
        if signal == "dev_match":
            prio = 0
        await self.db.enqueue("alert", {
            "mint": mint, "topic": topic, "text": text, "keyboard": kb, "level": e.level,
            "silent": e.level == "watch", "upgrade": upgraded, "alert_ids": e.alert_ids,
        }, priority=prio)

    async def _edit(self, e: AlertEntry, text: str | None = None, kb: Any = None, force: bool = False) -> None:
        now = time.monotonic()
        # правка цены в алерте — не чаще раза в 30 с: лимит Telegram на группу ~20 сообщений в минуту общий
        if not e.has_message or (not force and now - e.last_edit < float(self.cfg.get("edit_min_interval_s", 30))):
            return
        if text is None:
            text = fmt.render_alert(e.data, self.price.value, bool(self.cfg.get("compact")))
        if text == e.last_text:
            return
        e.last_edit = now
        e.last_text = text
        await self.db.enqueue("edit", {"mint": e.mint, "text": text, "keyboard": kb}, priority=6)

    # ------------------------------------------------------------------ сопровождение
    async def _follow(self, st: TokenState, e: AlertEntry, eff: Any, r: Any) -> None:
        cfg = self.cfg
        e.data["mc"] = st.mc
        e.data["age_s"] = time.time() - st.created_ts
        prof = (e.data.get("profile") or {}).get("medians") or {}
        if eff.cluster_sell and "emergency" not in e.flags:
            # экстренный сигнал — при ПЕРВОЙ продаже кластера дева, любого размера
            e.flags.add("emergency")
            extra = f"дев продал {eff.sold_fraction:.0%}"
            ttd = prof.get("time_to_dump_s")
            age_s = r["ts"].timestamp() - st.created_ts
            if ttd and age_s < 0.5 * ttd:
                extra += f" · раньше обычного ({fmt.ago(age_s)} вместо ~{fmt.ago(ttd)})"
            await self._follow_msg(e, "emergency", st, emergency=True, extra=extra)
        elif eff.cluster_buy_more and "dev_buys" not in e.flags:
            e.flags.add("dev_buys")
            await self._follow_msg(e, "dev_buys", st)
        if "emergency" not in e.flags:
            tgt = e.data.get("target_mc")
            if tgt and st.mc >= tgt and "target" not in e.flags:
                e.flags.add("target")
                await self._follow_msg(e, "target", st)
            dump = e.data.get("dump_mc")
            if dump and st.mc >= float(cfg.get("dump_zone_silence", 0.7)) * dump and "zone70" not in e.flags:
                e.flags.add("zone70")
                await self._follow_msg(e, "zone70", st)
        p = st.progress()
        if p is not None and p >= 0.9 and "pre_migration" not in e.flags and not st.migrated:
            e.flags.add("pre_migration")
            await self._follow_msg(e, "pre_migration", st)
        if st.migrated and "migrated" not in e.flags:
            e.flags.add("migrated")
            await self._follow_msg(e, "migrated", st)
        await self._edit(e)

    async def _follow_msg(self, e: AlertEntry, kind: str, st: TokenState, emergency: bool = False,
                          extra: str = "") -> None:
        if not e.has_message and not e.manual:
            return                   # алерта в чате не было («Наблюдение», сводка) — продолжение ни к чему
        text = fmt.render_follow(kind, {"mint": e.mint, "symbol": st.symbol, "mc": st.mc}, self.price.value, extra)
        kb = fmt.exit_keyboard(e.mint, self.settings.links(), self.env.redirect_base) if emergency else None
        await self.db.enqueue("follow", {"mint": e.mint, "text": text, "keyboard": kb, "emergency": emergency,
                                         "kind": kind}, priority=0 if emergency else 5)

    async def _finalize_alert(self, e: AlertEntry) -> None:
        if e.mint in self.trends.tracked:
            await self.trends.finalize(e)   # итог по ценам DexScreener (тренд или монета после миграции)
            return
        st = self.states.get(e.mint) or await load_state(self.db, e.mint, self.cfg)
        if st is None:
            return
        cfg = self.cfg
        costs = self.costs()
        reaction = float(cfg.get("reaction_s", 5))
        pos = float(cfg.get("position_sol", 0.5))
        t_alert = e.alert_ts or (e.created_wall.timestamp())
        t_entry = t_alert + reaction
        entry_mc = st.mc_at(t_entry)
        exit_reason, t_exit = "timeout", t_alert + 60 * float(cfg.get("follow_minutes", 60))
        tgt = e.data.get("target_mc")
        for t in st.trades:
            if t.ts < t_entry:
                continue
            if st.big_sell_ts and t.ts >= st.big_sell_ts:
                exit_reason, t_exit = "emergency", st.big_sell_ts + reaction
                break
            if tgt and t.mc >= tgt:
                exit_reason, t_exit = "target", t.ts + reaction
                break
        exit_mc = st.mc_at(t_exit)
        net = trade_net_profit(entry_mc, exit_mc, pos, costs)
        end = (st.big_sell_ts - reaction) if st.big_sell_ts and st.big_sell_ts > t_entry else t_exit
        max_mc = st.max_mc_between(t_entry, max(end, t_entry))
        best = trade_net_profit(entry_mc, max_mc, pos, costs)
        thr = float(cfg.get("min_net_profit", 0.3))
        if net > 10 and await self._price_glitch(e.mint, max(exit_mc, max_mc)):
            # «+354 660%» по монете, которая столько не стоила: неверный пул/котировка. Итог не считаем и не шлём
            for aid in e.alert_ids.values():
                await self.db.execute(
                    """INSERT INTO alert_outcomes(alert_id, max_mc, exit_mc, exit_reason, net_result, auto_mark)
                       VALUES ($1, NULL, NULL, 'glitch', NULL, 'glitch') ON CONFLICT (alert_id) DO UPDATE SET
                       exit_reason='glitch', net_result=NULL, auto_mark='glitch', computed_at=now()""", aid)
                await self.db.execute("UPDATE alerts SET closed_at=now() WHERE id=$1", aid)
            log.warning("итог %s: MC %.0f SOL не подтверждён Jupiter — сбой цены, итог не учитываю", st.symbol, exit_mc)
            return
        rug = st.big_sell_ts is not None and st.big_sell_ts - t_alert < 30 and st.mc_at(st.big_sell_ts + 5) < 0.5 * entry_mc
        # предварительная оценка — по фактическому результату сигнала, а не по достижимому максимуму
        auto = "scam" if rug else "great" if net >= max(1.0, 2 * thr) else "good" if net >= 0.05 else "fail"
        seen = sum(1 for t in st.trades if t.ts > t_alert)
        outcome = {"max_mc": max_mc, "exit_mc": exit_mc, "exit_reason": exit_reason, "net_result": net,
                   "achievable_profit": best, "auto_mark": auto, "trades_after": seen, "entry_mc": entry_mc}
        for aid in e.alert_ids.values():
            await self.db.execute(
                """INSERT INTO alert_outcomes(alert_id, max_mc, exit_mc, exit_reason, net_result, achievable_profit,
                       auto_mark) VALUES ($1,$2,$3,$4,$5,$6,$7)
                   ON CONFLICT (alert_id) DO UPDATE SET max_mc=EXCLUDED.max_mc, exit_mc=EXCLUDED.exit_mc,
                       exit_reason=EXCLUDED.exit_reason, net_result=EXCLUDED.net_result,
                       achievable_profit=EXCLUDED.achievable_profit, auto_mark=EXCLUDED.auto_mark,
                       computed_at=now()""",
                aid, max_mc, exit_mc, exit_reason, net, best, auto)
            await self.db.execute("UPDATE alerts SET closed_at=now() WHERE id=$1", aid)
        if e.manual:
            text = fmt.render_final({**e.data, "mint": e.mint, "symbol": st.symbol}, outcome, self.price.value)
            await self.db.enqueue("follow", {"mint": e.mint, "text": text, "keyboard": None, "emergency": False,
                                             "kind": "final"}, priority=5)
            return
        if seen == 0 or not e.has_message:
            # после алерта не было ни одной сделки (нет данных) или самого алерта не было в чате
            # («Наблюдение») — итог записан, но в чат не шлём
            return
        main_id = e.alert_ids.get("dev") or next(iter(e.alert_ids.values()), None)
        text = fmt.render_final({**e.data, "symbol": st.symbol}, outcome, self.price.value)
        kb = (fmt.marks_keyboard(main_id) if main_id else []) + [[{"text": "Разбор монеты", "callback": f"coin:{e.mint}"}]]
        await self.db.enqueue("final", {"mint": e.mint, "text": text, "keyboard": kb}, priority=5)

    # ------------------------------------------------------------------ таймеры и обслуживание
    async def _timer_loop(self) -> None:
        while True:
            await asyncio.sleep(0.2)
            now = time.monotonic()
            while self.timers and self.timers[0][0] <= now:
                _, _, mint, what = heapq.heappop(self.timers)
                try:
                    e = self.alerts.get(mint)
                    if what == "eval" and (e is None or e.modes.get("dev") != "strong"):
                        await self.evaluate_dev(mint, "timer")
                except Exception:  # noqa: BLE001
                    log.exception("timer %s", mint)
            for mint, e in list(self.alerts.items()):
                if now >= e.follow_until:
                    self.alerts.pop(mint, None)
                    try:
                        await self._finalize_alert(e)
                    except Exception:  # noqa: BLE001
                        log.exception("finalize alert %s", mint)

    async def _slots_loop(self) -> None:
        while True:
            await self.slots_dirty.wait()
            self.slots_dirty.clear()
            await asyncio.sleep(0.2)
            young = [m for m, s in self.states.items() if time.time() - s.created_ts < 180 and not s.slots_known()]
            if not young:
                continue
            rows = await self.db.fetch(
                "SELECT mint, signature, slot FROM trades WHERE mint = ANY($1) AND slot IS NOT NULL", young)
            toks = {r["mint"]: r["created_slot"] for r in await self.db.fetch(
                "SELECT mint, created_slot FROM tokens WHERE mint = ANY($1)", young)}
            by_mint: dict[str, dict[str, int]] = {}
            for r in rows:
                by_mint.setdefault(r["mint"], {})[r["signature"]] = r["slot"]
            for m in young:
                st = self.states.get(m)
                if st:
                    st.apply_slots(by_mint.get(m, {}), toks.get(m))

    async def _housekeeping_loop(self) -> None:
        """Завершение запусков старше окна → launches + профиль кластера."""
        while True:
            await asyncio.sleep(20)
            try:
                self.fee_bps = await measured_fee_bps(self.db) or self.fee_bps
                await self._load_traders()
                await self.copyguard.tick()
                ing = await self.db.kv_get("health:ingest", {}) or {}
                self.watch_ok = bool((ing.get("watch") or {}).get("connected"))
                window = 60 * float(self.cfg.get("follow_minutes", 60))
                paper_mints = {p.mint for p in self.paper.positions.values()}
                old = [m for m, s in self.states.items()
                       if time.time() - s.created_ts > window and m not in self.alerts and m not in paper_mints]
                for m in old:
                    self.new_eval_at.pop(m, None)
                    st = self.states.pop(m)
                    info = self.info.pop(m, None)
                    await self.finalize_launch(st, info)
                await self.ext.publish_health("analytics")
                await self.db.kv_set("usage:analytics", {"rpc_calls": self.rpc.calls, "rpc_429": self.rpc.rate_limited,
                                                          "rpc_today": self.rpc.day_calls,
                                                          "rpc_budget": self.rpc.budget,
                                                          "rpc_over": self.rpc_over_budget()})
                await self._follow_migrated()
                cut = time.time() - 600
                for m in [m for m, t in self.ext_blocked.items() if t < cut]:
                    self.ext_blocked.pop(m, None)
                await self.db.kv_set("health:analytics", {
                    "at": datetime.now(timezone.utc).isoformat(), "tokens_in_memory": len(self.states),
                    "following": len(self.alerts), "tracked_traders": len(self.traders),
                    "trade_feed_ok": self.trade_feed_ok(),
                    "sol_usd": self.price.value, "fee_bps": self.fee_bps,
                    "latency_median_s": _median(self.latencies), "latency_p95_s": _pct(self.latencies, 0.95),
                    "paper_false_exits": self.paper.false_exits, "paper_bad_prices": self.paper.bad_prices,
                    "paper_entry_fixed": self.paper.entry_fixed, "paper_exit_fixed": self.paper.exit_fixed,
                    "paper_no_price": self.paper.no_price, "paper_pricecheck": self.pricecheck.stats,
                    "paper_open": len(self.paper.positions),
                })
            except Exception:  # noqa: BLE001
                log.exception("housekeeping")

    async def _follow_migrated(self) -> None:
        """После миграции сделки идут в PumpSwap — сопровождение продолжаем по данным DexScreener."""
        mints = [m for m in self.alerts if m not in self.trends.tracked]
        if not mints:
            return
        rows = await self.db.fetch(
            "SELECT mint FROM tokens WHERE mint = ANY($1) AND migrated_at IS NOT NULL", mints)
        for r in rows:
            e = self.alerts.get(r["mint"])
            st = self.states.get(r["mint"])
            if st is not None:
                st.migrated = True
            pair = await self.ext.dex_pair(r["mint"], timeout=3.0)
            if not e or not pair or not pair.get("price_usd") or not pair.get("market_cap_usd"):
                continue
            price, mc_usd = pair["price_usd"], pair["market_cap_usd"]
            sol = self.price.value or 150
            tgt_mc = e.data.get("target_mc")
            target_price = price * (tgt_mc * sol / mc_usd) if tgt_mc else price * 1.5
            alert_mc = e.data.get("entry_mc") or e.data.get("mc")
            alert_price = price * (alert_mc * sol / mc_usd) if alert_mc else price
            self.trends.tracked[r["mint"]] = Tracked(r["mint"], alert_price, e.alert_ts or time.time(),
                                                      target_price, max_price=price,
                                                      flags={"target"} if "target" in e.flags else set())
            from types import SimpleNamespace
            view = SimpleNamespace(symbol=(st.symbol if st else e.data.get("symbol")) or "", mc=mc_usd / sol)
            await self._follow_msg(e, "migrated", view,  # type: ignore[arg-type]
                                   extra="MC — по DexScreener. Сопровождение продолжается по его данным.")
            e.flags.add("migrated")

    async def finalize_launch(self, st: TokenState, info: TokenInfo | None) -> None:
        from .insights import entry_curve, sell_sequences, top_traders
        s = launch_summary(st, self.cfg, self.costs(), _median(self.latencies) or 1.5)
        cfg = self.cfg
        # блок B: продажи по группам, кривая входа, лучшие трейдеры монеты
        s["features"]["sells"] = sell_sequences(st, set(self.traders))
        prof = info.profile if info else None
        target = (float(cfg.get("target_k", 0.8)) * prof.dump_mc) if prof and prof.dump_mc else (
            0.8 * st.dump_mc if st.dump_mc else None)
        s["features"]["entry_curve"] = entry_curve(st, target, float(cfg.get("reaction_s", 5)), 2.0,
                                                   float(cfg.get("position_sol", 0.5)), self.costs())
        excl = st.cluster_wallets() | st.bundle_wallets() | st.sniper_wallets()
        tops = top_traders(st, excl) if st.trades_count() >= 5 else []
        s["features"]["top_traders"] = [t["trader"] for t in tops]
        s["features"]["decision_reason"] = info.last_reason if info else ""
        s["features"]["decision"] = info.last_decision if info else {}
        good_mark = await self.db.fetchval(
            """SELECT EXISTS(SELECT 1 FROM user_marks m JOIN alerts a ON a.id=m.alert_id
                             WHERE a.mint=$1 AND m.mark IN ('great','good'))""", st.mint)
        await self.db.execute(
            """INSERT INTO launches(mint, cluster_id, deployer, created_at, initial_mc, peak_mc, dump_mc,
                   time_to_dump_s, sold_share, cluster_share_start, early_buyers, achievable_profit, entry_mc,
                   mc_30s, mc_60s, mc_120s, dev_buy_sol, bundle_wallets, bundle_share, migrated, features,
                   is_reference, trades_count)
               VALUES ($1,$2,$3,to_timestamp($4),$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
               ON CONFLICT (mint) DO UPDATE SET peak_mc=EXCLUDED.peak_mc, dump_mc=EXCLUDED.dump_mc,
                   time_to_dump_s=EXCLUDED.time_to_dump_s, achievable_profit=EXCLUDED.achievable_profit,
                   features=EXCLUDED.features, trades_count=EXCLUDED.trades_count, finalized_at=now()""",
            st.mint, st.cluster_id, st.deployer, st.created_ts, s["initial_mc"], s["peak_mc"], s["dump_mc"],
            s["time_to_dump_s"], s["sold_share"], s["cluster_share_start"], s["early_buyers"],
            s["achievable_profit"], s["entry_mc"], s["mc_30s"], s["mc_60s"], s["mc_120s"], s["dev_buy_sol"],
            s["bundle_wallets"], s["bundle_share"], s["migrated"], _jsonable(s["features"]), good_mark,
            s["trades_count"])
        if tops:
            await self.db.p.executemany(
                """INSERT INTO coin_top_traders(mint, rank, trader, pnl_sol, entry_mc, exit_mc, hold_s, entry_age_s)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (mint, rank) DO UPDATE SET trader=EXCLUDED.trader,
                   pnl_sol=EXCLUDED.pnl_sol, entry_mc=EXCLUDED.entry_mc, exit_mc=EXCLUDED.exit_mc,
                   hold_s=EXCLUDED.hold_s, entry_age_s=EXCLUDED.entry_age_s""",
                [(st.mint, i + 1, t["trader"], t["pnl_sol"], t["entry_mc"], t["exit_mc"], t["hold_s"],
                  t["entry_age_s"]) for i, t in enumerate(tops)])
        if st.cluster_id is None:
            return
        await self.linker.promote_slot_buyers(st.cluster_id, s["features"].get("first_slot_buyers") or [])
        prof, _ = await cluster_profile(self.db, st.cluster_id, self.cfg)
        await save_profile(self.db, st.cluster_id, prof, self.cfg)

    async def _summary_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            pending, self.summary = self.summary, {}
            for topic, lines in pending.items():
                await self.db.enqueue("summary", {
                    "topic": topic,
                    "text": f"📦 <b>Сводка: {len(lines)} второстепенных алертов за минуту</b>\n" + "\n".join(lines[:40]),
                }, priority=6)

    async def _jobs_loop(self) -> None:
        from .feedback import Feedback
        fb = Feedback(self)
        while True:
            await asyncio.sleep(1)
            rows = await self.db.fetch("SELECT * FROM jobs WHERE done_at IS NULL ORDER BY id LIMIT 20")
            for j in rows:
                err = None
                try:
                    await fb.handle(j["kind"], j["payload"] or {})
                except Exception as exc:  # noqa: BLE001
                    log.exception("job %s", j["kind"])
                    err = str(exc)[:500]
                await self.db.execute("UPDATE jobs SET done_at=now(), error=$2 WHERE id=$1", j["id"], err)

    async def _withdrawal_loop(self) -> None:
        from .feedback import WithdrawalWatcher
        await WithdrawalWatcher(self).run()

    async def _daily_loop(self) -> None:
        """Разбор дня в заданное время + ночной пересчёт трейдеров и профилей."""
        from ..reports.daily import send_daily
        last_daily = last_nightly = None
        while True:
            await asyncio.sleep(30)
            cfg = self.cfg
            tz = ZoneInfo(cfg.get("timezone", "UTC"))
            now = datetime.now(tz)
            hh, mm = (int(x) for x in str(cfg.get("daily_report_time", "21:00")).split(":"))
            if cfg.get("modes", {}).get("daily", True) and (now.hour, now.minute) >= (hh, mm) \
                    and last_daily != now.date():
                last_daily = now.date()
                try:
                    await send_daily(self.db, self.settings, now.date())
                except Exception:  # noqa: BLE001
                    log.exception("daily")
            utc = datetime.now(timezone.utc)
            if utc.hour == 3 and last_nightly != utc.date():
                last_nightly = utc.date()
                try:
                    res = await refresh_traders_adaptive(self.db, cfg, self.costs(), limit=2000, ext=self.ext)
                    from . import services as svc
                    from .recluster import mark_suspicious
                    from .wallets import refresh_roles, refresh_wallet_stats
                    await svc.detect(self.db)
                    await mark_suspicious(self.db)
                    await refresh_wallet_stats(self.db)
                    await refresh_roles(self.db)
                    await self.recompute_profiles()
                    await self.db.system(f"Ночной пересчёт: трейдеров-кандидатов {res['candidates']}, "
                                         f"прошли фильтр {res['qualified']}.")
                    if cfg.get("trend_autotune", True):
                        from .tuning import tune_trends
                        tr = await tune_trends(self.db, cfg)
                        self.trends.tuned = await self.db.kv_get("trends:tuned") or {}
                        await self.db.system(tr.text(cfg))
                except Exception:  # noqa: BLE001
                    log.exception("nightly")

    async def _traders_loop(self) -> None:
        """Пересчёт списка смарт-мани каждый час (пока данных мало — облегчённые пороги)."""
        await asyncio.sleep(120)
        last_count = None
        while True:
            try:
                res = await refresh_traders_adaptive(self.db, self.cfg, self.costs(), limit=500, ext=self.ext)
                from .wallets import top_trader_candidates
                res["qualified"] += await top_trader_candidates(self.db, self.cfg, self.costs())
                await self._load_traders()
                if res["qualified"] != last_count:
                    last_count = res["qualified"]
                    await self.db.system(f"🧠 Смарт-мани: {res['qualified']} из {res['candidates']}"
                                         + (" (мало истории)" if res.get("bootstrap") else ""))
            except Exception:  # noqa: BLE001
                log.exception("traders refresh")
            await asyncio.sleep(60 * float(self.cfg.get("trader_refresh_min", 60)))

    async def recompute_profiles(self) -> None:
        rows = await self.db.fetch(
            "SELECT DISTINCT cluster_id FROM launches WHERE created_at > now() - interval '90 days'")
        for r in rows:
            prof, _ = await cluster_profile(self.db, r["cluster_id"], self.cfg)
            await save_profile(self.db, r["cluster_id"], prof, self.cfg)
        await self._load_references()

    async def watch_token(self, mint: str) -> None:
        """/watch: сопровождение монеты без алерта."""
        st = self.states.get(mint) or await load_state(self.db, mint, self.cfg)
        if not st:
            return
        self.states[mint] = st
        self.info.setdefault(mint, TokenInfo())
        if mint in self.alerts:
            return
        e = AlertEntry(mint, time.monotonic(), datetime.now(timezone.utc), manual=True, alert_ts=time.time())
        e.follow_until = time.monotonic() + 60 * float(self.cfg.get("follow_minutes", 60))
        e.data = {"mint": mint, "symbol": st.symbol, "mc": st.mc, "modes": ["watch"], "level": "normal"}
        if st.cluster_id:
            await self._load_profile(mint, st.cluster_id)
            prof = self.info[mint].profile
            if prof and prof.dump_mc:
                e.data["dump_mc"] = prof.dump_mc
                e.data["target_mc"] = float(self.cfg.get("target_k", 0.8)) * prof.dump_mc
                e.data["profile"] = prof.to_json()
        self.alerts[mint] = e
        self.watch_tokens.add(mint)


def buy_window_s(prof: Profile | None, age_s: float) -> float:
    """Сколько секунд ещё имеет смысл покупать: до конца окна входа по истории, иначе до трети
    типичного времени до дампа; минимум 5 с."""
    if prof is not None:
        ew = (prof.extra or {}).get("entry_window")
        if ew:
            return max(5.0, float(ew["t_to"]) - age_s)
        if prof.time_to_dump:
            return max(5.0, 0.3 * float(prof.time_to_dump) - age_s)
    return 30.0


def _median(xs: list[float]) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    return s[len(s) // 2]


def _pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, int(q * len(s)))]


def _jsonable(v: Any) -> Any:
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_jsonable(x) for x in v]
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
        return None
    return v
