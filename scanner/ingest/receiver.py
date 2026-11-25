"""Приёмник событий (разд. 11–12 ТЗ).

* два независимых потока + досбор через RPC после разрывов;
* дедуп по сигнатуре (в памяти + UNIQUE в БД);
* пакетная запись в PostgreSQL и NOTIFY 'events' для аналитики;
* досбор слотов для событий PumpPortal (нужны для бандлов и снайперов);
* загрузка метаданных токена (картинка, соцсети, описание);
* переключение источника при тишине 30 с, сверка покрытия раз в час, /health.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

from ..analytics.curve import mc_sol
from ..config import Env, Settings
from ..db.pool import Database
from ..models import Event, Migration, ReserveCover, ReserveTick, SolTransfer, TokenCreated, Trade
from ..solana import PUMP_PROGRAM
from .pump_parser import ParseStats, parse_transaction
from .rpc import RpcError, SolanaRpc
from .reserves import ReserveSource, Target, curve_mc, pool_mc, token_amount
from .pumpdata import PumpDataStream
from .sources import HeliusSource, LogsSource, PumpPortalSource, Source, WatchSource

log = logging.getLogger(__name__)


class LruSet:
    def __init__(self, cap: int = 200_000):
        self.cap = cap
        self.d: OrderedDict[str, None] = OrderedDict()

    def add(self, key: str) -> bool:
        """True, если ключ новый."""
        if key in self.d:
            self.d.move_to_end(key)
            return False
        self.d[key] = None
        if len(self.d) > self.cap:
            self.d.popitem(last=False)
        return True


def event_key(ev: Event) -> str:
    if isinstance(ev, TokenCreated):
        return f"c:{ev.mint}"
    if isinstance(ev, Trade):
        return f"t:{ev.signature}:{ev.mint}:{ev.trader}:{int(ev.is_buy)}"
    if isinstance(ev, Migration):
        return f"m:{ev.mint}"
    return f"s:{ev.signature}:{ev.src}:{ev.dst}:{ev.sol}"


class Receiver:
    def __init__(self, env: Env, settings: Settings, db: Database):
        self.env = env
        self.settings = settings
        self.db = db
        rel = settings.reliability()
        self.rpc = SolanaRpc(env.rpc_endpoint, rps=env.rpc_rps, fallback=env.rpc_fallback_url or None)
        self.pool_to_mint: dict[str, str] = {}
        # сделки каждой новой монеты — первые trade_window_min минут, дольше — если монету нужно держать
        self.window_s = 60 * float(rel.get("trade_window_min", 30))
        self.max_extend_s = 3600 * float(rel.get("trade_extend_max_h", 6))
        self.pp = PumpPortalSource(env.pumpportal_ws, env.pumpportal_api_key, self.window_s / 60,
                                   keep=self.keep_mint)
        self.pp.on_raw = lambda kind: self._count("pp_raw_" + kind, trades=1)   # 27.11: за что платим (ppcost)
        # 27.11 (владелец): кошельки — только через слежку Helius; подписка PumpPortal на кошельки (боты вроде ASqDbh —
        # десятки тысяч платных сообщений) по умолчанию выключена
        self.pp_accounts = bool(rel.get("pumpportal_accounts", False))
        # 27.11: весь рынок из pumpdata.fun — своя таблица market_trades (охотник и модели), не в сигналы бота
        self.pumpdata: PumpDataStream | None = None
        if env.pumpdata_api_key:
            self.pumpdata = PumpDataStream(db, env.pumpdata_ws, env.pumpdata_api_key,
                                           float(rel.get("pumpdata_min_sol", 0.2)),
                                           on_rows=lambda n: self._count("pumpdata", trades=n))
        self.sources: list[Source] = [self.pp]
        self.helius: HeliusSource | None = None
        if env.helius_ws_url:
            self.helius = HeliusSource(env.helius_ws_url, self.pool_to_mint)
            if env.helius_stream != "primary":
                self.helius.gate.clear()   # резерв: подключается только при обрыве PumpPortal
            self.sources.append(self.helius)
        if env.logs_ws_url:
            self.sources.append(LogsSource(env.logs_ws_url, self.pool_to_mint))
        # слежка за выбранными адресами (Helius, дёшево) — отдельно от основных источников:
        # не участвует в сверке покрытия и в поиске обрывов, видит только свои кошельки и монеты
        self.watch: WatchSource | None = (WatchSource(env.watch_ws_url, self.pool_to_mint, env.watch_fallback_ws)
                                          if env.watch_ws_url else None)
        self.watch_devs: set[str] = set()         # девы-кандидаты: их новые монеты — в слежку сразу при создании
        self.watch_coin_until: dict[str, float] = {}   # монета → до какого времени следить
        self.watch_capped = False
        self._pool_miss: dict[str, float] = {}
        self.non_sol_pools: set[str] = set()
        self._http: aiohttp.ClientSession | None = None
        if self.watch is not None:
            self.watch.resolve_pool = self.resolve_pool
        # 25.11: подписка на резервы (кривая / хранилища пула) монет, купленных отслеживаемыми и кандидатами, — цена
        # на каждом изменении (reserves.py); своё соединение и свой лимит трафика
        self.reserves: ReserveSource | None = (
            ReserveSource(env.watch_ws_url, ready=lambda: self.watch is None or self.watch.connected)
            if env.watch_ws_url and rel.get("reserve_watch", True) else None)
        self.reserve_stats = {"added": 0, "no_account": 0, "not_sol": 0, "rpc_error": 0, "full": 0}
        self.reserve_last_error = ""
        if self.reserves is not None:
            self.reserves.on_complete = self.reserve_migrated
            self.reserves.on_jump = self.reserve_migrated            # 26.11 (B1): скачок — перечитать пул
            self.reserves.COIN_DAILY_BYTES = int(float(rel.get("reserve_coin_daily_mb", 3)) * 1e6)
        self.dev_watch_until: dict[str, float] = {}   # 26.11 (B2): дев монеты, купленной отслеживаемым, — 35 мин
        self.reserve_until: dict[str, float] = {}     # монета → до какого времени слушать резервы
        self.reserve_capped = False
        self._reserve_busy: set[str] = set()
        self.buf_reserves: list[ReserveTick] = []
        self.buf_cover: list[ReserveCover] = []
        self.store_hours = float(rel.get("trade_store_hours", 24))
        # 25.11: кандидаты в smart (посчитаны моделью «как бот», не в списке) — их сделки и сделки их монет после покупки
        # (правило A держит до 30 мин) пишутся, как у отслеживаемых: иначе оценке не хватает цены выхода
        self.candidate_limit = int(rel.get("candidate_track_limit", 150))
        self.coin_track_s = 60 * float(rel.get("candidate_coin_min", 35))
        self.known_mints: dict[str, float] = {}   # mint → unix-время создания
        self.tracked_wallets: set[str] = set()
        self.scout_wallets: set[str] = set()       # кандидаты разведки: храним их сделки, монеты не подписываем
        self.tracked_mints: set[str] = set()      # алерты, бумажные позиции, наблюдение
        self.extended: dict[str, float] = {}      # mint → до какого unix-времени собирать сделки
        self.mc_track: dict[str, list[float]] = {}  # mint → [начальный MC, последний, MC 10 мин назад, отметка]
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=200_000)
        self.dropped = 0
        self.backfill_q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=5000)
        self.helius_opened_at = 0.0
        self._pp_trades_seen = -1
        self._pp_mark_at = 0.0
        self._pp_warned = 0.0
        self.seen = LruSet()
        self.buf_tokens: list[TokenCreated] = []
        self.buf_trades: list[Trade] = []
        self.buf_migr: list[Migration] = []
        self.buf_transfers: list[SolTransfer] = []
        self.buf_source_creates: list[tuple[str, str]] = []
        self.slot_queue: dict[str, float] = {}   # подпись → когда поставлена в очередь
        # 19.11: номера блоков для разбора «снайперы / связка» — у сделок первой минуты монеты и у её создания.
        # Поток блоков (logs) подключён, но сделки новых монет из PumpPortal приходили без блока: снайперы нашлись
        # у 0.2% монет, связка — у 0.6%. Не срочно: пачками до 256 подписей раз в slot_research_every_s
        self.slot_research: dict[str, float] = {}
        self.slot_research_s = float(rel.get("slot_research_s", 60))
        self.slot_research_every_s = float(rel.get("slot_research_every_s", 20))
        self.meta_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=20_000)
        self.last_signature: str | None = None
        self.hour_counts: dict[tuple[str, datetime], list[int]] = {}
        self.silent: dict[str, bool] = {}
        self.gap_ids: dict[str, int] = {}
        self.backfill_stats = ParseStats()
        self._parse_alerted = 0.0
        self.created_mono: dict[str, float] = {}  # mint → monotonic создания (окно досбора слотов)

    # ------------------------------------------------------------------ события
    async def enqueue(self, ev: Event) -> None:
        """Вызывается из потоков: только кладёт событие в очередь и сразу возвращает управление."""
        try:
            self.queue.put_nowait(ev)
        except asyncio.QueueFull:
            self.dropped += 1

    async def _fast_create(self, ev: TokenCreated) -> None:
        """23.11: запуск — сразу в аналитику (NOTIFY с данными события), мимо пакетной записи в базу: метод
        «новый дев» покупает на запуске, и каждые 0.4 с — это блок, за который снайперы поднимают цену."""
        try:
            payload = json.dumps({
                "mint": ev.mint, "deployer": ev.deployer, "creator": ev.creator, "symbol": ev.symbol[:30],
                "sig": ev.signature, "slot": ev.slot, "buy": ev.initial_buy_sol, "v_sol": ev.v_sol,
                "v_tokens": ev.v_tokens, "src": ev.source, "rx": ev.received_at.timestamp(),
                "quote": ev.quote_mint})
            await self.db.execute("SELECT pg_notify('fastcreate', $1)", payload)
        except Exception as exc:  # noqa: BLE001 — быстрый путь не должен мешать записи
            log.debug("fastcreate: %s", exc)

    async def worker(self) -> None:
        while True:
            ev = await self.queue.get()
            try:
                await self.sink(ev)
            except Exception:  # noqa: BLE001
                log.exception("обработка события")

    def keep_mint(self, mint: str) -> bool:
        return mint in self.tracked_mints or self.extended.get(mint, 0) > time.time()

    def _track_mc(self, t: Trade) -> None:
        mc = mc_sol(t.v_sol, t.v_tokens)
        if mc <= 0:
            return
        tr = self.mc_track.get(t.mint)
        if tr is None:
            self.mc_track[t.mint] = [mc, mc, mc, time.time()]
        else:
            tr[1] = mc

    async def extend_loop(self) -> None:
        """Раз в минуту: продлить сбор сделок растущим монетам (по 10 минут, максимум trade_extend_max_h)."""
        while True:
            await asyncio.sleep(60)
            now = time.time()
            for mint, tr in list(self.mc_track.items()):
                born = self.known_mints.get(mint)
                if born is None or now - born > self.max_extend_s:
                    self.mc_track.pop(mint, None)
                    continue
                if now - tr[3] >= 600:          # каждые 10 минут — новая точка сравнения
                    tr[2], tr[3] = tr[1], now
                age = now - born
                growing = tr[1] >= 1.1 * tr[2] or tr[1] >= 3 * tr[0]
                if age >= self.window_s - 120 and growing:
                    self.extended[mint] = max(self.extended.get(mint, 0), now + 600)
            for m in [m for m, t in self.extended.items() if t < now]:
                self.extended.pop(m, None)

    async def sink(self, ev: Event) -> None:
        if isinstance(ev, ReserveTick):
            self.buf_reserves.append(ev)
            return
        if isinstance(ev, ReserveCover):
            self.buf_cover.append(ev)
            return
        key = event_key(ev)
        is_new = self.seen.add(key)
        src = getattr(ev, "source", "") or "rpc"
        if isinstance(ev, TokenCreated):
            if src != "watch":                   # слежка видит не весь рынок — в сверку покрытия не идёт
                self.buf_source_creates.append((src, ev.mint))
            self._count(src, creates=1)
            if is_new and self.watch is not None and ev.deployer in self.watch_devs:
                self.watch_coin(ev.mint, 1800)   # монета дева-кандидата: сделки — сразу, а не через минуту
        elif isinstance(ev, Trade):
            self._count(src, trades=1)
        if not is_new:
            # дубль из второго источника: дополняем слот/время блока, если их не было
            if isinstance(ev, (TokenCreated, Trade)) and ev.slot is not None:
                if isinstance(ev, TokenCreated):
                    self.buf_tokens.append(ev)
                elif self.should_store(ev):
                    self.buf_trades.append(ev)
            return
        if getattr(ev, "signature", None):
            self.last_signature = ev.signature
        if isinstance(ev, TokenCreated):
            self.buf_tokens.append(ev)
            asyncio.create_task(self._fast_create(ev))
            self.created_mono[ev.mint] = time.monotonic()
            self.known_mints[ev.mint] = time.time()
            if ev.slot is None:
                (self.slot_queue if not self.logs_alive() else self.slot_research)[ev.signature] = time.monotonic()
            if ev.uri:
                try:
                    self.meta_queue.put_nowait((ev.mint, ev.uri))
                except asyncio.QueueFull:
                    pass
            for s in self.sources:
                if isinstance(s, PumpPortalSource):
                    await s.subscribe_tokens([ev.mint])
        elif isinstance(ev, Trade):
            if ev.trader in self.tracked_wallets and ev.is_buy and ev.trader not in self.scout_wallets:
                # отслеживаемый трейдер или кандидат купил — сделки монеты нужны для цены и выхода (правило A — до
                # 30 мин после входа, с запасом 35), в том числе на PumpSwap: их приносит только подписка на монету
                self.extended[ev.mint] = max(self.extended.get(ev.mint, 0), time.time() + self.coin_track_s)
                if self.watch is not None:
                    self.watch_coin(ev.mint, self.coin_track_s)
                self.reserve_watch(ev.mint, self.coin_track_s)
                asyncio.create_task(self._watch_dev(ev.mint, self.coin_track_s))
            if not self.should_store(ev):
                return
            self._track_mc(ev)
            self.buf_trades.append(ev)
            born = self.created_mono.get(ev.mint)
            if ev.slot is None and born:
                age = time.monotonic() - born
                if age < 15 and not self.logs_alive():
                    self.slot_queue[ev.signature] = time.monotonic()
                elif age < self.slot_research_s:
                    self.slot_research[ev.signature] = time.monotonic()
        elif isinstance(ev, Migration):
            self.buf_migr.append(ev)
            if ev.pool:
                self.pool_to_mint[ev.pool] = ev.mint
        elif isinstance(ev, SolTransfer):
            self.buf_transfers.append(ev)

    async def resolve_pool(self, pool: str) -> str | None:
        """Пул PumpSwap → монета, ТОЛЬКО если пул к SOL. Бывают пулы монеты к PUMP, USDC и т. п.: бот читал их
        суммы как SOL, и капитализация выходила завышенной в 15–20 раз (фальшивые «+1500%» у Diamond).
        Сначала своя база (пулы из миграций pump.fun — всегда к SOL), потом DexScreener с проверкой котировки."""
        mint = await self.db.fetchval("SELECT mint FROM tokens WHERE amm_pool=$1 AND migrated_at IS NOT NULL", pool)
        if mint:
            return mint
        if time.time() - self._pool_miss.get(pool, 0) < 600:
            return None
        try:
            if self._http is None:
                self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))
            async with self._http.get(f"https://api.dexscreener.com/latest/dex/pairs/solana/{pool}") as r:
                data = await r.json(content_type=None) if r.status == 200 else None
        except Exception:  # noqa: BLE001
            data = None
        pairs = (data or {}).get("pairs") or ([data["pair"]] if (data or {}).get("pair") else [])
        wsol = "So11111111111111111111111111111111111111112"
        for p in pairs:
            if (p.get("pairAddress") or pool) != pool:
                continue
            base = (p.get("baseToken") or {}).get("address")
            quote = (p.get("quoteToken") or {}).get("address")
            if base and base != wsol and quote == wsol:
                await self.db.execute("UPDATE tokens SET amm_pool = COALESCE(amm_pool, $2) WHERE mint=$1", base, pool)
                return base
            if quote and quote != wsol:
                self.non_sol_pools.add(pool)       # пул не к SOL: его сделки не разбираем вовсе
                return None
        self._pool_miss[pool] = time.time()
        return None

    # ------------------------------------------------------------------ резервы (reserves.py)
    def reserve_watch(self, mint: str, seconds: float) -> None:
        if self.reserves is None:
            return
        self.reserve_until[mint] = max(self.reserve_until.get(mint, 0), time.time() + seconds)
        if mint in self.reserves.capped:
            return                                    # суточный лимит монеты кончился
        if mint not in self.reserves.targets and mint not in self._reserve_busy and not self.reserve_capped:
            self._reserve_busy.add(mint)
            asyncio.create_task(self._reserve_add(mint))

    async def _watch_dev(self, mint: str, seconds: float) -> None:
        """26.11 (B2): «продал дев» без логов по монете — 35 мин слушаем кошелёк дева этой монеты (его сделки редки:
        одна подписка, мало трафика)."""
        try:
            dev = await self.db.fetchval("SELECT deployer FROM tokens WHERE mint = $1", mint)
        except Exception:  # noqa: BLE001
            return
        if dev and self.watch is not None:
            self.dev_watch_until[dev] = max(self.dev_watch_until.get(dev, 0), time.time() + seconds)

    async def reserve_migrated(self, mint: str) -> None:
        """Кривая закрылась — монета на PumpSwap: слушаем хранилища пула."""
        if mint in self._reserve_busy:
            return
        self._reserve_busy.add(mint)
        await self._reserve_add(mint, pool_only=True)

    async def _account(self, addr: str) -> bytes | None:
        import base64 as b64
        res = await self.rpc.call("getAccountInfo", [addr, {"encoding": "base64", "commitment": "confirmed"}])
        data = ((res or {}).get("value") or {}).get("data")
        return b64.b64decode(data[0]) if data else None

    async def _reserve_add(self, mint: str, pool_only: bool = False) -> None:
        """Монета → что слушать (кривая или хранилища пула) + цена на момент подписки."""
        from solders.pubkey import Pubkey
        from ..real import pump, pumpswap
        try:
            mk = Pubkey.from_string(mint)
            if not pool_only:
                curve = str(pump.curve_address(mk))
                data = await self._account(curve)
                c = curve_mc(data) if data else None
                if c is not None and not c[2]:
                    if self.reserves is not None and self.reserves.set_target(mint, Target("pump", [curve])):
                        self.buf_reserves.append(ReserveTick(mint, c[0], "pump", None, c[1], source="reserves_start"))
                        self.reserve_stats["added"] += 1
                    else:
                        self.reserve_stats["full"] += 1
                    return
            pool = await self.db.fetchval("SELECT amm_pool FROM tokens WHERE mint = $1", mint) \
                or str(pumpswap.pool_address(mk))
            data = await self._account(pool)
            if not data:
                self.reserve_stats["no_account"] += 1
                return
            p = pumpswap.parse_pool(pool, data)
            if p.quote_mint != str(pump.WSOL):
                self.reserve_stats["not_sol"] += 1
                return                                  # пул не к SOL — цену в SOL не считаем
            res = await self.rpc.call("getMultipleAccounts", [[p.base_vault, p.quote_vault],
                                                              {"encoding": "base64", "commitment": "confirmed"}])
            import base64 as b64
            vals = [(v or {}).get("data") for v in ((res or {}).get("value") or [])]
            amounts = [token_amount(b64.b64decode(v[0])) if v else None for v in vals]
            if self.reserves is not None and self.reserves.set_target(
                    mint, Target("pump-amm", [p.base_vault, p.quote_vault], p.virtual_quote)):
                self.reserve_stats["added"] += 1
                if len(amounts) == 2 and amounts[0] and amounts[1] is not None:
                    self.buf_reserves.append(ReserveTick(mint, pool_mc(amounts[0], amounts[1], p.virtual_quote),
                                                         "pump-amm", None, amounts[1] / 1e9, source="reserves_start"))
            else:
                self.reserve_stats["full"] += 1
        except Exception as exc:  # noqa: BLE001 — монету без резервов просто не слушаем (счётчик — в health)
            self.reserve_stats["rpc_error"] += 1
            self.reserve_last_error = f"{type(exc).__name__}: {exc}"[:200]
            if self.reserve_stats["rpc_error"] <= 5 or self.reserve_stats["rpc_error"] % 100 == 0:
                log.warning("резервы %s: %s", mint, self.reserve_last_error)
        finally:
            self._reserve_busy.discard(mint)

    async def reserve_targets(self) -> None:
        """Раз в минуту: снять подписки, у которых прошли 35 мин; дневной лимит трафика."""
        if self.reserves is None:
            return
        now = time.time()
        for m in [m for m, t in self.reserve_until.items() if t <= now]:
            self.reserve_until.pop(m, None)
            self.reserves.drop(m)
        for m in [m for m in self.reserves.targets if m not in self.reserve_until]:
            self.reserves.drop(m)
        for m, t in list(self.reserves.targets.items()):
            if t.venue == "pump" and t.complete and m not in self._reserve_busy:
                asyncio.create_task(self.reserve_migrated(m))     # пул ещё не появился — пробуем раз в минуту
        limit_mb = float(self.settings.reliability().get("reserve_daily_mb", 150))
        used_mb = (self.reserves.bytes_in - self._usage_base.get("bytes_reserves", 0)) / 1e6
        capped = used_mb >= limit_mb
        if capped and not self.reserve_capped:
            await self.db.system(f"⚠️ Резервы монет: за сутки {used_mb:.0f} МБ (лимит {limit_mb:.0f}). Новые монеты до "
                                 "конца суток (UTC) не подписываю, начатые дослушиваю.")
        self.reserve_capped = capped

    def watch_coin(self, mint: str, seconds: float) -> None:
        if self.watch is None or self.watch_capped:
            return
        self.watch_coin_until[mint] = max(self.watch_coin_until.get(mint, 0), time.time() + seconds)
        self.watch.add_coin(mint)

    async def candidates(self) -> list[str]:
        """Кандидаты в smart: посчитаны моделью «как бот», не в списке, торговали за 7 дней — лучшие по копии."""
        if self.candidate_limit <= 0:
            return []
        rows = await self.db.fetch(
            """SELECT address FROM traders WHERE NOT active AND copy_n_30d IS NOT NULL
               AND last_active > now() - interval '7 days' ORDER BY copy_30d DESC NULLS LAST LIMIT $1""",
            self.candidate_limit)
        return [r["address"] for r in rows]

    async def watch_targets(self) -> None:
        """Раз в минуту: кошельки (трейдеры, девы из списка) и монеты (позиции, наблюдение, свежие монеты девов)."""
        if self.watch is None:
            return
        rows = await self.db.fetch(
            """SELECT address FROM traders WHERE active UNION SELECT address FROM watchlist WHERE kind IN ('trader', 'dev')
               UNION SELECT address FROM scout WHERE status = 'probation'""")
        wallets = [r["address"] for r in rows]
        seen = set(wallets)
        now0 = time.time()
        self.dev_watch_until = {a: t for a, t in self.dev_watch_until.items() if t > now0}
        wallets += [a for a in self.dev_watch_until if a not in seen]       # 26.11 (B2): девы монет — после списка
        seen |= set(self.dev_watch_until)
        wallets += [a for a in await self.candidates() if a not in seen]      # кандидаты — последними
        self.watch_devs = {r["wallet"] for r in await self.db.fetch(
            """SELECT l.wallet FROM cluster_links l JOIN clusters c ON c.id = l.cluster_id
               WHERE l.status <> 'rejected' AND l.confidence <> 'low' AND NOT c.blacklisted AND NOT c.suspicious
                 AND (c.priority OR c.qualified_at IS NOT NULL)
               LIMIT 5000""")} | {r["address"] for r in await self.db.fetch(
            "SELECT address FROM watchlist WHERE kind='dev'")}
        # монеты: открытые позиции и наблюдение — только молодые монеты на кривой (у мигрировавших слишком
        # много сделок, их цену берём из DexScreener)
        coin_rows = await self.db.fetch(
            """SELECT DISTINCT x.mint FROM (
                   SELECT mint FROM paper_positions WHERE closed_at IS NULL
                   UNION SELECT address AS mint FROM watchlist WHERE kind='token' AND (until IS NULL OR until > now())
               ) x JOIN tokens t ON t.mint = x.mint
               WHERE t.migrated_at IS NULL AND t.created_at > now() - interval '12 hours'""")
        now = time.time()
        for r in coin_rows:
            self.watch_coin_until[r["mint"]] = max(self.watch_coin_until.get(r["mint"], 0), now + 120)
        self.watch_coin_until = {m: t for m, t in self.watch_coin_until.items() if t > now}
        # дневной лимит трафика: кошельки остаются всегда, монеты — пока укладываемся
        # ~5 КБ на сделку: 250 МБ ≈ 50 тыс. сделок ≈ 5 тыс. кредитов Helius в сутки
        limit_mb = float(self.settings.reliability().get("watch_daily_mb", 250))
        used_mb = (self.watch.bytes_in - self._usage_base.get("bytes_watch", 0)) / 1e6
        capped = used_mb >= limit_mb
        if capped and not self.watch_capped:
            await self.db.system(f"⚠️ Слежка Helius: за сутки {used_mb:.0f} МБ (лимит {limit_mb:.0f}). Сделки монет "
                                 "до конца суток (UTC) не собираю, кошельки — по-прежнему.")
        self.watch_capped = capped
        coins = [] if capped else sorted(self.watch_coin_until, key=lambda m: -self.watch_coin_until[m])
        self.watch.set_targets(wallets, coins)
        await self.reserve_targets()

    def logs_alive(self) -> bool:
        """Поток со слотами (Helius/logs) подключён — слоты придут с ним, RPC для слотов не тратим."""
        return any(isinstance(s, (LogsSource, HeliusSource)) and s.connected for s in self.sources)

    def should_store(self, t: Trade) -> bool:
        """Сделки всех новых монет первые 30 минут; дольше — алерт, позиция, наблюдение, растущий MC,
        а также сделки отслеживаемых кошельков. Остальное не храним (по программе ~100 сделок/с)."""
        now = time.time()
        born = self.known_mints.get(t.mint)
        if born is not None and now - born < self.window_s:
            return True
        if self.extended.get(t.mint, 0) > now or t.mint in self.tracked_mints:
            return True
        return t.trader in self.tracked_wallets

    async def load_known_mints(self) -> None:
        rows = await self.db.fetch(
            "SELECT mint, extract(epoch FROM created_at) ts FROM tokens "
            "WHERE created_at > now() - make_interval(hours => $1)", int(self.store_hours))
        for r in rows:
            self.known_mints[r["mint"]] = float(r["ts"])
        # пулы, записанные старым резолвером без проверки котировки, могли быть не к SOL — сбрасываем,
        # их заново определит DexScreener; пулы из миграций pump.fun (migrated_at) всегда к SOL
        await self.db.execute("UPDATE tokens SET amm_pool=NULL WHERE amm_pool IS NOT NULL AND migrated_at IS NULL")
        for r in await self.db.fetch("SELECT amm_pool, mint FROM tokens WHERE amm_pool IS NOT NULL "
                                     "AND migrated_at IS NOT NULL AND created_at > now() - interval '30 days'"):
            self.pool_to_mint.setdefault(r["amm_pool"], r["mint"])

    def prune_known(self) -> None:
        cut = time.time() - self.store_hours * 3600
        for m in [m for m, t in self.known_mints.items() if t < cut]:
            self.known_mints.pop(m, None)

    def _count(self, source: str, creates: int = 0, trades: int = 0) -> None:
        hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        c = self.hour_counts.setdefault((source, hour), [0, 0])
        c[0] += creates
        c[1] += trades

    # ------------------------------------------------------------------ запись
    async def flush_loop(self) -> None:
        while True:
            await asyncio.sleep(0.1)
            try:
                await self.flush()
            except Exception:  # noqa: BLE001
                log.exception("ошибка записи в БД")
                await asyncio.sleep(1)

    async def flush(self) -> None:
        tokens, self.buf_tokens = self.buf_tokens, []
        trades, self.buf_trades = self.buf_trades, []
        migr, self.buf_migr = self.buf_migr, []
        transfers, self.buf_transfers = self.buf_transfers, []
        screates, self.buf_source_creates = self.buf_source_creates, []
        reserves, self.buf_reserves = self.buf_reserves, []
        covers, self.buf_cover = self.buf_cover, []
        if reserves or covers:
            await self.flush_reserves(reserves, covers)
        if not (tokens or trades or migr or transfers or screates):
            return
        async with self.db.p.acquire() as conn:
            async with conn.transaction():
                if tokens:
                    await conn.executemany(
                        """INSERT INTO tokens(mint, signature, deployer, current_owner, name, symbol, uri,
                               bonding_curve, created_slot, created_at, received_at, initial_mc_sol,
                               initial_buy_sol, source)
                           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                           ON CONFLICT (mint) DO UPDATE SET
                               created_slot = COALESCE(tokens.created_slot, EXCLUDED.created_slot),
                               created_at = CASE WHEN tokens.created_slot IS NULL AND EXCLUDED.created_slot IS NOT NULL
                                                 THEN LEAST(tokens.created_at, EXCLUDED.created_at) ELSE tokens.created_at END,
                               current_owner = CASE WHEN EXCLUDED.source = 'helius' THEN EXCLUDED.current_owner
                                                    ELSE tokens.current_owner END""",
                        [(t.mint, t.signature, t.deployer, t.creator or t.deployer, t.name[:200], t.symbol[:50],
                          t.uri[:500], t.bonding_curve, t.slot, t.ts, t.received_at,
                          mc_sol(t.v_sol, t.v_tokens) or 27.958993476234856, t.initial_buy_sol, t.source)
                         for t in tokens],
                    )
                if trades:
                    await conn.executemany(
                        """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol,
                               v_sol, v_tokens, real_tokens, new_balance, fee_sol, pool, slot, ts, received_at, source)
                           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
                           ON CONFLICT (signature, mint, trader, is_buy) DO UPDATE SET
                               -- 19.11: та же сделка PumpSwap пришла и из PumpPortal (MC без виртуальных резервов
                               -- пула), и из своего разбора — цена берётся из своего разбора
                               mc_sol = CASE WHEN trades.source = 'pumpportal' AND trades.pool = 'pump-amm'
                                             AND EXCLUDED.source <> 'pumpportal' THEN EXCLUDED.mc_sol ELSE trades.mc_sol END,
                               v_sol = CASE WHEN trades.source = 'pumpportal' AND trades.pool = 'pump-amm'
                                            AND EXCLUDED.source <> 'pumpportal' THEN EXCLUDED.v_sol ELSE trades.v_sol END,
                               v_tokens = CASE WHEN trades.source = 'pumpportal' AND trades.pool = 'pump-amm'
                                               AND EXCLUDED.source <> 'pumpportal' THEN EXCLUDED.v_tokens
                                               ELSE trades.v_tokens END,
                               source = CASE WHEN trades.source = 'pumpportal' AND trades.pool = 'pump-amm'
                                             AND EXCLUDED.source <> 'pumpportal' THEN EXCLUDED.source ELSE trades.source END,
                               slot = COALESCE(trades.slot, EXCLUDED.slot),
                               fee_sol = COALESCE(trades.fee_sol, EXCLUDED.fee_sol),
                               real_tokens = COALESCE(EXCLUDED.real_tokens, trades.real_tokens),
                               new_balance = COALESCE(trades.new_balance, EXCLUDED.new_balance)""",
                        [(t.signature, t.mint, t.trader, t.is_buy, t.sol_amount, t.token_amount,
                          mc_sol(t.v_sol, t.v_tokens), t.v_sol, t.v_tokens, t.real_tokens, t.new_token_balance,
                          t.fee_sol, t.pool, t.slot, t.ts, t.received_at, t.source) for t in trades],
                    )
                if trades:
                    # MC по каждой сделке; время создания не позже первой сделки (время блока)
                    agg: dict[str, list[Any]] = {}
                    for t in trades:
                        mc = mc_sol(t.v_sol, t.v_tokens)
                        a = agg.setdefault(t.mint, [t.ts, 0.0, 0.0, t.ts])
                        a[0] = min(a[0], t.ts)
                        if mc > 0:
                            if t.ts >= a[3]:
                                a[1], a[3] = mc, t.ts
                            a[2] = max(a[2], mc)
                    await conn.executemany(
                        """UPDATE tokens SET created_at = LEAST(created_at, $2),
                               last_mc_sol = CASE WHEN $3 > 0 THEN $3 ELSE last_mc_sol END,
                               peak_mc_sol = GREATEST(COALESCE(peak_mc_sol, 0), $4) WHERE mint = $1""",
                        [(m, a[0], a[1], a[2]) for m, a in agg.items()])
                if migr:
                    await conn.executemany(
                        "UPDATE tokens SET migrated_at = COALESCE(migrated_at, $2), "
                        "amm_pool = COALESCE(NULLIF($3, ''), amm_pool) WHERE mint = $1",
                        [(m.mint, m.ts, m.pool) for m in migr],
                    )
                if transfers:
                    await conn.executemany(
                        "INSERT INTO sol_transfers(signature, src, dst, sol, slot, ts) VALUES ($1,$2,$3,$4,$5,$6) "
                        "ON CONFLICT DO NOTHING",
                        [(t.signature, t.src, t.dst, t.sol, t.slot, t.ts) for t in transfers],
                    )
                if screates:
                    await conn.executemany(
                        "INSERT INTO source_creates(source, mint) VALUES ($1, $2) ON CONFLICT DO NOTHING", screates,
                    )
                await conn.execute("SELECT pg_notify('events', '')")

    # ------------------------------------------------------------------ слоты
    async def slot_loop(self) -> None:
        """Слоты для событий PumpPortal через getSignatureStatuses (пачками до 256)."""
        while True:
            await asyncio.sleep(0.3)
            now = time.monotonic()
            ready = [s for s, t in self.slot_queue.items() if now - t > 0.4]
            if not ready:
                continue
            ready = ready[:256]
            try:
                slots = await self.rpc.signature_slots(ready)
            except RpcError as exc:
                log.debug("slots: %s", exc)
                continue
            for sig in ready:
                if sig in slots or now - self.slot_queue.get(sig, now) > 20:
                    self.slot_queue.pop(sig, None)
            if slots:
                await self._save_slots(slots)
            cutoff = now - 120
            for m in [m for m, t in self.created_mono.items() if t < cutoff]:
                self.created_mono.pop(m, None)

    async def _save_slots(self, slots: dict[str, int]) -> None:
        rows = list(slots.items())
        await self.db.p.executemany("UPDATE trades SET slot=$2 WHERE signature=$1 AND slot IS NULL", rows)
        await self.db.p.executemany(
            "UPDATE tokens SET created_slot=$2 WHERE signature=$1 AND created_slot IS NULL", rows)
        await self.db.execute("SELECT pg_notify('slots', '')")

    async def research_slots_once(self) -> int:
        """Номера блоков для сделок первой минуты новых монет и их создания (разбор «снайперы / связка»): пачками
        до 256 подписей. Узел помнит статусы последних ~2 минут — не нашлось за 90 с, подпись выбрасываем."""
        now = time.monotonic()
        sigs = list(self.slot_research)
        found: dict[str, int] = {}
        for i in range(0, len(sigs), 256):
            try:
                found.update(await self.rpc.signature_slots(sigs[i:i + 256]))
            except RpcError as exc:
                log.debug("слоты для разбора: %s", exc)
                break
        for sig in sigs:
            if sig in found or now - self.slot_research.get(sig, now) > 90:
                self.slot_research.pop(sig, None)
        if found:
            await self._save_slots(found)
        return len(found)

    async def slot_research_loop(self) -> None:
        while True:
            await asyncio.sleep(self.slot_research_every_s)
            if self.slot_research:
                try:
                    await self.research_slots_once()
                except Exception:  # noqa: BLE001 — разбор не срочный: не вышло сейчас — попробуем через 20 с
                    log.exception("слоты для разбора")

    # ------------------------------------------------------------------ метаданные
    async def meta_worker(self) -> None:
        timeout = aiohttp.ClientTimeout(total=4)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            while True:
                mint, uri = await self.meta_queue.get()
                url = uri
                if url.startswith("ipfs://"):
                    url = "https://ipfs.io/ipfs/" + url[7:]
                if not url.startswith("http"):
                    continue
                try:
                    async with http.get(url) as resp:
                        if resp.status != 200:
                            continue
                        meta = await resp.json(content_type=None)
                except Exception:  # noqa: BLE001
                    continue
                if not isinstance(meta, dict):
                    continue
                await self.db.execute(
                    "UPDATE tokens SET image=$2, description=$3, twitter=$4, telegram=$5, website=$6 WHERE mint=$1",
                    mint, _s(meta.get("image")), _s(meta.get("description"), 1000),
                    _s(meta.get("twitter")), _s(meta.get("telegram")), _s(meta.get("website")),
                )
                await self.db.execute("SELECT pg_notify('meta', $1)", mint)

    # ------------------------------------------------------------------ надёжность
    async def watchdog_loop(self) -> None:
        """Живость — по соединению (ping/pong), а не по тишине. Резерв Helius — только при обрыве PumpPortal."""
        down_alert_s = float(self.settings.reliability().get("silence_failover_s", 30))
        all_down_since: float | None = None
        gap_id: int | None = None
        while True:
            await asyncio.sleep(5)
            now = time.monotonic()
            # резерв Helius: открыть при обрыве PumpPortal > 15 с, закрыть через 2 мин стабильной работы
            if self.helius is not None and self.env.helius_stream == "backup":
                pp_down = self.pp.down_s() > 15
                if pp_down and not self.helius.gate.is_set():
                    self.helius.gate.set()
                    self.helius_opened_at = now
                    await self.db.system("⚠️ PumpPortal упал → резерв Helius (платный трафик)")
                elif (self.helius.gate.is_set() and self.pp.connected and now - self.pp.connected_since > 120
                      and now - self.helius_opened_at > 120):
                    self.helius.gate.clear()
                    await self.helius.close_now()
                    await self.db.system("✅ PumpPortal снова в строю → резерв Helius выключен")
            active = [s for s in self.sources if s.gate.is_set()]
            alive = [s for s in active if s.connected]
            if not alive:
                if all_down_since is None:
                    all_down_since = now
                if now - all_down_since > down_alert_s and gap_id is None:
                    gap_id = await self.db.fetchval(
                        "INSERT INTO ingest_gaps(source, started_at) VALUES ('all', now() - make_interval(secs => $1)) "
                        "RETURNING id", float(now - all_down_since))
                    await self.db.system(f"⚠️ Нет соединения ни с одним источником {int(now - all_down_since)} с — "
                                         "после восстановления дособеру пропущенное через RPC.")
            elif all_down_since is not None:
                if gap_id is not None:
                    await self.db.execute("UPDATE ingest_gaps SET ended_at=now() WHERE id=$1", gap_id)
                    await self.db.system("✅ Соединение восстановлено, досбор пропущенного через RPC (очередь).")
                    asyncio.create_task(self.backfill())
                all_down_since, gap_id = None, None
            # подписки на сделки есть, а сделок нет — PumpPortal не принимает ключ (нет 0.02 SOL на кошельке)
            # 23.11: и при работающей слежке Helius — она берёт только нужные кошельки и часть монет (лимит 250 МБ/сутки);
            # без сделок PumpPortal база пишет обрывки: у монет, ушедших на PumpSwap за час, записана 1 сделка из сотен
            if self.pp.has_key and self.pp.connected and now - self.pp.connected_since > 600:
                trades = sum(self.pp.messages_by_pool.values())
                if (len(self.pp.token_subs) >= 50 and trades == self._pp_trades_seen
                        and now - self._pp_warned > 6 * 3600):
                    self._pp_warned = now
                    await self.db.system("⛔ PumpPortal не отдаёт сделки монет — база собирает обрывки (бандлы, снайперы, "
                                         "пики и анализ — по неполным данным). Пополнить кошелёк API-ключа PumpPortal "
                                         "до 0.05 SOL" + (" (отказ по балансу)" if self.pp.refused else ""))
                if now - self._pp_mark_at > 600:
                    self._pp_trades_seen, self._pp_mark_at = trades, now
            # всплеск ошибок парсинга → «Система»
            threshold = self.settings.reliability().get("parse_error_alert_per_min", 20)
            errs = sum(s.stats.errors_last_minute() for s in self.sources) + self.backfill_stats.errors_last_minute()
            if errs >= threshold and time.monotonic() - self._parse_alerted > 600:
                self._parse_alerted = time.monotonic()
                last = next((s.stats.last_error for s in self.sources if s.stats.last_error), "")
                await self.db.system(
                    f"🛠 Всплеск ошибок парсинга Pump.fun: {errs}/мин. Возможно обновление программы. "
                    f"Последняя: {last}")

    async def backfill(self, max_tx: int | None = None) -> None:
        """Досбор после разрыва: подписи программы → очередь → один воркер с лимитом RPC."""
        until = await self.db.kv_get("last_signature")
        if not until:
            return
        max_tx = max_tx or int(self.settings.reliability().get("backfill_max_tx", 300))
        before = None
        sigs: list[dict[str, Any]] = []
        try:
            while len(sigs) < max_tx:
                page = await self.rpc.signatures_for(PUMP_PROGRAM, limit=min(1000, max_tx), before=before,
                                                     until=until)
                if not page:
                    break
                sigs.extend(page)
                before = page[-1]["signature"]
                if len(page) < min(1000, max_tx):
                    break
        except RpcError as exc:
            log.warning("досбор: %s", exc)
            return
        todo = [x for x in sigs if not x.get("err")][:max_tx]
        for item in todo:
            try:
                self.backfill_q.put_nowait(item)
            except asyncio.QueueFull:
                break
        log.info("досбор: в очереди %d транзакций", self.backfill_q.qsize())
        if len(sigs) >= max_tx:
            await self.db.system(f"Досбор ограничен {max_tx} транзакциями — часть разрыва не восстановлена.")

    async def backfill_worker(self) -> None:
        """Один воркер: скорость ограничена лимитом RPC, при 429 RPC сам делает паузу."""
        while True:
            item = await self.backfill_q.get()
            try:
                tx = await self.rpc.transaction(item["signature"])
            except RpcError:
                continue
            if not tx:
                continue
            for ev in parse_transaction(tx, item["signature"], item.get("slot"), "rpc", self.backfill_stats,
                                        self.pool_to_mint):
                await self.enqueue(ev)

    async def coverage_loop(self) -> None:
        """Раз в час: сверка числа созданных токенов между источниками."""
        target = self.settings.reliability().get("coverage_target", 0.995)
        while True:
            await asyncio.sleep(3600)
            rows = await self.db.fetch(
                """WITH w AS (SELECT source, mint FROM source_creates WHERE seen_at > now() - interval '1 hour'),
                        u AS (SELECT count(DISTINCT mint) n FROM w)
                   SELECT source, count(*) c, (SELECT n FROM u) total FROM w GROUP BY source""")
            if not rows:
                await self.db.system("Сверка покрытия: за час не получено ни одного создания токена.")
                continue
            parts = []
            low = False
            for r in rows:
                cov = r["c"] / r["total"] if r["total"] else 0
                parts.append(f"{r['source']}: {r['c']}/{r['total']} ({cov:.1%})")
                low |= r["source"] != "rpc" and cov < target
            await self.db.kv_set("coverage", {"at": datetime.now(timezone.utc).isoformat(), "parts": parts})
            active = [x for x in self.sources if x.gate.is_set()]   # выключенный резерв не считается
            if low or len(rows) < len(active):
                await self.db.system("📉 Покрытие за час: " + "; ".join(parts))
            await self.db.execute("DELETE FROM source_creates WHERE seen_at < now() - interval '2 days'")

    async def accounts_loop(self) -> None:
        """Подписка PumpPortal на сделки отслеживаемых кошельков (режим 2)."""
        while True:
            try:
                rows = await self.db.fetch(
                    "SELECT address FROM traders WHERE active UNION SELECT address FROM watchlist "
                    "WHERE kind IN ('trader', 'dev') UNION SELECT address FROM scout WHERE status = 'probation'")
                wallets = {r["address"] for r in rows} | set(await self.candidates())
                # испытание разведки: цену и выход по ним бумага берёт из Jupiter — подписка на все сделки их
                # монет (трафик Helius) не нужна, только на сделки самих кошельков
                self.scout_wallets = {r["address"] for r in await self.db.fetch(
                    "SELECT address FROM scout WHERE status = 'probation'")} - {r["address"] for r in await self.db.fetch(
                    "SELECT address FROM traders WHERE active UNION SELECT address FROM watchlist")}
                self.tracked_wallets = wallets
                pp_wallets = wallets if self.pp_accounts else set()
                self.tracked_mints = {r["mint"] for r in await self.db.fetch(
                    """SELECT mint FROM alerts WHERE created_at > now() - interval '1 day'
                       UNION SELECT address FROM watchlist WHERE kind='token' AND (until IS NULL OR until > now())
                       UNION SELECT mint FROM paper_positions WHERE closed_at IS NULL""")}
                self.prune_known()
                await self.watch_targets()
                for s in self.sources:
                    if isinstance(s, PumpPortalSource):
                        await s.set_accounts(pp_wallets)
                        tokens = await self.db.fetch(
                            "SELECT address FROM watchlist WHERE kind='token' AND (until IS NULL OR until > now())")
                        if tokens:
                            await s.subscribe_tokens([r["address"] for r in tokens])
            except Exception:  # noqa: BLE001
                log.exception("accounts_loop")
            await asyncio.sleep(60)

    async def health_loop(self) -> None:
        while True:
            await asyncio.sleep(10)
            if self.last_signature:
                await self.db.kv_set("last_signature", self.last_signature)
            for s in self.sources:
                if isinstance(s, (HeliusSource, LogsSource)) and s.last_slot:
                    await self.db.kv_set("last_slot", s.last_slot)
            now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            for (src, hour), (c, t) in list(self.hour_counts.items()):
                await self.db.execute(
                    "INSERT INTO source_stats(source, hour, creates, trades) VALUES ($1,$2,$3,$4) "
                    "ON CONFLICT (source, hour) DO UPDATE SET creates=source_stats.creates+EXCLUDED.creates, "
                    "trades=source_stats.trades+EXCLUDED.trades", src, hour, c, t)
                if hour < now_hour - timedelta(hours=1):
                    self.hour_counts.pop((src, hour), None)
                else:
                    self.hour_counts[(src, hour)] = [0, 0]
            # расход за сутки (UTC): трафик потоков, сообщения PumpPortal, запросы RPC
            day = datetime.now(timezone.utc).date().isoformat()
            if self._usage_day != day:
                self._usage_day, self._usage_base = day, self._usage_snapshot()
            cur = self._usage_snapshot()
            usage = {k: cur[k] - self._usage_base.get(k, 0) for k in cur}
            usage["helius_stream_credits"] = round(
                (usage.get("bytes_helius", 0) + usage.get("bytes_logs", 0) + usage.get("bytes_watch", 0)
                 + usage.get("bytes_reserves", 0)) / 1e6 * 20)
            await self.db.kv_set("usage:ingest", {"day": day, **usage})
            await self.db.kv_set("health:ingest", {
                "at": datetime.now(timezone.utc).isoformat(),
                "sources": [{
                    "name": s.name, "connected": s.connected, "active": s.gate.is_set(),
                    "silence_s": round(min(s.silence_s(), 1e6), 1), "down_s": round(min(s.down_s(), 1e6), 1),
                    "events": s.events, "reconnects": s.reconnects, "mb_in": round(s.bytes_in / 1e6, 1),
                    "parse_ok": s.stats.ok, "parse_errors": s.stats.errors,
                } for s in self.sources],
                "pumpportal_key": self.pp.has_key, "pumpportal_subs": len(self.pp.token_subs),
                "pumpportal_trades": sum(self.pp.messages_by_pool.values()), "pumpportal_refused": self.pp.refused,
                "helius_mode": self.env.helius_stream if self.helius else "off",
                "queue": self.queue.qsize(), "dropped": self.dropped, "extended": len(self.extended),
                "tracked_mints": len(self.tracked_mints), "backfill_queue": self.backfill_q.qsize(),
                "slot_queue": len(self.slot_queue), "slot_research": len(self.slot_research),
                "meta_queue": self.meta_queue.qsize(),
                "rpc_calls": self.rpc.calls, "rpc_429": self.rpc.rate_limited,
                "watch": None if self.watch is None else {
                    "connected": self.watch.connected, **self.watch.counts(), "events": self.watch.events,
                    "mb_today": round(usage.get("bytes_watch", 0) / 1e6, 1), "capped": self.watch_capped,
                    "devs": len(self.watch_devs), "reconnects": self.watch.reconnects,
                    "fallback": self.watch.on_fallback, "last_error": self.watch.last_error},
                "rpc_fallback": self.rpc.on_fallback,
                "pumpdata": None if self.pumpdata is None else self.pumpdata.health(),
                "reserves": None if self.reserves is None else {
                    "connected": self.reserves.connected, **self.reserves.counts(),
                    "mb_today": round(usage.get("bytes_reserves", 0) / 1e6, 1), "capped": self.reserve_capped,
                    "reconnects": self.reserves.reconnects, "last_error": self.reserves.last_error,
                    "url_public": "api.mainnet-beta" in self.reserves.url, **self.reserve_stats,
                    "rpc_last_error": self.reserve_last_error},
            })

    def _usage_snapshot(self) -> dict[str, float]:
        snap: dict[str, float] = {"rpc_calls": self.rpc.calls}
        for src in self.sources:
            snap[f"bytes_{src.name}"] = src.bytes_in
            snap[f"events_{src.name}"] = src.events
        if self.watch is not None:
            snap["bytes_watch"] = self.watch.bytes_in
            snap["events_watch"] = self.watch.events
        if self.reserves is not None:
            snap["bytes_reserves"] = self.reserves.bytes_in
            snap["events_reserves"] = self.reserves.events
        snap["pp_pump_msgs"] = self.pp.messages_by_pool.get("pump", 0)
        snap["pp_amm_msgs"] = self.pp.messages_by_pool.get("pump-amm", 0)
        return snap

    async def flush_reserves(self, ticks: list[ReserveTick], covers: list[ReserveCover]) -> None:
        if ticks:
            await self.db.p.executemany(
                "INSERT INTO reserve_ticks(mint, ts, slot, mc_sol, venue, quote_sol) VALUES ($1,$2,$3,$4,$5,$6)",
                [(t.mint, t.ts, t.slot, t.mc_sol, t.venue, t.quote_sol) for t in ticks])
        if covers:
            await self.db.p.executemany(
                """INSERT INTO reserve_cover(mint, started_at, ended_at) VALUES ($1,$2,$3)
                   ON CONFLICT (mint, started_at) DO UPDATE SET ended_at = GREATEST(reserve_cover.ended_at,
                                                                                    EXCLUDED.ended_at)""",
                [(c.mint, c.started_at, c.ended_at) for c in covers])

    async def retention_loop(self) -> None:
        while True:
            await asyncio.sleep(6 * 3600)
            await self.retention_once()

    async def retention_once(self) -> None:
        days = int(self.settings.reliability().get("raw_trades_days", 30))
        keep = int(self.settings.reliability().get("tracked_trades_days", 30))
        # 26.11 (B4): сделки отслеживаемых кошельков (свой список, «за ним», охотник) — tracked_trades_days (30),
        # остальные — raw_trades_days; результаты охотника и копии после находки (hunter_*) не удаляются
        await self.db.execute(
            """DELETE FROM trades WHERE ts < now() - make_interval(days => $1) AND trader NOT IN (
                   SELECT address FROM traders WHERE active OR manual
                   UNION SELECT address FROM watchlist WHERE kind IN ('trader', 'dev')
                   UNION SELECT address FROM hunter_wallets WHERE list <> 'archive')""", days)
        await self.db.execute("DELETE FROM trades WHERE ts < now() - make_interval(days => $1)", max(days, keep))
        await self.db.execute("DELETE FROM sol_transfers WHERE ts < now() - make_interval(days => $1)", days * 2)
        await self.db.execute("DELETE FROM outbox WHERE sent_at < now() - interval '7 days'")
        await self.db.execute("DELETE FROM reserve_ticks WHERE ts < now() - make_interval(days => $1)", days)
        await self.db.execute("DELETE FROM reserve_cover WHERE ended_at < now() - make_interval(days => $1)", days)
        await self.db.execute("DELETE FROM market_trades WHERE ts < now() - make_interval(days => $1)",
                              int(self.settings.reliability().get("market_trades_days", 7)))

    async def run(self) -> None:
        await self.load_known_mints()
        if not self.pp.has_key:
            await self.db.system("⚠️ Нет PUMPPORTAL_API_KEY: PumpPortal отдаёт только создания монет, сделки не "
                                 "собираются. Нужен ключ с балансом ≥0.02 SOL (pumpportal.fun).")
        if "api.mainnet-beta.solana.com" in self.rpc.url:
            await self.db.system("⚠️ RPC_URL указывает на публичный RPC Solana — нужен Helius (HELIUS_API_KEY).")
        self._usage_day, self._usage_base = "", {}
        tasks = [asyncio.create_task(s.run(self.enqueue), name=s.name) for s in self.sources]
        if self.watch is not None:
            tasks.append(asyncio.create_task(self.watch.run(self.enqueue), name="watch"))
        if self.reserves is not None:
            tasks.append(asyncio.create_task(self.reserves.run(self.enqueue), name="reserves"))
        if self.pumpdata is not None:
            tasks += [asyncio.create_task(self.pumpdata.run(), name="pumpdata"),
                      asyncio.create_task(self.pumpdata.flush_loop(), name="pumpdata-flush")]
        tasks += [asyncio.create_task(c()) for c in (
            self.worker, self.flush_loop, self.slot_loop, self.slot_research_loop, self.watchdog_loop, self.coverage_loop,
            self.accounts_loop, self.health_loop, self.retention_loop, self.extend_loop, self.backfill_worker)]
        tasks += [asyncio.create_task(self.meta_worker()) for _ in range(4)]
        asyncio.create_task(self.backfill())  # досбор после рестарта
        try:
            await asyncio.gather(*tasks)
        finally:
            await self.rpc.close()


def _s(v: Any, n: int = 300) -> str | None:
    return str(v)[:n] if isinstance(v, str) and v else None
