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
import logging
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

from ..analytics.curve import mc_sol
from ..config import Env, Settings
from ..db.pool import Database
from ..models import Event, Migration, SolTransfer, TokenCreated, Trade
from ..solana import PUMP_PROGRAM
from .pump_parser import ParseStats, parse_transaction
from .rpc import RpcError, SolanaRpc
from .sources import HeliusSource, LogsSource, PumpPortalSource, Source

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
        self.rpc = SolanaRpc(env.rpc_endpoint, rps=env.rpc_rps)
        self.pool_to_mint: dict[str, str] = {}
        # сделки каждой новой монеты — первые trade_window_min минут, дольше — если монету нужно держать
        self.window_s = 60 * float(rel.get("trade_window_min", 30))
        self.max_extend_s = 3600 * float(rel.get("trade_extend_max_h", 6))
        self.pp = PumpPortalSource(env.pumpportal_ws, env.pumpportal_api_key, self.window_s / 60,
                                   keep=self.keep_mint)
        self.sources: list[Source] = [self.pp]
        self.helius: HeliusSource | None = None
        if env.helius_ws_url:
            self.helius = HeliusSource(env.helius_ws_url, self.pool_to_mint)
            if env.helius_stream != "primary":
                self.helius.gate.clear()   # резерв: подключается только при обрыве PumpPortal
            self.sources.append(self.helius)
        if env.logs_ws_url:
            self.sources.append(LogsSource(env.logs_ws_url, self.pool_to_mint))
        self.store_hours = float(rel.get("trade_store_hours", 24))
        self.known_mints: dict[str, float] = {}   # mint → unix-время создания
        self.tracked_wallets: set[str] = set()
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
        key = event_key(ev)
        is_new = self.seen.add(key)
        src = getattr(ev, "source", "") or "rpc"
        if isinstance(ev, TokenCreated):
            self.buf_source_creates.append((src, ev.mint))
            self._count(src, creates=1)
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
            self.created_mono[ev.mint] = time.monotonic()
            self.known_mints[ev.mint] = time.time()
            if ev.slot is None and not self.logs_alive():
                self.slot_queue[ev.signature] = time.monotonic()
            if ev.uri:
                try:
                    self.meta_queue.put_nowait((ev.mint, ev.uri))
                except asyncio.QueueFull:
                    pass
            for s in self.sources:
                if isinstance(s, PumpPortalSource):
                    await s.subscribe_tokens([ev.mint])
        elif isinstance(ev, Trade):
            if not self.should_store(ev):
                return
            self._track_mc(ev)
            self.buf_trades.append(ev)
            born = self.created_mono.get(ev.mint)
            if ev.slot is None and born and time.monotonic() - born < 15 and not self.logs_alive():
                self.slot_queue[ev.signature] = time.monotonic()
        elif isinstance(ev, Migration):
            self.buf_migr.append(ev)
            if ev.pool:
                self.pool_to_mint[ev.pool] = ev.mint
        elif isinstance(ev, SolTransfer):
            self.buf_transfers.append(ev)

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
                rows = list(slots.items())
                await self.db.p.executemany("UPDATE trades SET slot=$2 WHERE signature=$1 AND slot IS NULL", rows)
                await self.db.p.executemany(
                    "UPDATE tokens SET created_slot=$2 WHERE signature=$1 AND created_slot IS NULL", rows)
                await self.db.execute("SELECT pg_notify('slots', '')")
            cutoff = now - 120
            for m in [m for m, t in self.created_mono.items() if t < cutoff]:
                self.created_mono.pop(m, None)

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
            if self.pp.has_key and self.pp.connected and now - self.pp.connected_since > 600:
                trades = sum(self.pp.messages_by_pool.values())
                if (len(self.pp.token_subs) >= 50 and trades == self._pp_trades_seen
                        and now - self._pp_warned > 6 * 3600):
                    self._pp_warned = now
                    await self.db.system("⛔ PumpPortal: нет сделок — пополнить кошелёк ключа ≥0.02 SOL"
                                         + (" (отказ по балансу)" if self.pp.refused else ""))
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
                    "SELECT address FROM traders WHERE active UNION SELECT address FROM watchlist WHERE kind='trader'")
                wallets = {r["address"] for r in rows}
                self.tracked_wallets = wallets
                self.tracked_mints = {r["mint"] for r in await self.db.fetch(
                    """SELECT mint FROM alerts WHERE created_at > now() - interval '1 day'
                       UNION SELECT address FROM watchlist WHERE kind='token' AND (until IS NULL OR until > now())
                       UNION SELECT mint FROM paper_positions WHERE closed_at IS NULL""")}
                self.prune_known()
                for s in self.sources:
                    if isinstance(s, PumpPortalSource):
                        await s.set_accounts(wallets)
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
                (usage.get("bytes_helius", 0) + usage.get("bytes_logs", 0)) / 1e6 * 20)
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
                "slot_queue": len(self.slot_queue), "meta_queue": self.meta_queue.qsize(),
                "rpc_calls": self.rpc.calls, "rpc_429": self.rpc.rate_limited,
            })

    def _usage_snapshot(self) -> dict[str, float]:
        snap: dict[str, float] = {"rpc_calls": self.rpc.calls}
        for src in self.sources:
            snap[f"bytes_{src.name}"] = src.bytes_in
            snap[f"events_{src.name}"] = src.events
        snap["pp_pump_msgs"] = self.pp.messages_by_pool.get("pump", 0)
        snap["pp_amm_msgs"] = self.pp.messages_by_pool.get("pump-amm", 0)
        return snap

    async def retention_loop(self) -> None:
        days = int(self.settings.reliability().get("raw_trades_days", 30))
        while True:
            await asyncio.sleep(6 * 3600)
            await self.db.execute("DELETE FROM trades WHERE ts < now() - make_interval(days => $1)", days)
            await self.db.execute("DELETE FROM sol_transfers WHERE ts < now() - make_interval(days => $1)", days * 2)
            await self.db.execute("DELETE FROM outbox WHERE sent_at < now() - interval '7 days'")

    async def run(self) -> None:
        await self.load_known_mints()
        if not self.pp.has_key:
            await self.db.system("⚠️ Нет PUMPPORTAL_API_KEY: PumpPortal отдаёт только создания монет, сделки не "
                                 "собираются. Нужен ключ с балансом ≥0.02 SOL (pumpportal.fun).")
        if "api.mainnet-beta.solana.com" in self.rpc.url:
            await self.db.system("⚠️ RPC_URL указывает на публичный RPC Solana — нужен Helius (HELIUS_API_KEY).")
        self._usage_day, self._usage_base = "", {}
        tasks = [asyncio.create_task(s.run(self.enqueue), name=s.name) for s in self.sources]
        tasks += [asyncio.create_task(c()) for c in (
            self.worker, self.flush_loop, self.slot_loop, self.watchdog_loop, self.coverage_loop,
            self.accounts_loop, self.health_loop, self.retention_loop, self.extend_loop, self.backfill_worker)]
        tasks += [asyncio.create_task(self.meta_worker()) for _ in range(4)]
        asyncio.create_task(self.backfill())  # досбор после рестарта
        try:
            await asyncio.gather(*tasks)
        finally:
            await self.rpc.close()


def _s(v: Any, n: int = 300) -> str | None:
    return str(v)[:n] if isinstance(v, str) and v else None
