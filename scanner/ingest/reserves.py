"""25.11 (владелец): подписка на резервы монеты — accountSubscribe на счёт кривой pump.fun или на два хранилища пула
PumpSwap (base и quote), на 35 мин после покупки отслеживаемого кошелька или кандидата. Каждое изменение резервов —
цена (ReserveTick) без потока всех сделок: полный путь цены для оценки копий «как бот».

Своё соединение websocket к Helius (не мешает слежке за кошельками). Трафик: ~450–600 байт на изменение, 1–2 счёта на
монету; 740 окон в сутки — оценка 10–100 МБ/сутки (20 кредитов Helius за МБ). Лимит — reliability.reserve_daily_mb.

Цена пула = (quote + virtual_quote) / base (virtual_quote — из счёта пула, читается один раз). Оба хранилища меняются
в одной транзакции и приходят двумя уведомлениями одного слота — цена выдаётся, когда у обоих один и тот же слот.
Окно слежения (ReserveCover) отдаётся раз в минуту и при снятии подписки: внутри окна тишина = цена не менялась.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import websockets

from ..models import ReserveCover, ReserveTick
from .sources import Sink, Source

log = logging.getLogger(__name__)

SUPPLY_RAW = 1_000_000_000 * 1_000_000      # 1 млрд токенов × 6 знаков
LAMPORTS = 1_000_000_000
COVER_EVERY_S = 60.0


@dataclass
class Target:
    """Что слушать по монете: кривая (один счёт) или пул (два хранилища + виртуальный резерв SOL)."""
    venue: str                              # pump | pump-amm
    accounts: list[str]                     # pump: [кривая]; pump-amm: [base_vault, quote_vault]
    virtual_quote: int = 0
    base: tuple[int, int] | None = None     # (остаток, слот)
    quote: tuple[int, int] | None = None
    started: float | None = None            # когда подписались на все счета
    cover_sent: float = 0.0
    complete: bool = False                  # кривая закрылась — ждём пул PumpSwap


def curve_mc(data: bytes) -> tuple[float, float, bool] | None:
    """Счёт кривой → (MC в SOL, настоящих SOL, кривая закрыта)."""
    from ..real.pump import parse_curve
    try:
        c = parse_curve(data)
    except ValueError:
        return None
    mc = c.v_quote / c.v_tokens * SUPPLY_RAW / LAMPORTS if c.v_tokens else 0.0
    return mc, c.real_quote / LAMPORTS, c.complete


def pool_mc(base: int, quote: int, virtual_quote: int) -> float:
    return (quote + virtual_quote) / base * 1e15 / 1e9 if base > 0 else 0.0


def token_amount(data: bytes) -> int | None:
    return int.from_bytes(data[64:72], "little") if len(data) >= 72 else None


class ReserveSource(Source):
    """25.11 (2): на сервере за час 2 монеты вместо ~30 и last_error 429. Причина в коде: при первом 429 от Helius
    источник навсегда уходил на запасной публичный узел Solana (там подписки режутся тем же 429) и не возвращался.
    Теперь: только основной узел (публичный для этого не годится), подписки не залпом (SUB_GAP_S), подключение — после
    слежки за кошельками (ready), паузы после отказа — от 30 с; счётчики ошибок — в health:ingest.reserves."""
    name = "reserves"
    MAX_SUBS = 400
    SUB_GAP_S = 0.2                # не больше 5 запросов подписки в секунду
    COIN_DAILY_BYTES = 3_000_000   # 26.11 (B1): на одну монету за сутки не больше (монета-шторм не съест лимит)
    JUMP_X = 0.5                   # 26.11 (B1): цена пула прыгнула больше чем на 50% за одно изменение — перечитать пул
    REREAD_S = 300.0
    START_DELAY_S = 5.0            # перед каждым подключением (+10 с перед первым) — не в одну секунду с другими
    min_backoff = 30.0

    def __init__(self, url: str, fallback: str = "", ready: Callable[[], bool] | None = None):
        super().__init__()
        self.url = url
        self.primary = url
        self.ready = ready                           # подключаться, когда слежка за кошельками уже на связи
        self.refused = 0                             # сколько раз узел отказал (429 и т. п.)
        self.sub_errors = 0
        self.last_sub_error = ""
        self.targets: dict[str, Target] = {}         # монета → что слушаем
        self.acc_mint: dict[str, str] = {}           # счёт → монета
        self.subs: dict[str, int] = {}               # счёт → id подписки
        self.sid_acc: dict[int, str] = {}
        self.pending: dict[int, str] = {}            # id запроса → счёт
        self._ids = iter(range(1 << 40, 1 << 62))     # не пересекаются с номерами запросов слежки
        self._changed = asyncio.Event()
        self.ticks = 0
        self.on_complete: Callable[[str], Awaitable[None]] | None = None   # кривая закрылась → переезд на пул
        self.on_jump: Callable[[str], Awaitable[None]] | None = None       # резкий скачок цены пула → перечитать пул
        self.coin_bytes: dict[str, int] = {}
        self.coin_day = ""
        self.capped: set[str] = set()                # монеты, у которых кончился суточный лимит
        self.last_mc: dict[str, float] = {}
        self.reread_at: dict[str, float] = {}
        self._sink: Sink | None = None
        # 27.11 (владелец: «резервы лежат, 429 ×8 — переведи на соединение слежки»): общий режим — своего соединения
        # нет, подписки идут через WatchSource (attach / handle / detach), там же приходят уведомления
        self._send: Callable[[dict[str, Any]], Awaitable[bool]] | None = None
        self._task: asyncio.Task[None] | None = None
        self.shared = False

    # ------------------------------------------------------------------ цели
    def set_target(self, mint: str, target: Target) -> bool:
        """Добавить / заменить монету. False — нет места в подписках."""
        old = self.targets.get(mint)
        extra = len(target.accounts) - (len(old.accounts) if old else 0)
        if old is None and len(self.acc_mint) + extra > self.MAX_SUBS:
            return False
        if old is not None:
            self._drop_accounts(mint, old)
        self.targets[mint] = target
        for a in target.accounts:
            self.acc_mint[a] = mint
        self._changed.set()
        return True

    def drop(self, mint: str) -> None:
        t = self.targets.pop(mint, None)
        if t is None:
            return
        self._drop_accounts(mint, t)
        self._cover(mint, t, final=True)
        self._changed.set()

    def _drop_accounts(self, mint: str, t: Target) -> None:
        for a in t.accounts:
            if self.acc_mint.get(a) == mint:
                self.acc_mint.pop(a, None)

    def counts(self) -> dict[str, Any]:
        return {"coins": len(self.targets), "accounts": len(self.acc_mint), "active": len(self.subs),
                "ticks": self.ticks, "refused": self.refused, "sub_errors": self.sub_errors,
                "last_sub_error": self.last_sub_error, "capped_coins": len(self.capped)}

    # ------------------------------------------------------------------ окно слежения
    def _cover(self, mint: str, t: Target, final: bool = False) -> None:
        if t.started is None or self._sink is None:
            return
        now = time.time()
        if not final and now - t.cover_sent < COVER_EVERY_S:
            return
        t.cover_sent = now
        ev = ReserveCover(mint, datetime.fromtimestamp(t.started, timezone.utc), datetime.fromtimestamp(now, timezone.utc))
        asyncio.create_task(self._sink(ev))
        if final:
            t.started = None

    def _all_subscribed(self, mint: str) -> bool:
        t = self.targets.get(mint)
        return bool(t) and all(a in self.subs for a in t.accounts)

    # ------------------------------------------------------------------ уведомления
    def on_account(self, acc: str, data: bytes, slot: int | None) -> ReserveTick | None:
        """Изменение счёта → цена (или None, если пара хранилищ ещё не сошлась по слоту)."""
        mint = self.acc_mint.get(acc)
        t = self.targets.get(mint or "")
        if t is None or mint is None:
            return None
        if t.venue == "pump":
            r = curve_mc(data)
            if r is None:
                return None
            mc, real_sol, complete = r
            if complete and not t.complete:
                t.complete = True
                if self.on_complete is not None:
                    asyncio.create_task(self.on_complete(mint))  # монета ушла на PumpSwap — слушаем пул
            if complete or mc <= 0:
                return None
            return ReserveTick(mint, mc, "pump", slot, real_sol)
        amount = token_amount(data)
        if amount is None:
            return None
        if acc == t.accounts[0]:
            t.base = (amount, slot or 0)
        else:
            t.quote = (amount, slot or 0)
        if t.base is None or t.quote is None or t.base[1] != t.quote[1]:
            return None
        mc = pool_mc(t.base[0], t.quote[0], t.virtual_quote)
        prev = self.last_mc.get(mint)
        if mc > 0:
            self.last_mc[mint] = mc
        if (prev and mc > 0 and abs(mc / prev - 1) > self.JUMP_X and self.on_jump is not None
                and time.time() - self.reread_at.get(mint, 0.0) > self.REREAD_S):
            self.reread_at[mint] = time.time()
            asyncio.create_task(self.on_jump(mint))     # виртуальный резерв или пул могли смениться — перечитать
        return ReserveTick(mint, mc, "pump-amm", slot, t.quote[0] / LAMPORTS) if mc > 0 else None

    def count_bytes(self, mint: str, n: int) -> bool:
        """Трафик монеты за сутки (UTC); False — лимит кончился, монету снимаем до конца суток."""
        day = datetime.now(timezone.utc).date().isoformat()
        if day != self.coin_day:
            self.coin_day, self.coin_bytes, self.capped = day, {}, set()
        self.coin_bytes[mint] = self.coin_bytes.get(mint, 0) + n
        if self.coin_bytes[mint] > self.COIN_DAILY_BYTES:
            self.capped.add(mint)
            self.drop(mint)
            log.info("reserves: %s — суточный лимит %d Б на монету, снята до конца суток", mint, self.COIN_DAILY_BYTES)
            return False
        return True

    # ------------------------------------------------------------------ общий режим (соединение слежки)
    async def attach(self, send: Callable[[dict[str, Any]], Awaitable[bool]], sink: Sink) -> None:
        """Слежка подключилась: подписки — через её соединение."""
        self.shared = True
        self._sink, self._send = sink, send
        self.subs.clear()
        self.sid_acc.clear()
        self.pending.clear()
        self._set_connected(True)
        self._changed.set()
        if self._task is not None:
            self._task.cancel()
        self._task = asyncio.create_task(self._sync(None))
        log.info("reserves: в соединении слежки, монет %d, счетов %d", len(self.targets), len(self.acc_mint))

    def detach(self) -> None:
        """Соединение слежки оборвалось: окна слежения закрыты, подписки — заново при следующем подключении."""
        if self._task is not None:
            self._task.cancel()
            self._task = None
        for mint, t in list(self.targets.items()):
            self._cover(mint, t, final=True)
        self._set_connected(False)

    async def _out(self, ws: Any, obj: dict[str, Any]) -> None:
        if ws is not None:
            await ws.send(json.dumps(obj))
        elif self._send is not None:
            await self._send(obj)

    async def handle(self, msg: dict[str, Any], n: int) -> bool:
        """Сообщение из соединения: ответ на нашу подписку или уведомление о счёте — разобрать (True), иначе — не наше."""
        rid = msg.get("id")
        if rid is not None:
            if rid not in self.pending and not (isinstance(rid, int) and rid >= (1 << 40)):
                return False
            acc = self.pending.pop(rid, None)
            if acc is not None and isinstance(msg.get("result"), int) and acc in self.acc_mint:
                self.subs[acc] = msg["result"]
                self.sid_acc[msg["result"]] = acc
                mint = self.acc_mint[acc]
                t = self.targets.get(mint)
                if t is not None and t.started is None and self._all_subscribed(mint):
                    t.started = time.time()
            elif acc is not None and "error" in msg:
                self.sub_errors += 1
                self.last_sub_error = str(msg["error"])[:160]
                if self.sub_errors <= 5 or self.sub_errors % 100 == 0:
                    log.warning("reserves: подписка на %s отклонена: %s", acc, msg["error"])
            return True
        if msg.get("method") != "accountNotification":
            return False
        res = (msg.get("params") or {}).get("result") or {}
        acc = self.sid_acc.get((msg.get("params") or {}).get("subscription"))
        data = ((res.get("value") or {}).get("data") or [None])[0]
        if acc is None or not data:
            return True
        mint = self.acc_mint.get(acc)
        if mint is not None and not self.count_bytes(mint, n):
            return True
        try:
            raw_data = base64.b64decode(data)
        except ValueError:
            return True
        tick = self.on_account(acc, raw_data, (res.get("context") or {}).get("slot"))
        if tick is not None and self._sink is not None:
            self.ticks += 1
            self.mark_event()
            await self._sink(tick)
        return True

    # ------------------------------------------------------------------ соединение
    async def _sync(self, ws: Any) -> None:
        while True:
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=COVER_EVERY_S)
            except asyncio.TimeoutError:
                pass
            self._changed.clear()
            for mint, t in list(self.targets.items()):
                self._cover(mint, t)
            asked = set(self.pending.values())
            for acc in [a for a in self.acc_mint if a not in self.subs and a not in asked]:
                rid = next(self._ids)
                self.pending[rid] = acc
                await self._out(ws, {"jsonrpc": "2.0", "id": rid, "method": "accountSubscribe",
                                     "params": [acc, {"encoding": "base64", "commitment": "confirmed"}]})
                await asyncio.sleep(self.SUB_GAP_S)
            for acc in [a for a in self.subs if a not in self.acc_mint]:
                sid = self.subs.pop(acc)
                self.sid_acc.pop(sid, None)
                await self._out(ws, {"jsonrpc": "2.0", "id": next(self._ids), "method": "accountUnsubscribe",
                                     "params": [sid]})
                await asyncio.sleep(self.SUB_GAP_S)

    def on_failure(self, exc: Exception) -> None:
        if any(c in str(exc) for c in ("429", "401", "403")):
            self.refused += 1
            log.warning("reserves: узел отказал (%s) — повтор после паузы, на публичный узел не ухожу", str(exc)[:120])

    async def _session(self, sink: Sink) -> None:
        self._sink = sink
        for _ in range(120):                         # до 2 мин ждём, пока подключится слежка за кошельками
            if self.ready is None or self.ready():
                break
            await asyncio.sleep(1.0)
        await asyncio.sleep(self.START_DELAY_S * (1 + 2 * (self.reconnects == 0)))
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, max_size=2**22,
                                      open_timeout=20) as ws:
            self._ws = ws
            self.subs.clear()
            self.sid_acc.clear()
            self.pending.clear()
            self._set_connected(True)
            self._changed.set()
            log.info("reserves: подключено, монет %d, счетов %d", len(self.targets), len(self.acc_mint))
            sync = asyncio.create_task(self._sync(ws))
            try:
                async for raw in ws:
                    self.bytes_in += len(raw)
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    if not isinstance(msg, dict):
                        continue
                    await self.handle(msg, len(raw))
            finally:
                sync.cancel()
                for mint, t in list(self.targets.items()):    # обрыв — окно слежения закрыто
                    self._cover(mint, t, final=True)
