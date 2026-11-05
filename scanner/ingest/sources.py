"""Потоки событий: PumpPortal Data API (основной) и Helius Enhanced WebSockets (второй).

Оба переподключаются автоматически с нарастающей паузой и складывают нормализованные
события в общую очередь приёмника. Дедуп выполняет приёмник.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import random
import struct
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import base58
import websockets

from ..models import Event, Migration, TokenCreated, Trade
from ..solana import PUMP_AMM_PROGRAM, PUMP_PROGRAM, TOTAL_SUPPLY
from .pump_parser import D_AMM_BUY, D_AMM_SELL, KNOWN, ParseStats, TxContext, decode_event, parse_transaction

log = logging.getLogger(__name__)

Sink = Callable[[Event], Awaitable[None]]


class Source:
    """Базовый поток. Живость — по ping/pong websocket (ping_interval/ping_timeout), а не по «тишине»:
    если сервер не ответил на ping, websockets закрывает соединение и мы переподключаемся
    с нарастающей паузой от 5 с до 5 минут."""

    name = "source"
    min_backoff = 5.0
    max_backoff = 300.0

    def __init__(self) -> None:
        self.connected = False
        self.connected_since = 0.0
        self.disconnected_since = time.monotonic()
        self.last_event_at = 0.0      # monotonic
        self.last_event_wall: datetime | None = None
        self.reconnects = 0
        self.events = 0
        self.bytes_in = 0             # для оценки расхода (Helius считает трафик по МБ)
        self.stats = ParseStats()
        self._stop = asyncio.Event()
        self.gate = asyncio.Event()   # резервный поток подключается только при открытом «шлагбауме»
        self.gate.set()
        self._ws: Any = None

    def mark_event(self) -> None:
        self.events += 1
        self.last_event_at = time.monotonic()
        self.last_event_wall = datetime.now(timezone.utc)

    def silence_s(self) -> float:
        return time.monotonic() - self.last_event_at if self.last_event_at else float("inf")

    def down_s(self) -> float:
        """Сколько секунд нет соединения (0 — подключено)."""
        return 0.0 if self.connected else time.monotonic() - self.disconnected_since

    def _set_connected(self, value: bool) -> None:
        if value and not self.connected:
            self.connected_since = time.monotonic()
        if not value and self.connected:
            self.disconnected_since = time.monotonic()
        self.connected = value

    async def run(self, sink: Sink) -> None:
        backoff = self.min_backoff
        while not self._stop.is_set():
            await self.gate.wait()
            started = time.monotonic()
            try:
                await self._session(sink)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — любой сбой сети → переподключение
                log.warning("%s: соединение потеряно: %s", self.name, exc)
            self._set_connected(False)
            self._ws = None
            if not self.gate.is_set():
                continue  # резерв закрыли намеренно — ждём следующего открытия
            self.reconnects += 1
            if time.monotonic() - started > 120:
                backoff = self.min_backoff   # соединение жило долго — начинаем паузы сначала
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, self.max_backoff)

    async def close_now(self) -> None:
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass

    def stop(self) -> None:
        self._stop.set()

    async def _session(self, sink: Sink) -> None:  # pragma: no cover - abstract
        raise NotImplementedError


class PumpPortalSource(Source):
    """wss://pumpportal.fun/api/data — одно соединение на все подписки.

    subscribeNewToken / Migration — бесплатно; subscribeTokenTrade / AccountTrade — только с ключом,
    на кошельке которого ≥0.02 SOL (сделки на bonding curve бесплатны, PumpSwap — 0.01 SOL за 10 тыс.).
    Сделки каждой новой монеты слушаем window_min минут; дольше — если keep(mint) == True
    (алерт, бумажная позиция, наблюдение, растущий MC).
    """

    name = "pumpportal"

    def __init__(self, url: str, api_key: str = "", window_min: float = 30,
                 keep: Callable[[str], bool] | None = None):
        super().__init__()
        self.url = url + (f"?api-key={api_key}" if api_key else "")
        self.has_key = bool(api_key)
        self.window_s = window_min * 60
        self.keep = keep or (lambda _m: False)
        self.token_subs: dict[str, float] = {}     # mint → monotonic подписки
        self.account_subs: set[str] = set()
        self._pending_sub: set[str] = set()
        self._pending_unsub: set[str] = set()
        self._lock = asyncio.Lock()
        self.messages_by_pool: dict[str, int] = {"pump": 0, "pump-amm": 0}
        self.refused = ""          # ответ PumpPortal, если он отказал в подписке на сделки

    async def subscribe_tokens(self, mints: list[str]) -> None:
        if not self.has_key:
            return  # без ключа с балансом ≥0.02 SOL PumpPortal не отдаёт сделки
        now = time.monotonic()
        for m in mints:
            if m not in self.token_subs:
                self._pending_sub.add(m)
            self.token_subs[m] = now

    async def set_accounts(self, wallets: set[str]) -> None:
        if not self.has_key:
            return
        add = sorted(wallets - self.account_subs)
        rem = sorted(self.account_subs - wallets)
        self.account_subs = set(wallets)
        for i in range(0, len(add), 100):
            await self._send({"method": "subscribeAccountTrade", "keys": add[i:i + 100]})
        for i in range(0, len(rem), 100):
            await self._send({"method": "unsubscribeAccountTrade", "keys": rem[i:i + 100]})

    async def _send(self, msg: dict[str, Any]) -> bool:
        ws = self._ws
        if ws is None:
            return False
        try:
            async with self._lock:
                await ws.send(json.dumps(msg))
            return True
        except Exception as exc:  # noqa: BLE001
            log.debug("pumpportal send failed: %s", exc)
            return False

    async def _subs_loop(self) -> None:
        """Подписки пачками раз в 0.5 с; отписка по окончании окна, если монету не нужно держать."""
        last_expire = 0.0
        while True:
            await asyncio.sleep(0.5)
            if self._pending_sub:
                batch, self._pending_sub = sorted(self._pending_sub), set()
                for i in range(0, len(batch), 100):
                    if not await self._send({"method": "subscribeTokenTrade", "keys": batch[i:i + 100]}):
                        self._pending_sub.update(batch[i:])
                        break
            if time.monotonic() - last_expire > 30:
                last_expire = time.monotonic()
                cutoff = time.monotonic() - self.window_s
                old = [m for m, t in self.token_subs.items() if t < cutoff]
                for m in old:
                    if self.keep(m):
                        self.token_subs[m] = time.monotonic() - self.window_s + 600   # продлить на 10 минут
                    else:
                        self.token_subs.pop(m, None)
                        self._pending_unsub.add(m)
                if self._pending_unsub:
                    batch, self._pending_unsub = sorted(self._pending_unsub), set()
                    for i in range(0, len(batch), 100):
                        await self._send({"method": "unsubscribeTokenTrade", "keys": batch[i:i + 100]})

    async def _session(self, sink: Sink) -> None:
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, max_size=2**22,
                                      open_timeout=20) as ws:
            self._ws = ws
            self._set_connected(True)
            log.info("pumpportal: подключено (ключ: %s)", "да" if self.has_key else "нет — только создания")
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            await ws.send(json.dumps({"method": "subscribeMigration"}))
            keys = list(self.token_subs)
            for i in range(0, len(keys), 100):
                await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": keys[i:i + 100]}))
            if self.account_subs:
                acc = sorted(self.account_subs)
                for i in range(0, len(acc), 100):
                    await ws.send(json.dumps({"method": "subscribeAccountTrade", "keys": acc[i:i + 100]}))
            subs = asyncio.create_task(self._subs_loop())
            try:
                async for raw in ws:
                    self.bytes_in += len(raw)
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    ev = self.normalize(msg)
                    if ev is None:
                        if isinstance(msg, dict) and msg.get("message") and "only available" in str(msg["message"]):
                            log.warning("pumpportal: %s", msg["message"])
                            self.refused = str(msg["message"])[:300]
                        continue
                    self.mark_event()
                    if isinstance(ev, Trade):
                        self.messages_by_pool[ev.pool] = self.messages_by_pool.get(ev.pool, 0) + 1
                    if isinstance(ev, TokenCreated):
                        await self.subscribe_tokens([ev.mint])
                        await sink(ev)
                        if ev.initial_buy_tokens > 0:
                            # покупка дева входит в транзакцию создания: отдельным сообщением её нет
                            await sink(Trade(
                                signature=ev.signature, mint=ev.mint, trader=ev.deployer, is_buy=True,
                                sol_amount=ev.initial_buy_sol, token_amount=ev.initial_buy_tokens,
                                v_sol=ev.v_sol, v_tokens=ev.v_tokens, real_tokens=ev.real_tokens,
                                new_token_balance=ev.initial_buy_tokens, ts=ev.ts, source=self.name,
                                received_at=ev.received_at))
                        continue
                    await sink(ev)
            finally:
                subs.cancel()

    @staticmethod
    def normalize(msg: dict[str, Any]) -> Event | None:
        tx_type = msg.get("txType")
        sig = msg.get("signature")
        mint = msg.get("mint")
        if not sig or not mint or not tx_type:
            return None
        now = datetime.now(timezone.utc)
        v_sol = float(msg.get("vSolInBondingCurve") or 0)
        v_tok = float(msg.get("vTokensInBondingCurve") or 0)
        pool = msg.get("pool") or "pump"
        if tx_type == "create":
            return TokenCreated(
                signature=sig, mint=mint, deployer=msg.get("traderPublicKey", ""),
                creator=msg.get("traderPublicKey", ""), name=msg.get("name", ""),
                symbol=msg.get("symbol", ""), uri=msg.get("uri", ""),
                bonding_curve=msg.get("bondingCurveKey", ""),
                v_sol=v_sol or 30.0, v_tokens=v_tok or 1_073_000_000.0,
                real_tokens=max(0.0, (v_tok or 1_073_000_000.0) - 279_900_000.0),
                initial_buy_tokens=float(msg.get("initialBuy") or 0),
                initial_buy_sol=float(msg.get("solAmount") or 0),
                ts=now, source="pumpportal", received_at=now,
            )
        if tx_type in ("buy", "sell"):
            if not v_tok and msg.get("marketCapSol"):
                # PumpSwap: резервов кривой нет — восстанавливаем пропорцию из MC
                v_tok = TOTAL_SUPPLY
                v_sol = float(msg["marketCapSol"])
            real = max(0.0, v_tok - 279_900_000.0) if pool == "pump" else None
            return Trade(
                signature=sig, mint=mint, trader=msg.get("traderPublicKey", ""),
                is_buy=tx_type == "buy", sol_amount=float(msg.get("solAmount") or 0),
                token_amount=float(msg.get("tokenAmount") or 0), v_sol=v_sol, v_tokens=v_tok,
                real_tokens=real, new_token_balance=_f(msg.get("newTokenBalance")),
                pool="pump-amm" if pool not in ("pump", "") else "pump",
                ts=now, source="pumpportal", received_at=now,
            )
        if tx_type == "migrate":
            return Migration(signature=sig, mint=mint, pool=msg.get("pool", ""), ts=now, source="pumpportal")
        return None


def _f(v: Any) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


class HeliusSource(Source):
    """Helius transactionSubscribe (LaserStream WebSocket) по программам Pump.fun и PumpSwap.

    Нужен тариф Developer и выше. Трафик тарифицируется: 20 кредитов за МБ (~1 МБ/с на полном потоке),
    поэтому по умолчанию работает как резерв: подключается, только когда PumpPortal недоступен."""

    name = "helius"

    def __init__(self, url: str, pool_to_mint: dict[str, str]):
        super().__init__()
        self.url = url
        self.pool_to_mint = pool_to_mint
        self.last_slot: int | None = None
        self.last_signature: str | None = None

    async def _session(self, sink: Sink) -> None:
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, max_size=2**24,
                                      open_timeout=20) as ws:
            self._ws = ws
            self._set_connected(True)
            log.info("helius: подключено")
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": 1, "method": "transactionSubscribe",
                "params": [
                    # узкий фильтр подписки: потоки Helius тарифицируются по объёму
                    {"accountInclude": [PUMP_PROGRAM, PUMP_AMM_PROGRAM], "failed": False, "vote": False},
                    {"commitment": "processed", "encoding": "jsonParsed", "transactionDetails": "full",
                     "showRewards": False, "maxSupportedTransactionVersion": 1},   # 2026: есть транзакции v1
                ],
            }))
            async for raw in ws:
                self.bytes_in += len(raw)
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                result = ((msg.get("params") or {}).get("result")) if isinstance(msg, dict) else None
                if not result:
                    continue
                sig = result.get("signature")
                slot = result.get("slot")
                tx = result.get("transaction") or {}
                # Helius: result.transaction = {transaction: {...}, meta: {...}}
                if "meta" not in tx and isinstance(tx.get("transaction"), dict) and "meta" in tx["transaction"]:
                    tx = tx["transaction"]
                if not sig:
                    sigs = ((tx.get("transaction") or {}).get("signatures")) or []
                    sig = sigs[0] if sigs else None
                if not sig:
                    continue
                for ev in parse_transaction(tx, sig, slot, "helius", self.stats, self.pool_to_mint):
                    self.mark_event()
                    await sink(ev)
                self.last_slot = slot or self.last_slot
                self.last_signature = sig


class LogsSource(Source):
    """Все создания, сделки и миграции Pump.fun через logsSubscribe стандартного websocket RPC.

    ВНИМАНИЕ: Helius тарифицирует и этот поток (20 кредитов/МБ, ~1 МБ/с) — бесплатного лимита
    хватает на полдня. По умолчанию выключен (LOGS_STREAM=on — включить).

    События Anchor приходят в логах строками «Program data: <base64>» — транзакцию целиком
    скачивать не нужно. Работает на бесплатном тарифе Helius. Неудачные транзакции пропускаются.
    """

    name = "logs"

    def __init__(self, url: str, pool_to_mint: dict[str, str]):
        super().__init__()
        self.url = url
        self.pool_to_mint = pool_to_mint
        self.last_slot: int | None = None
        self.last_signature: str | None = None

    async def _session(self, sink: Sink) -> None:
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, max_size=2**24,
                                      open_timeout=20) as ws:
            self._ws = ws
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                                      "params": [{"mentions": [PUMP_PROGRAM]}, {"commitment": "processed"}]}))
            self._set_connected(True)
            log.info("logs: подключено")
            async for raw in ws:
                self.bytes_in += len(raw)
                for ev in self.parse_notification(raw, self.stats, self.pool_to_mint):
                    self.mark_event()
                    await sink(ev)
                    self.last_signature = getattr(ev, "signature", self.last_signature)
                    self.last_slot = getattr(ev, "slot", None) or self.last_slot

    @staticmethod
    def parse_notification(raw: str | bytes | dict[str, Any], stats: ParseStats,
                           pool_to_mint: dict[str, str] | None = None, source: str = "logs") -> list[Event]:
        if isinstance(raw, dict):
            msg = raw
        else:
            try:
                msg = json.loads(raw)
            except ValueError:
                return []
        result = (msg.get("params") or {}).get("result") if isinstance(msg, dict) else None
        if not result:
            return []
        value = result.get("value") or {}
        if value.get("err") is not None or not value.get("signature"):
            return []
        ctx = TxContext(value["signature"], (result.get("context") or {}).get("slot"), None, None, source)
        out: list[Event] = []
        seen: set[bytes] = set()
        for line in value.get("logs") or []:
            if not line.startswith("Program data: "):
                continue
            try:
                data = base64.b64decode(line[14:])
            except ValueError:
                continue
            if data[:8] not in KNOWN or data in seen:
                continue
            seen.add(data)
            try:
                ev = decode_event(data, ctx, pool_to_mint)
            except (ValueError, struct.error) as exc:
                stats.error(f"{ctx.signature[:12]}: {exc}")
                continue
            if ev is not None:
                stats.ok += 1
                out.append(ev)
        return out


class WatchSource(Source):
    """Сделки только нужных кошельков и монет: logsSubscribe {"mentions": [адрес]} на стандартном websocket Helius.

    Замена платной подписки PumpPortal на сделки: приходят транзакции только этих адресов (покупки и продажи
    отслеживаемых трейдеров, созданные ими монеты, сделки по монетам открытых позиций и девов-кандидатов).
    Бесплатный тариф Helius: 5 соединений × 1000 подписок, трафик 20 кредитов за МБ — на ~100 кошельков и
    десятки монет это тысячи кредитов в сутки. Источник не участвует в сверке покрытия и в поиске обрывов —
    он видит не весь рынок, а только выбранные адреса.
    """

    name = "watch"
    MAX_SUBS = 950

    def __init__(self, url: str, pool_to_mint: dict[str, str]):
        super().__init__()
        self.url = url
        self.pool_to_mint = pool_to_mint
        self.want: dict[str, str] = {}          # адрес → "wallet" | "coin"
        self.subs: dict[str, int] = {}          # адрес → id подписки
        self.pending: dict[int, str] = {}       # id запроса → адрес
        self.unsub_pending: set[int] = set()
        self._ids = iter(range(10, 1 << 62))
        self._changed = asyncio.Event()
        self.last_slot: int | None = None
        self.notifications = 0
        self.sid_addr: dict[int, str] = {}      # id подписки → адрес
        # processed присылает одну транзакцию дважды (двойной трафик и кредиты); confirmed — один раз, +~0.5 с
        self.commitment = "confirmed"
        # пул PumpSwap → монета через DexScreener (для покупок кошельков в уже мигрировавших монетах)
        self.resolve_pool: Callable[[str], Awaitable[str | None]] | None = None
        self.unresolved = 0

    def set_targets(self, wallets: list[str], coins: list[str]) -> None:
        """Кошельки важнее монет: при переполнении режем монеты."""
        want: dict[str, str] = {}
        for a in wallets:
            if len(want) >= self.MAX_SUBS:
                break
            want.setdefault(a, "wallet")
        for a in coins:
            if len(want) >= self.MAX_SUBS:
                break
            want.setdefault(a, "coin")
        if want != self.want:
            self.want = want
            self._changed.set()

    def add_coin(self, mint: str) -> None:
        """Мгновенно (не дожидаясь минутного обновления) — монета дева-кандидата сразу после создания."""
        if mint not in self.want and len(self.want) < self.MAX_SUBS:
            self.want[mint] = "coin"
            self._changed.set()

    def counts(self) -> dict[str, int]:
        kinds = list(self.want.values())
        return {"wallets": kinds.count("wallet"), "coins": kinds.count("coin"), "active": len(self.subs)}

    async def _sync(self, ws: Any) -> None:
        while True:
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            self._changed.clear()
            asked = set(self.pending.values())
            for addr in [a for a in self.want if a not in self.subs and a not in asked]:
                rid = next(self._ids)
                self.pending[rid] = addr
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, "method": "logsSubscribe",
                                          "params": [{"mentions": [addr]}, {"commitment": self.commitment}]}))
                await asyncio.sleep(0.02)
            for addr in [a for a in self.subs if a not in self.want]:
                sid = self.subs.pop(addr)
                rid = next(self._ids)
                self.unsub_pending.add(rid)
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, "method": "logsUnsubscribe", "params": [sid]}))
                await asyncio.sleep(0.02)

    async def _session(self, sink: Sink) -> None:
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, max_size=2**24,
                                      open_timeout=20) as ws:
            self._ws = ws
            self.subs.clear()
            self.pending.clear()
            self.sid_addr.clear()
            self._set_connected(True)
            self._changed.set()
            log.info("watch: подключено, адресов %d", len(self.want))
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
                    rid = msg.get("id")
                    if rid is not None:
                        if rid in self.pending:
                            addr = self.pending.pop(rid)
                            if isinstance(msg.get("result"), int) and addr in self.want:
                                self.subs[addr] = msg["result"]
                                self.sid_addr[msg["result"]] = addr
                            elif "error" in msg:
                                log.warning("watch: подписка на %s отклонена: %s", addr, msg["error"])
                        self.unsub_pending.discard(rid)
                        continue
                    if msg.get("method") != "logsNotification":
                        continue
                    self.notifications += 1
                    addr = self.sid_addr.get((msg.get("params") or {}).get("subscription"))
                    unknown = self.unknown_pools(msg)
                    if unknown:
                        if self.want.get(addr) == "coin" and len(unknown) == 1:
                            # подписка на монету: единственный незнакомый пул PumpSwap — пул этой монеты
                            self.pool_to_mint[unknown[0]] = addr
                        elif self.resolve_pool is not None:
                            asyncio.create_task(self._resolve_and_emit(msg, unknown, sink))
                            continue
                    await self._emit(msg, sink)
            finally:
                sync.cancel()

    async def _emit(self, msg: dict[str, Any], sink: Sink) -> None:
        for ev in LogsSource.parse_notification(msg, self.stats, self.pool_to_mint, source="watch"):
            self.mark_event()
            await sink(ev)
            self.last_slot = getattr(ev, "slot", None) or self.last_slot

    async def _resolve_and_emit(self, msg: dict[str, Any], pools: list[str], sink: Sink) -> None:
        for pool in pools:
            try:
                mint = await self.resolve_pool(pool) if self.resolve_pool else None
            except Exception:  # noqa: BLE001
                mint = None
            if mint:
                self.pool_to_mint[pool] = mint
            else:
                self.unresolved += 1
        await self._emit(msg, sink)

    def unknown_pools(self, msg: dict[str, Any]) -> list[str]:
        """Пулы PumpSwap из событий покупки/продажи, для которых монета ещё не известна."""
        value = (((msg.get("params") or {}).get("result") or {}).get("value") or {})
        if value.get("err") is not None:
            return []
        out: list[str] = []
        for line in value.get("logs") or []:
            if not line.startswith("Program data: "):
                continue
            try:
                data = base64.b64decode(line[14:])
            except ValueError:
                continue
            if data[:8] in (D_AMM_BUY, D_AMM_SELL) and len(data) >= 152:
                pool = base58.b58encode(data[120:152]).decode()
                if pool not in self.pool_to_mint and pool not in out:
                    out.append(pool)
        return out
