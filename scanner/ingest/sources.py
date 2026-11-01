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

import websockets

from ..models import Event, Migration, TokenCreated, Trade
from ..solana import PUMP_AMM_PROGRAM, PUMP_PROGRAM, TOTAL_SUPPLY
from .pump_parser import KNOWN, ParseStats, TxContext, decode_event, parse_transaction

log = logging.getLogger(__name__)

Sink = Callable[[Event], Awaitable[None]]


class Source:
    name = "source"

    def __init__(self) -> None:
        self.connected = False
        self.last_event_at = 0.0      # monotonic
        self.last_event_wall: datetime | None = None
        self.reconnects = 0
        self.events = 0
        self.stats = ParseStats()
        self._stop = asyncio.Event()

    def mark_event(self) -> None:
        self.events += 1
        self.last_event_at = time.monotonic()
        self.last_event_wall = datetime.now(timezone.utc)

    def silence_s(self) -> float:
        return time.monotonic() - self.last_event_at if self.last_event_at else float("inf")

    async def run(self, sink: Sink) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._session(sink)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — любой сбой сети → переподключение
                log.warning("%s: соединение потеряно: %s", self.name, exc)
            self.connected = False
            self.reconnects += 1
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60)

    def stop(self) -> None:
        self._stop.set()

    async def _session(self, sink: Sink) -> None:  # pragma: no cover - abstract
        raise NotImplementedError


class PumpPortalSource(Source):
    """wss://pumpportal.fun/api/data — subscribeNewToken / TokenTrade / AccountTrade / Migration."""

    name = "pumpportal"

    def __init__(self, url: str, api_key: str = "", trade_window_min: int = 60):
        super().__init__()
        self.url = url + (f"?api-key={api_key}" if api_key else "")
        self.has_key = bool(api_key)
        self.trade_window_s = trade_window_min * 60
        self.token_subs: dict[str, float] = {}     # mint → monotonic подписки
        self.account_subs: set[str] = set()
        self._ws: Any = None
        self._pending_tokens: list[str] = []
        self._lock = asyncio.Lock()

    async def subscribe_tokens(self, mints: list[str]) -> None:
        if not self.has_key:
            return  # без ключа с балансом ≥0.02 SOL PumpPortal не отдаёт сделки
        now = time.monotonic()
        new = [m for m in mints if m not in self.token_subs]
        for m in mints:
            self.token_subs[m] = now
        if new:
            await self._send({"method": "subscribeTokenTrade", "keys": new})

    async def set_accounts(self, wallets: set[str]) -> None:
        if not self.has_key:
            return
        add = sorted(wallets - self.account_subs)
        rem = sorted(self.account_subs - wallets)
        self.account_subs = set(wallets)
        if add:
            await self._send({"method": "subscribeAccountTrade", "keys": add})
        if rem:
            await self._send({"method": "unsubscribeAccountTrade", "keys": rem})

    async def _send(self, msg: dict[str, Any]) -> None:
        ws = self._ws
        if ws is None:
            return
        try:
            async with self._lock:
                await ws.send(json.dumps(msg))
        except Exception as exc:  # noqa: BLE001
            log.debug("pumpportal send failed: %s", exc)

    async def _expire_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            cutoff = time.monotonic() - self.trade_window_s
            old = [m for m, t in self.token_subs.items() if t < cutoff]
            for m in old:
                self.token_subs.pop(m, None)
            for i in range(0, len(old), 200):
                await self._send({"method": "unsubscribeTokenTrade", "keys": old[i : i + 200]})

    async def _session(self, sink: Sink) -> None:
        async with websockets.connect(self.url, ping_interval=20, max_size=2**22) as ws:
            self._ws = ws
            self.connected = True
            log.info("pumpportal: подключено")
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            await ws.send(json.dumps({"method": "subscribeMigration"}))
            keys = list(self.token_subs)
            for i in range(0, len(keys), 200):
                await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": keys[i : i + 200]}))
            if self.account_subs:
                await ws.send(json.dumps({"method": "subscribeAccountTrade", "keys": sorted(self.account_subs)}))
            expire = asyncio.create_task(self._expire_loop())
            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    ev = self.normalize(msg)
                    if ev is None:
                        continue
                    self.mark_event()
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
                expire.cancel()
                self._ws = None

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
    """Helius Enhanced WebSockets: transactionSubscribe по программам Pump.fun и PumpSwap."""

    name = "helius"

    def __init__(self, url: str, pool_to_mint: dict[str, str]):
        super().__init__()
        self.url = url
        self.pool_to_mint = pool_to_mint
        self.last_slot: int | None = None
        self.last_signature: str | None = None

    async def _session(self, sink: Sink) -> None:
        async with websockets.connect(self.url, ping_interval=30, max_size=2**24) as ws:
            self.connected = True
            log.info("helius: подключено")
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": 1, "method": "transactionSubscribe",
                "params": [
                    # узкий фильтр подписки: потоки Helius тарифицируются по объёму
                    {"accountInclude": [PUMP_PROGRAM, PUMP_AMM_PROGRAM], "failed": False, "vote": False},
                    {"commitment": "processed", "encoding": "jsonParsed", "transactionDetails": "full",
                     "showRewards": False, "maxSupportedTransactionVersion": 0},
                ],
            }))
            async for raw in ws:
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
        async with websockets.connect(self.url, ping_interval=30, max_size=2**24, open_timeout=20) as ws:
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                                      "params": [{"mentions": [PUMP_PROGRAM]}, {"commitment": "processed"}]}))
            self.connected = True
            log.info("logs: подключено")
            async for raw in ws:
                for ev in self.parse_notification(raw, self.stats, self.pool_to_mint):
                    self.mark_event()
                    await sink(ev)
                    self.last_signature = getattr(ev, "signature", self.last_signature)
                    self.last_slot = getattr(ev, "slot", None) or self.last_slot

    @staticmethod
    def parse_notification(raw: str | bytes, stats: ParseStats,
                           pool_to_mint: dict[str, str] | None = None) -> list[Event]:
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
        ctx = TxContext(value["signature"], (result.get("context") or {}).get("slot"), None, None, "logs")
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
