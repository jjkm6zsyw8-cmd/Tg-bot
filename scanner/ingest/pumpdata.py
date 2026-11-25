"""27.11 (владелец): весь рынок pump.fun и PumpSwap для охотника и моделей — живой поток pumpdata.fun.

Поток: wss://pumpdata.fun/stream?key=…&types=trade&min_sol=… (подписка $199/мес за весь рынок на одно соединение,
30 дней бесплатно с первого подключения ключа). Сделки кривой и PumpSwap ~200/с по всему рынку — берём от min_sol
(pumpdata_min_sol), пишем в свою таблицу market_trades. В сигналы бота они НЕ идут: задержка потока ~1.5 с, а бот
входит через 1.36 с — входы «за ним» остаются на слежке Helius. Читают их только охотник (кандидаты, сделки кошелька)
и модель копии (цены монет) — вместе со сделками из trades, повтор одной сделки отсекается по блоку.

Поля сделки (документация pumpdata.fun, llms.txt): venue (curve | pumpswap), mint, pool, wallet, side, sol,
token_amount, slot, idx, ts / block_time; пул в другой валюте — quote_mint / quote_amount вместо sol (такие пропускаем).
Подписи, резервов и цены в потоке нет: MC = sol / token_amount × 1 млрд (средняя цена сделки). Ключ — PUMPDATA_API_KEY
в .env; отстающего клиента поток отключает, поэтому чтение только кладёт строки в очередь, запись — отдельно.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable

import websockets

log = logging.getLogger(__name__)

TOTAL_SUPPLY = 1_000_000_000.0
MAX_QUEUE = 200_000
BATCH = 5_000
Row = tuple[int, int, datetime, str, str, bool, float, float, float, str]


def _ts(v: Any) -> datetime:
    if isinstance(v, (int, float)) and v > 0:
        return datetime.fromtimestamp(v / 1000 if v > 1e12 else v, timezone.utc)
    if isinstance(v, str) and v:
        try:
            if v.replace(".", "", 1).isdigit():
                return _ts(float(v))
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def parse(msg: Any) -> Row | None:
    """Сделка потока → строка market_trades: (slot, idx, ts, mint, кошелёк, покупка, SOL, токенов, MC в SOL, пул)."""
    if not isinstance(msg, dict) or msg.get("type") != "trade" or msg.get("quote_mint"):
        return None
    mint, wallet, slot, idx = msg.get("mint"), msg.get("wallet"), msg.get("slot"), msg.get("idx")
    side = str(msg.get("side") or "").lower()
    if not mint or not wallet or slot is None or idx is None or side not in ("buy", "sell", "b", "s"):
        return None
    try:
        sol, tok = float(msg.get("sol") or 0), float(msg.get("token_amount") or 0)
    except (TypeError, ValueError):
        return None
    if sol > 1e5:
        sol /= 1e9                        # пришло в лампортах
    if tok > 1.1 * TOTAL_SUPPLY:
        tok /= 1e6                        # пришло в сырых единицах (6 знаков)
    if sol <= 0 or tok <= 0:
        return None
    venue = str(msg.get("venue") or "").lower()
    pool = "pump-amm" if ("swap" in venue or "amm" in venue) else "pump"
    return (int(slot), int(idx), _ts(msg.get("block_time") or msg.get("ts")), str(mint), str(wallet),
            side in ("buy", "b"), sol, tok, sol / tok * TOTAL_SUPPLY, pool)


class PumpDataStream:
    name = "pumpdata"

    def __init__(self, db: Any, url: str, key: str, min_sol: float = 0.2,
                 on_rows: Callable[[int], None] | None = None):
        self.db, self.url, self.key, self.min_sol = db, url, key, float(min_sol)
        self.on_rows = on_rows
        self.queue: deque[Row] = deque()
        self.connected = False
        self.events = self.rows = self.dropped = self.reconnects = 0
        self.last_error = ""

    def stream_url(self) -> str:
        return f"{self.url}?key={self.key}&types=trade&min_sol={self.min_sol:g}"

    def take(self, raw: str | bytes) -> None:
        """Одно сообщение потока: в очередь (без ожиданий — отстающего клиента поток отключает)."""
        self.events += 1
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        for m in (msg if isinstance(msg, list) else [msg]):
            row = parse(m)
            if row is None:
                if isinstance(m, dict) and (m.get("error") or m.get("type") == "error"):
                    self.last_error = str(m.get("error") or m.get("message") or m)[:200]
                continue
            if len(self.queue) >= MAX_QUEUE:
                self.dropped += 1
                continue
            self.queue.append(row)

    async def run(self) -> None:
        backoff = 5.0
        while True:
            try:
                async with websockets.connect(self.stream_url(), ping_interval=20, ping_timeout=20, max_size=2**22,
                                              open_timeout=20) as ws:
                    self.connected, backoff = True, 5.0
                    log.info("pumpdata: подключено (сделки от %g SOL)", self.min_sol)
                    async for raw in ws:
                        self.take(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — обрыв: переподключиться
                self.last_error = f"{type(exc).__name__}: {exc}"[:200].replace(self.key, "…") if self.key else \
                    f"{type(exc).__name__}: {exc}"[:200]
                log.warning("pumpdata: %s", self.last_error)
            self.connected = False
            self.reconnects += 1
            await asyncio.sleep(backoff)
            backoff = min(300.0, backoff * 2)

    async def flush(self) -> int:
        if not self.queue:
            return 0
        rows = [self.queue.popleft() for _ in range(min(len(self.queue), BATCH))]
        await self.db.p.executemany(
            """INSERT INTO market_trades(slot, idx, ts, mint, trader, is_buy, sol_amount, token_amount, mc_sol, pool)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (slot, idx) DO NOTHING""", rows)
        self.rows += len(rows)
        if self.on_rows is not None:
            self.on_rows(len(rows))
        return len(rows)

    async def flush_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            try:
                while await self.flush() == BATCH:
                    pass
            except Exception:  # noqa: BLE001 — запись не должна ронять приём
                log.exception("pumpdata: запись")

    def health(self) -> dict[str, Any]:
        return {"connected": self.connected, "events": self.events, "rows": self.rows, "queue": len(self.queue),
                "dropped": self.dropped, "reconnects": self.reconnects, "min_sol": self.min_sol,
                "last_error": self.last_error}
