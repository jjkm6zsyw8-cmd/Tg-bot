"""Курс SOL из двух независимых источников (для пересчёта MC в USD)."""
from __future__ import annotations

import asyncio
import logging
from statistics import median

import aiohttp

from ..db.pool import Database

log = logging.getLogger(__name__)

SOURCES = {
    "binance": ("https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT", lambda d: float(d["price"])),
    "coinbase": ("https://api.coinbase.com/v2/prices/SOL-USD/spot", lambda d: float(d["data"]["amount"])),
    "coingecko": ("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
                  lambda d: float(d["solana"]["usd"])),
}


class SolPrice:
    def __init__(self, db: Database, fallback: float = 150.0):
        self.db = db
        self.value = fallback
        self.sources_ok: list[str] = []

    async def refresh(self, http: aiohttp.ClientSession) -> None:
        vals, ok = [], []
        for name, (url, parse) in SOURCES.items():
            try:
                async with http.get(url) as r:
                    v = parse(await r.json(content_type=None))
                if v > 0:
                    vals.append(v)
                    ok.append(name)
            except Exception:  # noqa: BLE001
                continue
        if vals:
            self.value = median(vals)
            self.sources_ok = ok
            await self.db.kv_set("sol_usd", {"price": self.value, "sources": ok})
        elif not self.sources_ok:
            cached = await self.db.kv_get("sol_usd")
            if cached:
                self.value = float(cached["price"])

    async def loop(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as http:
            while True:
                await self.refresh(http)
                await asyncio.sleep(30)


async def measured_fee_bps(db: Database) -> float | None:
    """Медиана фактических комиссий кривой по реальным сделкам за последний час."""
    v = await db.fetchval(
        """SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY fee_sol / sol_amount)
           FROM trades WHERE fee_sol IS NOT NULL AND sol_amount > 0.01 AND pool = 'pump'
             AND ts > now() - interval '1 hour'""")
    return float(v) * 10_000 if v else None
