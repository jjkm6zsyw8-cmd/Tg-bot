"""Самопроверка бумаги по рынку: цена входа и каждой продажи сверяется с минутными свечами GeckoTerminal.

Зачем: бумага не видит всех сделок монеты, и её цена иногда отставала от рынка на десятки процентов —
8–9.11 это давало входы ниже рынка (плюс завышался вдвое) и стопы по цене, которой на рынке не было.
Теперь бот сам ловит такие сделки: цена вне свечи своей минуты (с допуском 15%) — позиция помечается ⚠
(suspect) и не идёт в итоги, в отчёте /paper видно, сколько сделок проверено и сколько отброшено.

GeckoTerminal бесплатно — ~30 запросов в минуту на всех; «Тренды» берут до 12, самопроверка — до 10.
На монету 2–3 запроса (пулы + минутные свечи двух пулов), свечи кешируются на 10 минут.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

import aiohttp

from .trendcheck import RateBudget

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger(__name__)

GECKO = "https://api.geckoterminal.com/api/v2/networks/solana"
TOLERANCE = 1.15          # цена может выйти за свечу своей минуты не больше чем на 15% (курс SOL, разные пулы)
CHECK_EVERY_S = 60.0
CANDLE_TTL_S = 600.0


def within(price: float, lo: float, hi: float, tol: float = TOLERANCE) -> bool:
    return lo / tol <= price <= hi * tol


def minute_range(candles: list[list[float]], ts: float) -> tuple[float, float] | None:
    """(минимум, максимум) свечей минуты, в которую попадает ts; соседняя минута — если своей нет."""
    for shift in (0, -60, 60):
        sel = [c for c in candles if c[0] <= ts + shift < c[0] + 60]
        if sel:
            return min(c[3] for c in sel), max(c[2] for c in sel)
    return None


class PriceCheck:
    def __init__(self, engine: "Engine"):
        self.e = engine
        self.db = engine.db
        self.budget = RateBudget(10, 60)
        self.pause_until = 0.0
        self.candles: dict[str, tuple[float, list[list[float]]]] = {}
        self.stats = {"checked": 0, "bad": 0, "nodata": 0, "errors": 0}

    async def _get(self, http: aiohttp.ClientSession, url: str) -> Any:
        if time.time() < self.pause_until or not self.budget.take():
            raise TimeoutError("бюджет GeckoTerminal")
        async with http.get(url, headers={"Accept": "application/json"}) as r:
            if r.status == 429:
                self.pause_until = time.time() + 60
                raise TimeoutError("GeckoTerminal 429")
            if r.status != 200:
                return None
            return await r.json(content_type=None)

    async def _candles(self, http: aiohttp.ClientSession, mint: str) -> list[list[float]]:
        hit = self.candles.get(mint)
        if hit and time.time() - hit[0] < CANDLE_TTL_S:
            return hit[1]
        d = await self._get(http, f"{GECKO}/tokens/{mint}/pools?page=1")
        out: list[list[float]] = []
        for pool in ((d or {}).get("data") or [])[:2]:
            addr = (pool.get("attributes") or {}).get("address")
            if not addr:
                continue
            dd = await self._get(http, f"{GECKO}/pools/{addr}/ohlcv/minute?aggregate=1&limit=1000")
            out += ((dd or {}).get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
        self.candles[mint] = (time.time(), out)
        if len(self.candles) > 500:
            cut = time.time() - CANDLE_TTL_S
            self.candles = {m: v for m, v in self.candles.items() if v[0] > cut}
        return out

    def verdict(self, candles: list[list[float]], entry: tuple[float, float], sells: list[tuple[float, float]],
                sol_usd: float) -> str:
        """ok | entry | exit | nodata. entry/sells — (unix-время, MC в SOL)."""
        if not candles:
            return "nodata"
        usd = lambda mc: mc * sol_usd / 1e9   # noqa: E731 — цена токена в $ (pump.fun: 1 млрд токенов)
        rng = minute_range(candles, entry[0])
        seen = False
        if rng:
            seen = True
            if not within(usd(entry[1]), *rng):
                return "entry"
        for ts, mc in sells:
            rng = minute_range(candles, ts)
            if rng:
                seen = True
                if not within(usd(mc), *rng):
                    return "exit"
        return "ok" if seen else "nodata"

    async def run_once(self, http: aiohttp.ClientSession) -> int:
        rows = await self.db.fetch(
            """SELECT id, mint, opened_at, entry_mc FROM paper_positions
               WHERE closed_at IS NOT NULL AND exit_reason <> 'reset' AND price_check IS NULL
                 AND closed_at < now() - interval '90 seconds' AND closed_at > now() - interval '12 hours'
               ORDER BY closed_at LIMIT 60""")
        if not rows:
            return 0
        sells: dict[int, list[tuple[float, float]]] = {}
        for t in await self.db.fetch(
                "SELECT position_id, ts, mc FROM paper_trades WHERE side='sell' AND position_id = ANY($1::bigint[])",
                [r["id"] for r in rows]):
            sells.setdefault(t["position_id"], []).append((t["ts"].timestamp(), float(t["mc"])))
        sol_usd = self.e.price.value or 150
        done = 0
        for r in rows:
            try:
                candles = await self._candles(http, r["mint"])
            except (TimeoutError, aiohttp.ClientError, asyncio.TimeoutError):
                break                                  # бюджет кончился или GeckoTerminal занят — продолжим позже
            v = self.verdict(candles, (r["opened_at"].timestamp(), float(r["entry_mc"] or 0)),
                             sells.get(r["id"], []), sol_usd)
            bad = v in ("entry", "exit")
            await self.db.execute(
                "UPDATE paper_positions SET price_check=$2, suspect = suspect OR $3 WHERE id=$1", r["id"], v, bad)
            self.stats["checked"] += 1
            self.stats["bad"] += bad
            self.stats["nodata"] += v == "nodata"
            done += 1
        return done

    async def loop(self) -> None:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            while True:
                await asyncio.sleep(CHECK_EVERY_S)
                try:
                    await self.run_once(http)
                except Exception:  # noqa: BLE001 — самопроверка не должна ронять аналитику
                    self.stats["errors"] += 1
                    log.exception("самопроверка цен бумаги")
