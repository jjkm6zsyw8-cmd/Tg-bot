"""Загрузка истории запусков за N дней через Dune API (блок A8).

    DUNE_API_KEY=... python -m scanner history --days 30 [--min-launches 2]

Почему Dune: через RPC Helius история Pump.fun за 30 дней — это ~480 млн транзакций (нереально по
кредитам), а Dune считает сводку по запускам у себя и отдаёт только итог: одна строка на монету.
Стоимость — по документации Dune: исполнение SQL расходует кредиты по вычислениям, выдача результата —
1 кредит за 1000 значений (бесплатно 2 500 кредитов в месяц). Чтобы уложиться, берём только девов
с min_launches+ запусками (история нужна именно им).

Названия декодированных таблиц Pump.fun в Dune официально не подтверждены, поэтому перед выгрузкой
загрузчик проверяет таблицы и колонки и останавливается с понятным сообщением, если они другие
(названия задаются переменными DUNE_CREATE_TABLE и DUNE_TRADE_TABLE).
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any

import aiohttp

from ..analytics.curve import Costs, trade_net_profit
from ..db.pool import Database

log = logging.getLogger(__name__)
API = "https://api.dune.com/api/v1"
CREATE_TABLE = os.environ.get("DUNE_CREATE_TABLE", "pumpdotfun_solana.pump_evt_createevent")
TRADE_TABLE = os.environ.get("DUNE_TRADE_TABLE", "pumpdotfun_solana.pump_evt_tradeevent")
NEED_CREATE = {"mint", "user", "creator", "evt_block_time"}
NEED_TRADE = {"mint", "user", "isbuy", "solamount", "tokenamount", "virtualsolreserves", "virtualtokenreserves",
              "evt_block_time"}

SUMMARY_SQL = """
WITH c AS (
  SELECT mint, "user" AS deployer, creator, evt_block_time AS created_at, symbol
  FROM {create_table}
  WHERE evt_block_time > now() - interval '{days}' day
),
rep AS (SELECT deployer FROM c GROUP BY 1 HAVING count(*) >= {min_launches}),
cc AS (SELECT * FROM c WHERE deployer IN (SELECT deployer FROM rep)),
t AS (
  SELECT e.mint, e."user" AS trader, e.isBuy AS is_buy, e.evt_block_time AS ts,
         CAST(e.virtualSolReserves AS double) / CAST(e.virtualTokenReserves AS double) * 1000 AS mc
  FROM {trade_table} e
  WHERE e.evt_block_time > now() - interval '{days}' day AND e.mint IN (SELECT mint FROM cc)
),
d AS (
  SELECT t.mint, min(t.ts) AS dump_ts FROM t JOIN cc ON cc.mint = t.mint
  WHERE t.trader = cc.deployer AND NOT t.is_buy GROUP BY 1
)
SELECT cc.mint, cc.deployer, cc.creator, cc.created_at, cc.symbol,
  count(t.mint) AS trades_count,
  max(t.mc) AS peak_mc,
  max_by(t.mc, t.ts) FILTER (WHERE t.ts <= cc.created_at + interval '2' second) AS initial_mc,
  max_by(t.mc, t.ts) FILTER (WHERE t.ts <= cc.created_at + interval '7' second) AS entry_mc,
  max_by(t.mc, t.ts) FILTER (WHERE t.ts <= cc.created_at + interval '30' second) AS mc_30s,
  max_by(t.mc, t.ts) FILTER (WHERE t.ts <= cc.created_at + interval '60' second) AS mc_60s,
  max_by(t.mc, t.ts) FILTER (WHERE t.ts <= cc.created_at + interval '120' second) AS mc_120s,
  max_by(t.mc, t.ts) FILTER (WHERE t.ts < d.dump_ts) AS dump_mc,
  date_diff('second', cc.created_at, min(d.dump_ts)) AS time_to_dump_s,
  max(t.mc) FILTER (WHERE t.ts > cc.created_at + interval '7' second
                    AND t.ts < COALESCE(d.dump_ts - interval '5' second, cc.created_at + interval '60' minute)) AS best_mc
FROM cc LEFT JOIN t ON t.mint = cc.mint LEFT JOIN d ON d.mint = cc.mint
GROUP BY 1, 2, 3, 4, 5
"""


class DuneError(RuntimeError):
    pass


class Dune:
    def __init__(self, key: str):
        self.key = key

    async def _req(self, http: aiohttp.ClientSession, method: str, path: str, **kw: Any) -> Any:
        async with http.request(method, API + path, headers={"X-DUNE-API-KEY": self.key}, **kw) as r:
            data = await r.json(content_type=None)
            if r.status >= 400:
                raise DuneError(f"{path}: HTTP {r.status} {str(data)[:300]}")
            return data

    async def run_sql(self, http: aiohttp.ClientSession, sql: str, page: int = 10_000) -> list[dict[str, Any]]:
        ex = await self._req(http, "POST", "/sql/execute", json={"sql": sql, "performance": "medium"})
        eid = ex.get("execution_id")
        if not eid:
            raise DuneError(f"нет execution_id: {ex}")
        while True:
            st = await self._req(http, "GET", f"/execution/{eid}/status")
            state = st.get("state", "")
            if state.endswith("COMPLETED"):
                break
            if state.endswith(("FAILED", "CANCELLED", "EXPIRED")):
                raise DuneError(f"запрос не выполнен: {st.get('error') or state}")
            await asyncio.sleep(5)
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            res = await self._req(http, "GET", f"/execution/{eid}/results", params={"limit": page, "offset": offset})
            chunk = ((res.get("result") or {}).get("rows")) or []
            rows.extend(chunk)
            if len(chunk) < page:
                return rows
            offset += page


async def check_schema(dune: Dune, http: aiohttp.ClientSession) -> None:
    for table, need in ((CREATE_TABLE, NEED_CREATE), (TRADE_TABLE, NEED_TRADE)):
        rows = await dune.run_sql(http, f"SELECT * FROM {table} LIMIT 1")
        cols = {k.lower() for k in (rows[0].keys() if rows else [])}
        missing = need - cols
        if missing:
            raise DuneError(f"в {table} нет колонок {sorted(missing)}; есть: {sorted(cols)[:40]}. "
                            "Укажите правильные таблицы в DUNE_CREATE_TABLE / DUNE_TRADE_TABLE.")


def _ts(v: Any) -> datetime | None:
    if not v:
        return None
    return datetime.fromisoformat(str(v).replace(" UTC", "+00:00").replace("Z", "+00:00").replace(" ", "T", 1))


async def load(db: Database, key: str, days: int = 30, min_launches: int = 2) -> dict[str, int]:
    dune = Dune(key)
    costs = Costs()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as http:
        await check_schema(dune, http)
        sql = SUMMARY_SQL.format(create_table=CREATE_TABLE, trade_table=TRADE_TABLE, days=int(days),
                                 min_launches=int(min_launches))
        rows = await dune.run_sql(http, sql)
    tok, lau = [], []
    for r in rows:
        created = _ts(r.get("created_at"))
        if not created or not r.get("mint"):
            continue
        init = float(r.get("initial_mc") or 27.96) or 27.96
        entry, best = r.get("entry_mc"), r.get("best_mc")
        ach = trade_net_profit(float(entry), float(best), 0.5, costs) if entry and best else None
        ttd = r.get("time_to_dump_s")
        tok.append((r["mint"], "dune", r["deployer"], r.get("creator") or r["deployer"], r.get("symbol") or "",
                    created, init, float(r.get("peak_mc") or init)))
        lau.append((r["mint"], r["deployer"], created, init, float(r.get("peak_mc") or init),
                    r.get("dump_mc"), max(0.0, float(ttd)) if ttd is not None else None, ach, entry,
                    r.get("mc_30s"), r.get("mc_60s"), r.get("mc_120s"), int(r.get("trades_count") or 0)))
    async with db.p.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """INSERT INTO tokens(mint, signature, deployer, current_owner, symbol, created_at, initial_mc_sol,
                       peak_mc_sol, source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'dune') ON CONFLICT (mint) DO NOTHING""", tok)
            await conn.executemany(
                """INSERT INTO launches(mint, deployer, created_at, initial_mc, peak_mc, dump_mc, time_to_dump_s,
                       achievable_profit, entry_mc, mc_30s, mc_60s, mc_120s, trades_count, features)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, '{"source": "dune"}'::jsonb)
                   ON CONFLICT (mint) DO NOTHING""", lau)
    return {"rows": len(rows), "tokens": len(tok)}


def now() -> datetime:
    return datetime.now(timezone.utc)
