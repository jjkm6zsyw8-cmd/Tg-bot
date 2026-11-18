"""Поиск кошельков по ВСЕМУ PumpSwap через Dune (ключ DUNE_API_KEY, платный тариф Dune; раз в сутки).

Зачем: 7BNax торговал только на PumpSwap монетами старше часов, а своя база бота видит PumpSwap только по монетам,
за которыми уже следит. У Dune есть все сделки (таблица dex_solana.trades). GMGN просит $100 на балансе за ключ.
18.11 проверено на настоящем API: на бесплатном плане 0 кредитов в месяц — запросы только на платном тарифе.

Что считается (то же, что делает боевой бот, — копия по правилу A): за последние сутки для каждого кошелька, который
купил 10+ разных монет на PumpSwap по 0.3–5 SOL, по каждой его первой покупке монеты:
  вход — первая сделка по монете через 2+ слота после него (не дальше 10 с);
  дальше 5 минут: цена дошла до +25% чистыми раньше, чем он продал, — тейк; иначе выход по его продаже или по цене
  через 5 минут (комиссии 3%).
Кошелёк с тейком в 60%+ покупок и в среднем +10%+ на копию — в разведку, на испытание по правилу бота (портфель S).

Первый запуск проверяет таблицу: есть ли нужные колонки и площадка «pumpswap»; если нет — пишет, какие есть
(таблицу и площадку можно задать: DUNE_SCOUT_TABLE, DUNE_SCOUT_PROJECT).
Вручную: python -m scanner dune-scout [--queue].
"""
from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp

from ..alerts.format import esc
from ..history.dune import Dune, DuneError

log = logging.getLogger(__name__)

SOL_MINT = "So11111111111111111111111111111111111111112"
SOURCE = "Dune: весь PumpSwap"
DEFAULTS: dict[str, Any] = {
    "ds_every_h": 24.0, "ds_hours": 24, "ds_min_sol": 0.3, "ds_max_sol": 5.0, "ds_min_coins": 10,
    "ds_max_coins": 300, "ds_min_hit": 0.60, "ds_min_avg": 0.10, "ds_tp": 0.25, "ds_fee": 0.03, "ds_limit": 60,
}
NEED = {"block_time", "block_slot", "project", "trader_id", "token_bought_mint_address", "token_sold_mint_address",
        "token_bought_amount", "token_sold_amount"}
NAME = re.compile(r"^[a-z0-9_.]+$")


def cfg_get(cfg: dict[str, Any], key: str) -> Any:
    v = cfg.get(key)
    return DEFAULTS[key] if v is None else v


def table_name() -> str:
    t = os.environ.get("DUNE_SCOUT_TABLE", "dex_solana.trades").strip().lower()
    return t if NAME.match(t) else "dex_solana.trades"


def project_name() -> str:
    p = os.environ.get("DUNE_SCOUT_PROJECT", "pumpswap").strip().lower()
    return p if NAME.match(p) else "pumpswap"


def build_sql(cfg: dict[str, Any], table: str, project: str, has_month: bool) -> str:
    """Копия по правилу A на всех сделках площадки за сутки — итог по кошельку (Trino, Dune)."""
    hours = int(cfg_get(cfg, "ds_hours"))
    tp, fee = float(cfg_get(cfg, "ds_tp")), float(cfg_get(cfg, "ds_fee"))
    ratio = (1 + tp) / (1 - fee)
    month = (f"AND block_month >= date_trunc('month', now() - interval '{hours}' hour) " if has_month else "")
    return f"""
WITH t AS (
  SELECT block_time AS ts, block_slot AS slot, trader_id AS trader,
         token_sold_mint_address = '{SOL_MINT}' AS is_buy,
         CASE WHEN token_sold_mint_address = '{SOL_MINT}' THEN token_bought_mint_address
              ELSE token_sold_mint_address END AS mint,
         CASE WHEN token_sold_mint_address = '{SOL_MINT}' THEN token_sold_amount / token_bought_amount
              ELSE token_bought_amount / token_sold_amount END AS price,
         CASE WHEN token_sold_mint_address = '{SOL_MINT}' THEN token_sold_amount ELSE token_bought_amount END AS sol
  FROM {table}
  WHERE block_time > now() - interval '{hours}' hour {month}AND project = '{project}'
    AND (token_sold_mint_address = '{SOL_MINT}' OR token_bought_mint_address = '{SOL_MINT}')
    AND token_bought_amount > 0 AND token_sold_amount > 0
),
b AS (
  SELECT trader, mint, min(ts) AS ts, min_by(slot, ts) AS slot FROM t
  WHERE is_buy AND sol BETWEEN {float(cfg_get(cfg, "ds_min_sol"))} AND {float(cfg_get(cfg, "ds_max_sol"))}
    AND ts < now() - interval '6' minute
  GROUP BY 1, 2
),
act AS (SELECT trader FROM b GROUP BY 1
        HAVING count(*) BETWEEN {int(cfg_get(cfg, "ds_min_coins"))} AND {int(cfg_get(cfg, "ds_max_coins"))}),
bb AS (SELECT b.* FROM b JOIN act ON act.trader = b.trader),
e AS (
  SELECT bb.trader, bb.mint, bb.ts, min_by(t.price, t.slot) AS entry, min(t.ts) AS entry_ts
  FROM bb JOIN t ON t.mint = bb.mint AND t.slot >= bb.slot + 2 AND t.ts <= bb.ts + interval '10' second
  GROUP BY 1, 2, 3
),
w AS (
  SELECT e.trader, e.mint, e.entry, e.ts,
         min(t.ts) FILTER (WHERE t.price >= e.entry * {ratio:.6f}) AS hit_ts,
         min(t.ts) FILTER (WHERE t.trader = e.trader AND NOT t.is_buy) AS sell_ts,
         min_by(t.price, t.ts) FILTER (WHERE t.trader = e.trader AND NOT t.is_buy) AS sell_price,
         max_by(t.price, t.ts) AS last_price
  FROM e JOIN t ON t.mint = e.mint AND t.ts > e.entry_ts AND t.ts <= e.entry_ts + interval '300' second
  WHERE e.entry > 0
  GROUP BY 1, 2, 3, 4
),
r AS (
  SELECT trader,
         CASE WHEN hit_ts IS NOT NULL AND (sell_ts IS NULL OR hit_ts <= sell_ts + interval '1' second) THEN 1 ELSE 0
         END AS hit,
         CASE WHEN hit_ts IS NOT NULL AND (sell_ts IS NULL OR hit_ts <= sell_ts + interval '1' second) THEN {tp}
              WHEN sell_ts IS NOT NULL THEN greatest(-1.0, sell_price / entry * {1 - fee:.4f} - 1)
              ELSE greatest(-1.0, last_price / entry * {1 - fee:.4f} - 1) END AS out,
         CASE WHEN sell_ts IS NOT NULL AND date_diff('second', ts, sell_ts) < 20 THEN 1 ELSE 0 END AS quick,
         date_diff('second', ts, sell_ts) AS hold_s
  FROM w
)
SELECT trader, count(*) AS n, sum(hit) AS hits, avg(out) AS avg_out, avg(quick) AS quick,
       approx_percentile(hold_s, 0.5) AS hold_s
FROM r GROUP BY 1
HAVING count(*) >= {int(cfg_get(cfg, "ds_min_coins"))}
ORDER BY avg(out) DESC
LIMIT {int(cfg_get(cfg, "ds_limit"))}
"""


def good(r: dict[str, Any], cfg: dict[str, Any]) -> bool:
    n = int(r.get("n") or 0)
    return (n >= int(cfg_get(cfg, "ds_min_coins")) and int(r.get("hits") or 0) / n >= float(cfg_get(cfg, "ds_min_hit"))
            and float(r.get("avg_out") or 0) >= float(cfg_get(cfg, "ds_min_avg"))
            and float(r.get("quick") or 0) <= 0.3)


def describe(r: dict[str, Any]) -> str:
    n = int(r.get("n") or 0)
    hold = r.get("hold_s")
    return (f"копия как у бота за сутки на PumpSwap: {n} покупок, тейк {int(r.get('hits') or 0) / max(1, n):.0%}, "
            f"в среднем {float(r.get('avg_out') or 0):+.0%}" + (f", держит ~{float(hold):.0f} с" if hold else ""))


class DuneScout:
    def __init__(self, engine: Any, scout: Any):
        self.e = engine
        self.db = engine.db
        self.scout = scout

    @property
    def key(self) -> str:
        return os.environ.get("DUNE_API_KEY", "").strip()

    @property
    def cfg(self) -> dict[str, Any]:
        return getattr(self.e, "cfg", {}) or {}

    async def due(self) -> bool:
        last = await self.db.kv_get("dunescout:last") or {}
        try:
            at = datetime.fromisoformat(last["at"]).timestamp()
        except Exception:  # noqa: BLE001
            return True
        return time.time() - at >= float(cfg_get(self.cfg, "ds_every_h")) * 3600

    async def query(self, http: aiohttp.ClientSession, dune: Any = None) -> list[dict[str, Any]]:
        dune = dune or Dune(self.key)
        table, project = table_name(), project_name()
        sample = await dune.run_sql(http, f"SELECT * FROM {table} WHERE block_time > now() - interval '15' minute "
                                          f"AND project = '{project}' LIMIT 1")
        if not sample:
            projects = await dune.run_sql(http, f"SELECT project, count(*) AS n FROM {table} "
                                                f"WHERE block_time > now() - interval '15' minute GROUP BY 1 "
                                                f"ORDER BY 2 DESC LIMIT 25")
            raise DuneError(f"в {table} нет сделок площадки «{project}» за 15 минут; есть: "
                            + ", ".join(str(p.get("project")) for p in projects)
                            + ". Задайте DUNE_SCOUT_PROJECT.")
        cols = {k.lower() for k in sample[0]}
        missing = NEED - cols
        if missing:
            raise DuneError(f"в {table} нет колонок {sorted(missing)}. Задайте DUNE_SCOUT_TABLE.")
        return await dune.run_sql(http, build_sql(self.cfg, table, project, "block_month" in cols))

    async def run(self, force: bool = False, dune: Any = None, queue: bool = True) -> list[str]:
        if not self.key and dune is None:
            return []
        if not force and not await self.due():
            return []
        t0 = time.monotonic()
        last: dict[str, Any] = {"at": datetime.now(timezone.utc).isoformat()}
        events: list[str] = []
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=900)) as http:
                rows = await self.query(http, dune)
            fit = [r for r in rows if good(r, self.cfg)]
            last.update(checked=len(rows), found=len(fit),
                        top=[{k: r.get(k) for k in ("trader", "n", "hits", "avg_out", "quick", "hold_s")}
                             for r in rows[:10]])
            if queue:
                known = await self.scout._known()
                for r in fit:
                    w = str(r.get("trader") or "")
                    if not w or w in known:
                        continue
                    if await self.scout._queue(w, SOURCE, describe(r), 5000 + float(r.get("avg_out") or 0) * 1000,
                                               None):
                        events.append(f"🌐 {esc(SOURCE)}: <code>{w}</code> — {esc(describe(r))}")
        except Exception as exc:
            last["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
            raise
        finally:
            last["took_s"] = round(time.monotonic() - t0, 1)
            await self.db.kv_set("dunescout:last", last)
        return events


async def report_lines(db: Any) -> list[str]:
    last = await db.kv_get("dunescout:last") or {}
    if not last and not os.environ.get("DUNE_API_KEY"):
        return ["\n<b>🌐 Весь PumpSwap (Dune)</b>: выключен — нет ключа DUNE_API_KEY (запросы через API у Dune "
                "платные: на бесплатном плане 0 кредитов)."]
    if not last:
        return ["\n<b>🌐 Весь PumpSwap (Dune)</b>: ещё не считался (раз в сутки)."]
    lines = [f"\n<b>🌐 Весь PumpSwap (Dune)</b>: {esc(str(last.get('at', ''))[:16].replace('T', ' '))} UTC, "
             f"{last.get('took_s', '?')} с · кошельков в выборке {last.get('checked', 0)}, подходят "
             f"{last.get('found', 0)}"]
    if last.get("error"):
        lines.append(f"⚠️ {esc(last['error'])}")
    return lines
