"""Блок B5–B6: собственная база кошельков и кандидаты в смарт-мани из лучших трейдеров монет.

Роли: dev (создавал монеты), trader (закрытые сделки), sniper (большинство входов в первом блоке),
bundler (покупки в бандлах 4+ транзакций в блоке), kol (внешние метки), exchange / service
(биржи и служебные адреса — из блока A). Пересчёт раз в сутки.
"""
from __future__ import annotations

import logging
from typing import Any

from ..db.pool import Database
from .copytrade import compute_trader, linked_to_dev, qualifies, save_trader
from .curve import Costs
from .insights import WALLET_STATS_SQL

log = logging.getLogger(__name__)


async def refresh_wallet_stats(db: Database, min_mints: int = 3) -> int:
    """PnL 7/30 д, винрейт, число сделок, среднее удержание — по своим сделкам за 30 дней."""
    rows = await db.fetch(WALLET_STATS_SQL, min_mints)
    await db.p.executemany(
        """INSERT INTO wallets(address, pnl_7d, pnl_30d, winrate, trades_30d, avg_hold_s, stats_at)
           VALUES ($1,$2,$3,$4,$5,$6, now())
           ON CONFLICT (address) DO UPDATE SET pnl_7d=EXCLUDED.pnl_7d, pnl_30d=EXCLUDED.pnl_30d,
               winrate=EXCLUDED.winrate, trades_30d=EXCLUDED.trades_30d, avg_hold_s=EXCLUDED.avg_hold_s,
               stats_at=now()""",
        [(r["trader"], r["pnl_7d"], r["pnl_30d"], r["winrate"], int(r["trades_30d"] or 0), r["avg_hold_s"])
         for r in rows])
    await db.execute("UPDATE wallets w SET copyability = t.copy_30d FROM traders t WHERE t.address = w.address")
    return len(rows)


async def refresh_roles(db: Database) -> dict[str, int]:
    """Роли кошельков. Биржи и сервисы не перезаписываются."""
    keep = "role IS NULL OR role NOT IN ('exchange', 'service')"
    counts: dict[str, int] = {}
    q = {
        "kol": "UPDATE wallets SET role='kol', role_reason='метка KOL', role_source=COALESCE(tags_source,'gmgn') "
               "WHERE (" + keep + ") AND tags::text ILIKE ANY (ARRAY['%kol%', '%renowned%'])",
        "dev": "UPDATE wallets w SET role='dev', role_reason=x.n || ' монет за 30 дней', role_source='onchain' "
               "FROM (SELECT deployer a, count(*) n FROM tokens WHERE created_at > now() - interval '30 days' "
               "GROUP BY 1) x WHERE w.address = x.a "
               "AND (w.role IS NULL OR w.role NOT IN ('exchange', 'service', 'kol'))",
        "sniper": "UPDATE wallets w SET role='sniper', role_reason='входы в первом блоке: ' || x.n, "
                  "role_source='onchain' FROM (SELECT t.trader a, count(DISTINCT t.mint) n FROM trades t "
                  "JOIN tokens k ON k.mint = t.mint WHERE t.is_buy AND t.slot = k.created_slot "
                  "AND t.ts > now() - interval '7 days' GROUP BY 1 HAVING count(DISTINCT t.mint) >= 10) x "
                  "WHERE w.address = x.a AND (w.role IS NULL OR w.role IN ('trader', 'bundler'))",
        "bundler": "UPDATE wallets w SET role='bundler', role_reason='бандлы: ' || x.n || ' монет', "
                   "role_source='onchain' FROM (SELECT t.trader a, count(DISTINCT t.mint) n FROM trades t "
                   "JOIN (SELECT mint, slot FROM trades WHERE is_buy AND slot IS NOT NULL "
                   "AND ts > now() - interval '7 days' GROUP BY 1, 2 HAVING count(DISTINCT signature) >= 4) b "
                   "ON b.mint = t.mint AND b.slot = t.slot WHERE t.is_buy GROUP BY 1 "
                   "HAVING count(DISTINCT t.mint) >= 3) x WHERE w.address = x.a AND w.role IS NULL",
        "trader": "UPDATE wallets SET role='trader', role_reason='закрытые сделки', role_source='onchain' "
                  "WHERE role IS NULL AND trades_30d >= 10",
    }
    for role, sql in q.items():
        try:
            res = await db.execute(sql)
            counts[role] = int(res.split()[-1]) if res.startswith("UPDATE") else 0
        except Exception:  # noqa: BLE001
            log.exception("role %s", role)
    return counts


async def top_trader_candidates(db: Database, cfg: dict[str, Any], costs: Costs, min_coins: int = 3) -> int:
    """Трейдер из топ-10 по PnL на 3+ разных монетах за 7 дней → проверка копируемости с задержкой 5 с."""
    rows = await db.fetch(
        """SELECT trader, count(DISTINCT mint) n FROM coin_top_traders WHERE created_at > now() - interval '7 days'
           GROUP BY 1 HAVING count(DISTINCT mint) >= $1 ORDER BY 2 DESC LIMIT 100""", min_coins)
    added = 0
    eff = {**cfg, "trader_min_trades_30d": min(int(cfg.get("trader_min_trades_30d", 30)), 5)}
    for r in rows:
        try:
            s7, s30, _ = await compute_trader(db, r["trader"], eff, costs)
            linked = await linked_to_dev(db, r["trader"])
            ok, _fails = qualifies(s7, s30, linked, eff)
            await save_trader(db, r["trader"], s7, s30, ok, linked)
            if ok:
                await db.execute("UPDATE traders SET label = COALESCE(label, $2) WHERE address=$1", r["trader"],
                                 f"топ на {r['n']} монетах")
                added += 1
        except Exception:  # noqa: BLE001
            log.exception("candidate %s", r["trader"])
    return added
