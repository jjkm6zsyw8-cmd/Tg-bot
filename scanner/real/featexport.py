"""25.11 (владелец): признаки входа трендов (real_positions.extra.feat, features.py) — выгрузка CSV и сравнение
растущих и падающих сделок.

  python -m scanner trendfeat [--days N] [--kind trend|trendj] [--mode shadow|live]            — CSV в stdout
  python -m scanner trendfeat --compare [--days N] [--kind …] [--mode …]                       — медианы по группам
«Растущая» — итог сделки > 0, «падающая» — ≤ 0. Ещё есть пик после входа (max_up = max_mc / entry_mc − 1).
"""
from __future__ import annotations

import csv
import io
import statistics
from typing import Any

BASE_COLS = ("id", "kind", "mode", "symbol", "mint", "opened_at", "exit_reason", "pnl_pct", "pnl_sol", "max_up",
             "hold_min")
FEAT_COLS = ("dev_team_hold", "creator_hold", "insider_hold_rate", "top10", "sig_top10", "dev_status", "dev_n",
             "dev_hold", "dev_sold", "insiders_n", "insiders_hold", "insiders_sold", "bundlers_n", "bundlers_hold",
             "bundlers_sold", "bundler_volume", "snipers_n", "snipers_hold", "snipers_sold", "snipers_count",
             "fresh_rate", "holders", "pc_m5", "pc_m30", "pc_h1", "sig_pc_m5", "sig_pc_h1", "vol_m5_usd", "pool_sol",
             "vol_m5_to_pool", "sig_vol_m5_usd", "sig_liq_usd", "coin_age_h", "since_migr_h", "on_curve",
             "pair_age_h", "dex", "launchpad", "pool_exchange", "cluster_id", "sig_buyers", "sig_sellers", "src")
NUMERIC = [c for c in FEAT_COLS if c not in ("dev_status", "dex", "launchpad", "pool_exchange", "cluster_id", "src",
                                               "on_curve")]


async def load(db: Any, days: float | None = None, kind: str | None = None, mode: str | None = None
               ) -> list[dict[str, Any]]:
    rows = await db.fetch(
        """SELECT id, kind, mode, symbol, mint, opened_at, closed_at, exit_reason, pnl_pct, pnl_sol, entry_mc, max_mc,
                  extra->'feat' AS feat
           FROM real_positions WHERE kind IN ('trend', 'trendj') AND closed_at IS NOT NULL AND extra ? 'feat'
             AND ($1::float8 IS NULL OR opened_at > now() - make_interval(secs => $1 * 86400))
             AND ($2::text IS NULL OR kind = $2) AND ($3::text IS NULL OR mode = $3)
           ORDER BY id""", days, kind, mode)
    out = []
    for r in rows:
        f = dict(r["feat"] or {})
        x = {"id": r["id"], "kind": r["kind"], "mode": r["mode"], "symbol": r["symbol"], "mint": r["mint"],
             "opened_at": r["opened_at"].isoformat(timespec="seconds"), "exit_reason": r["exit_reason"],
             "pnl_pct": r["pnl_pct"], "pnl_sol": r["pnl_sol"],
             "max_up": (float(r["max_mc"]) / float(r["entry_mc"]) - 1) if r["max_mc"] and r["entry_mc"] else None,
             "hold_min": (r["closed_at"] - r["opened_at"]).total_seconds() / 60}
        for c in FEAT_COLS:
            v = f.get(c)
            x[c] = "+".join(v) if isinstance(v, list) else v
        out.append(x)
    return out


def to_csv(rows: list[dict[str, Any]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[*BASE_COLS, *FEAT_COLS], extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


def compare(rows: list[dict[str, Any]]) -> str:
    """Медианы признаков у растущих (итог > 0) и падающих сделок; заполненность — сколько сделок с этим полем."""
    up = [r for r in rows if (r["pnl_pct"] or 0) > 0]
    down = [r for r in rows if (r["pnl_pct"] or 0) <= 0]
    out = [f"Сделок с признаками: {len(rows)} · растущих (итог > 0): {len(up)} · падающих: {len(down)}",
           f"{'признак':<16}{'есть у':>7}{'медиана ↑':>12}{'медиана ↓':>12}"]

    def med(xs: list[dict[str, Any]], c: str) -> str:
        v = [float(r[c]) for r in xs if isinstance(r.get(c), (int, float)) and not isinstance(r.get(c), bool)]
        return f"{statistics.median(v):.3g}" if v else "—"
    for c in NUMERIC:
        have = sum(1 for r in rows if isinstance(r.get(c), (int, float)) and not isinstance(r.get(c), bool))
        if have:
            out.append(f"{c:<16}{have:>7}{med(up, c):>12}{med(down, c):>12}")
    for c in ("dev_status", "on_curve"):
        vals = sorted({str(r.get(c)) for r in rows})
        for v in vals:
            u = sum(1 for r in up if str(r.get(c)) == v)
            d = sum(1 for r in down if str(r.get(c)) == v)
            out.append(f"{c}={v}: растущих {u}, падающих {d}")
    if len(rows) < 200:
        out.append(f"Мало данных: {len(rows)} из 200 — разница медиан пока случайна.")
    return "\n".join(out)
