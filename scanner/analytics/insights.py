"""Блок B: продажи по группам, окна входа, лучшие трейдеры монеты, статистика кошельков.

Всё считается по завершённому запуску (TokenState со всеми сделками) и пишется в launches.features
и coin_top_traders; профиль кластера агрегирует это в типичную последовательность продаж,
окно входа и разброс MC дампа.
"""
from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any

from .curve import Costs, trade_net_profit
from .state import TokenState

ENTRY_GRID_S = (3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 300)
GROUPS = ("cluster", "bundle", "sniper", "top_holder", "tracked")
GROUP_NAMES = {"cluster": "кластер дева", "bundle": "бандлы", "sniper": "снайперы", "top_holder": "топ-холдеры",
               "tracked": "отслеживаемые трейдеры"}


def _quantile(vals: list[float], q: float) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    pos = (len(s) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


# ---------------------------------------------------------------------- B1: продажи по группам
def classify(st: TokenState, tracked: set[str] | None = None) -> dict[str, set[str]]:
    peak_hold: dict[str, float] = defaultdict(float)
    bal: dict[str, float] = defaultdict(float)
    for t in st.trades:
        bal[t.trader] = t.new_balance if t.new_balance is not None else bal[t.trader] + (t.tokens if t.is_buy else -t.tokens)
        peak_hold[t.trader] = max(peak_hold[t.trader], bal[t.trader])
    cluster = st.cluster_wallets()
    top = {w for w, _ in sorted(peak_hold.items(), key=lambda x: -x[1])[:10]} - cluster
    return {"cluster": cluster, "bundle": st.bundle_wallets() - cluster, "sniper": st.sniper_wallets() - cluster,
            "top_holder": top, "tracked": (tracked or set()) - cluster}


def sell_sequences(st: TokenState, tracked: set[str] | None = None) -> dict[str, dict[str, Any]]:
    """По каждой группе: первая продажа, 50% и 90% проданного — время от запуска, MC, доля."""
    groups = classify(st, tracked)
    out: dict[str, dict[str, Any]] = {}
    for g, wallets in groups.items():
        if not wallets:
            continue
        bought = sum(t.tokens for t in st.trades if t.is_buy and t.trader in wallets)
        if bought <= 0:
            continue
        sold = 0.0
        seq: dict[str, Any] = {"wallets": len(wallets), "events": 0}
        for t in st.trades:
            if t.is_buy or t.trader not in wallets:
                continue
            sold += t.tokens
            seq["events"] += 1
            frac = min(1.0, sold / bought)
            point = {"t": round(t.ts - st.created_ts, 1), "mc": round(t.mc, 2), "frac": round(frac, 3)}
            if "first" not in seq:
                seq["first"] = point
            if frac >= 0.5 and "half" not in seq:
                seq["half"] = point
            if frac >= 0.9 and "full" not in seq:
                seq["full"] = point
        seq["sold_frac"] = round(min(1.0, sold / bought), 3)
        out[g] = seq
    return out


# ---------------------------------------------------------------------- B2: окна входа
def entry_curve(st: TokenState, target_mc: float | None, reaction_s: float = 5.0, exit_delay_s: float = 2.0,
                position_sol: float = 0.5, costs: Costs | None = None) -> dict[str, dict[str, float]]:
    """Чистый результат входа через t секунд после запуска (+ реакция), выход — цель или
    первая продажа кластера (+ задержка), иначе по таймауту 30 минут."""
    costs = costs or Costs()
    out: dict[str, dict[str, float]] = {}
    end = st.trades[-1].ts if st.trades else st.created_ts
    first_cluster_sell = st.first_cluster_sell_ts
    for t_off in ENTRY_GRID_S:
        t_entry = st.created_ts + t_off + reaction_s
        if t_entry > end:
            break
        if first_cluster_sell is not None and first_cluster_sell <= t_entry:
            break  # дев уже продаёт — входа нет
        entry_mc = st.mc_at(t_entry)
        exit_ts = min(end, st.created_ts + 1800)
        for tr in st.trades:
            if tr.ts <= t_entry:
                continue
            if first_cluster_sell is not None and tr.ts >= first_cluster_sell:
                exit_ts = first_cluster_sell + exit_delay_s
                break
            if target_mc and tr.mc >= target_mc:
                exit_ts = tr.ts + exit_delay_s
                break
        net = trade_net_profit(entry_mc, st.mc_at(exit_ts), position_sol, costs)
        out[str(t_off)] = {"mc": round(entry_mc, 2), "net": round(net, 4)}
    return out


def entry_window(curves: list[dict[str, dict[str, float]]], threshold: float) -> dict[str, Any] | None:
    """По похожим запускам: интервал времени (и MC), где медианный чистый результат выше порога."""
    good = []
    for t_off in ENTRY_GRID_S:
        nets = [c[str(t_off)]["net"] for c in curves if str(t_off) in c]
        mcs = [c[str(t_off)]["mc"] for c in curves if str(t_off) in c]
        if len(nets) < 3:
            continue
        m = median(nets)
        if m >= threshold:
            good.append((t_off, m, median(mcs), _quantile(mcs, 0.75)))
    if not good:
        return None
    return {"t_from": good[0][0], "t_to": good[-1][0], "mc_from": round(good[0][2], 2),
            "mc_to": round(max(g[3] or g[2] for g in good), 2), "median_net": round(max(g[1] for g in good), 3),
            "samples": len(curves)}


# ---------------------------------------------------------------------- B3: разброс MC дампа
def dump_spread(dump_mcs: list[float]) -> dict[str, float] | None:
    vals = [v for v in dump_mcs if v]
    if not vals:
        return None
    return {"p25": round(_quantile(vals, 0.25) or 0, 2), "median": round(median(vals), 2),
            "p75": round(_quantile(vals, 0.75) or 0, 2), "n": len(vals)}


def typical_sells(seqs: list[dict[str, Any]]) -> dict[str, Any]:
    """Типичная последовательность продаж кластера: медианы первой продажи, 50% и 90% проданного."""
    out: dict[str, Any] = {}
    for stage in ("first", "half", "full"):
        pts = [s[stage] for s in seqs if s and stage in s]
        if len(pts) >= 2:
            out[stage] = {"t": round(median(p["t"] for p in pts), 1), "mc": round(median(p["mc"] for p in pts), 2),
                          "n": len(pts)}
    return out


# ---------------------------------------------------------------------- B5: лучшие трейдеры монеты
def top_traders(st: TokenState, exclude: set[str], n: int = 10) -> list[dict[str, Any]]:
    """Топ по реализованному PnL (только закрытые позиции), без кластера, бандлов и снайперов."""
    per: dict[str, dict[str, Any]] = {}
    for t in st.trades:
        if t.trader in exclude:
            continue
        p = per.setdefault(t.trader, {"buy_sol": 0.0, "sell_sol": 0.0, "bought": 0.0, "sold": 0.0,
                                      "first_buy": None, "last_sell": None, "buy_mc": [], "sell_mc": []})
        if t.is_buy:
            p["buy_sol"] += t.sol
            p["bought"] += t.tokens
            p["buy_mc"].append((t.mc, t.tokens))
            p["first_buy"] = p["first_buy"] or t.ts
        else:
            p["sell_sol"] += t.sol
            p["sold"] += t.tokens
            p["sell_mc"].append((t.mc, t.tokens))
            p["last_sell"] = t.ts

    def wavg(xs: list[tuple[float, float]]) -> float | None:
        w = sum(b for _, b in xs)
        return sum(a * b for a, b in xs) / w if w else None

    rows = []
    for w, p in per.items():
        if p["bought"] <= 0 or p["sold"] < 0.9 * p["bought"] or not p["first_buy"] or not p["last_sell"]:
            continue
        rows.append({"trader": w, "pnl_sol": round(p["sell_sol"] - p["buy_sol"], 4),
                     "entry_mc": round(wavg(p["buy_mc"]) or 0, 2), "exit_mc": round(wavg(p["sell_mc"]) or 0, 2),
                     "hold_s": round(p["last_sell"] - p["first_buy"], 1),
                     "entry_age_s": round(p["first_buy"] - st.created_ts, 1)})
    rows.sort(key=lambda r: -r["pnl_sol"])
    return [r for r in rows if r["pnl_sol"] > 0][:n]


# ---------------------------------------------------------------------- B6: статистика кошельков (SQL)
WALLET_STATS_SQL = """
WITH pos AS (
    SELECT trader, mint,
           sum(sol_amount) FILTER (WHERE is_buy) buy_sol, sum(sol_amount) FILTER (WHERE NOT is_buy) sell_sol,
           sum(token_amount) FILTER (WHERE is_buy) bought, sum(token_amount) FILTER (WHERE NOT is_buy) sold,
           min(ts) FILTER (WHERE is_buy) first_buy, max(ts) FILTER (WHERE NOT is_buy) last_sell,
           count(*) n, max(ts) last_ts
    FROM trades WHERE ts > now() - interval '30 days' GROUP BY trader, mint
), closed AS (
    SELECT * FROM pos WHERE bought > 0 AND sold >= 0.9 * bought
)
SELECT p.trader,
       sum(p.n) trades_30d,
       count(*) FILTER (WHERE c.trader IS NOT NULL) closed,
       sum(c.sell_sol - c.buy_sol) pnl_30d,
       sum(c.sell_sol - c.buy_sol) FILTER (WHERE c.last_ts > now() - interval '7 days') pnl_7d,
       avg(CASE WHEN c.sell_sol > c.buy_sol THEN 1.0 ELSE 0.0 END) FILTER (WHERE c.trader IS NOT NULL) winrate,
       avg(extract(epoch FROM c.last_sell - c.first_buy)) avg_hold_s
FROM pos p LEFT JOIN closed c ON c.trader = p.trader AND c.mint = p.mint
GROUP BY p.trader HAVING count(*) >= $1
"""
