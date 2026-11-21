"""Кошелёк — через все способы выхода (21.11, просьба владельца).

Раньше каждая проверка смотрела кошелёк одним способом: разбор при импорте — «купил через 5 с после него, продал через
5 с после него», бумага H — по правилу G, тень — все правила, но только у тех, за кем бот уже торгует. Теперь — его
последние 40 сделок за 30 дней (своя база) прогоняются через все наши выходы сразу, на настоящем пути цены монеты
(сделки своего потока), с задержкой входа 5 с и комиссиями — как бумага (та же функция decide_exit):

  F — за ним: продать, когда продаёт он (это и есть «копируемость»);
  A — всё на +25%, без стопа, до 30 мин;   B — 70% на +25%, остаток трейлинг 15%, до 30 мин;
  G — +25%, стоп −8%, 5 мин без +10% — выход, до 30 мин;   R — трейлинг 35% после +30%, стоп −50%, до 6 ч;
  W — продать через его обычное время удержания − 2 с (раньше него).
У A / B / G / R / W — ещё и выход, когда он продал (как у бумаги и реального бота).

Лучший выход — с наибольшим средним результатом среди устойчивых: 8+ сделок и t ≥ 1.645 (плюс не случайность).
Если у кошелька из своего списка лучший выход в устойчивом плюсе — бот сам включает «👻 тень за ним» (реальный бот
считает его сделки на настоящих транзакциях, без денег); перестал — снимает ту тень, что включил сам. Бой — только
вручную, кнопкой в карточке. Пересчёт — раз в 6 ч (разведка) и сразу при добавлении / импорте.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any

from .copytrade import MIN_COPY_T, compute_trader, t_stat
from .curve import trade_net_profit
from .paper import SCHEMES, Position, buy_fill, decide_exit, sell_fill

log = logging.getLogger(__name__)

RULES = ("F", "A", "B", "G", "R", "W")
RULE_NAMES = {"F": "за ним", "A": "A", "B": "B", "G": "G", "R": "R", "W": "W"}
RULE_TEXT = {"F": "продать, когда продаёт он", "A": "всё на +25%, без стопа", "B": "70% на +25%, остаток трейлинг 15%",
             "G": "+25%, стоп −8%, 5 мин без +10% — выход", "R": "трейлинг 35% после +30%, стоп −50%",
             "W": "продать раньше него: его время − 2 с"}
LAST_TRIPS = 40
HORIZON_S = 6 * 3600
MIN_N = 8
FOLLOW_KEY = "real:follow"
AUTO_KEY = "real:follow_auto"          # кого «тень за ним» включил сам бот (снимает только их)


def scheme(rule: str, hold_s: float, delay: float) -> dict[str, Any] | None:
    if rule == "W":
        if hold_s <= 0 or hold_s > 1800:
            return None
        return {"stop": None, "tp_net": None, "tp_share": 0.0, "trail": None, "target": False, "time_stop_s": None,
                "time_stop_gain": None, "max_hold_s": max(1.0, hold_s - 2.0 - delay)}
    base = dict(SCHEMES[rule])
    if rule == "R":
        base["max_hold_s"] = HORIZON_S
    return base


def simulate(path: list[tuple[float, float]], entry_at: float, entry_mc: float, trader_sell_at: float | None,
             sc: dict[str, Any], pos_sol: float, fee_bps: float, prio: float) -> float | None:
    """Результат копии по правилу sc на пути цены (ts, MC) после входа; None — пути нет."""
    if entry_mc <= 0:
        return None
    f = buy_fill(entry_mc, pos_sol, fee_bps, prio)
    p = Position(id=0, portfolio="X", mint="", opened_ts=entry_at, entry_mc=entry_mc, cost_sol=pos_sol,
                 tokens=f.tokens, tokens_initial=f.tokens, target_mc=None, max_mc=entry_mc)
    last = entry_mc
    for ts, mc in path:
        if ts <= entry_at or mc <= 0:
            continue
        last = mc
        reason = "trader_sell" if trader_sell_at is not None and ts >= trader_sell_at else None
        d = decide_exit(p, mc, ts, sc, fee_bps, prio, emergency=reason)
        if d is None:
            continue
        share = float(d[1])
        tokens = p.tokens * share if share < 1 else p.tokens
        p.proceeds_sol += sell_fill(mc, tokens, fee_bps, prio).sol
        p.tokens -= tokens
        if share < 1 and p.tokens > 0:
            p.tp_done = True
            continue
        p.tokens = 0
        break
    if p.tokens > 0:                     # путь кончился раньше выхода — по последней цене
        p.proceeds_sol += sell_fill(last, p.tokens, fee_bps, prio).sol
    return p.proceeds_sol / p.cost_sol - 1


async def evaluate(db: Any, address: str, cfg: dict[str, Any], costs: Any) -> dict[str, Any]:
    """Последние сделки кошелька — через все выходы. {"rules": {правило: {n, w, avg, sum, t}}, "best", "hold", "n"}."""
    _s7, s30, trips = await compute_trader(db, address, cfg, costs)
    trips = sorted(trips, key=lambda t: t.entry_ts)[-LAST_TRIPS:]
    delay = float(cfg.get("copy_delay_s", 5))
    pos = float(cfg.get("position_sol", 0.5))
    hold = float(s30.median_hold_s or 0)
    out: dict[str, Any] = {"rules": {}, "best": None, "hold": hold, "n": len(trips)}
    if not trips:
        return out
    mints = list({t.mint for t in trips})
    t0 = min(t.entry_ts for t in trips)
    t1 = max(max(t.exit_ts, t.entry_ts) for t in trips) + HORIZON_S
    rows = await db.fetch(
        """SELECT mint, extract(epoch FROM ts) AS t, mc_sol FROM trades
           WHERE mint = ANY($1::text[]) AND ts BETWEEN to_timestamp($2) AND to_timestamp($3) AND mc_sol > 0
           ORDER BY mint, ts""", mints, t0, t1)
    paths: dict[str, list[tuple[float, float]]] = {}
    for r in rows:
        paths.setdefault(r["mint"], []).append((float(r["t"]), float(r["mc_sol"])))
    res: dict[str, list[float]] = {k: [] for k in RULES}
    for t in trips:
        entry_at = t.entry_ts + delay
        entry_mc = float(t.copy_entry_mc or t.entry_mc or 0)
        sell_at = (t.sells[0][0] + delay) if t.sells else None
        if t.copy_exit_mc:
            res["F"].append(trade_net_profit(entry_mc, float(t.copy_exit_mc), pos, costs))
        path = [x for x in paths.get(t.mint, []) if entry_at < x[0] <= entry_at + HORIZON_S]
        if not path:
            continue
        for rule in RULES[1:]:
            sc = scheme(rule, hold, delay)
            if sc is None:
                continue
            v = simulate(path, entry_at, entry_mc, sell_at, sc, pos, costs.fee_bps, costs.priority_sol)
            if v is not None:
                res[rule].append(v)
    for rule, vals in res.items():
        if vals:
            out["rules"][rule] = {"n": len(vals), "w": sum(1 for v in vals if v > 0), "avg": statistics.fmean(vals),
                                  "sum": sum(vals), "t": t_stat(vals)}
    out["best"] = best_rule(out["rules"])
    return out


def best_rule(rules: dict[str, dict[str, Any]], min_n: int = MIN_N) -> str | None:
    """Лучший выход среди устойчивых (min_n+ сделок, t ≥ 1.645, в плюсе); нет таких — None."""
    ok = [(r["avg"], k) for k, r in rules.items() if r["n"] >= min_n and r["avg"] > 0 and r["t"] >= MIN_COPY_T]
    return max(ok)[1] if ok else None


def top_line(rules: dict[str, dict[str, Any]], best: str | None, limit: int = 3) -> str:
    """«лучший выход: за ним +21% (13 сд, t=2.3) · R +15% · A +3%» или «все выходы в минусе: …»."""
    if not rules:
        return "по выходам: сделок с путём цены нет"
    ranked = sorted(rules.items(), key=lambda kv: -kv[1]["avg"])
    parts = [f"{RULE_NAMES[k]} {r['avg']:+.0%}" for k, r in ranked[:limit]]
    if best:
        b = rules[best]
        return (f"лучший выход: {RULE_NAMES[best]} {b['avg']:+.0%} ({b['n']} сд, t={b['t']:.1f})"
                + (" · " + " · ".join(p for p in parts if not p.startswith(RULE_NAMES[best] + " ")) if len(parts) > 1
                   else ""))
    head = "все выходы в минусе" if ranked[0][1]["avg"] <= 0 else "плюс неустойчивый"
    return f"{head}: " + " · ".join(parts)


async def save(db: Any, address: str, res: dict[str, Any]) -> None:
    await db.execute("UPDATE traders SET rules_30d=$2, best_rule=$3 WHERE address=$1", address, res["rules"],
                     res["best"])


async def autofollow(db: Any, results: dict[str, dict[str, Any]], own: set[str]) -> list[str]:
    """Свои кошельки с лучшим выходом в устойчивом плюсе — «👻 тень за ним» (сам); перестали — снять свою тень.
    Бой не трогается: включённое вручную не меняется."""
    follow = dict(await db.kv_get(FOLLOW_KEY) or {})
    auto = set(await db.kv_get(AUTO_KEY) or [])
    events: list[str] = []
    for addr, res in results.items():
        name = f"{addr[:4]}…{addr[-4:]}"
        if res.get("best") and addr in own and addr not in follow:
            follow[addr] = "shadow"
            auto.add(addr)
            events.append(f"👻 {name}: тень за ним включена — {top_line(res['rules'], res['best'])}")
        elif not res.get("best") and addr in auto and follow.get(addr) == "shadow":
            follow.pop(addr, None)
            auto.discard(addr)
            events.append(f"⚪ {name}: тень за ним снята — {top_line(res['rules'], None)}")
    await db.kv_set(FOLLOW_KEY, follow)
    await db.kv_set(AUTO_KEY, sorted(auto))
    return events


async def refresh(db: Any, cfg: dict[str, Any], costs: Any, limit: int = 300) -> tuple[int, list[str]]:
    """Все кошельки своего списка, smart-money и испытания — через все выходы; авто-тень за лучшими."""
    own = {r["address"] for r in await db.fetch("SELECT address FROM watchlist WHERE kind='trader'")}
    addrs = list(own | {r["address"] for r in await db.fetch(
        """SELECT address FROM traders WHERE active
           UNION SELECT address FROM scout WHERE status = 'probation'""")})[:limit]
    results: dict[str, dict[str, Any]] = {}
    for a in addrs:
        try:
            res = await evaluate(db, a, cfg, costs)
        except Exception:  # noqa: BLE001
            log.exception("выходы кошелька %s", a)
            continue
        if res["n"]:
            await save(db, a, res)              # строка в traders есть: импорт / добавление / пересчёт трейдеров
            results[a] = res
    events = await autofollow(db, results, own)
    return len(results), events
