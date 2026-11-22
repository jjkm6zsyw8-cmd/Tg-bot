"""Кошелёк — через все способы выхода (21.11, просьба владельца).

Раньше каждая проверка смотрела кошелёк одним способом: разбор при импорте — «купил через 5 с после него, продал через
5 с после него», бумага H — по правилу G, тень — все правила, но только у тех, за кем бот уже торгует. Теперь — его
сделки за 30 дней (до 120, равномерно по времени) и отдельно за последние сутки (до 60) прогоняются через все наши
выходы сразу, на настоящем пути цены монеты
(сделки своего потока), со скоростью бота (вход через ~1 с) и комиссиями — как бумага (та же функция decide_exit):

  F — за ним: продать, когда продаёт он (это и есть «копируемость»);
  A — всё на +25%, без стопа, до 30 мин;   B — 70% на +25%, остаток трейлинг 15%, до 30 мин;
  G — +25%, стоп −8%, 5 мин без +10% — выход, до 30 мин;   R — трейлинг 35% после +30%, стоп −50%, до 6 ч;
  W — продать через его обычное время удержания − 2 с (раньше него).
У A / B / G / R / W — ещё и выход, когда он продал (как у бумаги и реального бота).

Лучший выход — с наибольшим средним результатом среди устойчивых: 8+ сделок и t ≥ 1.645 (плюс не случайность).
Если у кошелька из своего списка лучший выход в устойчивом плюсе — бот сам включает «👻 тень за ним» (реальный бот
считает его сделки на настоящих транзакциях, без денег); перестал — снимает ту тень, что включил сам. 22.11: то же —
для любого кошелька «Сейчас» (плюс устойчив за 30 дней и итог за сутки в плюсе, лучшие 15 по суткам), чтобы бот
сам находил тех, кто хорош именно сейчас; выпал из «Сейчас» — тень снимается. Бой — только
вручную, кнопкой в карточке. Пересчёт — раз в 6 ч (разведка) и сразу при добавлении / импорте.
"""
from __future__ import annotations

import logging
import math
import statistics
import time
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
LAST_TRIPS = 120                       # 30 дней: до 120 сделок, равномерно по времени (у скальпера 40 последних — час)
DAY_TRIPS = 60                         # сутки: последние до 60 сделок за 24 ч
BOT_DELAY_S = 1.0                      # бот входит через 4 блока после трейдера (~0.8 с) + посадка
HORIZON_S = 6 * 3600
MIN_N = 8
FOLLOW_KEY = "real:follow"
AUTO_KEY = "real:follow_auto"          # кого «тень за ним» включил сам бот (снимает только их)
HOT_LIMIT = 15                         # «Сейчас» (плюс за 30 дн и за сутки): тень за лучшими до 15 по суткам


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
    # 22.11: вход — со скоростью бота (4 блока ≈ 1 с), а не «руками» через 5 с: у Arkd (держит ~3 с) при 5 с все
    # выходы выходили около нуля, а бумага (вход через 4 блока) дала +$162 на 162 копиях
    delay = float(cfg.get("rules_delay_s", BOT_DELAY_S))
    _s7, s30, all_trips = await compute_trader(db, address, {**cfg, "copy_delay_s": delay}, costs)
    all_trips = sorted(all_trips, key=lambda t: t.entry_ts)
    step = max(1, len(all_trips) // LAST_TRIPS)
    month = all_trips[::-1][::step][:LAST_TRIPS][::-1]        # равномерно по 30 дням, свежие — обязательно
    day_cut = time.time() - 86400
    day = [t for t in all_trips if t.entry_ts >= day_cut][-DAY_TRIPS:]
    trips = list({id(t): t for t in month + day}.values())
    pos = float(cfg.get("position_sol", 0.5))
    hold = float(s30.median_hold_s or 0)
    # сколько дней его истории в базе: у добавленного вчера «30 дней» — на деле сутки (6d7y: 44 сделки и там, и там)
    days = min(30, math.ceil((time.time() - all_trips[0].entry_ts) / 86400)) if all_trips else 0
    out: dict[str, Any] = {"rules": {}, "rules_1d": {}, "best": None, "hold": hold, "n": len(month), "n_1d": len(day),
                           "days": days}
    if not trips:
        return out
    win: dict[str, list[float]] = {}             # по монете — только её окно (сделки за 30 дней разбросаны)
    for t in trips:
        lo, hi = t.entry_ts, max(t.exit_ts or 0, t.entry_ts) + HORIZON_S
        w = win.setdefault(t.mint, [lo, hi])
        w[0], w[1] = min(w[0], lo), max(w[1], hi)
    mints = list(win)
    rows = await db.fetch(
        """SELECT p.mint, extract(epoch FROM t.ts) AS t, t.mc_sol
           FROM unnest($1::text[], $2::float8[], $3::float8[]) AS p(mint, lo, hi)
           JOIN trades t ON t.mint = p.mint AND t.ts BETWEEN to_timestamp(p.lo) AND to_timestamp(p.hi) AND t.mc_sol > 0
           ORDER BY p.mint, t.ts""", mints, [win[m][0] for m in mints], [win[m][1] for m in mints])
    paths: dict[str, list[tuple[float, float]]] = {}
    for r in rows:
        paths.setdefault(r["mint"], []).append((float(r["t"]), float(r["mc_sol"])))
    res: dict[str, list[float]] = {k: [] for k in RULES}
    res_day: dict[str, list[float]] = {k: [] for k in RULES}
    month_ids, day_ids = {id(t) for t in month}, {id(t) for t in day}
    for t in trips:
        dst = [d for d, ids in ((res, month_ids), (res_day, day_ids)) if id(t) in ids]
        entry_at = t.entry_ts + delay
        entry_mc = float(t.copy_entry_mc or t.entry_mc or 0)
        sell_at = (t.sells[0][0] + delay) if t.sells else None
        if t.copy_exit_mc:
            v = trade_net_profit(entry_mc, float(t.copy_exit_mc), pos, costs)
            for d in dst:
                d["F"].append(v)
        path = [x for x in paths.get(t.mint, []) if entry_at < x[0] <= entry_at + HORIZON_S]
        if not path:
            continue
        for rule in RULES[1:]:
            sc = scheme(rule, hold, delay)
            if sc is None:
                continue
            v = simulate(path, entry_at, entry_mc, sell_at, sc, pos, costs.fee_bps, costs.priority_sol)
            if v is not None:
                for d in dst:
                    d[rule].append(v)
    for key, src in (("rules", res), ("rules_1d", res_day)):
        for rule, vals in src.items():
            if vals:
                out[key][rule] = {"n": len(vals), "w": sum(1 for v in vals if v > 0), "avg": statistics.fmean(vals),
                                  "sum": sum(vals), "t": t_stat(vals)}
    out["best"] = best_rule(out["rules"])
    return out


def exit_scheme(rules_or_best: Any, hold_s: float = 0.0) -> tuple[str, dict[str, Any]] | None:
    """Схема выхода копии по лучшему выходу кошелька: A / B / G / R — наши правила; F и W — «за ним» (продать, когда
    он; аварийный стоп −50%, до 6 ч). None — лучшего нет (тогда правило по умолчанию)."""
    best = rules_or_best if isinstance(rules_or_best, str) else None
    if best in ("A", "B", "G", "R"):
        return best, scheme(best, hold_s, BOT_DELAY_S) or SCHEMES[best]
    if best in ("F", "W"):
        return best, {"stop": 0.50, "tp_net": None, "tp_share": 0.0, "trail": None, "target": False,
                      "time_stop_s": None, "time_stop_gain": None, "max_hold_s": HORIZON_S}
    return None


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
    await db.execute("UPDATE traders SET rules_30d=$2, best_rule=$3, rules_1d=$4, rules_days=$5 WHERE address=$1",
                     address, res["rules"], res["best"], res.get("rules_1d") or {}, int(res.get("days") or 0))


def usd(rules: dict[str, Any] | None, rule: str | None, stake: float = 10.0) -> tuple[float, int] | None:
    """Итог по правилу при stake $ на сделку и сколько сделок: (доллары, сделок) или None."""
    r = (rules or {}).get(rule or "")
    return (float(r["sum"]) * stake, int(r["n"])) if r else None


def day_usd(res: dict[str, Any]) -> float | None:
    """Итог за сутки при $10 на сделку по лучшему выходу (None — лучшего нет или за сутки сделок нет)."""
    d = usd(res.get("rules_1d"), res.get("best"))
    return d[0] if d else None


def hot(results: dict[str, dict[str, Any]], limit: int = HOT_LIMIT) -> list[str]:
    """«Сейчас»: устойчивый плюс за 30 дней и итог за сутки в плюсе — лучшие за сутки, до limit."""
    ok = [(day_usd(r), a) for a, r in results.items() if r.get("best") and (day_usd(r) or 0) > 0]
    return [a for _v, a in sorted(ok, key=lambda x: -x[0])[:limit]]


async def top_text(db: Any, limit: int = 15) -> tuple[str, list[tuple[str, str]]]:
    """Кошельки с устойчивым лучшим выходом: итог при $10 на сделку за 30 дней и за сутки; лучшие сейчас — сверху.
    Возвращает текст и [(адрес, имя)] для кнопок карточек."""
    rows = await db.fetch(
        """SELECT address, label, best_rule, rules_30d, rules_1d, rules_days FROM traders
           WHERE best_rule IS NOT NULL AND (active OR address IN (SELECT address FROM watchlist WHERE kind = 'trader'))""")
    items = []
    for r in rows:
        m = usd(_j(r["rules_30d"]), r["best_rule"])
        d = usd(_j(r["rules_1d"]), r["best_rule"]) or (0.0, 0)
        if m:
            items.append((d[0], m[0], d[1], r))
    items.sort(key=lambda x: (-x[0], -x[1]))
    items = items[:limit]
    head = ("🏅 <b>Кошельки: итог при $10 на каждую его сделку</b>\nПродажа — по его лучшему выходу, вход со скоростью "
            "бота (~1 с), с комиссиями. Это расчёт по его сделкам, не деньги. Сверху — лучшие за последние сутки.")
    if not items:
        return head + "\n\nПока ни у кого нет устойчивого лучшего выхода (8+ сделок, плюс не случайность).", []
    table = [f"{'кошелёк':<9}{'выход':>7}{'30 дн':>7}{'сутки':>7}{'сд':>4}{'дн':>3}"]
    out = []
    for d_usd, m_usd, d_n, r in items:
        name = (r["label"] or f"{r['address'][:4]}…{r['address'][-4:]}")[:9]
        days = r["rules_days"] if r.get("rules_days") is not None else "?"
        table.append(f"{name:<9}{RULE_NAMES[r['best_rule']]:>7}{_m(m_usd):>7}{_m(d_usd) if d_n else '—':>7}"
                     f"{d_n if d_n else '':>4}{days:>3}")
        out.append((r["address"], name))
    tail = ("сд — его сделок за сутки; дн — сколько дней его истории в базе (1 — «30 дн» на деле сутки). Тень (без "
            "денег) за кошельками «Сейчас» и за своими с устойчивым плюсом бот включает сам и сам снимает. В бой — "
            "только твоей кнопкой в карточке.")
    return head + "\n<pre>" + "\n".join(table) + "</pre>\n" + tail, out


def _j(v: Any) -> dict[str, Any]:
    if isinstance(v, str):
        import json
        return json.loads(v)
    return v or {}


def _m(v: float) -> str:
    return f"{'+' if v >= 0 else '−'}${abs(v):.0f}"


async def autofollow(db: Any, results: dict[str, dict[str, Any]], own: set[str],
                     hot_limit: int = HOT_LIMIT) -> list[str]:
    """«👻 тень за ним» (сам): свои кошельки с лучшим выходом в устойчивом плюсе и кошельки «Сейчас» (плюс за 30
    дней и за сутки, лучшие до hot_limit); перестали — снять ту тень, что включил сам. Бой и включённое вручную
    не трогаются."""
    follow = dict(await db.kv_get(FOLLOW_KEY) or {})
    auto = set(await db.kv_get(AUTO_KEY) or [])
    now = set(hot(results, hot_limit))
    events: list[str] = []
    for addr, res in results.items():
        name = f"{addr[:4]}…{addr[-4:]}"
        want = bool(res.get("best")) and (addr in own or addr in now)
        if want and addr not in follow:
            follow[addr] = "shadow"
            auto.add(addr)
            why = top_line(res["rules"], res["best"])
            if addr in now:
                m = usd(res["rules"], res["best"])
                why = f"сейчас в плюсе: при $10 — 30 дн {_m(m[0])}, сутки {_m(day_usd(res) or 0)} · " + why
            events.append(f"👻 {name}: тень за ним включена — {why}")
        elif not want and addr in auto and follow.get(addr) == "shadow":
            follow.pop(addr, None)
            auto.discard(addr)
            why = "за сутки не в плюсе" if res.get("best") else top_line(res["rules"], None)
            events.append(f"⚪ {name}: тень за ним снята — {why}")
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
        await save(db, a, res)                  # строка в traders есть: импорт / добавление / пересчёт трейдеров
        if res["n"] or res.get("n_1d"):
            results[a] = res
    events = await autofollow(db, results, own)
    return len(results), events
