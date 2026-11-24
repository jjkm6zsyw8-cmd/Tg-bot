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

import asyncio
import logging
import math
import statistics
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from .copytrade import MIN_COPY_T, compute_trader, t_stat
from .curve import trade_net_profit
from ..alerts.format import esc
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
SELL_MARGIN_S = 300.0                  # путь цены — до его продажи + 5 мин (первая сделка после неё)
REFRESH_PAUSE_S = 0.5                  # пауза между кошельками при пересчёте — база и бот не задыхаются
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


def spread(trips: list[Any], limit: int) -> list[Any]:
    """До limit сделок равномерно по времени (свежая — обязательно), порядок — по времени."""
    step = max(1, len(trips) // limit) if limit else 1
    return trips[::-1][::step][:limit][::-1]


PERIOD_TRIPS = {"today": 60, "yday": 60, "week": 120}


async def evaluate(db: Any, address: str, cfg: dict[str, Any], costs: Any) -> dict[str, Any]:
    """Сделки кошелька — через все выходы. {"rules": 30 дн, "rules_1d": 24 ч, "periods": сегодня / вчера / 7 дн /
    30 дн, "best", "hold", "n", "n_1d", "days"}; правило → {n, w, avg, sum, t}."""
    # 22.11: вход — со скоростью бота (4 блока ≈ 1 с), а не «руками» через 5 с: у Arkd (держит ~3 с) при 5 с все
    # выходы выходили около нуля, а бумага (вход через 4 блока) дала +$162 на 162 копиях
    delay = float(cfg.get("rules_delay_s", BOT_DELAY_S))
    _s7, s30, all_trips = await compute_trader(db, address, {**cfg, "copy_delay_s": delay}, costs)
    all_trips = sorted(all_trips, key=lambda t: t.entry_ts)
    now = time.time()
    month = spread(all_trips, LAST_TRIPS)
    day = [t for t in all_trips if t.entry_ts >= now - 86400][-DAY_TRIPS:]
    # периоды для владельца (22.11): сегодня и вчера — по местному времени, неделя, месяц; всех его сделок периода
    # может быть тысячи (Arkd — 158 в день), поэтому считается выборка, а итог = средний результат × сколько сделок
    try:
        tz = ZoneInfo(str(cfg.get("timezone") or "Asia/Tbilisi"))
    except Exception:  # noqa: BLE001
        tz = ZoneInfo("Asia/Tbilisi")
    d0 = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    bounds = {"today": (d0, math.inf), "yday": (d0 - 86400, d0), "week": (now - 7 * 86400, math.inf)}
    p_all = {k: [t for t in all_trips if lo <= t.entry_ts < hi] for k, (lo, hi) in bounds.items()}
    p_all["month"] = all_trips
    samples = {k: spread(v, PERIOD_TRIPS[k]) for k, v in p_all.items() if k != "month"}
    samples["month"] = month
    trips = list({id(t): t for t in month + day + [x for v in samples.values() for x in v]}.values())
    pos = float(cfg.get("position_sol", 0.5))
    hold = float(s30.median_hold_s or 0)
    # сколько дней его истории в базе: у добавленного вчера «30 дней» — на деле сутки (6d7y: 44 сделки и там, и там)
    days = min(30, math.ceil((now - all_trips[0].entry_ts) / 86400)) if all_trips else 0
    out: dict[str, Any] = {"rules": {}, "rules_1d": {}, "periods": {}, "best": None, "hold": hold, "n": len(month),
                           "n_1d": len(day), "days": days}
    if not trips:
        return out
    # окно цены каждой сделки — от входа до его первой продажи (+5 мин запаса): все выходы продают, когда продал он,
    # дальше путь не нужен (22.11: окно «+6 ч» на сотнях сделок грузило базу так, что бот переставал отвечать)
    def until(t: Any) -> float:
        end = (t.sells[0][0] + delay + SELL_MARGIN_S) if t.sells else t.entry_ts + HORIZON_S
        return min(end, t.entry_ts + delay + HORIZON_S)
    win: dict[str, list[float]] = {}             # по монете — только её окна (сделки за 30 дней разбросаны)
    for t in trips:
        lo, hi = t.entry_ts, until(t)
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
    per: dict[int, dict[str, float]] = {}        # результат каждой сделки по каждому выходу
    for t in trips:
        v: dict[str, float] = {}
        entry_at = t.entry_ts + delay
        entry_mc = float(t.copy_entry_mc or t.entry_mc or 0)
        sell_at = (t.sells[0][0] + delay) if t.sells else None
        if t.copy_exit_mc:
            v["F"] = trade_net_profit(entry_mc, float(t.copy_exit_mc), pos, costs)
        end = until(t)
        path = [x for x in paths.get(t.mint, []) if entry_at < x[0] <= end]
        if path:
            for rule in RULES[1:]:
                sc = scheme(rule, hold, delay)
                if sc is None:
                    continue
                res = simulate(path, entry_at, entry_mc, sell_at, sc, pos, costs.fee_bps, costs.priority_sol)
                if res is not None:
                    v[rule] = res
        per[id(t)] = v

    def agg(group: list[Any]) -> dict[str, dict[str, float]]:
        out_: dict[str, dict[str, float]] = {}
        for rule in RULES:
            vals = [per[id(t)][rule] for t in group if rule in per[id(t)]]
            if vals:
                out_[rule] = {"n": len(vals), "w": sum(1 for x in vals if x > 0), "avg": statistics.fmean(vals),
                              "sum": sum(vals), "t": t_stat(vals)}
        return out_
    out["rules"], out["rules_1d"] = agg(month), agg(day)
    out["periods"] = {k: {"N": len(p_all[k]), "rules": agg(samples[k])} for k in p_all}
    out["best"] = best_rule(out["rules"])
    return out


def period_usd(periods: dict[str, Any] | None, rule: str | None, key: str, stake: float = 10.0) -> float | None:
    """≈ итог периода при stake $ на каждую его сделку: средний результат выборки × сколько его сделок за период."""
    p = (periods or {}).get(key) or {}
    r = (p.get("rules") or {}).get(rule or "")
    if not p.get("N"):
        return 0.0 if p else None
    return float(r["avg"]) * int(p["N"]) * stake if r else None


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
    await db.execute("UPDATE traders SET rules_30d=$2, best_rule=$3, rules_1d=$4, rules_days=$5, rules_periods=$6 "
                     "WHERE address=$1", address, res["rules"], res["best"], res.get("rules_1d") or {},
                     int(res.get("days") or 0), res.get("periods") or {})


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
    """Коротко: кошелёк · выход: сегодня / вчера / 7д / 30д — ≈ итог, если бы бот брал $10 на каждую его сделку.
    Возвращает текст и [(адрес, имя)] для кнопок карточек."""
    rows = await db.fetch(
        """SELECT address, label, best_rule, rules_periods, rules_days, rules_1d FROM traders
           WHERE best_rule IS NOT NULL AND (active OR address IN (SELECT address FROM watchlist WHERE kind = 'trader'))""")
    items = []
    for r in rows:
        d = usd(_j(r["rules_1d"]), r["best_rule"])
        items.append((d[0] if d else 0.0, r))
    items.sort(key=lambda x: -x[0])
    items = items[:limit]
    head = "💰 <b>Если бы бот брал $10 на каждую его сделку</b> (расчёт по истории, ≈)"
    if not items:
        return head + "\nПока нет кошельков с устойчивым плюсом.", []
    # 25.11: рядом с расчётом — что на самом деле дал реальный бот за этим кошельком (тень и бой, настоящие цены,
    # 7 дней): расчёт берёт цену трейдера, а бот входит позже и дороже — разница бывает в разы
    real = {x["trader"]: x for x in await db.fetch(
        """SELECT trader, count(*) n, COALESCE(sum(pnl_sol), 0) s FROM real_positions
           WHERE trader = ANY($1::text[]) AND closed_at IS NOT NULL AND closed_at > now() - interval '7 days'
           GROUP BY trader""", [r["address"] for _d, r in items])}
    sol_usd = float(((await db.kv_get("sol_usd", {})) or {}).get("price") or 150)
    lines, out = [head, "👻 — что на самом деле дал реальный бот за ним (настоящие цены, 7 дн)"], []
    for _d, r in items:
        name = r["label"] or f"{r['address'][:4]}…{r['address'][-4:]}"
        x = real.get(r["address"])
        fact = f"👻 {_m(float(x['s']) * sol_usd)} ({x['n']} сд)" if x else "👻 бот за ним не торговал"
        lines.append(f"<b>{esc(name[:14])}</b> · {RULE_NAMES[r['best_rule']]}: " + periods_text(
            _j(r["rules_periods"]), r["best_rule"], r["rules_days"]) + f" · {fact}")
        out.append((r["address"], name[:14]))
    return "\n".join(lines), out


def periods_text(periods: dict[str, Any], rule: str | None, days: int | None) -> str:
    """«сегодня +$12 · вчера +$43 · 7д +$180 · 30д +$600»; истории меньше периода — «16д +$…»."""
    if not periods:
        return "ещё не посчитано"
    parts = []
    for key, name in (("today", "сегодня"), ("yday", "вчера"), ("week", "7д"), ("month", "30д")):
        v = period_usd(periods, rule, key)
        if key == "month" and days is not None and days < 30:
            if days == 7:
                continue                    # «7д» уже есть — второй раз то же самое не пишем
            name = f"{days}д"
        if key == "week" and days is not None and days < 7:
            continue
        parts.append(f"{name} {_m(v) if v is not None else '—'}")
    return " · ".join(parts)


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
    не трогаются. Выключено в настройках (real_autofollow, «🧹 Сбросить всё») — ничего не включает."""
    try:
        overrides = await db.get_overrides()
    except Exception:  # noqa: BLE001
        overrides = {}
    if overrides.get("real_autofollow") is False:
        return []
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
            events.append(f"👻 {name}: тень включена")
        elif not want and addr in auto and follow.get(addr) == "shadow":
            follow.pop(addr, None)
            auto.discard(addr)
            why = "за сутки не в плюсе" if res.get("best") else "плюс пропал"
            events.append(f"⚪ {name}: тень снята — {why}")
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
        await asyncio.sleep(REFRESH_PAUSE_S)
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
