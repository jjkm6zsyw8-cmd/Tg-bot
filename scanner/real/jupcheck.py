"""25.11 (владелец, после рагов 1847 и 1863): пересчёт сделок других бирж (тренды trendj, крупные монеты majorj) с
фильтрами входа — LP заблокирован или сожжён и пул старше N часов. Сколько сделок осталось бы и какой итог.

Запуск на сервере: python -m scanner jupcheck [--all] [--days N] [--lp 0.9]   (--all — и до «считать с нуля»)
27.11: jupcheck --trials [--days 3] — по каждой сделке выход позиции и выходы проб A/B/G/R (время, цена, причина);
       jupcheck --filter [--hours 24] — сколько сигналов фильтры входа пропустили и сколько отсекли и почему.
Данные — то, что бот записал на входе в extra позиции: lp_locked (RugCheck, доля LP главного пула, заблокированная
или сожжённая; есть с 24.11) и age_s (возраст пула, по которому пришёл сигнал). Чёрный список создателей задним числом
не применяется: у старых позиций создатель не записан.
"""
from __future__ import annotations

import statistics
from datetime import datetime
from typing import Any

from .report import sol_price, stats_since

AGES_H = (0, 1, 3, 6, 12, 24)
KINDS = (("trendj", "тренды других бирж"), ("majorj", "крупные монеты"))


def _num(v: Any) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _line(label: str, rows: list[dict[str, Any]], sol_usd: float) -> str:
    if not rows:
        return f"{label:<10}{0:>4}"
    pct = [r["pnl_pct"] for r in rows]
    sol = sum(r["pnl_sol"] for r in rows)
    win = sum(1 for x in pct if x > 0) / len(pct)
    rugs = sum(1 for r in rows if r["exit_reason"] in ("rug", "rug_back"))
    return (f"{label:<10}{len(rows):>4}{statistics.fmean(pct):>+9.1%}{sol:>+10.3f}{sol * sol_usd:>+9.2f}"
            f"{win:>8.0%}{rugs:>6}")


def report_text(rows: list[dict[str, Any]], min_lp: float, sol_usd: float, since: datetime | None,
                ages_h: tuple[float, ...] = AGES_H) -> str:
    out = [f"Пересчёт с фильтрами входа: LP заблокирован или сожжён от {min_lp:.0%}, пул старше N ч"
           + (f" · сделки с {since:%d.%m %H:%M} UTC" if since else " · все сделки")]
    head = f"{'':<10}{'сд':>4}{'ср.':>9}{'SOL':>10}{'$':>9}{'в плюсе':>8}{'раги':>6}"
    for kind, name in KINDS:
        for mode, mname in (("shadow", "тень"), ("live", "бой")):
            xs = [r for r in rows if r["kind"] == kind and r["mode"] == mode]
            if not xs:
                continue
            nodata = [r for r in xs if r["lp_locked"] is None]
            unlocked = [r for r in xs if r["lp_locked"] is not None and r["lp_locked"] < min_lp]
            locked = [r for r in xs if r["lp_locked"] is not None and r["lp_locked"] >= min_lp]
            out += ["", f"{name} · {mname}", head, _line("все", xs, sol_usd),
                    _line("LP нет д.", nodata, sol_usd), _line("LP откр.", unlocked, sol_usd),
                    _line("LP закр.", locked, sol_usd), "LP закрыт + пул старше N ч:"]
            for h in ages_h:
                keep = [r for r in locked if r["age_s"] is not None and r["age_s"] >= h * 3600]
                out.append(_line(f"N = {h:g} ч", keep, sol_usd))
            if mode == "live":
                why = {r["id"]: ("LP нет данных" if r["lp_locked"] is None else
                                 f"LP {r['lp_locked']:.0%}" if r["lp_locked"] < min_lp else "LP закрыт")
                       + (f", пул {r['age_s'] / 3600:.1f} ч" if r["age_s"] is not None else ", возраст ?")
                       for r in xs}
                out.append("Сделки боя: " + "; ".join(f"{r['id']} ${r['symbol'] or '?'} {r['pnl_pct']:+.0%} — "
                                                     f"{why[r['id']]}" for r in xs))
    if len(out) == 1:
        out.append("Сделок других бирж за период нет.")
    out += ["", "«LP нет д.» — позиции до 24.11 (RugCheck на входе ещё не записывался): по новому правилу в них бот не "
                "вошёл бы (нет данных — не входим)."]
    return "\n".join(out)


def _hm(v: Any) -> str:
    return v.strftime("%d.%m %H:%M") if v is not None else "—"


def _x(entry: Any, exit_: Any) -> str:
    e, x = _num(entry), _num(exit_)
    return f"×{x / e:.2f}" if e and x is not None else "?"


async def trials_text(db: Any, days: float = 3.0, kinds: tuple[str, ...] = ("trendj", "majorj")) -> str:
    """27.11 (владелец: «как R в плюсе, если 5 монет из 9 обнулились»): по каждой сделке — выход позиции и выходы проб
    (время после входа, цена выхода к цене входа ×, причина, итог), чтобы видеть, кто и когда вышел до рага."""
    pos = await db.fetch(
        """SELECT id, kind, mode, symbol, mint, opened_at, closed_at, entry_mc, exit_mc, exit_reason, pnl_pct,
                  COALESCE(extra->>'rule', '?') AS rule FROM real_positions
           WHERE kind = ANY($1::text[]) AND opened_at > now() - make_interval(secs => $2 * 86400) ORDER BY id""",
        list(kinds), days)
    if not pos:
        return f"Сделок {'/'.join(kinds)} за {days:g} дн нет."
    tri = await db.fetch(
        """SELECT position_id, rule, opened_at, closed_at, entry_mc, exit_mc, max_mc, exit_reason, pnl_pct
           FROM real_rule_trials WHERE position_id = ANY($1::bigint[]) ORDER BY rule""", [p["id"] for p in pos])
    by: dict[int, list[Any]] = {}
    for t in tri:
        by.setdefault(t["position_id"], []).append(t)

    def mins(a: Any, b: Any) -> str:
        return f"{(b - a).total_seconds() / 60:.0f} мин" if a is not None and b is not None else "открыта"
    lines = [f"Сделки {'/'.join(kinds)} за {days:g} дн и пробы правил (время после входа · выход/вход · причина · итог)"]
    agg: dict[str, list[float]] = {}
    for p in pos:
        lines.append(f"#{p['id']} {p['symbol'] or p['mint'][:6]} ({p['mode']}, правило {p['rule']}) вход "
                     f"{_hm(p['opened_at'])}: позиция — {mins(p['opened_at'], p['closed_at'])} · "
                     f"{_x(p['entry_mc'], p['exit_mc'])} · {p['exit_reason'] or '—'} · "
                     + (f"{float(p['pnl_pct']):+.0%}" if p["pnl_pct"] is not None else "—"))
        for t in by.get(p["id"], []):
            pk = f", пик ×{float(t['max_mc']) / float(t['entry_mc']):.2f}" if t["max_mc"] and t["entry_mc"] else ""
            lines.append(f"    {t['rule']}: {mins(t['opened_at'], t['closed_at'])} · {_x(t['entry_mc'], t['exit_mc'])} · "
                         f"{t['exit_reason'] or '—'} · "
                         + (f"{float(t['pnl_pct']):+.0%}" if t["pnl_pct"] is not None else "—") + pk)
            if t["pnl_pct"] is not None:
                agg.setdefault(t["rule"], []).append(float(t["pnl_pct"]))
        if p["pnl_pct"] is not None:
            agg.setdefault("позиция", []).append(float(p["pnl_pct"]))
    lines.append("Итого: " + "; ".join(f"{k} {len(v)} сд, {statistics.fmean(v):+.1%}, в плюсе "
                                        f"{sum(1 for x in v if x > 0) / len(v):.0%}" for k, v in agg.items()))
    return "\n".join(lines)


FILTER_GROUPS = (("LP не заблокирован / не сожжён", "LP пула"), ("пул моложе порога", "пул моложе"),
                 ("создатель уже рагал", "создатель монеты уже рагал"), ("права создателя", "может заморозить"),
                 ("права создателя", "может допечатать"), ("метод выключен", "выключен"))


async def filter_text(db: Any, hours: float = 24.0) -> str:
    """27.11: сигналы других бирж за hours: сколько прошли фильтры входа (позиции) и сколько отсечено — по причинам."""
    rows = await db.fetch(
        """SELECT kind, why, count(*) AS n, count(DISTINCT mint) AS coins FROM real_skips
           WHERE kind IN ('trendj', 'majorj') AND at > now() - make_interval(secs => $1 * 3600)
           GROUP BY 1, 2""", hours)
    opened = {r["kind"]: (int(r["n"]), int(r["coins"])) for r in await db.fetch(
        """SELECT kind, count(*) AS n, count(DISTINCT mint) AS coins FROM real_positions
           WHERE kind IN ('trendj', 'majorj') AND opened_at > now() - make_interval(secs => $1 * 3600)
           GROUP BY 1""", hours)}
    lines = [f"Фильтры входа других бирж за {hours:g} ч (монет)"]
    for kind, name in KINDS:
        groups: dict[str, int] = {}
        for r in rows:
            if r["kind"] != kind:
                continue
            g = next((label for label, key in FILTER_GROUPS if key in r["why"]), "другое: " + r["why"][:40])
            groups[g] = groups.get(g, 0) + int(r["coins"])
        n_open, c_open = opened.get(kind, (0, 0))
        cut = sum(v for k, v in groups.items() if k != "метод выключен")
        lines.append(f"{name}: прошли {c_open} (сделок {n_open}), отсечено {cut}"
                     + ("".join(f"\n   {k}: {v}" for k, v in sorted(groups.items(), key=lambda kv: -kv[1]))))
        if not groups and not c_open:
            lines.append("   сигналов не было — кошелёк бота выключен или метод не включён (тогда фильтр не проверяется)")
    return "\n".join(lines)


async def run(db: Any, min_lp: float = 0.9, days: float | None = None, all_time: bool = False) -> str:
    """all_time — без даты «считать с нуля» (все сделки, что есть в базе)."""
    since = None if all_time else await stats_since(db)
    rows = await db.fetch(
        """SELECT id, kind, mode, symbol, pnl_pct, pnl_sol, exit_reason, opened_at, extra->>'lp_locked' AS lp,
                  extra->>'age_s' AS age FROM real_positions
           WHERE kind IN ('trendj', 'majorj') AND closed_at IS NOT NULL AND pnl_pct IS NOT NULL
             AND ($1::timestamptz IS NULL OR opened_at >= $1)
             AND ($2::float8 IS NULL OR opened_at > now() - make_interval(secs => $2 * 86400))
           ORDER BY id""", since, days)
    data = [{"id": r["id"], "kind": r["kind"], "mode": r["mode"], "symbol": r["symbol"],
             "pnl_pct": float(r["pnl_pct"]), "pnl_sol": float(r["pnl_sol"] or 0), "exit_reason": r["exit_reason"],
             "lp_locked": _num(r["lp"]), "age_s": _num(r["age"])} for r in rows]
    return report_text(data, min_lp, await sol_price(db), since)
