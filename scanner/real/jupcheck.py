"""25.11 (владелец, после рагов 1847 и 1863): пересчёт сделок других бирж (тренды trendj, крупные монеты majorj) с
фильтрами входа — LP заблокирован или сожжён и пул старше N часов. Сколько сделок осталось бы и какой итог.

Запуск на сервере: python -m scanner jupcheck [--days N] [--lp 0.9]
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


async def run(db: Any, min_lp: float = 0.9, days: float | None = None) -> str:
    since = await stats_since(db)
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
