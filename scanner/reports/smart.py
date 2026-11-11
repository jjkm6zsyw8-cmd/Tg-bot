"""/smart — только smart-сигналы: что они дали на бумаге, по дням, по кошелькам и что отличает плюсовые колы.

Источник — эталонный портфель G (все сигналы, одна схема: тейк +25%, стоп −8%, нет +10% за 5 мин — выход,
выход за трейдером; вход по рынку через ~5 с). Только закрытые сделки, цена которых сошлась с рынком (без ⚠).
Результат пересчитан на $10 в сделку: так дни и кошельки сравнимы между собой при любом балансе портфеля.

«Что отличает плюсовые колы» — признаки монеты в момент сигнала (как их видел бот) → средний результат smart-сделок
в каждом диапазоне признака, отклонение от общего среднего, «стянутое» к нулю, пока примеров мало (как в автофильтре).
Проверка «вперёд по времени»: модель учится на днях до D и решает по дню D — показывает, помог бы такой отбор или нет.
"""
from __future__ import annotations

import statistics as st
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..alerts.format import esc

EXIT_SHORT = {"emergency": "дев продал", "trader_sell": "трейдер продал", "stop": "стоп", "take_profit": "тейк",
              "target": "цель", "trailing": "трейлинг", "time_stop": "нет роста", "max_hold": "долго без выхода"}
USD = 10.0


def _zone(tz: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz)
    except Exception:  # noqa: BLE001
        return ZoneInfo("UTC")


def chunks(text: str, limit: int = 3900) -> list[str]:
    """Режем по строкам, чтобы не порвать HTML-теги."""
    out, cur = [], ""
    for line in text.split("\n"):
        if cur and len(cur) + len(line) + 1 > limit:
            out.append(cur)
            cur = ""
        cur += line + "\n"
    if cur.strip():
        out.append(cur)
    return out


def summary_line(pcts: list[float]) -> str:
    """N сделок · в плюсе · итог при $10 · средняя · средний плюс/минус · плюсы/минусы."""
    if not pcts:
        return "сделок нет"
    wins = [p for p in pcts if p > 0]
    losses = [p for p in pcts if p <= 0]
    pf = (sum(wins) / -sum(losses)) if losses and sum(losses) < 0 else None
    return (f"{len(pcts)} сд. · в плюсе {len(wins)} ({len(wins) / len(pcts):.0%}) · <b>{sum(pcts) * USD:+.0f}$</b> · "
            f"в среднем {st.mean(pcts):+.1%}"
            + (f" · средний плюс {st.mean(wins):+.0%}" if wins else "")
            + (f", минус {st.mean(losses):+.0%}" if losses else "")
            + (f" · плюсы/минусы ×{pf:.1f}" if pf is not None else ""))


async def smart_report(db: Any, days: int = 7, tz: str = "Asia/Tbilisi", accounts: int = 2) -> str:
    from ..analytics.origin import OUTLIER, UNMARKED, origins
    zone = _zone(tz)
    accounts = max(1, int(accounts))
    rows = await db.fetch(
        """SELECT p.opened_at, p.pnl_pct, p.level, p.symbol, p.exit_reason, a.trader, p.source_wallet,
                  a.payload->'traders' AS traders
           FROM paper_positions p JOIN alerts a ON a.id = p.alert_id
           WHERE p.portfolio = 'G' AND a.mode = 'smart' AND p.closed_at IS NOT NULL AND NOT p.suspect
             AND p.exit_reason <> 'reset' AND p.opened_at > now() - make_interval(days => $1)
           ORDER BY p.opened_at""", days)
    opened = await db.fetchval(
        """SELECT count(*) FROM paper_positions p JOIN alerts a ON a.id = p.alert_id
           WHERE p.portfolio = 'G' AND a.mode = 'smart' AND p.closed_at IS NULL""")
    suspect = await db.fetchval(
        """SELECT count(*) FROM paper_positions p JOIN alerts a ON a.id = p.alert_id
           WHERE p.portfolio = 'G' AND a.mode = 'smart' AND p.closed_at IS NOT NULL AND p.suspect
             AND p.exit_reason <> 'reset' AND p.opened_at > now() - make_interval(days => $1)""", days)
    lines = [f"🧠 <b>Smart-сигналы</b> · {days} дн. · схема G (тейк +25%, стоп −8%, 5 мин на +10%, выход за "
             f"трейдером), вход по рынку через ~5 с · <b>$10 на сделку</b> · только сделки, сверенные с рынком"
             + (f" (не сошлись с рынком и не считаются: {suspect})" if suspect else "")]
    pcts = [float(r["pnl_pct"] or 0) for r in rows]
    lines.append("Итого: " + summary_line(pcts) + (f" · открыто сейчас {opened}" if opened else ""))
    if not rows:
        return "\n".join(lines)
    if accounts > 1:
        lines.append(f"На {accounts} аккаунта по $10 (одни и те же колы): <b>{sum(pcts) * USD * accounts:+.0f}$</b>")
    xs = [r for r in rows if float(r["pnl_pct"] or 0) >= OUTLIER]
    if xs:
        rest = [float(r["pnl_pct"] or 0) for r in rows if float(r["pnl_pct"] or 0) < OUTLIER]
        lines.append(f"Иксы (сделка ≥+{OUTLIER:.0%}), отдельно: {len(xs)} сд. · "
                     f"{sum(float(r['pnl_pct'] or 0) for r in xs) * USD:+.0f}$ — "
                     + ", ".join(f"{esc(r['symbol'] or '?')} {float(r['pnl_pct'] or 0):+.0%} "
                                 f"({r['opened_at'].astimezone(zone).strftime('%d.%m')})" for r in xs[:6]))
        lines.append("Без иксов: " + summary_line(rest))
    one = [float(r["pnl_pct"] or 0) for r in rows if r["level"] != "strong"]
    strong = [float(r["pnl_pct"] or 0) for r in rows if r["level"] == "strong"]
    lines.append(f"Один кошелёк: {summary_line(one)}")
    lines.append(f"Сильные (2+ кошелька): {summary_line(strong)}")
    by_exit: dict[str, list[float]] = {}
    for r in rows:
        by_exit.setdefault(EXIT_SHORT.get(r["exit_reason"] or "", r["exit_reason"] or "?"), []).append(
            float(r["pnl_pct"] or 0))
    lines.append("Выходы: " + " · ".join(f"{k} {len(v)} ({st.mean(v):+.0%})"
                                         for k, v in sorted(by_exit.items(), key=lambda kv: -len(kv[1]))))

    lines.append("\n<b>По дням</b>")
    by_day: dict[str, list[Any]] = {}
    for r in rows:
        by_day.setdefault(r["opened_at"].astimezone(zone).strftime("%m.%d"), []).append(r)
    cum = 0.0
    for day in sorted(by_day):
        rs = by_day[day]
        v = [float(r["pnl_pct"] or 0) for r in rs]
        cum += sum(v) * USD
        best = max(rs, key=lambda r: float(r["pnl_pct"] or 0))
        no_x = [x for x in v if x < OUTLIER]
        lines.append(f"{day[3:]}.{day[:2]}: {len(v)} сд. · в плюсе {sum(x > 0 for x in v) / len(v):.0%} · "
                     f"<b>{sum(v) * USD:+.0f}$</b>"
                     + (f" (без иксов {sum(no_x) * USD:+.0f}$)" if len(no_x) < len(v) else "")
                     + f" · лучшая {float(best['pnl_pct'] or 0):+.0%} {esc(best['symbol'] or '')} · с начала {cum:+.0f}$")

    labels = {r["address"]: r["label"] for r in await db.fetch(
        """SELECT address, label FROM traders WHERE label IS NOT NULL
           UNION ALL SELECT address, label FROM watchlist WHERE label IS NOT NULL""")}
    by_w: dict[str, list[float]] = {}
    for r in rows:
        w = r["source_wallet"] or r["trader"]
        if w:
            by_w.setdefault(w, []).append(float(r["pnl_pct"] or 0))
    ranked = sorted(((sum(v) * USD, w, v) for w, v in by_w.items()), key=lambda x: -x[0])
    if ranked:
        name = lambda w: esc(labels.get(w) or f"{w[:4]}…{w[-4:]}")
        lines.append(f"\n<b>Кошельки</b> ({len(ranked)} дали сигналы)")
        show = ranked if len(ranked) <= 8 else ranked[:5] + ranked[-3:]
        for usd, w, v in show:
            lines.append(f"{name(w)}: {len(v)} сд. · в плюсе {sum(x > 0 for x in v)} · {usd:+.0f}$"
                         + (f"\n   <code>{w}</code>" if usd < 0 else ""))
    # копии своего списка по источникам — какой источник даёт кошельки, на которых бумага в плюсе
    org = await origins(db)
    copies = await db.fetch(
        """SELECT p.pnl_pct, COALESCE(p.source_wallet, a.trader) w FROM paper_positions p
           JOIN alerts a ON a.id = p.alert_id
           WHERE p.portfolio = 'G' AND a.mode = 'copy' AND p.closed_at IS NOT NULL AND NOT p.suspect
             AND p.exit_reason <> 'reset' AND p.opened_at > now() - make_interval(days => $1)""", days)
    by_o: dict[str, list[float]] = {}
    for r in copies:
        by_o.setdefault(org.get(r["w"] or "", UNMARKED), []).append(float(r["pnl_pct"] or 0))
    if by_o:
        lines.append("\n<b>Для сравнения — копии своего списка по источникам</b> (G, $10 на сделку)")
        for o, v in sorted(by_o.items(), key=lambda kv: -sum(kv[1])):
            no_x = [x for x in v if x < OUTLIER]
            lines.append(f"{esc(o)}: {summary_line(v)}" + (f" · без иксов {sum(no_x) * USD:+.0f}$"
                                                           if len(no_x) < len(v) else ""))
    lines += await _what_differs(db)
    return "\n".join(lines)


async def _what_differs(db: Any, days: int = 14) -> list[str]:
    from ..analytics.autofilter import NAMES, dataset, fit, walk_forward
    from ..analytics.origin import OUTLIER, capped
    rows = [(x, capped(y), d) for x, y, d in await dataset(db, days) if str(x.get("kind") or "").startswith("smart")]
    out = [f"\n<b>Что отличает плюсовые колы</b> ({days} дн., {len(rows)} smart-сделок; иксы выше +{OUTLIER:.0%} "
           f"считаются как +{OUTLIER:.0%}, чтобы одна ракета не решала за всех)"]
    if len(rows) < 40:
        out.append(f"Пока мало сделок для разбора (нужно 40+, есть {len(rows)}).")
        return out
    model = fit([(x, y) for x, y, _ in rows])
    effects: list[tuple[float, str, int]] = []
    for f, spec in (model.get("num") or {}).items():
        edges = spec["edges"]
        for b, d in spec["delta"].items():
            n = spec["n"].get(b, 0)
            if n < 8:
                continue
            i = int(b)
            lo = f"{edges[i - 1]:.3g}" if i > 0 else "…"
            hi = f"{edges[i]:.3g}" if i < len(edges) else "…"
            effects.append((d, f"{NAMES.get(f, f)} {lo}–{hi}", n))
    if effects:
        effects.sort()
        out.append(f"Средний результат smart-сделки {model.get('base', 0):+.1%}. Отклонение от него:")
        up = [f"{name}: {d:+.1%} ({n})" for d, name, n in effects[::-1][:5] if d > 0]
        down = [f"{name}: {d:+.1%} ({n})" for d, name, n in effects[:5] if d < 0]
        if up:
            out.append("↑ " + " · ".join(up))
        if down:
            out.append("↓ " + " · ".join(down))
    c = walk_forward(rows)
    if c.get("taken_avg") is not None and c.get("skipped"):
        out.append(f"Проверка вперёд по времени (решение только по прошлым дням): отбор взял бы {c['taken']} "
                   f"(в среднем {c['taken_avg']:+.1%}), отсеял бы {c['skipped']} (в среднем {c['skipped_avg']:+.1%}) — "
                   + ("отбор помогает" if c.get("useful") else "пользы пока не доказал"))
    else:
        out.append("Проверка вперёд по времени: пока мало дней истории.")
    return out
