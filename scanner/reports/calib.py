"""/calib — самонастройка стопов и тейков по реальной истории сигналов.

Для каждой сделки бумаги самопроверка записывает путь цены за 30 минут после входа (минутные свечи
GeckoTerminal): лучший рост, худшую просадку, просадку до пика и цену через 30 минут. Здесь по этим путям
перебирается сетка «тейк × стоп» и считается средний результат на сигнал — отдельно для копий, smart-money,
лотереи и каждого кошелька. Это подсказка, какие стоп и тейк ставить; применять — после проверки вживую.

Модель: тейк срабатывает, если монета дошла до него и до пика не проседала глубже стопа; иначе стоп — с
проскальзыванием 5% (на дампе стоп исполняется ниже уровня); иначе выход по цене через 30 минут. Комиссии
круга ~3%. Это грубее бумаги (минутные свечи), поэтому сравнивать варианты между собой, а не верить в проценты.
"""
from __future__ import annotations

import statistics as st
from typing import Any

from ..alerts.format import esc

FEE = 0.03
SLIP = 0.05
TPS = (0.25, 0.40, 0.60, 1.0, 2.0)
SLS = (0.08, 0.15, 0.20, 0.30)
MIN_N = 20
MIN_WALLET_N = 10


def sim(r: dict[str, float], tp: float, sl: float) -> float:
    if r["mfe"] >= tp and r["dip"] > -sl:
        return tp - FEE
    if r["mae"] <= -sl:
        return -sl - SLIP - FEE
    return r["p30"] - FEE


def best(rows: list[dict[str, float]]) -> tuple[float, float, float]:
    """(тейк, стоп, средний результат на сигнал) — лучшая пара сетки."""
    return max(((tp, sl, st.mean(sim(r, tp, sl) for r in rows)) for tp in TPS for sl in SLS), key=lambda x: x[2])


def group_line(name: str, rows: list[dict[str, float]]) -> str:
    n = len(rows)
    if n < MIN_N:
        return f"• <b>{name}</b>: {n} сигналов — мало данных (нужно {MIN_N}+)"
    hit = lambda x: sum(r["mfe"] >= x for r in rows) / n
    winners = sorted(r["dip"] for r in rows if r["mfe"] >= 0.25)
    dip75 = winners[int(len(winners) * 0.25)] if winners else 0.0
    tp, sl, avg = best(rows)
    g = st.mean(sim(r, 0.25, 0.08) for r in rows)
    return (f"• <b>{name}</b>: {n} сигналов · за 30 мин +25% у {hit(0.25):.0%}, +50% у {hit(0.5):.0%}, ×2 у {hit(1.0):.0%}"
            f" · просадка до +25% у 75% выстреливших не глубже {dip75:+.0%}\n"
            f"   лучше всего: тейк +{tp:.0%}, стоп −{sl:.0%} → {avg:+.1%} на сигнал (сейчас G: +25%/−8% → {g:+.1%})")


async def calib_report(db: Any, days: float = 7.0) -> str:
    rows = await db.fetch(
        """SELECT p.portfolio, p.mint, date_trunc('minute', p.opened_at) m, p.source_wallet,
                  p.path_mfe mfe, p.path_mae mae, p.path_dip dip, p.path_p30 p30,
                  (SELECT w.label FROM watchlist w WHERE w.address = p.source_wallet AND w.label IS NOT NULL LIMIT 1) label
           FROM paper_positions p
           WHERE p.path_mfe IS NOT NULL AND NOT p.suspect AND p.opened_at > now() - $1::float8 * interval '1 day'""",
        float(days))
    groups: dict[str, dict[tuple[str, Any], dict[str, Any]]] = {"копии": {}, "smart-money": {}, "лотерея D": {}}
    for r in rows:
        g = "лотерея D" if r["portfolio"] == "D" else "smart-money" if r["portfolio"] == "F" else "копии"
        groups[g].setdefault((r["mint"], r["m"]), dict(r))          # один сигнал — одна запись на все портфели
    lines = [f"🧪 <b>Калибровка по истории за {days:g} дн.</b> (путь цены 30 мин после входа, свечи GeckoTerminal)"]
    for name, sigs in groups.items():
        lines.append(group_line(name, list(sigs.values())))
    wallets: dict[str, list[dict[str, Any]]] = {}
    for r in groups["копии"].values():
        if r["source_wallet"]:
            wallets.setdefault(r["source_wallet"], []).append(r)
    ranked = []
    for w, rs in wallets.items():
        if len(rs) >= MIN_WALLET_N:
            tp, sl, avg = best(rs)
            ranked.append((st.mean(sim(r, 0.25, 0.08) for r in rs), w, rs[0]["label"], len(rs), tp, sl, avg))
    if ranked:
        lines.append(f"\n<b>Кошельки</b> ({MIN_WALLET_N}+ сигналов), сначала лучшие при G (+25%/−8%):")
        for g, w, label, n, tp, sl, avg in sorted(ranked, reverse=True):
            name = esc(label or f"{w[:4]}…{w[-4:]}")
            lines.append(f"{'✅' if g > 0 else '❌'} {name}: {n} сиг. · G {g:+.1%} · лучше: +{tp:.0%}/−{sl:.0%} → {avg:+.1%}"
                         + (f"\n   <code>{w}</code>" if g <= 0 else ""))
    lines.append("<i>Модель грубее бумаги (минутные свечи): сравнивать варианты между собой. "
                 "Меняем схему только после проверки на портфелях-экспериментах.</i>")
    return "\n".join(lines)
