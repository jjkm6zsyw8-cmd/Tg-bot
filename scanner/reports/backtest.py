"""/backtest: прогон настроек за период, сравнение двух наборов порогов рядом.

Пороги подбираются на первой половине периода и проверяются на второй;
отчёт показывает оба результата (защита от подгонки под прошлое).
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from itertools import product
from typing import Any

from ..alerts.format import pct
from ..analytics.curve import Costs
from ..config import Settings
from ..db.pool import Database
from .sim import Prepared, run_dev, series, simulate_smart

GRID = {"min_net_profit": [0.2, 0.3, 0.5, 0.75, 1.0], "min_repeatability": [0.5, 0.6, 0.7]}


def parse_period(text: str, now: datetime | None = None) -> tuple[datetime, datetime]:
    now = now or datetime.now(timezone.utc)
    text = (text or "7d").strip()
    m = re.fullmatch(r"(\d+)\s*([dhд])", text)
    if m:
        n = int(m.group(1))
        delta = timedelta(hours=n) if m.group(2) == "h" else timedelta(days=n)
        return now - delta, now
    m = re.fullmatch(r"(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})", text)
    if m:
        a = datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc)
        b = datetime.fromisoformat(m.group(2)).replace(tzinfo=timezone.utc) + timedelta(days=1)
        return a, b
    raise ValueError("Период: 7d, 48h или 2026-10-01..2026-10-14")


def parse_args(args: str) -> tuple[str, dict[str, Any], dict[str, Any] | None]:
    """'14d min_net_profit=0.5 vs min_net_profit=0.3' → период, набор A, набор B."""
    parts = (args or "").split()
    period = parts[0] if parts and "=" not in parts[0] else "7d"
    rest = parts[1:] if parts and "=" not in parts[0] else parts
    a: dict[str, Any] = {}
    b: dict[str, Any] | None = None
    cur = a
    for p in rest:
        if p.lower() == "vs":
            b = {}
            cur = b
            continue
        if "=" in p:
            k, v = p.split("=", 1)
            try:
                cur[k] = float(v)
            except ValueError:
                cur[k] = v
    return period, a, b


def _split(prepared: list[Prepared], mid: datetime) -> tuple[list[Prepared], list[Prepared]]:
    return ([p for p in prepared if p.launch["created_at"] < mid], [p for p in prepared if p.launch["created_at"] >= mid])


def calibrate(train: list[Prepared], cfg: dict[str, Any], costs: Costs, latency: float) -> dict[str, Any]:
    best, best_s = {}, None
    for vals in product(*GRID.values()):
        over = dict(zip(GRID.keys(), vals))
        s = series(run_dev(train, {**cfg, **over}, costs, latency))
        if s["n"] < 5:
            continue
        if best_s is None or s["series"] > best_s["series"]:
            best, best_s = over, s
    return best


async def run_backtest(db: Database, settings: Settings, args: str) -> str:
    from .sim import prepare_dev
    period, over_a, over_b = parse_args(args)
    start, end = parse_period(period)
    base = settings.effective()
    costs = Costs.from_config(settings.costs())
    latency = float(((await db.kv_get("health:analytics", {})) or {}).get("latency_median_s") or 1.5)
    prepared = await prepare_dev(db, base, start, end)
    mid = start + (end - start) / 2
    train, test = _split(prepared, mid)
    cfg_a = {**base, **over_a}
    calibrated = False
    if over_b is None:
        over_b = calibrate(train, base, costs, latency)
        calibrated = True
    cfg_b = {**base, **over_b}

    def row(cfg: dict[str, Any]) -> tuple[dict[str, float], dict[str, float]]:
        return series(run_dev(train, cfg, costs, latency)), series(run_dev(test, cfg, costs, latency))

    a1, a2 = row(cfg_a)
    b1, b2 = row(cfg_b)
    outage = sum(1 for p in prepared if p.outage)

    def fmt_s(s: dict[str, float]) -> str:
        if not s["n"]:
            return "нет сделок"
        return (f"{s['n']} сделок, удачных {s['win_rate']:.0%}, +{s['avg_win']:.0%}/−{s['avg_loss']:.0%}, "
                f"<b>итог {pct(s['series'])}</b>")

    def desc(o: dict[str, Any]) -> str:
        return ", ".join(f"{k}={v}" for k, v in o.items()) or "текущие настройки"

    lines = [
        f"🧪 <b>Бэктест режима «Девы»</b> {start:%d.%m %H:%M} – {end:%d.%m %H:%M} UTC",
        f"Запусков {len(prepared)}, с профилем кластера {sum(1 for p in prepared if p.profile)}; "
        f"сбой источника: {outage} (не входят в статистику). Задержка бота {latency:.1f} с, "
        f"реакция {base.get('reaction_s', 5)} с, позиция {base.get('position_sol', 0.5)} SOL.",
        "",
        f"<b>A</b> ({desc(over_a)})",
        f"  подбор (1-я половина): {fmt_s(a1)}",
        f"  проверка (2-я половина): {fmt_s(a2)}",
        f"<b>B</b> ({desc(over_b)}{' — подобрано на 1-й половине' if calibrated else ''})",
        f"  подбор (1-я половина): {fmt_s(b1)}",
        f"  проверка (2-я половина): {fmt_s(b2)}",
    ]
    smart = series(await simulate_smart(db, cfg_a, costs, start, end))
    lines += ["", f"<b>Смарт-мани</b> (текущий список трейдеров, набор A): {fmt_s(smart)}"]
    if a2["n"] and a2["series"] < 0 and b2["series"] < 0:
        lines.append("\n⚠️ Итог серии на проверочном периоде отрицательный при обоих наборах — "
                     "стратегию нужно пересмотреть до включения алертов (точка решения после этапа 4).")
    return "\n".join(lines)
