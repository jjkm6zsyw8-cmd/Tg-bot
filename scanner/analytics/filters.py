"""Фильтры безопасности монеты (разд. 7 ТЗ).

Определения бандлов, снайперов и инсайдеров совпадают с теми, что используют
Axiom и Jupiter, чтобы цифры можно было сверить с терминалом.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

LABELS = {
    "cluster": "Кластер",
    "bundles": "Бандлы",
    "snipers": "Снайперы",
    "insiders": "Инсайдеры",
    "top10": "Топ-10",
    "fresh_wallets": "Свежие",
    "growth_1m": "Рост 1м",
    "wash_volume": "Накрутка",
}


@dataclass
class FilterResult:
    passed: bool
    violations: list[str]
    values: dict[str, Any]
    limits: dict[str, float]


def check(metrics: dict[str, Any], limits: dict[str, float], mode: str, blacklisted: bool = False) -> FilterResult:
    """mode='dev': доля кластера — часть паттерна, не фильтр; mode='smart': кластер до cluster_share_smart."""
    lim = {
        "bundles": limits.get("bundles", 0.20),
        "snipers": limits.get("snipers", 0.15),
        "insiders": limits.get("insiders", 0.10),
        "top10": limits.get("top10", 0.30),
        "fresh_wallets": limits.get("fresh_wallets", 0.40),
        "growth_1m": limits.get("growth_1m", 1.0),
        "wash_volume": limits.get("wash_volume", 0.30),
    }
    if mode == "smart":
        lim["cluster"] = limits.get("cluster_share_smart", 0.10)
    violations = [k for k, v in lim.items() if metrics.get(k) is not None and metrics[k] > v]
    if blacklisted:
        violations.insert(0, "blacklist")
    return FilterResult(passed=not violations, violations=violations, values=metrics, limits=lim)


def which_would_block(metrics: dict[str, Any], presets: dict[str, dict[str, float]], mode: str) -> list[str]:
    """Для отметки «Провал»/«Скам»: какой фильтр и в каком пресете отсеял бы монету."""
    out = []
    for name in ("aggressive", "normal", "cautious"):
        res = check(metrics, presets.get(name, {}), mode)
        if not res.passed:
            parts = [f"{LABELS.get(v, v)} {metrics[v]:.0%} > {res.limits[v]:.0%}" for v in res.violations
                     if v in res.limits]
            out.append(f"{name}: " + ", ".join(parts))
    return out


def compact_line(metrics: dict[str, Any], violations: list[str]) -> str:
    """Строка показателей для алерта, нарушенные выделены."""
    parts = []
    for key in ("cluster", "bundles", "snipers", "insiders", "top10", "fresh_wallets"):
        v = metrics.get(key)
        if v is None:
            continue
        txt = f"{LABELS[key]} {v:.0%}"
        parts.append(f"<b><u>⚠{txt}</u></b>" if key in violations else txt)
    prog = metrics.get("progress")
    if prog is not None:
        parts.append(f"Кривая {prog:.0%}")
    return " · ".join(parts)
