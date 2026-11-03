"""Решение об алерте по деву: потенциал, чистый профит, уровень (разд. 2–3 ТЗ).

Чистая функция без I/O — одна и та же для живого потока и для бэктеста.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .curve import Costs, entry_cost_fraction, exit_cost_fraction, net_profit, potential
from .profiles import Profile

LEVELS = {"watch": 0, "normal": 1, "strong": 2}
REASONS = {
    "blacklist": "кластер в чёрном списке",
    "little_history": "мало истории",
    "instant_rug": "мгновенные раги",
    "dev_sold": "дев уже продавал",
    "late": "MC уже в зоне дампа",
    "cluster_share": "кластер держит слишком много",
    "repeatability": "низкая повторяемость",
    "max_entry_mc": "MC выше максимума входа",
    "low_profit": "мало чистого профита",
    "low_confidence": "связь низкой уверенности",
    "no_profile": "нет кластера",
    "suspicious": "подозрительный кластер (50+ запусков в сутки)",
    "external_filters": "внешние фильтры безопасности",
}


@dataclass
class Snapshot:
    mc: float
    age_s: float
    cluster_sold: bool
    cluster_share: float
    link_confidence: str = "high"
    link_reason: str = ""
    blacklisted: bool = False
    suspicious: bool = False
    observed_growth_rate: float = 0.0
    threshold_override: float | None = None


@dataclass
class Decision:
    alert: bool
    reason: str = ""
    level: str | None = None
    potential: float = 0.0
    net: float = 0.0
    entry_mc: float = 0.0          # оценка MC входа с учётом реакции
    entry_max_mc: float = 0.0      # «Вход до MC»
    target_mc: float = 0.0
    dump_mc: float = 0.0
    f_in: float = 0.0
    f_out: float = 0.0
    threshold: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)


def evaluate_dev(profile: Profile | None, snap: Snapshot, cfg: dict[str, Any], costs: Costs) -> Decision:
    thr = snap.threshold_override if snap.threshold_override is not None else float(cfg.get("min_net_profit", 0.3))
    d = Decision(alert=False, threshold=thr)
    if snap.blacklisted:
        d.reason = "blacklist"
        return d
    if snap.suspicious:
        d.reason = "suspicious"
        return d
    if profile is None:
        d.reason = "no_profile"
        return d
    if snap.link_confidence == "low":
        d.reason = "low_confidence"
        return d
    if profile.launches < int(cfg.get("min_launches", 3)) or not profile.dump_mc:
        d.reason = "little_history"
        return d
    if profile.instant_rugger:
        d.reason = "instant_rug"
        return d
    if snap.cluster_sold:
        d.reason = "dev_sold"
        return d
    dump = float(profile.dump_mc)
    d.dump_mc = dump
    if snap.mc >= float(cfg.get("dump_zone_silence", 0.7)) * dump:
        d.reason = "late"
        return d
    if snap.cluster_share > float(cfg.get("max_cluster_share", 0.35)):
        d.reason = "cluster_share"
        return d
    if profile.repeatability < float(cfg.get("min_repeatability", 0.5)):
        d.reason = "repeatability"
        return d

    reaction = float(cfg.get("reaction_s", 5))
    rate = max(profile.growth_rate or 0.0, snap.observed_growth_rate)
    rate = min(rate, 0.2)  # защита от выбросов: не больше +20%/с
    delta = snap.mc * ((1 + rate) ** reaction - 1)
    k = float(cfg.get("target_k", 0.8))
    pos = float(cfg.get("position_sol", 0.5))
    entry = snap.mc + delta
    target = k * dump
    pot = potential(dump, snap.mc, delta, k)
    f_in = entry_cost_fraction(entry, pos, costs)
    f_out = exit_cost_fraction(target, pos * pot, costs)
    net = net_profit(pot, f_in, f_out)
    d.potential, d.net, d.entry_mc, d.target_mc, d.f_in, d.f_out = pot, net, entry, target, f_in, f_out
    d.entry_max_mc = target * (1 - f_in) * (1 - f_out) / (1 + thr)
    max_entry = float(cfg.get("max_entry_mc_sol") or 0)
    if max_entry and entry > max_entry:
        d.reason = "max_entry_mc"
        return d
    if net < thr:
        d.reason = "low_profit"
        return d
    strong = (net >= float(cfg.get("strong_potential_mult", 1.5)) * thr
              and profile.repeatability >= float(cfg.get("strong_repeatability", 0.7))
              and snap.link_confidence == "high")
    d.alert = True
    d.level = "strong" if strong else "normal"
    return d


def upgrade(level_a: str | None, level_b: str | None) -> str | None:
    if level_a is None:
        return level_b
    if level_b is None:
        return level_a
    return level_a if LEVELS[level_a] >= LEVELS[level_b] else level_b


def bump(level: str) -> str:
    """Повышение уровня при совпадении двух режимов на одной монете."""
    return {"watch": "normal", "normal": "strong", "strong": "strong"}[level]
