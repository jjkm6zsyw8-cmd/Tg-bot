"""Режим «Новые монеты»: каждая свежая монета проверяется при каждой сделке.

Алерт уходит, когда монета проходит фильтры активности и безопасности: живые покупатели,
объём, перевес покупок, дев не продаёт и держит немного, нет перекоса бандлов/снайперов.
Значения по умолчанию задаются здесь и перекрываются ключами new_* в config.yaml или /settings.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .filters import check
from .state import TokenState

DEFAULTS: dict[str, float] = {
    "new_window_min": 15,        # сколько минут после запуска монета проверяется
    "new_min_age_s": 20,         # раньше — слишком мало данных
    "new_mc_min_sol": 35,        # ≈ $3–4k
    "new_mc_max_sol": 400,
    "new_min_buyers": 15,        # уникальных покупателей
    "new_min_volume_sol": 8,     # объём с запуска
    "new_min_buy_share_1m": 0.55,  # доля покупок в объёме за последнюю минуту
    "new_max_dev_share": 0.10,   # доля кластера дева
}

REASONS = {
    "age": "слишком рано",
    "old": "окно проверки прошло",
    "mc": "MC вне диапазона",
    "buyers": "мало покупателей",
    "volume": "мало объёма",
    "pressure": "продают больше, чем покупают",
    "dev_sold": "дев продаёт",
    "dev_share": "дев держит много",
    "filters": "не прошла фильтры безопасности",
    "blacklist": "дев в чёрном списке",
}


def param(cfg: dict[str, Any], key: str) -> float:
    v = cfg.get(key)
    return float(v) if v is not None else DEFAULTS[key]


@dataclass
class NewCoinCheck:
    ok: bool
    reason: str = ""
    buyers: int = 0
    volume: float = 0.0
    buy_share: float = 0.0
    metrics: dict[str, Any] = field(default_factory=dict)
    violations: list[str] = field(default_factory=list)


def evaluate_new(st: TokenState, now: float, cfg: dict[str, Any], limits: dict[str, float],
                 blacklisted: bool = False) -> NewCoinCheck:
    age = now - st.created_ts
    if age < param(cfg, "new_min_age_s"):
        return NewCoinCheck(False, "age")
    if age > 60 * param(cfg, "new_window_min"):
        return NewCoinCheck(False, "old")
    if blacklisted:
        return NewCoinCheck(False, "blacklist")
    if not (param(cfg, "new_mc_min_sol") <= st.mc <= param(cfg, "new_mc_max_sol")):
        return NewCoinCheck(False, "mc")
    buyers = len({t.trader for t in st.trades if t.is_buy and t.trader != st.deployer})
    volume = sum(t.sol for t in st.trades)
    res = NewCoinCheck(False, buyers=buyers, volume=volume)
    if buyers < param(cfg, "new_min_buyers"):
        res.reason = "buyers"
        return res
    if volume < param(cfg, "new_min_volume_sol"):
        res.reason = "volume"
        return res
    recent = [t for t in st.trades if now - t.ts <= 60]
    tot = sum(t.sol for t in recent)
    res.buy_share = sum(t.sol for t in recent if t.is_buy) / tot if tot > 0 else 0.0
    if res.buy_share < param(cfg, "new_min_buy_share_1m"):
        res.reason = "pressure"
        return res
    if st.big_sell_ts is not None or any(not t.is_buy and t.trader == st.deployer for t in st.trades):
        res.reason = "dev_sold"
        return res
    if st.share(st.cluster_wallets()) > param(cfg, "new_max_dev_share"):
        res.reason = "dev_share"
        return res
    res.metrics = st.metrics()
    f = check(res.metrics, limits, "dev")
    res.violations = f.violations
    if not f.passed:
        res.reason = "filters"
        return res
    res.ok = True
    return res
