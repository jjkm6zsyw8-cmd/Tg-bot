"""Признаки запуска (разд. 3 ТЗ) и итоги запуска для профиля кластера."""
from __future__ import annotations

import hashlib
import re
from typing import Any

from .curve import Costs, trade_net_profit
from .state import TokenState


def ticker_style(symbol: str) -> str:
    s = symbol or ""
    case = "upper" if s.isupper() else "lower" if s.islower() else "mixed"
    digits = "d" if any(c.isdigit() for c in s) else ""
    length = "s" if len(s) <= 4 else "m" if len(s) <= 7 else "l"
    return f"{case}{digits}:{length}"


def description_template(desc: str | None) -> str | None:
    """Шаблон описания: слова → w, числа → n, ссылки → u; хэш структуры."""
    if not desc:
        return None
    t = re.sub(r"https?://\S+", "u", desc.lower())
    t = re.sub(r"\d+", "n", t)
    t = re.sub(r"[^\W\d_]+", "w", t)
    t = re.sub(r"\s+", " ", t).strip()
    return hashlib.sha1(t.encode()).hexdigest()[:12]


def metadata_style(state: TokenState) -> dict[str, Any]:
    m = state.meta
    return {
        "ticker_len": len(state.symbol or ""),
        "ticker_style": ticker_style(state.symbol),
        "has_twitter": bool(m.get("twitter")),
        "has_telegram": bool(m.get("telegram")),
        "has_website": bool(m.get("website")),
        "desc_template": description_template(m.get("description")),
    }


def creation_mc(state: TokenState) -> float:
    """MC сразу после транзакции создания (включая покупку дева)."""
    return state.creation_mc()


def achievable(state: TokenState, entry_delay_s: float, reaction_s: float, position_sol: float,
               costs: Costs) -> tuple[float | None, float | None]:
    """Достижимый профит пользователя: вход через entry_delay_s после создания,
    выход по максимуму MC до (дамп − реакция). Возвращает (профит, MC входа)."""
    t_entry = state.created_ts + entry_delay_s
    if not state.trades or state.trades[-1].ts < t_entry:
        return None, None
    entry = state.mc_at(t_entry)
    end = (state.big_sell_ts - reaction_s) if state.big_sell_ts else state.trades[-1].ts
    if end <= t_entry:
        return trade_net_profit(entry, state.mc_at(t_entry + reaction_s), position_sol, costs), entry
    best = state.max_mc_between(t_entry, end)
    return trade_net_profit(entry, best, position_sol, costs), entry


def launch_summary(state: TokenState, cfg: dict[str, Any], costs: Costs, bot_latency_s: float = 1.5) -> dict[str, Any]:
    """Строка таблицы launches по завершённому (или текущему) запуску."""
    init = creation_mc(state)
    peak_hold = state.cluster_peak_hold or 0.0
    reaction = float(cfg.get("reaction_s", 5))
    prof, entry = achievable(state, bot_latency_s + reaction, reaction, float(cfg.get("position_sol", 0.5)), costs)
    bundles = state.bundle_wallets()
    first_slot_buy = {t.trader for t in state.trades if t.is_buy and state.created_slot is not None
                      and t.slot == state.created_slot}
    return {
        "initial_mc": init,
        "peak_mc": state.peak_mc,
        "dump_mc": state.dump_mc,
        "time_to_dump_s": max(0.0, state.big_sell_ts - state.created_ts) if state.big_sell_ts else None,
        "trades_count": state.trades_count(),
        "sold_share": state.sold_share_at_dump,
        "cluster_share_start": peak_hold / 1_000_000_000 if peak_hold else state.share(state.cluster_wallets()),
        "early_buyers": state.early_buyers(),
        "achievable_profit": prof,
        "entry_mc": entry,
        "mc_30s": state.mc_at_age(30),
        "mc_60s": state.mc_at_age(60),
        "mc_120s": state.mc_at_age(120),
        "dev_buy_sol": state.dev_buy_sol(),
        "bundle_wallets": len(bundles),
        "bundle_share": state.share(bundles),
        "migrated": state.migrated,
        "features": {
            **metadata_style(state),
            "image": state.meta.get("image"),
            "twitter": state.meta.get("twitter"),
            "website": state.meta.get("website"),
            "first_slot_buyers": sorted(first_slot_buy)[:50],
            "cluster_wallets": sorted(state.cluster_wallets())[:100],
            "early_growth_rate": early_growth_rate(state, init),
        },
    }


def early_growth_rate(state: TokenState, init: float, horizon_s: float = 30.0) -> float | None:
    """Относительный рост MC в секунду в первые horizon_s (для оценки ΔMC за время реакции)."""
    mc = state.mc_at_age(horizon_s)
    if not mc or init <= 0:
        return None
    if state.big_sell_ts and state.big_sell_ts - state.created_ts < horizon_s:
        mc = state.max_mc_between(state.created_ts, state.big_sell_ts)
        horizon_s = max(1.0, state.big_sell_ts - state.created_ts)
    if mc <= init:
        return 0.0
    return (mc / init) ** (1 / horizon_s) - 1
