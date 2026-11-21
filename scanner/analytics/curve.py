"""Математика bonding curve Pump.fun, MC и издержки сделки (разд. 2 ТЗ).

Кривая — constant product по виртуальным резервам: v_sol * v_tokens = k.
MC = цена * total supply. Все сравнения паттернов идут в SOL.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..solana import INITIAL_REAL_TOKENS, INITIAL_V_SOL, INITIAL_V_TOKENS, TOTAL_SUPPLY


def mc_sol(v_sol: float, v_tokens: float) -> float:
    if v_tokens <= 0:
        return 0.0
    return v_sol / v_tokens * TOTAL_SUPPLY


def reserves_for_mc(target_mc_sol: float, k: float | None = None) -> tuple[float, float]:
    """Виртуальные резервы кривой, при которых MC равен target (для симуляции)."""
    k = k or INITIAL_V_SOL * INITIAL_V_TOKENS
    # mc = v_sol/v_tok*S, v_sol*v_tok = k → v_sol = sqrt(mc*k/S)
    v_sol = (max(target_mc_sol, 1e-9) * k / TOTAL_SUPPLY) ** 0.5
    return v_sol, k / v_sol


def curve_progress(real_tokens: float | None) -> float | None:
    """Доля пути до миграции 0..1."""
    if real_tokens is None:
        return None
    return max(0.0, min(1.0, 1 - real_tokens / INITIAL_REAL_TOKENS))


def buy_tokens_out(v_sol: float, v_tokens: float, sol_in: float) -> float:
    k = v_sol * v_tokens
    return v_tokens - k / (v_sol + sol_in)


def sell_sol_out(v_sol: float, v_tokens: float, tokens_in: float) -> float:
    k = v_sol * v_tokens
    return v_sol - k / (v_tokens + tokens_in)


@dataclass(slots=True)
class Costs:
    fee_bps: float = 125.0          # комиссия кривой (протокол + создатель)
    priority_sol: float = 0.0005    # приоритет на транзакцию
    extra_slippage_bps: float = 100.0

    @classmethod
    def from_config(cls, cfg: dict, measured_fee_bps: float | None = None) -> "Costs":
        return cls(
            fee_bps=measured_fee_bps if measured_fee_bps else float(cfg.get("curve_fee_bps", 125)),
            priority_sol=float(cfg.get("priority_fee_sol", 0.0005)),
            extra_slippage_bps=float(cfg.get("extra_slippage_bps", 100)),
        )


def entry_cost_fraction(mc: float, position_sol: float, costs: Costs) -> float:
    """f вход: комиссия + приоритет + проскальзывание по состоянию кривой при MC."""
    v_sol, v_tok = reserves_for_mc(mc)
    fee = costs.fee_bps / 10_000
    sol_net = position_sol * (1 - fee)
    got = buy_tokens_out(v_sol, v_tok, sol_net)
    ideal = position_sol / (v_sol / v_tok)
    slip = 1 - got / ideal if ideal > 0 else 0.0
    prio = costs.priority_sol / position_sol if position_sol > 0 else 0.0
    # slip уже включает fee (sol_net), поэтому не складываем его повторно
    total = slip + prio + costs.extra_slippage_bps / 10_000
    return max(0.0, min(0.99, total))


def exit_cost_fraction(mc: float, position_tokens_value_sol: float, costs: Costs) -> float:
    """f выход: продажа позиции стоимостью value SOL по состоянию кривой при MC."""
    v_sol, v_tok = reserves_for_mc(mc)
    price = v_sol / v_tok
    tokens = position_tokens_value_sol / price if price > 0 else 0.0
    gross = sell_sol_out(v_sol, v_tok, tokens)
    fee = costs.fee_bps / 10_000
    got = gross * (1 - fee)
    slip = 1 - got / position_tokens_value_sol if position_tokens_value_sol > 0 else 0.0
    prio = costs.priority_sol / position_tokens_value_sol if position_tokens_value_sol > 0 else 0.0
    total = slip + prio + costs.extra_slippage_bps / 10_000
    return max(0.0, min(0.99, total))


def potential(mc_dump: float, mc_alert: float, delta_mc_reaction: float, k: float = 0.8) -> float:
    """Потенциал = k × MC дампа / (MC алерта + ΔMC за время реакции)."""
    entry = mc_alert + max(0.0, delta_mc_reaction)
    if entry <= 0:
        return 0.0
    return k * mc_dump / entry


def net_profit(pot: float, f_in: float, f_out: float) -> float:
    """Чистый профит = Потенциал × (1 − f вход) × (1 − f выход) − 1."""
    return pot * (1 - f_in) * (1 - f_out) - 1


def trade_net_profit(entry_mc: float, exit_mc: float, position_sol: float, costs: Costs) -> float:
    """Чистый результат сделки вход→выход по MC с учётом всех издержек."""
    if entry_mc <= 0 or exit_mc <= 0:
        return -1.0
    f_in = entry_cost_fraction(entry_mc, position_sol, costs)
    value_at_exit = position_sol * (1 - f_in) * exit_mc / entry_mc
    f_out = exit_cost_fraction(exit_mc, value_at_exit, costs)
    return net_profit(exit_mc / entry_mc, f_in, f_out)
