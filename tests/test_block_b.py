"""Блок B: продажи по группам, окна входа, разброс дампа, лучшие трейдеры."""
from __future__ import annotations

import pytest

from scanner.analytics.curve import Costs
from scanner.analytics.insights import (dump_spread, entry_curve, entry_window, sell_sequences, top_traders,
                                        typical_sells)
from scanner.analytics.state import TokenState, TradeRow

from .helpers import Curve, addr


def pumped_launch(dev: str, dump_after: float = 120.0) -> TokenState:
    st = TokenState(mint=addr("bm" + dev[:4]), deployer=dev, created_ts=1000.0, created_slot=100)
    c = Curve()
    dev_tok = c.buy(2.0)
    st.add_trade(TradeRow(1000.0, 100, dev, True, 2.0, dev_tok, c.mc, "c"))
    for i in range(20):                                   # толпа поднимает цену
        tok = c.buy(1.5)
        st.add_trade(TradeRow(1003.0 + 5 * i, 110 + i, addr(f"crowd{i}"), True, 1.5, tok, c.mc, f"b{i}"))
    w = addr("crowd3")                                     # один трейдер успевает выйти в плюс
    sol = c.sell(st.balances[w])
    st.add_trade(TradeRow(1000.0 + dump_after - 10, 300, w, False, sol, st.balances[w], c.mc, "w"))
    sol = c.sell(dev_tok * 0.6)
    st.add_trade(TradeRow(1000.0 + dump_after, 400, dev, False, sol, dev_tok * 0.6, c.mc, "d1"))
    sol = c.sell(dev_tok * 0.4)
    st.add_trade(TradeRow(1000.0 + dump_after + 20, 401, dev, False, sol, dev_tok * 0.4, c.mc, "d2"))
    return st


def test_sell_sequences_cluster():
    st = pumped_launch(addr("seqdev"))
    seq = sell_sequences(st)["cluster"]
    assert seq["first"]["t"] == pytest.approx(120.0)
    assert seq["half"]["t"] == pytest.approx(120.0) and seq["full"]["t"] == pytest.approx(140.0)
    assert seq["sold_frac"] == pytest.approx(1.0)
    ts = typical_sells([seq, seq])
    assert ts["first"]["t"] == pytest.approx(120.0)


def test_entry_curve_and_window():
    st = pumped_launch(addr("ewdev"))
    target = 0.8 * st.dump_mc
    curve = entry_curve(st, target, 5, 2, 0.5, Costs())
    assert curve["3"]["net"] > 0.3                         # ранний вход — в плюсе
    assert "120" not in curve                              # после продажи дева входа нет
    win = entry_window([curve, curve, curve], 0.3)
    assert win and win["t_from"] == 3 and win["mc_to"] >= win["mc_from"]


def test_dump_spread_and_top_traders():
    ds = dump_spread([50, 60, 70, 80, 90])
    assert ds["median"] == 70 and ds["p25"] == 60 and ds["p75"] == 80
    st = pumped_launch(addr("ttdev"))
    excl = st.cluster_wallets() | st.bundle_wallets() | st.sniper_wallets()
    tops = top_traders(st, excl)
    assert tops and tops[0]["trader"] == addr("crowd3") and tops[0]["pnl_sol"] > 0
    assert all(t["trader"] != st.deployer for t in tops)
