"""Бумага как реальный бот: покупка через 2 блока после трейдера по цене блокчейна, стоп/тейк по каждой сделке
монеты, продажа через 2 блока после сделки, давшей сигнал."""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

from scanner.analytics import paper as pp
from scanner.analytics.curve import mc_sol
from scanner.models import Trade
from tests.test_paper import bare_trader


def row(slot, mc, trader="X", is_buy=True):
    return SimpleNamespace(slot=slot, mc=mc, ts=time.time(), trader=trader, is_buy=is_buy)


class FakeRpc:
    def __init__(self, sigs):
        self.sigs = sigs
        self.tx_calls = []

    async def signatures_for(self, address, limit=100, before=None, until=None):
        return self.sigs

    async def transaction(self, sig):
        self.tx_calls.append(sig)
        return {"sig": sig}


async def test_block_mc_takes_last_trade_up_to_block(monkeypatch):
    sigs = [{"signature": "s5", "slot": 105}, {"signature": "s2b", "slot": 102}, {"signature": "s2a", "slot": 102},
            {"signature": "s1", "slot": 101, "err": {"x": 1}}]
    rpc = FakeRpc(sigs)
    tr = bare_trader({})
    tr.e.rpc = rpc
    tr.db = SimpleNamespace(fetchval=lambda *a: asyncio.sleep(0, result=None))
    import scanner.ingest.pump_parser as parser

    def fake_parse(tx, sig, slot, source, stats, pool_map=None):
        return [Trade(signature=sig, mint="M", trader="Y", is_buy=True, sol_amount=1, token_amount=1,
                      v_sol=40.0, v_tokens=8e8)] if sig == "s2b" else []
    monkeypatch.setattr(parser, "parse_transaction", fake_parse)
    mc = await tr.block_mc("M", "TRIG", 100, 102, 30.0)
    assert mc == pytest.approx(mc_sol(40.0, 8e8)) and rpc.tx_calls == ["s2b"]   # последняя в блоке 102, не 105
    rpc.sigs = [{"signature": "s9", "slot": 109}]
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) == 30.0                # до блока +2 сделок не было
    tr.e.rpc = None
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) is None


def realtime_trader(monkeypatch, trades):
    monkeypatch.setattr(pp, "ENTRY_RPC_WAIT_S", 0)
    monkeypatch.setattr(pp, "EXIT_WAIT_S", 0)
    monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
    st = SimpleNamespace(mint="M", mc=trades[-1].mc if trades else 30.0, trades=trades)
    tr = bare_trader({"M": st})
    tr.e.fee_bps = 125
    tr.lock = asyncio.Lock()
    sold = []

    async def no_msg(text):
        return None

    async def fake_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        fill = pp.buy_fill(mc, 0.1, 125, 0.0005)
        pid = len(tr.positions) + 1
        tr.positions[pid] = pp.Position(id=pid, portfolio=name, mint=mint, opened_ts=time.time(), entry_mc=mc,
                                        cost_sol=0.1, tokens=fill.tokens, tokens_initial=fill.tokens, target_mc=None,
                                        trader=trader, max_mc=mc, extra={"src": src, "last_mc": mc})
        return 0.1

    async def fake_sell(p, reason, share, mc):
        sold.append((p.portfolio, reason, mc))
        tr.positions.pop(p.id, None)
    tr._msg, tr._open, tr._sell = no_msg, fake_open, fake_sell
    return tr, st, sold


def signal(**kw):
    return {"modes": ["smart"], "symbol": "T", "mc": 30.0, "trader": "W", "trigger_slot": 100, "trigger_sig": "TRIG",
            "trigger_mc": 30.0, "trigger_at": time.time(), **kw}


async def test_realtime_entry_and_stop_exit_two_blocks_later(monkeypatch):
    trades = [row(101, 31.0), row(103, 34.0), row(104, 29.0), row(105, 27.0), row(107, 40.0)]
    tr, st, sold = realtime_trader(monkeypatch, trades)

    async def block(mint, sig, s0, target, trig_mc):
        assert (s0, target) == (100, 102)
        return 33.0
    tr.block_mc = block
    tr.route = lambda data, level: ({"G"}, "")
    await tr.on_alert("M", "normal", None, signal(), "W")
    await asyncio.sleep(0.05)
    # вход по цене блока +2 (33); стоп −8% пробит сделкой блока 104 (29) — продажа по последней сделке к блоку 106 (27)
    assert sold == [("G", "stop", 27.0)]


async def test_entry_falls_back_to_trader_price_plus_slip(monkeypatch):
    tr, st, sold = realtime_trader(monkeypatch, [])
    opened = []

    async def none_block(*a):
        return None

    async def rec_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append((mc, src))
        return None
    tr.block_mc, tr._open = none_block, rec_open
    tr.route = lambda data, level: ({"G"}, "")
    await tr.on_alert("M", "normal", None, signal(), "W")
    assert opened == [(pytest.approx(30.0 * 1.02), "оценка")]
    opened.clear()
    await tr.on_alert("M", "normal", None, signal(trigger_at=time.time() - 60), "W")   # покупка давняя — старый путь
    assert not opened or opened[0][1] != "оценка"


async def test_each_trade_checks_exit_and_trader_sell(monkeypatch):
    tr, st, sold = realtime_trader(monkeypatch, [row(100, 30.0)])
    await tr._open("G", "M", "normal", None, {}, "W", 30.0, "блок")
    p = next(iter(tr.positions.values()))
    p.extra["entry_slot"] = 102
    st.trades.append(row(101, 20.0))
    st.mc = 20.0
    await tr.on_trade(st, None, "X", False, 101)          # сделка до нашего блока — не считается
    await asyncio.sleep(0.02)
    assert not sold
    st.trades.append(row(103, 31.0))
    st.mc = 31.0
    await tr.on_trade(st, None, "W", False, 103)          # трейдер продал — выходим через 2 блока
    st.trades.append(row(105, 32.0))
    await asyncio.sleep(0.05)
    assert sold == [("G", "trader_sell", 32.0)]


def test_trigger_from_trade_row():
    from scanner.analytics.engine import trigger_of
    t = trigger_of({"slot": 5, "signature": "S", "mc_sol": 40.0})
    assert t["trigger_slot"] == 5 and t["trigger_sig"] == "S" and t["trigger_mc"] == 40.0 and t["trigger_at"] > 0
    assert trigger_of({"slot": None, "signature": "S", "mc_sol": 40.0}) == {}
