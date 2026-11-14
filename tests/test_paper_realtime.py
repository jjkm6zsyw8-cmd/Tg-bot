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
    assert mc == pytest.approx(mc_sol(40.0, 8e8)) and rpc.tx_calls == ["s2b", "s2a"]   # блок 102, не 105
    rpc.sigs = [{"signature": "s9", "slot": 109}]
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) == 30.0                # до блока +2 сделок не было
    # запуск: других сделок до блока +2 нет — цена из самой транзакции создания (покупка дева)
    rpc.sigs = []

    def parse_create(tx, sig, slot, source, stats, pool_map=None):
        return [Trade(signature=sig, mint="M", trader="DEV", is_buy=True, sol_amount=1, token_amount=1,
                      v_sol=32.0, v_tokens=9e8)] if sig == "CREATE" else []
    monkeypatch.setattr(parser, "parse_transaction", parse_create)
    assert await tr.block_mc("M", "CREATE", 100, 102, None, with_trigger=True) == pytest.approx(mc_sol(32.0, 9e8))
    tr.e.rpc = None
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) is None


class NullDb:
    def __init__(self):
        self.sql = []

    async def execute(self, sql, *a):
        self.sql.append((sql, a))

    async def fetchrow(self, sql, *a):
        self.sql.append((sql, a))
        return None

    async def fetchval(self, sql, *a):
        return None


def realtime_trader(monkeypatch, trades):
    monkeypatch.setattr(pp, "ENTRY_RPC_WAIT_S", 0)
    monkeypatch.setattr(pp, "ENTRY_RETRY_S", (0.0, 0.0))
    monkeypatch.setattr(pp, "ENTRY_VERIFY_S", 0.0)
    monkeypatch.setattr(pp, "EXIT_WAIT_S", 0)
    monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
    st = SimpleNamespace(mint="M", mc=trades[-1].mc if trades else 30.0, trades=trades)
    tr = bare_trader({"M": st})
    tr.db = NullDb()
    tr.e.cfg = {}                                  # по умолчанию: вход только как реальный бот
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


async def test_pumped_block_price_is_kept(monkeypatch):
    """12.11: за 2 блока монета ×4 — это настоящая цена входа (раньше отбрасывалась и бралась цена трейдера +2%)."""
    tr, st, sold = realtime_trader(monkeypatch, [])
    opened = []

    async def rec_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append((mc, src))
        return None

    async def pumped(*a):
        return 120.0                                       # ×4 к цене трейдера 30
    tr._open, tr.block_mc = rec_open, pumped
    tr.route = lambda data, level: ({"G"}, "")
    await tr.on_alert("M", "normal", None, signal(), "W")
    assert opened == [(120.0, "блок")]


async def test_no_entry_without_block_price(monkeypatch):
    """13.11: 42% входов шли по угаданной цене («рынок сейчас», «цена трейдера +2%») и искажали итог —
    теперь только цена блока +2: не узнали (после повторов) — сделки нет, причина считается."""
    tr, st, sold = realtime_trader(monkeypatch, [])
    opened, calls = [], []

    async def rec_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append((mc, src))
        return None

    async def none_block(*a):
        calls.append(a)
        tr._why(a[0], a[1], "RPC отвечает 429 (лимит)")
        return None

    async def market(mint):
        return 75.0, "jup"
    tr.block_mc, tr._open, tr.market_mc = none_block, rec_open, market
    tr.route = lambda data, level: ({"G"}, "")
    await tr.on_alert("M", "normal", None, signal(), "W")
    assert opened == [] and len(calls) == 3                       # первая попытка и 2 повтора
    assert "цена блока +2 не получена (RPC отвечает 429 (лимит))" in tr.skipped["*"]
    assert tr.no_block["smart: RPC отвечает 429 (лимит)"] == 1
    # покупка трейдера давняя — по «рынку через 5 с» не входим
    await tr.on_alert("M", "normal", None, signal(trigger_at=time.time() - 60), "W")
    assert opened == [] and tr.no_block["smart: покупка трейдера давняя (>15 с)"] == 1
    # нет покупки в блоке вовсе
    await tr.on_alert("M", "normal", None, {"modes": ["smart"], "mc": 30.0}, "W")
    assert opened == [] and tr.no_block["smart: нет покупки трейдера в блоке"] == 1


async def test_block_price_far_from_trader_is_judged_by_market(monkeypatch):
    tr, st, sold = realtime_trader(monkeypatch, [])
    opened = []

    async def rec_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append((mc, src))
        return None

    async def far(*a):
        return 900.0                                       # ×30 к цене трейдера 30: неверна цена трейдера или блока

    async def market_agrees(mint):
        return 850.0, "jup"
    tr.block_mc, tr._open, tr.market_mc = far, rec_open, market_agrees
    tr.route = lambda data, level: ({"G"}, "")
    await tr.on_alert("M", "normal", None, signal(), "W")
    assert opened == [(900.0, "блок")]                     # рынок подтвердил цену блока — неверной была цена трейдера

    async def market_disagrees(mint):
        return 31.0, "jup"
    tr.market_mc = market_disagrees
    opened.clear()
    await tr.on_alert("M", "normal", None, signal(), "W")
    assert opened == [] and "не сходится" in tr.skipped["*"]


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


def test_swing_route_scheme_and_holder():
    from scanner.analytics.copytrade import swing_like
    tr = bare_trader({})
    assert tr.route({"modes": ["swing"], "source_wallet": "W"}, "normal")[0] == {"R"}
    from tests.test_paper import pos
    r = pp.SCHEMES["R"]
    p = pos()
    p.portfolio = "R"
    assert pp.decide_exit(p, 25.0, 10, r, 125, 0.0005) is None                  # −37% — держим (стоп −50%)
    assert pp.decide_exit(p, 19.0, 10, r, 125, 0.0005) == ("stop", 1.0)
    assert pp.decide_exit(p, 120.0, 20, r, 125, 0.0005) is None                 # ×3 — трейлинг включился
    assert pp.decide_exit(p, 75.0, 30, r, 125, 0.0005) == ("trailing", 1.0)    # −37% от пика 120
    assert pp.decide_exit(p, 41.0, 24 * 3600 + 1, r, 125, 0.0005)[0] in ("max_hold", "trailing")
    assert swing_like({"median_hold_s": 3600}) and not swing_like({"median_hold_s": 600})
    assert not swing_like({"median_hold_s": 3600, "linked_to_dev": True})


async def test_engine_swing_buy_goes_to_paper(monkeypatch):
    from scanner.analytics.engine import Engine
    calls = []

    class Paper:
        async def on_alert(self, mint, level, aid, data, trader):
            calls.append((mint, data["modes"], data.get("trigger_slot")))
    eng = SimpleNamespace(cfg={}, paper=Paper())
    st = SimpleNamespace(deployer="DEV", symbol="X", mc=500.0, mint="MX",
                         trades=[SimpleNamespace(trader="W", is_buy=True)])
    r = {"trader": "W", "sol_amount": 1.0, "slot": 10, "signature": "S", "mc_sol": 500.0}
    Engine._on_swing_buy(eng, st, r)  # type: ignore[arg-type]
    await asyncio.sleep(0.01)
    assert calls == [("MX", ["swing"], 10)]
    st.trades.append(SimpleNamespace(trader="W", is_buy=True))                  # докупка — не входим повторно
    Engine._on_swing_buy(eng, st, r)  # type: ignore[arg-type]
    await asyncio.sleep(0.01)
    assert len(calls) == 1


async def test_daily_reset_keeps_swing_positions():
    tr = bare_trader({})
    tr.lock = asyncio.Lock()
    sqls = []

    class DB:
        async def execute(self, sql, *a):
            sqls.append((sql, a))

        async def kv_set(self, *a):
            return None
    tr.db = DB()
    tr.balance, tr.paused = {n: 0.5 for n in pp.SCHEMES}, {}
    for i, name in enumerate(("A", "R"), 1):
        tr.positions[i] = pp.Position(id=i, portfolio=name, mint="M", opened_ts=0, entry_mc=40.0, cost_sol=0.1,
                                      tokens=1.0, tokens_initial=1.0, target_mc=None)
    await tr.reset(keep={"R"})
    assert [p.portfolio for p in tr.positions.values()] == ["R"] and tr.balance["R"] == 0.5
    assert sqls[0][1] == (["R"],) and not any(a and a[0] == "R" for _s, a in sqls[1:])


async def test_tiny_rebuy_of_trader_exits_two_blocks_later(monkeypatch):
    tr, st, sold = realtime_trader(monkeypatch, [row(100, 30.0)])
    await tr._open("G", "M", "normal", None, {}, "W", 30.0, "блок")
    p = next(iter(tr.positions.values()))
    p.extra["entry_slot"] = 102
    st.trades.append(row(104, 33.0, trader="W"))
    st.mc = 33.0
    await tr.on_trade(st, None, "W", True, 104, 0.5)        # крупная докупка — держим
    await asyncio.sleep(0.02)
    assert not sold
    st.trades.append(row(106, 32.0, trader="W"))
    st.mc = 32.0
    await tr.on_trade(st, None, "W", True, 106, 0.02)       # мелкая докупка ($1.7) — выходим
    st.trades.append(row(108, 29.0))
    await asyncio.sleep(0.05)
    assert sold == [("G", "tiny_rebuy", 29.0)]


async def test_mydev_entry_uses_dev_buy_in_create_block(monkeypatch):
    """12.11: цена в событии создания неверная (0.02 SOL) — вход только по сделке блокчейна (создание + покупка дева)."""
    tr, st, sold = realtime_trader(monkeypatch, [])
    opened, asked = [], []

    async def rec_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append((name, mc, src))
        return None

    async def chain(mint, sig, s0, target, trig_mc, with_trigger=False):
        asked.append((trig_mc, with_trigger))
        return 34.0                                         # цена после покупки дева из транзакции создания
    tr._open, tr.block_mc = rec_open, chain
    data = {"modes": ["mydev"], "symbol": "L", "mc": 0.02, "trigger_slot": 500, "trigger_sig": "CREATE",
            "trigger_mc": 0.02, "trigger_at": time.time(), "trigger_create": True}
    await tr.on_alert("M", "normal", None, data, "DEV")
    assert opened == [("Y", 34.0, "блок")] and asked == [(None, True)]
    opened.clear()

    async def nothing(*a, **k):
        return None
    tr.block_mc = nothing                                   # из блокчейна не узнали — не входим вовсе
    await tr.on_alert("M2", "normal", None, data, "DEV")
    assert opened == [] and "цена блока +2 не получена" in tr.skipped["*"]


async def test_block_mc_reasons_and_node_lag(monkeypatch):
    """Почему цену блока не узнали — пишется причина; пустой ответ верен, только если узел уже дошёл до блока +2."""
    tr = bare_trader({})
    tr.db = SimpleNamespace(fetchval=lambda *a: asyncio.sleep(0, result=None))

    class LagRpc(FakeRpc):
        def __init__(self, slot):
            super().__init__([])
            self.slot = slot

        async def call(self, method, params):
            assert method == "getSlot"
            return self.slot
    tr.e.rpc = LagRpc(101)                                   # узел ещё на блоке 101 — сделки 102 мог не отдать
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) is None
    assert tr._why_pop("M", "TRIG") == "узел ещё не дошёл до блока +2"
    tr.e.rpc = LagRpc(103)                                   # узел дальше — сделок правда не было
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) == 30.0

    class BadRpc(FakeRpc):
        async def signatures_for(self, *a, **k):
            raise RuntimeError("getSignaturesForAddress: rate limited")
    tr.e.rpc = BadRpc([])
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) is None
    assert tr._why_pop("M", "TRIG") == "RPC отвечает 429 (лимит)"
    # общий лимит аналитики исчерпан — у цены входа свой запас; кончился и он — причина «лимит»
    tr.e.rpc = LagRpc(103)
    tr.e.rpc_over_budget = lambda: True
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) == 30.0
    tr.e.cfg = {"paper_entry_rpc_daily": 0}
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) is None
    assert tr._why_pop("M", "TRIG") == "дневной лимит запросов RPC"


async def test_block_mc_pages_past_1000_newer(monkeypatch):
    """Горячая монета: 1000 транзакций новее блока +2 — листаем дальше, а не считаем, что сделок не было."""
    newer = [{"signature": f"n{i}", "slot": 200} for i in range(1000)]

    class PagedRpc(FakeRpc):
        async def signatures_for(self, address, limit=100, before=None, until=None):
            return newer if before is None else [{"signature": "s2", "slot": 102}]
    rpc = PagedRpc([])
    tr = bare_trader({})
    tr.e.rpc = rpc
    tr.db = SimpleNamespace(fetchval=lambda *a: asyncio.sleep(0, result=None))
    import scanner.ingest.pump_parser as parser
    monkeypatch.setattr(parser, "parse_transaction", lambda tx, sig, slot, src, stats, pool_map=None: [
        Trade(signature=sig, mint="M", trader="Y", is_buy=True, sol_amount=1, token_amount=1, v_sol=40.0,
              v_tokens=8e8)] if sig == "s2" else [])
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) == pytest.approx(mc_sol(40.0, 8e8))


class BalanceDb:
    def __init__(self, closed_row=None, reset_at=None):
        self.closed_row, self.reset_at, self.sql = closed_row, reset_at, []

    async def execute(self, sql, *a):
        self.sql.append((sql, a))

    async def fetchrow(self, sql, *a):
        self.sql.append((sql, a))
        return self.closed_row

    async def fetchval(self, sql, *a):
        return self.reset_at


async def test_entry_verify_marks_suspect_and_voids_balance(monkeypatch):
    """Сверка через 30 с не сошлась больше чем на 10% — сделка ⚠, её результат убирается из баланса."""
    from datetime import datetime, timedelta, timezone
    monkeypatch.setattr(pp, "ENTRY_VERIFY_S", 0.0)
    tr = bare_trader({})
    now = datetime.now(timezone.utc)
    tr.db = BalanceDb({"portfolio": "A", "pnl_sol": 0.5, "closed_at": now}, reset_at=now - timedelta(hours=1))
    tr.balance = {"A": 2.0}

    async def verify_price(*a, **k):
        return 40.0                                          # вход был по 30 — расхождение 25%
    tr.block_mc = verify_price
    await tr._verify_entry([7], "M", "TRIG", 100, 102, 30.0, False, 30.0)
    assert tr.verified["не сошлась"] == 1
    assert any("entry_verify_mc" in q for q, _a in tr.db.sql) and any("suspect=true" in q for q, _a in tr.db.sql)
    assert tr.balance["A"] == pytest.approx(1.5)             # фальшивые +0.5 SOL больше не увеличивают ставки
    # открытая позиция: ⚠ при закрытии
    tr.positions[8] = pp.Position(id=8, portfolio="A", mint="M", opened_ts=time.time(), entry_mc=30.0, cost_sol=0.1,
                                  tokens=1.0, tokens_initial=1.0, target_mc=None, trader="W", max_mc=30.0, extra={})
    await tr._verify_entry([8], "M", "TRIG", 100, 102, 30.0, False, 30.0)
    assert tr.positions[8].extra["suspect"] is True
    # сошлась — ничего не меняем
    tr.db.sql.clear()

    async def same(*a, **k):
        return 31.0
    tr.block_mc = same
    await tr._verify_entry([9], "M", "TRIG", 100, 102, 30.0, False, 30.0)
    assert tr.verified["сошлась"] == 1 and not any("suspect=true" in q for q, _a in tr.db.sql)


async def test_void_pnl_skips_after_reset():
    from datetime import datetime, timedelta, timezone
    tr = bare_trader({})
    now = datetime.now(timezone.utc)
    tr.db = BalanceDb(reset_at=now)
    tr.balance = {"A": 2.0}
    await tr.void_pnl("A", 0.5, now - timedelta(hours=2))    # сделка до сброса — баланс уже новый
    assert tr.balance["A"] == 2.0
    await tr.void_pnl("A", -0.3, now + timedelta(seconds=1)) # фальшивый минус тоже убирается
    assert tr.balance["A"] == pytest.approx(2.3)


async def test_block_mc_finds_pumpswap_pool_in_tx(monkeypatch):
    """Монета на PumpSwap, пула нет в базе — пул берётся из транзакции, только если он к SOL."""
    usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    class TxRpc(FakeRpc):
        def __init__(self, quote):
            super().__init__([{"signature": "s2", "slot": 102}])
            self.quote = quote

        async def transaction(self, sig):
            return {"meta": {"postTokenBalances": [{"mint": "M", "owner": "POOL"}, {"mint": self.quote, "owner": "POOL"},
                                                   {"mint": "M", "owner": "USER"}]}}
    tr = bare_trader({})
    tr.db = SimpleNamespace(fetchval=lambda *a: asyncio.sleep(0, result=None))      # пула в базе нет
    import scanner.ingest.pump_parser as parser

    def parse(tx, sig, slot, src, stats, pool_map=None):
        if (pool_map or {}).get("POOL") != "M":
            return []
        return [Trade(signature=sig, mint="M", trader="Y", is_buy=True, sol_amount=1, token_amount=1, v_sol=90.0,
                      v_tokens=2e8, pool="pump-amm")]
    monkeypatch.setattr(parser, "parse_transaction", parse)
    tr.e.rpc = TxRpc(pp.WSOL)
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) == pytest.approx(mc_sol(90.0, 2e8))
    tr.e.rpc = TxRpc(usdc)                                         # пул к USDC — не SOL, не берём
    assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) is None
    assert tr._why_pop("M", "TRIG") == "в транзакциях блока нет сделки этой монеты на pump.fun"


async def test_mydev_launch_needs_no_slot_or_event_mc():
    """13.11: запуски «моего дева» без блока или без MC в событии создания пропускались — алерт идёт сразу
    (без ожидания RPC), блок создания бумага узнаёт сама."""
    from scanner.analytics.engine import Engine
    sent = []

    async def compose(mode, mint, level, data, signal=None, trader=None):
        sent.append(data)

    async def log(*a, **k):
        return None
    eng = SimpleNamespace(cfg={}, price=SimpleNamespace(value=100.0), _compose_inner=compose,
                          copyguard=SimpleNamespace(dev_gap_reason=lambda dev: None, log=log))
    st = SimpleNamespace(mc=0.0, deployer="DEV", symbol="L", created_ts=time.time(), cluster_id=None,
                         created_slot=None, mint="M")
    await Engine._my_dev_launch(eng, st, {"address": "DEV"}, {"created_slot": None, "signature": "CREATE"})
    assert sent and sent[0]["trigger_slot"] == 0 and sent[0]["trigger_create"] and sent[0]["trigger_sig"] == "CREATE"


async def test_mydev_entry_resolves_creation_slot(monkeypatch):
    tr, st, sold = realtime_trader(monkeypatch, [])
    opened, asked, tries = [], [], []

    async def rec_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append((name, mc, src))
        return None

    async def chain(mint, sig, s0, target, trig_mc, with_trigger=False):
        asked.append((s0, target))
        return 34.0

    class Rpc:
        async def signature_slots(self, sigs):
            tries.append(sigs)
            return {} if len(tries) == 1 else {"CREATE": 777}       # первый раз узел ещё не знает транзакцию
    tr.e.rpc = Rpc()
    tr._open, tr.block_mc = rec_open, chain
    data = {"modes": ["mydev"], "symbol": "L", "mc": 0.0, "trigger_slot": 0, "trigger_sig": "CREATE",
            "trigger_mc": 1.0, "trigger_at": time.time(), "trigger_create": True}
    assert tr.realtime(data)
    await tr.on_alert("M", "normal", None, data, "DEV")
    assert opened == [("Y", 34.0, "блок")] and asked == [(777, 779)] and len(tries) == 2


async def test_block_mc_create_without_dev_buy_and_reasons(monkeypatch):
    """Создание без покупки дева и без сделок до блока +2 — цена старта кривой; иначе причина точнее."""
    import scanner.ingest.pump_parser as parser
    from scanner.models import TokenCreated
    tr = bare_trader({})
    tr.db = SimpleNamespace(fetchval=lambda *a: asyncio.sleep(0, result=None))
    rpc = FakeRpc([])
    tr.e.rpc = rpc
    monkeypatch.setattr(parser, "parse_transaction", lambda tx, sig, slot, src, stats, pool_map=None: [
        TokenCreated(signature=sig, mint="M", deployer="DEV", creator="DEV", name="L", symbol="L", uri="",
                     v_sol=0.001, v_tokens=1.0)])                    # резервы в событии неверные — не используем
    assert await tr.block_mc("M", "CREATE", 100, 102, None, with_trigger=True) == pytest.approx(pp.START_MC)
    assert 27 < pp.START_MC < 29
    monkeypatch.setattr(parser, "parse_transaction", lambda *a, **k: [])
    assert await tr.block_mc("M", "CREATE", 100, 102, None, with_trigger=True) is None
    assert tr._why_pop("M", "CREATE") == "в транзакциях блока нет сделки этой монеты на pump.fun"

    async def no_tx(sig):
        return None
    rpc.transaction = no_tx
    assert await tr.block_mc("M", "CREATE", 100, 102, None, with_trigger=True) is None
    assert tr._why_pop("M", "CREATE") == "узел не отдал транзакции блока"


async def test_block_mc_takes_worst_price_of_the_block(monkeypatch):
    """Порядок транзакций внутри блока узел отдаёт по-разному — цена блока +2 одна и та же: худшая для покупки."""
    import scanner.ingest.pump_parser as parser
    tr = bare_trader({})
    tr.db = SimpleNamespace(fetchval=lambda *a: asyncio.sleep(0, result=None))
    prices = {"a": 40.0, "b": 55.0, "c": 47.0}

    def parse(tx, sig, slot, src, stats, pool_map=None):
        return [Trade(signature=sig, mint="M", trader="Y", is_buy=True, sol_amount=1, token_amount=1,
                      v_sol=prices[sig], v_tokens=8e8)] if sig in prices else []
    monkeypatch.setattr(parser, "parse_transaction", parse)
    for order in (["a", "b", "c"], ["c", "a", "b"]):
        tr.e.rpc = FakeRpc([{"signature": s_, "slot": 102} for s_ in order] + [{"signature": "old", "slot": 101}])
        assert await tr.block_mc("M", "TRIG", 100, 102, 30.0) == pytest.approx(mc_sol(55.0, 8e8))


async def test_exit_takes_worst_price_of_the_block(monkeypatch):
    """Продажа — худшая цена последнего блока со сделками (порядок внутри блока неизвестен), как вход — худшая."""
    trades = [row(101, 31.0), row(103, 40.0), row(106, 44.0), row(106, 38.0), row(106, 41.0), row(108, 50.0)]
    tr, st, sold = realtime_trader(monkeypatch, trades)
    assert tr.exit_mc_at_slot("M", 106) == 38.0 and tr.exit_mc_at_slot("M", 105) == 40.0
    assert tr.exit_mc_at_slot("M", 100) is None
