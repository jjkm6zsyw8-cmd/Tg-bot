"""Блок E: исполнение бумажных сделок и правила выхода."""
from __future__ import annotations

import os
import time
from types import SimpleNamespace

import pytest

from scanner.analytics import paper as pp
from scanner.analytics.paper import Position, buy_fill, decide_exit, sell_fill

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


def test_fees_and_slippage_are_realistic():
    # маленькая позиция: фиксированная приоритетная комиссия заметна
    small = buy_fill(40, 0.04, 125, 0.0005)
    assert small.fee_sol > 0.0005
    back = sell_fill(40, small.tokens, 125, 0.0005)
    loss_small = 1 - back.sol / 0.04
    big = buy_fill(40, 5.0, 125, 0.0005)
    back_big = sell_fill(40, big.tokens, 125, 0.0005)
    loss_big = 1 - back_big.sol / 5.0
    assert 0.03 < loss_small < 0.06            # 2 × приоритет + 2 × 1.25% на позиции 0.04 SOL
    assert loss_big > 0.10                     # большая позиция — проскальзывание по кривой
    assert big.tokens / 5.0 < small.tokens / 0.04


def pos(entry=40.0, target=80.0) -> Position:
    f = buy_fill(entry, 0.2, 125, 0.0005)
    return Position(id=1, portfolio="B", mint="M", opened_ts=0, entry_mc=entry, cost_sol=0.2, tokens=f.tokens,
                    tokens_initial=f.tokens, target_mc=target, max_mc=entry)


def test_exit_rules_scheme_b():
    s = pp.SCHEMES["B"]
    assert decide_exit(pos(), 33.0, 10, s, 125, 0.0005) == ("stop", 1.0)           # −17.5% по цене
    p = pos()
    r = decide_exit(p, 56.0, 10, s, 125, 0.0005)
    assert r == ("take_profit", 0.5)                                                # +30% чистыми
    p.tp_done = True
    p.max_mc = 70
    assert decide_exit(p, 59.0, 20, s, 125, 0.0005) == ("trailing", 1.0)           # −15% от максимума
    assert decide_exit(pos(), 81.0, 10, s, 125, 0.0005)[0] == "target"
    assert decide_exit(pos(), 42.0, 601, s, 125, 0.0005) == ("time_stop", 1.0)     # 10 мин без +10%
    assert decide_exit(pos(), 42.0, 10, s, 125, 0.0005, "emergency") == ("emergency", 1.0)


def test_exit_rules_a_and_c():
    assert decide_exit(pos(), 37.0, 10, pp.SCHEMES["A"], 125, 0.0005) == ("stop", 1.0)   # −6% хватает
    c = pp.SCHEMES["C"]
    assert decide_exit(pos(), 20.0, 10, c, 125, 0.0005) is None                         # без ценового стопа
    assert decide_exit(pos(), 60.0, 10, c, 125, 0.0005) is None                         # без тейк-профита
    assert decide_exit(pos(), 80.0, 10, c, 125, 0.0005)[0] == "target"


async def test_paper_trader_full_cycle(monkeypatch):
    from scanner.config import Settings, YamlConfig
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=5)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
        monkeypatch.setattr(pp, "EXIT_DELAY_S", 0)
        st = SimpleNamespace(mc=40.0, mint="MINT1", trades=[SimpleNamespace(ts=__import__("time").time())])
        settings = Settings(YamlConfig("config/config.yaml"))
        eng = SimpleNamespace(db=db, price=SimpleNamespace(value=100.0), fee_bps=None, settings=settings,
                              cfg=settings.effective(), states={"MINT1": st}, ext=None)
        tr = pp.PaperTrader(eng)  # type: ignore[arg-type]
        await tr.start()
        assert tr.balance["B"] == pytest.approx(0.3)                    # $30 при SOL = $100
        n = len(pp.SCHEMES)
        await tr.on_alert("MINT1", "strong", None, {"symbol": "T", "mc": 40.0, "target_mc": 90.0}, None)
        assert len(tr.positions) == n and tr.balance["B"] == pytest.approx(0.21)   # «Сильный»: 20% × 1.5 = 30%
        await tr.on_alert("MINT1", "strong", None, {"symbol": "T", "mc": 40.0}, None)
        assert len(tr.positions) == n                                    # одна позиция на монету
        st.mc = 56.0
        for p in list(tr.positions.values()):
            await tr._check(p, 56.0, None)
        await __import__("asyncio").sleep(0.05)
        b = next(p for p in tr.positions.values() if p.portfolio == "B")
        assert b.tp_done                                                 # B продал 50% на +30%
        assert next(p for p in tr.positions.values() if p.portfolio == "A").tp_done
        assert not any(p.tp_done for p in tr.positions.values() if p.portfolio == "C")
        assert next(p for p in tr.positions.values() if p.portfolio == "D").tp_done   # тейк 50% на +20%
        assert not any(p.portfolio == "G" for p in tr.positions.values())             # скальп: всё на +25%
        st.mc = 45.0
        for p in list(tr.positions.values()):
            await tr._check(p, 45.0, "emergency")
        await __import__("asyncio").sleep(0.05)
        assert not tr.positions
        rows = await db.fetch("SELECT portfolio, pnl_sol, exit_reason FROM paper_positions ORDER BY portfolio")
        assert [r["exit_reason"] for r in rows] == ["emergency"] * 6 + ["take_profit"]
        # 7 входов + 3 частичных тейка (A, B, D) + 7 полных выходов
        assert (await db.fetchval("SELECT count(*) FROM paper_trades")) == 17
        await tr.flush_messages()
        exits = await db.fetchval("SELECT payload->>'text' FROM outbox WHERE kind='text' ORDER BY id DESC LIMIT 1")
        closed = [x for x in exits.split("\n") if x[:1] in "✅❌"]
        assert len(closed) == 1 and "G +" in closed[0]          # выходы по монете из 7 портфелей — одной строкой
        text = await tr.render(hourly=True)
        assert "Бумага · за 1 час" in text and "<pre>" in text and "G" in text and text.count("\n") <= 12
        period = await tr.render_period(1)
        assert "за 1 час" in period and "★" in period and "скальп" in period
        whole = await tr.render()                                 # /paper и кнопка «Всё» (hours=None)
        assert "за всё время" in whole and "период" not in whole
        hist = await tr.history()
        assert "дев продал" in hist
        day = await tr.trades_text(24)
        assert len(day) == 1 and day[0].count("$T") == 1 and "G +" in day[0] and "сделок 7" in day[0]
        table = await tr.trades_csv(None)
        assert table.count("\n") == 8 and "портфель;монета" in table and "экстренный выход" in table
        await tr.reset()
        assert tr.balance["A"] == pytest.approx(0.3)
    finally:
        await db.close()


async def test_exit_uses_current_price_not_peak(monkeypatch):
    """Регрессия: монета без своего потока сделок закрывалась по максимуму за позицию."""
    monkeypatch.setattr(pp, "EXIT_DELAY_S", 0)
    eng = SimpleNamespace(states={}, ext=None, price=SimpleNamespace(value=100.0))
    tr = pp.PaperTrader.__new__(pp.PaperTrader)
    tr.e = eng
    p = pos(entry=40.0)
    p.max_mc = 400.0                       # когда-то был пик ×10
    p.extra["last_mc"] = 30.0              # сейчас цена упала
    assert await tr.current_mc(p) == 30.0


def test_position_without_price_still_hits_time_rules():
    """Регрессия: позиция без источника цены висела часами и не закрывалась."""
    p = pos(entry=40.0)
    assert decide_exit(p, 40.0, 601, pp.SCHEMES["A"], 125, 0.0005) == ("time_stop", 1.0)
    p2 = pos(entry=40.0)
    assert decide_exit(p2, 40.0, pp.MAX_HOLD_S + 1, pp.SCHEMES["C"], 125, 0.0005) == ("max_hold", 1.0)


def test_new_schemes_d_to_g():
    d = pp.SCHEMES["D"]
    assert decide_exit(pos(), 51.0, 10, d, 125, 0.0005) == ("take_profit", 0.5)        # +20% чистыми
    assert decide_exit(pos(), 35.5, 10, d, 125, 0.0005) == ("stop", 1.0)               # −11%
    e = pp.SCHEMES["E"]
    assert decide_exit(pos(), 56.0, 10, e, 125, 0.0005) is None                         # +30% ещё мало
    assert decide_exit(pos(), 42.0, 700, e, 125, 0.0005) is None                        # стоп по времени — 15 мин
    f = pp.SCHEMES["F"]
    p = pos()
    assert decide_exit(p, 46.0, 10, f, 125, 0.0005) is None                             # +15% цены ≈ +10% чистыми
    assert decide_exit(p, 52.0, 20, f, 125, 0.0005) is None                             # трейлинг включился
    assert decide_exit(p, 44.0, 30, f, 125, 0.0005) == ("trailing", 1.0)               # −15% от пика
    p2 = pos()
    assert decide_exit(p2, 44.0, 30, f, 125, 0.0005) is None                            # без +15% трейлинга нет
    g = pp.SCHEMES["G"]
    assert decide_exit(pos(), 53.0, 10, g, 125, 0.0005) == ("take_profit", 1.0)        # всё на +25%
    assert decide_exit(pos(), 42.0, 301, g, 125, 0.0005) == ("time_stop", 1.0)          # 5 минут
    assert decide_exit(pos(target=50.0), 50.0, 10, g, 125, 0.0005) is None             # цель алерта не используется


def test_position_size_by_signal_level():
    tr = pp.PaperTrader.__new__(pp.PaperTrader)
    tr.e = SimpleNamespace(cfg={"paper_position_share": 0.2})
    assert tr.share_for("strong") == pytest.approx(0.30)
    assert tr.share_for("normal") == pytest.approx(0.15)
    assert tr.share_for("watch") == pytest.approx(0.10)
    tr.e = SimpleNamespace(cfg={"paper_position_share": 0.4})
    assert tr.share_for("strong") == pytest.approx(0.5)                                 # потолок 50%
    tr.e = SimpleNamespace(cfg={"paper_position_share": 0.2, "paper_size_by_level": False})
    assert tr.share_for("strong") == pytest.approx(0.2)


class FakeExt:
    """DexScreener: MC и цена за токен (MC / 1e9); Jupiter — своя цена (None — нет ответа)."""
    def __init__(self, mc_usd, jup=None):
        self.mc_usd = mc_usd
        self.jup = jup

    async def dex_pair(self, mint, timeout=None, ttl=15.0):
        return {"market_cap_usd": self.mc_usd, "price_usd": self.mc_usd / 1e9} if self.mc_usd else None

    async def jup_price(self, mint):
        return self.jup


def bare_trader(states, ext=None) -> pp.PaperTrader:
    tr = pp.PaperTrader.__new__(pp.PaperTrader)
    tr.e = SimpleNamespace(states=states, ext=ext, price=SimpleNamespace(value=100.0), cfg={})
    tr.skipped = {}
    tr.positions = {}
    return tr


def fresh_state(mc, age_s=1.0):
    return SimpleNamespace(mc=mc, mint="M", trades=[SimpleNamespace(ts=time.time() - age_s)])


async def test_stale_own_price_is_not_used():
    """Регрессия: без сделок цена застревала на стартовой (~28 SOL), бумага покупала «по $2.7k» монету за $3M."""
    tr = bare_trader({"M": fresh_state(28.0, age_s=600)})
    assert tr.state_price("M") is None
    tr2 = bare_trader({"M": fresh_state(28.0, age_s=5)})
    assert tr2.state_price("M") == 28.0


async def test_entry_price_sources(monkeypatch):
    monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
    opened = []

    async def fake_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append((mc, src))
        return None
    # «Тренды»: только DexScreener, даже если свой поток свежий
    tr = bare_trader({"M": fresh_state(28.0)}, FakeExt(300_000))     # $300k при SOL $100 = 3000 SOL
    tr._open = fake_open
    tr.lock = __import__("asyncio").Lock()
    await tr.on_alert("M", "normal", None, {"trend": {"pc_m5": 10}, "mc": 2900.0}, None)
    assert opened and opened[0] == (3000.0, "dex")
    # девы/новые: свой свежий поток, но при расхождении с DexScreener > 2× — DexScreener
    opened.clear()
    tr.e.ext = FakeExt(4_000)                                       # 40 SOL
    await tr.on_alert("M", "normal", None, {"mc": 28.0}, None)
    assert opened[0] == (40.0, "auto") or opened[0] == (28.0, "auto")
    opened.clear()
    tr.e.ext = FakeExt(300_000)
    await tr.on_alert("M", "normal", None, {"mc": 28.0}, None)
    assert opened[0] == (3000.0, "dex")
    # свой поток устарел и DexScreener нет — не входим
    opened.clear()
    tr.e.states = {"M": fresh_state(28.0, age_s=600)}
    tr.e.ext = None
    await tr.on_alert("M", "normal", None, {"mc": 28.0}, None)
    assert not opened and "нет свежей цены" in tr.skipped["*"]


async def test_price_jump_needs_confirmation(monkeypatch):
    monkeypatch.setattr(pp, "EXIT_DELAY_S", 0)
    tr = bare_trader({})
    sold = []

    async def fake_exit(p, reason, share):
        sold.append(reason)
    tr._exit_later = fake_exit
    tr.fee_bps = lambda: 125
    tr.priority = lambda: 0.0005
    p = pos(entry=40.0, target=80.0)
    p.opened_ts = time.time()
    p.extra["last_mc"] = 40.0
    await tr._check(p, 40_000.0, None)            # ×1000 за одну проверку — сбой? ждём подтверждения
    assert not sold and p.extra["last_mc"] == 40.0
    await tr._check(p, 41.0, None)                 # не подтвердилось
    assert not sold and "jump" not in p.extra
    await tr._check(p, 400.0, None)
    await tr._check(p, 410.0, None)                # подтвердилось через проверку — работаем
    await __import__("asyncio").sleep(0)
    assert sold == ["target"]


async def test_exit_price_capped_against_glitch():
    tr = bare_trader({}, FakeExt(4_000_000))       # DexScreener внезапно показывает 40 000 SOL
    p = pos(entry=40.0)
    p.extra["last_mc"] = 45.0
    assert await tr.current_mc(p) == 45.0          # выход не по сбойной цене
    tr.e.ext = FakeExt(5_000)
    assert await tr.current_mc(p) == 50.0


async def test_persistent_dexscreener_glitch_needs_jupiter(monkeypatch):
    """Регрессия: DexScreener несколько проверок подряд показывал $306M вместо $20k (пара к экзотическому токену)."""
    tr = bare_trader({}, FakeExt(0, jup=None))
    tr.bad_prices = 0
    sold = []

    async def fake_exit(p, reason, share):
        sold.append(reason)
    tr._exit_later = fake_exit
    tr.fee_bps = lambda: 125
    tr.priority = lambda: 0.0005
    p = pos(entry=200.0, target=300.0)
    p.opened_ts = time.time()
    p.extra.update({"src": "dex", "last_mc": 200.0})
    glitch_usd = 306e6 / 1e9
    for _ in range(3):                                   # сбой держится — Jupiter не отвечает
        await tr._check(p, 3_000_000.0, None, glitch_usd)
    assert not sold and p.extra["last_mc"] == 200.0 and tr.bad_prices == 3
    tr.e.ext = FakeExt(0, jup=2e-5)                      # Jupiter: $20k — сбой DexScreener
    await tr._check(p, 3_000_000.0, None, glitch_usd)
    assert not sold
    tr.e.ext = FakeExt(0, jup=glitch_usd * 1.1)          # Jupiter подтверждает — рост настоящий
    await tr._check(p, 3_000_000.0, None, glitch_usd)
    await __import__("asyncio").sleep(0)
    assert sold == ["target"]


async def test_entry_skipped_when_jupiter_disagrees(monkeypatch):
    monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
    tr = bare_trader({}, FakeExt(300_000, jup=0.03))     # DexScreener $0.0003, Jupiter $0.03 — в 100 раз
    tr.lock = __import__("asyncio").Lock()
    opened = []

    async def fake_open(*a, **k):
        opened.append(a)
    tr._open = fake_open
    await tr.on_alert("M", "normal", None, {"trend": {"pc_m5": 10}, "mc": 3000.0}, None)
    assert not opened and "Jupiter" in tr.skipped["*"]
    tr.e.ext = FakeExt(300_000, jup=None)                # Jupiter молчит — входим по DexScreener
    await tr.on_alert("M", "normal", None, {"trend": {"pc_m5": 10}, "mc": 3000.0}, None)
    assert opened


async def test_price_exit_needs_confirmation_after_delay(monkeypatch):
    """Регрессия: один низкий замер выбивал стоп −15%, а продажа шла по нормальной цене («−5% стоп»)."""
    monkeypatch.setattr(pp, "EXIT_DELAY_S", 0)
    st = SimpleNamespace(mc=40.0, mint="M", trades=[SimpleNamespace(ts=time.time())])
    tr = bare_trader({"M": st})
    tr.false_exits = 0
    tr.fee_bps = lambda: 125
    tr.priority = lambda: 0.0005
    sold = []

    async def fake_sell(p, reason, share, mc):
        sold.append((reason, mc))
    tr._sell = fake_sell
    tr.lock = __import__("asyncio").Lock()
    p = pos(entry=40.0)
    p.portfolio = "B"
    p.extra["last_mc"] = 40.0
    await tr._check(p, 33.0, None)                       # замер −17.5% → стоп B
    await __import__("asyncio").sleep(0.01)
    assert not sold and tr.false_exits == 1 and not p.closing   # через 2 с цена 40 — стоп не подтвердился
    st.mc = 33.0
    await tr._check(p, 33.0, None)
    await __import__("asyncio").sleep(0.01)
    assert sold == [("stop", 33.0)]                     # падение настоящее — продаём по текущей цене
    await tr._check(pos(entry=40.0), 20.0, "emergency")
    await __import__("asyncio").sleep(0.01)
    assert sold[-1][0] == "emergency"                   # экстренный выход — без проверки
