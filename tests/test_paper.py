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
                              cfg={**settings.effective(), "paper_start_usd": 30}, states={"MINT1": st}, ext=None)
        tr = pp.PaperTrader(eng)  # type: ignore[arg-type]
        await tr.start()
        assert tr.balance["B"] == pytest.approx(0.3)                    # старт $30 (настройка) при SOL = $100
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
        assert not next(p for p in tr.positions.values() if p.portfolio == "D").tp_done   # лотерея ждёт ×2
        assert not any(p.portfolio == "G" for p in tr.positions.values())             # скальп: всё на +25%
        st.mc = 45.0
        for p in list(tr.positions.values()):
            await tr._check(p, 45.0, "emergency")
        await __import__("asyncio").sleep(0.05)
        assert not tr.positions
        rows = await db.fetch("SELECT portfolio, pnl_sol, exit_reason FROM paper_positions ORDER BY portfolio")
        assert [r["exit_reason"] for r in rows] == ["emergency"] * 6 + ["take_profit"]
        # 7 входов + 2 частичных тейка (A, B; лотерея D ждёт ×2) + 7 полных выходов
        assert (await db.fetchval("SELECT count(*) FROM paper_trades")) == 16
        await tr.flush_messages()
        exits = await db.fetchval("SELECT payload->>'text' FROM outbox WHERE kind='text' ORDER BY id DESC LIMIT 1")
        closed = [x for x in exits.split("\n") if x[:1] in "✅❌"]
        assert len(closed) == 1 and "G +" in closed[0]          # выходы по монете из 7 портфелей — одной строкой
        text = await tr.render(hourly=True)
        assert "Бумага · за 1 час" in text and "<pre>" in text and "G" in text and text.count("\n") <= 12
        period = await tr.render_period(1)
        assert "за 1 час" in period and "★" in period and "всё на +25%" in period
        whole = await tr.render()                                 # /paper и кнопка «Всё» (hours=None)
        assert "за всё время" in whole and "период" not in whole
        hist = await tr.history()
        assert "дев продал" in hist
        day = await tr.trades_text(24)
        assert len(day) == 1 and day[0].count("$T") == 1 and "G +" in day[0] and "сделок 7" in day[0]
        table = await tr.trades_csv(None)
        assert table.count("\n") == 8 and "портфель;монета" in table and "экстренный выход" in table
        await tr.reset()
        assert tr.balance["A"] == pytest.approx(0.3)   # сброс — до старта из настройки ($30)
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
    d = pp.SCHEMES["D"]                                                                 # лотерея — держит иксы
    assert decide_exit(pos(), 51.0, 10, d, 125, 0.0005) is None                         # +20% — держим
    assert decide_exit(pos(), 33.5, 10, d, 125, 0.0005) == ("stop", 1.0)               # −16%
    assert decide_exit(pos(), 84.0, 10, d, 125, 0.0005) == ("take_profit", 0.34)       # ×2 чистыми — треть
    pr = pos()
    assert decide_exit(pr, 400.0, 20, d, 125, 0.0005) == ("take_profit", 0.34)
    pr.tp_done = True
    assert decide_exit(pr, 800.0, 30, d, 125, 0.0005) is None                           # ×20 — едем дальше
    assert decide_exit(pr, 600.0, 40, d, 125, 0.0005) is None                           # −25% от пика — ещё держим
    assert decide_exit(pr, 550.0, 50, d, 125, 0.0005) == ("trailing", 1.0)             # −31% от пика
    assert decide_exit(pos(), 41.0, 3 * 3600, d, 125, 0.0005) == ("time_stop", 1.0)   # без роста — время
    held = pos()
    held.max_net = 0.6
    assert decide_exit(held, 60.0, 3 * 3600, d, 125, 0.0005) is None                    # держит до 4 часов
    assert decide_exit(held, 60.0, 4 * 3600 + 1, d, 125, 0.0005) == ("max_hold", 1.0)
    e = pp.SCHEMES["E"]                                                                 # копии: всё на +40%
    assert decide_exit(pos(), 56.0, 10, e, 125, 0.0005) is None                         # +36% чистыми — мало
    assert decide_exit(pos(), 58.5, 10, e, 125, 0.0005) == ("take_profit", 1.0)
    assert decide_exit(pos(), 36.5, 10, e, 125, 0.0005) == ("stop", 1.0)               # −8.75%
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

    async def jup_price(self, mint, ttl=None):
        return self.jup

    async def jup_prices(self, mints, ttl=None):
        return {m: self.jup for m in mints} if self.jup else {}


def bare_trader(states, ext=None) -> pp.PaperTrader:
    tr = pp.PaperTrader.__new__(pp.PaperTrader)
    tr.e = SimpleNamespace(states=states, ext=ext, price=SimpleNamespace(value=100.0), cfg={})
    tr.skipped = {}
    tr.positions = {}
    tr.jup_cache = {}
    tr.entry_fixed = tr.exit_fixed = tr.false_exits = tr.no_price = tr.bad_prices = 0
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
    """Вход по рыночной цене: Jupiter → сделка своего потока не старше 10 с → не входим. «Тренды» — DexScreener."""
    monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
    opened = []

    async def fake_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append((mc, src))
        return None
    tr = bare_trader({"M": fresh_state(28.0)}, FakeExt(300_000))     # $300k при SOL $100 = 3000 SOL
    tr.e.cfg = {"paper_trends": True}
    tr._open = fake_open
    tr.lock = __import__("asyncio").Lock()
    await tr.on_alert("M", "normal", None, {"trend": {"pc_m5": 10}, "mc": 2900.0}, None)
    assert opened and opened[0] == (3000.0, "dex")                   # «Тренды» — по DexScreener
    opened.clear()
    await tr.on_alert("M", "normal", None, {"mc": 28.0}, None)       # Jupiter нет — свежая сделка своего потока
    assert opened[0] == (28.0, "own")
    opened.clear()
    tr.e.ext = FakeExt(300_000, jup=45 / 1e9 * 100)                  # Jupiter есть — по Jupiter
    await tr.on_alert("M", "normal", None, {"mc": 28.0}, None)
    assert opened[0] == (pytest.approx(45.0), "jup")
    opened.clear()
    tr.jup_cache.clear()
    tr.e.states = {"M": fresh_state(28.0, age_s=60)}                 # сделке минута, Jupiter не знает монету
    tr.e.ext = FakeExt(300_000)
    await tr.on_alert("M", "normal", None, {"mc": 28.0}, None)
    assert not opened and "нет подтверждённой цены" in tr.skipped["*"] and tr.no_price == 1


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
    tr.e.cfg = {"paper_trends": True}
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


async def test_own_stream_jump_checked_with_jupiter(monkeypatch):
    """Свой поток стабильно показывал ×17 (пул не к SOL) — подтверждение повтором не спасало. Теперь — Jupiter."""
    monkeypatch.setattr(pp, "EXIT_DELAY_S", 0)
    tr = bare_trader({}, FakeExt(0, jup=40 * 100 / 1e9))       # Jupiter: MC 40 SOL при SOL $100
    tr.bad_prices = 0
    sold = []

    async def fake_exit(p, reason, share):
        sold.append(reason)
    tr._exit_later = fake_exit
    tr.fee_bps = lambda: 125
    tr.priority = lambda: 0.0005
    p = pos(entry=40.0, target=80.0)
    p.opened_ts = time.time()
    p.extra["last_mc"] = 40.0
    for _ in range(3):
        await tr._check(p, 700.0, None)                         # свой поток: ×17.5, Jupiter не согласен
    assert not sold and tr.bad_prices == 3 and p.extra["last_mc"] == 40.0
    tr.e.ext = FakeExt(0, jup=700 * 100 / 1e9)                  # Jupiter подтверждает — рост настоящий
    await tr._check(p, 700.0, None)
    await __import__("asyncio").sleep(0)
    assert sold == ["target"]


async def test_suspect_flag_only_when_jupiter_disagrees(monkeypatch):
    """Настоящая ракета (×56) не помечается сбоем; выход по цене, которую Jupiter не подтвердил, — помечается."""
    from scanner.config import Settings, YamlConfig
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
        settings = Settings(YamlConfig("config/config.yaml"))
        sts = {m: SimpleNamespace(mc=28.0, mint=m, trades=[SimpleNamespace(ts=time.time())]) for m in ("REAL", "FAKE")}
        eng = SimpleNamespace(db=db, price=SimpleNamespace(value=100.0), fee_bps=None, settings=settings,
                              cfg=settings.effective(), states=sts, ext=None)
        tr = pp.PaperTrader(eng)  # type: ignore[arg-type]
        await tr.start()
        for m in ("REAL", "FAKE"):
            await tr.on_alert(m, "strong", None, {"symbol": m, "mc": 28.0}, None)
        real = next(p for p in tr.positions.values() if p.mint == "REAL" and p.portfolio == "F")
        fake = next(p for p in tr.positions.values() if p.mint == "FAKE" and p.portfolio == "F")
        eng.ext = FakeExt(0, jup=1500 * 100 / 1e9)                 # Jupiter: MC 1500 SOL
        await tr._sell(real, "trailing", 1.0, 1500.0)               # ×54 — подтверждено
        await tr._sell(fake, "trailing", 1.0, 1500.0 * 20)          # ×1000 — у Jupiter в 20 раз меньше
        rows = {r["mint"]: r["suspect"] for r in await db.fetch(
            "SELECT mint, suspect FROM paper_positions WHERE portfolio='F' AND closed_at IS NOT NULL")}
        assert rows == {"REAL": False, "FAKE": True}
        text = await tr.render_period(None)
        assert "⚠ 1 с неверной ценой не входят в итог" in text
    finally:
        await db.close()


def test_copy_follows_portfolio_scheme():
    """Копии выходят по правилу своего портфеля (C — только цель, F — трейл после +15%): единое правило
    «½ на +30%» срезало иксы, которые и давали плюс (6.11 вечером C/F +1 SOL на копиях)."""
    tr = pp.PaperTrader.__new__(pp.PaperTrader)
    for name in "ACF":
        p = Position(id=1, portfolio=name, mint="M", opened_ts=0, entry_mc=1, cost_sol=1, tokens=1,
                     tokens_initial=1, target_mc=None, rule="copy")
        assert tr.scheme_for(p) is pp.SCHEMES[name]
    c = pp.SCHEMES["C"]
    assert decide_exit(pos(entry=5.0, target=7.0), 36.0, 60, c, 125, 0.0005)[0] == "target"   # ×7 — по цели


async def test_price_arbitration_without_and_with_jupiter():
    tr = bare_trader({})
    tr.e.ext = FakeExt(2_800)                                    # DexScreener: стартовые 28 SOL
    assert await tr.arbitrate("M", 50.0, 28.0) == "own"         # KREO: DexScreener ещё не видел сделок
    assert await tr.arbitrate("M", 28.0, 3000.0) == "dex"       # свой поток застрял на старте
    assert await tr.arbitrate("M", 400.0, 100.0) == "dex"       # как раньше — DexScreener
    tr.e.ext = FakeExt(2_800, jup=50 / 1e9 * 100)                # Jupiter: 50 SOL
    assert await tr.arbitrate("M", 50.0, 28.0) == "own"
    tr.e.ext = FakeExt(2_800, jup=3000 / 1e9 * 100)
    assert await tr.arbitrate("M", 28.0, 3000.0) == "dex"


async def test_copy_skips_when_dexscreener_is_at_start_price(monkeypatch):
    monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
    opened = []

    async def fake_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append((mc, src))
    tr = bare_trader({"M": fresh_state(60.0, age_s=600)}, FakeExt(2_800))   # свой поток устарел, DexScreener 28
    tr._open = fake_open
    tr.lock = __import__("asyncio").Lock()
    data = {"mc": 60.0, "copy_ref_mc": 60.0}
    await tr.on_alert("M", "normal", None, data, None)
    assert not opened and "нет подтверждённой цены" in tr.skipped["*"]
    tr.e.ext = FakeExt(2_800, jup=58 / 1e9 * 100)                         # Jupiter знает: 58 SOL
    await tr.on_alert("M", "normal", None, data, None)
    assert opened and opened[0][0] == pytest.approx(58.0) and opened[0][1] == "jup"


async def test_rugged_coin_and_its_dev_are_banned():
    """Раг (−50% и хуже): ни в эту монету, ни в новые монеты её дева больше не входим — во всех портфелях."""
    tr = bare_trader({"NEW": SimpleNamespace(deployer="DEV", mc=30.0, trades=[])})
    tr.balance = {"A": 0.33, "B": 0.33}
    tr.loss_exits = {}
    tr.rugged = {"NK": -0.99}
    tr.rug_devs = {"DEV"}
    tr.priority = lambda: 0.0005
    for name in "AB":
        assert await tr._open(name, "NK", "normal", None, {}, None, 30.0) is None
        assert "уже рагнула (-99%)" in tr.skipped[name]
    assert await tr._open("A", "NEW", "normal", None, {}, None, 30.0) is None
    assert "дев этой монеты уже рагал" in tr.skipped["A"]
    tr.e.my_devs = {"DEV": {}}                       # свой дев из списка — его монеты не запрещаем
    tr.e.states["NEW"].deployer = "DEV"
    tr.rugged, tr.rug_devs = {}, {"DEV"}
    tr.loss_exits = {("A", "NEW"): time.time()}      # дальше упрёмся в паузу после убытка — значит, бан не сработал
    assert await tr._open("A", "NEW", "normal", None, {}, None, 30.0) is None
    assert "повторно не входим" in tr.skipped["A"]


async def test_no_reentry_after_loss_and_day_cap():
    from datetime import datetime, timezone
    tr = bare_trader({})
    tr.balance = {"A": 0.33}
    tr.rugged, tr.rug_devs = {}, set()
    tr.loss_exits = {("A", "RUG"): time.time() - 300}
    tr.priority = lambda: 0.0005
    assert await tr._open("A", "RUG", "normal", None, {}, None, 30.0) is None
    assert "уже дала убыток" in tr.skipped["A"]
    # днём (12:00 Батуми = 08:00 UTC) — не больше $35 в позициях лотереи: при SOL $100 это 0.35 SOL
    noon = datetime(2026, 11, 7, 8, 0, tzinfo=timezone.utc)
    night = datetime(2026, 11, 7, 22, 0, tzinfo=timezone.utc)        # 02:00 Батуми
    from tests.conftest import REAL_DAY_CAP
    assert REAL_DAY_CAP(tr, noon) == pytest.approx(0.35) and REAL_DAY_CAP(tr, night) is None   # $35 при SOL $100
    tr.e.cfg = {"paper_day_cap_usd": 0}
    assert REAL_DAY_CAP(tr, noon) is None                             # 0 — без лимита
    tr.e.cfg = {}
    tr.day_cap_sol = lambda now=None: 0.1                             # дневной лимит — только у лотереи (D, E)
    tr.balance["D"] = 0.33
    tr.day_pnl, tr._day_key = {}, tr.day_key()
    tr.positions = {1: Position(id=1, portfolio="D", mint="X", opened_ts=0, entry_mc=1, cost_sol=0.09, tokens=1,
                                tokens_initial=1, target_mc=None)}
    assert await tr._open("D", "NEW", "watch", None, {}, None, 30.0) is None    # занято 0.09 из 0.1 — нет места
    assert "лимит занят" in tr.skipped["D"]


async def test_trends_not_in_paper_by_default(monkeypatch):
    monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
    tr = bare_trader({}, FakeExt(300_000))
    opened = []

    async def fake_open(*a, **k):
        opened.append(a)
    tr._open = fake_open
    tr.lock = __import__("asyncio").Lock()
    await tr.on_alert("M", "normal", None, {"trend": {"pc_m5": 10}, "mc": 3000.0}, None)
    assert not opened and "Тренды" in tr.skipped["*"]
    tr.e.cfg = {"paper_trends": True}
    await tr.on_alert("M", "normal", None, {"trend": {"pc_m5": 10}, "mc": 3000.0}, None)
    assert opened


async def test_daily_reset_with_summary(monkeypatch):
    from scanner.config import Settings, YamlConfig
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
        settings = Settings(YamlConfig("config/config.yaml"))
        st = SimpleNamespace(mc=40.0, mint="M1", trades=[SimpleNamespace(ts=time.time())])
        eng = SimpleNamespace(db=db, price=SimpleNamespace(value=100.0), fee_bps=None, settings=settings,
                              cfg=settings.effective(), states={"M1": st}, ext=None)
        tr = pp.PaperTrader(eng)  # type: ignore[arg-type]
        await tr.start()
        await tr.maybe_daily_reset()                                 # первый запуск — только запомнить день
        assert await db.kv_get("paper:daily_reset") and not await db.fetchval("SELECT count(*) FROM outbox")
        await tr.on_alert("M1", "strong", None, {"symbol": "T", "mc": 40.0, "copy_wallet": "W", "copy_sol": 1.0}, "W")
        assert {p.rule for p in tr.positions.values()} == {"copy"}
        assert await db.fetchval("SELECT count(*) FROM paper_positions WHERE rule='copy'") == 7
        p = next(iter(tr.positions.values()))
        await tr._sell(p, "take_profit", 1.0, 60.0)
        await db.kv_set("paper:daily_reset", "2000-01-01")           # наступил новый день
        await tr.maybe_daily_reset()
        texts = [r["payload"]["text"] for r in await db.fetch("SELECT payload FROM outbox ORDER BY id")]
        assert any("Итог дня бумаги" in t and "По времени суток" in t for t in texts)
        assert not tr.positions and tr.balance["A"] == pytest.approx(1.0)
        await tr.flush_messages()
        assert "Новый день" in (await db.fetchval("SELECT payload->>'text' FROM outbox ORDER BY id DESC LIMIT 1"))
    finally:
        await db.close()


def test_route_signals_to_portfolios():
    """Одиночные копии и smart — в A, B, C, E, F, G; «Наблюдение» (режим «Девы») — лотерея D;
    сильные копии, «Мой дев» и новые монеты — в бумагу не идут."""
    tr = bare_trader({})
    main, lottery = set("ABCEFG"), set("D")
    assert tr.route({"modes": ["copy"], "traders": [{"address": "W"}]}, "normal")[0] == main
    targets, why = tr.route({"modes": ["copy"], "traders": [{"address": "W"}, {"address": "X"}]}, "strong")
    assert not targets and "сильная копия" in why
    assert tr.route({"modes": ["smart"], "traders": [{"address": "S"}]}, "normal")[0] == main
    one_sol = {"cluster": 0.0342}                                 # дев купил ~1 SOL при запуске
    assert tr.route({"modes": ["dev"], "metrics": one_sol}, "watch")[0] == lottery
    targets, why = tr.route({"modes": ["dev"], "metrics": {"cluster": 0.12}}, "watch")      # D — только ~1 SOL
    assert not targets and "дев купил не ~1 SOL" in why and "дев купил не ~1 SOL" in tr.skipped["D"]
    assert not tr.route({"modes": ["dev"]}, "watch")[0]                                     # нет замера — не берём
    assert tr.route({"modes": ["dev", "copy"], "traders": [{"address": "W"}], "metrics": one_sol},
                    "strong")[0] == main | lottery
    assert "Мой дев" in tr.route({"modes": ["mydev"]}, "normal")[1]
    assert "новые монеты" in tr.route({"modes": ["new"]}, "strong")[1]
    assert tr.route({}, "normal")[0] == set(pp.SCHEMES)          # старые вызовы без modes — во все
    tr.e.cfg = {"paper_strong_copies": True}
    assert tr.route({"modes": ["copy"], "traders": [{}, {}]}, "strong")[0] == main


async def test_lottery_ticket_slots_and_day_stop(monkeypatch):
    tr = bare_trader({})
    tr.balance = {"D": 0.30, "A": 0.30}
    tr.rugged, tr.rug_devs, tr.loss_exits = {}, set(), {}
    tr.priority = lambda: 0.0005
    tr.fee_bps = lambda: 125
    tr.day_pnl, tr._day_key = {}, tr.day_key()
    opened = []

    class DB:
        async def fetchval(self, *a):
            opened.append(a)
            return len(opened)

        async def execute(self, *a):
            return None
    tr.db = DB()
    size = await tr._open("D", "M1", "watch", None, {}, None, 30.0)
    assert size == pytest.approx(0.03)                          # 10% баланса
    tr.balance["D"] = 0.10
    assert await tr._open("D", "M2", "watch", None, {}, None, 30.0) == pytest.approx(0.02)   # не меньше 0.02
    for i in range(3, 9):                                       # 8 мест в лотерее
        tr.balance["D"] = 0.30
        assert await tr._open("D", f"M{i}", "watch", None, {}, None, 30.0)
    assert await tr._open("D", "M9", "watch", None, {}, None, 30.0) is None and "лимит 8" in tr.skipped["D"]
    tr.positions.clear()
    tr.day_pnl["D"] = -0.36                                      # −$36 при SOL $100 (стоп $35)
    assert await tr._open("D", "M10", "watch", None, {}, None, 30.0) is None
    assert "дневной стоп" in tr.skipped["D"]
    tr.day_pnl["A"] = -0.5                                       # основной портфель: стопа лотереи нет
    assert await tr._open("A", "M11", "normal", None, {}, None, 30.0) == pytest.approx(0.05)


async def test_manual_reset_clears_lottery_day_stop():
    """Регрессия 8.11: после ручного /paper reset лотерея всё равно стояла на дневном стопе до 00:00."""
    from scanner.config import Settings, YamlConfig
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        settings = Settings(YamlConfig("config/config.yaml"))
        eng = SimpleNamespace(db=db, price=SimpleNamespace(value=100.0), fee_bps=None, settings=settings,
                              cfg=settings.effective(), states={}, ext=None)
        tr = pp.PaperTrader(eng)  # type: ignore[arg-type]
        await tr.start()
        await db.execute("""INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol,
                               tokens, tokens_initial, exit_reason, pnl_sol, pnl_pct)
                            VALUES ('D', 'M', now(), now(), 30, 0.05, 1, 1, 'stop', -0.4, -0.5)""")
        tr2 = pp.PaperTrader(eng)  # type: ignore[arg-type]
        await tr2.start()
        assert tr2.lottery_stopped("D")                              # −$40 за сегодня — стоп ($35)
        await tr2.reset()
        assert not tr2.lottery_stopped("D")                          # сброс снимает стоп сразу…
        tr3 = pp.PaperTrader(eng)  # type: ignore[arg-type]
        await tr3.start()
        assert not tr3.lottery_stopped("D")                          # …и после перезапуска (считаем от сброса)
    finally:
        await db.close()


async def test_entry_price_follows_jupiter_when_own_stream_lags(monkeypatch):
    """Регрессия 8.11: вход записывался ниже всей минутной свечи (свой поток видел старую сделку)."""
    monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
    opened = []

    async def fake_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        opened.append(mc)
    tr = bare_trader({"M": fresh_state(30.0)}, FakeExt(3_000, jup=45 / 1e9 * 100))   # Jupiter: 45 SOL
    tr._open = fake_open
    tr.lock = __import__("asyncio").Lock()
    tr.entry_fixed = 0
    await tr.on_alert("M", "normal", None, {"mc": 30.0}, None)
    assert opened[-1] == pytest.approx(45.0) and tr.entry_fixed == 1        # покупаем по рынку, не по старой сделке
    tr.jup_cache.clear()
    tr.e.ext = FakeExt(3_000, jup=31 / 1e9 * 100)                           # разница 3% — тоже по рынку,
    await tr.on_alert("M", "normal", None, {"mc": 30.0}, None)              # но «поправленным» не считаем
    assert opened[-1] == pytest.approx(31.0) and tr.entry_fixed == 1


async def test_exit_is_checked_against_jupiter(monkeypatch):
    """Регрессия 9.11: «стоп» через 2–3 с после входа по устаревшей низкой цене своего потока / DexScreener
    (BIFT: стоп на $5.9k, рынок $10.8k → $15k). Jupiter не подтверждает — позиция остаётся; подтверждает —
    продаём по цене Jupiter."""
    monkeypatch.setattr(pp, "EXIT_DELAY_S", 0)
    p = pos(entry=40.0, target=80.0)                                   # схема B: стоп −15%
    tr = bare_trader({"M": fresh_state(30.0)}, FakeExt(0, jup=41 / 1e9 * 100))   # свой поток 30, Jupiter 41
    tr.exit_fixed, tr.false_exits = 0, 0
    tr.positions = {1: p}
    sold = []

    async def fake_sell(pp_, reason, share, mc):
        sold.append((reason, mc))
    tr._sell = fake_sell
    tr.lock = __import__("asyncio").Lock()
    p.closing = True
    await tr._exit_later(p, "stop", 1.0)
    assert not sold and not p.closing and tr.false_exits == 1 and tr.exit_fixed == 1   # ложный стоп отменён
    assert p.extra["last_mc"] == pytest.approx(41.0)
    tr.e.ext = FakeExt(0, jup=31 / 1e9 * 100)                                   # Jupiter: правда упала до 31
    tr.jup_cache.clear()
    p.closing = True
    await tr._exit_later(p, "stop", 1.0)
    assert sold == [("stop", pytest.approx(31.0))]


async def test_target_moves_with_jupiter_entry(monkeypatch):
    """Регрессия 9.11: вход поправлен на Jupiter, а цель алерта осталась от старой цены — ниже входа."""
    monkeypatch.setattr(pp, "ENTRY_DELAY_S", 0)
    got = []

    async def fake_open(name, mint, level, alert_id, data, trader, mc, src="auto"):
        got.append((mc, data.get("target_mc")))
    tr = bare_trader({"M": fresh_state(30.0)}, FakeExt(3_000, jup=45 / 1e9 * 100))
    tr._open = fake_open
    tr.lock = __import__("asyncio").Lock()
    tr.entry_fixed = 0
    await tr.on_alert("M", "normal", None, {"mc": 30.0, "target_mc": 39.0}, None)
    assert got[-1] == (pytest.approx(45.0), pytest.approx(58.5))          # цель +30% от нового входа


async def test_stop_needs_confirmed_price(monkeypatch):
    """Регрессия 9.11: стоп по цене DexScreener / старой сделке (Jupiter монету ещё не знает) — 12 ложных стопов."""
    monkeypatch.setattr(pp, "EXIT_DELAY_S", 0)
    p = pos(entry=40.0, target=80.0)
    tr = bare_trader({"M": fresh_state(30.0, age_s=60)}, FakeExt(3_000))     # свой поток: сделка минуту назад
    tr.exit_fixed, tr.false_exits = 0, 0
    sold = []

    async def fake_sell(pp_, reason, share, mc):
        sold.append((reason, mc))
    tr._sell = fake_sell
    tr.lock = __import__("asyncio").Lock()
    tr.current_mc = lambda p_: __import__("asyncio").sleep(0, result=30.0)
    p.closing = True
    await tr._exit_later(p, "stop", 1.0)
    assert not sold and tr.false_exits == 1                                 # цена не подтверждена — не продаём
    tr.e.states = {"M": fresh_state(30.0, age_s=2)}                         # свежая сделка своего потока
    p.closing = True
    await tr._exit_later(p, "stop", 1.0)
    assert sold == [("stop", 30.0)]
