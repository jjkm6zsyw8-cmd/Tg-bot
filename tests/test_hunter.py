"""Режим «охотник» (25.11, v2 по сводному ТЗ 26.11): задержка входа, правило на первой половине, $ в день, всплеск,
слив на копировщиков, списки «в бой» / «наблюдение» / архив, «остыл» по t, допуск к бою после 24 ч тени, /hunt,
исторический прогон."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from scanner.analytics import copymodel as M, hunter as H
from scanner.analytics.curve import Costs
from tests.test_copymodel import DEV, ME, OTHER, T0, db, put_coin, put_trade, tk  # noqa: F401 — фикстура db

NOW = 1_795_000_000.0
OLD = H.Bar(min_coins=15, min_t=0.0)          # маленькая планка — на маленьких синтетических данных
NOPOOL = H.Params(delay=1.36, min_pool=0.0, sol_usd=150.0)


@pytest.fixture(autouse=True)
def _small_bar(monkeypatch):
    monkeypatch.setattr(H, "BAR", OLD)


def copy(mint: str, res: float | None, ts: float = NOW, burst: bool = False, why: str | None = None):
    return M.Entry(mint, ts, 1, 1.0), M.Copy(res, why if why else (None if res is not None else "нет цены выхода"),
                                             "take_profit", burst=burst)


# ====================================================================== критерии, правило, метрики
def test_hot_criteria_and_new_bar():
    good = [copy(f"M{i}", 0.2 if i % 4 else -0.1) for i in range(20)]
    s = H.stats_of("W", good, [])
    assert s.coins_all == 20 and s.win == 0.75 and s.avg == pytest.approx(0.125) and s.hot() is None
    assert H.stats_of("W", good[:14], []).hot().startswith("монет 14")
    s.rule_test = -0.02
    assert s.hot().startswith("правило A на второй половине")            # A2: на второй половине — минус
    s.rule_test, s.dump = 0.05, 0.4
    assert s.hot().startswith("сливает на копировщиков")                  # C2
    assert H.Bar() == OLD and H.bar_of({}) == OLD                           # 27.11: мягкая планка по умолчанию
    assert H.bar_of({"hunter_min_coins": 50, "hunter_min_t": 3}) == H.STRICT_BAR
    new = H.STRICT_BAR
    assert new.min_coins == 50 and new.min_t == 3.0
    many = [copy(f"M{i}", 0.3 if i % 3 else -0.1) for i in range(60)]
    assert H.stats_of("W", many, []).hot(new) is None
    shaky = [copy(f"M{i}", 0.9 if i % 5 < 3 else -0.95) for i in range(55)]
    assert H.stats_of("W", shaky, []).hot(new).startswith("t ")


def test_small_pool_copies_are_not_counted():
    cs = [copy(f"M{i}", 0.2) for i in range(16)] + [copy(f"S{i}", None, why=M.SMALL_POOL) for i in range(10)]
    s = H.stats_of("W", cs, [])
    assert s.small_pool == 10 and s.total == 16 and s.coins_all == 16 and s.noprice == 0.0


def test_pick_rule_on_first_half_checks_second():
    ents = [NOW + 60 * i for i in range(20)]
    by = {"A": [copy(f"M{i}", 0.05, ts) for i, ts in enumerate(ents)],
          "B": [copy(f"M{i}", 0.30 if i < 10 else -0.10, ts) for i, ts in enumerate(ents)],
          "G": [copy(f"M{i}", 0.10, ts) for i, ts in enumerate(ents)]}
    rule, train, test = H.pick_rule(by)
    # B лучше на первой половине (+30%), но на второй −10% — хуже A (+5%): проверка не прошла, остаётся A
    assert rule == "A" and train == pytest.approx(0.05) and test == pytest.approx(0.05)
    by["B"] = [copy(f"M{i}", 0.30 if i < 10 else 0.20, ts) for i, ts in enumerate(ents)]
    assert H.pick_rule(by)[0] == "B"                                       # и на второй лучше — берём B
    assert H.pick_rule({"A": by["A"][:3]}) == ("A", None, None)


def test_entry_by_our_delay_and_burst():
    ticks = [tk(0, 50, 100, ME), tk(0.5, 54, 101), tk(1.2, 58, 103), tk(2.0, 70, 105), tk(20, 75, 150)]
    e = M.Entry("MINT", T0, 100, 1.0)
    assert M.entry_by_delay(e, ticks, 1.36).mc == 58                       # худшая цена за 1.36 с
    assert M.entry_tick(e, ticks).mc == 54                                 # блок +2 — только сделка блока +1
    c = M.copy_trade(e, ticks, ME, DEV, T0 - 60, entry_delay_s=1.36)
    assert c.burst                                                         # +25% от 58 за 30 с: 75 ≥ 72.5
    flat = [tk(0, 50, 100, ME), tk(10, 52), tk(29, 55)]
    assert not M.copy_trade(e, flat, ME, DEV, T0 - 60, entry_delay_s=1.36).burst


def test_min_pool_and_depth_stake():
    e = M.Entry("MINT", T0, 100, 1.0)
    low = [tk(0, 50, 100, ME), tk(1, 50, 101), tk(40, 80, 160)]
    assert M.copy_trade(e, low, ME, DEV, T0 - 60, min_pool_sol=30).why == M.SMALL_POOL   # MC 50 → ~10 SOL в кривой
    assert M.pool_sol(112.0, "pump") == pytest.approx(30, abs=0.3)
    deep = [tk(0, 200, 100, ME), tk(1, 200, 101), tk(40, 300, 160)]
    c = M.copy_trade(e, deep, ME, DEV, T0 - 60, min_pool_sol=30, stake_share=0.03)
    assert c.res is not None and c.stake == pytest.approx(0.03 * M.reserves(200, "pump")[0])


def test_dump_detector():
    e = M.Entry("C1", T0, 100, 1.0)
    p = M.Prepared(ME, [e], {"C1": [tk(0, 50, 100, ME), tk(2, 55, who="b1"), tk(4, 60, who="b2"), tk(6, 66, who="b3")]},
                   {}, {}, 1800)
    sells = [{"mint": "C1", "ts": T0 + 20, "is_buy": False}]
    assert H.dump_share(p, sells) == 1.0                                   # продал через 14 с после 3-го покупателя
    assert H.dump_share(p, [{"mint": "C1", "ts": T0 + 300, "is_buy": False}]) == 0.0


def test_lists_and_cooling():
    assert H.list_for(60, 55.0) == ("battle", None)
    assert H.list_for(30, 55.0) == ("watch", None)                         # $ есть, но копий < 50
    assert H.list_for(30, 16.0) == ("watch", None)
    assert H.list_for(25, 5.0)[0] == "archive" and "< $15" in H.list_for(25, 5.0)[1]
    assert H.list_for(5, 5.0) == ("watch", None)                           # мало данных — в «наблюдении», в тени
    assert H.list_for(19, 0.0) == ("watch", None)
    assert H.cool_reason(12, -2.5, NOW, NOW - 3600, NOW).startswith("12 сделок")
    assert H.cool_reason(12, -1.0, NOW, NOW - 3600, NOW) is None           # минус, но неустойчивый — не снимаем
    assert H.cool_reason(3, 1.0, NOW - 13 * 3600, NOW - 20 * 3600, NOW) == "нет покупок 12 ч"


def test_forward_stops_on_steady_minus():
    t0 = NOW
    good = [copy(f"G{i}", 0.1, t0 + 600 * i) for i in range(5)]
    bad = [copy(f"B{i}", -0.2 - 0.01 * (i % 3), t0 + 3600 + 300 * i) for i in range(20)]
    f = H.forward(good + bad, [{"ts": t0 + 3600 * 5, "is_buy": True}], t0, t0 + 6 * 3600)
    assert f["cooled"].startswith("устойчивый минус") and f["n"] < 25
    idle = H.forward([copy("A", 0.1, t0 + 60), copy("B", 0.1, t0 + 14 * 3600)], [], t0, t0 + 20 * 3600)
    assert idle["n"] == 1 and idle["cooled"] == "нет покупок 12 ч"


def test_alert_text_has_money_burst_and_buttons():
    s = H.stats_of("W", [copy("A", 0.3, burst=True), copy("B", 0.1)], [{"mint": "A", "ts": NOW - 30, "is_buy": True}],
                   {"A": "AAA"})
    s.usd_day, s.usd_day_max, s.per_day, s.rule, s.rule_test, s.block2_avg = 52.0, 140.0, 40.0, "B", 0.12, 0.18
    text, kb = H.alert_text(s, NOW, NOPOOL)
    assert "$52 в день" in text and "$140 при 3% пула" in text and "Всплеск (+25% за 30 с): 50%" in text
    assert "правило <b>B</b>" in text and "🟢 активен" in text and "через 1.36 с" in text
    assert "→ 👻 тень и «наблюдение»" in text and "пришлю кнопку «Бой 0.14»" in text
    assert [b["callback"] for b in kb[0]] == ["hn:i:W"]                    # найденный — сразу в тени, денег нет
    assert "Сделок в день: 0 · баланс ?" in text
    s.trades_day, s.balance = 340.0, 0.01
    assert "🤖 <b>бот: 340 сделок/день, баланс ~0</b>" in H.alert_text(s, NOW, NOPOOL)[0]
    s.trades_day, s.balance = 12.0, 109.3
    assert "Сделок в день: 12 · баланс 109.30 SOL" in H.alert_text(s, NOW, NOPOOL)[0]
    text, kb = H.ready_text("W", {"n": 7, "avg": 0.05, "sol": 0.03}, 44.0)
    assert "Можно в бой" in text and [b["callback"] for b in kb[0]] == ["hn:l:W", "hn:i:W"]
    addr = "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5"
    assert all(len(f"hn:{a}:{addr}".encode()) <= 64 for a in "slLix")


# ====================================================================== база
async def hot_wallet(db, addr=ME, n=16, own=False, bundled=False, hours_ago=20.0):
    """n монет: покупка → через 20 с чужая покупка на +40% (тейк A) → его продажа через 40 с."""
    now = datetime.now(timezone.utc)
    for i in range(n):
        m = f"H{i:03d}{addr[:3]}" + "h" * 34
        t = now - timedelta(hours=hours_ago - i)
        await put_coin(db, m, t - timedelta(minutes=2), dev=addr if own and i == 0 else DEV)
        await db.execute("UPDATE tokens SET created_slot = $2 WHERE mint = $1", m, 1000 + i * 100 if bundled else 1)
        await put_trade(db, m, addr, t, True, 50, 1000 + i * 100)
        await put_trade(db, m, OTHER, t + timedelta(seconds=20), True, 70, 1000 + i * 100 + 40)
        await put_trade(db, m, addr, t + timedelta(seconds=40), False, 65, 1000 + i * 100 + 80)


class FakeRpc:
    """RPC для отсева ферм: подписи и транзакции кошельков (по умолчанию — пусто, кошелёк чистый)."""
    def __init__(self, sigs=None, txs=None):
        self.sigs, self.txs, self.calls = sigs or {}, txs or {}, 0

    async def signatures_for(self, addr, limit=100, before=None, until=None):
        self.calls += 1
        return self.sigs.get(addr, [])

    async def transaction(self, sig):
        self.calls += 1
        return self.txs.get(sig)

    async def call(self, method, params, retries=2):
        return {"context": {}, "value": 2_500_000_000} if method == "getBalance" else None


def transfer_tx(src, dst, extra_program=None):
    ixs = [{"programId": "ComputeBudget111111111111111111111111111111", "parsed": None},
           {"programId": "11111111111111111111111111111111", "program": "system",
            "parsed": {"type": "transfer", "info": {"source": src, "destination": dst, "lamports": 10_000_000}}}]
    if extra_program:
        ixs.append({"programId": extra_program, "accounts": [], "data": ""})
    return {"meta": {"err": None, "logMessages": []}, "transaction": {"message": {
        "accountKeys": [{"pubkey": src, "signer": True}], "instructions": ixs}}}


class Engine:
    def __init__(self, db, rpc=None, **cfg):
        self.db, self._cfg, self.sent = db, {"trader_min_trades_30d": 5, "real_min_pool_sol": 0, **cfg}, []
        self.rpc = rpc if rpc is not None else FakeRpc()
        orig = db.enqueue

        async def enqueue(kind, payload, priority=5):
            self.sent.append(payload)
            return await orig(kind, payload, priority)
        db.enqueue = enqueue

    @property
    def cfg(self):
        return self._cfg

    def costs(self):
        return Costs()


async def test_scan_finds_hot_wallet_once_and_skips_dev_and_bundles(db):
    await hot_wallet(db, ME)
    devw, bund = "DEVW" + "1" * 40, "BUND" + "2" * 40
    await hot_wallet(db, devw, own=True)
    await hot_wallet(db, bund, bundled=True)
    now = time.time()
    cands = {c["trader"]: c for c in await H.candidates(db, now - 72 * 3600, now)}
    assert cands[ME]["coins"] == 16 and cands[devw]["own"] == 1 and cands[bund]["bundled"] == 16
    e = Engine(db, hunter_on=True)
    hot = await H.Hunter(e).scan(now)
    assert [s.address for s in hot] == [ME]
    s = hot[0]
    assert s.coins_all == 16 and s.win == 1.0 and 0.2 < s.avg < 0.4 and s.usd_day > 0 and s.rule in H.RULES
    alerts = [p for p in e.sent if "горячий кошелёк" in p["text"]]
    assert len(alerts) == 1 and "в день" in alerts[0]["text"]
    assert s.balance == 2.5 and "баланс 2.50 SOL" in alerts[0]["text"] and s.trades_day > 0
    assert "⚠️ оценка не сверена с тенью" in alerts[0]["text"]                 # пока модель не сверена с тенью
    assert H.unchecked({"hunter_model_checked": True}) == "" and "не сверена" in await H.hunt_text(db, 24)
    r = await db.fetchrow("SELECT status, list, rule, usd_day, followed_at FROM hunter_wallets WHERE address = $1", ME)
    # 27.11: найденный — сразу в тень и «наблюдение»; денег нет
    assert r["status"] == "shadow" and r["list"] == "watch" and r["rule"] == s.rule and r["usd_day"] > 0
    assert r["followed_at"] is not None and (await db.kv_get("real:follow"))[ME] == "shadow"
    assert ME not in (await db.kv_get("real:follow_stake") or {})
    assert await H.Hunter(e).scan(now) == []
    await db.execute("DELETE FROM hunter_wallets")
    assert await H.Hunter(e).scan(now + 13 * 3600) == []                  # 27.11: 12+ ч без покупок — не берём
    assert H.idle_reason(now - 3600, now) is None and H.idle_reason(None, now) == "нет покупок 12 ч"


async def test_track_lists_copies_and_battle_gate(db):
    """После находки — новые сделки: копии бессрочно (hunter_copies), список по $ в день; в бой — только после 24 ч
    тени (hunter:battle_ok)."""
    await hot_wallet(db, ME, hours_ago=60)
    e = Engine(db, hunter_on=True)
    h = H.Hunter(e)
    await h.scan(time.time() - 44 * 3600)                                # поиск тогда, когда он торговал
    await db.execute("UPDATE hunter_wallets SET found_at = now() - interval '50 hours' WHERE address = $1", ME)
    await h.track(time.time())
    r = await db.fetchrow("SELECT list, n_after, avg_after, usd_day_after FROM hunter_wallets WHERE address = $1", ME)
    assert r["n_after"] and r["avg_after"] > 0 and r["usd_day_after"] > 0
    assert await db.fetchval("SELECT count(*) FROM hunter_copies WHERE address = $1", ME) == r["n_after"]
    assert not await db.fetchval("SELECT count(*) FROM hunter_copies WHERE address = $1 AND hold_s IS NULL", ME)
    monkey = H.BATTLE_N, H.BATTLE_USD
    H.BATTLE_N, H.BATTLE_USD = 3, 1.0
    try:
        await h.track(time.time())
        assert await db.fetchval("SELECT list FROM hunter_wallets WHERE address = $1", ME) == "battle"
        assert "не в списке" not in (await H.battle_ready(db, ME) or "")
        assert "меньше 24 ч" in await H.battle_ready(db, ME)                # тень включена при находке
        await db.execute("UPDATE hunter_wallets SET followed_at = now() - interval '25 hours' WHERE address = $1", ME)
        assert "сделок" in await H.battle_ready(db, ME)                    # в тени ещё нет сделок
        for _ in range(5):
            await db.execute(
                """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens,
                       entry_mc, pnl_sol, pnl_pct) VALUES ('shadow', 'copy', 'M', $1, now() - interval '2 hours',
                       now(), 0.14, 1, 40, 0.01, 0.07)""", ME)
        assert await H.battle_ready(db, ME) is None
        ok = await h.battle_list(time.time())
        assert ME in ok and ME in (await db.kv_get(H.BATTLE_KEY))
        ready = [p for p in e.sent if "Можно в бой" in p["text"]]
        assert len(ready) == 1 and ready[0]["keyboard"][0][0]["callback"] == f"hn:l:{ME}"
        await h.battle_list(time.time())
        assert len([p for p in e.sent if "Можно в бой" in p["text"]]) == 1       # кнопка — один раз
    finally:
        H.BATTLE_N, H.BATTLE_USD = monkey


async def test_archive_by_results_stops_shadow(db):
    """27.11: шум отсеивает тень — 20+ копий после находки и меньше $15/день → архив, копирование снято."""
    await hot_wallet(db, ME, hours_ago=60)
    e = Engine(db, hunter_on=True)
    h = H.Hunter(e)
    await h.scan(time.time() - 44 * 3600)
    await db.execute("UPDATE hunter_wallets SET found_at = now() - interval '50 hours' WHERE address = $1", ME)
    monkey = H.ARCHIVE_MIN_N, H.WATCH_USD
    H.ARCHIVE_MIN_N, H.WATCH_USD = 3, 10_000.0
    try:
        await h.track(time.time())
    finally:
        H.ARCHIVE_MIN_N, H.WATCH_USD = monkey
    r = await db.fetchrow("SELECT list, status, cool_reason FROM hunter_wallets WHERE address = $1", ME)
    assert r["list"] == "archive" and r["status"] == "cooled" and "< $10000" in r["cool_reason"]
    assert ME not in (await db.kv_get("real:follow") or {})
    assert any("→ архив" in p["text"] and "снято" in p["text"] for p in e.sent)


def test_journey_moves_to_battle_after_24h_and_splits_copies():
    """До перевода в «в бой» — тень (копии считаются отдельно), перевод — по итогу после находки и 24 ч тени."""
    t0 = NOW
    cs = [copy(f"J{i}", 0.12 if i % 5 else -0.05, t0 + 600 + 1200 * i) for i in range(120)]   # 3 копии в час, 40 ч
    trades = [{"ts": e.ts, "is_buy": True} for e, _c in cs]
    j = H.journey(cs, trades, t0, t0 + 41 * 3600, 150.0)
    assert j["battle_at"] is not None and j["battle_at"] - t0 >= 24 * 3600          # не раньше 24 ч тени
    assert j["list"] == "battle" and len(j["pre"]) >= 50 and j["bat"] and j["out"] == 0
    assert len(j["pre"]) + len(j["bat"]) == j["n"] == 120
    assert all(e.ts < j["battle_at"] for e, _c in cs[:len(j["pre"])])
    weak = [copy(f"K{i}", 0.01 if i % 2 else -0.012, t0 + 600 + 1200 * i) for i in range(60)]
    k = H.journey(weak, [{"ts": e.ts, "is_buy": True} for e, _c in weak], t0, t0 + 21 * 3600, 150.0)
    assert k["list"] == "archive" and k["battle_at"] is None and k["n"] < 60 and "< $15" in k["why_not"]
    assert "до перевода" not in H.phase_text([], 150.0) and "$+25" in H.phase_text([0.6, 0.6], 150.0)


def test_once_per_coin_like_the_bot():
    """27.11: монета у нескольких — один раз, пока копия открыта; бой занимает только бой, тень — любая."""
    items = [(0, 60, "X", False, "a"), (10, 70, "X", False, "b"),        # второй кошелёк, копия по X ещё открыта
             (100, 130, "X", False, "c"),                                 # первая закрылась — новый вход
             (110, 200, "X", True, "d"),                                  # бой: тень его не занимает
             (150, 160, "X", False, "e"), (120, 125, "X", True, "f"),     # бой по X открыт — мимо и тени, и бою
             (0, 10, "Y", True, "g")]
    kept, dup = H.once_per_coin(items)
    assert [it[4] for it in kept] == ["a", "g", "c", "d"] and dup == 3


async def test_hunt_total_counts_coin_once(db):
    """/hunt: итог по всем кошелькам в тени — копии после находки (монета у двух — один раз), SOL и $ в день."""
    w1, w2 = "TOTA" + "1" * 40, "TOTB" + "2" * 40
    now = datetime.now(timezone.utc)
    for w in (w1, w2):
        await db.execute(
            """INSERT INTO hunter_wallets(address, found_at, status, list, followed_at)
               VALUES ($1, now() - interval '24 hours', 'shadow', 'watch', now() - interval '24 hours')""", w)
    for w, mint, ago, res in ((w1, "MA", 3600, 0.20), (w2, "MA", 3590, 0.10),    # MA у второго — копия открыта
                              (w1, "MB", 7200, -0.05), (w2, "MC", 9000, 0.15)):
        await db.execute(
            """INSERT INTO hunter_copies(address, mint, entry_at, rule, res, reason, hold_s)
               VALUES ($1, $2, $3, 'A', $4, 'take_profit', 120)""", w, mint, now - timedelta(seconds=ago), res)
    await db.execute(
        """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, entry_mc,
               pnl_sol, pnl_pct) VALUES ('shadow', 'copy', 'MA', $1, now() - interval '1 hour', now(), 0.14, 1, 40,
               0.02, 0.15)""", w1)
    text = await H.hunt_text(db, 48)
    assert "Итого за 2 кошельками" in text and "3 копий, в среднем +10.0%" in text and "убрано 1" in text
    assert "+0.042 SOL" in text and "/день" in text and "Бот за ними" in text and "1 сд, в среднем +15.0%" in text
    # 27.11 (/hunt: «без цены 24 из 44»): без цены — по причинам; моложе 35 мин — «ещё открыты», не «без цены»
    for mint, ago, why in (("MD", 5000, "нет цены входа"), ("ME", 6000, "нет цены входа"), ("MF", 600, "нет цены выхода")):
        await db.execute(
            """INSERT INTO hunter_copies(address, mint, entry_at, rule, res, reason, hold_s)
               VALUES ($1, $2, $3, 'A', NULL, $4, NULL)""", w1, mint, now - timedelta(seconds=ago), why)
    text = await H.hunt_text(db, 48)
    assert "без цены 2 (нет цены входа 2)" in text and "ещё открыты 1" in text


async def test_save_copies_drops_vanished_entries(db):
    """27.11: копия, которой при пересчёте больше нет (пыль, мусор pumpdata), из hunter_copies уходит; старше since —
    остаётся (сделки за ней могли уже удалиться по сроку хранения)."""
    now = time.time()
    mk = lambda mint, ago: (M.Entry(mint, now - ago, 1, 1.0), M.Copy(0.1, reason="take_profit"))   # noqa: E731
    await H.save_copies(db, ME, "A", [mk("OLD", 9000), mk("KEEP", 3000), mk("GONE", 2000)])
    await H.save_copies(db, ME, "A", [mk("KEEP", 3000)], since=now - 5000)
    left = {r["mint"] for r in await db.fetch("SELECT mint FROM hunter_copies WHERE address = $1", ME)}
    assert left == {"OLD", "KEEP"}


async def test_data_error_goes_to_archive_without_alert(db, monkeypatch):
    """27.11 (владелец: FUEL3… «$3 769/день»): больше $200/день при 0.14 SOL или в среднем больше +100% — архив
    «ошибка данных», без алерта и без тени; уже найденный с такой оценкой — тоже в архив, тень снимается."""
    assert H.data_error(0.5, 150) is None and H.data_error(-0.5, -900) is None
    assert "$3,769/день" in H.data_error(0.5, 3769) and "+150%" in H.data_error(1.5, 50)
    await hot_wallet(db, ME)
    e = Engine(db, hunter_on=True)
    h = H.Hunter(e)
    monkeypatch.setattr(H, "SANE_AVG", 0.1)
    assert await h.scan(time.time()) == []
    r = await db.fetchrow("SELECT list, status, list_reason FROM hunter_wallets WHERE address = $1", ME)
    assert r["list"] == "archive" and r["status"] == "ignored" and "ошибка данных" in r["list_reason"]
    assert not any("горячий кошелёк" in p["text"] for p in e.sent)
    assert any("ошибка данных у 1" in p["text"] for p in e.sent) and ME not in (await db.kv_get("real:follow") or {})
    # найденный раньше проверки: оценка при находке $3 769/день → track снимает тень и архивирует
    monkeypatch.setattr(H, "SANE_AVG", 1.0)
    await db.execute("DELETE FROM hunter_wallets")
    await h.scan(time.time())
    assert ME in (await db.kv_get("real:follow") or {})
    await db.execute("UPDATE hunter_wallets SET usd_day = 3769 WHERE address = $1", ME)
    await h.track(time.time())
    r = await db.fetchrow("SELECT list, status, list_reason FROM hunter_wallets WHERE address = $1", ME)
    assert r["list"] == "archive" and "$3,769/день" in r["list_reason"] and ME not in (await db.kv_get("real:follow") or {})
    assert any("🧪" in p["text"] and "копирование за ним снято" in p["text"] for p in e.sent)
    text = await H.explain_text(db, await H.params_of(db, e.cfg), ME)
    assert "Откуда $/день" in text and "$3,769/день" in text and "При находке" in text and "вход:" in text
    assert "самая высокая цена за удержание" in text
    assert "Откуда $/день" in await H.explain_text(db, await H.params_of(db, e.cfg), ME[:5])     # начало адреса


async def test_follow_uses_wallet_rule_and_cools_on_steady_minus(db):
    await hot_wallet(db, ME)
    e = Engine(db, hunter_on=True)
    h = H.Hunter(e)
    await h.scan(time.time())
    await db.execute("UPDATE hunter_wallets SET rule = 'G' WHERE address = $1", ME)
    await H.follow(db, e.cfg, Costs(), ME, "live")
    assert (await db.kv_get("real:follow"))[ME] == "live" and (await db.kv_get("real:follow_rule"))[ME] == "G"
    assert (await db.kv_get("real:follow_stake"))[ME] == H.STAKE_SOL
    for i in range(12):
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, entry_mc,
                   pnl_sol, pnl_pct) VALUES ('live', 'copy', 'M', $1, now(), now(), 0.14, 1, 40, -0.01, $2)""",
            ME, -0.07 - 0.01 * (i % 3))
    assert await h.watch(time.time()) == [ME]
    assert ME not in (await db.kv_get("real:follow") or {}) and not await db.kv_get("real:follow_stake")
    r = await db.fetchrow("SELECT status, cool_reason FROM hunter_wallets WHERE address = $1", ME)
    assert r["status"] == "cooled" and "устойчивый минус" in r["cool_reason"]
    assert any("🧊 <b>Остыл</b>" in p["text"] for p in e.sent)


async def test_dust_buys_do_not_keep_wallet_warm(db):
    """27.11: пылевой бот (покупки по 0.00002 SOL) в тени — для «остыл» его мелочь не покупки."""
    await hot_wallet(db, ME)
    e = Engine(db, hunter_on=True)
    h = H.Hunter(e)
    await h.scan(time.time())
    await H.follow(db, e.cfg, Costs(), ME, "shadow")
    await db.execute("UPDATE hunter_wallets SET followed_at = now() - interval '13 hours' WHERE address = $1", ME)
    await db.execute("DELETE FROM trades WHERE trader = $1", ME)
    await put_trade(db, "DUST", ME, datetime.now(timezone.utc), True, 50.0, 9, sol=0.00002, tokens=400)
    assert await h.watch(time.time()) == [ME]
    r = await db.fetchrow("SELECT status, cool_reason FROM hunter_wallets WHERE address = $1", ME)
    assert r["status"] == "cooled" and "нет покупок" in r["cool_reason"]


async def test_hunt_text_daily_weekly(db):
    await hot_wallet(db, ME)
    e = Engine(db, hunter_on=True)
    h = H.Hunter(e)
    await h.scan(time.time())
    text = await H.hunt_text(db, 24)
    assert "найден" in text and ME in text and "/день" in text
    await H.follow(db, e.cfg, Costs(), ME, "shadow")
    await db.execute(
        """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, entry_mc,
               pnl_sol, pnl_pct) VALUES ('shadow', 'copy', 'M', $1, now(), now(), 0.14, 1, 40, 0.02, 0.15)""", ME)
    # 27.11: утренний отчёт — в 10:00 по Тбилиси (06:00 UTC), раз в день, с окном до 14:00
    day = datetime.now(timezone.utc).replace(hour=6, minute=0, second=0, microsecond=0)
    assert await h.morning((day - timedelta(minutes=1)).timestamp()) is None          # 09:59 — ещё рано
    await db.kv_set("health:ingest", {"at": datetime.now(timezone.utc).isoformat(),
                                      "pumpdata": {"connected": True, "dropped": 0}})
    await db.kv_set("watch:state", {"connected": False, "fallback": True, "wallets_in": 3, "wallets": 3,
                                    "key": {ME: "down"}, "primary_error": "HTTP 429"})
    text = await h.morning(day.timestamp() + 60)
    assert "Охотник: утро" in text and "Нашёл за сутки: 1" in text
    assert "<b>в среднем +15.0% на сделку</b> (1 сд) · сумма +$3.00/день" in text
    assert f"Лучший: <code>{ME}</code> в среднем +15.0% на сделку (1 сд, +$3.00)" in text
    assert "pumpdata: ✅ подключён" in text and "слежка НЕ подключена (запасной узел)" in text and "HTTP 429" in text
    assert "без подписки 1" in text and "Диск: " in text and "база" in text
    assert await h.morning(day.timestamp() + 120) is None                               # второй раз за день — нет
    assert any("Охотник: утро" in p["text"] for p in e.sent)
    text = await h.weekly(time.time())
    assert "найдено горячих 1" in text and "в плюсе 1 из 1 кошельков" in text
    assert await h.weekly(time.time()) is None


async def test_hunter_off_does_not_scan(db):
    await hot_wallet(db, ME)
    await H.Hunter(Engine(db, hunter_on=False)).tick(time.time())
    assert not await db.fetchval("SELECT count(*) FROM hunter_wallets")


async def test_history_run(db):
    await hot_wallet(db, ME, n=20)
    text = await H.history(db, days=5, step_h=6, prm=NOPOOL, trace=ME)
    assert "Горячих нашлось бы: 1" in text and "кошельков с 15+ монетами (не дев, не бандл): 2" in text
    assert "Найден" in text and "Для сравнения вход «блок +2»" in text
    assert "Переведены в «в бой»" in text and "До перевода в «в бой» (тень, денег нет)" in text
    assert "из них ферм 0" in text
    sigs = [{"signature": f"h{i}", "err": None} for i in range(30)]
    rpc = FakeRpc({ME: sigs}, {f"h{i}": transfer_tx(ME, f"D{i}") for i in range(30)})
    text = await H.history(db, days=5, step_h=6, prm=NOPOOL, trace=ME, rpc=rpc)
    assert "Горячих по планке: 1, из них ферм 1" in text and "Горячих нашлось бы: 0" in text
    assert "профинансировал 21+ кошельков" in text


async def test_follow_stake_in_executor(db):
    from scanner.real import settings as S
    from tests.real_helpers import FakeChain
    from tests.test_real import shadow
    tr = shadow(db, FakeChain())
    tr.follow_stake = {"W": 0.14}
    sol_usd = tr.sol_usd()
    stake, why = tr.stake_lamports(10**15, "smart", "W")
    assert why is None and stake == int(min(0.14 * sol_usd, S.max_stake_usd()) / sol_usd * 1e9)


# ====================================================================== 27.11: разбор, методы, удаление
async def test_why_text_explains_each_buy_after_find(db):
    """/hunt why: покупки после находки и что с ними стало — модель, журнал копий, бот."""
    await hot_wallet(db, ME, hours_ago=60)
    e = Engine(db, hunter_on=True)
    await H.Hunter(e).scan(time.time() - 44 * 3600)
    await db.execute("UPDATE hunter_wallets SET found_at = now() - interval '50 hours' WHERE address = $1", ME)
    mint = await db.fetchval("SELECT mint FROM trades WHERE trader = $1 AND is_buy AND ts > now() - interval '49 hours' "
                             "ORDER BY ts LIMIT 1", ME)
    ts = await db.fetchval("SELECT ts FROM trades WHERE trader = $1 AND mint = $2 AND is_buy", ME, mint)
    await db.execute("INSERT INTO real_skips(at, mint, mode, kind, why) VALUES ($1, $2, 'shadow', 'copy', "
                     "'мелкий пул, не входим')", ts + timedelta(seconds=2), mint)
    text = await H.why_text(db, NOPOOL)
    assert ME in text and "покупок после находки:" in text and "модель: с ценой" in text
    assert "бот: мелкий пул, не входим — 1" in text and "ни сигнала, ни пропуска бота" in text
    assert "без фильтра пула с ценой было бы" in text and "до находки последняя покупка" in text
    assert await H.why_text(db, NOPOOL, wallet="NOPE") == "Такого кошелька у охотника нет."
    # 27.11 (владелец: «до бота не дошли — из какого источника покупки, подписан ли в слежке Helius?»)
    await db.kv_set("watch:state", {"connected": True, "wallets": 900, "wallets_in": 500, "coins_in": 200, "cap": 700,
                                    "cut_n": 400, "key": {ME: "cut"}})

    class Rpc:
        async def signatures_for(self, addr, limit=100):
            return [{"signature": "s", "blockTime": time.time() - 7200}]
    text = await H.why_text(db, NOPOOL, rpc=Rpc())
    assert "Слежка Helius: подключена, кошельков в подписке 500 из 900" in text and "не влезли 400" in text
    assert "не влез в лимит подписок" in text and "источник его покупок: ?" in text
    assert "в сети последняя транзакция" in text, text


async def test_pnl_has_trades_and_average_per_method(db):
    from scanner.real import stats as RS
    for pct in (0.10, -0.02, 0.06):
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, entry_mc,
                   pnl_sol, pnl_pct) VALUES ('shadow', 'swing', 'R', 'W', now() - interval '2 hours', now(), 0.1, 1, 40,
                   $1, $2)""", pct * 0.1, pct)
    lines = await RS.kind_lines(db, "shadow", 150.0)
    assert lines and "ракеты: 3 · +4.7% · 67%" in lines[1]


async def test_removewallet_by_prefix_also_stops_shadow(db):
    """/removewallet 7HcK Arkd — по началу адреса, несколько сразу; снимает и «за ним» реального бота."""
    from types import SimpleNamespace

    from aiogram import Router
    from aiogram.filters import CommandObject

    from scanner.bot import handlers
    from scanner.config import Env, Settings, YamlConfig
    a, b, c, d = "7HcK" + "a" * 40, "Arkd" + "b" * 40, "Abcd" + "c" * 40, "Abcd" + "d" * 40
    for x in (a, b, c, d):
        await db.execute("INSERT INTO traders(address, manual, active) VALUES ($1, true, true)", x)
    await db.kv_set("real:follow", {a: "shadow", c: "shadow"})
    router = Router()
    handlers.setup(router, Env(alert_chat_id="-100", allowed_user_ids="1"), Settings(YamlConfig("config/config.yaml")),
                   db, SimpleNamespace(chat_id=-100, thread=lambda t: None))
    cmd = next(h.callback for h in router.message.handlers if h.callback.__name__ == "cmd_removewallet")
    said = []

    async def answer(text, **kw):
        said.append(text)
    await cmd(SimpleNamespace(answer=answer), CommandObject(prefix="/", command="removewallet",
                                                            args="7HcK Arkd Abcd zzzz Ab"))
    text = "\n".join(said)
    assert f"Убран из слежки и тени «за ним»: <code>{a}</code>" in text and f"Убран из слежки: <code>{b}</code>" in text
    assert "Abcd: подходит несколько" in text and "zzzz: в слежке нет" in text and "Ab: в слежке нет" in text
    act = {r["address"]: r["active"] for r in await db.fetch("SELECT address, active FROM traders")}
    assert act == {a: False, b: False, c: True, d: True}
    assert await db.kv_get("real:follow") == {c: "shadow"}



# ====================================================================== 27.11: отсев ферм
def test_farm_scan_tx_counts_only_plain_transfers_and_fee_claims():
    from scanner.analytics import farm as F
    assert F.scan_tx(transfer_tx("ME", "W1"), "ME")["to"] == {"W1"}
    pump = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
    assert F.scan_tx(transfer_tx("ME", "TIP", pump), "ME")["to"] == set()          # перевод внутри сделки — не в счёт
    assert F.scan_tx(transfer_tx("OTHER", "ME"), "ME")["to"] == set()              # входящий — не в счёт
    fee = transfer_tx("ME", "X", pump)
    fee["meta"]["logMessages"] = [f"Program {pump} invoke [1]", "Program log: Instruction: CollectCreatorFee"]
    assert F.scan_tx(fee, "ME")["fee"] and not F.scan_tx(fee, "OTHER")["fee"]


async def test_farm_check_db_and_rpc(db):
    from scanner.analytics import farm as F
    w = "FARM" + "1" * 40
    assert await F.check(db, FakeRpc(), w) == (None, True)
    assert await F.check(db, None, w) == (None, False)                             # без RPC — не проверен
    sigs = [{"signature": f"s{i}", "err": None} for i in range(25)]
    rpc = FakeRpc({w: sigs}, {f"s{i}": transfer_tx(w, f"D{i}") for i in range(25)})
    why, done = await F.check(db, rpc, w)
    assert done and why == "ферма: профинансировал 21+ кошельков" and rpc.calls == 22     # остановился на 21-м
    await db.execute("INSERT INTO tokens(mint, signature, deployer, current_owner, created_at, initial_mc_sol, source) "
                     "VALUES ('FM', 'sFM', $1, $1, now(), 28, 'rpc')", w)
    assert await F.check(db, FakeRpc(), w) == ("ферма: создал монет 1", True)
    assert (await F.check(db, FakeRpc(), w, over_budget=lambda: True))[0]          # по базе — и без бюджета


async def test_scan_archives_farm_and_track_drops_old_farm(db):
    """Ферма при находке — в архив «ферма», без тени и алерта о находке; найденный раньше — проверка в track."""
    await hot_wallet(db, ME)
    sigs = [{"signature": f"f{i}", "err": None} for i in range(30)]
    rpc = FakeRpc({ME: sigs}, {f"f{i}": transfer_tx(ME, f"D{i}") for i in range(30)})
    e = Engine(db, rpc=rpc, hunter_on=True)
    h = H.Hunter(e)
    assert await h.scan(time.time()) == [] and h.stats["farms"] == 1
    r = await db.fetchrow("SELECT list, status, farm FROM hunter_wallets WHERE address = $1", ME)
    assert r["list"] == "archive" and r["status"] == "ignored" and r["farm"].startswith("ферма: профинансировал")
    assert ME not in (await db.kv_get("real:follow") or {})
    assert any("отсеяно ферм 1" in p["text"] for p in e.sent) and not any("горячий кошелёк" in p["text"] for p in e.sent)
    assert await h.scan(time.time()) == []                                         # больше не предлагает
    # найден до отсева ферм (farm_at пусто), в тени → track проверяет, снимает тень
    await db.execute("UPDATE hunter_wallets SET list = 'watch', status = 'new', farm = NULL, farm_at = NULL")
    await H.follow(db, e.cfg, Costs(), ME, "shadow")
    await h.track(time.time())
    r = await db.fetchrow("SELECT list, status, farm FROM hunter_wallets WHERE address = $1", ME)
    assert r["list"] == "archive" and r["status"] == "ignored" and ME not in (await db.kv_get("real:follow") or {})
    assert any("🚜" in p["text"] and "снято" in p["text"] for p in e.sent)
    # свой /follow (со сроком) проверка не снимает — только сообщает
    from scanner.bot import board
    await db.execute("UPDATE hunter_wallets SET list = 'watch', status = 'shadow', farm = NULL, farm_at = NULL")
    await H.follow(db, e.cfg, Costs(), ME, "shadow")
    await board.set_follow_rule(db, ME, "G", time.time() + 3 * 86400)
    await h.track(time.time())
    assert (await db.kv_get("real:follow"))[ME] == "shadow"
    assert any("оставил" in p["text"] for p in e.sent)


async def test_scan_waits_when_farm_check_incomplete(db):
    await hot_wallet(db, ME)
    e = Engine(db, hunter_on=True)
    e.rpc = None                                                                    # узел не ответил
    h = H.Hunter(e)
    assert await h.scan(time.time()) == [] and h.stats["farm_unchecked"] == 1
    assert not await db.fetchval("SELECT count(*) FROM hunter_wallets")              # в следующий час — снова
    e.rpc = FakeRpc()
    assert [s.address for s in await h.scan(time.time())] == [ME]
    assert await db.fetchval("SELECT farm_at IS NOT NULL FROM hunter_wallets WHERE address = $1", ME)



async def test_warns_when_all_bot_wallets_off(db):
    """27.11: тень «за ним» идёт только при кошельке бота в «тени» или «бое» — иначе охотник прямо об этом говорит."""
    assert await H.bot_watching(db) is None
    await db.kv_set("real:desk", {"wallets": [{"id": 1, "mode": "off"}, {"id": 2, "mode": "off"}]})
    assert await H.bot_watching(db) is False and "Все кошельки бота выключены" in await H.hunt_text(db, 24)
    await hot_wallet(db, ME)
    e = Engine(db, hunter_on=True)
    await H.Hunter(e).scan(time.time())
    assert any("горячий кошелёк" in p["text"] and "Все кошельки бота выключены" in p["text"] for p in e.sent)
    await db.kv_set("real:desk", {"wallets": [{"id": 1, "mode": "off"}, {"id": 2, "mode": "shadow"}]})
    assert await H.bot_watching(db) is True and "выключены" not in await H.hunt_text(db, 24)
