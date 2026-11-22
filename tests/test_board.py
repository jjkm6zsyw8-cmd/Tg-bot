"""Доска кошельков: три секции по рейтингу копий, кошелёк — одной кнопкой, карточка с цифрами и «за ним»."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from scanner.bot import board as B
from scanner.db.pool import Database

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


async def test_board_sections_buttons_card_and_follow():
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = datetime.now(timezone.utc)
        star, mid, bad = "7HcK" + "1" * 36 + "U9Vt", "nogZ" + "2" * 35 + "gmNz", "8vsw" + "3" * 36 + "bPgt"
        for a, label in ((star, "звезда"), (mid, None), (bad, "скальп")):
            await db.execute("INSERT INTO watchlist(kind, address, label, created_at) VALUES ('trader', $1, $2, $3)",
                             a, label, now - timedelta(days=1))
        await db.execute("""INSERT INTO traders(address, win_rate, trades_30d, median_hold_s, copy_30d, copy_t_30d)
                            VALUES ($1, 0.79, 19, 180, 0.14, 2.1)""", star)
        await db.execute("INSERT INTO wallet_rating(address, tier, copies, wins) VALUES ($1, 'top', 6, 4), "
                         "($2, 'paused', 5, 1)", star, bad)
        for i, pct in enumerate((0.3, 0.2, -0.05)):
            await db.execute(
                """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                       tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet)
                   VALUES ('H', $1, now(), now(), 30, 0.1, 1, 1, 'take_profit', 0, $2, $3)""", f"S{i}", pct, star)
        await db.execute(
            """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                   tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet)
               VALUES ('A', 'S0', now(), now(), 30, 0.1, 1, 1, 'take_profit', 0, 0.9, $1)""", star)   # та же монета
        for i, (buy, sol) in enumerate(((True, 1.0), (False, 1.5))):
            await db.execute("""INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol,
                                    ts, pool) VALUES ($1, 'X', $2, $3, $4, 1000, 40, now(), 'pump')""",
                             f"t{i}", star, buy, sol)
        text, kb = await B.board_view(db, "best")
        assert "🟢 Лучшие — 1 из 3" in text and kb[0][1]["text"].startswith("● 🟢 Лучшие (1)")
        assert kb[0][0]["text"] == "🔥 Сейчас (0)" and len(kb[1]) == 2      # вкладки — в два ряда
        assert kb[2][0]["text"].startswith("🟢 звезда · 67% · +$4.50 · ") and kb[2][0]["callback"] == f"bd:w:{star}"
        assert (await B.board_view(db))[0].startswith("🏆 <b>Доска кошельков</b> · 🟢 Лучшие")   # «Сейчас» пуст
        _t, kb_bad = await B.board_view(db, "bad")
        assert kb_bad[2][0]["text"].startswith("🔴 скальп")
        _t, kb_mid = await B.board_view(db, "mid")
        assert kb_mid[2][0]["callback"] == f"bd:w:{mid}"                  # новый — без копий
        card, ckb = await B.card_view(db, {"real_stake_usd": 5}, star)
        assert "удачных 79% · сегодня 2" in card and "держит ~3м" in card and "+0.50 SOL" in card
        assert "Копии на бумаге ($10): 3 сд., в плюсе 2 (67%), итог <b>+$4.50</b>" in card   # одна копия на монету
        assert "твой кошелёк (ставка $5) ≈ +$2.25" in card and "t=2.1" in card and "⚪ нет" in card
        cbs = [b.get("callback") for row in ckb for b in row]
        assert f"bd:f:{star}:shadow" in cbs and f"bd:f:{star}:live" in cbs and all(len(c.encode()) <= 64 for c in cbs if c)
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, pnl_sol,
                   pnl_pct) VALUES ('shadow', 'copy', 'Q', $1, now(), now(), 0.05, 1, 0.01, 0.2)""", star)
        await db.kv_set("sol_usd", {"price": 100.0})
        card, _ = await B.card_view(db, {}, star)
        assert "тень за 14 дн: 1 сд., в плюсе 1, в среднем +20.0%, итог +$1.00" in card
        await B.set_follow(db, star, "live")
        card, _ = await B.card_view(db, {}, star)
        assert "💵 бой за ним" in card and (await db.kv_get(B.FOLLOW_KEY)) == {star: "live"}
        await B.set_follow(db, star, None)
        assert (await db.kv_get(B.FOLLOW_KEY)) == {}
        text, kb = B.live_confirm({}, star)
        assert "реальные деньги" in text and kb[0][0]["callback"] == f"bd:f:{star}:live!"
    finally:
        await db.close()


async def test_mid_section_sorted_by_copyability_and_find():
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        for a, copy in (("aaaa" + "1" * 36 + "zzzz", 0.02), ("6d7y" + "2" * 36 + "vAdy", 0.21), ("bbbb" + "3" * 36 + "yyyy", None)):
            await db.execute("INSERT INTO watchlist(kind, address) VALUES ('trader', $1)", a)
            if copy is not None:
                await db.execute("INSERT INTO traders(address, copy_30d) VALUES ($1, $2)", a, copy)
        _t, kb = await B.board_view(db, "mid")
        assert [r[0]["callback"][5:9] for r in kb[2:5]] == ["6d7y", "aaaa", "bbbb"]   # лучшие по копируемости — первыми
        assert [w["address"][:4] for w in await B.find(db, "6d7y")] == ["6d7y"]
        assert await B.find(db, "zzzz") and not await B.find(db, "nope")
    finally:
        await db.close()


async def test_rating_of_holder_uses_follow_copies():
    """Держатель с 3+ копиями «за трейдером» — рейтинг по ним, а не по старым копиям G (выход через 5 мин)."""
    from scanner.analytics import rating
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        rows = [("G", -0.08, None)] * 6 + [("H", 0.25, "hold")] * 3
        for i, (port, pct, rule) in enumerate(rows):
            await db.execute(
                """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                       tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet, rule)
                   VALUES ($1, $2, now(), now() - make_interval(secs => $5), 30, 0.1, 1, 1, 'x', 0, $3, 'HOLDER', $4)""",
                port, f"M{i}", pct, rule, float(i))
        r = (await rating.compute(db, {}))["HOLDER"]
        assert r["n"] == 3 and r["tier"] in ("ok", "top") and r["sum"] > 0
    finally:
        await db.close()


async def test_now_section_30d_and_day_money_and_top():
    """22.11: итог при $10 по лучшему выходу за 30 дней и за сутки; «🔥 Сейчас» — плюс за 30 дн и за сутки; /top."""
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        hot, cold, weak = "HoTw" + "1" * 36 + "aaaa", "CoLd" + "2" * 36 + "bbbb", "WeaK" + "3" * 36 + "cccc"
        g = {"G": {"n": 40, "w": 25, "avg": 0.12, "sum": 4.8, "t": 2.5}}
        for a, day in ((hot, {"G": {"n": 6, "w": 5, "avg": 0.2, "sum": 1.2, "t": 2.0}}),
                       (cold, {"G": {"n": 5, "w": 1, "avg": -0.1, "sum": -0.5, "t": -1.0}}), (weak, {})):
            await db.execute("INSERT INTO watchlist(kind, address) VALUES ('trader', $1)", a)
            await db.execute("INSERT INTO traders(address, rules_30d, rules_1d, best_rule) VALUES ($1, $2, $3, $4)",
                             a, g, day, None if a == weak else "G")
        text, kb = await B.board_view(db)                                 # по умолчанию — «Сейчас»
        assert "🔥 Сейчас — 1 из 3" in text and kb[0][0]["text"] == "● 🔥 Сейчас (1)"
        assert kb[2][0]["text"].startswith("🔥 HoTw…aaaa · G · 30д +$48 · 24ч +$12 · ")
        _t, kb_best = await B.board_view(db, "best")
        assert kb_best[2][0]["text"].startswith("🟢 CoLd…bbbb · G · 30д +$48 · 24ч −$5.00")
        card, _ = await B.card_view(db, {}, hot)
        assert "Итог при $10 на сделку по G: 30 дн <b>+$48</b> · сутки <b>+$12</b> (6 сд.)" in card
        top, tkb = await B.top_view(db)
        rows = top.split("<pre>")[1].split("</pre>")[0].splitlines()
        assert rows[1].split() == ["HoTw…aaaa", "G", "+$48", "+$12", "6"]
        assert rows[2].split() == ["CoLd…bbbb", "G", "+$48", "−$5", "5"] and len(rows) == 3   # без лучшего — нет
        assert tkb[0][0]["callback"] == f"bd:w:{hot}" and tkb[-1][1]["callback"] == "bd:t"
    finally:
        await db.close()
