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
        # 24.11: на кнопке — только реальное (итог реального бота за ним за 7 дней)
        assert kb[2][0]["text"].startswith("🟢 звезда · реальных сделок нет · ") and kb[2][0]["callback"] == f"bd:w:{star}"
        assert (await B.board_view(db))[0].startswith("🏆 <b>Доска кошельков</b> · 🟢 Лучшие")   # «Сейчас» пуст
        _t, kb_bad = await B.board_view(db, "bad")
        assert kb_bad[2][0]["text"].startswith("🔴 скальп")
        _t, kb_mid = await B.board_view(db, "mid")
        assert kb_mid[2][0]["callback"] == f"bd:w:{mid}"                  # новый — без копий
        card, ckb = await B.card_view(db, {"real_stake_usd": 5}, star)
        assert "удачных 79% · сегодня 2" in card and "держит ~3м" in card and "⚪ нет" in card
        assert "📊 <b>Реальный бот за ним</b>" in card and "сделок не было — реальных цифр по нему пока нет" in card
        assert "Копии на бумаге" not in card and "≈" not in card and "t=2.1" not in card     # расчётов нет
        cbs = [b.get("callback") for row in ckb for b in row]
        assert f"bd:f:{star}:shadow" in cbs and f"bd:f:{star}:live" in cbs and all(len(c.encode()) <= 64 for c in cbs if c)
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, pnl_sol,
                   pnl_pct) VALUES ('shadow', 'copy', 'Q', $1, now(), now(), 0.05, 1, 0.01, 0.2)""", star)
        await db.kv_set("sol_usd", {"price": 100.0})
        card, _ = await B.card_view(db, {}, star)
        assert "👻 тень · копия: 1 сд., в плюсе 1, в среднем +20.0%, итог <b>+$1.00</b>" in card
        _t, kb = await B.board_view(db, "best")
        assert kb[2][0]["text"] == "🟢 звезда · реально 7д +$1.00 (1 сд)"
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
        # 23.11: копии по его лучшему выходу (best:A) — тоже его рейтинг, а не smart-копии по G
        await db.execute("UPDATE paper_positions SET rule = 'best:A' WHERE rule = 'hold'")
        r = (await rating.compute(db, {}))["HOLDER"]
        assert r["n"] == 3 and r["tier"] in ("ok", "top")
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
        assert kb[2][0]["text"].startswith("⚪ HoTw…aaaa · реальных сделок нет")       # 24.11: только реальное
        card, _ = await B.card_view(db, {}, hot)
        assert "Итог при $10" not in card and "≈" not in card
        per = {"today": {"N": 3, "rules": {"G": {"n": 3, "avg": 0.1}}}, "yday": {"N": 0, "rules": {}},
               "week": {"N": 40, "rules": {"G": {"n": 40, "avg": 0.12}}},
               "month": {"N": 100, "rules": {"G": {"n": 40, "avg": 0.12}}}}
        await db.execute("UPDATE traders SET rules_periods = $2, rules_days = 16 WHERE address = $1", hot, per)
        top, tkb = await B.top_view(db)
        assert top.startswith("📊 <b>Реальный бот за кошельками</b>") and "реальных цифр нет" in top
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, pnl_sol,
                   pnl_pct, extra) VALUES ('shadow', 'mydev', 'Q1', $1, now(), now(), 0.05, 1, -0.01, -0.2, '{}'),
                   ('shadow', 'copy', 'Q2', $1, now(), now(), 0.05, 1, 0.01, 0.2, '{"followed": true}')""", hot)
        card, _ = await B.card_view(db, {}, hot)
        assert "👻 тень · мой дев: 1 сд., в плюсе 0" in card
        assert "👻 тень · копия: 1 сд., из них «за ним» 1, в плюсе 1" in card
        top, tkb = await B.top_view(db)
        assert "<b>HoTw…aaaa</b>: +$0 · 2 сд, в плюсе 1" in top                      # реальный бот за 7 дней
        assert tkb[0][0]["callback"] == f"bd:w:{hot}" and tkb[-1][1]["callback"] == "bd:t"
    finally:
        await db.close()


async def test_mine_view_and_reset():
    """22.11: «👛 Мои кошельки» — бот за выбранными и пробы выходов, $10 на сделку, с «Считать с нуля»."""
    from scanner.real import report as R
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        text, _ = await R.mine_view(db, {})
        assert "Пока ни за кем" in text
        await db.kv_set("real:follow", {"MINEaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1": "shadow",
                                        "BOTzbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2": "shadow"})
        await db.kv_set("real:follow_auto", ["BOTzbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2"])
        me = "MINEaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
        await db.execute(
            """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, pnl_sol,
                   pnl_pct) VALUES ('shadow', 'copy', 'Q1', $1, now(), now(), 0.05, 1, 0.02, 0.4),
                   ('shadow', 'copy', 'Q2', $1, now(), now(), 0.05, 1, -0.01, -0.1),
                   ('shadow', 'copy', 'Q3', $1, now(), NULL, 0.05, 1, NULL, NULL)""", me)
        for rule, pct in (("A", 0.2), ("R", 0.5)):
            await db.execute(
                """INSERT INTO real_rule_trials(mode, kind, rule, mint, trader, cost_sol, tokens_raw, closed_at, pnl_pct)
                   VALUES ('shadow', 'copy', $1, 'Q1', $2, 0.05, 1, now(), $3)""", rule, me, pct)
        text, kb = await R.mine_view(db, {})
        assert "👤 <b>Выбрал ты</b>\n👻 <b>MINE…aaa1</b>: 2 сд (+1 откр.), в плюсе 1 · бот <b>+$3.00</b>" in text
        assert "выходы: A +$2.00 · R +$5.00" in text and "🤖 <b>Бот включил сам</b>" in text
        assert kb[0][1]["callback"] == "rl:zero:mine"
        async def reload():
            return {}
        r = await R.on_callback(db, reload, "zero:mine")
        assert r.kb[0][1]["callback"] == "rl:mine"
        r = await R.on_callback(db, reload, "zero!:mine")
        assert "сделок пока нет" in r.text and "с " in r.text.splitlines()[0]
    finally:
        await db.close()


async def test_top_shows_totals_worst_and_same_names_apart():
    """25.11: /top — итог по всем кошелькам и худшие (а не только лучшие); одинаковые имена — с началом адреса."""
    from scanner.analytics import wallet_rules as WR
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        a, b, c = "AAAA" + "1" * 36 + "aaaa", "BBBB" + "2" * 36 + "bbbb", "CCCC" + "3" * 36 + "cccc"
        for addr in (a, b):
            await db.execute("INSERT INTO watchlist(kind, address, label) VALUES ('trader', $1, 'топ на 3 монет')", addr)
        for i, (addr, pnl) in enumerate(((a, 0.03), (b, 0.02), (c, -0.1))):
            await db.execute(
                """INSERT INTO real_positions(mode, kind, mint, trader, opened_at, closed_at, cost_sol, tokens, pnl_sol,
                       pnl_pct, extra) VALUES ('shadow', 'copy', $1, $2, now(), now(), 0.05, 1, $3, 0.1, '{}')""",
                f"M{i}", addr, pnl)
        text, items = await WR.top_text(db, 2)
        assert "Всего: 3 кошельков · 3 сд · итог −$8 · в плюсе 2, в минусе 1" in text, text
        assert "<b>топ на 3 монет AAAA</b>" in text and "<b>топ на 3 монет BBBB</b>" in text
        assert "Худшие:\n<b>CCCC…cccc</b>: −$15" in text and [x[0] for x in items] == [a, b]
    finally:
        await db.close()
