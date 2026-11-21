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
        assert "🟢 Лучшие — 1 из 3" in text and kb[0][0]["text"].startswith("● 🟢 Лучшие (1)")
        assert kb[1][0]["text"].startswith("🟢 звезда · 67% · +$4.50 · ") and kb[1][0]["callback"] == f"bd:w:{star}"
        _t, kb_bad = await B.board_view(db, "bad")
        assert kb_bad[1][0]["text"].startswith("🔴 скальп")
        _t, kb_mid = await B.board_view(db, "mid")
        assert kb_mid[1][0]["callback"] == f"bd:w:{mid}"                  # новый — без копий
        card, ckb = await B.card_view(db, {"real_stake_usd": 5}, star)
        assert "удачных 79% · сегодня 2" in card and "держит ~3м" in card and "+0.50 SOL" in card
        assert "Копии на бумаге ($10): 3 сд., в плюсе 2 (67%), итог <b>+$4.50</b>" in card   # одна копия на монету
        assert "твой кошелёк (ставка $5) ≈ +$2.25" in card and "t=2.1" in card and "⚪ нет" in card
        cbs = [b.get("callback") for row in ckb for b in row]
        assert f"bd:f:{star}:shadow" in cbs and f"bd:f:{star}:live" in cbs and all(len(c.encode()) <= 64 for c in cbs if c)
        await B.set_follow(db, star, "live")
        card, _ = await B.card_view(db, {}, star)
        assert "💵 бой за ним" in card and (await db.kv_get(B.FOLLOW_KEY)) == {star: "live"}
        await B.set_follow(db, star, None)
        assert (await db.kv_get(B.FOLLOW_KEY)) == {}
        text, kb = B.live_confirm({}, star)
        assert "реальные деньги" in text and kb[0][0]["callback"] == f"bd:f:{star}:live!"
    finally:
        await db.close()
