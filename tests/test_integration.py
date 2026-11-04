"""Сквозной сценарий на реальном PostgreSQL (TEST_DATABASE_URL), RPC и Telegram подменены."""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scanner.analytics import clusters as cl
from scanner.analytics.engine import Engine
from scanner.analytics.feedback import Feedback
from scanner.analytics.loader import load_state
from scanner.bot.sender import Sender
from scanner.config import Env, Settings, YamlConfig
from scanner.db.pool import Database
from scanner.ingest.receiver import Receiver
from scanner.models import TokenCreated, Trade
from scanner.reports.backtest import run_backtest
from scanner.reports.daily import build_daily

from .helpers import Curve, addr, rand_addr

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


async def _db() -> Database | None:
    try:
        db = await Database(DSN).connect(min_size=1, max_size=10)
    except Exception:  # noqa: BLE001
        return None
    await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await db.migrate()
    return db


async def feed_launch(rec: Receiver, dev: str, created: datetime, slot: int, peak_buys: int = 12,
                      dump_after_s: float = 120, buy_sol: float = 1.5) -> str:
    """Запуск по сценарию «запуск, рост, дамп»: дев покупает, толпа поднимает MC, дев сливает всё."""
    mint = rand_addr()
    c = Curve()
    ts = created
    await rec.sink(TokenCreated(signature=f"c{mint[:20]}", mint=mint, deployer=dev, creator=dev, name="Coin",
                                symbol="COIN", uri="", slot=slot, ts=ts, source="helius", received_at=ts))
    dev_tokens = c.buy(2.0)
    await rec.sink(Trade(signature=f"c{mint[:20]}", mint=mint, trader=dev, is_buy=True, sol_amount=2.0,
                         token_amount=dev_tokens, v_sol=c.v_sol, v_tokens=c.v_tok, slot=slot, ts=ts, source="helius"))
    for i in range(peak_buys):
        t = ts + timedelta(seconds=5 * (i + 1))
        tok = c.buy(buy_sol)
        await rec.sink(Trade(signature=f"b{i}{mint[:20]}", mint=mint, trader=addr(f"buyer{i}"), is_buy=True,
                             sol_amount=buy_sol, token_amount=tok, v_sol=c.v_sol, v_tokens=c.v_tok,
                             slot=slot + 10 + i, ts=t, source="helius"))
    t = ts + timedelta(seconds=dump_after_s)
    sol = c.sell(dev_tokens)
    await rec.sink(Trade(signature=f"d{mint[:20]}", mint=mint, trader=dev, is_buy=False, sol_amount=sol,
                         token_amount=dev_tokens, v_sol=c.v_sol, v_tokens=c.v_tok, slot=slot + 500, ts=t,
                         source="helius"))
    for i in range(3):
        tok = 1e7
        sol = c.sell(tok)
        await rec.sink(Trade(signature=f"p{i}{mint[:20]}", mint=mint, trader=addr(f"buyer{i}"), is_buy=False,
                             sol_amount=sol, token_amount=tok, v_sol=c.v_sol, v_tokens=c.v_tok, slot=slot + 600 + i,
                             ts=t + timedelta(seconds=10 * (i + 1)), source="helius"))
    await rec.flush()
    return mint


class FakeBot:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.edits: list[dict] = []
        self.docs = 0
        self._id = 100

    async def send_message(self, chat, text, **kw):
        self._id += 1
        self.sent.append({"chat": chat, "text": text, **kw})
        return SimpleNamespace(chat=SimpleNamespace(id=chat), message_thread_id=kw.get("message_thread_id"),
                               message_id=self._id)

    async def edit_message_text(self, **kw):
        self.edits.append(kw)

    async def send_document(self, *a, **kw):
        self.docs += 1

    async def get_chat(self, chat):
        return SimpleNamespace(is_forum=False)


async def test_end_to_end(monkeypatch):
    db = await _db()
    if db is None:
        pytest.skip("PostgreSQL недоступен (TEST_DATABASE_URL)")
    try:
        env = Env(database_url=DSN, redirect_base="https://bot.example", rpc_url="http://127.0.0.1:9",
                  allowed_user_ids="111", bot_token="x", helius_api_key="")
        settings = Settings(YamlConfig("config/config.yaml"))
        rec = Receiver(env, settings, db)
        dev = addr("integration-dev")

        # --- история: 4 запуска дева за прошлые дни
        now = datetime.now(timezone.utc)
        cid = await cl.new_cluster(db, dev)
        engine = Engine(env, settings, db)
        engine.ext.disable_all("тест")   # внешние источники проверяются отдельными тестами

        async def no_transfers(*a, **k):
            return []

        async def not_exchange(*a, **k):
            return False

        async def no_sigs(*a, **k):
            return []

        monkeypatch.setattr(engine.linker, "transfers_around", no_transfers)
        monkeypatch.setattr(engine.linker, "is_exchange", not_exchange)
        monkeypatch.setattr(engine.rpc, "signatures_for", no_sigs)

        for i in range(4):
            mint = await feed_launch(rec, dev, now - timedelta(days=i + 1), slot=1000 * (i + 1))
            await db.execute("UPDATE tokens SET cluster_id=$2 WHERE mint=$1", mint, cid)
            st = await load_state(db, mint, settings.effective())
            assert st.big_sell_ts is not None and st.dump_mc > st.initial_mc
            await engine.finalize_launch(st, None)
        prof = await db.fetchval("SELECT profile FROM clusters WHERE id=$1", cid)
        assert prof["launches"] == 4 and prof["repeatability"] == pytest.approx(1.0)
        assert prof["medians"]["dump_mc"] > 60

        # --- живой запуск: алерт в первые секунды
        await engine.start()
        engine._feed_ts.extend([time.monotonic()] * 25)   # поток сделок жив
        t0 = datetime.now(timezone.utc)
        live = rand_addr()
        c = Curve()
        await rec.sink(TokenCreated(signature="live-create", mint=live, deployer=dev, creator=dev, name="Live",
                                    symbol="LIVE", uri="", slot=99_000, ts=t0, source="helius", received_at=t0))
        dev_tok = c.buy(2.0)
        await rec.sink(Trade(signature="live-create", mint=live, trader=dev, is_buy=True, sol_amount=2.0,
                             token_amount=dev_tok, v_sol=c.v_sol, v_tokens=c.v_tok, slot=99_000, ts=t0,
                             source="helius"))
        await rec.flush()
        await engine._consume()
        for _ in range(50):
            await asyncio.sleep(0.05)
            alert = await db.fetchrow("SELECT * FROM outbox WHERE kind='alert'")
            if alert:
                break
        assert alert, f"алерт по деву не создан: {engine.info[live].last_decision} {engine.info[live].profile}"
        p = alert["payload"]
        assert "ДЕВ" in p["text"].split("\n")[0] and "потенциал" in p["text"]
        assert p["topic"] == "strong" and p["level"] == "strong"
        assert any(b.get("copy") == live for row in p["keyboard"] for b in row)
        row = await db.fetchrow("SELECT * FROM alerts WHERE mint=$1 AND mode='dev'", live)
        assert row["net_profit"] >= 0.3 and row["latency_ms"] < 3000

        # повторная оценка того же уровня не создаёт дубль
        await engine.evaluate_dev(live, "again")
        assert await db.fetchval("SELECT count(*) FROM outbox WHERE kind='alert'") == 1

        # --- рост и продажа кластера → экстренный сигнал
        for i in range(6):
            tok = c.buy(1.5)
            await rec.sink(Trade(signature=f"lb{i}", mint=live, trader=addr(f"lbuyer{i}"), is_buy=True,
                                 sol_amount=1.5, token_amount=tok, v_sol=c.v_sol, v_tokens=c.v_tok,
                                 slot=99_010 + i, ts=t0 + timedelta(seconds=6 * (i + 1)), source="helius"))
        sol = c.sell(dev_tok)
        await rec.sink(Trade(signature="ldump", mint=live, trader=dev, is_buy=False, sol_amount=sol,
                             token_amount=dev_tok, v_sol=c.v_sol, v_tokens=c.v_tok, slot=99_100,
                             ts=t0 + timedelta(seconds=60), source="helius"))
        await rec.flush()
        await engine._consume()
        emerg = await db.fetchrow("SELECT * FROM outbox WHERE kind='follow' AND (payload->>'emergency')::bool")
        assert emerg and "кластер продаёт" in emerg["payload"]["text"]
        assert emerg["priority"] == 0
        assert emerg["payload"]["keyboard"][0][0]["style"] == "danger"

        # --- итог через 60 минут (ускоряем) и отметка пользователя
        e = engine.alerts[live]
        e.follow_until = 0
        engine.alerts.pop(live)
        await engine._finalize_alert(e)
        out = await db.fetchrow("SELECT * FROM alert_outcomes WHERE alert_id=$1", row["id"])
        assert out["exit_reason"] in ("target", "emergency") and out["auto_mark"]
        final = await db.fetchrow("SELECT * FROM outbox WHERE kind='final'")
        assert "Итог 60 минут" in final["payload"]["text"]
        assert final["payload"]["keyboard"][0][0]["callback"] == f"mk:{row['id']}:great"

        await db.execute("INSERT INTO user_marks(alert_id, user_id, mark) VALUES ($1, 111, 'good')", row["id"])
        await Feedback(engine).on_mark(row["id"], "good")
        assert await db.fetchval("SELECT priority FROM clusters WHERE id=$1", cid)
        assert await db.fetchval("SELECT count(*) FROM withdrawal_watch WHERE cluster_id=$1", cid) >= 1

        # «Скам» на другом кластере → чёрный список
        await Feedback(engine).on_mark(row["id"], "scam")
        assert await db.fetchval("SELECT blacklisted FROM clusters WHERE id=$1", cid)

        # --- разбор дня и бэктест
        text, csv_bytes, _kb = await build_daily(db, settings, datetime.now(timezone.utc).date())
        assert "Разбор дня" in text and "Итог сигналов" in text
        assert b"mint;symbol" in csv_bytes
        await db.execute("UPDATE clusters SET blacklisted=false, pattern_weight=1 WHERE id=$1", cid)
        bt = await run_backtest(db, settings, "10d")
        assert "Бэктест" in bt and "проверка" in bt

        # --- блок F: проверка на истории и расширенный CSV
        from scanner.reports.verify import run_verify
        vtext, vcsv = await run_verify(db, settings, datetime.now(timezone.utc).date(), 7)
        assert "Проверка на истории" in vtext and "Воронка" in vtext
        assert b"glue_reason" in vcsv and b"top_traders" in vcsv and b"entry_window" in vcsv

        # --- блок B: разбор монеты и карточка кошелька
        from scanner.bot import views
        hist_mint = await db.fetchval("SELECT mint FROM launches ORDER BY created_at LIMIT 1")
        ctext, ckb = await views.coin_view(db, settings, hist_mint)
        assert "Разбор монеты" in ctext and "Продажи" in ctext and ckb
        wtext, wkb = await views.wallet_view(db, dev)
        assert "Созданные токены" in wtext and wkb[1][1]["callback"] == f"wb:{dev}"

        # --- доставка в Telegram: одна монета — одно сообщение, edit вместо новых
        fake = FakeBot()
        sender = Sender(fake, env, settings, db)  # type: ignore[arg-type]
        await sender.setup_topics()
        await sender.drain()
        assert any("ДЕВ" in m["text"] for m in fake.sent)
        assert await db.fetchval("SELECT message_id FROM token_messages WHERE mint=$1", live)
        emerg_msgs = [m for m in fake.sent if "кластер продаёт" in m["text"]]
        assert emerg_msgs and emerg_msgs[0]["disable_notification"] is False
        # пауза глушит обычные алерты, но не экстренные
        await db.kv_set("mute_until", (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat())
        await db.enqueue("alert", {"mint": rand_addr(), "topic": "dev", "text": "x", "keyboard": None, "level": "normal"})
        n = len(fake.sent)
        await sender.drain()
        assert len(fake.sent) == n
        assert time.time() > 0
    finally:
        await engine.rpc.close() if "engine" in locals() else None
        await db.close()
