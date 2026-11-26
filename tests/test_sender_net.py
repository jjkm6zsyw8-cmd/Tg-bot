"""Связь с Telegram «залипла» (20.11 23:40–01:00): сообщения не теряются, соединение пересоздаётся само, а если не
помогло — сервис бота перезапускается."""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from aiogram.exceptions import TelegramNetworkError

from scanner.bot import sender as snd
from scanner.config import Env, Settings, YamlConfig
from scanner.db.pool import Database

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


class Session:
    closed = 0

    async def close(self):
        Session.closed += 1


class Bot:
    def __init__(self):
        self.down = True
        self.sent = []
        self.session = Session()

    async def send_message(self, chat, text, **kw):
        if self.down:
            raise TelegramNetworkError(method=None, message="HTTP Client says - Request timeout error")
        self.sent.append(text)
        return SimpleNamespace(chat=SimpleNamespace(id=chat), message_thread_id=None, message_id=len(self.sent))

    async def get_me(self):
        if self.down:
            raise TelegramNetworkError(method=None, message="timeout")
        return SimpleNamespace(id=1)

    async def get_chat(self, chat):
        return SimpleNamespace(is_forum=False)


async def test_network_outage_keeps_queue_and_heals(monkeypatch):
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        monkeypatch.setattr(snd, "NET_BACKOFF_S", 0)
        clock = [1000.0]
        monkeypatch.setattr(snd, "_mono", lambda: clock[0])
        env = Env(database_url=DSN, redirect_base="https://bot.example", rpc_url="http://127.0.0.1:9",
                  allowed_user_ids="111", bot_token="x", helius_api_key="")
        bot = Bot()
        s = snd.Sender(bot, env, Settings(YamlConfig("config/config.yaml")), db)  # type: ignore[arg-type]
        exits = []
        s.exit = exits.append
        await db.enqueue("text", {"topic": "paper", "text": "отчёт"}, priority=5)
        for _ in range(8):                                     # 8 обрывов подряд — раньше хватало 5, чтобы выбросить
            await s.drain()
        row = await db.fetchrow("SELECT attempts, sent_at FROM outbox")
        assert row["attempts"] == 0 and row["sent_at"] is None  # сообщение ждёт связь
        clock[0] += snd.NET_RESET_S + 1
        await s.drain()
        assert Session.closed == 1 and not exits               # 3 мин без связи — соединение пересоздано
        clock[0] += snd.NET_EXIT_S
        await s.ping()
        assert exits == [1]                                    # 10 мин — перезапуск сервиса
        bot.down = False
        await s.drain()
        assert bot.sent == ["отчёт"] and s.net_down_since is None   # связь вернулась — сообщение ушло
        clock[0] += snd.NET_PING_S + 1
        await s.ping()
        assert s.net_down_since is None
    finally:
        await db.close()
