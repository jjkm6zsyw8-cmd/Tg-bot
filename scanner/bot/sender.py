"""Алерт-сервис: очередь outbox → Telegram (разд. 8, 9, 11 ТЗ).

Приоритеты, лимиты Telegram (≈20 сообщений в минуту на группу), топики супергруппы,
пауза и тихие часы, редактирование MC в том же сообщении, экстренные — со звуком.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.types import BufferedInputFile, LinkPreviewOptions, ReplyParameters

from ..config import Env, Settings
from ..db.pool import Database
from .keyboards import build

log = logging.getLogger(__name__)
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)
MUTABLE = {"alert", "summary", "full"}  # что глушится паузой и тихими часами


class Limiter:
    def __init__(self, per_minute: int = 20):
        self.per_minute = per_minute
        self.sent: deque[float] = deque()

    def budget(self) -> int:
        now = time.monotonic()
        while self.sent and now - self.sent[0] > 60:
            self.sent.popleft()
        return self.per_minute - len(self.sent)

    def take(self) -> None:
        self.sent.append(time.monotonic())


def in_quiet_hours(spec: str, tz: str) -> bool:
    if not spec or "-" not in spec:
        return False
    try:
        a, b = spec.split("-")
        ah, am = map(int, a.split(":"))
        bh, bm = map(int, b.split(":"))
    except ValueError:
        return False
    now = datetime.now(ZoneInfo(tz))
    cur, start, end = now.hour * 60 + now.minute, ah * 60 + am, bh * 60 + bm
    return start <= cur < end if start <= end else (cur >= start or cur < end)


class Sender:
    def __init__(self, bot: Bot, env: Env, settings: Settings, db: Database):
        self.bot = bot
        self.env = env
        self.settings = settings
        self.db = db
        self.wake = asyncio.Event()
        self.topics: dict[str, int] = {}
        self.limiter = Limiter(20)
        self.styles = True
        self.sys_buf: list[str] = []      # системные сообщения копятся и уходят не чаще раза в 10 минут
        self.sys_last = 0.0
        self.sys_interval = 600.0

    @property
    def chat_id(self) -> int | None:
        if self.env.alert_chat_id:
            return self.env.alert_chat_id
        ids = sorted(self.env.allowed_ids)
        return ids[0] if ids else None

    async def setup_topics(self) -> str:
        """Создаёт топики супергруппы, если их id ещё не сохранены."""
        self.topics = await self.db.kv_get("topics", {}) or {}
        chat = self.chat_id
        if not chat or not self.env.alert_chat_id:
            return "Чат алертов не задан (ALERT_CHAT_ID) — алерты идут в личные сообщения."
        try:
            info = await self.bot.get_chat(chat)
        except Exception as exc:  # noqa: BLE001
            return f"Не удалось открыть чат алертов: {exc}"
        if not getattr(info, "is_forum", False):
            self.topics = {}
            return "Чат алертов без топиков — всё идёт в общий поток. Включите темы в настройках группы."
        created = []
        for key, name in self.settings.topics().items():
            if key in self.topics:
                continue
            try:
                t = await self.bot.create_forum_topic(chat, name)
                self.topics[key] = t.message_thread_id
                created.append(name)
            except Exception as exc:  # noqa: BLE001
                return f"Не удалось создать топик «{name}»: {exc}. Нужны права администратора «Управление темами»."
        await self.db.kv_set("topics", self.topics)
        return ("Созданы топики: " + ", ".join(created)) if created else "Топики на месте."

    def thread(self, topic: str | None) -> int | None:
        return self.topics.get(topic or "") if self.topics else None

    async def listen(self) -> None:
        conn = await self.db.p.acquire()
        try:
            await conn.add_listener("outbox", lambda *_: self.wake.set())
            while True:
                await asyncio.sleep(3600)
        finally:
            await self.db.p.release(conn)

    async def run(self) -> None:
        asyncio.create_task(self.listen())
        while True:
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            self.wake.clear()
            try:
                await self.drain()
            except Exception:  # noqa: BLE001
                log.exception("sender")
                await asyncio.sleep(2)

    async def muted(self) -> bool:
        cfg = self.settings.effective()
        until = await self.db.kv_get("mute_until")
        if until and datetime.fromisoformat(until) > datetime.now(timezone.utc):
            return True
        return in_quiet_hours(cfg.get("quiet_hours", ""), cfg.get("timezone", "UTC"))

    async def drain(self) -> None:
        self.settings.overrides = await self.db.get_overrides()
        await self.flush_system()
        # Telegram пускает в группу ~20 сообщений в минуту. Строго по приоритету правки алертов и сопровождение
        # съедали весь лимит, а отчёты, «Копитрейд», сводки ждали часами и потом приходили пачкой.
        # Старение: каждая минута ожидания повышает приоритет на 1 (до 6) — через ~5 мин сообщение наверху очереди
        # строка полной ленты старше 2 минут бесполезна: такие ждали лимита, старели, поднимались наверх и забивали
        # окно из 50 сообщений — за ними часами стояли алерты и отчёты (10.11: в очереди 483, самое старое 2ч38м)
        await self.db.execute(
            """UPDATE outbox SET sent_at = now(), error = 'stale' WHERE sent_at IS NULL AND kind = 'full'
                 AND created_at < now() - interval '2 minutes'""")
        rows = await self.db.fetch(
            """SELECT * FROM outbox WHERE sent_at IS NULL AND attempts < 5
               ORDER BY priority - CASE WHEN kind = 'full' THEN 0
                                        ELSE LEAST(6, EXTRACT(EPOCH FROM (now() - created_at)) / 60.0) END, id
               LIMIT 50""")
        if not rows:
            return
        muted = await self.muted()
        latest_edit: dict[str, int] = {}
        for r in rows:
            if r["kind"] == "edit":
                latest_edit[r["payload"]["mint"]] = max(latest_edit.get(r["payload"]["mint"], 0), r["id"])
        for r in rows:
            kind, p = r["kind"], r["payload"]
            if kind == "system" or (kind == "text" and p.get("topic") == "system"):
                self.sys_buf.append(str(p.get("text", "")))
                await self._done(r["id"], "buffered")
                continue
            if kind == "edit" and latest_edit.get(p["mint"]) != r["id"]:
                await self._done(r["id"], "superseded")
                continue
            if muted and kind in MUTABLE:
                await self._done(r["id"], "muted")
                continue
            urgent = r["priority"] <= 1 or p.get("emergency")
            need = 2 if kind == "follow" and p.get("emergency") else 1
            age = time.time() - r["created_at"].timestamp()
            if kind == "edit" and age > 20:
                await self._done(r["id"], "stale")       # правка цены устарела — следующая придёт сама
                continue
            # правки и полная лента — только при заметном запасе лимита: обычные сообщения важнее
            if not urgent and self.limiter.budget() < (10 if kind in ("edit", "full") else need):
                continue
            try:
                await self.deliver(kind, p)
                await self._done(r["id"])
            except TelegramRetryAfter as exc:
                log.warning("flood control: %s с", exc.retry_after)
                try:
                    await self.db.kv_set("sender:flood", {"at": datetime.now(timezone.utc).isoformat(),
                                                          "seconds": exc.retry_after})
                except Exception:  # noqa: BLE001
                    pass
                await asyncio.sleep(exc.retry_after + 0.5)
                return
            except TelegramBadRequest as exc:
                msg = str(exc)
                if "message is not modified" in msg:
                    await self._done(r["id"])
                elif "style" in msg.lower() and self.styles:
                    self.styles = False  # клиент/сервер без стилей кнопок — шлём без них
                    await self.db.execute("UPDATE outbox SET attempts=attempts+1 WHERE id=$1", r["id"])
                else:
                    log.warning("telegram: %s", msg)
                    await self.db.execute("UPDATE outbox SET attempts=attempts+1, error=$2 WHERE id=$1",
                                          r["id"], msg[:500])
            except Exception as exc:  # noqa: BLE001
                log.exception("deliver %s", kind)
                await self.db.execute("UPDATE outbox SET attempts=attempts+1, error=$2 WHERE id=$1",
                                      r["id"], str(exc)[:500])

    async def flush_system(self) -> None:
        """Только в топик «Система» и не чаще раза в sys_interval: всё накопленное — одним сообщением."""
        if not self.sys_buf or time.monotonic() - self.sys_last < self.sys_interval:
            return
        lines = list(dict.fromkeys(self.sys_buf))      # одинаковые подряд не повторяем
        self.sys_buf = []
        text = "\n\n".join(lines)
        if len(text) > 3800:
            text = text[:3800] + f"\n… и ещё {len(lines)} сообщ."
        try:
            await self.send(text, "system", None, silent=True)
            self.sys_last = time.monotonic()
        except Exception:  # noqa: BLE001
            log.exception("system message")
            self.sys_buf = lines + self.sys_buf

    async def send_test_alerts(self) -> list[str]:
        """/testalert: тестовое сообщение в каждый топик."""
        sent = []
        for key, name in self.settings.topics().items():
            try:
                await self.send(f"🧪 <b>Тестовый алерт</b> · топик «{name}»\nЕсли вы это видите — доставка работает.",
                                key, [[{"text": "Pump.fun", "url": "https://pump.fun"},
                                       {"text": "Скопировать", "copy": "test"}]], silent=True)
                sent.append(name)
            except Exception as exc:  # noqa: BLE001
                sent.append(f"{name}: ошибка {exc}")
        return sent

    async def _done(self, oid: int, note: str | None = None) -> None:
        await self.db.execute("UPDATE outbox SET sent_at=now(), error=COALESCE($2, error) WHERE id=$1", oid, note)

    async def send(self, text: str, topic: str | None, keyboard: Any = None, silent: bool = True,
                   reply_to: int | None = None, thread: int | None = None) -> Any:
        chat = self.chat_id
        if chat is None:
            raise RuntimeError("нет получателя: задайте ALERT_CHAT_ID или ALLOWED_USER_IDS")
        self.limiter.take()
        return await self.bot.send_message(
            chat, text, parse_mode="HTML", reply_markup=build(keyboard, self.styles),
            disable_notification=silent, message_thread_id=thread if thread is not None else self.thread(topic),
            link_preview_options=NO_PREVIEW,
            reply_parameters=ReplyParameters(message_id=reply_to, allow_sending_without_reply=True) if reply_to else None,
        )

    async def deliver(self, kind: str, p: dict[str, Any]) -> None:
        if kind == "alert":
            msg = await self.send(p["text"], p["topic"], p.get("keyboard"), silent=bool(p.get("silent")))
            await self.db.execute(
                """INSERT INTO token_messages(mint, chat_id, thread_id, message_id, level, text, keyboard, updated_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,now()) ON CONFLICT (mint) DO UPDATE SET chat_id=EXCLUDED.chat_id,
                   thread_id=EXCLUDED.thread_id, message_id=EXCLUDED.message_id, level=EXCLUDED.level,
                   text=EXCLUDED.text, keyboard=EXCLUDED.keyboard, updated_at=now()""",
                p["mint"], msg.chat.id, msg.message_thread_id, msg.message_id, p.get("level"), p["text"],
                p.get("keyboard"))
        elif kind == "edit":
            tm = await self.db.fetchrow("SELECT * FROM token_messages WHERE mint=$1", p["mint"])
            if not tm:
                return
            kb = p.get("keyboard") or tm["keyboard"]
            self.limiter.take()
            await self.bot.edit_message_text(text=p["text"], chat_id=tm["chat_id"], message_id=tm["message_id"],
                                             parse_mode="HTML", reply_markup=build(kb, self.styles),
                                             link_preview_options=NO_PREVIEW)
            await self.db.execute("UPDATE token_messages SET text=$2, updated_at=now() WHERE mint=$1",
                                  p["mint"], p["text"])
        elif kind == "follow":
            emergency = bool(p.get("emergency"))
            await self.send(p["text"], "follow", p.get("keyboard"), silent=not emergency)
            if emergency:
                tm = await self.db.fetchrow("SELECT * FROM token_messages WHERE mint=$1", p["mint"])
                if tm and tm["thread_id"] != self.thread("follow"):
                    await self.send(p["text"], None, p.get("keyboard"), silent=False, reply_to=tm["message_id"],
                                    thread=tm["thread_id"])
        elif kind == "final":
            await self.send(p["text"], "follow", p.get("keyboard"), silent=True)
        elif kind in ("summary", "full", "text", "system"):
            topic = {"full": "full", "system": "system"}.get(kind, p.get("topic"))
            await self.send(p["text"], topic, p.get("keyboard"), silent=True)
        elif kind == "daily":
            await self.send(p["text"][:4000], "daily", p.get("keyboard"), silent=False)
            if p.get("csv"):
                chat = self.chat_id
                self.limiter.take()
                await self.bot.send_document(
                    chat, BufferedInputFile(p["csv"].encode("utf-8"), filename=p.get("filename", "day.csv")),
                    message_thread_id=self.thread("daily"), disable_notification=True)
        elif kind == "file":
            self.limiter.take()
            await self.bot.send_document(
                self.chat_id, BufferedInputFile(p["csv"].encode("utf-8-sig"),   # BOM — Excel видит кириллицу
                                                filename=p.get("filename", "data.csv")),
                caption=p.get("caption"), message_thread_id=self.thread(p.get("topic")), disable_notification=True)
        elif kind == "zip":                  # архив выгрузки (reports/export.py), в очереди — base64
            import base64
            self.limiter.take()
            await self.bot.send_document(
                self.chat_id, BufferedInputFile(base64.b64decode(p["b64"]), filename=p.get("filename", "export.zip")),
                caption=p.get("caption"), message_thread_id=self.thread(p.get("topic")), disable_notification=True)
        else:
            log.warning("неизвестный тип сообщения %s", kind)
