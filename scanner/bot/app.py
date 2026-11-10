"""Сервис бота: команды + доставка очереди outbox."""
from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher, Router
from aiogram.types import BotCommand

from ..config import Env, Settings
from ..db.pool import Database
from . import handlers
from .sender import Sender

log = logging.getLogger(__name__)

COMMANDS = [
    ("start", "Главное меню"), ("mode", "Режимы"), ("settings", "Настройки"), ("token", "Разбор монеты"),
    ("dev", "Профиль дева"), ("trader", "Профиль трейдера"), ("wallet", "Кошелёк"), ("coin", "Разбор монеты"), ("watch", "Сопровождение монеты"),
    ("watchlist", "Отслеживаемое"), ("addwallet", "Добавить кошелёк"), ("removewallet", "Убрать кошелёк"),
    ("import", "Импорт кошельков (файл из Axiom или список)"),
    ("copylog", "Копитрейд: что скопировано и что пропущено"),
    ("diag", "Диагностика: застрявшие задачи и ошибки отправки"),
    ("devonce", "Мой дев: заходить только один раз"),
    ("blacklist", "Чёрный список"), ("today", "Разбор дня"), ("backtest", "Бэктест"), ("stats", "Итог серии"),
    ("mute", "Пауза алертов"), ("health", "Состояние"), ("testalert", "Тест доставки во все топики"),
    ("paper", "Бумажная торговля: состояние, history, day, csv, reset"),
    ("tune", "Автоподбор фильтров «Трендов» по истории"),
    ("calib", "Калибровка стопов и тейков по истории сигналов"),
    ("filter", "Автофильтр: чему бот научился на сделках трейдеров"),
    ("rating", "Рейтинг кошельков: кого копировать больше, кого на паузу"),
    ("scout", "Разведка: поиск и проверка новых кошельков"),
    ("wallets", "Файл по всем кошелькам с итогом копий"),
    ("export", "Архив для разбора: сделки, кошельки, копитрейд"),
]


async def run_bot(env: Env, settings: Settings, db: Database) -> None:
    if not env.bot_token:
        raise SystemExit("BOT_TOKEN не задан")
    if not env.allowed_ids:
        raise SystemExit("ALLOWED_USER_IDS не задан — бот не будет никому отвечать")
    bot = Bot(env.bot_token)
    try:
        me = await bot.get_me()
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(
            f"Telegram не принимает BOT_TOKEN ({type(exc).__name__}: {exc}). Проверьте токен в .env: "
            "формат 123456789:AAxxxxxxxx… без пробелов; после /revoke нужен новый токен.") from exc
    log.info("бот @%s подключён к Telegram", me.username)
    dp = Dispatcher()
    router = Router()
    sender = Sender(bot, env, settings, db)
    handlers.setup(router, env, settings, db, sender)
    dp.include_router(router)
    settings.overrides = await db.get_overrides()
    log.info(await sender.setup_topics())
    try:
        await bot.set_my_commands([BotCommand(command=c, description=d) for c, d in COMMANDS])
    except Exception as exc:  # noqa: BLE001
        log.warning("set_my_commands: %s", exc)
    sender_task = asyncio.create_task(sender.run())
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        sender_task.cancel()  # иначе при остановке отправщик пишет ошибки «pool is closing»
