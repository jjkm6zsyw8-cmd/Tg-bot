"""Точка входа: python -m scanner {ingest|analytics|bot|web|migrate|all}."""
from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import sys

from .config import Settings, YamlConfig, load_env
from .db.pool import Database


def setup_logging(level: str, name: str) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(level.upper())
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(fmt)
    root.addHandler(h)
    log_dir = os.environ.get("LOG_DIR")
    if log_dir:
        try:
            os.makedirs(log_dir, exist_ok=True)
            fh = logging.handlers.RotatingFileHandler(os.path.join(log_dir, f"{name}.log"), maxBytes=20_000_000,
                                                      backupCount=5, encoding="utf-8")
            fh.setFormatter(fmt)
            root.addHandler(fh)
        except OSError as exc:  # нет прав на папку логов — пишем только в stdout, но не падаем
            root.warning("файловый лог отключён: %s", exc)
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)


async def main(cmd: str) -> None:
    env = load_env()
    settings = Settings(YamlConfig(env.config_path))
    db = await Database(env.database_url).connect(max_size=20 if cmd in ("analytics", "all") else 10)
    await db.migrate()
    if cmd == "migrate":
        await db.close()
        return
    tasks = []
    if cmd in ("ingest", "all"):
        from .ingest.receiver import Receiver
        tasks.append(Receiver(env, settings, db).run())
    if cmd in ("analytics", "all"):
        from .analytics.engine import Engine
        tasks.append(Engine(env, settings, db).run())
    if cmd in ("bot", "all"):
        from .bot.app import run_bot
        tasks.append(run_bot(env, settings, db))
    if cmd in ("web", "all"):
        from .web.redirect import run_web
        tasks.append(run_web(env.web_host, env.web_port))
    if not tasks:
        raise SystemExit(f"неизвестная команда {cmd}")
    try:
        await asyncio.gather(*tasks)
    except BaseException as exc:
        if not isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
            logging.getLogger("scanner").critical("СЕРВИС %s ОСТАНОВЛЕН: %s: %s", cmd, type(exc).__name__, exc)
        raise
    finally:
        # terminate, а не close: close ждёт соединения LISTEN и может зависнуть навсегда
        if db.pool is not None:
            db.pool.terminate()


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    setup_logging(os.environ.get("LOG_LEVEL", "INFO"), command)
    asyncio.run(main(command))
