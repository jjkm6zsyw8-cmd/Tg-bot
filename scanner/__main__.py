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
    if cmd in ("recluster", "history", "verify"):
        await run_tool(cmd, db)
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


async def run_tool(cmd: str, db: Database) -> None:
    """Разовые операции: отчёт/пересборка кластеров, загрузка истории."""
    args = sys.argv[2:]
    if cmd == "recluster":
        from .analytics.recluster import rebuild, report
        print(await report(db))
        if "--report" not in args:
            print("\nПересборка…")
            res = await rebuild(db)
            print(res)
            print("\nПосле пересборки:\n" + await report(db))
    elif cmd == "verify":
        import re
        from datetime import date
        from .config import Settings, YamlConfig
        from .reports.verify import run_verify
        day = date.fromisoformat(args[args.index("--day") + 1]) if "--day" in args else date.today()
        days = int(args[args.index("--days") + 1]) if "--days" in args else 7
        settings = Settings(YamlConfig("config/config.yaml"), await db.get_overrides())
        text, csv_bytes = await run_verify(db, settings, day, days)
        print(re.sub(r"<[^>]+>", "", text))
        await db.enqueue("daily", {"text": text, "csv": csv_bytes.decode("utf-8"),
                                   "filename": f"verify_{day.isoformat()}_{days}d.csv"}, priority=4)
        print("\nОтчёт и CSV отправлены в топик «Разбор дня».")
    elif cmd == "history":
        from .history.dune import load
        key = os.environ.get("DUNE_API_KEY", "")
        if not key:
            raise SystemExit("Нужен DUNE_API_KEY (dune.com → Settings → API)")
        days = int(args[args.index("--days") + 1]) if "--days" in args else 30
        mn = int(args[args.index("--min-launches") + 1]) if "--min-launches" in args else 2
        print(await load(db, key, days, mn))
        from .analytics.recluster import rebuild
        print(await rebuild(db, days=days + 1))


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    setup_logging(os.environ.get("LOG_LEVEL", "INFO"), command)
    asyncio.run(main(command))
