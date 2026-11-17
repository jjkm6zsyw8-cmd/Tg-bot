"""Точка входа: python -m scanner {ingest|analytics|bot|web|migrate|all}.

Разовые команды: recluster, history, verify; реальный бот — real-keygen (кошелёк в .env), real-masterkey (ключ
шифра для кошельков, создаваемых в Telegram), real-check (проверка сборки сделок на живой сети без денег),
real-export (ключи кошельков — только на экран сервера, для переноса в Phantom); lookalike — поиск кошельков по
образцу smart A (копия A на истории сделок), например: lookalike --hours 96 <кошелёк>."""
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
    if cmd in ("recluster", "history", "verify", "real-check", "real-export", "lookalike"):
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
    elif cmd == "real-check":
        import re
        from types import SimpleNamespace
        from .config import Settings, YamlConfig, load_env
        from .real.executor import RealTrader
        from .real.selfcheck import run
        env = load_env()
        settings = Settings(YamlConfig(env.config_path), await db.get_overrides())
        price = float((await db.kv_get("sol_usd", {}) or {}).get("price", 150))
        eng = SimpleNamespace(db=db, env=env, cfg=settings.effective(), price=SimpleNamespace(value=price), states={})
        text = await run(RealTrader(eng), [a for a in args if not a.startswith("-")] or None)   # type: ignore[arg-type]
        print(re.sub(r"<[^>]+>", "", text))
    elif cmd == "lookalike":
        # поиск по образцу smart A вручную: python -m scanner lookalike [--hours 24] [--age N] [--all] [кошелёк …]
        from datetime import datetime, timezone
        from .analytics.lookalike import cfg_get, diagnose, evaluate, format_cli
        from .config import Settings, YamlConfig, load_env
        env = load_env()
        cfg = dict(Settings(YamlConfig(env.config_path), await db.get_overrides()).effective())
        opt = {k: float(args[args.index(k) + 1]) for k in ("--hours", "--age") if k in args}
        skip = {args.index(k) + 1 for k in opt}
        if "--age" in opt:
            cfg["lk_max_age_min"] = opt["--age"]
        if "--all" in args:                         # все покупки на PumpSwap — для отслеживаемых кошельков (7BNax)
            cfg["lk_amm_prior"] = 0
        wallets = [a for i, a in enumerate(args) if not a.startswith("-") and i not in skip] or None
        now = datetime.now(timezone.utc)
        evals = await evaluate(db, cfg, now, hours=opt.get("--hours"), wallets=wallets)
        print(format_cli(evals, cfg))
        for w in wallets or []:
            print("\n" + await diagnose(db, cfg, w, now, opt.get("--hours") or float(cfg_get(cfg, "lk_hours"))))
    elif cmd == "real-export":
        # аварийный выход: ключи кошельков, созданных в Telegram, — только сюда, на экран сервера
        from .real.vault import load_vault
        vault, note = load_vault()
        rows = await db.fetch("SELECT id, name, pubkey, secret_enc, archived FROM real_wallets ORDER BY id")
        print("# ВНИМАНИЕ: ниже приватные ключи. Не пересылайте их никому и не отправляйте в чат.", file=sys.stderr)
        for r in rows:
            if r["secret_enc"] is None:
                print(f"{r['id']}\t{r['name']}\t{r['pubkey']}\tключ в .env (REAL_PRIVATE_KEY)")
                continue
            try:
                key = str(vault.decrypt(bytes(r["secret_enc"]), r["pubkey"]))
            except Exception as exc:  # noqa: BLE001
                key = f"не расшифрован: {exc} ({note})"
            print(f"{r['id']}\t{r['name']}{' (удалён)' if r['archived'] else ''}\t{r['pubkey']}\t{key}")
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
    if command == "real-keygen":        # до настройки логов и базы: в stdout — только строки для .env
        from .real.keys import keygen
        raise SystemExit(keygen())
    if command == "real-masterkey":
        from .real.vault import masterkey_gen
        raise SystemExit(masterkey_gen())
    setup_logging(os.environ.get("LOG_LEVEL", "INFO"), command)
    asyncio.run(main(command))
