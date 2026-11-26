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


# разовые команды (run_tool): новая команда — и сюда, иначе «неизвестная команда» (tests/test_cli.py)
TOOLS = ("recluster", "history", "verify", "real-check", "real-export", "lookalike", "dune-scout", "cobuyers",
         "patterns", "dips", "whynot", "migration", "research", "copycheck", "jupcheck", "trendfeat", "hunt", "ppcost",
         "datacheck", "pdcheck")


async def main(cmd: str) -> None:
    env = load_env()
    settings = Settings(YamlConfig(env.config_path))
    db = await Database(env.database_url).connect(max_size=20 if cmd in ("analytics", "all") else 10)
    await db.migrate()
    if cmd == "migrate":
        await db.close()
        return
    if cmd in TOOLS:
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
        # поиск по образцу smart A вручную: python -m scanner lookalike [--hours 24] [--age N] [--amm] [--all] [--coins] [кошелёк …]
        from datetime import datetime, timezone
        from .analytics.lookalike import cfg_get, coin_details, diagnose, evaluate, format_cli
        from .config import Settings, YamlConfig, load_env
        env = load_env()
        cfg = dict(Settings(YamlConfig(env.config_path), await db.get_overrides()).effective())
        opt = {k: float(args[args.index(k) + 1]) for k in ("--hours", "--age") if k in args}
        skip = {args.index(k) + 1 for k in opt}
        if "--age" in opt:
            cfg["lk_max_age_min"] = opt["--age"]
        if "--amm" in args or "--all" in args:      # PumpSwap (в автопоиске пока выключен)
            cfg["lk_amm"] = True
        if "--all" in args:                         # все покупки на PumpSwap — для отслеживаемых кошельков (7BNax)
            cfg["lk_amm_prior"] = 0
        wallets = [a for i, a in enumerate(args) if not a.startswith("-") and i not in skip] or None
        now = datetime.now(timezone.utc)
        evals = await evaluate(db, cfg, now, hours=opt.get("--hours"), wallets=wallets)
        print(format_cli(evals, cfg))
        for w in wallets or []:
            print("\n" + await diagnose(db, cfg, w, now, opt.get("--hours") or float(cfg_get(cfg, "lk_hours"))))
            if "--coins" in args:                   # по монетам: цена в базе по источникам против бумаги A
                print("\n" + await coin_details(db, w, now, opt.get("--hours") or float(cfg_get(cfg, "lk_hours"))))
    elif cmd == "cobuyers":
        # соседи лучшего кошелька по блокчейну: python -m scanner cobuyers [кошелёк] [--coins 60] [--rps 4] [--queue]
        from types import SimpleNamespace
        from .analytics.cobuy import best_wallet, neighbours, queue_leaders, report
        from .analytics.scout import Scout
        from .config import Settings, YamlConfig, load_env
        from .ingest.rpc import SolanaRpc
        env = load_env()
        settings = Settings(YamlConfig(env.config_path), await db.get_overrides())
        opt = {k: float(args[args.index(k) + 1]) for k in ("--coins", "--rps") if k in args}
        coins = int(opt.get("--coins", 60))
        skip = {args.index(k) + 1 for k in opt}
        named = [a for i, a in enumerate(args) if not a.startswith("-") and i not in skip]
        wallet = named[0] if named else await best_wallet(db)
        if not wallet:
            raise SystemExit("Не нашёл кошелёк: укажите адрес")
        # 18.11: на полной скорости поиск делил с работающим ботом лимит Helius (~10 запросов/с на ключ) — сотни
        # «429: пауза». По умолчанию 4 запроса/с: боту остаётся запас; предупреждения о паузах — не в вывод
        logging.getLogger("scanner.ingest.rpc").setLevel(logging.ERROR)
        rpc = SolanaRpc(env.rpc_endpoint, rps=opt.get("--rps", min(4.0, float(env.rpc_rps))), concurrency=4,
                        fallback=env.rpc_fallback_url or None)
        print(f"Кошелёк {wallet}, монет до {coins}: окно −90…+40 с вокруг каждой его покупки, по блокчейну…",
              flush=True)
        try:
            res = await neighbours(db, rpc, wallet, coins, settings.exchanges(),
                                   progress=lambda r: print(f"  монета {r.coins}: найдено {r.found}, соседей "
                                                            f"{len(r.near)}, запросов {r.calls}", flush=True))
        finally:
            await rpc.close()
        print("\n" + report(res) + f"\n\nПауз из-за лимита узла (429): {rpc.rate_limited}")
        if "--queue" in args:
            eng = SimpleNamespace(db=db, cfg=settings.effective(), price=SimpleNamespace(value=150.0), ext=None,
                                  scouts=set(), _load_traders=None)
            queued = await queue_leaders(Scout(eng), res)  # type: ignore[arg-type]
            print("\nНа испытание (первыми в очереди):" if queued else "\nНовых кандидатов для испытания нет.")
            for q in queued:
                print("  " + q)
    elif cmd == "dune-scout":
        # весь PumpSwap через Dune вручную: python -m scanner dune-scout [--queue] (с --queue — найденных в разведку)
        from types import SimpleNamespace
        from .analytics.dunescout import DuneScout, describe, good
        from .analytics.scout import Scout
        from .config import Settings, YamlConfig, load_env
        env = load_env()
        cfg = dict(Settings(YamlConfig(env.config_path), await db.get_overrides()).effective())
        if not os.environ.get("DUNE_API_KEY"):
            raise SystemExit("Нужен DUNE_API_KEY в .env (бесплатно: dune.com → Settings → API)")
        eng = SimpleNamespace(db=db, cfg=cfg, price=SimpleNamespace(value=150.0), ext=None, scouts=set(),
                              _load_traders=None)
        sc = Scout(eng)  # type: ignore[arg-type]
        events = await sc.dune.run(force=True, queue="--queue" in args)
        last = await db.kv_get("dunescout:last") or {}
        print(f"Кошельков в выборке: {last.get('checked', 0)}, подходят: {last.get('found', 0)}, "
              f"запрос {last.get('took_s')} с")
        for r in last.get("top") or []:
            print(f"{r.get('trader')} | {describe(r)} | {'подходит' if good(r, cfg) else 'нет'}")
        for e in events:
            print(e)
    elif cmd == "research":
        # выгрузка для исследования: python -m scanner research [--days 14] [--window 60] [--no-send]
        from .analytics.research import run as research_run
        from .config import load_env as _load_env      # в run_tool load_env — локальное имя (импорты в ветках)
        days = float(args[args.index("--days") + 1]) if "--days" in args else 14.0
        window = float(args[args.index("--window") + 1]) if "--window" in args else 60.0
        print(await research_run(db, _load_env(), days=days, window_min=window, send="--no-send" not in args,
                                 progress=lambda m: print(m, file=sys.stderr, flush=True)))
    elif cmd == "patterns":
        # паттерны монет: python -m scanner patterns [--days 3] [--at 30,60] [--slots] [--no-send] — итог и CSV в Telegram
        from .analytics.patterns import run as patterns_run
        days = float(args[args.index("--days") + 1]) if "--days" in args else 3.0
        at = [float(x) for x in args[args.index("--at") + 1].split(",")] if "--at" in args else [30.0, 60.0]
        print(await patterns_run(db, days=days, at=at, send="--no-send" not in args, slots_only="--slots" in args,
                                 progress=lambda m: print(m, file=sys.stderr, flush=True)))
    elif cmd == "dips":
        # вход после падения: python -m scanner dips [--days 3] [--no-send] — итог и Excel со всеми правилами в Telegram
        from .analytics.dips import run as dips_run
        days = float(args[args.index("--days") + 1]) if "--days" in args else 3.0
        print(await dips_run(db, days=days, send="--no-send" not in args,
                             progress=lambda m: print(m, file=sys.stderr, flush=True)))
    elif cmd == "migration":
        # покупка вокруг миграции на PumpSwap: python -m scanner migration [--days 3] [--no-send] — итог и Excel
        from .analytics.migration import run as migration_run
        days = float(args[args.index("--days") + 1]) if "--days" in args else 3.0
        print(await migration_run(db, days=days, send="--no-send" not in args,
                                  progress=lambda m: print(m, file=sys.stderr, flush=True)))
    elif cmd == "whynot":
        # почему лучшие кошельки не копируются: python -m scanner whynot [--days 7] [--no-send] — итог в Telegram
        from .analytics.whynot import run as whynot_run
        settings = Settings(YamlConfig(load_env().config_path))
        settings.overrides = await db.get_overrides()
        days = float(args[args.index("--days") + 1]) if "--days" in args else 7.0
        print(await whynot_run(db, settings.effective(), days=days, send="--no-send" not in args))
    elif cmd == "copycheck":
        # 25.11: контроль оценки копий «как бот» против бумаги A: python -m scanner copycheck [кошелёк] [с] [по]
        # (даты ГГГГ-ММ-ДД, UTC; по умолчанию 7BNaxx… за 10–17.11)
        from datetime import datetime, timezone
        from .analytics.copymodel import control_text
        from .analytics.copytrade import REFERENCE_WALLETS
        named = [a for a in args if not a.startswith("-")]
        wallet = next((a for a in named if len(a) > 30), REFERENCE_WALLETS[0])
        dates = [datetime.strptime(a, "%Y-%m-%d").replace(tzinfo=timezone.utc) for a in named if len(a) == 10]
        lo = dates[0] if dates else datetime(2026, 11, 10, tzinfo=timezone.utc)
        hi = dates[1] if len(dates) > 1 else datetime(2026, 11, 18, tzinfo=timezone.utc)
        print(await control_text(db, wallet, lo.timestamp(), hi.timestamp()))
    elif cmd == "jupcheck":
        # 25.11: сделки других бирж с фильтрами входа (LP закрыт / сожжён, пул старше N ч): python -m scanner jupcheck
        # [--days N] [--lp 0.9]
        from .real.jupcheck import filter_text, run as jup_run, trials_text
        days = float(args[args.index("--days") + 1]) if "--days" in args else None
        lp = float(args[args.index("--lp") + 1]) if "--lp" in args else 0.9
        if "--trials" in args:
            print(await trials_text(db, days or 3.0))         # 27.11: выходы позиции и проб A/B/G/R по каждой сделке
        elif "--filter" in args:
            hours = float(args[args.index("--hours") + 1]) if "--hours" in args else 24.0
            print(await filter_text(db, hours))               # 27.11: сколько фильтры входа пропустили и отсекли
        else:
            print(await jup_run(db, lp, days, all_time="--all" in args))
    elif cmd == "trendfeat":
        # 25.11: признаки входа трендов: python -m scanner trendfeat [--compare] [--days N] [--kind trend|trendj]
        # [--mode shadow|live] (CSV — в stdout: … > /opt/tg-bot/trend_feat.csv)
        from .real.featexport import compare, load, to_csv
        days = float(args[args.index("--days") + 1]) if "--days" in args else None
        kind = args[args.index("--kind") + 1] if "--kind" in args else None
        mode = args[args.index("--mode") + 1] if "--mode" in args else None
        rows = await load(db, days, kind, mode)
        print(compare(rows) if "--compare" in args else to_csv(rows).rstrip("\n"))
    elif cmd == "hunt":
        # 25.11: охотник — python -m scanner hunt --history [--days 7] [--step 6] [--max 500] (исторический прогон)
        # или python -m scanner hunt (один проход сейчас: кто горячий за 72 ч, без алертов)
        import time as _t
        from .analytics import hunter as H
        from datetime import datetime as _dt, timezone as _tz
        days = float(args[args.index("--days") + 1]) if "--days" in args else 7.0
        step = float(args[args.index("--step") + 1]) if "--step" in args else 6.0
        mx = int(args[args.index("--max") + 1]) if "--max" in args else 500
        arg = lambda k: args[args.index(k) + 1] if k in args else None   # noqa: E731
        day = lambda v: _dt.strptime(v, "%Y-%m-%d").replace(tzinfo=_tz.utc).timestamp() if v else None   # noqa: E731
        from .config import Settings as HSettings, YamlConfig as HYaml, load_env as h_env
        hs = HSettings(HYaml(h_env().config_path))
        hs.overrides = await db.get_overrides()
        base = H.bar_of(hs.effective())                  # 27.11: мягкая планка 15 монет (hunter_min_coins / _min_t)
        bar = H.Bar(min_coins=int(arg("--min-coins") or base.min_coins),
                    min_t=float(arg("--min-t") if arg("--min-t") is not None else base.min_t))
        prm = await H.params_of(db, hs.effective())
        if arg("--delay"):
            prm.delay = float(arg("--delay"))            # вход через N с (по умолчанию — hunter_entry_delay_s, 1.36)
        if arg("--min-pool") is not None:
            prm.min_pool = float(arg("--min-pool"))
        rules = tuple(arg("--rules").upper().split(",")) if arg("--rules") else H.RULES
        if "--history" in args:
            # --from / --to — даты ГГГГ-ММ-ДД (UTC); --wallet адрес — разбор одного кошелька по точкам проверки;
            # 27.11: отсев ферм через RPC (--no-farm-rpc — только по базе)
            from .ingest.rpc import SolanaRpc
            henv = h_env()
            hrpc = None if "--no-farm-rpc" in args else SolanaRpc(henv.rpc_endpoint, rps=henv.rpc_rps,
                                                                    fallback=henv.rpc_fallback_url or None)
            try:
                print(await H.history(db, days, step, mx, until=day(arg("--to")), bar=bar, trace=arg("--wallet"),
                                      start=day(arg("--from")), prm=prm, rules=rules, rpc=hrpc))
            finally:
                if hrpc is not None:
                    await hrpc.close()
        elif "--list" in args:
            print(await H.hunt_text(db, float(arg("--hours") or 24)))
        elif "--entry-gap" in args:
            # 27.11: копии с наибольшим разрывом между входом «блок +2» и входом через 1.36 с (--wallet адрес)
            if not arg("--wallet"):
                print("Укажите кошелёк: hunt --entry-gap --wallet <адрес> [--hours 72] [--top 3]")
            else:
                print(H.fmt.plain(await H.entry_gap_text(db, prm, arg("--wallet"), float(arg("--hours") or 72),
                                                         int(arg("--top") or 3))))
        elif "--why" in args:
            # 27.11: что стало с покупками кошельков охотника после находки (модель, журнал копий, бот)
            print(H.fmt.plain(await H.why_text(db, prm, arg("--wallet"))))
        else:
            now = _t.time()
            cands = await H.candidates(db, now - H.WINDOW_H * 3600, now, bar.min_coins, mx)
            print(f"Кошельков с {bar.min_coins}+ монетами за {H.WINDOW_H} ч: {len(cands)}")
            for c in cands:
                s = await H.evaluate(db, c["trader"], now - H.WINDOW_H * 3600, now, prm, cand=c)
                why = s.hot(bar)
                if why is None or "--all" in args:
                    print(f"{'🔥' if why is None else '  '} {c['trader']} монет {s.coins_all} копий {s.n} правило "
                          f"{s.rule} ср. {s.avg:+.1%} в плюсе {s.win:.0%} t={s.t:.1f} без цены {s.noprice:.0%} "
                          f"${s.usd_day:,.0f}/день всплеск {s.burst:.0%}" + (f" — {why}" if why else ""))
    elif cmd == "ppcost":
        # 27.11: сколько PumpPortal стоил в SOL по дням: python -m scanner ppcost [--days 30]
        from .analytics.datacheck import pp_cost_text
        print(await pp_cost_text(db, int(args[args.index("--days") + 1]) if "--days" in args else 30))
    elif cmd == "datacheck":
        # 26.11: проверки данных и модели: python -m scanner datacheck [--hours 24] [--days 3] [--delay 1.36]
        from .analytics.datacheck import run as dc_run
        hours = float(args[args.index("--hours") + 1]) if "--hours" in args else 24.0
        dd = float(args[args.index("--days") + 1]) if "--days" in args else 3.0
        delay = float(args[args.index("--delay") + 1]) if "--delay" in args else 1.36
        if "--gap" in args:
            # 27.11: модель против тени по частям: вход, выход, издержки, причина выхода, правило, биржа
            from .analytics.datacheck import model_gap
            print("\n".join(await model_gap(db, dd, delay)))
        else:
            print(await dc_run(db, hours, dd, delay))
    elif cmd == "pdcheck":
        # 26.11 (B5): сверка образца pumpdata.fun с нашей базой: python -m scanner pdcheck файл.parquet
        from .analytics.pdcheck import run as pd_run
        path = next((a for a in args if a.endswith(".parquet")), None)
        print(await pd_run(db, path) if path else "Укажите файл образца: python -m scanner pdcheck /app/data/x.parquet")
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
