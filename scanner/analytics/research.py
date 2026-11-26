"""Выгрузка для исследования (23.11, владелец: «в базе 40к монет — найди по данным правильный подход к монетам и
кошелькам, а не 25%/−8% и скальперские фильтры»).

Сырые данные своей базы — без расчётов бота, чтобы анализ шёл снаружи и с любой стороны:
  coins.csv.gz    — монета: создание, дев, кластер, бандлы, ранние покупатели, MC через 30/60/120 с, пик, слив, миграция,
                    признаки запуска (launches.features), алерты по ней;
  trades_NNN.csv.gz — все сделки первых --window минут жизни монеты: секунда от создания, блок от создания, кошелёк
                    (номер), покупка/продажа, SOL, токены, MC, резерв SOL кривой, пул;
  wallets.csv.gz  — кошельки (номер → адрес) с 3+ сделками в выгрузке или известной ролью: роль (снайпер, бандлер,
                    дев…), дата создания, кто пополнил, метки, статистика, свой список / smart / мои девы;
  real.csv.gz     — все сделки реального бота (тень и бой): задержка, блоки, MC сигнала и входа, итог — для калибровки
                    скорости и проскальзывания; trials.csv.gz — пробы всех правил выхода на тех же входах;
  alerts.csv.gz   — алерты.
Ключей и личного в выгрузке нет: только данные блокчейна и настройки отбора. Файлы по ≤ 45 МБ — уходят в Telegram
документами (тема «Система») и остаются на сервере в logs/research/.

Сервер: docker compose run --rm bot research [--days 14] [--window 60] [--no-send]
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

log = logging.getLogger(__name__)

BATCH = 300                      # монет на один запрос сделок
PART_BYTES = 40_000_000          # сжатый файл сделок — не больше ~40 МБ (Telegram — до 50 МБ)
MIN_WALLET_TRADES = 3
OUT_DIR = os.environ.get("RESEARCH_DIR", "/app/logs/research")

README = """Выгрузка для исследования — сырые данные базы бота (UTF-8, разделитель «;», числа с точкой).

coins.csv.gz — одна строка на монету (созданы за период, у каждой есть полное окно сделок):
  i — номер монеты (как в trades), mint, created (unix, с), created_slot, dev (номер кошелька в wallets), cluster,
  init_mc / init_buy (SOL, из события создания), socials (сколько ссылок), migrated_s (через сколько секунд ушла на
  PumpSwap, пусто — не ушла), dev_prev7d (монет у дева за 7 дней до этой), cl_blacklisted / cl_launches /
  cl_repeat (кластер дева: чёрный список, запусков, повторяемость), а также поля разбора запуска (launches):
  peak_mc, dump_mc, time_to_dump_s, sold_share, cluster_share_start, early_buyers, achievable_profit, entry_mc,
  mc_30s, mc_60s, mc_120s, dev_buy_sol, bundle_wallets, bundle_share, l_migrated, features (json), а ещё
  alerts (сколько алертов), alert_modes, alert_first_s (через сколько секунд после создания первый алерт),
  name, symbol, image (ссылка на картинку), twitter, website, desc_len (длина описания).

trades_NNN.csv.gz — сделки первых N минут монеты, по времени:
  c — номер монеты, t — секунды от создания, s — блоков от блока создания (пусто — неизвестно), w — номер кошелька,
  b — 1 покупка / 0 продажа, sol, tok (токены), mc (капитализация после сделки, SOL), vsol (виртуальный резерв SOL
  кривой после сделки, если известен), amm — 1 сделка в пуле PumpSwap, nb — баланс токенов кошелька после сделки.

wallets.csv.gz — w, address, role (dev/trader/sniper/bundler/kol/exchange/service), created (unix, оценка),
  funded_by, funded_sol, exchange, label, tags, winrate, pnl_7d, pnl_30d, trades_30d, avg_hold_s, copyability,
  own (в своём списке), my_dev, smart (активный smart-money), copy_30d, median_hold_s, best_rule, n (сделок в выгрузке).

real.csv.gz — сделки реального бота (тень и бой): режим, метод, монета, трейдер, открыта/закрыта (unix), блок и MC
  сигнала, задержка, MC входа и выхода, вложено/получено SOL, итог, причина выхода, extra (json).
trials.csv.gz — пробы правил выхода (A/B/G/R/W) на тех же входах реального бота.
alerts.csv.gz — алерты: монета, режим, уровень, сигнал, трейдер, время, MC алерта / входа / цели.
"""


class GzCsv:
    """CSV в gzip с переходом на следующий файл, когда сжатый размер подходит к пределу."""

    def __init__(self, folder: str, stem: str, header: list[str], part_bytes: int | None = None):
        self.folder, self.stem, self.header, self.part_bytes = folder, stem, header, part_bytes
        self.files: list[str] = []
        self.rows = 0
        self._open()

    def _open(self) -> None:
        name = f"{self.stem}_{len(self.files) + 1:03d}.csv.gz" if self.part_bytes else f"{self.stem}.csv.gz"
        path = os.path.join(self.folder, name)
        self.files.append(path)
        self._raw = open(path, "wb")
        self._gz = gzip.GzipFile(fileobj=self._raw, mode="wb", compresslevel=9)
        self._txt = io.TextIOWrapper(self._gz, encoding="utf-8", newline="")
        self._w = csv.writer(self._txt, delimiter=";")
        self._w.writerow(self.header)

    def write(self, row: list[Any]) -> None:
        self._w.writerow(row)
        self.rows += 1
        if self.part_bytes and self.rows % 20000 == 0:
            self._txt.flush()
            if self._raw.tell() >= self.part_bytes:
                self.close()
                self._open()

    def close(self) -> None:
        self._txt.close()
        self._raw.close()


def _n(v: Any, nd: int = 6) -> str:
    """Число коротко: без лишних нулей; None — пусто."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, int):
        return str(v)
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if f != f:                                 # NaN
        return ""
    s = f"{f:.{nd}g}"
    return s


def _clean(v: Any, limit: int = 120) -> str:
    """Текст для CSV одной строкой: без «;» и переводов строк."""
    return "" if v is None else str(v).replace(";", ",").replace("\n", " ").replace("\r", " ")[:limit]


def _ts(v: Any) -> str:
    return "" if v is None else str(int(v.timestamp()))


async def run(db: Any, env: Any = None, days: float = 14.0, window_min: float = 60.0, send: bool = True,
              progress: Callable[[str], None] = print, out_dir: str = OUT_DIR) -> str:
    now = datetime.now(timezone.utc)
    stamp = f"{now:%Y%m%d_%H%M}"
    folder = os.path.join(out_dir, stamp)
    os.makedirs(folder, exist_ok=True)
    window = timedelta(minutes=window_min)
    since, until = now - timedelta(days=days), now - window
    wid: dict[str, int] = {}
    wcount: dict[int, int] = {}

    def w(addr: str | None) -> str:
        if not addr:
            return ""
        i = wid.get(addr)
        if i is None:
            i = wid[addr] = len(wid) + 1
        return str(i)

    # ------------------------------------------------------------------ монеты
    progress(f"монеты за {days:g} дн. …")
    coins = await db.fetch(
        """SELECT * FROM (
               SELECT t.mint, t.created_at, t.created_slot, t.deployer, t.cluster_id, t.initial_mc_sol,
                      t.initial_buy_sol, t.name, t.symbol, t.image, t.twitter, t.website,
                      length(t.description) AS desc_len, (t.twitter IS NOT NULL)::int + (t.telegram IS NOT NULL)::int
                          + (t.website IS NOT NULL)::int AS socials,
                      extract(epoch FROM t.migrated_at - t.created_at) AS migrated_s,
                      count(*) OVER (PARTITION BY t.deployer ORDER BY t.created_at
                                     RANGE BETWEEN interval '7 days' PRECEDING AND CURRENT ROW) - 1 AS dev_prev7d
               FROM tokens t WHERE t.created_at >= $1::timestamptz - interval '7 days' AND t.created_at < $2) x
           WHERE created_at >= $1 ORDER BY created_at""", since, until)
    mints = [r["mint"] for r in coins]
    launches = {r["mint"]: r for r in await db.fetch("SELECT * FROM launches WHERE mint = ANY($1::text[])", mints)}
    clusters = {r["id"]: r for r in await db.fetch("SELECT id, blacklisted, profile FROM clusters")}
    alerts_by: dict[str, list[Any]] = {}
    for r in await db.fetch(
            """SELECT mint, mode, created_at FROM alerts WHERE created_at >= $1::timestamptz - interval '1 hour'
               ORDER BY created_at""", since):
        alerts_by.setdefault(r["mint"], []).append(r)
    lcols = ["peak_mc", "dump_mc", "time_to_dump_s", "sold_share", "cluster_share_start", "early_buyers",
             "achievable_profit", "entry_mc", "mc_30s", "mc_60s", "mc_120s", "dev_buy_sol", "bundle_wallets",
             "bundle_share"]
    cw = GzCsv(folder, "coins", ["i", "mint", "created", "created_slot", "dev", "cluster", "init_mc", "init_buy",
                                 "socials", "migrated_s", "dev_prev7d", "cl_blacklisted", "cl_launches", "cl_repeat",
                                 *lcols, "l_migrated", "features", "alerts", "alert_modes", "alert_first_s",
                                 "name", "symbol", "image", "twitter", "website", "desc_len"])
    cidx: dict[str, tuple[int, datetime, int | None]] = {}
    for i, r in enumerate(coins, 1):
        cidx[r["mint"]] = (i, r["created_at"], r["created_slot"])
        lr = launches.get(r["mint"])
        cl = clusters.get(r["cluster_id"]) if r["cluster_id"] else None
        prof = (cl["profile"] if cl else None) or {}
        if isinstance(prof, str):
            prof = json.loads(prof)
        al = alerts_by.get(r["mint"], [])
        feats = (lr["features"] if lr else None) or {}
        cw.write([i, r["mint"], _ts(r["created_at"]), r["created_slot"] or "", w(r["deployer"]), r["cluster_id"] or "",
                  _n(r["initial_mc_sol"]), _n(r["initial_buy_sol"]), r["socials"], _n(r["migrated_s"], 7),
                  r["dev_prev7d"], _n(cl["blacklisted"]) if cl else "", _n(prof.get("launches")),
                  _n(prof.get("repeatability")), *[_n(lr[c]) if lr else "" for c in lcols],
                  _n(lr["migrated"]) if lr else "",
                  json.dumps(feats if isinstance(feats, dict) else json.loads(feats), ensure_ascii=False,
                             separators=(",", ":")) if feats else "",
                  len(al), ",".join(sorted({a["mode"] for a in al})),
                  _n((al[0]["created_at"] - r["created_at"]).total_seconds(), 7) if al else "",
                  _clean(r["name"]), _clean(r["symbol"]), _clean(r["image"]), _clean(r["twitter"]),
                  _clean(r["website"]), r["desc_len"] or 0])
    cw.close()
    progress(f"монет: {len(coins)}")

    # ------------------------------------------------------------------ сделки
    tw = GzCsv(folder, "trades", ["c", "t", "s", "w", "b", "sol", "tok", "mc", "vsol", "amm", "nb"], PART_BYTES)
    for k in range(0, len(mints), BATCH):
        part = mints[k:k + BATCH]
        rows = await db.fetch(
            """SELECT t.mint, extract(epoch FROM t.ts) AS ts, t.slot, t.trader, t.is_buy, t.sol_amount, t.token_amount,
                      t.mc_sol, t.v_sol, t.pool, t.new_balance
               FROM unnest($1::text[], $2::timestamptz[]) AS p(mint, c)
               JOIN trades t ON t.mint = p.mint AND t.ts >= p.c - interval '5 seconds' AND t.ts < p.c + $3::interval
               ORDER BY t.mint, t.ts, t.id""",
            part, [cidx[m][1] for m in part], window)
        for r in rows:
            ci, created, cslot = cidx[r["mint"]]
            wi = w(r["trader"])
            wcount[int(wi)] = wcount.get(int(wi), 0) + 1
            tw.write([ci, f"{float(r['ts']) - created.timestamp():.2f}",
                      (r["slot"] - cslot) if (r["slot"] is not None and cslot is not None) else "", wi,
                      1 if r["is_buy"] else 0, _n(r["sol_amount"]), _n(r["token_amount"], 7), _n(r["mc_sol"]),
                      _n(r["v_sol"]), 0 if r["pool"] == "pump" else 1, _n(r["new_balance"], 7)])
        if (k // BATCH) % 10 == 0:
            progress(f"сделки: монет {min(k + BATCH, len(mints))} из {len(mints)}, строк {tw.rows}")
    tw.close()
    progress(f"сделок: {tw.rows}, файлов {len(tw.files)}")

    # ------------------------------------------------------------------ кошельки
    own = {r["address"] for r in await db.fetch("SELECT address FROM watchlist WHERE kind = 'trader'")}
    my_devs = {r["address"] for r in await db.fetch("SELECT address FROM watchlist WHERE kind = 'dev'")}
    traders = {r["address"]: r for r in await db.fetch(
        "SELECT address, active, copy_30d, median_hold_s, best_rule FROM traders")}
    keep = [a for a, i in wid.items() if wcount.get(i, 0) >= MIN_WALLET_TRADES or a in own or a in my_devs
            or a in traders]
    addrs = list(wid)
    roles: set[str] = set()
    for k in range(0, len(addrs), 20000):
        roles |= {r["address"] for r in await db.fetch(
            "SELECT address FROM wallets WHERE role IS NOT NULL AND address = ANY($1::text[])", addrs[k:k + 20000])}
    keep_set = set(keep) | roles
    info: dict[str, Any] = {}
    keep_list = list(keep_set)
    for k in range(0, len(keep_list), 5000):
        for r in await db.fetch(
                """SELECT address, role, created_est, funded_by, funded_sol, is_exchange, label, tags, winrate, pnl_7d,
                          pnl_30d, trades_30d, avg_hold_s, copyability FROM wallets WHERE address = ANY($1::text[])""",
                keep_list[k:k + 5000]):
            info[r["address"]] = r
    ww = GzCsv(folder, "wallets", ["w", "address", "role", "created", "funded_by", "funded_sol", "exchange", "label",
                                   "tags", "winrate", "pnl_7d", "pnl_30d", "trades_30d", "avg_hold_s", "copyability",
                                   "own", "my_dev", "smart", "copy_30d", "median_hold_s", "best_rule", "n"])
    for a in keep_set:
        i = wid[a]
        r, t = info.get(a), traders.get(a)
        ww.write([i, a, (r["role"] if r else "") or "", _ts(r["created_est"]) if r else "",
                  (r["funded_by"] if r else "") or "", _n(r["funded_sol"]) if r else "",
                  _n(r["is_exchange"]) if r else "", ((r["label"] or "") if r else "").replace(";", ","),
                  json.dumps(r["tags"], ensure_ascii=False) if r and r["tags"] else "",
                  *[_n(r[c]) if r else "" for c in ("winrate", "pnl_7d", "pnl_30d", "trades_30d", "avg_hold_s",
                                                    "copyability")],
                  1 if a in own else 0, 1 if a in my_devs else 0, 1 if t and t["active"] else 0,
                  _n(t["copy_30d"]) if t else "", _n(t["median_hold_s"]) if t else "",
                  (t["best_rule"] or "") if t else "", wcount.get(i, 0)])
    ww.close()
    progress(f"кошельков: всего {len(wid)}, в файле {len(keep_set)}")

    # ------------------------------------------------------------------ реальный бот, пробы, алерты
    rw = GzCsv(folder, "real", ["id", "mode", "kind", "mint", "trader", "venue", "opened", "closed", "trigger_slot",
                                "trigger_mc", "lag_s", "entry_mc", "entry_slot", "exit_mc", "cost_sol", "proceeds_sol",
                                "pnl_sol", "pnl_pct", "exit_reason", "extra"])
    for r in await db.fetch("SELECT * FROM real_positions ORDER BY id"):
        d = dict(r)
        rw.write([d.get("id"), d.get("mode"), d.get("kind"), d.get("mint"), d.get("trader") or "", d.get("venue") or "",
                  _ts(d.get("opened_at")), _ts(d.get("closed_at")), d.get("trigger_slot") or "",
                  _n(d.get("trigger_mc")), _n(d.get("lag_s")), _n(d.get("entry_mc")), d.get("entry_slot") or "",
                  _n(d.get("exit_mc")), _n(d.get("cost_sol")), _n(d.get("proceeds_sol")), _n(d.get("pnl_sol")),
                  _n(d.get("pnl_pct")), d.get("exit_reason") or "",
                  json.dumps(d.get("extra") or {}, ensure_ascii=False, default=str, separators=(",", ":"))])
    rw.close()
    trw = GzCsv(folder, "trials", ["position_id", "mode", "kind", "rule", "mint", "trader", "opened", "closed",
                                   "entry_mc", "exit_mc", "max_mc", "pnl_pct", "exit_reason"])
    for r in await db.fetch("SELECT * FROM real_rule_trials ORDER BY id"):
        trw.write([r["position_id"] or "", r["mode"], r["kind"], r["rule"], r["mint"], r["trader"] or "",
                   _ts(r["opened_at"]), _ts(r["closed_at"]), _n(r["entry_mc"]), _n(r["exit_mc"]), _n(r["max_mc"]),
                   _n(r["pnl_pct"]), r["exit_reason"] or ""])
    trw.close()
    aw = GzCsv(folder, "alerts", ["id", "mint", "mode", "level", "signal", "trader", "created", "mc_alert", "entry_mc",
                                  "target_mc"])
    for r in await db.fetch(
            """SELECT id, mint, mode, level, signal, trader, created_at, mc_alert, entry_mc, target_mc FROM alerts
               WHERE created_at >= $1 ORDER BY id""", since):
        aw.write([r["id"], r["mint"], r["mode"], r["level"], r["signal"] or "", r["trader"] or "", _ts(r["created_at"]),
                  _n(r["mc_alert"]), _n(r["entry_mc"]), _n(r["target_mc"])])
    aw.close()
    sol = (await db.kv_get("sol_usd", {}) or {}).get("price")
    with open(os.path.join(folder, "README.txt"), "w", encoding="utf-8") as f:
        f.write(README + f"\nВыгружено {now:%Y-%m-%d %H:%M} UTC · период {days:g} дн. · окно сделок {window_min:g} мин"
                f" · курс SOL ${sol}\n")
    files = [os.path.join(folder, "README.txt"), *cw.files, *ww.files, *rw.files, *trw.files, *aw.files, *tw.files]
    total = sum(os.path.getsize(p) for p in files)
    summary = (f"🔬 Выгрузка для исследования: монет {len(coins)}, сделок {tw.rows}, кошельков {len(keep_set)} · "
               f"файлов {len(files)}, {total / 1e6:.1f} МБ · на сервере: logs/research/{stamp}/")
    if send and env is not None:
        await _send(db, env, files, summary, progress)
    return summary


async def _send(db: Any, env: Any, files: list[str], summary: str, progress: Callable[[str], None]) -> None:
    """Файлы — прямо в Telegram (тема «Система»), минуя очередь: архивы большие, в базу их не кладём."""
    from aiogram import Bot
    from aiogram.types import FSInputFile
    if not env.bot_token or not env.alert_chat_id:
        progress("Telegram не настроен — файлы остались на сервере")
        return
    topics = await db.kv_get("topics", {}) or {}
    thread = topics.get("system")
    bot = Bot(env.bot_token)
    try:
        await bot.send_message(env.alert_chat_id, summary + "\nПерешли эти файлы в чат для анализа.",
                               message_thread_id=thread)
        for p in files:
            size = os.path.getsize(p)
            if size > 49_000_000:
                progress(f"{os.path.basename(p)}: {size / 1e6:.0f} МБ — больше лимита Telegram, остался на сервере")
                continue
            await bot.send_document(env.alert_chat_id, FSInputFile(p), message_thread_id=thread,
                                    disable_notification=True, request_timeout=600)
            progress(f"отправлен {os.path.basename(p)} ({size / 1e6:.1f} МБ)")
    finally:
        await bot.session.close()
