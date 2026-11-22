"""Экран /real в Telegram: состояние реального бота, кнопки управления, сделки, сравнение с бумагой, настройки.

Работает в сервисе бота и читает только базу: состояние исполнителя (kv «real:status», аналитика обновляет его
раз в 5 с), таблицы real_positions / real_orders и настройки. Команды исполнителю уходят через очередь заданий
(db.job("real", …)) — так же, как у бумаги. Ключа кошелька здесь нет и быть не может.

Все нажатия обрабатывает on_callback(): он возвращает Reply (текст, кнопки, всплывающая подпись, файл), а
обработчик в bot/handlers.py только показывает его. Поэтому логику экрана можно проверять тестами без Telegram.
"""
from __future__ import annotations

import csv
import io
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from ..alerts.format import esc
from ..analytics.paper import ENTRY_BLOCKS, SCHEMES as PAPER_SCHEMES
from . import settings as S

STATUS_KEY = "real:status"
STALE_S = 90.0
KINDS = S.KIND_NAMES
MODE_ICON = {"off": "⚪", "shadow": "👻", "live": "💵"}
MODE_SHORT = {"off": "выкл", "shadow": "тень", "live": "бой"}
EXIT_NAMES = {"take_profit": "тейк", "stop": "стоп", "trailing": "трейлинг", "time_stop": "нет роста",
              "max_hold": "время", "trader_sell": "трейдер продал", "tiny_rebuy": "мелкая докупка",
              "emergency": "дев продаёт", "manual": "вручную", "sell_all": "продать всё", "gone": "токенов нет"}
PAPER_OF = {"smart": "M", "mydev": "Y"}        # с каким бумажным портфелем сравнивается вид сигнала
Kb = list[list[dict[str, Any]]]


@dataclass
class Reply:
    text: str | None = None
    kb: Kb | None = None
    toast: str = ""
    edit: bool = False                         # заменить текст сообщения с кнопкой, а не слать новое
    document: tuple[bytes, str, str] | None = None     # (содержимое, имя файла, подпись)
    ask: str = ""                              # ждать текст от пользователя (например, название кошелька)


def tz_of(cfg: dict[str, Any]) -> ZoneInfo:
    try:
        return ZoneInfo(str(cfg.get("paper_reset_tz") or "Asia/Tbilisi"))
    except Exception:  # noqa: BLE001
        return ZoneInfo("Asia/Tbilisi")


def day_start(cfg: dict[str, Any]) -> datetime:
    return datetime.now(tz_of(cfg)).replace(hour=0, minute=0, second=0, microsecond=0)


# «Считать с нуля» (кнопка в «Против бумаги» и на экране методов): с этого момента считаются сделки бота, пробы правил
# и итог бумаги на экранах методов. Ничего не удаляется — меняется только начало подсчёта
STATS_SINCE_KEY = "real:stats_since"


async def stats_since(db: Any) -> datetime | None:
    v = await db.kv_get(STATS_SINCE_KEY)
    try:
        return datetime.fromisoformat(v) if v else None
    except (TypeError, ValueError):
        return None


def money(sol: float, sol_usd: float) -> str:
    v = sol * sol_usd
    return f"{'+' if v > 0 else '−' if v < 0 else ''}${abs(v):.2f}"


def scheme_text(name: str) -> str:
    return str(PAPER_SCHEMES[name]["name"]).split(": ", 1)[-1].replace("smart — ", "")


async def sol_price(db: Any, st: dict[str, Any] | None = None) -> float:
    """Курс SOL, $: тот же, что публикует аналитика (kv «sol_usd»); запасной — из состояния исполнителя.
    17.11: при кошельках общего состояния нет, и список сделок считал по $150 вместо $74 — доллары вдвое больше."""
    price = (await db.kv_get("sol_usd", {}) or {}).get("price")
    return float(price or (st or {}).get("sol_usd") or 150)


async def load_status(db: Any) -> tuple[dict[str, Any], float | None]:
    """Состояние исполнителя и его возраст в секундах (None — исполнитель ещё не запускался)."""
    st = await db.kv_get(STATUS_KEY) or {}
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(st["at"])).total_seconds()
    except Exception:  # noqa: BLE001
        age = None
    return st, age


async def totals(db: Any, since: datetime | None = None, wallet: Any = "all") -> dict[str, dict[str, float]]:
    """Итог закрытых сделок по режимам: число, в плюс, итог в SOL; для боя — комиссии отклонённых транзакций.
    wallet — номер кошелька (None — общая полоса без кошелька), "all" — все вместе.
    После «🔄 Считать с нуля» — не раньше нажатия (стопы боя считает исполнитель сам, кнопка их не трогает)."""
    zero = await stats_since(db)
    if zero is not None and (since is None or zero > since):
        since = zero
    out: dict[str, dict[str, float]] = {m: {"n": 0, "wins": 0, "pnl": 0.0, "fails": 0, "fail_fees": 0.0}
                                        for m in ("shadow", "live")}
    every = wallet == "all"
    wid = None if every else wallet
    for r in await db.fetch(
            "SELECT mode, count(*) n, count(*) FILTER (WHERE pnl_sol > 0) wins, COALESCE(sum(pnl_sol), 0) pnl "
            "FROM real_positions WHERE closed_at IS NOT NULL AND ($1::timestamptz IS NULL OR closed_at >= $1) "
            "AND ($2 OR wallet_id IS NOT DISTINCT FROM $3::bigint) GROUP BY mode", since, every, wid):
        if r["mode"] in out:
            out[r["mode"]].update(n=int(r["n"]), wins=int(r["wins"]), pnl=float(r["pnl"]))
    for r in await db.fetch(
            "SELECT mode, count(*) n, COALESCE(sum(fee_sol), 0) fees FROM real_orders WHERE status = 'failed' "
            "AND mode = 'live' AND side IN ('buy', 'sell') AND signature IS NOT NULL "
            "AND ($1::timestamptz IS NULL OR created_at >= $1) AND ($2 OR wallet_id IS NOT DISTINCT FROM $3::bigint) "
            "GROUP BY mode", since, every, wid):
        out["live"].update(fails=int(r["n"]), fail_fees=float(r["fees"]))
    return out


DESK_KEY = "real:desk"


async def load_desk(db: Any) -> tuple[dict[str, Any], float | None]:
    """Состояние кошельков (публикует аналитика) и его возраст в секундах."""
    st = await db.kv_get(DESK_KEY) or {}
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(st["at"])).total_seconds()
    except Exception:  # noqa: BLE001
        age = None
    return st, age


async def wallet_rows(db: Any) -> list[Any]:
    """Кошельки из списка — без ключей: бот (Telegram) их не читает никогда."""
    return await db.fetch("SELECT id, name, pubkey, source, settings, created_at, secret_enc IS NOT NULL AS stored "
                          "FROM real_wallets WHERE NOT archived ORDER BY id")


async def lane_status(db: Any, wid: int) -> tuple[dict[str, Any], float | None]:
    st = await db.kv_get(f"{STATUS_KEY}:{wid}") or {}
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(st["at"])).total_seconds()
    except Exception:  # noqa: BLE001
        age = None
    return st, age


def _row(name: str, t: dict[str, float], sol_usd: float) -> str:
    n = int(t["n"])
    win = f"{t['wins'] / n:.0%}" if n else "—"
    total = t["pnl"] - t.get("fail_fees", 0.0)
    return f"{name:<13}{n:>5} {win:>6} {money(total, sol_usd):>9}"


# ---------------------------------------------------------------------- главный экран
async def status_view(db: Any, cfg: dict[str, Any]) -> tuple[str, Kb]:
    st, age = await load_status(db)
    desk, desk_age = await load_desk(db)
    wallets = await wallet_rows(db)
    if desk:
        age = desk_age
    sol_usd = await sol_price(db, st)
    want = S.get(cfg, "real_mode")
    fresh = age is not None and age < STALE_S
    mode = st.get("mode") if fresh and st else None
    lines = ["💵 <b>Реальный бот</b>"]
    if age is None:
        lines.append("⚠️ Исполнитель ещё не запускался (сервис analytics не обновлён или не поднялся).")
    elif age >= STALE_S:
        lines.append(f"⚠️ Исполнитель не отвечает {age / 60:.0f} мин — сервис analytics остановлен или перезапускается.")
    paused = str(cfg.get("real_paused") or "")
    statuses: list[tuple[str, dict[str, Any]]] = []     # (имя кошелька, состояние его полосы)
    if st and fresh:
        statuses.append(("", st))
    kb: Kb = [[{"text": "🔄 Обновить", "callback": "rl:view"}]]
    if wallets:
        lines.append(f"<b>Кошельки: {len(wallets)}</b>")
        for w in wallets:
            ws, wage = await lane_status(db, int(w["id"]))
            wfresh = wage is not None and wage < STALE_S
            if wfresh:
                statuses.append((w["name"], ws))
            wmode = ws.get("mode") if wfresh else S.get({**cfg, **(w["settings"] or {})}, "real_mode")
            bal = ws.get("balance_sol") if wfresh else None
            lines.append(f"{MODE_ICON.get(wmode, '')} {esc(w['name'])} — "
                         + (f"{bal:.4f} SOL (${bal * sol_usd:.2f})" if bal is not None else "баланс неизвестен")
                         + f" · позиций {len(ws.get('open') or []) if wfresh else 0}")
        if paused:
            lines.append(f"⛔ Новые входы остановлены у всех кошельков: {esc(paused)}")
        lines.append(f"По умолчанию для кошельков: ставка {S.fmt('real_stake_usd', S.get(cfg, 'real_stake_usd'))}, "
                     f"позиций до {S.get(cfg, 'real_max_open')}, дневной стоп "
                     f"{S.fmt('real_day_stop_usd', S.get(cfg, 'real_day_stop_usd'))}, общий стоп "
                     f"{S.fmt('real_total_stop_usd', S.get(cfg, 'real_total_stop_usd'))} — у каждого кошелька "
                     "можно своё.")
        kb.append([{"text": f"👛 Кошельки ({len(wallets)})", "callback": "rl:wl"}])
    else:
        shown = mode or want
        lines.append(f"Режим: {MODE_ICON[shown]} <b>{S.MODES[shown]}</b>")
        if want == "live" and mode and mode != "live":
            if st.get("want") != "live" or not st.get("why_not_live"):
                lines.append("⏳ Бой включается — нажмите 🔄 Обновить через пару секунд.")
            else:
                lines.append(f"⚠️ «Бой» выбран, но не включён: {esc(st['why_not_live'])}.")
        if paused:
            lines.append(f"⛔ Новые входы остановлены: {esc(paused)}")
        if st.get("wallet"):
            bal = st.get("balance_sol")
            lines.append(f"Кошелёк: <code>{st['wallet']}</code> · "
                         + (f"{bal:.4f} SOL (${bal * sol_usd:.2f})" if bal is not None else "баланс неизвестен")
                         + f" · ключ {'есть' if st.get('can_sign') else 'не задан'}")
        else:
            lines.append("Кошелёк не задан — тень считает сделки по состоянию сети, без симуляции транзакций.")
        lines.append(f"Ставка <b>{S.fmt('real_stake_usd', S.get(cfg, 'real_stake_usd'))}</b> (потолок сервера "
                     f"${S.max_stake_usd():g}) · позиций до {S.get(cfg, 'real_max_open')} · дневной стоп "
                     f"{S.fmt('real_day_stop_usd', S.get(cfg, 'real_day_stop_usd'))} · общий стоп "
                     f"{S.fmt('real_total_stop_usd', S.get(cfg, 'real_total_stop_usd'))}")
        kb.append([{"text": ("● " if want == m else "") + f"{MODE_ICON[m]} {MODE_SHORT[m].capitalize()}"
                    + ("…" if m == "live" and want != "live" else ""), "callback": f"rl:mode:{m}"}
                   for m in ("off", "shadow", "live")])
        kb.append([{"text": "👛 Кошельки: создать и пополнить", "callback": "rl:wl"}])
    lines.append(("Методы по умолчанию: " if wallets else "Методы: ") + S.methods_summary(cfg))
    opened = [(name, o) for name, x in statuses for o in (x.get("open") or [])]
    if opened:
        lines.append(f"\n<b>Открыто: {len(opened)}</b>")
        for name, o in opened[:10]:
            lines.append(f"• {MODE_ICON.get(o.get('mode'), '')} ${esc(o.get('symbol') or '?')} "
                         f"({KINDS.get(o.get('kind'), o.get('kind'))}) {float(o.get('net') or 0):+.0%} · "
                         f"{float(o.get('age_s') or 0) / 60:.0f} мин" + (f" · {esc(name)}" if name else "")
                         + (" · 🆘 продажа не проходит" if o.get("stuck") else ""))
        for _name, o in opened[:4]:
            if o.get("id"):
                kb.append([{"text": f"Продать ${(o.get('symbol') or '?')[:12]}", "callback": f"rl:sell:{o['id']}"}])
    today, alltime = await totals(db, day_start(cfg)), await totals(db)
    zero = await stats_since(db)
    tail = f"с {zero.astimezone(tz_of(cfg)):%d.%m}" if zero else "всего"
    table = [f"{'':<13}{'сдел.':>5} {'в плюс':>6} {'итог':>9}",
             _row("Тень сегодня", today["shadow"], sol_usd), _row(f"Тень {tail}", alltime["shadow"], sol_usd),
             _row("Бой сегодня", today["live"], sol_usd), _row(f"Бой {tail}", alltime["live"], sol_usd)]
    lines.append("\n<pre>" + "\n".join(table) + "</pre>")
    if zero:
        lines.append(f"Итоги считаются с {zero.astimezone(tz_of(cfg)):%d.%m %H:%M} (кнопка «🔄 Считать с нуля»).")
    if alltime["live"]["fails"]:
        lines.append(f"Отклонённые сетью транзакции боя: {int(alltime['live']['fails'])}, комиссии "
                     f"{money(-alltime['live']['fail_fees'], sol_usd)} (уже в итоге).")
    skips: dict[str, int] = {}
    for _name, x in statuses:
        for k, v in (x.get("skips") or {}).items():
            skips[k] = skips.get(k, 0) + int(v)
    if skips:
        since = min((x.get("since") or "" for _n, x in statuses if x.get("since")), default=None)
        lines.append("\n<b>Пропуски</b> (с запуска " + _since(since, cfg) + "):")
        lines += [f"• {esc(k)} — {v}" for k, v in sorted(skips.items(), key=lambda kv: -kv[1])[:8]]
    kb.append([{"text": "▶️ Продолжить входы", "callback": "rl:resume"} if paused else
               {"text": "⛔ Стоп входов", "callback": "rl:pause"},
               {"text": "💰 Продать всё…", "callback": "rl:sellall"}])
    kb.append([{"text": "📋 Сделки", "callback": "rl:trades"}, {"text": "⚖️ Против бумаги", "callback": "rl:cmp"}])
    kb.append([{"text": "📊 Excel: весь отчёт", "callback": "rl:xlsx"}, {"text": "📄 CSV сделок", "callback": "rl:csv"}])
    kb.append([{"text": "🎯 Методы", "callback": "rl:wme:g"}, {"text": "⚙️ Настройки", "callback": "rl:set"},
               {"text": "🧪 Проверка", "callback": "rl:check"}])
    kb.append([{"text": "👛 Мои кошельки", "callback": "rl:mine"},
               {"text": "🔄 Считать с нуля", "callback": "rl:zero:view"}])
    return "\n".join(lines), kb


MINE_USD = 10.0
MINE_RULES = ("A", "B", "G", "R", "W")


async def mine_view(db: Any, cfg: dict[str, Any]) -> tuple[str, Kb]:
    """22.11: «👛 Мои кошельки» — как торговал бы бот за кошельками «за ним» с момента «Считать с нуля»: итог бота
    (его выход по лучшему правилу кошелька) и пробы всех наших выходов на тех же входах; $10 на сделку."""
    follow = dict(await db.kv_get("real:follow") or {})
    auto = set(await db.kv_get("real:follow_auto") or [])
    zero = await stats_since(db)
    since = zero or datetime(2000, 1, 1, tzinfo=timezone.utc)
    head = (f"👛 <b>Мои кошельки</b> — реальный бот за ними, ${MINE_USD:g} на сделку, "
            + (f"с {zero.astimezone(tz_of(cfg)):%d.%m %H:%M}" if zero else "за всё время"))
    back = [[{"text": "🔄 Обновить", "callback": "rl:mine"}, {"text": "🔄 Считать с нуля", "callback": "rl:zero:mine"}],
            [{"text": "← Назад", "callback": "rl:view"}]]
    if not follow:
        return (head + "\n\nПока ни за кем. Выбрать: /card начало адреса → «👻 Тень за ним» (без денег) или "
                "«💵 Бой за ним»."), back
    addrs = list(follow)
    labels = {r["address"]: r["label"] for r in await db.fetch(
        "SELECT address, label FROM watchlist WHERE kind = 'trader' AND address = ANY($1::text[])", addrs)}
    pos: dict[str, dict[str, float]] = {}
    for r in await db.fetch(
            """SELECT trader, count(*) FILTER (WHERE closed_at IS NOT NULL) n,
                      count(*) FILTER (WHERE closed_at IS NULL) open,
                      count(*) FILTER (WHERE closed_at IS NOT NULL AND pnl_pct > 0) w,
                      COALESCE(sum(pnl_pct) FILTER (WHERE closed_at IS NOT NULL), 0) s
               FROM real_positions WHERE trader = ANY($1::text[]) AND opened_at >= $2 GROUP BY 1""", addrs, since):
        pos[r["trader"]] = dict(r)
    trials: dict[str, dict[str, float]] = {}
    for r in await db.fetch(
            """SELECT trader, rule, COALESCE(sum(pnl_pct), 0) s FROM real_rule_trials
               WHERE trader = ANY($1::text[]) AND closed_at IS NOT NULL AND opened_at >= $2 GROUP BY 1, 2""",
            addrs, since):
        trials.setdefault(r["trader"], {})[r["rule"]] = float(r["s"])

    def usd(v: float) -> str:
        v *= MINE_USD
        return f"{'+' if v >= 0 else '−'}${abs(v):.0f}" if abs(v) >= 10 else f"{'+' if v >= 0 else '−'}${abs(v):.2f}"

    def line(a: str) -> str:
        name = esc((labels.get(a) or f"{a[:4]}…{a[-4:]}")[:14])
        icon = "💵" if follow[a] == "live" else "👻"
        p = pos.get(a)
        if not p:
            return f"{icon} <b>{name}</b>: сделок пока нет"
        opened = f" (+{p['open']} откр.)" if p["open"] else ""
        rules = " · ".join(f"{k} {usd(trials[a][k])}" for k in MINE_RULES if k in trials.get(a, {}))
        return (f"{icon} <b>{name}</b>: {p['n']} сд{opened}, в плюсе {p['w']} · бот <b>{usd(float(p['s']))}</b>"
                + (f"\n   выходы: {rules}" if rules else ""))
    mine = [a for a in addrs if a not in auto]
    bot = [a for a in addrs if a in auto]
    lines = [head]
    if mine:
        lines += ["", "👤 <b>Выбрал ты</b>", *[line(a) for a in mine]]
    if bot:
        lines += ["", "🤖 <b>Бот включил сам</b> (не торгуют, если включено «🎯 Только выбранные мной»)",
                  *[line(a) for a in bot]]
    lines += ["", "«бот» — как торговал бот (его выход); «выходы» — те же входы, если бы продавал по A / B / G / R / W. "
              "👻 — тень (без денег), 💵 — бой."]
    return "\n".join(lines), back


def _since(iso: str | None, cfg: dict[str, Any]) -> str:
    try:
        return datetime.fromisoformat(str(iso)).astimezone(tz_of(cfg)).strftime("%d.%m %H:%M")
    except Exception:  # noqa: BLE001
        return "—"


# ---------------------------------------------------------------------- включение боя
async def live_confirm_view(db: Any, cfg: dict[str, Any]) -> tuple[str, Kb]:
    st, age = await load_status(db)
    sol_usd = await sol_price(db, st)
    fresh = age is not None and age < STALE_S
    allowed, can_sign = bool(st.get("live_allowed")) and fresh, bool(st.get("can_sign")) and fresh
    bal = st.get("balance_sol") if fresh else None
    need = S.get(cfg, "real_min_stake_usd") / sol_usd + S.get(cfg, "real_reserve_sol")
    first = await db.fetchval("SELECT min(opened_at) FROM real_positions WHERE mode = 'shadow'")
    days = (datetime.now(timezone.utc) - first).total_seconds() / 86400 if first else 0.0
    t = (await totals(db))["shadow"]
    ok = lambda v: "✅" if v else "❌"  # noqa: E731
    lines = ["💵 <b>Включить бой — торговлю реальными деньгами?</b>", "",
             f"{ok(fresh)} исполнитель работает",
             f"{ok(allowed)} бой разрешён на сервере (REAL_LIVE=1 в .env)",
             f"{ok(can_sign)} ключ кошелька задан на сервере",
             f"{ok(bal is not None and bal >= need)} на кошельке есть SOL: "
             + (f"{bal:.4f} (нужно от {need:.3f})" if bal is not None else "баланс неизвестен"),
             "",
             f"Тень за {days:.1f} дн.: сделок {int(t['n'])}, в плюс "
             + (f"{t['wins'] / t['n']:.0%}" if t["n"] else "—") + f", итог {money(t['pnl'], sol_usd)} при ставке "
             f"{S.fmt('real_stake_usd', S.get(cfg, 'real_stake_usd'))}.",
             "Ваше правило: бой — только после 2+ недель положительного итога без денег и вашего отдельного решения.",
             "",
             f"Что будет: ставка {S.fmt('real_stake_usd', S.get(cfg, 'real_stake_usd'))} на сделку, одновременно до "
             f"{S.get(cfg, 'real_max_open')} позиций, дневной стоп "
             f"{S.fmt('real_day_stop_usd', S.get(cfg, 'real_day_stop_usd'))}, общий стоп "
             f"{S.fmt('real_total_stop_usd', S.get(cfg, 'real_total_stop_usd'))} (после него бой сам выключится)."]
    if not (allowed and can_sign):
        lines += ["", "Сейчас включить нельзя: не выполнены условия на сервере. Как их выполнить — docs/REAL_BOT.md, "
                      "раздел «Как включить бой»."]
        return "\n".join(lines), [[{"text": "Назад", "callback": "rl:view"}]]
    return "\n".join(lines), [[{"text": "Да, включить бой", "callback": "rl:mode:live!", "style": "danger"},
                               {"text": "Нет", "callback": "rl:view"}]]


# ---------------------------------------------------------------------- настройки
def settings_view(cfg: dict[str, Any], wid: int | None = None, name: str = "") -> tuple[str, Kb]:
    """Общие настройки (wid=None) или настройки одного кошелька (поверх общих). Описания — только в общих:
    у кошелька экран короче, чтобы влезал в одно сообщение."""
    one = wid is not None
    pre, back = (f"rl:ws:{wid}:", f"rl:w:{wid}") if one else ("rl:s:", "rl:view")
    lines = [f"⚙️ <b>Настройки кошелька {esc(name)}</b>" if one else "⚙️ <b>Настройки реального бота</b>",
             "Меняются кнопками «−» и «+», действуют сразу."
             + (" Только для этого кошелька; остальное — общие настройки /real." if one else "")]
    kb: Kb = []
    for key, (label, _default, _step, _lo, _hi, _kind, desc) in S.SETTINGS.items():
        shown = S.fmt(key, S.get(cfg, key))
        lines.append(f"• {label}: <b>{shown}</b>" + (f" — {desc}" if desc and not one else ""))
        kb.append([{"text": "−", "callback": f"{pre}{key}:-"},
                   {"text": f"{label[:22]}: {shown}", "callback": back if one else "rl:set"},
                   {"text": "+", "callback": f"{pre}{key}:+"}])
    lines.append("")
    method_keys = {m[1] for m in S.METHODS.values()} | {"real_live_mydev"}
    for key, (label, _default, desc) in S.TOGGLES.items():
        if key in method_keys:
            continue                                 # методы и их правила — на экране «🎯 Методы»
        on = S.get(cfg, key)
        lines.append(f"• {label}: <b>{'да' if on else 'нет'}</b>" + (f" — {desc}" if desc and not one else ""))
        kb.append([{"text": f"{'✅' if on else '⬜'} {label[:40]}",
                    "callback": f"rl:wt:{wid}:{key}" if one else f"rl:t:{key}"}])
    lines.append(f"\n🎯 Методы торговли ({S.methods_summary(cfg)}) — правила выхода, отбор и ставка у каждого "
                 "метода свои: кнопка «🎯 Методы».")
    kb.append([{"text": "🎯 Методы", "callback": f"rl:wme:{wid if one else 'g'}"}])
    lines.append(f"\nПотолок ставки на сервере: ${S.max_stake_usd():g} (REAL_MAX_STAKE_USD в .env).")
    kb.append([{"text": "← Назад", "callback": back}])
    return "\n".join(lines), kb


# ---------------------------------------------------------------------- сделки
async def trades_text(db: Any, cfg: dict[str, Any], limit: int = 20, wallet: Any = "all") -> str:
    every = wallet == "all"
    zero = await stats_since(db)
    rows = await db.fetch("SELECT p.*, t.label trader_label FROM real_positions p "
                          "LEFT JOIN traders t ON t.address = p.trader WHERE p.closed_at IS NOT NULL "
                          "AND ($2 OR p.wallet_id IS NOT DISTINCT FROM $3::bigint) "
                          "AND ($4::timestamptz IS NULL OR p.closed_at >= $4) ORDER BY p.closed_at DESC LIMIT $1",
                          limit, every, None if every else wallet, zero)
    if not rows:
        return ("📋 Закрытых сделок реального бота пока нет"
                + (f" с {zero.astimezone(tz_of(cfg)):%d.%m %H:%M} (кнопка «🔄 Считать с нуля»)." if zero else "."))
    sol_usd = await sol_price(db)
    tz = tz_of(cfg)
    lines = [f"📋 <b>Последние сделки реального бота</b> ({len(rows)})"]
    for r in rows:
        blocks = (r["extra"] or {}).get("blocks_after")
        lines.append(
            f"{'✅' if (r['pnl_sol'] or 0) > 0 else '❌'} {MODE_ICON.get(r['mode'], '')} "
            f"{r['closed_at'].astimezone(tz):%d.%m %H:%M} ${esc(r['symbol'] or '?')} ({KINDS.get(r['kind'], r['kind'])}) "
            f"{float(r['pnl_pct'] or 0):+.0%} {money(float(r['pnl_sol'] or 0), sol_usd)} · "
            f"{EXIT_NAMES.get(r['exit_reason'], r['exit_reason'])}"
            + (f" · вход +{blocks} бл." if blocks is not None else "")
            + (f" · за {esc(trader_name(r['trader'], r['trader_label']))}" if r["trader"] else ""))
    return "\n".join(lines)


def skip_text(raw: str) -> str:
    """«live|smart|причина» → «бой · smart: причина» — у какого метода пропуск (18.11: «тень: дневной стоп» у сделок
    бумаги A выглядел как пропуск smart, а был у «мой дев» в тени по той же монете)."""
    mode, _, rest = (raw or "").partition("|")
    kind, _, why = rest.partition("|")
    if not why:
        return raw
    return f"{'бой' if mode == 'live' else 'тень'} · {KINDS.get(kind, kind)}: {why}"


def trader_name(addr: str, label: str | None) -> str:
    """Чей сигнал: метка из списка (🔍 / 🧬 — найден разведкой) или начало и конец адреса."""
    short = f"{addr[:4]}…{addr[-4:]}" if len(addr) > 10 else addr
    return f"{label} ({short})" if label and short not in label else label or short


async def trades_csv(db: Any, cfg: dict[str, Any]) -> tuple[str, int]:
    rows = await db.fetch("SELECT * FROM real_positions ORDER BY opened_at")
    tz = tz_of(cfg)
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["режим", "сигнал", "монета", "адрес монеты", "трейдер", "площадка", "открыта", "закрыта",
                "блоков после сигнала", "задержка, с", "MC сигнала, SOL", "MC входа, SOL", "MC выхода, SOL",
                "потрачено, SOL", "получено, SOL", "итог, SOL", "итог, %", "причина выхода", "алерт",
                "подпись покупки", "подпись продажи", "примечание"])
    f = lambda v, n=6: "" if v is None else f"{float(v):.{n}f}".replace(".", ",")  # noqa: E731
    for r in rows:
        extra = r["extra"] or {}
        w.writerow([MODE_SHORT.get(r["mode"], r["mode"]), KINDS.get(r["kind"], r["kind"]), r["symbol"] or "", r["mint"],
                    r["trader"] or "", r["venue"] or "", f"{r['opened_at'].astimezone(tz):%d.%m.%Y %H:%M:%S}",
                    f"{r['closed_at'].astimezone(tz):%d.%m.%Y %H:%M:%S}" if r["closed_at"] else "",
                    "" if extra.get("blocks_after") is None else extra["blocks_after"], f(r["lag_s"], 2),
                    f(r["trigger_mc"], 2), f(r["entry_mc"], 2), f(r["exit_mc"], 2), f(r["cost_sol"]),
                    f(r["proceeds_sol"]), f(r["pnl_sol"]), f(None if r["pnl_pct"] is None else r["pnl_pct"] * 100, 1),
                    EXIT_NAMES.get(r["exit_reason"], r["exit_reason"] or ""), r["alert_id"] or "", r["buy_sig"] or "",
                    r["sell_sig"] or "", extra.get("note") or ""])
    return buf.getvalue(), len(rows)


# ---------------------------------------------------------------------- сравнение с бумагой
DEFAULT_RULE = {"smart": "A", "mydev": "G", "copy": "G", "bigcopy": "G", "swing": "R", "trend": "G"}


def paper_target(kind: str, extra: dict[str, Any]) -> str | None:
    """С каким бумажным портфелем сравнивать сделку бота: тот же вход и то же правило выхода (правило запомнено в
    позиции; у старых позиций — правило по умолчанию)."""
    rule = str(extra.get("rule") or DEFAULT_RULE.get(kind, "A"))
    return S.paper_portfolio(kind, rule, str(extra.get("filter") or "all"))


async def paper_pairs(db: Any, rows: list[Any], days: int) -> list[tuple[Any, str | None, Any]]:
    """(сделка бота, портфель бумаги, сделка бумаги или None). По алерту; копии и кандидаты разведки идут без алерта
    (19.11: у всех копий было «бумага не входила») — их пару ищем по монете, портфелю, кошельку и времени (±2 мин)."""
    paper: dict[tuple[Any, str, str], Any] = {}
    for p in await db.fetch(
            """SELECT alert_id, mint, portfolio, pnl_pct, entry_mc, exit_reason FROM paper_positions
               WHERE alert_id = ANY($1::bigint[]) AND NOT suspect AND closed_at IS NOT NULL
                 AND exit_reason IS DISTINCT FROM 'reset' ORDER BY id""",
            list({r["alert_id"] for r in rows if r["alert_id"] is not None})):
        paper.setdefault((p["alert_id"], p["mint"], p["portfolio"]), p)
    loose: dict[tuple[str, str], list[Any]] = {}
    no_alert = [r for r in rows if r["alert_id"] is None]
    if no_alert:
        for p in await db.fetch(
                """SELECT mint, portfolio, source_wallet, opened_at, pnl_pct, entry_mc, exit_reason FROM paper_positions
                   WHERE mint = ANY($1::text[]) AND NOT suspect AND closed_at IS NOT NULL
                     AND exit_reason IS DISTINCT FROM 'reset' AND opened_at > now() - make_interval(days => $2)
                   ORDER BY id""", list({r["mint"] for r in no_alert}), int(days) + 1):
            loose.setdefault((p["mint"], p["portfolio"]), []).append(p)

    def match(r: Any, port: str) -> Any:
        if r["alert_id"] is not None:
            return paper.get((r["alert_id"], r["mint"], port))
        best = None
        for p in loose.get((r["mint"], port), []):
            if r["trader"] and p["source_wallet"] and p["source_wallet"] != r["trader"]:
                continue
            gap = abs((p["opened_at"] - r["opened_at"]).total_seconds()) if r["opened_at"] and p["opened_at"] else 1e9
            if gap <= 120 and (best is None or gap < best[0]):
                best = (gap, p)
        return best[1] if best else None
    out = []
    for r in rows:
        port = paper_target(r["kind"], dict(r["extra"] or {}))
        out.append((r, port, match(r, port) if port else None))
    return out


async def compare_text(db: Any, cfg: dict[str, Any], days: int = 14, last: int = 12) -> str:
    """Те же сигналы у реального бота и у бумаги: результат, цена входа, задержка — итогом и по каждой сделке."""
    zero = await stats_since(db)
    period = f"с {zero.astimezone(tz_of(cfg)):%d.%m %H:%M}" if zero else f"{days} дн."
    rows = await db.fetch(
        """SELECT r.id, r.mode, r.kind, r.symbol, r.mint, r.alert_id, r.pnl_pct, r.pnl_sol, r.entry_mc, r.closed_at,
                  r.exit_reason, r.extra, (r.extra->>'blocks_after')::int AS blocks, r.opened_at, r.trader
           FROM real_positions r WHERE r.closed_at IS NOT NULL AND r.closed_at > now() - make_interval(days => $1)
             AND ($2::timestamptz IS NULL OR r.opened_at >= $2)
           ORDER BY r.closed_at""", int(days), zero)
    if not rows:
        return f"⚖️ {period.capitalize()}: закрытых сделок реального бота нет — сравнивать нечего."
    pairs = await paper_pairs(db, rows, days)
    sol_usd = await sol_price(db)
    head = f"{'':<17}{'сдел.':>5} {'в плюс':>6} {'средн.':>7} {'итог':>9}"
    table, notes = [head], []
    for mode in ("live", "shadow"):
        for kind in ("smart", "mydev", "bigcopy", "copy", "swing", "trend"):
            rs = [x for x in pairs if x[0]["mode"] == mode and x[0]["kind"] == kind]
            if not rs:
                continue
            n = len(rs)
            wins = sum(1 for r, _p, _q in rs if (r["pnl_sol"] or 0) > 0)
            avg = sum(float(r["pnl_pct"] or 0) for r, _p, _q in rs) / n
            total = sum(float(r["pnl_sol"] or 0) for r, _p, _q in rs)
            name = f"{MODE_SHORT[mode]} {KINDS[kind]}"
            table.append(f"{name:<17}{n:>5} {wins / n:>6.0%} {avg:>+7.1%} {money(total, sol_usd):>9}")
            both = [(r, port, q) for r, port, q in rs if q is not None]
            if both:
                ports = "/".join(sorted({port for _r, port, _q in both}))
                p_avg = sum(float(q["pnl_pct"] or 0) for _r, _p, q in both) / len(both)
                p_wins = sum(1 for _r, _p, q in both if float(q["pnl_pct"] or 0) > 0)
                r_avg = sum(float(r["pnl_pct"] or 0) for r, _p, _q in both) / len(both)
                table.append(f"{' бумага ' + ports + ', те же':<17}{len(both):>5} {p_wins / len(both):>6.0%} "
                             f"{p_avg:>+7.1%} {'':>9}")
                prem = [float(r["entry_mc"]) / float(q["entry_mc"]) - 1 for r, _p, q in both
                        if r["entry_mc"] and q["entry_mc"]]
                notes.append(f"{name}: на тех же {len(both)} сигналах бот {r_avg:+.1%} на сделку, бумага {p_avg:+.1%}"
                             + (f"; бот входил дороже бумаги на {statistics.median(prem):+.1%} (медиана)" if prem
                                else ""))
            blocks = [int(r["blocks"]) for r, _p, _q in rs if r["blocks"] is not None]
            if blocks:
                notes.append(f"{name}: вход через {statistics.median(blocks):.0f} бл. после сигнала (медиана; "
                             f"от {min(blocks)} до {max(blocks)}); бумага считает {ENTRY_BLOCKS}")
    tz = tz_of(cfg)
    lines = []
    for r, port, q in pairs[-last:][::-1]:
        bot = float(r["pnl_pct"] or 0)
        if q is not None:
            prem = (float(r["entry_mc"]) / float(q["entry_mc"]) - 1) if r["entry_mc"] and q["entry_mc"] else None
            vs = (f"бумага {port} {float(q['pnl_pct'] or 0):+.0%}"
                  + (f" · вход дороже на {prem:+.0%}" if prem is not None else ""))
        elif port is None:
            vs = "на бумаге так не торгуют"
        else:
            vs = f"бумага {port} по этому сигналу не входила"
        lines.append(f"{'✅' if bot > 0 else '❌'} {r['closed_at'].astimezone(tz):%H:%M} ${esc(r['symbol'] or '?')} "
                     f"({KINDS.get(r['kind'], r['kind'])}): бот {bot:+.0%} · {vs}")
    first = min(r["closed_at"] for r in rows)
    since = max(datetime.now(timezone.utc) - timedelta(days=days),
                await db.fetchval("SELECT min(opened_at) FROM real_positions") or first)
    if zero is not None:
        since = max(since, zero)
    missed = await db.fetchval(
        """SELECT count(*) FROM paper_positions pp WHERE pp.portfolio IN ('A', 'Y') AND pp.opened_at >= $1
             AND pp.exit_reason IS DISTINCT FROM 'reset' AND NOT pp.suspect
             AND NOT EXISTS (SELECT 1 FROM real_positions r WHERE r.alert_id = pp.alert_id AND r.mint = pp.mint)""",
        since)
    # какие именно сделки бумаги бот не взял и почему (причины пишутся с 18.11 — таблица real_skips)
    missed_rows = await db.fetch(
        """SELECT pp.portfolio, pp.symbol, pp.opened_at, pp.pnl_pct,
                  (SELECT s.mode || '|' || s.kind || '|' || s.why FROM real_skips s
                   WHERE s.mint = pp.mint AND s.at BETWEEN pp.opened_at - interval '2 minutes'
                                                 AND pp.opened_at + interval '2 minutes'
                   ORDER BY (s.mode = 'live') DESC, s.at DESC LIMIT 1) why
           FROM paper_positions pp WHERE pp.portfolio IN ('A', 'Y') AND pp.opened_at >= $1
             AND pp.exit_reason IS DISTINCT FROM 'reset' AND NOT pp.suspect
             AND NOT EXISTS (SELECT 1 FROM real_positions r WHERE r.alert_id = pp.alert_id AND r.mint = pp.mint)
           ORDER BY pp.opened_at DESC LIMIT 8""", since)
    no_pair = sum(1 for _r, port, q in pairs if q is None)
    out = [f"⚖️ <b>Реальный бот против бумаги</b> · {period}",
           "Сравнение с бумажным портфелем, который торгует так же: smart A — с A, «мой дев» — с Y и т. д.",
           "<pre>" + "\n".join(table) + "</pre>"]
    out += [f"• {esc(x)}" for x in notes]
    out.append(f"• Бумага A / Y вошла, а бот — нет: {int(missed or 0)} · у бота есть сделка, у бумаги — нет: {no_pair}")
    if lines:
        out += ["", f"<b>Последние {len(lines)} сделок:</b>"] + lines
    if missed_rows:
        out += ["", f"<b>Бумага вошла, бот — нет (последние {len(missed_rows)}):</b>"]
        for m in missed_rows:
            res = f"{float(m['pnl_pct']):+.0%}" if m["pnl_pct"] is not None else "открыта"
            out.append(f"• {m['opened_at'].astimezone(tz):%d.%m %H:%M} ${esc(m['symbol'] or '?')} "
                       f"(бумага {m['portfolio']} {res}) — "
                       + (f"бот: {esc(skip_text(m['why']))}" if m["why"] else "причина не записана (сигнал до бота не дошёл "
                          "или это было до 18.11)"))
    out += await rules_lines(db, since, sol_usd)
    out.append("Почему бот не входил — «Пропуски» на главном экране /real.")
    return "\n".join(out)


async def rules_lines(db: Any, since: datetime, sol_usd: float) -> list[str]:
    """Пробы правил выхода: на тех же входах бота — что дало бы каждое правило A / B / G / R."""
    rows = await db.fetch(
        """SELECT mode, kind, rule, count(*) n, count(*) FILTER (WHERE pnl_sol > 0) w, avg(pnl_pct) a,
                  COALESCE(sum(pnl_sol), 0) s
           FROM real_rule_trials WHERE closed_at IS NOT NULL AND opened_at >= $1 GROUP BY 1, 2, 3""", since)
    if not rows:
        return []
    by: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        by.setdefault((r["mode"], r["kind"]), {})[r["rule"]] = r
    table = [f"{'':<17}{'сдел.':>5} {'в плюс':>6} {'средн.':>7} {'итог':>9}"]
    for mode in ("live", "shadow"):
        for kind in S.METHODS:
            rules = by.get((mode, kind))
            if not rules:
                continue
            best = max(rules, key=lambda x: float(rules[x]["s"]))
            table.append(f"{MODE_SHORT[mode]} {KINDS.get(kind, kind)}")
            for rule in S.TRIAL_RULES:
                r = rules.get(rule)
                if r is None:
                    continue
                n = int(r["n"])
                table.append(f"{'  ' + rule + (' ★' if rule == best and len(rules) > 1 else ''):<17}{n:>5} "
                             f"{int(r['w']) / n:>6.0%} {float(r['a'] or 0):>+7.1%} {money(float(r['s']), sol_usd):>9}")
    return ["", "🧪 <b>Все правила выхода на тех же входах</b> (пробы: тот же вход, продажа по цене в сети, без "
            "транзакций и денег). ★ — больше всех в сумме.",
            "<pre>" + "\n".join(table) + "</pre>",
            "A — всё на +25%, без стопа · B — 70% на +25%, остаток трейлинг 15% · G — +25%, стоп −8%, нет +10% за "
            "5 мин — выход · R — трейлинг 35% после +30%, стоп −50%, до суток · W — продать раньше трейдера: через его "
            "обычное время удержания − 2 с (только проба, у копий)"]


def zero_back(back: str) -> str:
    """Куда вернуться из подтверждения «Считать с нуля» по «Нет»."""
    if back == "cmp":
        return "rl:cmp"
    if back == "mine":
        return "rl:mine"
    if back[:1] == "m":
        return f"rl:wme:{back[1:]}"
    if back[:1] == "w" and back[1:].isdigit():
        return f"rl:w:{back[1:]}"
    return "rl:view"


# ---------------------------------------------------------------------- нажатия кнопок
async def on_callback(db: Any, reload: Callable[[], Awaitable[dict[str, Any]]], data: str, rpc: Any = None) -> Reply:
    """data — то, что после «rl:» в кнопке. reload() возвращает действующие настройки (после записи — свежие).
    rpc — клиент сети для свежего баланса в карточке кошелька (необязательно)."""
    cfg = await reload()
    action, _, rest = data.partition(":")
    if action.startswith("w"):
        from .wallets_ui import on_wallet_callback
        return await on_wallet_callback(db, cfg, action, rest, rpc)

    async def main(toast: str = "", edit: bool = True) -> Reply:
        text, kb = await status_view(db, await reload())
        return Reply(text, kb, toast, edit)

    async def conf(toast: str = "") -> Reply:
        text, kb = settings_view(await reload())
        return Reply(text, kb, toast, True)

    if action in ("view", "no"):
        return await main("Обновлено" if action == "view" else "")
    if action == "mode":
        if rest in ("off", "shadow"):
            await db.set_override("real_mode", rest)
            return await main(f"Режим: {S.MODES[rest]}")
        if rest == "live":
            if S.get(cfg, "real_mode") == "live":
                return await main("Бой уже выбран")
            text, kb = await live_confirm_view(db, cfg)
            return Reply(text, kb, "", True)
        if rest == "live!":
            st, age = await load_status(db)
            if age is None or age >= STALE_S or not st.get("live_allowed") or not st.get("can_sign"):
                text, kb = await live_confirm_view(db, cfg)
                return Reply(text, kb, "Бой не включён: условия на сервере не выполнены", True)
            await db.set_override("real_armed_at", datetime.now(timezone.utc).isoformat())
            await db.set_override("real_mode", "live")
            return await main("Бой включён")
        return await main()
    if action == "pause":
        await db.set_override("real_paused", "остановлено кнопкой")
        return await main("Новые входы остановлены")
    if action == "resume":
        await db.set_override("real_paused", None)
        await db.job("real", {"cmd": "resume"})
        return await main("Входы разрешены")
    if action == "sellall":
        return Reply("Продать все открытые позиции реального бота по рынку прямо сейчас?",
                     [[{"text": "Да, продать всё", "callback": "rl:sellall!", "style": "danger"},
                       {"text": "Нет", "callback": "rl:no"}]], "", True)
    if action == "sellall!":
        await db.job("real", {"cmd": "sell_all"})
        return await main("Продаю всё — итог придёт в тему «Реальный бот»")
    if action == "sell" and rest.isdigit():
        await db.job("real", {"cmd": "sell", "id": int(rest)})
        return await main("Продаю позицию")
    if action == "trades":
        return Reply(await trades_text(db, cfg), [[{"text": "← Назад", "callback": "rl:view"}]])
    if action == "mine":
        text, kb = await mine_view(db, cfg)
        return Reply(text, kb, "Обновлено", True)
    if action == "cmp":
        return Reply(await compare_text(db, cfg), [[{"text": "📊 Excel: весь отчёт", "callback": "rl:xlsx"},
                                                    {"text": "← Назад", "callback": "rl:view"}],
                                                   [{"text": "🔄 Считать с нуля", "callback": "rl:zero:cmp"}]])
    if action == "zero":
        back = rest if rest in ("cmp", "view", "mine") or rest[:1] in ("m", "w") else "view"
        return Reply("🔄 <b>Считать с нуля?</b>\nС этой минуты считаются все итоги реального бота: тень и бой "
                     "(главный экран, кошельки, сделки), «⚖️ Против бумаги» с пробами правил, итог бумаги на экранах "
                     "методов. Ничего не удаляется, деньги и открытые позиции не трогаются; дневной и общий стоп боя "
                     "считаются как прежде (это защита). Бумажные портфели /paper сбрасываются сами в 00:00 "
                     "и кнопкой «🔄 Сброс портфелей» в /paper.",
                     [[{"text": "Да, с нуля", "callback": f"rl:zero!:{back}"},
                       {"text": "Нет", "callback": zero_back(back)}]], "", True)
    if action == "zero!":
        await db.kv_set(STATS_SINCE_KEY, datetime.now(timezone.utc).isoformat())
        if rest == "mine":
            text, kb = await mine_view(db, cfg)
            return Reply(text, kb, "Считаю с нуля", True)
        if rest[:1] in ("m", "w"):
            from .wallets_ui import methods_view, wallet_view
            tag = rest[1:]
            if rest[0] == "m":
                text, kb = await methods_view(db, cfg, int(tag) if tag.isdigit() else None)
            elif tag.isdigit():
                text, kb = await wallet_view(db, cfg, int(tag), rpc)
            else:
                return await main("Считаю с нуля")
            return Reply(text, kb, "Считаю с нуля", True)
        if rest == "cmp":
            return Reply(await compare_text(db, cfg), [[{"text": "← Назад", "callback": "rl:view"}]], "Считаю с нуля",
                         True)
        return await main("Считаю с нуля")
    if action == "xlsx":
        from ..reports.excel import build_xlsx
        data, caption = await build_xlsx(db, cfg)
        stamp = datetime.now(tz_of(cfg)).strftime("%Y-%m-%d_%H%M")
        return Reply(document=(data, f"report_{stamp}.xlsx", caption))
    if action == "csv":
        text, n = await trades_csv(db, cfg)
        return Reply(document=(text.encode("utf-8-sig"), "real_trades.csv",
                               f"📄 Сделки реального бота: {n}. Открывается в Numbers / Excel."))
    if action == "check":
        await db.job("real", {"cmd": "check"})
        return Reply(toast="Проверяю на живой сети — результат придёт в тему «Реальный бот» через несколько секунд")
    if action == "set":
        return await conf()
    if action == "s":
        key, _, sign = rest.rpartition(":")
        if key in S.SETTINGS and sign in ("+", "-"):
            v = S.step(cfg, key, sign)
            await db.set_override(key, v)
            return await conf(f"{S.SETTINGS[key][0]}: {S.fmt(key, v)}")
        return await conf()
    if action == "t":
        if rest in S.TOGGLES:
            await db.set_override(rest, not S.get(cfg, rest))
        return await conf("Сохранено")
    if action == "sc":
        kind, _, name = rest.partition(":")
        if kind in S.SCHEME_KEYS and name in S.SCHEMES:
            await db.set_override(S.SCHEME_KEYS[kind], name)
        return await conf("Сохранено")
    return await main()
