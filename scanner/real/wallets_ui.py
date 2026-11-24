"""Экран «👛 Кошельки» в /real: список кошельков, карточка кошелька, пополнение, вывод, режим, настройки, удаление.

Работает в сервисе бота: читает базу (адреса, настройки, состояние полос исполнителя) и шлёт команды аналитике
через очередь заданий. Ключей кошельков здесь нет: таблица real_wallets читается без колонки с ключом, а создаёт
кошельки, подписывает выводы и сделки только сервис analytics.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ..alerts.format import esc
from . import settings as S
from .report import (KINDS, MODE_ICON, MODE_SHORT, STALE_S, Kb, Reply, day_start, lane_status, load_desk, money,
                     settings_view, sol_price, totals, trades_text, tz_of, wallet_rows, stats_since)

CREATE_WAIT_S = 8.0
MASTERKEY_CMD = ("cd /opt/tg-bot &amp;&amp; echo &gt;&gt; .env &amp;&amp; docker compose run --rm --no-deps -T analytics "
                 "real-masterkey &gt;&gt; .env &amp;&amp; docker compose up -d --force-recreate analytics")
WITHDRAW_CMD = ("cd /opt/tg-bot &amp;&amp; printf '\\nREAL_WITHDRAW_TO=АДРЕС_ВАШЕГО_PHANTOM\\n' &gt;&gt; .env &amp;&amp; "
                "docker compose up -d --force-recreate analytics")


def short(addr: str) -> str:
    return f"{addr[:4]}…{addr[-4:]}" if addr and len(addr) > 10 else addr




async def live_balance(rpc: Any, pubkey: str) -> float | None:
    """Свежий баланс из сети (SOL) или None — тогда берётся последний известный исполнителю."""
    if rpc is None:
        return None
    try:
        res = await asyncio.wait_for(rpc.call("getBalance", [pubkey, {"commitment": "confirmed"}], retries=0), 4.0)
        return int((res or {}).get("value") or 0) / 1e9
    except Exception:  # noqa: BLE001
        return None


async def wallet_row(db: Any, wid: int) -> Any:
    return await db.fetchrow("SELECT id, name, pubkey, source, settings, secret_enc IS NOT NULL AS stored "
                             "FROM real_wallets WHERE id=$1 AND NOT archived", wid)


async def set_wallet(db: Any, wid: int, patch: dict[str, Any] | None = None, drop: str | None = None) -> None:
    """Изменить настройки кошелька и попросить аналитику перечитать их."""
    if patch:
        await db.execute("UPDATE real_wallets SET settings = settings || $2::jsonb WHERE id=$1", wid, patch)
    if drop:
        await db.execute("UPDATE real_wallets SET settings = settings - $2::text WHERE id=$1", wid, drop)
    await db.job("real", {"cmd": "reload"})


async def wait_lane(db: Any, wid: int, want: str, timeout_s: float = 3.0) -> None:
    """Подождать (до timeout_s), пока исполнитель кошелька перечитает настройки, — чтобы карточка сразу показала
    новый режим, а не прошлый."""
    end = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < end:
        ws, age = await lane_status(db, wid)
        if age is None or age >= STALE_S or ws.get("want") == want:
            return
        await asyncio.sleep(0.3)


# ---------------------------------------------------------------------- список
async def wallets_view(db: Any, cfg: dict[str, Any]) -> tuple[str, Kb]:
    desk, age = await load_desk(db)
    rows = await wallet_rows(db)
    sol_usd = await sol_price(db)
    lines = ["👛 <b>Кошельки реального бота</b>"]
    kb: Kb = []
    if not rows:
        lines.append("Кошельков пока нет. Создайте — бот пришлёт адрес для пополнения.")
    since = day_start(cfg)
    for i, w in enumerate(rows, 1):
        ws, wage = await lane_status(db, int(w["id"]))
        fresh = wage is not None and wage < STALE_S
        mode = ws.get("mode") if fresh else S.get({**cfg, **(w["settings"] or {})}, "real_mode")
        bal = ws.get("balance_sol") if fresh else None
        t = await totals(db, since, wallet=int(w["id"]))
        day = t[mode if mode in ("live", "shadow") else "shadow"]
        lines.append(f"{i}. {MODE_ICON.get(mode, '')} <b>{esc(w['name'])}</b> — "
                     + (f"{bal:.4f} SOL (${bal * sol_usd:.2f})" if bal is not None else "баланс неизвестен")
                     + f" · сегодня {money(day['pnl'] - day.get('fail_fees', 0.0), sol_usd)}"
                     + (f" · позиций {len(ws.get('open') or [])}" if fresh and ws.get("open") else ""))
        kb.append([{"text": f"{MODE_ICON.get(mode, '')} {w['name'][:30]}", "callback": f"rl:w:{w['id']}"}])
    targets = desk.get("withdraw_to") or []
    lines.append("")
    lines.append("Вывод денег — только на ваш адрес из .env сервера: "
                 + (", ".join(f"<code>{short(a)}</code>" for a in targets) if targets else "не задан"))
    if age is None:
        lines.append("⚠️ Исполнитель ещё не запускался — обновите сервер.")
    elif desk and not desk.get("vault"):
        lines.append(f"⚠️ Создание кошельков выключено: на сервере нет ключа шифра. Одна команда:\n<code>{MASTERKEY_CMD}</code>")
    kb.append([{"text": "➕ Создать кошелёк", "callback": "rl:wn"}])
    kb.append([{"text": "← Назад", "callback": "rl:view"}])
    return "\n".join(lines), kb


# ---------------------------------------------------------------------- карточка
async def wallet_view(db: Any, cfg: dict[str, Any], wid: int, rpc: Any = None) -> tuple[str, Kb]:
    w = await wallet_row(db, wid)
    if w is None:
        return "Кошелёк не найден (удалён).", [[{"text": "← Кошельки", "callback": "rl:wl"}]]
    wcfg = {**cfg, **(w["settings"] or {})}
    ws, wage = await lane_status(db, wid)
    fresh = wage is not None and wage < STALE_S
    sol_usd = await sol_price(db)
    bal = await live_balance(rpc, w["pubkey"])
    if bal is None and fresh:
        bal = ws.get("balance_sol")
    want = S.get(wcfg, "real_mode")
    mode = ws.get("mode") if fresh else want
    key = (ws.get("wallet_note") if fresh else None) or (
        "ключ в базе (зашифрован)" if w["stored"] else "ключ в .env" if w["source"] == "env" else "ключа нет")
    lines = [f"👛 <b>{esc(w['name'])}</b>",
             f"Адрес: <code>{w['pubkey']}</code>",
             "Баланс: " + (f"<b>{bal:.4f} SOL</b> (${bal * sol_usd:.2f})" if bal is not None else "неизвестен"),
             f"Режим: {MODE_ICON.get(mode, '')} <b>{S.MODES.get(mode, mode)}</b> · ставка "
             f"{S.fmt('real_stake_usd', S.get(wcfg, 'real_stake_usd'))} · позиций до {S.get(wcfg, 'real_max_open')}",
             f"Методы: {S.methods_summary(wcfg)}",
             f"Стопы: день {S.fmt('real_day_stop_usd', S.get(wcfg, 'real_day_stop_usd'))}, всего "
             f"{S.fmt('real_total_stop_usd', S.get(wcfg, 'real_total_stop_usd'))} · {esc(key)}"]
    changed = (w["settings"] or {}).get("real_mode_at")
    if changed:
        try:
            at = datetime.fromisoformat(str(changed)).astimezone(tz_of(cfg)).strftime("%d.%m %H:%M")
        except Exception:  # noqa: BLE001
            at = str(changed)[:16]
        lines.append(f"Режим выбран {at}: {esc(str((w['settings'] or {}).get('real_mode_by') or ''))}")
    if want == "live" and fresh and mode != "live":
        if ws.get("want") != "live" or not ws.get("why_not_live"):
            # исполнитель ещё не перечитал настройки (нажали секунду назад) — это не отказ
            lines.append("⏳ Бой включается — нажмите 🔄 Обновить через пару секунд.")
        else:
            lines.append(f"⚠️ «Бой» выбран, но не включён: {esc(ws['why_not_live'])}.")
    if not fresh:
        lines.append("⚠️ Исполнитель не отвечает — состояние может быть устаревшим.")
    own_pause = str((w["settings"] or {}).get("real_paused") or "")
    if own_pause or cfg.get("real_paused"):
        lines.append(f"⛔ Новые входы остановлены: {esc(own_pause or str(cfg.get('real_paused')))}")
    today, alltime = await totals(db, day_start(cfg), wallet=wid), await totals(db, wallet=wid)
    zero = await stats_since(db)
    for name, t in (("Сегодня", today), (f"С {zero.astimezone(tz_of(cfg)):%d.%m %H:%M}" if zero else
                                          "Всего", alltime)):
        parts = [f"{MODE_SHORT[m]} {int(t[m]['n'])} сд. {money(t[m]['pnl'] - t[m].get('fail_fees', 0.0), sol_usd)}"
                 for m in ("live", "shadow") if t[m]["n"] or t[m].get("fails")]
        lines.append(f"{name}: " + (" · ".join(parts) if parts else "сделок нет"))
    opened = ws.get("open") or [] if fresh else []
    if opened:
        # 25.11: в бою у кошелька и тень-замер — сразу видно, что на деньги (💵), а что без денег (👻); деньги — первыми
        n_live = sum(1 for o in opened if o.get("mode") == "live")
        lines.append(f"Открыто: 💵 {n_live} на деньги · 👻 {len(opened) - n_live} в тени (без денег)")
        opened = sorted(opened, key=lambda o: o.get("mode") != "live")
        lines += [f"• {MODE_ICON.get(o.get('mode'), '')} ${esc(o.get('symbol') or '?')} "
                  f"({KINDS.get(o.get('kind'), o.get('kind'))}) {float(o.get('net') or 0):+.0%} · "
                  f"{float(o.get('age_s') or 0) / 60:.0f} мин" for o in opened[:6]]
    kb: Kb = [
        [{"text": "📥 Пополнить", "callback": f"rl:wd:{wid}"}, {"text": "📤 Вывести", "callback": f"rl:wo:{wid}"}],
        [{"text": ("● " if want == m else "") + f"{MODE_ICON[m]} {MODE_SHORT[m].capitalize()}"
          + ("…" if m == "live" and want != "live" else ""), "callback": f"rl:wm:{wid}:{m}"}
         for m in ("off", "shadow", "live")],
        [{"text": "🎯 Методы", "callback": f"rl:wme:{wid}"}, {"text": "⚙️ Настройки", "callback": f"rl:wsg:{wid}"},
         {"text": "📋 Сделки", "callback": f"rl:wtr:{wid}"}],
        [{"text": "▶️ Продолжить входы", "callback": f"rl:wr:{wid}"} if own_pause else
         {"text": "⛔ Стоп входов", "callback": f"rl:wp:{wid}"},
         {"text": "💰 Продать всё…", "callback": f"rl:wx:{wid}"}],
        [{"text": "🔄 Обновить", "callback": f"rl:w:{wid}"}, {"text": "🗑 Удалить…", "callback": f"rl:wa:{wid}"}],
        [{"text": "🔄 Считать с нуля", "callback": f"rl:zero:w{wid}"}, {"text": "← Кошельки", "callback": "rl:wl"}],
    ]
    return "\n".join(lines), kb


async def deposit_view(db: Any, cfg: dict[str, Any], wid: int, rpc: Any = None) -> tuple[str, Kb]:
    w = await wallet_row(db, wid)
    if w is None:
        return "Кошелёк не найден (удалён).", [[{"text": "← Кошельки", "callback": "rl:wl"}]]
    wcfg = {**cfg, **(w["settings"] or {})}
    sol_usd = await sol_price(db)
    bal = await live_balance(rpc, w["pubkey"])
    if bal is None:
        ws, wage = await lane_status(db, wid)
        bal = ws.get("balance_sol") if wage is not None and wage < STALE_S else None
    need = S.get(wcfg, "real_stake_usd") * S.get(wcfg, "real_max_open") / sol_usd + 0.03
    lines = [f"📥 <b>Пополнение: {esc(w['name'])}</b>",
             "Отправьте <b>SOL</b> (сеть <b>Solana</b>, не USDC) на адрес:",
             f"<code>{w['pubkey']}</code>",
             "Сейчас на кошельке: " + (f"{bal:.4f} SOL (${bal * sol_usd:.2f})" if bal is not None else "неизвестно"),
             f"Для боя при этих настройках нужно от {need:.3f} SOL (≈${need * sol_usd:.0f}): ставка × позиции + "
             "0.03 SOL на комиссии и залоги счетов."]
    kb: Kb = [[{"text": "📋 Скопировать адрес", "copy": w["pubkey"]}],
              [{"text": "🔍 Solscan", "url": f"https://solscan.io/account/{w['pubkey']}"},
               {"text": "🔄 Проверить баланс", "callback": f"rl:wd:{wid}"}],
              [{"text": "← Кошелёк", "callback": f"rl:w:{wid}"}]]
    return "\n".join(lines), kb


# ---------------------------------------------------------------------- вывод
async def withdraw_view(db: Any, cfg: dict[str, Any], wid: int) -> tuple[str, Kb]:
    w = await wallet_row(db, wid)
    if w is None:
        return "Кошелёк не найден (удалён).", [[{"text": "← Кошельки", "callback": "rl:wl"}]]
    desk, _age = await load_desk(db)
    targets = desk.get("withdraw_to") or []
    back = [{"text": "← Кошелёк", "callback": f"rl:w:{wid}"}]
    if not targets:
        return ("📤 <b>Вывод</b>\nВывод идёт только на ваш адрес, записанный на сервере. Это защита: даже тот, кто "
                "получит доступ к вашему Telegram, не сможет вывести деньги себе.\nАдрес ещё не задан — одна "
                f"команда на сервере (вставьте адрес своего Phantom):\n<code>{WITHDRAW_CMD}</code>"), [back]
    ws, wage = await lane_status(db, wid)
    opened = len(ws.get("open") or []) if wage is not None and wage < STALE_S else 0
    lines = [f"📤 <b>Вывод: {esc(w['name'])}</b>", "Куда — только ваши адреса из .env сервера:"]
    lines += [f"{i + 1}. <code>{a}</code>" for i, a in enumerate(targets)]
    if opened:
        lines.append(f"Открытых позиций: {opened} — «всё» можно вывести после их закрытия; половину — сейчас.")
    kb: Kb = [[{"text": f"📤 Всё → {short(a)}", "callback": f"rl:wo:{wid}:all:{i}"},
               {"text": f"½ → {short(a)}", "callback": f"rl:wo:{wid}:half:{i}"}] for i, a in enumerate(targets)]
    kb.append(back)
    return "\n".join(lines), kb


async def withdraw_confirm(db: Any, wid: int, what: str, i: int, rpc: Any = None) -> tuple[str, Kb]:
    w = await wallet_row(db, wid)
    desk, _age = await load_desk(db)
    targets = desk.get("withdraw_to") or []
    if w is None or not 0 <= i < len(targets) or what not in ("all", "half"):
        return "Вывод недоступен.", [[{"text": "← Кошельки", "callback": "rl:wl"}]]
    bal = await live_balance(rpc, w["pubkey"])
    amount = None if bal is None else (max(0.0, bal - 0.00001) if what == "all" else bal / 2)
    text = (f"📤 Вывести <b>{'всё' if what == 'all' else 'половину'}</b>"
            + (f" (≈{amount:.4f} SOL)" if amount is not None else "")
            + f" с кошелька <b>{esc(w['name'])}</b> на\n<code>{targets[i]}</code>?"
            + ("\nПосле вывода всего кошелёк перейдёт в режим «выкл»." if what == "all" else ""))
    return text, [[{"text": "Да, вывести", "callback": f"rl:wo!:{wid}:{what}:{i}", "style": "danger"},
                   {"text": "Нет", "callback": f"rl:w:{wid}"}]]


# ---------------------------------------------------------------------- бой для кошелька
async def live_confirm(db: Any, cfg: dict[str, Any], wid: int, rpc: Any = None) -> tuple[str, Kb, bool]:
    """(текст, кнопки, можно ли включить)."""
    w = await wallet_row(db, wid)
    if w is None:
        return "Кошелёк не найден (удалён).", [[{"text": "← Кошельки", "callback": "rl:wl"}]], False
    wcfg = {**cfg, **(w["settings"] or {})}
    desk, _ = await load_desk(db)
    ws, wage = await lane_status(db, wid)
    fresh = wage is not None and wage < STALE_S
    sol_usd = await sol_price(db)
    bal = await live_balance(rpc, w["pubkey"])
    if bal is None and fresh:
        bal = ws.get("balance_sol")
    need = S.get(wcfg, "real_min_stake_usd") / sol_usd + S.get(wcfg, "real_reserve_sol")
    allowed = bool(desk.get("live_allowed")) and fresh
    can_sign = bool(ws.get("can_sign")) and fresh
    t = (await totals(db, wallet=wid))["shadow"]
    ok = lambda v: "✅" if v else "❌"  # noqa: E731
    lines = [f"💵 <b>Включить бой для кошелька {esc(w['name'])}?</b>", "",
             f"{ok(fresh)} исполнитель работает",
             f"{ok(allowed)} бой разрешён на сервере (REAL_LIVE=1 в .env)",
             f"{ok(can_sign)} ключ кошелька доступен на сервере",
             f"{ok(bal is not None and bal >= need)} на кошельке есть SOL: "
             + (f"{bal:.4f} (нужно от {need:.3f})" if bal is not None else "баланс неизвестен"), "",
             f"Тень этого кошелька: сделок {int(t['n'])}, в плюс " + (f"{t['wins'] / t['n']:.0%}" if t["n"] else "—")
             + f", итог {money(t['pnl'], sol_usd)}.",
             f"Что будет: ставка {S.fmt('real_stake_usd', S.get(wcfg, 'real_stake_usd'))}, до "
             f"{S.get(wcfg, 'real_max_open')} позиций, дневной стоп "
             f"{S.fmt('real_day_stop_usd', S.get(wcfg, 'real_day_stop_usd'))}, общий стоп "
             f"{S.fmt('real_total_stop_usd', S.get(wcfg, 'real_total_stop_usd'))} (после него бой сам выключится)."]
    money_kinds = S.live_methods(wcfg)
    lines.append("<b>На деньги пойдут:</b> " + (", ".join(
        f"{k} (ставка ${S.method_stake(wcfg, kk) or S.get(wcfg, 'real_stake_usd'):g})"
        for kk, k in ((kk, S.KIND_NAMES.get(kk, kk)) for kk in S.METHODS) if k in money_kinds)
        if money_kinds else "ни один метод (только кошельки «💵 Бой за ним»)") + ". Остальное — только тень.")
    can = allowed and can_sign and bal is not None and bal >= need
    if not can:
        lines += ["", "Сейчас включить нельзя: не выполнены условия выше."]
        return "\n".join(lines), [[{"text": "← Кошелёк", "callback": f"rl:w:{wid}"}]], False
    return "\n".join(lines), [[{"text": "Да, включить бой", "callback": f"rl:wm:{wid}:live!", "style": "danger"},
                               {"text": "Нет", "callback": f"rl:w:{wid}"}]], True


# ---------------------------------------------------------------------- создание
async def create_and_wait(db: Any, cfg: dict[str, Any], name: str, rpc: Any = None) -> Reply:
    """Попросить аналитику создать кошелёк и дождаться его (до нескольких секунд) — сразу показать пополнение."""
    desk, age = await load_desk(db)
    if age is None or age >= STALE_S:
        return Reply("⚠️ Исполнитель не отвечает — кошелёк сейчас не создать. Обновите / перезапустите сервер.",
                     [[{"text": "← Кошельки", "callback": "rl:wl"}]])
    if not desk.get("vault"):
        return Reply("⚠️ Создание кошельков выключено: на сервере нет ключа шифра. Одна команда на сервере:\n"
                     f"<code>{MASTERKEY_CMD}</code>", [[{"text": "← Кошельки", "callback": "rl:wl"}]])
    last = int(await db.fetchval("SELECT COALESCE(max(id), 0) FROM real_wallets") or 0)
    await db.job("real", {"cmd": "create_wallet", "name": name})
    deadline = asyncio.get_running_loop().time() + CREATE_WAIT_S
    while asyncio.get_running_loop().time() < deadline:
        wid = await db.fetchval("SELECT min(id) FROM real_wallets WHERE id > $1 AND NOT archived", last)
        if wid:
            text, kb = await deposit_view(db, cfg, int(wid), rpc)
            return Reply("✅ Кошелёк создан.\n\n" + text, kb)
        await asyncio.sleep(0.5)
    return Reply("Создаю кошелёк… Адрес для пополнения придёт в тему «Реальный бот».",
                 [[{"text": "👛 Кошельки", "callback": "rl:wl"}]])


# ---------------------------------------------------------------------- методы торговли
PAPER_DAYS = 7
ORDER = ("smart", "mydev", "bigcopy", "copy", "swing", "trend", "newdev", "trendj")
ABOUT = {"smart": "покупка smart-кошелька", "mydev": "запуск монеты девом из вашего списка",
         "bigcopy": "трейдер из списка зашёл от 5 SOL", "copy": "покупка кошелька из списка или кандидата разведки",
         "swing": "покупка держателя (кошелёк, который держит монеты часами)",
         "trend": "монета уже растёт (DexScreener), вход после отката; только pump.fun и PumpSwap",
         "newdev": "дев без монет за 7 дней купил на запуске 0.5–2 или от 10 SOL — вход сразу на запуске, пока цена низкая",
         "trendj": "тренд (DexScreener) на монете не с pump.fun — Raydium, Meteora и др.; цена — котировка Jupiter"}


async def paper_stats(db: Any, days: int = PAPER_DAYS) -> dict[str, tuple[int, int, float]]:
    """Бумага по портфелям за days дней: (сделок, в плюс, итог в $ при $10 на сделку). Только честные входы
    (цена блока +2 из блокчейна) и без сделок ⚠."""
    from .report import stats_since
    zero = await stats_since(db)
    rows = await db.fetch(
        """SELECT portfolio, count(*) n, count(*) FILTER (WHERE pnl_pct > 0) w, COALESCE(sum(pnl_pct), 0) s
           FROM paper_positions WHERE closed_at > now() - make_interval(days => $1) AND NOT suspect
             AND ($2::timestamptz IS NULL OR opened_at >= $2)
             AND exit_reason IS DISTINCT FROM 'reset' AND (entry_src LIKE 'блок%' OR portfolio = 'T')
           GROUP BY portfolio""", int(days), zero)
    return {r["portfolio"]: (int(r["n"]), int(r["w"]), float(r["s"]) * 10) for r in rows}


async def trial_stats(db: Any, kind: str, wid: int | None, days: int = PAPER_DAYS) -> dict[str, tuple[int, int, float, float]]:
    """Пробы правил выхода метода (тень, все 4 правила на тех же входах): правило → (сделок, в плюс, итог SOL,
    средний %). Кошелёк — его пробы; общие настройки — все."""
    zero = await stats_since(db)
    rows = await db.fetch(
        """SELECT rule, count(*) n, count(*) FILTER (WHERE pnl_sol > 0) w, COALESCE(sum(pnl_sol), 0) s,
                  COALESCE(avg(pnl_pct), 0) a
           FROM real_rule_trials WHERE kind = $1 AND closed_at IS NOT NULL
             AND opened_at > now() - make_interval(days => $2) AND ($3::timestamptz IS NULL OR opened_at >= $3)
             AND ($4::bigint IS NULL OR wallet_id = $4) GROUP BY rule""", kind, int(days), zero, wid)
    return {r["rule"]: (int(r["n"]), int(r["w"]), float(r["s"]), float(r["a"])) for r in rows}


async def paper_period(db: Any, days: int = PAPER_DAYS) -> str:
    """«за 7 дн.» или «с 20.11 21:05» — после кнопки «Считать с нуля»."""
    from .report import stats_since
    zero = await stats_since(db)
    if zero and zero > datetime.now(timezone.utc) - timedelta(days=days):
        return f"с {zero.astimezone(ZoneInfo('Asia/Tbilisi')):%d.%m %H:%M}"
    return f"за {days} дн."


def paper_of(kind: str, rule: str, smart_filter: str = "all") -> str | None:
    """Какой бумажный портфель торгует так же (тот же вход и то же правило выхода)."""
    return S.paper_portfolio(kind, rule, smart_filter)


def _usd(v: float) -> str:
    """+$35 / −$9.79: от $10 — без центов."""
    return f"{'+' if v >= 0 else '−'}${abs(v):.0f}" if abs(v) >= 10 else f"{'+' if v >= 0 else '−'}${abs(v):.2f}"


def _stat(st: dict[str, tuple[int, int, float]], port: str | None) -> str:
    if port is None:
        return "на бумаге так не торговали — данных нет"
    n, w, usd = st.get(port, (0, 0, 0.0))
    if not n:
        return f"бумага {port}: сделок нет"
    return f"бумага {port}: {n} сд., в плюс {w / n:.0%}, {'+' if usd >= 0 else '−'}${abs(usd):.0f}"


async def _scope(db: Any, cfg: dict[str, Any], tag: str) -> tuple[dict[str, Any], str, str] | None:
    """(действующие настройки, заголовок, «назад») для кошелька или общих настроек (tag = «g»)."""
    if tag == "g":
        return cfg, "общие (без кошелька и по умолчанию)", "rl:view"
    w = await wallet_row(db, int(tag)) if tag.isdigit() else None
    if w is None:
        return None
    return {**cfg, **(w["settings"] or {})}, esc(w["name"]), f"rl:w:{tag}"


async def methods_view(db: Any, cfg: dict[str, Any], wid: int | None) -> tuple[str, Kb]:
    """Методы кошелька (wid) или общие (None): что включено, вход, правило выхода, ставка и итог бумаги."""
    tag = "g" if wid is None else str(wid)
    sc = await _scope(db, cfg, tag)
    if sc is None:
        return "Кошелёк не найден (удалён).", [[{"text": "← Кошельки", "callback": "rl:wl"}]]
    wcfg, title, back = sc
    st = await paper_stats(db)
    wallet_stake = S.get(wcfg, "real_stake_usd")
    lines = [f"🎯 <b>Методы торговли: {title}</b>",
             "Метод = <b>когда покупать</b> (чей сигнал) + <b>когда продавать</b> (правило A / B / G / R) + "
             "<b>на сколько</b> (ставка). Включайте сколько угодно; на один сигнал — одна покупка.",
             f"Итог бумаги — {await paper_period(db)} при $10 на сделку (честные входы, без ⚠). Это сумма за "
             "несколько дней: в /paper — только сегодня (портфели сбрасываются в 00:00).", ""]
    kb: Kb = []
    for i, kind in enumerate(ORDER, 1):
        name, on_key, _port = S.METHODS[kind]
        on = S.get(wcfg, on_key)
        rule = S.get(wcfg, S.SCHEME_KEYS[kind])
        filt = S.get(wcfg, "real_smart_filter") if kind == "smart" else "all"
        stake = S.method_stake(wcfg, kind)
        lines.append(f"<b>{i}. {name}</b> — {'✅ ' + S.method_name(wcfg, kind) if on else '⬜ выкл'}")
        if on:
            lines.append(f"   ставка {'$%g' % stake if stake else 'как у кошелька ($%g)' % wallet_stake} · "
                         f"{_stat(st, paper_of(kind, rule, filt))}")
        kb.append([{"text": f"{'✅' if on else '⬜'} {name}", "callback": f"rl:wmx:{tag}:{kind}:{'off' if on else 'on'}"},
                   {"text": "⚙️ настроить", "callback": f"rl:wmk:{tag}:{kind}"}])
    lines += ["", "Бумага — не реальные сделки: реальный результат покажет тень и «⚖️ Против бумаги».",
              "Правила выхода A / B / G / R тень проверяет на каждом входе сразу (пробы) — таблица внизу «⚖️ Против "
              "бумаги». «✅ Включить все методы» — все сигналы (в тени без денег; если кошелёк в бою — спросит).",
              "«📋 Как бумага A»: только smart, все сигналы, выход A, ставка $10, до 3 позиций, дневной стоп $30, "
              "общий стоп $50 — как бумажный портфель A, давший +$710 за 14–17.11."]
    kb.append([{"text": "✅ Включить все методы", "callback": f"rl:wmall:{tag}:on"},
               {"text": "⬜ Выключить все", "callback": f"rl:wmall:{tag}:off"}])
    # 24.11 (владелец: «пока всё пускай работает»): выключенные методы — в тень, без денег; 25.11 — и у кошелька
    # в бою: на деньги — только то, что видно в строке «💵 На деньги»
    meas = bool(S.get(wcfg, "real_shadow_all"))
    money_kinds = S.live_methods(wcfg)
    lines.append("💵 На деньги (когда кошелёк в бою): " + (", ".join(money_kinds) if money_kinds else "ничего"))
    lines.append("👻 Мерить всё в тени: " + ("✅ остальные методы" + (" и горячие кошельки доски" if tag == "g" else "")
                                             + " тоже идут в тень (без денег; копии своего списка — только бумага H)"
                                             if meas else "⬜ выкл"))
    kb.append([{"text": f"{'✅' if meas else '⬜'} 👻 Мерить всё в тени (без денег)",
                "callback": f"rl:wmm:{tag}:{'off' if meas else 'on'}"}])
    kb.append([{"text": "🧹 Сбросить всё — выбрать заново", "callback": f"rl:wmz:{tag}"}])
    kb.append([{"text": "📋 Как бумага A ($10)", "callback": f"rl:wmp:{tag}"}])
    kb.append([{"text": "🔄 Считать с нуля", "callback": f"rl:zero:m{tag}"}])
    kb.append([{"text": "← Назад", "callback": back}])
    return "\n".join(lines), kb


PAPER_A_PRESET = {"real_smart": True, "real_scheme": "A", "real_smart_filter": "all", "real_mydev": False,
                  "real_live_mydev": False, "real_live_trendj": False, "real_copy": False, "real_bigcopy": False, "real_swing": False, "real_trend": False,
                  "real_stake_usd": 10, "real_max_open": 3, "real_day_stop_usd": 30, "real_total_stop_usd": 50}


async def method_view(db: Any, cfg: dict[str, Any], tag: str, kind: str) -> tuple[str, Kb]:
    """Один метод: включён ли, вход (у smart — все сигналы или как M), правило выхода, своя ставка."""
    sc = await _scope(db, cfg, tag)
    if sc is None or kind not in S.METHODS:
        return "Метод не найден.", [[{"text": "← Кошельки", "callback": "rl:wl"}]]
    wcfg, title, _back = sc
    name, on_key, _port = S.METHODS[kind]
    on = S.get(wcfg, on_key)
    rule = S.get(wcfg, S.SCHEME_KEYS[kind])
    filt = S.get(wcfg, "real_smart_filter") if kind == "smart" else "all"
    stake = S.method_stake(wcfg, kind)
    st = await paper_stats(db)
    trials = await trial_stats(db, kind, None if tag == "g" else int(tag))
    sol_usd = await sol_price(db)
    # 22.11: экран по разделам — каждый с новой строки, тень таблицей, бумага отдельно (владелец: «всё наслоено»)
    rules = [x for x in S.TRIAL_RULES if x != "W" or kind in ("smart", "copy", "bigcopy", "swing")]
    if kind == "mydev":
        rules.append("D")
    stake_txt = (f"${stake:g}" if stake else f"как у кошелька ({S.fmt('real_stake_usd', S.get(wcfg, 'real_stake_usd'))})")
    state = f"{'✅ включён' if on else '⬜ выключен'} · ставка {stake_txt} · потолок ${S.max_stake_usd():g}"
    if kind == "mydev":
        state += " · в бою: " + ("да" if S.get(wcfg, "real_live_mydev") else "нет, только тень")
    if kind == "trendj":
        state += " · в бою: " + ("да" if S.get(wcfg, "real_live_trendj") else "нет, только тень")
    lines = [f"🎯 <b>{name}</b> — {title}", state, "",
             "<b>Покупает</b>", ABOUT[kind] + (f" — {S.SMART_FILTERS[filt]}" if kind == "smart" else ""), "",
             f"<b>Продаёт</b> по правилу ● <b>{rule}</b> (кнопки «Выход» внизу)", "",
             f"<b>Тень</b> {await paper_period(db)} — тот же вход, разные выходы"]
    table = [f"{'выход':<6}{'сд':>4}{'плюс':>6}{'итог':>9}"]
    for x in rules:
        t = trials.get(x)
        mark = f"{x} ●" if x == rule else x
        table.append(f"{mark:<6}" + (f"{t[0]:>4}{t[1] / t[0]:>6.0%}{_usd(t[2] * sol_usd):>9}" if t else f"{'—':>4}{'':>6}{'нет':>9}"))
    lines.append("<pre>" + "\n".join(table) + "</pre>")
    lines += ["", "<b>Бумага</b> " + await paper_period(db)]
    for x in S.SCHEMES:
        port = paper_of(kind, x, filt)
        if port is None:
            continue
        n, w, usd = st.get(port, (0, 0, 0.0))
        lines.append(f"{x} → портфель {port}: " + (f"{n} сд · плюс {w / n:.0%} · {_usd(usd)}" if n else "сделок нет"))
    if all(paper_of(kind, x, filt) is None for x in S.SCHEMES):
        lines.append("так на бумаге не торгуют — данных нет")
    if kind == "smart":
        n, w, usd = st.get(paper_of("smart", "A", "m") or "", (0, 0, 0.0))
        lines += ["", "<b>Какие сигналы брать</b>",
                  f"{'●' if filt == 'all' else '○'} все smart-сигналы",
                  f"{'●' if filt == 'm' else '○'} только отобранные, как у M"
                  + (f" · бумага M: {n} сд · плюс {w / n:.0%} · {_usd(usd)}" if n else "")]
    lines += ["", "<b>Правила</b>"] + [f"{x} — {S.SCHEME_TEXT[x]}" + (" (только проба)" if x == "W" else "")
                                       for x in rules]
    exits = [{"text": ("● " if rule == x else "") + f"Выход {x}", "callback": f"rl:wmr:{tag}:{kind}:{x}"}
             for x in S.SCHEMES if x != "D" or kind == "mydev"]
    kb: Kb = [[{"text": "✅ Включён — выключить" if on else "⬜ Выключен — включить",
                "callback": f"rl:wmo:{tag}:{kind}:{'off' if on else 'on'}"}],
              *[exits[i:i + 5] for i in range(0, len(exits), 5)]]
    if kind == "smart":
        kb.append([{"text": ("● " if filt == "all" else "") + "Все сигналы", "callback": f"rl:wmf:{tag}:all"},
                   {"text": ("● " if filt == "m" else "") + "Только отобранные (M)", "callback": f"rl:wmf:{tag}:m"}])
        kb.append([{"text": "⚡ Сделать как M (отбор M + выход A)", "callback": f"rl:wmf:{tag}:M"}])
    if kind == "mydev":
        dlive = S.get(wcfg, "real_live_mydev")
        kb.append([{"text": f"{'✅' if dlive else '⬜'} Торговать и в бою",
                    "callback": f"rl:wmo:{tag}:devlive:{0 if dlive else 1}"}])
    if kind == "trendj":
        lines += await trendj_live_lines(db, wcfg, None if tag == "g" else int(tag))
        jlive = S.get(wcfg, "real_live_trendj")
        kb.append([{"text": "✅ 💵 Бой включён — выключить" if jlive else "⬜ 💵 Торговать и в бою (Jupiter)",
                    "callback": f"rl:wmo:{tag}:jlive:{0 if jlive else 1}"}])
        kb.append([{"text": f"🧪 Пробная сделка ${JUP_TEST_USD:g} (SOL→USDC→SOL)", "callback": f"rl:wjt:{tag}"}])
    kb.append([{"text": "− $5", "callback": f"rl:wms:{tag}:{kind}:-"},
               {"text": f"Ставка: {'$%g' % stake if stake else 'как у кошелька'}", "callback": f"rl:wmk:{tag}:{kind}"},
               {"text": "+ $5", "callback": f"rl:wms:{tag}:{kind}:+"}])
    if stake:
        kb.append([{"text": "↺ Ставка как у кошелька", "callback": f"rl:wms:{tag}:{kind}:0"}])
    kb.append([{"text": "← Методы", "callback": f"rl:wme:{tag}"}])
    return "\n".join(lines), kb


JUP_TEST_USD = 1


async def trendj_live_lines(db: Any, wcfg: dict[str, Any], wid: int | None) -> list[str]:
    """Экран «тренды на других биржах»: бой (включён ли и что мешает) и сухой прогон настоящей транзакции за сутки."""
    out = ["", "<b>Бой через Jupiter</b>"]
    if S.get(wcfg, "real_live_trendj"):
        out.append("✅ включён твоей кнопкой — " + ("работает, когда кошелёк в режиме «бой»"
                                                   if S.get(wcfg, "real_mode") == "live" else
                                                   "но кошелёк сейчас в тени: сделки идут в тень"))
        if S.get(wcfg, "real_only_follow"):
            out.append("⚠️ включено «🎯 На деньги — только выбранные мной кошельки» — пока оно включено, тренды в бой "
                       "не идут")
    else:
        out.append("⬜ выключен — только тень. Включается только этой кнопкой, с подтверждением.")
    r = await db.fetchrow(
        """SELECT count(*) n, count(*) FILTER (WHERE status = 'simulated') ok,
                  (array_agg(error ORDER BY id DESC) FILTER (WHERE status = 'failed'))[1] last_err
           FROM real_orders WHERE note LIKE 'сухой прогон%' AND created_at > now() - interval '24 hours'
             AND wallet_id IS NOT DISTINCT FROM $1""", wid)
    if r and r["n"]:
        out.append(f"🔍 Сухой прогон за сутки: {r['n']}, прошли проверку {r['ok']}"
                   + (f" · последняя ошибка: {esc(str(r['last_err'])[:90])}" if r["last_err"] else ""))
    elif S.get(wcfg, "real_jup_dry_run"):
        out.append("🔍 Сухой прогон за сутки не шёл: нужен кошелёк бота с ~0.01+ SOL (транзакция собирается и "
                   "проверяется в сети, но не отправляется)")
    return out


def method_patch(cfg: dict[str, Any], method: str, value: str) -> tuple[dict[str, Any], list[str]] | None:
    """Нажатие на экранах методов → (что записать, какие ключи убрать)."""
    if method in S.METHODS:
        on_key = S.METHODS[method][1]
        if value in ("on", "1"):
            return {on_key: True}, []
        if value in ("off", "0"):
            return {on_key: False}, []
        if value in S.SCHEMES:                    # старые кнопки: «включить с правилом»
            return {on_key: True, S.SCHEME_KEYS[method]: value}, []
        return None
    if method == "devlive" and value in ("0", "1"):
        return {"real_live_mydev": value == "1"}, []
    if method == "jlive" and value in ("0", "1!"):           # «1» без «!» — сначала подтверждение (methods_callback)
        return {"real_live_trendj": value == "1!"}, []
    if method == "jlive" and value == "only!":
        # в бой — только тренды других бирж; остальные методы выключены и меряются в тени
        return {**{m[1]: False for m in S.METHODS.values()}, "real_trendj": True, "real_shadow_all": True,
                "real_live_trendj": True}, []
    return None


async def apply_patch(db: Any, tag: str, patch: dict[str, Any], drop: list[str]) -> None:
    """Записать настройки: общие (tag «g») или кошелька."""
    if tag == "g":
        for k, v in patch.items():
            await db.set_override(k, v)
        for k in drop:
            await db.set_override(k, None)
        return
    wid = int(tag)
    if patch:
        await db.execute("UPDATE real_wallets SET settings = settings || $2::jsonb WHERE id=$1", wid, patch)
    for k in drop:
        await db.execute("UPDATE real_wallets SET settings = settings - $2::text WHERE id=$1", wid, k)
    await db.job("real", {"cmd": "reload"})


async def methods_callback(db: Any, cfg: dict[str, Any], action: str, parts: list[str]) -> Reply | None:
    """Кнопки экранов методов: wme (список), wmk (метод), wmx (вкл/выкл из списка), wmo (вкл/выкл на экране метода),
    wmr (правило), wmf (отбор smart), wms (ставка метода). parts[0] — номер кошелька или «g» (общие настройки)."""
    if not parts or not (parts[0] == "g" or parts[0].isdigit()):
        return None
    tag = parts[0]
    sc = await _scope(db, cfg, tag)
    if sc is None:
        text, kb = await wallets_view(db, cfg)
        return Reply(text, kb, "Кошелёк не найден", True)
    wcfg = sc[0]
    patch: dict[str, Any] = {}
    drop: list[str] = []
    kind = parts[1] if len(parts) > 1 else ""
    if action == "wmo" and len(parts) == 3 and kind == "jlive" and parts[2] == "1":
        sl = S.method_stake(wcfg, "trendj") or S.get(wcfg, "real_stake_usd")
        others = S.live_methods(wcfg)
        warn = (f"\n\n⚠️ Сейчас включены и другие методы — когда кошелёк в бою, на деньги пойдут и они: "
                f"{', '.join(others)}. Чтобы на деньги шли только тренды других бирж — нижняя кнопка (остальные "
                "методы выключатся и будут мериться в тени, без денег)." if others else "")
        kb = [[{"text": "Да, включить бой", "callback": f"rl:wmo:{tag}:jlive:1!", "style": "danger"},
               {"text": "Нет", "callback": f"rl:wmk:{tag}:trendj"}]]
        if others:
            kb.insert(1, [{"text": "💵 В бой только тренды др. бирж, остальное — в тень",
                           "callback": f"rl:wmo:{tag}:jlive:only!", "style": "danger"}])
        return Reply("💵 <b>Включить бой для «трендов на других биржах»?</b>\n"
                     f"Бот будет покупать и продавать через Jupiter на настоящие деньги: ставка ${sl:g}, правило "
                     f"{S.get(wcfg, S.SCHEME_KEYS['trendj'])}, дневной стоп ${S.get(wcfg, 'real_day_stop_usd'):g}, "
                     f"до {S.get(wcfg, 'real_max_open')} позиций сразу. Работает, только когда сам кошелёк в режиме "
                     "«бой». Каждая сделка перед отправкой проверяется симуляцией. Выключить — той же кнопкой." + warn,
                     kb, "", True)
    if action == "wjt":
        if not (len(parts) == 2 and parts[1] == "!"):
            return Reply(f"🧪 <b>Пробная сделка ${JUP_TEST_USD:g}?</b>\nБот купит USDC примерно на ${JUP_TEST_USD:g} "
                         "через Jupiter и сразу продаст обратно в SOL — на настоящие деньги. Теряются только комиссии "
                         "(центы). Так проверяется весь путь боя. Итог со ссылками на транзакции придёт в тему "
                         "«Реальный бот».",
                         [[{"text": "Да, сделать пробную", "callback": f"rl:wjt:{tag}:!", "style": "danger"},
                           {"text": "Нет", "callback": f"rl:wmk:{tag}:trendj"}]], "", True)
        await db.job("real", {"cmd": "jup_test", "wallet": None if tag == "g" else int(tag)})
        text, kb = await method_view(db, cfg, tag, "trendj")
        return Reply(text, kb, "Пробная сделка запущена — итог придёт в тему «Реальный бот»", True)
    if action in ("wmx", "wmo") and len(parts) == 3:
        res = method_patch(wcfg, kind, parts[2])
        if res is None:
            return Reply(None, None, "Не понял кнопку")
        patch, drop = res
        kind = kind if kind in S.METHODS else ("trendj" if kind == "jlive" else "mydev")
    elif action == "wmr" and len(parts) == 3 and kind in S.METHODS and parts[2] in S.SCHEMES:
        patch = {S.SCHEME_KEYS[kind]: parts[2]}
    elif action == "wmf" and len(parts) == 2 and parts[1] in ("all", "m", "M"):
        kind = "smart"
        patch = ({"real_smart": True, "real_smart_filter": "m", "real_scheme": "A"} if parts[1] == "M"
                 else {"real_smart_filter": parts[1]})
    elif action == "wms" and len(parts) == 3 and kind in S.METHODS and parts[2] in ("+", "-", "0"):
        key = S.stake_key(kind)
        if parts[2] == "0":
            drop = [key]
        else:
            cur = S.method_stake(wcfg, kind) or S.get(wcfg, "real_stake_usd")
            v = cur + 5 if parts[2] == "+" else cur - 5
            patch = {key: min(S.max_stake_usd(), max(1.0, round(v)))}
    elif action == "wmm" and len(parts) == 2 and parts[1] in ("on", "off"):
        on = parts[1] == "on"
        patch = {"real_shadow_all": on}
        if tag == "g":
            patch["real_autofollow"] = on           # и тень за горячими кошельками доски — тоже без денег (общее)
    elif action == "wmp":
        patch = dict(PAPER_A_PRESET)
        drop = [S.stake_key(k) for k in S.METHODS]
    elif action == "wmall" and len(parts) == 2 and parts[1] in ("on", "on!", "off"):
        live = S.get(wcfg, "real_mode") == "live"
        if tag == "g" and not live:
            # общие методы действуют у всех кошельков без своей настройки — в том числе у тех, что в бою
            live = any(S.get({**cfg, **(w["settings"] or {})}, "real_mode") == "live"
                       and not any(S.METHODS[k][1] in (w["settings"] or {}) for k in S.METHODS)
                       for w in await wallet_rows(db))
        if parts[1] == "on" and live:
            return Reply("💵 <b>Кошелёк в режиме «бой».</b> Включить все методы — значит торговать на реальные деньги "
                         "по всем сигналам: smart, копии, крупные копии, смарт-ракеты, тренды (у «мой дев» в бою — "
                         "отдельный переключатель, он не меняется). Ставка, лимит позиций и стопы — как настроены.",
                         [[{"text": "Да, включить все", "callback": f"rl:wmall:{tag}:on!", "style": "danger"},
                           {"text": "Нет", "callback": f"rl:wme:{tag}"}]], "", True)
        on = parts[1] != "off"
        patch = {S.METHODS[k][1]: on for k in S.METHODS}
    elif action == "wmz":
        if not (len(parts) == 2 and parts[1] == "!"):
            return Reply("🧹 <b>Сбросить всё?</b>\nВыключаются все методы, замер всех методов в тени, тень, которую бот "
                         "включал за кошельками сам, и авто-тень. Твои «за ним» (выбранные вручную) остаются. Дальше "
                         "включаешь только то, чем хочешь торговать — и ничего лишнего не подключится.",
                         [[{"text": "Да, сбросить", "callback": f"rl:wmz:{tag}:!", "style": "danger"},
                           {"text": "Нет", "callback": f"rl:wme:{tag}"}]], "", True)
        patch = {S.METHODS[k][1]: False for k in S.METHODS}
        patch["real_live_trendj"] = False
        if tag == "g":
            patch.update(real_shadow_all=False, real_autofollow=False)
            follow = dict(await db.kv_get("real:follow") or {})
            auto = set(await db.kv_get("real:follow_auto") or [])
            await db.kv_set("real:follow", {a: m for a, m in follow.items() if a not in auto})
            await db.kv_set("real:follow_auto", [])
        action = "wme"
    elif action not in ("wme", "wmk"):
        return Reply(None, None, "Не понял кнопку")
    if patch or drop:
        await apply_patch(db, tag, patch, drop)
        if tag == "g":
            cfg = {**cfg, **patch}
            for k in drop:
                cfg.pop(k, None)
    if action in ("wme", "wmx", "wmp", "wmall", "wmm"):   # список методов (с него нажимают «вкл/выкл» и «как бумага»)
        text, kb = await methods_view(db, cfg, None if tag == "g" else int(tag))
    else:                                     # экран одного метода
        text, kb = await method_view(db, cfg, tag, kind if kind in S.METHODS else "smart")
    return Reply(text, kb, "Сохранено" if (patch or drop) else "", True)


# ---------------------------------------------------------------------- нажатия
async def on_wallet_callback(db: Any, cfg: dict[str, Any], action: str, rest: str, rpc: Any = None) -> Reply:
    parts = rest.split(":") if rest else []
    wid = int(parts[0]) if parts and parts[0].isdigit() else None
    if action in ("wme", "wmk", "wmx", "wmo", "wmr", "wmf", "wms", "wmp", "wmall", "wmz", "wmm", "wjt"):
        r = await methods_callback(db, cfg, action, parts)
        if r is not None:
            return r

    async def card(toast: str = "") -> Reply:
        text, kb = await wallet_view(db, cfg, wid, rpc) if wid else await wallets_view(db, cfg)
        return Reply(text, kb, toast, True)

    if action == "wl":
        text, kb = await wallets_view(db, cfg)
        return Reply(text, kb, "", True)
    if action == "wn":
        return Reply("👛 Как назвать новый кошелёк? Напишите название ответом (например «Основной» или «Тест $30»).",
                     [[{"text": "Без названия", "callback": "rl:wn!"}], [{"text": "✖ Отмена", "callback": "rl:wl"}]],
                     ask="wallet_name")
    if action == "wn!":
        return await create_and_wait(db, cfg, "", rpc)
    if wid is None:
        text, kb = await wallets_view(db, cfg)
        return Reply(text, kb, "", True)
    if action == "w":
        return await card()
    if action == "wd":
        text, kb = await deposit_view(db, cfg, wid, rpc)
        return Reply(text, kb, "", True)
    if action == "wo":
        if len(parts) == 3 and parts[2].isdigit():
            text, kb = await withdraw_confirm(db, wid, parts[1], int(parts[2]), rpc)
        else:
            text, kb = await withdraw_view(db, cfg, wid)
        return Reply(text, kb, "", True)
    if action == "wo!" and len(parts) == 3 and parts[2].isdigit() and parts[1] in ("all", "half"):
        await db.job("real", {"cmd": "withdraw", "wallet": wid, "what": parts[1], "to": int(parts[2])})
        return await card("Вывожу — результат придёт в тему «Реальный бот»")
    if action == "wm" and len(parts) == 2:
        m = parts[1]
        if m in ("off", "shadow"):
            await set_wallet(db, wid, {"real_mode": m, "real_mode_at": datetime.now(timezone.utc).isoformat(),
                                       "real_mode_by": f"кнопка «{'Выкл' if m == 'off' else 'Тень'}» в Telegram"})
            return await card(f"Режим: {S.MODES[m]}")
        if m == "live":
            text, kb, _can = await live_confirm(db, cfg, wid, rpc)
            return Reply(text, kb, "", True)
        if m == "live!":
            text, kb, can = await live_confirm(db, cfg, wid, rpc)
            if not can:
                return Reply(text, kb, "Бой не включён: условия не выполнены", True)
            now = datetime.now(timezone.utc).isoformat()
            await set_wallet(db, wid, {"real_mode": "live", "real_armed_at": now, "real_mode_at": now,
                                       "real_mode_by": "кнопка «Да, включить бой» в Telegram"})
            await wait_lane(db, wid, "live")
            return await card("Бой включён")
    if action == "wsg":
        w = await wallet_row(db, wid)
        if w is None:
            return await card()
        text, kb = settings_view({**cfg, **(w["settings"] or {})}, wid, w["name"])
        return Reply(text, kb, "", True)
    if action in ("ws", "wt", "wsc"):
        w = await wallet_row(db, wid)
        if w is None:
            return await card()
        wcfg = {**cfg, **(w["settings"] or {})}
        toast = "Сохранено"
        if action == "ws" and len(parts) == 3 and parts[1] in S.SETTINGS and parts[2] in ("+", "-"):
            v = S.step(wcfg, parts[1], parts[2])
            await set_wallet(db, wid, {parts[1]: v})
            wcfg[parts[1]] = v
            toast = f"{S.SETTINGS[parts[1]][0]}: {S.fmt(parts[1], v)}"
        elif action == "wt" and len(parts) == 2 and parts[1] in S.TOGGLES and parts[1] not in S.LIVE_TOGGLES:
            wcfg[parts[1]] = not S.get(wcfg, parts[1])
            await set_wallet(db, wid, {parts[1]: wcfg[parts[1]]})
        elif action == "wsc" and len(parts) == 3 and parts[1] in S.SCHEME_KEYS and parts[2] in S.SCHEMES:
            wcfg[S.SCHEME_KEYS[parts[1]]] = parts[2]
            await set_wallet(db, wid, {S.SCHEME_KEYS[parts[1]]: parts[2]})
        text, kb = settings_view(wcfg, wid, w["name"])
        return Reply(text, kb, toast, True)
    if action == "wp":
        await set_wallet(db, wid, {"real_paused": "остановлено кнопкой"})
        return await card("Новые входы этого кошелька остановлены")
    if action == "wr":
        await set_wallet(db, wid, drop="real_paused")
        await db.job("real", {"cmd": "resume", "wallet": wid})
        return await card("Входы разрешены")
    if action == "wx":
        return Reply("Продать все открытые позиции этого кошелька по рынку прямо сейчас?",
                     [[{"text": "Да, продать всё", "callback": f"rl:wx!:{wid}", "style": "danger"},
                       {"text": "Нет", "callback": f"rl:w:{wid}"}]], "", True)
    if action == "wx!":
        await db.job("real", {"cmd": "sell_all", "wallet": wid})
        return await card("Продаю всё — итог придёт в тему «Реальный бот»")
    if action == "wtr":
        return Reply(await trades_text(db, cfg, wallet=wid), [[{"text": "← Кошелёк", "callback": f"rl:w:{wid}"}]])
    if action == "wa":
        return Reply("🗑 Убрать кошелёк из списка? Можно, только если на нём меньше 0.001 SOL и нет открытых позиций. "
                     "Ключ останется в базе зашифрованным — деньги, пришедшие позже, не пропадут.",
                     [[{"text": "Да, убрать", "callback": f"rl:wa!:{wid}", "style": "danger"},
                       {"text": "Нет", "callback": f"rl:w:{wid}"}]], "", True)
    if action == "wa!":
        await db.job("real", {"cmd": "archive", "wallet": wid})
        text, kb = await wallets_view(db, cfg)
        return Reply(text, kb, "Удаляю — подтверждение придёт в тему «Реальный бот»", True)
    return await card()
