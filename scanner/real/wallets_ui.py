"""Экран «👛 Кошельки» в /real: список кошельков, карточка кошелька, пополнение, вывод, режим, настройки, удаление.

Работает в сервисе бота: читает базу (адреса, настройки, состояние полос исполнителя) и шлёт команды аналитике
через очередь заданий. Ключей кошельков здесь нет: таблица real_wallets читается без колонки с ключом, а создаёт
кошельки, подписывает выводы и сделки только сервис analytics.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from ..alerts.format import esc
from . import settings as S
from .report import (KINDS, MODE_ICON, MODE_SHORT, STALE_S, Kb, Reply, day_start, lane_status, load_desk, money,
                     settings_view, totals, trades_text, wallet_rows)

CREATE_WAIT_S = 8.0
MASTERKEY_CMD = ("cd /opt/tg-bot &amp;&amp; echo &gt;&gt; .env &amp;&amp; docker compose run --rm --no-deps -T analytics "
                 "real-masterkey &gt;&gt; .env &amp;&amp; docker compose up -d --force-recreate analytics")
WITHDRAW_CMD = ("cd /opt/tg-bot &amp;&amp; printf '\\nREAL_WITHDRAW_TO=АДРЕС_ВАШЕГО_PHANTOM\\n' &gt;&gt; .env &amp;&amp; "
                "docker compose up -d --force-recreate analytics")


def short(addr: str) -> str:
    return f"{addr[:4]}…{addr[-4:]}" if addr and len(addr) > 10 else addr


async def sol_price(db: Any) -> float:
    return float((await db.kv_get("sol_usd", {}) or {}).get("price") or 150)


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
        "в базе (зашифрован)" if w["stored"] else "в .env" if w["source"] == "env" else "нет")
    lines = [f"👛 <b>{esc(w['name'])}</b>",
             f"Адрес: <code>{w['pubkey']}</code>",
             "Баланс: " + (f"<b>{bal:.4f} SOL</b> (${bal * sol_usd:.2f})" if bal is not None else "неизвестен"),
             f"Режим: {MODE_ICON.get(mode, '')} <b>{S.MODES.get(mode, mode)}</b> · ставка "
             f"{S.fmt('real_stake_usd', S.get(wcfg, 'real_stake_usd'))} · позиций до {S.get(wcfg, 'real_max_open')}",
             f"Сигналы: smart {'✅' if S.get(wcfg, 'real_smart') else '❌'} · мой дев "
             f"{'✅' if S.get(wcfg, 'real_mydev') else '❌'}"
             + ("" if S.get(wcfg, "real_live_mydev") else " (в бою — нет)")
             + f" · выход: {S.get(wcfg, 'real_scheme')} / {S.get(wcfg, 'real_scheme_mydev')}",
             f"Стопы: день {S.fmt('real_day_stop_usd', S.get(wcfg, 'real_day_stop_usd'))}, всего "
             f"{S.fmt('real_total_stop_usd', S.get(wcfg, 'real_total_stop_usd'))} · ключ: {esc(key)}"]
    if want == "live" and fresh and mode != "live":
        lines.append(f"⚠️ «Бой» выбран, но не включён: {esc(ws.get('why_not_live') or 'нет разрешения на сервере')}.")
    if not fresh:
        lines.append("⚠️ Исполнитель не отвечает — состояние может быть устаревшим.")
    own_pause = str((w["settings"] or {}).get("real_paused") or "")
    if own_pause or cfg.get("real_paused"):
        lines.append(f"⛔ Новые входы остановлены: {esc(own_pause or str(cfg.get('real_paused')))}")
    today, alltime = await totals(db, day_start(cfg), wallet=wid), await totals(db, wallet=wid)
    for name, t in (("Сегодня", today), ("Всего", alltime)):
        parts = [f"{MODE_SHORT[m]} {int(t[m]['n'])} сд. {money(t[m]['pnl'] - t[m].get('fail_fees', 0.0), sol_usd)}"
                 for m in ("live", "shadow") if t[m]["n"] or t[m].get("fails")]
        lines.append(f"{name}: " + (" · ".join(parts) if parts else "сделок нет"))
    opened = ws.get("open") or [] if fresh else []
    if opened:
        lines.append(f"Открыто: {len(opened)}")
        lines += [f"• ${esc(o.get('symbol') or '?')} ({KINDS.get(o.get('kind'), o.get('kind'))}) "
                  f"{float(o.get('net') or 0):+.0%} · {float(o.get('age_s') or 0) / 60:.0f} мин" for o in opened[:6]]
    kb: Kb = [
        [{"text": "📥 Пополнить", "callback": f"rl:wd:{wid}"}, {"text": "📤 Вывести", "callback": f"rl:wo:{wid}"}],
        [{"text": ("● " if want == m else "") + f"{MODE_ICON[m]} {MODE_SHORT[m].capitalize()}"
          + ("…" if m == "live" and want != "live" else ""), "callback": f"rl:wm:{wid}:{m}"}
         for m in ("off", "shadow", "live")],
        [{"text": "⚙️ Настройки", "callback": f"rl:wsg:{wid}"}, {"text": "📋 Сделки", "callback": f"rl:wtr:{wid}"}],
        [{"text": "▶️ Продолжить входы", "callback": f"rl:wr:{wid}"} if own_pause else
         {"text": "⛔ Стоп входов", "callback": f"rl:wp:{wid}"},
         {"text": "💰 Продать всё…", "callback": f"rl:wx:{wid}"}],
        [{"text": "🔄 Обновить", "callback": f"rl:w:{wid}"}, {"text": "🗑 Удалить…", "callback": f"rl:wa:{wid}"}],
        [{"text": "← Кошельки", "callback": "rl:wl"}],
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


# ---------------------------------------------------------------------- нажатия
async def on_wallet_callback(db: Any, cfg: dict[str, Any], action: str, rest: str, rpc: Any = None) -> Reply:
    parts = rest.split(":") if rest else []
    wid = int(parts[0]) if parts and parts[0].isdigit() else None

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
            await set_wallet(db, wid, {"real_mode": m})
            return await card(f"Режим: {S.MODES[m]}")
        if m == "live":
            text, kb, _can = await live_confirm(db, cfg, wid, rpc)
            return Reply(text, kb, "", True)
        if m == "live!":
            text, kb, can = await live_confirm(db, cfg, wid, rpc)
            if not can:
                return Reply(text, kb, "Бой не включён: условия не выполнены", True)
            await set_wallet(db, wid, {"real_mode": "live", "real_armed_at": datetime.now(timezone.utc).isoformat()})
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
        elif action == "wt" and len(parts) == 2 and parts[1] in S.TOGGLES:
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
