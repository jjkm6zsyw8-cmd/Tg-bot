"""Команды и кнопки Telegram (разд. 9 ТЗ). Доступ — только user_id из белого списка."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import BufferedInputFile, CallbackQuery, Message, TelegramObject

from ..alerts.format import esc, pct
from ..analytics.copytrade import compute_trader, linked_to_dev, qualifies, save_trader
from ..config import EDITABLE, PRESET_NAMES, PRESETS, Env, Settings
from ..db.pool import Database
from ..reports.backtest import run_backtest
from ..reports.daily import build_daily, today_in
from ..solana import is_valid_address, short
from . import views
from .keyboards import build
from .sender import NO_PREVIEW

log = logging.getLogger(__name__)


class Whitelist(BaseMiddleware):
    """Команды и нажатия от остальных пользователей игнорируются."""

    def __init__(self, allowed: set[int]):
        self.allowed = allowed

    async def __call__(self, handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
                       event: TelegramObject, data: dict[str, Any]) -> Any:
        user = getattr(event, "from_user", None)
        if user is None or user.id not in self.allowed:
            return None
        return await handler(event, data)


HELP = """<b>Pump.fun Dev Scanner</b>
Бот присылает ранние сигналы по девам и смарт-мани. Торговля — вручную в Axiom или Pump.fun.

/mode — режимы и «фильтрованный / полный»
/settings — пороги, пресет фильтров, реакция, позиция
/token &lt;адрес&gt; — разбор монеты
/dev &lt;адрес|#&gt; — профиль кластера дева
/trader &lt;адрес&gt; — профиль трейдера
/watch &lt;адрес&gt; — сопровождение монеты
/watchlist — что отслеживается
/addwallet &lt;адрес&gt; &lt;метка&gt; [dev|trader]
/removewallet &lt;адрес&gt;
/blacklist &lt;адрес&gt;
/today — разбор дня · /backtest &lt;период&gt; [a=1 vs a=2]
/stats — итог серии · /mute &lt;минуты&gt; · /health
/quiet 01:00-08:00 · /tz Europe/Moscow · /dailytime 21:00 · /mywallet &lt;адрес&gt;"""


def setup(router: Router, env: Env, settings: Settings, db: Database, sender: Any) -> None:
    router.message.outer_middleware(Whitelist(env.allowed_ids))
    router.callback_query.outer_middleware(Whitelist(env.allowed_ids))

    async def reload() -> dict[str, Any]:
        settings.overrides = await db.get_overrides()
        return settings.effective()

    async def reply(m: Message, text: str, kb: Any = None) -> None:
        for i in range(0, len(text), 4000):
            await m.answer(text[i:i + 4000], parse_mode="HTML", reply_markup=build(kb) if i == 0 else None,
                           link_preview_options=NO_PREVIEW)

    def main_menu() -> list[list[dict[str, Any]]]:
        return [
            [{"text": "Режимы", "callback": "menu:mode"}, {"text": "Настройки", "callback": "menu:settings"}],
            [{"text": "Отслеживаемое", "callback": "menu:watchlist"}, {"text": "Разбор дня", "callback": "menu:today"}],
            [{"text": "Итог серии", "callback": "menu:stats"}, {"text": "Состояние", "callback": "menu:health"}],
        ]

    def mode_view(cfg: dict[str, Any]) -> tuple[str, list[list[dict[str, Any]]]]:
        m = cfg.get("modes", {})
        on = lambda v: "✅" if v else "⬜"  # noqa: E731
        text = ("<b>Режимы</b>\nСмарт-мани: " + ("фильтрованный" if cfg.get("smart_filtered", True) else "полный")
                + "\nУровень «Наблюдение»: " + ("включён" if cfg.get("watch_level_enabled") else "выключен"))
        kb = [
            [{"text": f"{on(m.get('dev', True))} Девы", "callback": "mode:dev"},
             {"text": f"{on(m.get('smart', True))} Смарт-мани", "callback": "mode:smart"},
             {"text": f"{on(m.get('daily', True))} Разбор дня", "callback": "mode:daily"}],
            [{"text": "Смарт: " + ("фильтрованный → полный" if cfg.get("smart_filtered", True) else "полный → фильтрованный"),
              "callback": "tog:smart_filtered"}],
            [{"text": f"{on(cfg.get('smart_full_feed'))} Лента полного режима вместе с фильтрованным",
              "callback": "tog:smart_full_feed"}],
            [{"text": f"{on(cfg.get('watch_level_enabled'))} Уровень «Наблюдение»", "callback": "tog:watch_level_enabled"}],
        ]
        return text, kb

    def settings_view(cfg: dict[str, Any]) -> tuple[str, list[list[dict[str, Any]]]]:
        lines = ["<b>Настройки</b>"]
        kb = []
        for key, (label, _step, _lo, _hi, kind) in EDITABLE.items():
            v = cfg.get(key)
            shown = pct(v, False) if kind == "pct" else f"{v:g}" if isinstance(v, (int, float)) else str(v)
            lines.append(f"{label}: <b>{shown}</b>")
            kb.append([{"text": f"− {label[:24]}", "callback": f"set:{key}:-"},
                       {"text": "+", "callback": f"set:{key}:+"}])
        preset = cfg.get("filter_preset", "normal")
        lines.append(f"Пресет фильтров: <b>{PRESET_NAMES.get(preset, preset)}</b>")
        kb.append([{"text": ("● " if p == preset else "") + PRESET_NAMES[p], "callback": f"preset:{p}"} for p in PRESETS])
        lines.append(f"Формат: <b>{'компактный' if cfg.get('compact') else 'подробный'}</b> · "
                     f"Неполные данные: <b>{'ждать' if cfg.get('wait_full_data') else 'слать с пометкой'}</b>")
        kb.append([{"text": "Компактный/подробный", "callback": "tog:compact"},
                   {"text": "Ждать полные данные", "callback": "tog:wait_full_data"}])
        lines.append(f"Тихие часы: <b>{cfg.get('quiet_hours') or 'нет'}</b> · Часовой пояс: <b>{cfg.get('timezone')}</b>"
                     f" · Разбор дня в <b>{cfg.get('daily_report_time')}</b>")
        if cfg.get("trading_wallet"):
            lines.append(f"Торговый кошелёк (только чтение): <code>{cfg['trading_wallet']}</code>")
        return "\n".join(lines), kb

    # ------------------------------------------------------------------ команды
    @router.message(Command("start", "help"))
    async def cmd_start(m: Message) -> None:
        note = await sender.setup_topics() if m.chat.type in ("supergroup", "group") else ""
        await reply(m, HELP + (f"\n\n<i>{esc(note)}</i>" if note else ""), main_menu())

    @router.message(Command("mode"))
    async def cmd_mode(m: Message) -> None:
        text, kb = mode_view(await reload())
        await reply(m, text, kb)

    @router.message(Command("settings"))
    async def cmd_settings(m: Message) -> None:
        text, kb = settings_view(await reload())
        await reply(m, text, kb)

    def arg_addr(c: CommandObject | None) -> str | None:
        a = (c.args or "").split()[0] if c and c.args else None
        return a if is_valid_address(a) else None

    @router.message(Command("token"))
    async def cmd_token(m: Message, command: CommandObject) -> None:
        a = arg_addr(command)
        if not a:
            return await reply(m, "Формат: /token &lt;адрес токена&gt;")
        await reply(m, await views.token_view(db, settings, a),
                    [[{"text": "Отслеживать", "callback": f"trk:{a}"}, {"text": "Скопировать адрес", "copy": a}]])

    @router.message(Command("dev"))
    async def cmd_dev(m: Message, command: CommandObject) -> None:
        ref = (command.args or "").strip().lstrip("#")
        if not ref:
            return await reply(m, "Формат: /dev &lt;адрес деплоера, монеты или номер кластера&gt;")
        await reply(m, await views.dev_view(db, settings, ref))

    @router.message(Command("trader"))
    async def cmd_trader(m: Message, command: CommandObject) -> None:
        a = arg_addr(command)
        if not a:
            return await reply(m, "Формат: /trader &lt;адрес&gt;")
        await reply(m, await views.trader_view(db, settings, a))

    @router.message(Command("watch"))
    async def cmd_watch(m: Message, command: CommandObject) -> None:
        a = arg_addr(command)
        if not a:
            return await reply(m, "Формат: /watch &lt;адрес токена&gt;")
        await watch_token(a)
        await reply(m, f"👁 Сопровождаю <code>{a}</code>: сигналы на выход придут в «Сопровождение монет».")

    async def watch_token(mint: str) -> None:
        cfg = await reload()
        await db.execute(
            "INSERT INTO watchlist(kind, address, until) VALUES ('token', $1, now() + make_interval(mins => $2)) "
            "ON CONFLICT (kind, address) DO UPDATE SET until=EXCLUDED.until", mint, int(cfg.get("follow_minutes", 60)))
        await db.job("watch", {"mint": mint})

    @router.message(Command("watchlist"))
    async def cmd_watchlist(m: Message) -> None:
        text, kb = await views.watchlist_view(db)
        await reply(m, text, kb)

    @router.message(Command("addwallet"))
    async def cmd_addwallet(m: Message, command: CommandObject) -> None:
        parts = (command.args or "").split()
        if not parts or not is_valid_address(parts[0]):
            return await reply(m, "Формат: /addwallet &lt;адрес&gt; &lt;метка&gt; [dev|trader]")
        addr = parts[0]
        kind = parts[-1].lower() if len(parts) > 1 and parts[-1].lower() in ("dev", "trader") else None
        label = " ".join(p for p in parts[1:] if p.lower() not in ("dev", "trader"))[:64] or None
        deployed = await db.fetchval("SELECT count(*) FROM tokens WHERE deployer=$1", addr)
        if kind == "dev" or (kind is None and deployed):
            from ..analytics.clusters import add_link, cluster_of, new_cluster
            known = await cluster_of(db, addr)
            cid = known[0] if known else await new_cluster(db, addr)
            await add_link(db, cid, addr, "добавлен пользователем", "high", status="confirmed")
            await db.execute("UPDATE clusters SET priority=true, label=COALESCE($2, label) WHERE id=$1", cid, label)
            await db.execute("INSERT INTO watchlist(kind, address, label) VALUES ('dev', $1, $2) "
                             "ON CONFLICT (kind, address) DO UPDATE SET label=EXCLUDED.label", addr, label)
            await db.job("recompute_cluster", {"cluster_id": cid})
            await reply(m, f"✅ Дев добавлен в кластер #{cid}.\n\n" + await views.dev_view(db, settings, str(cid)))
            return
        cfg = await reload()
        s7, s30, _ = await compute_trader(db, addr, cfg, views.costs_of(settings))
        linked = await linked_to_dev(db, addr)
        ok, fails = qualifies(s7, s30, linked, cfg)
        await save_trader(db, addr, s7, s30, ok, linked, label=label, manual=True)
        await db.execute("UPDATE traders SET active=true WHERE address=$1", addr)
        await db.job("reload")
        note = "" if ok else "\n⚠️ Не проходит автофильтр (" + "; ".join(fails) + "), но оставлен в списке по вашему выбору."
        await reply(m, "✅ Трейдер добавлен." + note + "\n\n" + await views.trader_view(db, settings, addr),
                    [[{"text": "Убрать из списка", "callback": f"wl:t:{addr}"}]])

    @router.message(Command("removewallet"))
    async def cmd_removewallet(m: Message, command: CommandObject) -> None:
        a = arg_addr(command)
        if not a:
            return await reply(m, "Формат: /removewallet &lt;адрес&gt;")
        await remove(a)
        await reply(m, f"Убран из отслеживания: <code>{a}</code>")

    async def remove(addr: str, code: str | None = None) -> None:
        """code: t — трейдер, d — дев, k — монета, None — везде."""
        if code in (None, "t"):
            await db.execute("UPDATE traders SET active=false, manual=false WHERE address=$1", addr)
        if code is None:
            await db.execute("DELETE FROM watchlist WHERE address=$1", addr)
        else:
            kind = {"t": "trader", "d": "dev", "k": "token"}[code]
            await db.execute("DELETE FROM watchlist WHERE address=$1 AND kind=$2", addr, kind)
        await db.job("reload")

    @router.message(Command("blacklist"))
    async def cmd_blacklist(m: Message, command: CommandObject) -> None:
        ref = (command.args or "").strip().lstrip("#")
        if not ref:
            return await reply(m, "Формат: /blacklist &lt;адрес или номер кластера&gt;")
        cid = await views.resolve_cluster(db, ref)
        if is_valid_address(ref):
            await db.execute("INSERT INTO wallets(address, blacklisted) VALUES ($1, true) "
                             "ON CONFLICT (address) DO UPDATE SET blacklisted=true", ref)
        if cid:
            await db.execute("UPDATE clusters SET blacklisted=true, blacklist_reason='вручную' WHERE id=$1", cid)
        await remove(ref)
        await reply(m, f"⛔ В чёрном списке: {esc(ref)}" + (f" (кластер #{cid})" if cid else ""))

    @router.message(Command("today"))
    async def cmd_today(m: Message) -> None:
        cfg = await reload()
        text, csv_bytes, kb = await build_daily(db, settings, today_in(cfg.get("timezone", "UTC")))
        await reply(m, text, kb)
        await m.answer_document(BufferedInputFile(csv_bytes, filename="today.csv"))

    @router.message(Command("backtest"))
    async def cmd_backtest(m: Message, command: CommandObject) -> None:
        await reload()
        await reply(m, "🧪 Считаю бэктест, это может занять пару минут…")

        async def job() -> None:
            try:
                text = await run_backtest(db, settings, command.args or "7d")
            except ValueError as exc:
                text = f"Ошибка: {esc(exc)}"
            except Exception as exc:  # noqa: BLE001
                log.exception("backtest")
                text = f"Бэктест не удался: {esc(exc)}"
            await reply(m, text)
        asyncio.create_task(job())

    @router.message(Command("stats"))
    async def cmd_stats(m: Message) -> None:
        await reply(m, await views.stats_view(db))

    @router.message(Command("mute"))
    async def cmd_mute(m: Message, command: CommandObject) -> None:
        arg = (command.args or "60").strip()
        minutes = int(arg) if arg.isdigit() else 60
        if minutes == 0:
            await db.kv_set("mute_until", None)
            return await reply(m, "🔔 Алерты снова включены.")
        until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        await db.kv_set("mute_until", until.isoformat())
        await reply(m, f"🔕 Пауза алертов на {minutes} мин. Экстренные сигналы на выход по открытым монетам "
                       "продолжат приходить. /mute 0 — снять паузу.")

    @router.message(Command("health"))
    async def cmd_health(m: Message) -> None:
        await reply(m, await views.health_view(db))

    async def set_text(m: Message, key: str, value: Any, ok: str) -> None:
        await db.set_override(key, value)
        await reply(m, ok)

    @router.message(Command("quiet"))
    async def cmd_quiet(m: Message, command: CommandObject) -> None:
        v = (command.args or "").strip()
        await set_text(m, "quiet_hours", v or None, f"Тихие часы: {esc(v) or 'выключены'}")

    @router.message(Command("tz"))
    async def cmd_tz(m: Message, command: CommandObject) -> None:
        from zoneinfo import ZoneInfo
        v = (command.args or "").strip()
        try:
            ZoneInfo(v)
        except Exception:  # noqa: BLE001
            return await reply(m, "Формат: /tz Europe/Moscow")
        await set_text(m, "timezone", v, f"Часовой пояс: {esc(v)}")

    @router.message(Command("dailytime"))
    async def cmd_dailytime(m: Message, command: CommandObject) -> None:
        v = (command.args or "").strip()
        try:
            datetime.strptime(v, "%H:%M")
        except ValueError:
            return await reply(m, "Формат: /dailytime 21:00")
        await set_text(m, "daily_report_time", v, f"Разбор дня в {v}")

    @router.message(Command("mywallet"))
    async def cmd_mywallet(m: Message, command: CommandObject) -> None:
        a = arg_addr(command)
        if not a:
            return await reply(m, "Формат: /mywallet &lt;публичный адрес&gt;. Ключи не нужны и не принимаются.")
        await set_text(m, "trading_wallet", a, "Торговый кошелёк сохранён (только публичный адрес, только чтение).")

    # ------------------------------------------------------------------ кнопки
    @router.callback_query(F.data.startswith("menu:"))
    async def cb_menu(c: CallbackQuery) -> None:
        what = c.data.split(":", 1)[1]
        await c.answer()
        m = c.message
        if what == "mode":
            text, kb = mode_view(await reload())
        elif what == "settings":
            text, kb = settings_view(await reload())
        elif what == "watchlist":
            text, kb = await views.watchlist_view(db)
        elif what == "stats":
            text, kb = await views.stats_view(db), None
        elif what == "health":
            text, kb = await views.health_view(db), None
        else:
            cfg = await reload()
            text, _csv, kb = await build_daily(db, settings, today_in(cfg.get("timezone", "UTC")))
        await m.answer(text[:4000], parse_mode="HTML", reply_markup=build(kb))

    @router.callback_query(F.data.startswith(("mode:", "tog:", "set:", "preset:")))
    async def cb_settings(c: CallbackQuery) -> None:
        cfg = await reload()
        kind, _, rest = c.data.partition(":")
        if kind == "mode":
            modes = dict(cfg.get("modes", {}))
            modes[rest] = not modes.get(rest, True)
            await db.set_override("modes", modes)
            view = mode_view
        elif kind == "tog":
            await db.set_override(rest, not cfg.get(rest, rest == "smart_filtered"))
            view = mode_view if rest in ("smart_filtered", "smart_full_feed", "watch_level_enabled") else settings_view
        elif kind == "set":
            key, _, sign = rest.partition(":")
            label, step, lo, hi, _k = EDITABLE[key]
            v = float(cfg.get(key) or 0) + (step if sign == "+" else -step)
            await db.set_override(key, round(min(hi, max(lo, v)), 4))
            view = settings_view
        else:
            await db.set_override("filter_preset", rest)
            view = settings_view
        text, kb = view(await reload())
        await c.answer("Сохранено")
        try:
            await c.message.edit_text(text, parse_mode="HTML", reply_markup=build(kb))
        except Exception:  # noqa: BLE001
            pass

    @router.callback_query(F.data.startswith("prof:"))
    async def cb_profile(c: CallbackQuery) -> None:
        _, kind, ref = c.data.split(":", 2)
        await c.answer()
        text = await (views.dev_view(db, settings, ref) if kind == "d" else views.trader_view(db, settings, ref))
        await c.message.answer(text[:4000], parse_mode="HTML")

    @router.callback_query(F.data.startswith("trk:"))
    async def cb_track(c: CallbackQuery) -> None:
        mint = c.data[4:]
        if is_valid_address(mint):
            await watch_token(mint)
        await c.answer("Сопровождаю монету")

    @router.callback_query(F.data.startswith("mk:"))
    async def cb_mark(c: CallbackQuery) -> None:
        _, aid, mark = c.data.split(":")
        await db.execute(
            "INSERT INTO user_marks(alert_id, user_id, mark) VALUES ($1,$2,$3) "
            "ON CONFLICT (alert_id, user_id) DO UPDATE SET mark=EXCLUDED.mark, created_at=now()",
            int(aid), c.from_user.id, mark)
        await db.job("mark", {"alert_id": int(aid), "mark": mark})
        names = {"great": "Очень успешно", "good": "Успешно", "fail": "Провал", "scam": "Скам"}
        await c.answer(f"Отмечено: {names[mark]}")

    @router.callback_query(F.data.startswith("lk:"))
    async def cb_link(c: CallbackQuery) -> None:
        _, lid, yn = c.data.split(":")
        row = await db.fetchrow(
            "UPDATE cluster_links SET status=$2, decided_at=now(), confidence=CASE WHEN $2='confirmed' THEN 'high' "
            "ELSE confidence END WHERE id=$1 RETURNING cluster_id, wallet",
            int(lid), "confirmed" if yn == "y" else "rejected")
        if row:
            await db.job("recompute_cluster", {"cluster_id": row["cluster_id"]})
        await c.answer("Подтверждено" if yn == "y" else "Отклонено")
        try:
            await c.message.edit_reply_markup(reply_markup=None)
            await c.message.answer(("✅ Кошелёк вошёл в кластер" if yn == "y" else "✖ Связь отклонена, больше не предложу")
                                   + (f" #{row['cluster_id']}: <code>{row['wallet']}</code>" if row else ""),
                                   parse_mode="HTML")
        except Exception:  # noqa: BLE001
            pass

    @router.callback_query(F.data.startswith("lt:"))
    async def cb_lower(c: CallbackQuery) -> None:
        cid = int(c.data[3:])
        cfg = await reload()
        thr = round(float(cfg.get("min_net_profit", 0.3)) * 0.7, 3)
        await db.execute("UPDATE clusters SET threshold_override=$2 WHERE id=$1", cid, thr)
        await c.answer(f"Порог для кластера #{cid}: {thr:.0%}")

    @router.callback_query(F.data.startswith("cand:"))
    async def cb_candidate(c: CallbackQuery) -> None:
        _, kind, ref = c.data.split(":", 2)
        if kind == "c":
            await db.execute("UPDATE clusters SET priority=true WHERE id=$1", int(ref))
            await db.execute("INSERT INTO watchlist(kind, address, label) VALUES ('dev', $1, $2) ON CONFLICT DO NOTHING",
                             f"cluster:{ref}", f"кластер #{ref}")
        else:
            await db.execute("UPDATE traders SET active=true, manual=true WHERE address=$1", ref)
            await db.job("reload")
        await c.answer("Отслеживаю")

    @router.callback_query(F.data.startswith("wl:"))
    async def cb_unwatch(c: CallbackQuery) -> None:
        _, kind, addr = c.data.split(":", 2)
        await remove(addr, kind if kind in ("t", "d", "k") else None)
        await c.answer(f"Убрано: {short(addr)}")
