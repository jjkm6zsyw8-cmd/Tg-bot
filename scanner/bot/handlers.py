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
from ..analytics.newcoins import DEFAULTS as _NEW_DEFAULTS

from ..analytics.copyguard import DEFAULTS as _COPY_DEFAULTS  # noqa: E402

NEW_DEFAULTS = {**_NEW_DEFAULTS, **_COPY_DEFAULTS, "paper_priority_sol": 0.0005, "paper_min_position_sol": 0.05,
                "paper_day_cap_usd": 35, "paper_day_from_h": 8, "paper_day_to_h": 18, "paper_reentry_cooldown_min": 60,
                "paper_start_usd": 100, "paper_lottery_day_stop_usd": 35}
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


TOGGLES_ON_BY_DEFAULT = {"smart_filtered", "paper_size_by_level", "trend_autotune", "trend_wait_pullback",
                         "paper_daily_reset"}


def setup(router: Router, env: Env, settings: Settings, db: Database, sender: Any) -> None:
    from ..external import ExternalHub
    ext = ExternalHub(env, db)   # внешние источники только по командам /token, /dev, /trader
    router.message.outer_middleware(Whitelist(env.allowed_ids))
    router.callback_query.outer_middleware(Whitelist(env.allowed_ids))

    async def reload() -> dict[str, Any]:
        settings.overrides = await db.get_overrides()
        return settings.effective()

    @router.errors()
    async def on_error(event: Any) -> bool:
        """Команда упала — не молчим: ответ с ошибкой в тот же чат/топик и запись для /diag."""
        exc = event.exception
        log.error("ошибка обработчика: %r", exc, exc_info=exc)
        upd = event.update
        msg = upd.message or (upd.callback_query.message if upd.callback_query else None)
        text = f"{type(exc).__name__}: {exc}"[:300]
        try:
            errs = (await db.kv_get("bot:errors", []) or [])[-9:]
            from datetime import datetime, timezone
            errs.append({"at": datetime.now(timezone.utc).isoformat(), "text": text,
                         "cmd": ((upd.message.text or "") if upd.message else "")[:40]})
            await db.kv_set("bot:errors", errs)
        except Exception:  # noqa: BLE001
            pass
        if msg is not None:
            try:
                await msg.answer(f"⚠️ Команда упала: <code>{esc(text)}</code>\nПришлите скрин — разберу.",
                                 parse_mode="HTML")
            except Exception:  # noqa: BLE001
                try:
                    await msg.answer(f"Команда упала: {text}")
                except Exception:  # noqa: BLE001
                    pass
        return True

    async def reply(m: Message, text: str, kb: Any = None) -> None:
        try:
            await ext.publish_health("bot")
        except Exception:  # noqa: BLE001
            pass
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
            [{"text": f"{on(m.get('new', True))} Новые монеты (проверка каждой монеты)", "callback": "mode:new"}],
            [{"text": f"{on(m.get('trend', True))} Тренды (DexScreener, GeckoTerminal)", "callback": "mode:trend"}],
            [{"text": f"{on(m.get('copy', True))} Копитрейд (свой список: девы и трейдеры)", "callback": "mode:copy"}],
            [{"text": "Смарт: " + ("фильтрованный → полный" if cfg.get("smart_filtered", True) else "полный → фильтрованный"),
              "callback": "tog:smart_filtered"}],
            [{"text": f"{on(cfg.get('smart_full_feed'))} Лента полного режима вместе с фильтрованным",
              "callback": "tog:smart_full_feed"}],
            [{"text": f"{on(cfg.get('watch_level_enabled'))} Уровень «Наблюдение»", "callback": "tog:watch_level_enabled"}],
            [{"text": f"{on(cfg.get('trend_autotune', True))} Тренды: автоподбор фильтров по истории",
              "callback": "tog:trend_autotune"}],
            [{"text": f"{on(cfg.get('trend_wait_pullback', True))} Тренды: вход только на откате",
              "callback": "tog:trend_wait_pullback"}],
        ]
        return text, kb

    def settings_view(cfg: dict[str, Any]) -> tuple[str, list[list[dict[str, Any]]]]:
        lines = ["<b>Настройки</b>"]
        kb = []
        for key, (label, _step, _lo, _hi, kind) in EDITABLE.items():
            v = cfg.get(key, NEW_DEFAULTS.get(key))
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
        pl = set(cfg.get("paper_levels") or ["strong", "normal"])
        lines.append("Бумажная торговля входит по уровням: <b>" + (", ".join(
            {"strong": "Сильный", "normal": "Обычный", "watch": "Наблюдение"}[x] for x in ("strong", "normal", "watch")
            if x in pl) or "ни по каким") + "</b>")
        kb.append([{"text": ("✅ " if x in pl else "⬜ ") + n, "callback": f"pl:{x}"}
                   for x, n in (("strong", "Сильный"), ("normal", "Обычный"), ("watch", "Наблюдение"))])
        share = float(cfg.get("paper_position_share") or 0.2)
        by_level = cfg.get("paper_size_by_level", True)
        lines.append(f"Бумажная позиция: <b>{share:.0%}</b> баланса"
                     + (" · по силе сигнала: Сильный ×1.5, Обычный ×0.75" if by_level else ""))
        kb.append([{"text": ("● " if abs(share - v) < 1e-6 else "") + f"{v:.0%}", "callback": f"ps:{v}"}
                   for v in (0.10, 0.15, 0.20, 0.25, 0.35)])
        kb.append([{"text": f"{'✅' if by_level else '⬜'} Размер по силе сигнала", "callback": "tog:paper_size_by_level"}])
        trends_on, daily = cfg.get("paper_trends", False), cfg.get("paper_daily_reset", True)
        lines.append(f"Бумага: «Тренды» — <b>{'входят' if trends_on else 'не входят'}</b> · сброс каждый день в 00:00 "
                     f"(Батуми) — <b>{'да' if daily else 'нет'}</b>")
        kb.append([{"text": f"{'✅' if trends_on else '⬜'} Тренды в бумаге", "callback": "tog:paper_trends"},
                   {"text": f"{'✅' if daily else '⬜'} Сброс в 00:00", "callback": "tog:paper_daily_reset"}])
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
        await reply(m, await views.token_view(db, settings, a, ext),
                    [[{"text": "Отслеживать", "callback": f"trk:{a}"}, {"text": "Скопировать адрес", "copy": a}]])

    @router.message(Command("dev"))
    async def cmd_dev(m: Message, command: CommandObject) -> None:
        ref = (command.args or "").strip().lstrip("#")
        if not ref:
            return await reply(m, "Формат: /dev &lt;адрес деплоера, монеты или номер кластера&gt;")
        await reply(m, await views.dev_view(db, settings, ref, ext))

    @router.message(Command("trader"))
    async def cmd_trader(m: Message, command: CommandObject) -> None:
        a = arg_addr(command)
        if not a:
            return await reply(m, "Формат: /trader &lt;адрес&gt;")
        await reply(m, await views.trader_view(db, settings, a, ext))

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
            await reply(m, f"✅ Дев добавлен в кластер #{cid}.\n\n" + await views.dev_view(db, settings, str(cid), ext))
            return
        cfg = await reload()
        s7, s30, _ = await compute_trader(db, addr, cfg, views.costs_of(settings))
        linked = await linked_to_dev(db, addr)
        ok, fails = qualifies(s7, s30, linked, cfg)
        await save_trader(db, addr, s7, s30, ok, linked, label=label, manual=True)
        await db.execute("UPDATE traders SET active=true WHERE address=$1", addr)
        await db.job("reload")
        note = "" if ok else "\n⚠️ Не проходит автофильтр (" + "; ".join(fails) + "), но оставлен в списке по вашему выбору."
        await reply(m, "✅ Трейдер добавлен." + note + "\n\n" + await views.trader_view(db, settings, addr, ext),
                    [[{"text": "Убрать из списка", "callback": f"wl:t:{addr}"}]])

    async def import_wallets(m: Message, text: str) -> None:
        from .importer import extract_wallets
        found = extract_wallets(text)
        if not found:
            return await reply(m, "Не нашёл адресов кошельков. Пришлите файл экспорта из Axiom (JSON/CSV/TXT) "
                                  "или адреса списком: <code>/import адрес подпись</code>, по одному в строке.")
        new = 0
        for addr, label in found:
            existed = await db.fetchval("SELECT 1 FROM watchlist WHERE kind='trader' AND address=$1", addr)
            new += not existed
            await db.execute("INSERT INTO watchlist(kind, address, label) VALUES ('trader', $1, $2) "
                             "ON CONFLICT (kind, address) DO UPDATE SET label=COALESCE(EXCLUDED.label, watchlist.label)",
                             addr, label)
        await db.job("reload")
        await db.job("import_wallets", {"wallets": [[a, lb] for a, lb in found]})
        sample = "\n".join(f"• {esc(lb or '')} <code>{a[:4]}…{a[-4:]}</code>" for a, lb in found[:10])
        more = f"\n… и ещё {len(found) - 10}" if len(found) > 10 else ""
        await reply(m, f"📥 Принял кошельков: <b>{len(found)}</b> (новых {new}).\n{sample}{more}\n\n"
                       "Слежка в реальном времени начнётся в течение минуты. Проверяю историю каждого "
                       "(~30 с на кошелёк) — отчёт «кого стоит копировать» придёт в топик «Смарт-мани».")

    @router.message(Command("diag"))
    async def cmd_diag(m: Message) -> None:
        """Диагностика без терминала: живы ли процессы, не застряли ли задачи и сообщения, ошибки."""
        from datetime import datetime, timezone

        from ..alerts import format as fmt
        now = datetime.now(timezone.utc)

        def age(iso: str | None) -> str:
            if not iso:
                return "нет данных"
            sec = (now - datetime.fromisoformat(iso)).total_seconds()
            return f"{fmt.ago(sec)} назад" + (" ⚠️" if sec > 120 else "")
        ing = await db.kv_get("health:ingest", {}) or {}
        ana = await db.kv_get("health:analytics", {}) or {}
        lines = ["🔧 <b>Диагностика</b>",
                 f"Приёмник: {age(ing.get('at'))} · Аналитика: {age(ana.get('at'))}",
                 f"Бумага: открыто {ana.get('paper_open', '—')}, ложных ценовых выходов отменено "
                 f"{ana.get('paper_false_exits', 0)}, сбойных цен отброшено {ana.get('paper_bad_prices', 0)}, "
                 f"входов по цене Jupiter (свой поток отстал) {ana.get('paper_entry_fixed', 0)}, "
                 f"выходов по цене Jupiter {ana.get('paper_exit_fixed', 0)}"]
        pend = await db.fetchrow("SELECT count(*) n, min(created_at) oldest FROM jobs WHERE done_at IS NULL")
        lines.append(f"\n<b>Задачи</b> (команды бота → аналитика): в очереди {pend['n']}"
                     + (f", самая старая {fmt.ago((now - pend['oldest']).total_seconds())} назад ⚠️"
                        if pend["n"] and (now - pend["oldest"]).total_seconds() > 60 else ""))
        for j in await db.fetch("SELECT * FROM jobs ORDER BY id DESC LIMIT 6"):
            took = (f"за {fmt.ago((j['done_at'] - j['created_at']).total_seconds())}" if j["done_at"]
                    else "ещё не выполнена")
            cmd = (j["payload"] or {}).get("cmd", "")
            lines.append(f"• {j['kind']}{' ' + str(cmd) if cmd else ''} · {fmt.ago((now - j['created_at']).total_seconds())} "
                         f"назад · {took}" + (f" · ❌ {esc(j['error'][:120])}" if j["error"] else ""))
        ob = await db.fetchrow(
            """SELECT count(*) FILTER (WHERE sent_at IS NULL AND attempts < 5) pending,
                      count(*) FILTER (WHERE sent_at IS NULL AND attempts >= 5 AND created_at > now() - interval '1 day') failed,
                      min(created_at) FILTER (WHERE sent_at IS NULL AND attempts < 5) oldest
               FROM outbox""")
        lines.append(f"\n<b>Отправка в Telegram</b>: ждут {ob['pending']}"
                     + (f" (самое старое {fmt.ago((now - ob['oldest']).total_seconds())})" if ob["pending"] else "")
                     + f", не отправлено за сутки {ob['failed']}")
        for r in await db.fetch(
                """SELECT kind, payload->>'topic' topic, attempts, error, created_at FROM outbox
                   WHERE error IS NOT NULL AND error NOT IN ('buffered', 'superseded', 'muted', 'stale')
                   AND created_at > now() - interval '1 day' ORDER BY id DESC LIMIT 5"""):
            lines.append(f"• {r['kind']}/{r['topic'] or '—'} · {fmt.ago((now - r['created_at']).total_seconds())} назад · "
                         f"попыток {r['attempts']} · {esc((r['error'] or '')[:150])}")
        flood = await db.kv_get("sender:flood", {}) or {}
        if flood.get("at"):
            lines.append(f"Последнее ограничение Telegram (flood): пауза {flood.get('seconds')} с, "
                         f"{fmt.ago((now - datetime.fromisoformat(flood['at'])).total_seconds())} назад")
        errs = await db.kv_get("bot:errors", []) or []
        if errs:
            lines.append("\n<b>Ошибки команд бота</b>")
            for e_ in errs[-5:]:
                lines.append(f"• {fmt.ago((now - datetime.fromisoformat(e_['at'])).total_seconds())} назад · "
                             f"{esc(e_.get('cmd') or '')} · {esc(e_['text'][:150])}")
        await reply(m, "\n".join(lines)[:4000])

    @router.message(Command("copylog"))
    async def cmd_copylog(m: Message, command: CommandObject) -> None:
        """Журнал копитрейда: по каждому своему кошельку — скопировано/пропущено, дельта входа, анти-PVP
        и итог бумаги по его копиям (/copylog 72 — за 72 часа)."""
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo

        from ..alerts import format as fmt
        cfg = await reload()
        try:
            tz = ZoneInfo(str(cfg.get("timezone") or "UTC"))
        except Exception:  # noqa: BLE001
            tz = ZoneInfo("UTC")
        labels = {r["address"]: r["label"] for r in await db.fetch(
            "SELECT address, label FROM watchlist WHERE kind IN ('trader', 'dev')")}
        name = lambda w: esc(labels.get(w) or f"{w[:4]}…{w[-4:]}")  # noqa: E731
        paused = await db.kv_get("copy:paused", {}) or {}
        now = datetime.now(timezone.utc).timestamp()
        arg = (command.args or "").strip().lower().rstrip("чh")
        hours = min(24 * 30, max(1, int(arg))) if arg.isdigit() else 24
        stats = {r["wallet"]: r for r in await db.fetch(
            """SELECT wallet, count(*) FILTER (WHERE action='copied') c, count(*) FILTER (WHERE action='skipped') s,
                      avg(delta) d, count(delta) dn
               FROM copy_log WHERE ts > now() - $1::float8 * interval '1 hour' GROUP BY wallet""", float(hours))}
        # итог бумаги по кошельку: одна копия = одна монета, результат — средний по портфелям (позиция одна и та же)
        paper = {r["w"]: r for r in await db.fetch(
            """WITH x AS (SELECT source_wallet w, COALESCE(alert_id::text, mint) k, avg(pnl_sol) s, avg(pnl_pct) p
                          FROM paper_positions WHERE source_wallet IS NOT NULL AND closed_at IS NOT NULL
                            AND exit_reason <> 'reset' AND NOT suspect
                            AND closed_at > now() - $1::float8 * interval '1 hour' GROUP BY 1, 2)
               SELECT w, count(*) n, count(*) FILTER (WHERE s > 0) wins, sum(s) sol, min(p) worst FROM x GROUP BY w""",
            float(hours))}
        sol = float((await db.kv_get("sol_usd", {}) or {}).get("price", 150))
        lines = [f"📒 <b>Копитрейд за {hours} ч</b> · по итогу бумаги, сначала худшие"]
        for w in sorted(set(stats) | set(paper), key=lambda w: (paper[w]["sol"] if w in paper else 0.0,
                                                                -(stats[w]["c"] if w in stats else 0))):
            r, pr = stats.get(w), paper.get(w)
            p_until = float(paused.get(w, 0) or 0)
            late = bool(r and r["dn"] and r["dn"] >= float(cfg.get("copy_delta_min_n", 5))
                        and (r["d"] or 0) > float(cfg.get("copy_max_delta", 0.15)))
            res = ""
            if pr:
                mark = "✅" if pr["sol"] > 0 else "❌"
                res = (f" · {mark} бумага {pr['n']} сд., в плюсе {pr['wins']}, {pr['sol']:+.3f} SOL "
                       f"(${pr['sol'] * sol:+.2f}), худшая {pr['worst']:+.0%}")
            lines.append(f"• {name(w)}: скопировано {r['c'] if r else 0}, пропущено {r['s'] if r else 0}"
                         + (f" · дельта входа {r['d']:+.0%} ({r['dn']})" if r and r["dn"] else "")
                         + res
                         + (f" · ⏸ анти-PVP ещё {fmt.ago(p_until - now)}" if p_until > now else "")
                         + (" · ⛔ не копируется: опаздываем" if late else "")
                         + (f"\n   <code>{w}</code>" if (pr and pr["sol"] < 0) or late or p_until > now
                            else ""))   # нажать — скопировать адрес для /removewallet
        if len(lines) == 1:
            lines.append("Сделок своих кошельков пока не было.")
        else:
            lines.append("<i>бумага: SOL на один портфель; убрать кошелёк — /removewallet адрес</i>")
        rows = await db.fetch("SELECT * FROM copy_log ORDER BY id DESC LIMIT 15")
        if rows:
            lines.append("\n<b>Последние</b>")
        for r in rows:
            icon = {"copied": "✅", "skipped": "⛔"}.get(r["action"], "↘")
            verb = {"buy": "купил", "sell": "продал", "launch": "запустил"}.get(r["side"], r["side"])
            sol_part = f" {r['sol']:.2f} SOL" if r["sol"] else ""
            delta = f" · вход {r['delta']:+.0%}" if r["delta"] is not None else ""
            lines.append(f"{icon} {r['ts'].astimezone(tz):%H:%M} {name(r['wallet'])} {verb}{sol_part} "
                         f"${esc(r['symbol'] or '?')} · MC {fmt.mc_usd(r['mc'], sol)}{delta}"
                         + (f" — {esc(r['reason'])}" if r["reason"] else ""))
        chunk = ""
        for line in lines:                     # по строкам: длинный список не рвёт HTML-теги посередине
            if chunk and len(chunk) + len(line) + 1 > 3900:
                await reply(m, chunk)
                chunk = ""
            chunk = f"{chunk}\n{line}" if chunk else line
        await reply(m, chunk)

    @router.message(Command("devonce"))
    async def cmd_devonce(m: Message, command: CommandObject) -> None:
        addr = (command.args or "").strip().split(" ")[0]
        if not is_valid_address(addr):
            return await reply(m, "Формат: <code>/devonce адрес</code> — заходить к этому деву только один раз "
                                  "(повтор команды выключает).")
        cur = await db.fetchval("SELECT once FROM my_devs WHERE address=$1", addr)
        new = not bool(cur)
        await db.execute("INSERT INTO my_devs(address, once, once_done) VALUES ($1, $2, false) "
                         "ON CONFLICT (address) DO UPDATE SET once=EXCLUDED.once, once_done=false", addr, new)
        await db.execute("INSERT INTO watchlist(kind, address) VALUES ('dev', $1) ON CONFLICT DO NOTHING", addr)
        await reply(m, "✅ Режим «один раз» включён: после первого алерта по этому деву — пауза."
                    if new else "Режим «один раз» выключен: алерты по каждому запуску.")

    @router.message(Command("import"))
    async def cmd_import(m: Message, command: CommandObject) -> None:
        if m.document is not None:          # файл с подписью /import
            return await on_document(m)
        if not command.args:
            return await reply(m, "Пришлите файл экспорта из трекера Axiom (JSON/CSV/TXT) прямо в этот чат "
                                  "или адреса: <code>/import адрес подпись</code>, по одному в строке.")
        await import_wallets(m, command.args)

    @router.message(F.document)
    async def on_document(m: Message) -> None:
        doc = m.document
        if doc is None or (doc.file_size or 0) > 2_000_000:
            return await reply(m, "Файл больше 2 МБ — это не похоже на список кошельков.")
        buf = await m.bot.download(doc)
        raw = buf.read() if buf is not None else b""
        text = raw.decode("utf-8-sig", errors="ignore")
        await import_wallets(m, text)

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

    @router.message(Command("wallet"))
    async def cmd_wallet(m: Message, command: CommandObject) -> None:
        a = arg_addr(command)
        if not a:
            return await reply(m, "Формат: /wallet &lt;адрес&gt;")
        text, kb = await views.wallet_view(db, a, ext)
        await reply(m, text, kb)

    @router.message(Command("coin"))
    async def cmd_coin(m: Message, command: CommandObject) -> None:
        a = arg_addr(command)
        if not a:
            return await reply(m, "Формат: /coin &lt;адрес токена&gt;")
        text, kb = await views.coin_view(db, settings, a, ext)
        await reply(m, text, kb)

    @router.callback_query(F.data.startswith(("w:", "coin:")))
    async def cb_wallet_coin(c: CallbackQuery) -> None:
        kind, _, addr = c.data.partition(":")
        await c.answer()
        if not is_valid_address(addr):
            return
        text, kb = await (views.wallet_view(db, addr, ext) if kind == "w" else views.coin_view(db, settings, addr, ext))
        await c.message.answer(text[:4000], parse_mode="HTML", reply_markup=build(kb), link_preview_options=NO_PREVIEW)

    @router.callback_query(F.data.startswith(("wt:", "wb:")))
    async def cb_wallet_actions(c: CallbackQuery) -> None:
        kind, _, addr = c.data.partition(":")
        if not is_valid_address(addr):
            return await c.answer()
        if kind == "wt":
            await db.execute("INSERT INTO watchlist(kind, address) VALUES ('trader', $1) ON CONFLICT DO NOTHING", addr)
            await db.job("reload")
            await c.answer("Отслеживаю кошелёк")
        else:
            await db.execute("INSERT INTO wallets(address, blacklisted) VALUES ($1, true) "
                             "ON CONFLICT (address) DO UPDATE SET blacklisted=true", addr)
            cid = await views.resolve_cluster(db, addr)
            if cid:
                await db.execute("UPDATE clusters SET blacklisted=true, blacklist_reason='вручную' WHERE id=$1", cid)
            await remove(addr)
            await c.answer("В чёрном списке")

    async def paper_view() -> Any:
        """Отчёт бумажной торговли строит сам бот по БД — не зависит от того, занята ли аналитика."""
        from types import SimpleNamespace

        from ..analytics.paper import PaperTrader
        cfg = await reload()
        price = float((await db.kv_get("sol_usd", {}) or {}).get("price", 150))
        eng = SimpleNamespace(db=db, price=SimpleNamespace(value=price), fee_bps=None, settings=settings, cfg=cfg,
                              states={}, ext=None)
        tr = PaperTrader(eng)  # type: ignore[arg-type]
        await tr.start()
        return tr

    async def paper_answer(target: Message, cmd: str, hours: float | None = None) -> None:
        from ..analytics.paper import PERIOD_KEYBOARD
        tr = await paper_view()
        if cmd == "csv":
            data = await tr.trades_csv(None)
            n = max(0, data.count("\n") - 1)
            await target.answer_document(BufferedInputFile(data.encode("utf-8-sig"), filename="paper_trades.csv"),
                                         caption=f"📄 Все сделки с последнего сброса: {n}. Открывается в Numbers / Excel.")
            return
        if cmd == "trades":
            parts = await tr.trades_text(hours or 24)
            for i, part in enumerate(parts):
                await reply(target, part, PERIOD_KEYBOARD if i == len(parts) - 1 else None)
            return
        text = await tr.history(20) if cmd == "history" else await tr.render_period(hours)
        await reply(target, text, PERIOD_KEYBOARD)

    @router.message(Command("paper"))
    async def cmd_paper(m: Message, command: CommandObject) -> None:
        arg = (command.args or "").strip().lower()
        cmd = {"reset": "reset", "history": "history", "day": "trades", "24": "trades", "csv": "csv",
               "file": "csv"}.get(arg, "state")
        if cmd == "reset":
            await db.job("paper", {"cmd": "reset"})
            return await reply(m, "Сбрасываю портфели до $30… Подтверждение придёт в «Бумажная торговля».")
        await paper_answer(m, cmd, 24 if cmd == "trades" else None)

    @router.message(Command("tune"))
    async def cmd_tune(m: Message) -> None:
        """Автоподбор фильтров «Трендов» по истории (обучение 2/3, проверка 1/3)."""
        from ..analytics.tuning import tune_trends
        cfg = await reload()
        res = await tune_trends(db, cfg)
        tail = ("\nПрименится в течение 5 минут. Ночью (03:00 UTC) подбор повторяется сам."
                if res.adopted else "")
        await reply(m, res.text(cfg) + tail)

    @router.callback_query(F.data.startswith("pp:"))
    async def cb_paper_period(c: CallbackQuery) -> None:
        arg = c.data[3:]
        await c.answer("Считаю…")
        if c.message is None:
            return
        if arg == "hist":
            await paper_answer(c.message, "history")
        elif arg == "day":
            await paper_answer(c.message, "trades", 24)
        elif arg == "csv":
            await paper_answer(c.message, "csv")
        else:
            await paper_answer(c.message, "period", None if arg == "all" else float(arg))

    @router.message(Command("testalert"))
    async def cmd_testalert(m: Message) -> None:
        await sender.setup_topics()
        res = await sender.send_test_alerts()
        await reply(m, "Тестовые алерты отправлены: " + ", ".join(esc(x) for x in res))

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
            await db.set_override(rest, not cfg.get(rest, rest in TOGGLES_ON_BY_DEFAULT))
            view = mode_view if rest in ("smart_filtered", "smart_full_feed", "watch_level_enabled",
                                         "trend_autotune", "trend_wait_pullback") else settings_view
        elif kind == "set":
            key, _, sign = rest.partition(":")
            label, step, lo, hi, _k = EDITABLE[key]
            v = float(cfg.get(key, NEW_DEFAULTS.get(key)) or 0) + (step if sign == "+" else -step)
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

    @router.callback_query(F.data.startswith("ps:"))
    async def cb_paper_share(c: CallbackQuery) -> None:
        v = float(c.data[3:])
        await db.set_override("paper_position_share", min(0.5, max(0.05, v)))
        text, kb = settings_view(await reload())
        await c.answer(f"Позиция {v:.0%} баланса")
        try:
            await c.message.edit_text(text, parse_mode="HTML", reply_markup=build(kb))
        except Exception:  # noqa: BLE001
            pass

    @router.callback_query(F.data.startswith("pl:"))
    async def cb_paper_levels(c: CallbackQuery) -> None:
        cfg = await reload()
        lv = c.data[3:]
        cur = set(cfg.get("paper_levels") or ["strong", "normal"])
        cur ^= {lv}
        await db.set_override("paper_levels", sorted(cur))
        text, kb = settings_view(await reload())
        await c.answer("Сохранено")
        try:
            await c.message.edit_text(text, parse_mode="HTML", reply_markup=build(kb))
        except Exception:  # noqa: BLE001
            pass

    @router.callback_query(F.data.startswith("prof:"))
    async def cb_profile(c: CallbackQuery) -> None:
        _, kind, ref = c.data.split(":", 2)
        await c.answer()
        text = await (views.dev_view(db, settings, ref, ext) if kind == "d" else views.trader_view(db, settings, ref, ext))
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
