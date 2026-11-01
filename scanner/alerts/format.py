"""Тексты алертов и кнопки (разд. 8 ТЗ).

Первые три строки — всё для решения за 2–3 секунды: уровень, потенциал, MC,
точка входа и цель выхода. Клавиатуры описываются JSON-структурой, бот
превращает её в InlineKeyboardMarkup (copy_text, style=danger).
"""
from __future__ import annotations

import html
from typing import Any

from ..analytics.filters import compact_line
from ..solana import short

ICONS = {"strong": "🟢", "normal": "🟡", "watch": "⚪", "exit": "🔴", "info": "🔵"}
LEVEL_NAMES = {"strong": "СИЛЬНЫЙ", "normal": "ОБЫЧНЫЙ", "watch": "НАБЛЮДЕНИЕ"}
MODE_NAMES = {"dev": "ДЕВ", "smart": "СМАРТ-МАНИ", "new_dev_wallet": "НОВЫЙ КОШЕЛЁК ДЕВА", "new": "НОВАЯ МОНЕТА"}
CONF_NAMES = {"high": "высокая", "medium": "средняя", "low": "низкая"}
SIGNAL_NAMES = {"single": "покупка трейдера", "consensus": "консенсус", "dev_match": "совпадение с девом"}


def esc(s: Any) -> str:
    return html.escape(str(s if s is not None else ""), quote=False)


def usd(v: float | None) -> str:
    if v is None:
        return "—"
    a = abs(v)
    if a >= 1e6:
        return f"${v / 1e6:.2f}M"
    if a >= 1e3:
        return f"${v / 1e3:.1f}k"
    return f"${v:.0f}"


def mc_usd(mc_sol: float | None, sol_usd: float) -> str:
    return usd(mc_sol * sol_usd) if mc_sol is not None else "—"


def pct(v: float | None, sign: bool = True) -> str:
    if v is None:
        return "—"
    return f"{v * 100:+.0f}%" if sign else f"{v * 100:.0f}%"


def age(seconds: float) -> str:
    s = max(0, int(seconds))
    if s < 60:
        return f"{s} с"
    if s < 3600:
        return f"{s // 60} мин"
    return f"{s // 3600} ч {s % 3600 // 60} мин"


def link(template: str, mint: str = "", wallet: str = "", redirect_base: str = "") -> str:
    return template.format(mint=mint, wallet=wallet, redirect_base=redirect_base.rstrip("/"))


def pump_url(links: dict[str, Any], mint: str, redirect_base: str) -> str:
    if links.get("use_direct_pump_link"):
        return link(links["pump_web"], mint=mint)
    return link(links.get("pump_redirect", "{redirect_base}/p/{mint}"), mint=mint, redirect_base=redirect_base)


def alert_keyboard(mint: str, links: dict[str, Any], redirect_base: str, profile_kind: str,
                   profile_ref: str, marks_alert_id: int | None = None) -> list[list[dict[str, Any]]]:
    rows: list[list[dict[str, Any]]] = [
        [{"text": "Pump.fun (приложение)", "url": pump_url(links, mint, redirect_base)},
         {"text": "Axiom", "url": link(links["axiom"], mint=mint)}],
        [{"text": "Скопировать адрес", "copy": mint},
         {"text": "График", "url": link(links["chart"], mint=mint)}],
        [{"text": "Профиль дева" if profile_kind == "d" else "Профиль трейдера",
          "callback": f"prof:{profile_kind}:{profile_ref}"},
         {"text": "Отслеживать", "callback": f"trk:{mint}"}],
    ]
    if marks_alert_id is not None:
        rows.append(marks_keyboard(marks_alert_id)[0])
    return rows


def marks_keyboard(alert_id: int) -> list[list[dict[str, Any]]]:
    return [[
        {"text": "Очень успешно", "callback": f"mk:{alert_id}:great", "style": "success"},
        {"text": "Успешно", "callback": f"mk:{alert_id}:good"},
        {"text": "Провал", "callback": f"mk:{alert_id}:fail"},
        {"text": "Скам", "callback": f"mk:{alert_id}:scam", "style": "danger"},
    ]]


def exit_keyboard(mint: str, links: dict[str, Any], redirect_base: str) -> list[list[dict[str, Any]]]:
    return [[
        {"text": "🔴 Выйти: Pump.fun", "url": pump_url(links, mint, redirect_base), "style": "danger"},
        {"text": "Axiom", "url": link(links["axiom"], mint=mint), "style": "danger"},
    ]]


def dev_block(profile: dict[str, Any] | None, link_conf: str, link_reason: str, sol_usd: float) -> list[str]:
    if not profile:
        return []
    med = profile.get("medians") or {}
    ttd = med.get("time_to_dump_s")
    lines = [
        f"Дев: {profile.get('launches', 0)} запусков, похожих {profile.get('similar_launches', 0)}/"
        f"{profile.get('launches', 0)}, связь {CONF_NAMES.get(link_conf, link_conf)}"
        + (f" ({esc(link_reason)})" if link_reason else ""),
    ]
    if med.get("dump_mc"):
        lines.append(f"Типичный дамп: MC {mc_usd(med['dump_mc'], sol_usd)}"
                     + (f" через ~{age(ttd)}" if ttd else ""))
    if profile.get("migrated"):
        lines.append(f"Мигрировало: {profile['migrated']}")
    return lines


def render_alert(a: dict[str, Any], sol_usd: float, compact: bool = False) -> str:
    """a — объединённый алерт по монете (см. engine.Composer)."""
    level = a["level"]
    modes = a["modes"]
    head_modes = " + ".join(MODE_NAMES.get(m, m.upper()) for m in modes)
    pot = a.get("potential")
    net = a.get("net")
    head = f"{ICONS[level]} <b>{LEVEL_NAMES[level]} · {head_modes}</b>"
    if pot:
        head += f" · потенциал x{pot:.1f} (около {pct(net)} чистыми)"
    elif a.get("signal"):
        head += f" · {SIGNAL_NAMES.get(a['signal'], a['signal'])}"
    l2 = f"<b>${esc(a.get('symbol') or '?')}</b> · MC {mc_usd(a['mc'], sol_usd)} · {age(a['age_s'])} от запуска"
    exit_rule = a.get("exit_rule") or "Выход сразу при продаже кластера"
    l3 = (f"Вход до MC {mc_usd(a.get('entry_max_mc'), sol_usd)} · Цель MC {mc_usd(a.get('target_mc'), sol_usd)}"
          f" · {exit_rule}")
    lines = [head, l2, l3]
    if a.get("incomplete"):
        lines.append("<i>⏳ данные неполные (холдеры/связи ещё досчитываются)</i>")
    if not compact:
        lines.append("")
        lines += dev_block(a.get("profile"), a.get("link_confidence", "high"), a.get("link_reason", ""), sol_usd)
        for t in a.get("traders") or []:
            lines.append(
                f"Трейдер «{esc(t.get('label') or short(t['address']))}» ({esc(t.get('tier_name', ''))}): "
                f"копируемость {pct(t.get('copy_30d'))}/сделку, {pct(t.get('win_rate'), False)} удачных, "
                f"купил {t.get('buy_sol', 0):.2f} SOL")
        if a.get("smart_in_coin"):
            lines.append(f"Смарт-мани в монете: {a['smart_in_coin']}")
        for r in a.get("extra_reasons") or []:
            lines.append(esc(r))
        metrics = a.get("metrics") or {}
        if metrics:
            lines.append(compact_line(metrics, a.get("violations") or []))
        if a.get("stage"):
            lines.append(f"Стадия: {esc(a['stage'])}")
        if a.get("latency_s") is not None:
            lines.append(f"Задержка бота: {a['latency_s']:.1f} с")
    lines += ["", f"<code>{esc(a['mint'])}</code>"]
    return "\n".join(lines)


def render_follow(kind: str, a: dict[str, Any], sol_usd: float, extra: str = "") -> str:
    sym = f"${esc(a.get('symbol') or '?')}"
    mc = mc_usd(a.get("mc"), sol_usd)
    texts = {
        "target": f"🎯 <b>Цель достигнута</b> · {sym} · MC {mc}. Фиксируйте прибыль.",
        "zone70": f"🟠 <b>MC дошёл до 70% типичного дампа</b> · {sym} · MC {mc}. Пора выходить.",
        "emergency": f"🔴 <b>ВЫХОД: кластер продаёт</b> · {sym} · MC {mc}",
        "dev_early": f"⚠️ <b>Нетипично: дев продаёт раньше обычного</b> · {sym} · MC {mc}",
        "dev_buys": f"⚠️ <b>Нетипично: дев докупает</b> · {sym} · MC {mc}",
        "trader_sell": f"🔴 <b>ВЫХОД: трейдер продал больше половины</b> · {sym} · MC {mc}",
        "level_up": f"⬆️ <b>Уровень повышен</b> · {sym} · MC {mc}",
        "pre_migration": f"🚀 <b>Скоро миграция</b> · {sym} · MC {mc}",
        "migrated": f"🔁 <b>Мигрировала в PumpSwap</b> · {sym} · MC {mc}. Отслеживание продолжается.",
    }
    out = texts.get(kind, f"{kind} · {sym} · MC {mc}")
    if extra:
        out += "\n" + extra
    return out + f"\n<code>{esc(a['mint'])}</code>"


def render_final(a: dict[str, Any], outcome: dict[str, Any], sol_usd: float) -> str:
    res = outcome.get("net_result")
    best = outcome.get("achievable_profit")
    reason = {"target": "по цели", "emergency": "по экстренному сигналу", "timeout": "через 60 мин",
              "trader_sell": "по продаже трейдера"}.get(outcome.get("exit_reason"), "")
    auto = {"great": "Очень успешно", "good": "Успешно", "fail": "Провал", "scam": "Скам"}.get(
        outcome.get("auto_mark") or "", "—")
    return "\n".join([
        f"📊 <b>Итог 60 минут</b> · ${esc(a.get('symbol') or '?')}",
        f"Максимум MC {mc_usd(outcome.get('max_mc'), sol_usd)} · достижимый профит {pct(best)}",
        f"Результат алерта (вход +{a.get('reaction_s', 5)} с, выход {reason}): {pct(res)}",
        f"Предварительная оценка: {auto}. Отметьте результат — отметка важнее авторазметки.",
        f"<code>{esc(a['mint'])}</code>",
    ])


def render_full_line(t: dict[str, Any], sol_usd: float) -> str:
    side = "🟢 BUY" if t["is_buy"] else "🔴 SELL"
    return (f"{side} {esc(t.get('label') or short(t['trader']))} {t['sol']:.2f} SOL · "
            f"${esc(t.get('symbol') or '?')} · MC {mc_usd(t['mc'], sol_usd)} · <code>{esc(t['mint'])}</code>")
