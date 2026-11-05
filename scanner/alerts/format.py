"""Тексты алертов и кнопки (разд. 8 ТЗ).

Первые три строки — всё для решения за 2–3 секунды: уровень, потенциал, MC,
точка входа и цель выхода. Клавиатуры описываются JSON-структурой, бот
превращает её в InlineKeyboardMarkup (copy_text, style=danger).
"""
from __future__ import annotations

import html
from typing import Any

from ..solana import short

ICONS = {"strong": "🟢", "normal": "🟡", "watch": "⚪", "exit": "🔴", "info": "🔵"}
LEVEL_NAMES = {"strong": "СИЛЬНЫЙ", "normal": "ОБЫЧНЫЙ", "watch": "НАБЛЮДЕНИЕ"}
MODE_NAMES = {"dev": "ДЕВ", "smart": "СМАРТ-МАНИ", "new_dev_wallet": "НОВЫЙ КОШЕЛЁК ДЕВА", "new": "НОВАЯ МОНЕТА", "trend": "ТРЕНД",
              "copy": "КОПИТРЕЙД", "mydev": "МОЙ ДЕВ"}
CONF_NAMES = {"high": "высокая", "medium": "средняя", "low": "низкая"}
SIGNAL_NAMES = {"single": "покупка трейдера", "consensus": "консенсус", "dev_match": "совпадение с девом",
                "impulse": "импульс"}


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


def has_redirect(redirect_base: str) -> bool:
    return bool(redirect_base) and "example.com" not in redirect_base and redirect_base.startswith("https://")


def pump_url(links: dict[str, Any], mint: str, redirect_base: str) -> str:
    """Своя HTTPS-страница → pump://coin/<адрес>, запасной вариант https://pump.fun/coin/<адрес>.
    Без своей страницы (REDIRECT_BASE не задан) — сразу pump.fun."""
    if not has_redirect(redirect_base):
        return link(links["pump_web"], mint=mint)
    return link(links.get("pump_redirect", "{redirect_base}/p/{mint}"), mint=mint, redirect_base=redirect_base)


def alert_keyboard(mint: str, links: dict[str, Any], redirect_base: str, profile_kind: str,
                   profile_ref: str, marks_alert_id: int | None = None,
                   wallet: str | None = None) -> list[list[dict[str, Any]]]:
    """Ряд 1 — все площадки, ряд 2 — адрес, разбор, слежение, ряд 3 — дев/трейдер."""
    rows: list[list[dict[str, Any]]] = [
        [{"text": "Pump", "url": pump_url(links, mint, redirect_base)},
         {"text": "Axiom", "url": link(links["axiom"], mint=mint)},
         {"text": "DEX", "url": link(links["chart"], mint=mint)},
         {"text": "GMGN", "url": link(links["gmgn"], mint=mint)}],
        [{"text": "📋 Адрес", "copy": mint},
         {"text": "🔍 Разбор", "callback": f"coin:{mint}"},
         {"text": "👁 Следить", "callback": f"trk:{mint}"}],
    ]
    third = []
    if profile_ref and profile_ref != "None" and profile_kind == "d":
        third.append({"text": "👤 Дев", "callback": f"prof:d:{profile_ref}"})
    if wallet:
        third.append({"text": "👛 Кошелёк", "callback": f"w:{wallet}"})
    if third:
        rows.append(third)
    if marks_alert_id is not None:
        rows += marks_keyboard(marks_alert_id)
    return rows


def marks_keyboard(alert_id: int) -> list[list[dict[str, Any]]]:
    return [[
        {"text": "🚀 Отлично", "callback": f"mk:{alert_id}:great", "style": "success"},
        {"text": "✅ Успех", "callback": f"mk:{alert_id}:good"},
        {"text": "❌ Провал", "callback": f"mk:{alert_id}:fail"},
        {"text": "☠️ Скам", "callback": f"mk:{alert_id}:scam", "style": "danger"},
    ]]


def exit_keyboard(mint: str, links: dict[str, Any], redirect_base: str) -> list[list[dict[str, Any]]]:
    return [[
        {"text": "🔴 Pump", "url": pump_url(links, mint, redirect_base), "style": "danger"},
        {"text": "🔴 Axiom", "url": link(links["axiom"], mint=mint), "style": "danger"},
        {"text": "📋 Адрес", "copy": mint},
    ]]


def ago(seconds: float | None) -> str:
    """Короткое время: 14с, 4м, 2ч10м."""
    if seconds is None:
        return "—"
    s = max(0, int(seconds))
    if s < 60:
        return f"{s}с"
    if s < 3600:
        return f"{s // 60}м"
    return f"{s // 3600}ч{s % 3600 // 60:02d}м"


SHORT_MODES = {"dev": "ДЕВ", "smart": "СМАРТ", "new_dev_wallet": "НОВЫЙ КОШЕЛЁК ДЕВА", "new": "НОВАЯ", "trend": "ТРЕНД",
               "copy": "КОПИЯ", "mydev": "МОЙ ДЕВ"}


def predicted(a: dict[str, Any]) -> float | None:
    """Прогноз чистого результата: у дева — расчёт потенциала, иначе — путь до цели от точки входа."""
    if a.get("net") is not None:
        return a["net"]
    entry = a.get("entry_mc") or a.get("mc")
    if a.get("target_mc") and entry:
        return a["target_mc"] / entry - 1
    return None


def shares_line(m: dict[str, Any], violations: list[str]) -> str:
    parts = []
    for key, name in (("cluster", "дев"), ("bundles", "бандл"), ("snipers", "снайп"), ("top10", "топ-10")):
        v = m.get(key)
        if v is not None:
            parts.append(("⚠" if key in violations else "") + f"{name} {v:.0%}")
    return "👥 " + " · ".join(parts) if parts else ""


def render_alert(a: dict[str, Any], sol_usd: float, compact: bool = False) -> str:
    """Главное — в первых двух строках: что, почём, где вход, какая цель и прогноз."""
    level, modes = a["level"], a["modes"]
    mode = "+".join(SHORT_MODES.get(m, m.upper()) for m in modes)
    head = (f"{ICONS[level]} <b>{mode} · ${esc(a.get('symbol') or '?')}</b> · MC {mc_usd(a['mc'], sol_usd)}"
            f" · {ago(a.get('age_s'))}" + (" · ⏳" if a.get("incomplete") else ""))
    pred = predicted(a)
    within = a.get("buy_within_s")
    entry = (f"🎯 вход ≤ {mc_usd(a.get('entry_max_mc'), sol_usd)}" + (f" (ещё {ago(within)})" if within else "")
             + f" → цель {mc_usd(a.get('target_mc'), sol_usd)}" + (f" ({pred:+.0%})" if pred is not None else ""))
    lines = [head, entry]
    prof = a.get("profile") or {}
    med = prof.get("medians") or {}
    ds = (prof.get("extra") or {}).get("dump_spread") or {}
    dump = ds.get("median") or med.get("dump_mc")
    if dump:
        lines.append(f"💀 сливает ~{mc_usd(dump, sol_usd)} через ~{ago(med.get('time_to_dump_s'))} · похожих "
                     f"{prof.get('similar_launches', 0)}/{prof.get('launches', 0)}")
    tr = a.get("trend") or {}
    if tr:
        lines.append(f"📈 5м {tr.get('pc_m5', 0):+.0f}% · 🟢{tr.get('buys', 0)} / 🔴{tr.get('sells', 0)} · "
                     f"{usd(tr.get('vol_m5'))} · ликв {usd(tr.get('liq'))}" + (" · буст" if tr.get("boosts") else ""))
    nc = a.get("newcoin") or {}
    if nc:
        lines.append(f"📈 покупателей {nc.get('buyers')} · объём {nc.get('volume', 0):.0f} SOL · "
                     f"покупки {nc.get('buy_share', 0):.0%}")
    for t in (a.get("traders") or [])[:2]:
        tags = ", ".join(map(str, (t.get("tags") or [])[:2]))
        lines.append(f"🧠 {esc(t.get('label') or short(t['address']))} купил {t.get('buy_sol', 0):.2f} SOL · "
                     f"копир {pct(t.get('copy_30d'))}" + (f" · {esc(tags)}" if tags else ""))
    sl = shares_line(a.get("metrics") or {}, a.get("violations") or [])
    smart = a.get("smart_in_coin") or 0
    ext_smart = a.get("smart_in_coin_ext")
    if smart or ext_smart:
        sl += f" · 🧠 {smart}" + (f"/{ext_smart}" if ext_smart is not None else "")
    if sl:
        lines.append(sl)
    if a.get("ext_line"):
        lines.append(esc(a["ext_line"]))
    if a.get("note"):
        lines.append(esc(a["note"]))
    lines.append(f"<code>{esc(a['mint'])}</code>")
    return "\n".join(lines)


def render_follow(kind: str, a: dict[str, Any], sol_usd: float, extra: str = "") -> str:
    sym = f"${esc(a.get('symbol') or '?')}"
    mc = mc_usd(a.get("mc"), sol_usd)
    texts = {
        "target": f"🎯 <b>ЦЕЛЬ</b> · {sym} · MC {mc} → фиксируй",
        "zone70": f"🟠 <b>зона дампа</b> · {sym} · MC {mc} → выходи",
        "emergency": f"🔴 <b>ВЫХОД</b> · {sym} · {extra or 'кластер продаёт'} · MC {mc}",
        "trader_sell": f"🔴 <b>ВЫХОД</b> · {sym} · {extra or 'трейдер продал'} >50% · MC {mc}",
        "trend_exit": f"🔴 <b>ВЫХОД</b> · {sym} · {extra or 'разворот'} · MC {mc}",
        "dev_buys": f"⚠️ дев докупает · {sym} · MC {mc}",
        "level_up": f"⬆️ уровень выше · {sym} · MC {mc}",
        "pre_migration": f"🚀 скоро миграция · {sym} · MC {mc}",
        "migrated": f"🔁 миграция · {sym} · MC {mc}",
    }
    return texts.get(kind, f"{kind} · {sym} · MC {mc}") + f"\n<code>{esc(a['mint'])}</code>"


EXIT_SHORT = {"target": "по цели", "emergency": "дев продал", "timeout": "60 мин", "trader_sell": "трейдер продал",
              "trend_exit": "разворот"}


def render_final(a: dict[str, Any], outcome: dict[str, Any], sol_usd: float) -> str:
    """Прогноз → факт одной строкой."""
    res = outcome.get("net_result")
    pred = predicted(a)
    icon = "✅" if (res or 0) > 0 else "❌"
    entry, exit_mc = outcome.get("entry_mc"), outcome.get("exit_mc")
    path = (f"{mc_usd(entry, sol_usd)} → {mc_usd(exit_mc, sol_usd)}, " if entry and exit_mc else "")
    return (f"{icon} <b>${esc(a.get('symbol') or '?')}</b>  прогноз {pct(pred)} → <b>факт {pct(res)}</b>  "
            f"({path}{EXIT_SHORT.get(outcome.get('exit_reason'), outcome.get('exit_reason') or '')})"
            f"\n<code>{esc(a['mint'])}</code>")


def render_full_line(t: dict[str, Any], sol_usd: float) -> str:
    side = "🟢 BUY" if t["is_buy"] else "🔴 SELL"
    return (f"{side} {esc(t.get('label') or short(t['trader']))} {t['sol']:.2f} SOL · "
            f"${esc(t.get('symbol') or '?')} · MC {mc_usd(t['mc'], sol_usd)} · <code>{esc(t['mint'])}</code>")
