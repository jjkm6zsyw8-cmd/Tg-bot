"""Доска кошельков (21.11, просьба владельца): все кошельки своего списка и smart-money — без груды текста.

Три секции по рейтингу копий (analytics/rating.py: последние 20 копий на бумаге G/H, «стянутый» к нулю средний
результат): 🟢 лучшие (топ и норм), 🟡 средние (новые — копий меньше 3), 🔴 на вылет (слабые и на паузе). В секции —
до 20 кошельков, каждый одной кнопкой: имя · копий в плюсе · итог копий при $10 · последняя сделка. Нажатие —
карточка: его сделки (удачных, сегодня, последняя, удержание), его итог с добавления, копии на бумаге и сколько дал
бы твой кошелёк, и кнопки «👻 тень за ним» / «💵 в бой за ним» / «не торговать» / «убрать из списка».

«За ним» — kv real:follow {адрес: shadow|live}: реальный бот берёт его покупки, даже если метод сигнала выключен
(scanner/real/executor.py). «Бой» — только у кошельков в режиме «бой» (REAL_LIVE, ключ), с подтверждением; остальные
считают такие сделки в тени. Все защиты боя (ставка, лимиты, стопы) действуют как обычно.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..alerts import format as fmt
from ..alerts.format import esc
from ..real import settings as S

FOLLOW_KEY = "real:follow"
SECTIONS = {"best": "🟢 Лучшие", "mid": "🟡 Средние", "bad": "🔴 На вылет"}
TIER_ICON = {"top": "🟢", "ok": "🟡", "new": "⚪", "weak": "🟠", "paused": "🔴"}
TIER_NAME = {"top": "топ", "ok": "норм", "new": "новый", "weak": "слабый", "paused": "пауза"}
FOLLOW_NAME = {"shadow": "👻 тень за ним", "live": "💵 бой за ним"}
COPY_USD = 10.0
LIMIT = 20


def short(a: str) -> str:
    return f"{a[:4]}…{a[-4:]}"


def section_of(tier: str | None) -> str:
    if tier in ("top", "ok"):
        return "best"
    if tier in ("weak", "paused"):
        return "bad"
    return "mid"


def money(usd: float) -> str:
    return f"{'+' if usd >= 0 else '−'}${abs(usd):.0f}" if abs(usd) >= 10 else f"{'+' if usd >= 0 else '−'}${abs(usd):.2f}"


async def wallets(db: Any) -> list[dict[str, Any]]:
    """Кошельки своего списка и smart-money с цифрами для доски и карточки."""
    rows = await db.fetch(
        """WITH w AS (
               SELECT address, label, created_at AS added FROM watchlist WHERE kind = 'trader'
               UNION ALL
               SELECT address, label, COALESCE(qualified_at, updated_at) FROM traders
               WHERE active AND address NOT IN (SELECT address FROM watchlist WHERE kind = 'trader'))
           SELECT w.address, COALESCE(w.label, t.label) AS label, w.added, t.win_rate, t.trades_30d, t.median_hold_s,
                  t.copy_30d, t.copy_t_30d, t.rules_30d, t.best_rule, r.tier, r.manual_tier, r.copies, r.wins
           FROM w LEFT JOIN traders t ON t.address = w.address LEFT JOIN wallet_rating r ON r.address = w.address""")
    out = {r["address"]: {**dict(r), "last": None, "today": 0, "own_pnl": None, "n": 0, "plus": 0, "sum_pct": 0.0}
           for r in rows}
    if not out:
        return []
    addrs = list(out)
    day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    for r in await db.fetch(
            """SELECT t.trader, max(t.ts) AS last, count(*) FILTER (WHERE t.ts >= $2) AS today,
                      sum(CASE WHEN t.is_buy THEN -t.sol_amount ELSE t.sol_amount END) FILTER (WHERE t.ts >= a.added) pnl
               FROM trades t JOIN unnest($1::text[], $3::timestamptz[]) AS a(addr, added) ON a.addr = t.trader
               WHERE t.ts > now() - interval '30 days' GROUP BY t.trader""",
            addrs, day, [out[a]["added"] for a in addrs]):
        out[r["trader"]].update(last=r["last"], today=int(r["today"] or 0),
                                own_pnl=float(r["pnl"]) if r["pnl"] is not None else None)
    for r in await db.fetch(
            """SELECT DISTINCT ON (source_wallet, mint) source_wallet, pnl_pct FROM paper_positions
               WHERE source_wallet = ANY($1::text[]) AND closed_at IS NOT NULL AND NOT suspect
                 AND exit_reason IS DISTINCT FROM 'reset' AND opened_at > now() - interval '30 days'
               ORDER BY source_wallet, mint, id""", addrs):
        w = out[r["source_wallet"]]
        pct = float(r["pnl_pct"] or 0)
        w["n"] += 1
        w["plus"] += pct > 0
        w["sum_pct"] += pct
    for r in await db.fetch(
            """SELECT source_wallet, count(*) n, count(*) FILTER (WHERE pnl_pct > 0) w, COALESCE(sum(pnl_pct), 0) s
               FROM paper_positions WHERE source_wallet = ANY($1::text[]) AND portfolio = 'H' AND rule = 'hold'
                 AND closed_at IS NOT NULL AND NOT suspect AND exit_reason IS DISTINCT FROM 'reset'
               GROUP BY 1""", addrs):
        out[r["source_wallet"]]["hold"] = (int(r["n"]), int(r["w"]), float(r["s"]))
    for w in out.values():
        w["tier"] = w.get("manual_tier") or w.get("tier") or "new"
        w["section"] = section_of(w["tier"])
        rules = w.get("rules_30d") or {}
        if isinstance(rules, str):
            import json
            rules = json.loads(rules)
        w["rules_30d"] = rules
        # 21.11: его сделки через все выходы — главнее рейтинга по копиям одного правила: устойчивый плюс по
        # лучшему выходу — «лучшие»; все выходы в минусе на 8+ сделках — «на вылет»
        if w.get("best_rule") and not w.get("manual_tier"):
            w["section"] = "best"
        elif rules and not w.get("manual_tier") and all(r["avg"] <= 0 for r in rules.values()) \
                and max(r["n"] for r in rules.values()) >= 8:
            w["section"] = "bad"
        w["copy_usd"] = w["sum_pct"] * COPY_USD
    return list(out.values())


def rules_line(w: dict[str, Any]) -> str:
    """Его сделки через все выходы: за ним / A / B / G / R / W — средний результат копии и сколько сделок."""
    from ..analytics.wallet_rules import RULE_NAMES, RULES
    rules = w.get("rules_30d") or {}
    if not rules:
        return "Выходы: ещё не посчитаны (раз в 6 ч и при добавлении)."
    parts = []
    for k in RULES:
        r = rules.get(k)
        if r:
            star = "★" if k == w.get("best_rule") else ""
            parts.append(f"{star}{RULE_NAMES[k]} {r['avg']:+.0%}")
    n = max(r["n"] for r in rules.values())
    best = w.get("best_rule")
    tail = (f" — лучший: <b>{RULE_NAMES[best]}</b>" if best else " — устойчивого плюса нет")
    return f"Выходы ({n} его сделок, вход через ~1 с, как бот): " + " · ".join(parts) + tail


def button(w: dict[str, Any]) -> dict[str, Any]:
    name = (w.get("label") or short(w["address"]))[:14]
    rate = f"{w['plus'] / w['n']:.0%}" if w["n"] else "—"
    last = fmt.ago((datetime.now(timezone.utc) - w["last"]).total_seconds()) if w.get("last") else "—"
    return {"text": f"{TIER_ICON.get(w['tier'], '⚪')} {name} · {rate} · {money(w['copy_usd'])} · {last}",
            "callback": f"bd:w:{w['address']}"}


async def board_view(db: Any, section: str = "best") -> tuple[str, list[list[dict[str, Any]]]]:
    section = section if section in SECTIONS else "best"
    ws = await wallets(db)
    cnt = {s: sum(1 for w in ws if w["section"] == s) for s in SECTIONS}
    rows = [w for w in ws if w["section"] == section]
    if section == "bad":
        rows.sort(key=lambda w: w["copy_usd"])
    elif section == "mid":
        # новые — без копий на бумаге: первыми те, у кого копия по истории лучше (6d7y +21%, 7HcK +7% и т. п.)
        rows.sort(key=lambda w: (w.get("copy_30d") is None, -float(w.get("copy_30d") or 0), -(w["today"] or 0)))
    else:
        rows.sort(key=lambda w: -w["copy_usd"])
    text = (f"🏆 <b>Доска кошельков</b> · {SECTIONS[section]} — {len(rows)} из {len(ws)}\n"
            f"Кнопка: кошелёк · копий в плюсе · итог копий при ${COPY_USD:g} · последняя сделка. Нажми — карточка. "
            "Найти любой: /card начало адреса (например /card 6d7y)."
            + (" Средние — первыми лучшие по копируемости." if section == "mid" else "")
            + ("" if rows else "\n\nВ этой секции пока никого."))
    kb: list[list[dict[str, Any]]] = [[{"text": ("● " if s == section else "") + f"{name} ({cnt[s]})",
                                        "callback": f"bd:s:{s}"} for s, name in SECTIONS.items()]]
    kb += [[button(w)] for w in rows[:LIMIT]]
    kb.append([{"text": "🔄 Обновить", "callback": f"bd:s:{section}"}])
    return text, kb


async def find(db: Any, part: str) -> list[dict[str, Any]]:
    """Кошельки доски, чей адрес начинается с part (или содержит его), либо метка совпадает."""
    p = part.strip().lower()
    ws = await wallets(db)
    hit = [w for w in ws if w["address"].lower().startswith(p)]
    return hit or [w for w in ws if p in w["address"].lower() or p in str(w.get("label") or "").lower()]


async def card_view(db: Any, cfg: dict[str, Any], addr: str) -> tuple[str, list[list[dict[str, Any]]]]:
    w = next((x for x in await wallets(db) if x["address"] == addr), None)
    if w is None:
        return "Кошелька нет в списке (убран).", [[{"text": "← Доска", "callback": "bd:s:best"}]]
    follow = (await db.kv_get(FOLLOW_KEY) or {}).get(addr)
    real = await db.fetch(
        """SELECT mode, count(*) n, count(*) FILTER (WHERE pnl_sol > 0) w, COALESCE(avg(pnl_pct), 0) a,
                  COALESCE(sum(pnl_sol), 0) s
           FROM real_positions WHERE trader = $1 AND closed_at IS NOT NULL
             AND closed_at > now() - interval '14 days' GROUP BY mode""", addr)
    sol_usd = float(((await db.kv_get("sol_usd", {})) or {}).get("price") or 150)
    now = datetime.now(timezone.utc)
    last = f"{fmt.ago((now - w['last']).total_seconds())} назад" if w.get("last") else "нет в базе"
    hold = fmt.ago(w["median_hold_s"]) if w.get("median_hold_s") else "—"
    wr = f"{float(w['win_rate']):.0%}" if w.get("win_rate") is not None else "—"
    stake = float(S.get(cfg, "real_stake_usd"))
    added = w["added"].strftime("%d.%m") if w.get("added") else "—"
    lines = [f"{TIER_ICON.get(w['tier'], '⚪')} <b>{esc(w.get('label') or short(addr))}</b> · "
             f"{TIER_NAME.get(w['tier'], w['tier'])}",
             f"<code>{addr}</code>",
             f"Его сделки: удачных {wr} · сегодня {w['today']} · последняя {last} · держит ~{hold}",
             "Его итог с добавления (" + added + "): "
             + (f"{w['own_pnl']:+.2f} SOL (грубо: незакрытые — в минусе)" if w.get("own_pnl") is not None
                else "сделок в базе нет"),
             (f"Копии на бумаге (${COPY_USD:g}): {w['n']} сд., в плюсе {w['plus']} ({w['plus'] / w['n']:.0%}), итог "
              f"<b>{money(w['copy_usd'])}</b> · твой кошелёк (ставка ${stake:g}) ≈ {money(w['sum_pct'] * stake)}")
             if w["n"] else "Копий на бумаге пока нет.",
             (f"   из них «за трейдером» (продаёт, когда он): {w['hold'][0]} сд., в плюсе {w['hold'][1]}, итог "
              f"{money(w['hold'][2] * COPY_USD)}" if w.get("hold") else
              ("   копии держателя теперь идут «за трейдером» (продаёт, когда он), а не по G"
               if float(w.get("median_hold_s") or 0) >= 180 else "")),
             rules_line(w),
             "Копируемость 30 д: " + (f"{float(w['copy_30d']):+.0%} на сделку" if w.get("copy_30d") is not None else "—")
             + (f", устойчивость t={float(w['copy_t_30d']):.1f} (от 1.645 — не случайность)"
                if w.get("copy_t_30d") is not None else ""),
             f"Реальный бот за ним: {FOLLOW_NAME.get(follow, '⚪ нет')}"]
    for r in real:                       # 22.11: подтверждает ли бумагу реальный бот (тень — настоящие транзакции)
        lines.append(f"   {'тень' if r['mode'] == 'shadow' else 'бой'} за 14 дн: {r['n']} сд., в плюсе {r['w']}, "
                     f"в среднем {float(r['a']):+.1%}, итог {money(float(r['s']) * sol_usd)}")
    kb = [[{"text": ("● " if follow == "shadow" else "") + "👻 Тень за ним", "callback": f"bd:f:{addr}:shadow"},
           {"text": ("● " if follow == "live" else "") + "💵 В бой за ним…", "callback": f"bd:f:{addr}:live"}],
          [{"text": ("● " if not follow else "") + "⚪ Не торговать за ним", "callback": f"bd:f:{addr}:off"}],
          [{"text": "GMGN ↗", "url": f"https://gmgn.ai/sol/address/{addr}"},
           {"text": "🗑 Убрать из списка…", "callback": f"bd:x:{addr}"}],
          [{"text": "← Доска", "callback": f"bd:s:{w['section']}"}]]
    return "\n".join(x for x in lines if x), kb


def live_confirm(cfg: dict[str, Any], addr: str) -> tuple[str, list[list[dict[str, Any]]]]:
    stake = float(S.get(cfg, "real_stake_usd"))
    text = (f"💵 <b>Торговать за {short(addr)} на реальные деньги?</b>\n"
            f"Каждая его покупка → покупка ботом на ставку кошелька (${stake:g}, у метода может быть своя), выход — по "
            "правилу метода (копии — G, smart — A). Действуют лимит позиций, дневной и общий стоп.\n"
            "Только у кошельков в режиме «бой» (REAL_LIVE и ключ на сервере); остальные считают это в тени.")
    return text, [[{"text": "Да, в бой за ним", "callback": f"bd:f:{addr}:live!", "style": "danger"},
                   {"text": "Нет", "callback": f"bd:w:{addr}"}]]


async def set_follow(db: Any, addr: str, mode: str | None) -> None:
    cur = dict(await db.kv_get(FOLLOW_KEY) or {})
    if mode:
        cur[addr] = mode
    else:
        cur.pop(addr, None)
    await db.kv_set(FOLLOW_KEY, cur)
