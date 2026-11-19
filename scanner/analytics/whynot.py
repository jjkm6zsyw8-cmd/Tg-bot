"""Почему кошелёк не копируется: разбор его покупок за N дней по своей базе.

Зачем (19.11): у кошельков с лучшей копируемостью по своей базе (32FE +52%, BojC +52%, CDXG +38%) за неделю нет ни
одной копии в бумаге, а в журнале копий их нет совсем: свои кошельки, которые бот ведёт как smart, и smart-money шли
через рамки smart, и отказы там не записывались. Здесь — по каждой первой покупке монеты за N дней: есть ли по этому
кошельку бумажная позиция (любой портфель) в пределах 3 минут, а если нет — почему: запись в журнале копий (с 19.11
туда пишутся и отказы smart), кошелёк ещё не был в списке, или рамка, которая её отсекла бы (размер покупки,
капитализация — то, что видно по самой сделке). Остальное — «прошла рамки, но не скопирована» (фильтры безопасности,
внешняя проверка RugCheck, отказ бумаги — это видно в журнале копий и /paper).

Сервер: docker compose run --rm bot whynot [--days 7] [--no-send] — итог в Telegram (тема system).
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from .copytrade import OWN_SMART_MIN_HOLD_S
from .patterns import split

TOP = 20                 # кошельков в разборе (по копируемости)
MIN_TRADES = 10          # сделок за 30 дней по своей базе
MATCH_S = 240            # бумажная позиция по кошельку в пределах 4 минут после его покупки — его копия
COPIED = "скопирована"


def short(a: str) -> str:
    return f"{a[:4]}…{a[-4:]}"


def first_clause(reason: str | None) -> str:
    """Причина из журнала без чисел конкретной сделки — чтобы одинаковые причины складывались."""
    text = (reason or "без причины").split(";")[0].split(":")[0].strip()
    return text[:60]


def classify(buy: dict[str, Any], w: dict[str, Any], cfg: dict[str, Any], copied: bool, logged: tuple[str, str] | None,
             listed_at: datetime | None) -> str:
    """Что стало с покупкой кошелька: скопирована или почему нет (одной фразой, для подсчёта)."""
    if copied:
        return COPIED
    if logged is not None:
        action, reason = logged
        if action == "skipped":
            return "пропущена: " + first_clause(reason)
        return "в журнале «скопирована», но бумага не вошла"
    if listed_at is not None and buy["ts"] < listed_at:
        return "кошелёк ещё не был в списке"
    sol, mc = float(buy["sol_amount"] or 0), float(buy["mc_sol"] or 0)
    hold = float(w.get("median_hold_s") or 0)
    need = float(cfg.get("own_smart_min_hold_s", OWN_SMART_MIN_HOLD_S))
    smart_path = not w["manual"] or (hold >= need and float(w.get("copy_30d") or 0) > 0)
    if smart_path:
        min_buy = float(cfg.get("smart_min_buy_sol", 0.5))
        lo, hi = float(cfg.get("smart_mc_min_sol", 0)), float(cfg.get("smart_mc_max_sol", 1e9))
        if sol < min_buy:
            return f"покупка меньше порога smart {min_buy:g} SOL"
        if not lo <= mc <= hi:
            return f"капитализация вне рамки smart {lo:g}–{hi:g} SOL"
        return "прошла рамки smart, но не скопирована (фильтры, проверка, бумага)"
    min_buy = float(cfg.get("copy_min_buy_sol", 0.1))
    if sol < min_buy:
        return f"мелкая покупка (меньше {min_buy:g} SOL)"
    return "не скопирована, причина не записана"


async def candidates(db: Any, min_trades: int = MIN_TRADES, top: int = TOP) -> list[dict[str, Any]]:
    """Кошельки с копируемостью за 30 дней в плюсе: свои, smart-money и кандидаты разведки — лучшие первыми."""
    rows = await db.fetch(
        """SELECT t.address, t.copy_30d, t.trades_30d, t.median_hold_s, t.active,
                  (t.manual OR w.address IS NOT NULL) AS manual, w.created_at AS listed_at, t.qualified_at,
                  s.status AS scout_status, s.started_at, s.decided_at
           FROM traders t
           LEFT JOIN watchlist w ON w.kind = 'trader' AND w.address = t.address
           LEFT JOIN scout s ON s.address = t.address
           WHERE t.copy_30d > 0 AND t.trades_30d >= $1
             AND (t.manual OR t.active OR w.address IS NOT NULL OR s.address IS NOT NULL)
           ORDER BY t.copy_30d DESC LIMIT $2""", min_trades, top)
    return [dict(r) for r in rows]


def kind_of(w: dict[str, Any]) -> str:
    if w["manual"]:
        return "свой"
    if w["active"]:
        return "smart-money"
    return "разведка"


async def analyze(db: Any, w: dict[str, Any], cfg: dict[str, Any], since: datetime) -> dict[str, Any]:
    addr = w["address"]
    buys = await db.fetch(
        """SELECT DISTINCT ON (mint) mint, ts, sol_amount, mc_sol FROM trades
           WHERE trader = $1 AND is_buy AND ts >= $2 ORDER BY mint, ts""", addr, since)
    pos: dict[str, list[datetime]] = {}
    for p in await db.fetch("SELECT mint, opened_at FROM paper_positions WHERE source_wallet = $1 AND opened_at >= $2",
                            addr, since):
        pos.setdefault(p["mint"], []).append(p["opened_at"])
    logs = {r["mint"]: (r["action"], r["reason"]) for r in await db.fetch(
        """SELECT DISTINCT ON (mint) mint, action, reason FROM copy_log
           WHERE wallet = $1 AND side = 'buy' AND ts >= $2 ORDER BY mint, ts""", addr, since)}
    if w["manual"]:
        listed = w.get("listed_at")
    elif w["active"]:
        listed = w.get("qualified_at")
    else:
        listed = w.get("started_at")
    why: Counter[str] = Counter()
    for b in buys:
        copied = any(0 <= (t - b["ts"]).total_seconds() <= MATCH_S for t in pos.get(b["mint"], []))
        why[classify(dict(b), w, cfg, copied, logs.get(b["mint"]), listed)] += 1
    return {"buys": len(buys), "copied": why.pop(COPIED, 0), "why": why}


def hold_text(s: float | None) -> str:
    if not s:
        return "—"
    return f"~{s / 60:.0f} мин" if s >= 90 else f"~{s:.0f} с"


async def run(db: Any, cfg: dict[str, Any], days: float = 7.0, send: bool = True, now: datetime | None = None,
              top: int = TOP) -> str:
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=days)
    ws = await candidates(db, top=top)
    lines = [f"<b>Почему не копируются лучшие кошельки</b> — покупки за {days:g} дн. по своей базе",
             "Кошельки с копируемостью за 30 дней в плюсе (повтор их сделок с задержкой 5 с и комиссиями), лучшие "
             "первыми. «Покупок» — первые покупки монет; «скопировано» — есть бумажная позиция по этому кошельку.", ""]
    total: Counter[str] = Counter()
    n_buys = n_copied = 0
    for w in ws:
        a = await analyze(db, w, cfg, since)
        n_buys += a["buys"]
        n_copied += a["copied"]
        total.update(a["why"])
        head = (f"• <code>{short(w['address'])}</code> · {kind_of(w)} · копируемость {float(w['copy_30d']):+.0%}, "
                f"держит {hold_text(w.get('median_hold_s'))}: покупок {a['buys']}, скопировано {a['copied']}")
        if a["why"]:
            head += ". Почему нет: " + "; ".join(f"{k} — {v}" for k, v in a["why"].most_common(4))
        lines.append(head)
    if not ws:
        lines.append("Кошельков с плюсовой копируемостью нет.")
    lines += ["", f"<b>Итого</b>: покупок {n_buys}, скопировано {n_copied}."]
    if total:
        lines.append("Главные причины: " + "; ".join(f"{k} — {v}" for k, v in total.most_common(6)) + ".")
    lines.append("С 19.11 отказы smart по своим кошелькам и кошелькам с копируемостью от 10% пишутся в журнал копий "
                 "(/copylog), а такие покупки копируются в бумагу H (без денег).")
    text = "\n".join(lines)
    if send:
        for part in split(text):
            await db.system(part)
    return text
