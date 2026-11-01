"""Разбор дня (разд. 5 ТЗ): итог сигналов, упущенные возможности, лучшие окна входа,
новые кандидаты, сделки пользователя. Короткая сводка в Telegram + CSV-файлом."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..alerts.format import esc, mc_usd, pct
from ..analytics.copytrade import round_trips
from ..analytics.scoring import REASONS
from ..config import Settings
from ..db.pool import Database
from ..solana import short
from .sim import SimTrade, best_windows, series, utc_day_bounds

MISSED_REASONS = {**REASONS, "": "нет решения (не оценивалась)", "alerted": "алерт был",
                  "outage": "сбой источника", "latency": "задержка"}


async def build_daily(db: Database, settings: Settings, day: date) -> tuple[str, bytes, list[list[dict[str, Any]]]]:
    cfg = settings.effective()
    tz = ZoneInfo(cfg.get("timezone", "UTC"))
    start, end = utc_day_bounds(day, tz)
    price = float((await db.kv_get("sol_usd", {}) or {}).get("price", 150))
    thr = float(cfg.get("min_net_profit", 0.3))

    alerts = await db.fetch(
        """SELECT a.*, o.net_result, o.achievable_profit, o.exit_reason, o.max_mc,
                  (SELECT mark FROM user_marks m WHERE m.alert_id=a.id ORDER BY m.id DESC LIMIT 1) user_mark,
                  t.symbol
           FROM alerts a LEFT JOIN alert_outcomes o ON o.alert_id=a.id LEFT JOIN tokens t ON t.mint=a.mint
           WHERE a.created_at >= $1 AND a.created_at < $2 ORDER BY a.created_at""", start, end)
    lines = [f"📅 <b>Разбор дня {day.strftime('%d.%m.%Y')}</b>"]

    # 1) итог сигналов
    lines.append("\n<b>Итог сигналов</b> (вход +{} с, выход по цели или экстренному сигналу)".format(
        cfg.get("reaction_s", 5)))
    sims: dict[str, list[SimTrade]] = {}
    for a in alerts:
        if a["net_result"] is None:
            continue
        p = a["payload"] or {}
        sims.setdefault(a["mode"], []).append(SimTrade(
            mint=a["mint"], mode=a["mode"], alerted=True, reason="alerted", t_alert=a["created_at"].timestamp(),
            age_s=float(p.get("age_s") or 0), mc_alert=float(a["mc_alert"] or 0), net=a["net_result"]))
    all_s = series([s for v in sims.values() for s in v])
    if all_s["n"]:
        lines.append(f"Алертов {len(alerts)}, закрыто {all_s['n']}, удачных {all_s['wins']} ({all_s['win_rate']:.0%}), "
                     f"средний плюс {pct(all_s['avg_win'])}, средний минус {pct(-all_s['avg_loss'])}, "
                     f"<b>итог серии {pct(all_s['series'])}</b> на сделку")
        for mode, items in sims.items():
            s = series(items)
            lines.append(f"  · {mode}: {s['n']} сделок, итог {pct(s['series'])}")
    else:
        lines.append(f"Алертов {len(alerts)}, закрытых с итогом пока нет.")

    # 2) упущенные возможности
    launches = await db.fetch(
        """SELECT l.*, t.symbol, t.received_at, EXISTS(SELECT 1 FROM alerts a WHERE a.mint=l.mint) alerted
           FROM launches l JOIN tokens t ON t.mint=l.mint WHERE l.created_at >= $1 AND l.created_at < $2""",
        start, end)
    gaps = await db.fetch("SELECT started_at, COALESCE(ended_at, now()) e FROM ingest_gaps "
                          "WHERE started_at < $2 AND COALESCE(ended_at, now()) > $1", start, end)
    missed, outage_missed = [], []
    for l in launches:
        if l["alerted"] or l["achievable_profit"] is None or l["achievable_profit"] <= thr:
            continue
        reason = (l["features"] or {}).get("decision_reason") or ""
        out = any(g["started_at"] <= l["created_at"] <= g["e"] for g in gaps) or (
            l["received_at"] and (l["received_at"] - l["created_at"]).total_seconds() > 10)
        if out:
            outage_missed.append(l)
        else:
            missed.append((l, reason))
    lines.append(f"\n<b>Упущенные возможности</b>: {len(missed)} (+{len(outage_missed)} из-за сбоя источника, "
                 "не входят в статистику)")
    by_reason: dict[str, int] = {}
    for _l, r in missed:
        by_reason[r] = by_reason.get(r, 0) + 1
    if by_reason:
        lines.append("Причины: " + ", ".join(f"{MISSED_REASONS.get(k, k)} — {v}"
                                             for k, v in sorted(by_reason.items(), key=lambda x: -x[1])))
    for l, r in sorted(missed, key=lambda x: -(x[0]["achievable_profit"] or 0))[:5]:
        lines.append(f"  · ${esc(l['symbol'] or '?')} {pct(l['achievable_profit'])} — {MISSED_REASONS.get(r, r)} "
                     f"<code>{l['mint']}</code>")

    # 3) лучшие окна входа
    windows = []
    for mode, items in sims.items():
        for w in best_windows(items, price):
            windows.append(f"  · {mode}, {w}")
    dev_launch_sims = [SimTrade(mint=l["mint"], mode="dev", alerted=True, reason="", t_alert=0,
                                age_s=float(cfg.get("reaction_s", 5)) + 1.5, mc_alert=float(l["entry_mc"] or 0),
                                net=l["achievable_profit"]) for l in launches if l["achievable_profit"] is not None]
    for w in best_windows(dev_launch_sims, price):
        windows.append(f"  · все запуски (достижимый профит), {w}")
    if windows:
        lines.append("\n<b>Лучшие окна входа</b>")
        lines += windows

    # 4) новые кандидаты
    kb: list[list[dict[str, Any]]] = []
    new_cl = await db.fetch("SELECT id, profile FROM clusters WHERE qualified_at >= $1 AND qualified_at < $2 "
                            "ORDER BY qualified_at LIMIT 10", start, end)
    new_tr = await db.fetch("SELECT address, label, copy_30d, win_rate FROM traders "
                            "WHERE qualified_at >= $1 AND qualified_at < $2 LIMIT 10", start, end)
    if new_cl or new_tr:
        lines.append("\n<b>Новые кандидаты</b>")
        for c in new_cl:
            p = c["profile"] or {}
            lines.append(f"  · дев #{c['id']}: {p.get('launches')} запусков, повторяемость "
                         f"{pct(p.get('repeatability'), False)}, типичный дамп "
                         f"{mc_usd((p.get('medians') or {}).get('dump_mc'), price)}")
            kb.append([{"text": f"Отслеживать дева #{c['id']}", "callback": f"cand:c:{c['id']}"}])
        for t in new_tr:
            lines.append(f"  · трейдер {short(t['address'])}: копируемость {pct(t['copy_30d'])}, "
                         f"удачных {pct(t['win_rate'], False)}")
            kb.append([{"text": f"Отслеживать {short(t['address'])}", "callback": f"cand:t:{t['address']}"}])

    # 5) сделки пользователя
    wallet = (await db.get_overrides()).get("trading_wallet")
    if wallet:
        rows = await db.fetch(
            "SELECT t.*, k.created_slot FROM trades t LEFT JOIN tokens k ON k.mint=t.mint "
            "WHERE t.trader=$1 AND t.ts >= $2 AND t.ts < $3 ORDER BY t.ts", wallet, start, end)
        trips = round_trips([{"mint": r["mint"], "ts": r["ts"].timestamp(), "is_buy": r["is_buy"],
                              "sol": r["sol_amount"], "tokens": r["token_amount"], "slot": r["slot"],
                              "mc": r["mc_sol"]} for r in rows], {r["mint"]: r["created_slot"] for r in rows})
        alerted = {a["mint"]: a for a in alerts}
        lines.append(f"\n<b>Ваши сделки</b> ({short(wallet)}): {len(trips)} закрытых, "
                     f"PnL {sum(t.own_pnl for t in trips):+.2f} SOL")
        by_alert = [t for t in trips if t.mint in alerted]
        if by_alert:
            pnl = sum(t.own_pnl for t in by_alert)
            delays = [t.entry_ts - alerted[t.mint]["created_at"].timestamp() for t in by_alert]
            lines.append(f"  по алертам бота: {len(by_alert)} сделок, PnL {pnl:+.2f} SOL, "
                         f"средняя задержка входа {sum(delays) / len(delays):.0f} с")
        other = len(trips) - len(by_alert)
        if other:
            lines.append(f"  без алерта: {other} сделок, PnL {sum(t.own_pnl for t in trips if t.mint not in alerted):+.2f} SOL")

    # CSV
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["mint", "symbol", "created_at", "cluster_id", "initial_mc_sol", "peak_mc_sol", "dump_mc_sol",
                "time_to_dump_s", "achievable_profit", "alerted", "decision_reason", "alert_level", "alert_net"])
    alert_by_mint = {a["mint"]: a for a in alerts}
    for l in launches:
        a = alert_by_mint.get(l["mint"])
        w.writerow([l["mint"], l["symbol"], l["created_at"].astimezone(tz).isoformat(), l["cluster_id"],
                    _r(l["initial_mc"]), _r(l["peak_mc"]), _r(l["dump_mc"]), _r(l["time_to_dump_s"]),
                    _r(l["achievable_profit"]), bool(a), (l["features"] or {}).get("decision_reason", ""),
                    a["level"] if a else "", _r(a["net_result"]) if a else ""])
    return "\n".join(lines), buf.getvalue().encode("utf-8-sig"), kb


def _r(v: Any) -> Any:
    return round(v, 4) if isinstance(v, float) else v


async def send_daily(db: Database, settings: Settings, day: date) -> None:
    text, csv_bytes, kb = await build_daily(db, settings, day)
    await db.enqueue("daily", {"text": text, "keyboard": kb or None,
                               "csv": csv_bytes.decode("utf-8"), "filename": f"day_{day.isoformat()}.csv"},
                     priority=5)


def today_in(tz_name: str) -> date:
    return datetime.now(ZoneInfo(tz_name)).date()

