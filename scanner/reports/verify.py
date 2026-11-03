"""Блок F: проверка на истории — пересчёт за день и за N дней.

    python -m scanner verify --day 2026-11-03 --days 7

Считает: сколько токенов с данными о сделках, сколько профилей с 3+ запусками, сколько было бы алертов и
итог серии (вход +5 с, комиссии и проскальзывание, профиль только по прошлым запускам), воронку отсева.
Пишет расширенный CSV: число сделок, число продаж кластера, причина склейки кластера, окно входа, лучшие
трейдеры.
"""
from __future__ import annotations

import csv
import io
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any

from ..analytics.curve import Costs
from ..analytics.scoring import REASONS
from ..config import Settings
from ..db.pool import Database
from .sim import prepare_dev, run_dev, series


async def verify_period(db: Database, settings: Settings, start: datetime, end: datetime) -> dict[str, Any]:
    cfg = settings.effective()
    costs = Costs.from_config(settings.costs())
    tokens = await db.fetchval("SELECT count(*) FROM tokens WHERE created_at >= $1 AND created_at < $2", start, end)
    with_trades = await db.fetchval(
        "SELECT count(*) FROM launches WHERE created_at >= $1 AND created_at < $2 AND COALESCE(trades_count, 0) > 0",
        start, end)
    with_trades_raw = await db.fetchval(
        "SELECT count(DISTINCT t.mint) FROM trades t JOIN tokens k ON k.mint = t.mint "
        "WHERE k.created_at >= $1 AND k.created_at < $2 AND t.ts > k.created_at + interval '2 seconds'", start, end)
    prepared = await prepare_dev(db, cfg, start, end)
    profiled = sum(1 for p in prepared if p.profile is not None)
    res = run_dev(prepared, cfg, costs, latency_s=1.5)
    alerts = [r for r in res if r.alerted and not r.outage]
    funnel = Counter(r.reason for r in res)
    profiles_now = await db.fetchval(
        "SELECT count(*) FROM clusters WHERE (profile->>'launches')::int >= 3 AND NOT suspicious")
    return {"tokens": tokens, "with_trades": max(with_trades or 0, with_trades_raw or 0), "launches": len(prepared),
            "with_profile": profiled, "profiles_now": profiles_now, "alerts": len(alerts), "series": series(res),
            "funnel": funnel, "outage": sum(1 for r in res if r.outage), "results": res}


def _fmt(name: str, v: dict[str, Any]) -> list[str]:
    s = v["series"]
    lines = [f"<b>{name}</b>",
             f"Токенов {v['tokens']}, с данными о сделках {v['with_trades']} "
             f"({v['with_trades'] / v['tokens']:.1%})" if v["tokens"] else "Токенов 0",
             f"Запусков в launches {v['launches']}, с профилем кластера (3+ прошлых запуска со сделками) "
             f"{v['with_profile']}; профилей сейчас {v['profiles_now']}",
             f"Было бы алертов «Девы»: {v['alerts']}" + (
                 f", удачных {s['wins']} ({s['win_rate']:.0%}), +{s['avg_win']:.0%}/−{s['avg_loss']:.0%}, "
                 f"итог серии {s['series']:+.1%} на сделку" if s["n"] else ""),
             "Воронка: " + ", ".join(f"{REASONS.get(k, 'алерт' if k == 'alerted' else k)} {n}"
                                     for k, n in v["funnel"].most_common())]
    if v["outage"]:
        lines.append(f"Сбой источника: {v['outage']} запусков (не в статистике)")
    return lines


async def extended_csv(db: Database, start: datetime, end: datetime, sim: dict[str, Any] | None = None) -> bytes:
    rows = await db.fetch(
        """SELECT l.*, t.symbol, t.identity, t.deployer AS tdeployer, c.profile, c.suspicious,
                  (SELECT reason || ' (' || confidence || ')' FROM cluster_links cl
                   WHERE cl.cluster_id = l.cluster_id AND cl.wallet = COALESCE(t.identity, t.deployer) LIMIT 1) glue,
                  (SELECT string_agg(left(trader, 6) || ':' || round(pnl_sol::numeric, 2), ' ' ORDER BY rank)
                   FROM coin_top_traders tt WHERE tt.mint = l.mint) tops
           FROM launches l JOIN tokens t ON t.mint = l.mint LEFT JOIN clusters c ON c.id = l.cluster_id
           WHERE l.created_at >= $1 AND l.created_at < $2 ORDER BY l.created_at""", start, end)
    simmap = {r.mint: r for r in (sim or {}).get("results", [])}
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["mint", "symbol", "created_at", "cluster_id", "suspicious", "glue_reason", "trades_count",
                "cluster_sells", "initial_mc_sol", "peak_mc_sol", "dump_mc_sol", "time_to_dump_s",
                "achievable_profit", "entry_window", "top_traders", "would_alert", "reason", "sim_net"])
    for r in rows:
        f = r["features"] or {}
        sells = ((f.get("sells") or {}).get("cluster") or {}).get("events")
        ew = ((r["profile"] or {}).get("extra") or {}).get("entry_window") if r["profile"] else None
        s = simmap.get(r["mint"])
        w.writerow([r["mint"], r["symbol"], r["created_at"].isoformat(), r["cluster_id"], bool(r["suspicious"]),
                    r["glue"] or "", r["trades_count"] or 0, sells or 0, _r(r["initial_mc"]), _r(r["peak_mc"]),
                    _r(r["dump_mc"]), _r(r["time_to_dump_s"]), _r(r["achievable_profit"]),
                    f"{ew['t_from']}-{ew['t_to']}s MC {ew['mc_from']}-{ew['mc_to']}" if ew else "",
                    r["tops"] or "", bool(s and s.alerted), s.reason if s else f.get("decision_reason", ""),
                    _r(s.net) if s and s.net is not None else ""])
    return buf.getvalue().encode("utf-8-sig")


def _r(v: Any) -> Any:
    return round(v, 4) if isinstance(v, float) else v


async def run_verify(db: Database, settings: Settings, day: date, days: int = 7) -> tuple[str, bytes]:
    d0 = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    one = await verify_period(db, settings, d0, d0 + timedelta(days=1))
    week = await verify_period(db, settings, d0 + timedelta(days=1) - timedelta(days=days), d0 + timedelta(days=1))
    text = "\n".join(["🔎 <b>Проверка на истории</b> (вход +5 с, комиссии и проскальзывание, профиль только по "
                      "прошлым запускам)", ""] + _fmt(f"{day.isoformat()}", one) + [""] +
                     _fmt(f"последние {days} дней по {day.isoformat()}", week))
    return text, await extended_csv(db, d0 + timedelta(days=1) - timedelta(days=days), d0 + timedelta(days=1), week)
