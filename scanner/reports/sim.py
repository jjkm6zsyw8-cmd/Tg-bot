"""Симуляция сделок по истории (разд. 5 ТЗ) — основа разбора дня и бэктеста.

Защита от самообмана:
* профиль кластера строится только по запускам ДО симулируемого (point-in-time);
* вход с задержкой реакции, проскальзывание по фактическому состоянию кривой, все комиссии;
* монеты, пропущенные из-за сбоя источника, считаются отдельно и не улучшают статистику.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from ..analytics.curve import Costs, trade_net_profit
from ..analytics.loader import load_state
from ..analytics.profiles import build_profile
from ..analytics.scoring import Snapshot, evaluate_dev
from ..analytics.state import TokenState
from ..db.pool import Database


@dataclass
class SimTrade:
    mint: str
    mode: str
    alerted: bool
    reason: str
    t_alert: float
    age_s: float
    mc_alert: float
    entry_mc: float | None = None
    exit_mc: float | None = None
    exit_reason: str | None = None
    net: float | None = None
    achievable: float | None = None
    outage: bool = False

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


def simulate_exit(st: TokenState, t_alert: float, target: float | None, reaction: float, follow_s: float,
                  position: float, costs: Costs, extra_exit_ts: float | None = None) -> tuple[float, float, str, float]:
    """Вход через reaction после алерта, выход по цели / экстренному сигналу / таймауту."""
    t_entry = t_alert + reaction
    entry = st.mc_at(t_entry)
    t_exit, why = t_alert + follow_s, "timeout"
    emerg = st.big_sell_ts if st.big_sell_ts and st.big_sell_ts > t_alert else None
    if extra_exit_ts and extra_exit_ts > t_alert and (emerg is None or extra_exit_ts < emerg):
        emerg = extra_exit_ts
    for t in st.trades:
        if t.ts < t_entry:
            continue
        if emerg and t.ts >= emerg:
            t_exit, why = emerg + reaction, "emergency"
            break
        if target and t.mc >= target:
            t_exit, why = t.ts + reaction, "target"
            break
        if t.ts > t_alert + follow_s:
            break
    exit_mc = st.mc_at(t_exit)
    return entry, exit_mc, why, trade_net_profit(entry, exit_mc, position, costs)


def _rate(st: TokenState, t: float, window: float = 10.0) -> float:
    return st.growth_rate_per_s(t, window)


async def outage_windows(db: Database, start: datetime, end: datetime) -> list[tuple[float, float]]:
    rows = await db.fetch(
        "SELECT started_at, COALESCE(ended_at, now()) e FROM ingest_gaps WHERE started_at < $2 AND COALESCE(ended_at, now()) > $1",
        start, end)
    return [(r["started_at"].timestamp(), r["e"].timestamp()) for r in rows]


@dataclass
class Prepared:
    launch: dict[str, Any]
    outage: bool
    profile: Any = None
    state: TokenState | None = None


async def prepare_dev(db: Database, cfg: dict[str, Any], start: datetime, end: datetime,
                      limit: int = 20000) -> list[Prepared]:
    """Загружает запуски периода, point-in-time профили и сделки (один раз для любых порогов)."""
    launches = await db.fetch(
        "SELECT l.*, t.received_at FROM launches l JOIN tokens t ON t.mint = l.mint "
        "WHERE l.created_at >= $1 AND l.created_at < $2 ORDER BY l.created_at LIMIT $3", start, end, limit)
    gaps = await outage_windows(db, start, end)
    hist_cache: dict[int, list[dict[str, Any]]] = {}
    follow_s = 60 * float(cfg.get("follow_minutes", 60))
    out: list[Prepared] = []
    for l in launches:
        created = l["created_at"]
        ts = created.timestamp()
        outage = any(a <= ts <= b for a, b in gaps) or bool(
            l["received_at"] is not None and (l["received_at"] - created).total_seconds() > 10)
        item = Prepared(launch=dict(l), outage=outage)
        out.append(item)
        cid = l["cluster_id"]
        if cid is None:
            continue
        if cid not in hist_cache:
            rows = await db.fetch("SELECT * FROM launches WHERE cluster_id=$1 ORDER BY created_at", cid)
            hist_cache[cid] = [dict(r) for r in rows]
        prior = [x for x in hist_cache[cid] if x["created_at"] < created]
        if len(prior) < int(cfg.get("min_launches", 3)):
            continue
        item.profile = build_profile(prior, cfg, cluster_id=cid, now=created)
        item.state = await load_state(db, l["mint"], cfg, until=created + timedelta(seconds=follow_s + 120))
    return out


def run_dev(prepared: list[Prepared], cfg: dict[str, Any], costs: Costs, latency_s: float = 1.5) -> list[SimTrade]:
    reaction = float(cfg.get("reaction_s", 5))
    follow_s = 60 * float(cfg.get("follow_minutes", 60))
    pos = float(cfg.get("position_sol", 0.5))
    out: list[SimTrade] = []
    for item in prepared:
        l = item.launch
        ts = l["created_at"].timestamp()
        t_alert = ts + latency_s
        sim = SimTrade(mint=l["mint"], mode="dev", alerted=False, reason="little_history", t_alert=t_alert,
                       age_s=latency_s, mc_alert=l["initial_mc"] or 0, achievable=l["achievable_profit"],
                       outage=item.outage)
        st = item.state
        if item.profile is None or st is None:
            out.append(sim)
            continue
        sold = (st.big_sell_ts is not None and st.big_sell_ts <= t_alert) or any(
            not t.is_buy and t.trader == st.deployer and t.ts <= t_alert for t in st.trades)
        snap = Snapshot(mc=st.mc_at(t_alert), age_s=latency_s, cluster_sold=sold,
                        cluster_share=l["cluster_share_start"] or 0.0, link_confidence="high",
                        observed_growth_rate=_rate(st, t_alert))
        dec = evaluate_dev(item.profile, snap, cfg, costs)
        sim.mc_alert = snap.mc
        sim.reason = dec.reason or "alerted"
        if dec.alert:
            entry, exit_mc, why, net = simulate_exit(st, t_alert, dec.target_mc, reaction, follow_s, pos, costs)
            sim.alerted, sim.entry_mc, sim.exit_mc, sim.exit_reason, sim.net = True, entry, exit_mc, why, net
        out.append(sim)
    return out


async def simulate_dev(db: Database, cfg: dict[str, Any], costs: Costs, start: datetime, end: datetime,
                       latency_s: float = 1.5) -> list[SimTrade]:
    return run_dev(await prepare_dev(db, cfg, start, end), cfg, costs, latency_s)


async def simulate_smart(db: Database, cfg: dict[str, Any], costs: Costs, start: datetime, end: datetime,
                         limit: int = 2000) -> list[SimTrade]:
    """Сделки по покупкам отслеживаемых трейдеров (по текущему списку)."""
    rows = await db.fetch(
        """SELECT t.* FROM trades t JOIN traders r ON r.address = t.trader AND r.active
           WHERE t.is_buy AND t.ts >= $1 AND t.ts < $2 AND t.sol_amount >= $3 AND t.mc_sol BETWEEN $4 AND $5
           ORDER BY t.ts LIMIT $6""",
        start, end, float(cfg.get("smart_min_buy_sol", 0.5)), float(cfg.get("smart_mc_min_sol", 0)),
        float(cfg.get("smart_mc_max_sol", 1e9)), limit)
    reaction = float(cfg.get("reaction_s", 5))
    follow_s = 60 * float(cfg.get("follow_minutes", 60))
    pos = float(cfg.get("position_sol", 0.5))
    thr = float(cfg.get("min_net_profit", 0.3))
    seen: set[str] = set()
    out: list[SimTrade] = []
    for r in rows:
        if r["mint"] in seen:
            continue
        seen.add(r["mint"])
        t0 = r["ts"].timestamp()
        st = await load_state(db, r["mint"], cfg, until=r["ts"] + timedelta(seconds=follow_s + 120))
        if st is None:
            continue
        if st.growth(t0) > float(cfg.get("smart_max_growth_1m", 1.0)):
            continue
        sells = [t for t in st.trades if t.trader == r["trader"] and not t.is_buy and t.ts > t0]
        sold, exit_ts = 0.0, None
        for s in sells:
            sold += s.tokens
            if sold > 0.5 * r["token_amount"]:
                exit_ts = s.ts
                break
        target = st.mc_at(t0) * (1 + thr) * 1.1
        entry, exit_mc, why, net = simulate_exit(st, t0, target, reaction, follow_s, pos, costs, exit_ts)
        out.append(SimTrade(mint=r["mint"], mode="smart", alerted=True, reason="alerted", t_alert=t0,
                            age_s=t0 - st.created_ts, mc_alert=st.mc_at(t0), entry_mc=entry, exit_mc=exit_mc,
                            exit_reason=why, net=net))
    return out


def series(results: list[SimTrade] | list[float]) -> dict[str, float]:
    """Итог серии: доля удачных × средний плюс − доля неудачных × средний минус."""
    nets = [r.net for r in results if isinstance(r, SimTrade) and r.alerted and r.net is not None and not r.outage] \
        if results and isinstance(results[0], SimTrade) else [float(x) for x in results]  # type: ignore[arg-type]
    if not nets:
        return {"n": 0, "wins": 0, "win_rate": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "series": 0.0}
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x <= 0]
    wr = len(wins) / len(nets)
    aw = sum(wins) / len(wins) if wins else 0.0
    al = -sum(losses) / len(losses) if losses else 0.0
    return {"n": len(nets), "wins": len(wins), "win_rate": wr, "avg_win": aw, "avg_loss": al,
            "series": wr * aw - (1 - wr) * al}


MC_BUCKETS = [(0, 35), (35, 50), (50, 80), (80, 150), (150, 400), (400, 1e12)]
AGE_BUCKETS = [(0, 10), (10, 30), (30, 60), (60, 180), (180, 900), (900, 1e9)]


def best_windows(results: list[SimTrade], sol_usd: float) -> list[str]:
    """Диапазоны MC и время от запуска с лучшим результатом."""
    lines = []
    for name, buckets, key in (("MC входа", MC_BUCKETS, "mc_alert"), ("время от запуска", AGE_BUCKETS, "age_s")):
        best = None
        for lo, hi in buckets:
            sub = [r for r in results if r.alerted and r.net is not None and lo <= getattr(r, key) < hi]
            if len(sub) < 3:
                continue
            s = series(sub)
            if best is None or s["series"] > best[2]["series"]:
                best = (lo, hi, s)
        if best:
            lo, hi, s = best
            rng = (f"${lo * sol_usd / 1000:.1f}k–{'∞' if hi > 1e6 else f'${hi * sol_usd / 1000:.1f}k'}"
                   if key == "mc_alert" else f"{int(lo)}–{'∞' if hi > 1e6 else int(hi)} с")
            lines.append(f"{name}: {rng} — итог серии {s['series']:+.0%} на {s['n']} сделках")
    return lines


def utc_day_bounds(day: datetime | Any, tz: Any) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=tz).astimezone(timezone.utc)
    return start, start + timedelta(days=1)
