"""Копируемость трейдера и отбор смарт-мани (разд. 4 ТЗ).

Копируемость — средний чистый результат, который получил бы пользователь, повторяя
сделки трейдера с задержкой на вход и выход (по фактическому MC через delay секунд),
с комиссиями и проскальзыванием кривой.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Callable

from ..db.pool import Database
from .curve import Costs, trade_net_profit

log = logging.getLogger(__name__)


@dataclass
class RoundTrip:
    mint: str
    entry_ts: float
    exit_ts: float
    buy_sol: float
    sell_sol: float
    entry_mc: float
    copy_entry_mc: float = 0.0
    copy_exit_mc: float = 0.0
    sniped: bool = False
    sells: list[tuple[float, float]] = field(default_factory=list)

    @property
    def own_pnl(self) -> float:
        return self.sell_sol - self.buy_sol

    @property
    def hold_s(self) -> float:
        return self.exit_ts - self.entry_ts


@dataclass
class TraderStats:
    trades: int
    win_rate: float
    copyability: float          # средний чистый результат копии на сделку
    pnl_sol: float
    median_hold_s: float
    avg_entry_mc: float
    snipe_share: float
    last_active: float | None

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def round_trips(trades: list[dict[str, Any]], create_slots: dict[str, int | None]) -> list[RoundTrip]:
    """Пары вход→выход по каждой монете. trades: dict(mint, ts, is_buy, sol, tokens, slot, mc)."""
    by_mint: dict[str, list[dict[str, Any]]] = {}
    for t in sorted(trades, key=lambda x: x["ts"]):
        by_mint.setdefault(t["mint"], []).append(t)
    out: list[RoundTrip] = []
    for mint, ts in by_mint.items():
        bal = peak = buy_sol = sell_sol = 0.0
        entry: dict[str, Any] | None = None
        sells: list[tuple[float, float]] = []
        for t in ts:
            if t["is_buy"]:
                if entry is None:
                    entry = t
                    buy_sol = sell_sol = peak = 0.0
                    sells = []
                bal += t["tokens"]
                peak = max(peak, bal)
                buy_sol += t["sol"]
            elif entry is not None:
                sold = min(t["tokens"], bal)
                bal -= sold
                sell_sol += t["sol"]
                sells.append((t["ts"], sold))
                if bal <= 0.1 * peak:
                    cs = create_slots.get(mint)
                    rt = RoundTrip(mint=mint, entry_ts=entry["ts"],
                                   exit_ts=sum(a * b for a, b in sells) / max(1e-9, sum(b for _, b in sells)),
                                   buy_sol=buy_sol, sell_sol=sell_sol, entry_mc=entry["mc"],
                                   sniped=bool(cs is not None and entry.get("slot") is not None
                                               and entry["slot"] - cs <= 1),
                                   sells=sells)
                    out.append(rt)
                    entry, bal = None, 0.0
    return out


def stats_from_trips(trips: list[RoundTrip], position_sol: float, costs: Costs) -> TraderStats:
    if not trips:
        return TraderStats(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, None)
    copies = [trade_net_profit(t.copy_entry_mc or t.entry_mc, t.copy_exit_mc or t.entry_mc, position_sol, costs)
              for t in trips]
    return TraderStats(
        trades=len(trips),
        win_rate=sum(1 for t in trips if t.own_pnl > 0) / len(trips),
        copyability=sum(copies) / len(copies),
        pnl_sol=sum(t.own_pnl for t in trips),
        median_hold_s=median(t.hold_s for t in trips),
        avg_entry_mc=sum(t.entry_mc for t in trips) / len(trips),
        snipe_share=sum(1 for t in trips if t.sniped) / len(trips),
        last_active=max(t.exit_ts for t in trips),
    )


def apply_copy_prices(trips: list[RoundTrip], mc_at: Callable[[str, float], float | None], delay_s: float) -> None:
    for t in trips:
        t.copy_entry_mc = mc_at(t.mint, t.entry_ts + delay_s) or t.entry_mc
        sells = t.sells or [(t.exit_ts, 1.0)]
        total = sum(b for _, b in sells) or 1.0
        t.copy_exit_mc = sum((mc_at(t.mint, a + delay_s) or t.entry_mc) * b for a, b in sells) / total


async def mc_lookup(db: Database, pairs: list[tuple[str, float]]) -> list[float | None]:
    if not pairs:
        return []
    rows = await db.fetch(
        """SELECT q.i, t.mc_sol FROM unnest($1::text[], $2::timestamptz[]) WITH ORDINALITY AS q(mint, ts, i)
           LEFT JOIN LATERAL (SELECT mc_sol FROM trades WHERE trades.mint = q.mint AND trades.ts <= q.ts
                              ORDER BY ts DESC LIMIT 1) t ON true ORDER BY q.i""",
        [p[0] for p in pairs], [datetime.fromtimestamp(p[1], tz=timezone.utc) for p in pairs],
    )
    return [r["mc_sol"] for r in rows]


async def compute_trader(db: Database, address: str, cfg: dict[str, Any], costs: Costs,
                         days: int = 30) -> tuple[TraderStats, TraderStats, list[RoundTrip]]:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows = await db.fetch(
        """SELECT t.mint, t.ts, t.is_buy, t.sol_amount, t.token_amount, t.slot, t.mc_sol, k.created_slot
           FROM trades t LEFT JOIN tokens k ON k.mint = t.mint
           WHERE t.trader = $1 AND t.ts >= $2 ORDER BY t.ts""", address, since)
    trades = [{"mint": r["mint"], "ts": r["ts"].timestamp(), "is_buy": r["is_buy"], "sol": r["sol_amount"],
               "tokens": r["token_amount"], "slot": r["slot"], "mc": r["mc_sol"]} for r in rows]
    create_slots = {r["mint"]: r["created_slot"] for r in rows}
    trips = round_trips(trades, create_slots)
    delay = float(cfg.get("copy_delay_s", 5))
    pairs: list[tuple[str, float]] = []
    for t in trips:
        pairs.append((t.mint, t.entry_ts + delay))
        for a, _ in t.sells:
            pairs.append((t.mint, a + delay))
    values = await mc_lookup(db, pairs)
    cache = {p: v for p, v in zip(pairs, values)}
    apply_copy_prices(trips, lambda m, ts: cache.get((m, ts)), delay)
    pos = float(cfg.get("position_sol", 0.5))
    cut7 = (datetime.now(timezone.utc) - timedelta(days=7)).timestamp()
    s30 = stats_from_trips(trips, pos, costs)
    s7 = stats_from_trips([t for t in trips if t.entry_ts >= cut7], pos, costs)
    return s7, s30, trips


def qualifies(s7: TraderStats, s30: TraderStats, linked: bool, cfg: dict[str, Any]) -> tuple[bool, list[str]]:
    fails = []
    if not (s7.copyability > 0 and s30.copyability > 0):
        fails.append("копируемость ≤ 0")
    if s30.trades < int(cfg.get("trader_min_trades_30d", 30)):
        fails.append(f"сделок {s30.trades} < {cfg.get('trader_min_trades_30d', 30)}")
    if s30.median_hold_s < float(cfg.get("trader_min_hold_s", 120)):
        fails.append(f"удержание {s30.median_hold_s:.0f} с — не повторить вручную")
    if s30.snipe_share >= float(cfg.get("trader_max_snipe_share", 0.2)):
        fails.append("снайпер")
    if linked:
        fails.append("связан с кластером дева")
    act_cut = datetime.now(timezone.utc).timestamp() - 86400 * float(cfg.get("trader_active_days", 7))
    if not s30.last_active or s30.last_active < act_cut:
        fails.append("нет активности 7 дней")
    return not fails, fails


OWN_SMART_MIN_HOLD_S = 180.0


def smart_like(t: Any, cfg: dict[str, Any] | None = None) -> bool:
    """Свой кошелёк, которого бот ведёт как smart: держит монету минутами (медиана удержания ≥ 3 мин) и повтор его
    сделок с задержкой и комиссиями за 30 дней в плюсе (копируемость > 0), не связан с девами. Скальперов (держат
    секунды) с задержкой не повторить — за неделю 5–11.11 копии таких кошельков были в минусе при любой тактике."""
    get = t.get if hasattr(t, "get") else (lambda k, d=None: d)
    hold, copy = get("median_hold_s"), get("copy_30d")
    need = float((cfg or {}).get("own_smart_min_hold_s", OWN_SMART_MIN_HOLD_S))
    return hold is not None and float(hold) >= need and copy is not None and float(copy) > 0 \
        and not get("linked_to_dev")


def tier(s30: TraderStats) -> str:
    return "excellent" if s30.copyability >= 0.15 and s30.win_rate >= 0.55 else "good"


TIER_NAMES = {"excellent": "Отличный", "good": "Хороший"}


async def linked_to_dev(db: Database, address: str) -> bool:
    row = await db.fetchval(
        """SELECT EXISTS(SELECT 1 FROM cluster_links WHERE wallet=$1 AND status IN ('auto','confirmed'))
           OR EXISTS(SELECT 1 FROM sol_transfers s JOIN cluster_links l ON l.wallet = s.src
                     AND l.status IN ('auto','confirmed') WHERE s.dst = $1)
           OR (SELECT count(*) FROM trades t JOIN tokens k ON k.mint = t.mint
               WHERE t.trader=$1 AND t.is_buy AND t.slot IS NOT NULL AND t.slot = k.created_slot
                 AND k.cluster_id IS NOT NULL) >= 2""", address)
    return bool(row)


async def save_trader(db: Database, address: str, s7: TraderStats, s30: TraderStats, ok: bool, linked: bool,
                      label: str | None = None, manual: bool | None = None) -> None:
    await db.execute(
        """INSERT INTO traders(address, label, tier, manual, active, qualified, qualified_at, pnl_7d, pnl_30d,
               copy_7d, copy_30d, win_rate, trades_30d, median_hold_s, avg_entry_mc, snipe_share, linked_to_dev,
               last_active, updated_at)
           VALUES ($1, $2, $3, COALESCE($4, false), $5, $5, CASE WHEN $5 THEN now() END, $6, $7, $8, $9, $10, $11,
                   $12, $13, $14, $15, to_timestamp($16), now())
           ON CONFLICT (address) DO UPDATE SET
               label = COALESCE(EXCLUDED.label, traders.label),
               tier = EXCLUDED.tier,
               manual = COALESCE($4, traders.manual),
               active = CASE WHEN COALESCE($4, traders.manual) THEN COALESCE($4, traders.active)
                             WHEN traders.pinned THEN traders.active
                             ELSE EXCLUDED.qualified END,
               qualified = EXCLUDED.qualified,
               qualified_at = CASE WHEN EXCLUDED.qualified AND NOT traders.qualified THEN now()
                                   WHEN EXCLUDED.qualified THEN traders.qualified_at ELSE NULL END,
               pnl_7d = EXCLUDED.pnl_7d, pnl_30d = EXCLUDED.pnl_30d, copy_7d = EXCLUDED.copy_7d,
               copy_30d = EXCLUDED.copy_30d, win_rate = EXCLUDED.win_rate, trades_30d = EXCLUDED.trades_30d,
               median_hold_s = EXCLUDED.median_hold_s, avg_entry_mc = EXCLUDED.avg_entry_mc,
               snipe_share = EXCLUDED.snipe_share, linked_to_dev = EXCLUDED.linked_to_dev,
               last_active = EXCLUDED.last_active, updated_at = now()""",
        address, label, tier(s30), manual, ok, s7.pnl_sol, s30.pnl_sol, s7.copyability, s30.copyability,
        s30.win_rate, s30.trades, s30.median_hold_s, s30.avg_entry_mc, s30.snipe_share, linked,
        s30.last_active or 0,
    )


async def refresh_traders(db: Database, cfg: dict[str, Any], costs: Costs, limit: int = 2000) -> dict[str, int]:
    """Ежесуточный пересчёт списка трейдеров из собственной базы сделок."""
    min_trades = int(cfg.get("trader_min_trades_30d", 30))
    rows = await db.fetch(
        """SELECT trader FROM trades WHERE ts > now() - interval '30 days'
           GROUP BY trader HAVING count(DISTINCT mint) >= $1 AND max(ts) > now() - interval '7 days'
           ORDER BY sum(sol_amount) DESC LIMIT $2""", min_trades, limit)
    candidates = {r["trader"] for r in rows}
    candidates |= {r["address"] for r in await db.fetch("SELECT address FROM traders")}
    qualified = 0
    for addr in candidates:
        try:
            s7, s30, _ = await compute_trader(db, addr, cfg, costs)
            linked = await linked_to_dev(db, addr)
            ok, _ = qualifies(s7, s30, linked, cfg)
            await save_trader(db, addr, s7, s30, ok, linked)
            qualified += ok
        except Exception:  # noqa: BLE001
            log.exception("trader %s", addr)
    return {"candidates": len(candidates), "qualified": qualified}


async def apply_external_labels(db: Database, ext: Any, cfg: dict[str, Any], limit: int = 30) -> int:
    """Метки GMGN (smart_degen, KOL) для лучших кандидатов в смарт-мани.

    Ончейн-критерии в приоритете: в список попадает только кошелёк с положительной копируемостью,
    удержанием не меньше порога и без связи с девами. Метка лишь снимает требование к числу сделок.
    """
    if ext is None or not ext.gmgn.enabled:
        return 0
    rows = await db.fetch(
        """SELECT address FROM traders WHERE copy_30d > 0 AND copy_7d > 0 AND NOT linked_to_dev
               AND median_hold_s >= $1 AND snipe_share < $2 AND last_active > now() - interval '7 days'
               AND (tags_source IS NULL OR updated_at > now() - interval '1 day')
           ORDER BY qualified, copy_30d DESC LIMIT $3""",
        float(cfg.get("trader_min_hold_s", 120)), float(cfg.get("trader_max_snipe_share", 0.2)), limit)
    added = 0
    for r in rows:
        info = await ext.wallet_info(r["address"], timeout=ext.command_timeout)
        if not info:
            continue
        smart_or_kol = bool(info.get("is_smart") or info.get("is_kol"))
        await db.execute(
            """UPDATE traders SET tags=$2, tags_source='gmgn', is_kol = is_kol OR $3,
                   qualified = qualified OR $4, active = active OR $4,
                   qualified_at = CASE WHEN $4 AND NOT qualified THEN now() ELSE qualified_at END,
                   label = COALESCE(label, $5)
               WHERE address=$1""",
            r["address"], info.get("tags") or [], bool(info.get("is_kol")), smart_or_kol,
            ("@" + info["twitter"]) if info.get("twitter") else None)
        added += smart_or_kol
    return added


async def refresh_traders_adaptive(db: Database, cfg: dict[str, Any], costs: Costs, limit: int = 500,
                                   ext: Any = None) -> dict[str, Any]:
    """Пока в базе меньше 7 дней сделок, порог числа сделок снижается пропорционально истории."""
    first = await db.fetchval("SELECT min(ts) FROM trades")
    days = (datetime.now(timezone.utc) - first).total_seconds() / 86400 if first else 0.0
    eff = dict(cfg)
    bootstrap = days < 7
    if bootstrap:
        full = int(cfg.get("trader_min_trades_30d", 30))
        eff["trader_min_trades_30d"] = max(int(cfg.get("trader_min_trades_bootstrap", 5)),
                                           int(full * max(days, 0.0) / 30))
    res = await refresh_traders(db, eff, costs, limit=limit)
    labeled = await apply_external_labels(db, ext, eff)
    return {**res, "qualified": res["qualified"] + labeled, "labeled": labeled, "bootstrap": bootstrap,
            "min_trades": eff.get("trader_min_trades_30d")}
