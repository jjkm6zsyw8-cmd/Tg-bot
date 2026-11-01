"""Загрузка состояния монеты и профилей из PostgreSQL."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..db.pool import Database
from .profiles import Profile, build_profile
from .state import TokenState, TradeRow


def state_from_token(rec: Any, cfg: dict[str, Any]) -> TokenState:
    return TokenState(
        mint=rec["mint"], deployer=rec["deployer"], creator=rec["current_owner"],
        created_ts=rec["created_at"].timestamp(), created_slot=rec["created_slot"],
        symbol=rec["symbol"] or "", name=rec["name"] or "", uri=rec["uri"] or "",
        meta={k: rec[k] for k in ("image", "description", "twitter", "telegram", "website") if rec[k]},
        initial_mc=rec["initial_mc_sol"] or 27.96, initial_buy_sol=rec["initial_buy_sol"] or 0.0,
        cluster_id=rec["cluster_id"], migrated=rec["migrated_at"] is not None,
        big_sell_single=float(cfg.get("big_sell_single_tx_pct", 30)) / 100,
        big_sell_window=float(cfg.get("big_sell_window_pct", 50)) / 100,
        big_sell_window_s=float(cfg.get("big_sell_window_s", 60)),
    )


async def load_state(db: Database, mint: str, cfg: dict[str, Any], until: datetime | None = None,
                     with_cluster: bool = True) -> TokenState | None:
    tok = await db.fetchrow("SELECT * FROM tokens WHERE mint=$1", mint)
    if not tok:
        return None
    st = state_from_token(tok, cfg)
    if with_cluster and tok["cluster_id"]:
        await attach_cluster(db, st, tok["cluster_id"])
    rows = await db.fetch(
        "SELECT * FROM trades WHERE mint=$1 AND ts <= $2 ORDER BY ts, id", mint,
        until or datetime.now(timezone.utc))
    for r in rows:
        st.add_trade(TradeRow.from_record(r))
    return st


async def attach_cluster(db: Database, st: TokenState, cluster_id: int) -> None:
    """Постоянные связи кластера + профинансированные деплоером до запуска."""
    st.cluster_id = cluster_id
    links = await db.fetch(
        """SELECT wallet, reason, confidence FROM cluster_links
           WHERE cluster_id=$1 AND status IN ('auto','confirmed') AND confidence <> 'low'""", cluster_id)
    from .state import Link
    for r in links:
        if r["wallet"] not in st.cluster:
            st.cluster[r["wallet"]] = Link(r["reason"], r["confidence"])
    st.replay()


async def launches_of(db: Database, cluster_id: int, before: datetime | None = None) -> list[dict[str, Any]]:
    rows = await db.fetch(
        "SELECT * FROM launches WHERE cluster_id=$1 AND created_at < $2 ORDER BY created_at",
        cluster_id, before or datetime.now(timezone.utc))
    return [dict(r) for r in rows]


async def cluster_profile(db: Database, cluster_id: int, cfg: dict[str, Any],
                          before: datetime | None = None) -> tuple[Profile, Any]:
    cl = await db.fetchrow("SELECT * FROM clusters WHERE id=$1", cluster_id)
    launches = await launches_of(db, cluster_id, before)
    prof = build_profile(launches, cfg, cluster_id=cluster_id, now=before,
                         pattern_weight=float(cl["pattern_weight"]) if cl else 1.0)
    return prof, cl


async def save_profile(db: Database, cluster_id: int, prof: Profile, cfg: dict[str, Any]) -> bool:
    """Сохраняет профиль; возвращает True, если кластер впервые прошёл пороги."""
    qualifies = (prof.launches >= int(cfg.get("min_launches", 3))
                 and prof.repeatability >= float(cfg.get("min_repeatability", 0.5)) and not prof.instant_rugger)
    first = await db.fetchval(
        """UPDATE clusters SET profile=$2, profile_updated_at=now(),
               qualified_at = CASE WHEN $3 AND qualified_at IS NULL THEN now() ELSE qualified_at END,
               blacklisted = blacklisted OR $4,
               blacklist_reason = CASE WHEN $4 AND NOT blacklisted THEN 'мгновенные раги' ELSE blacklist_reason END
           WHERE id=$1 RETURNING qualified_at > now() - interval '5 seconds'""",
        cluster_id, prof.to_json(), qualifies, prof.instant_rugger)
    return bool(first)
