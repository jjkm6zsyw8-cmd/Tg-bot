"""Кластеры девов: связывание кошельков (разд. 2, 3, 6 ТЗ).

Кластер = исходный деплоер + купившие в слоте создания/бандлом + профинансированные
деплоером до запуска + подтверждённые пользователем. Каждая связь хранится с причиной
и уверенностью; низкая уверенность в расчётах не участвует.

Снайперы первого блока покупают у множества девов, поэтому покупатель из слота создания
закрепляется в кластере навсегда, только если повторяется в запусках этого кластера
и почти не встречается у других (иначе все кластеры склеились бы через ботов).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from ..db.pool import Database
from ..ingest.pump_parser import ParseStats, TxContext, sol_transfers
from ..ingest.rpc import RpcError, SolanaRpc

log = logging.getLogger(__name__)
CONF_RANK = {"low": 0, "medium": 1, "high": 2}


@dataclass
class Resolution:
    cluster_id: int | None
    confidence: str
    reason: str
    new_cluster: bool = False
    known_dev_wallet: bool = False   # кошелёк получил SOL от приоритетного кластера после дампа


async def cluster_of(db: Database, wallet: str) -> tuple[int, str, str] | None:
    row = await db.fetchrow(
        """SELECT cluster_id, confidence, reason FROM cluster_links
           WHERE wallet=$1 AND status IN ('auto','confirmed') AND confidence <> 'low'
           ORDER BY (status='confirmed') DESC, CASE confidence WHEN 'high' THEN 0 ELSE 1 END, id LIMIT 1""", wallet)
    return (row["cluster_id"], row["confidence"], row["reason"]) if row else None


async def add_link(db: Database, cluster_id: int, wallet: str, reason: str, confidence: str,
                   status: str = "auto") -> int | None:
    """Добавляет связь, не понижая уже существующую и не трогая отклонённые."""
    return await db.fetchval(
        """INSERT INTO cluster_links(cluster_id, wallet, reason, confidence, status)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (cluster_id, wallet) DO UPDATE SET
               reason = CASE WHEN cluster_links.status IN ('rejected', 'confirmed') THEN cluster_links.reason
                             ELSE EXCLUDED.reason END,
               confidence = CASE WHEN cluster_links.status IN ('rejected', 'confirmed') THEN cluster_links.confidence
                                 WHEN EXCLUDED.confidence = 'high' THEN 'high' ELSE cluster_links.confidence END
           RETURNING id""", cluster_id, wallet, reason, confidence, status)


async def new_cluster(db: Database, deployer: str) -> int:
    cid = await db.fetchval("INSERT INTO clusters(label) VALUES ($1) RETURNING id", deployer[:6])
    await add_link(db, cid, deployer, "исходный деплоер", "high")
    return cid


class Linker:
    """Связь свежего кошелька с известными кластерами в реальном времени."""

    def __init__(self, db: Database, rpc: SolanaRpc, exchanges: set[str], exchange_tx_heuristic: int = 1000,
                 ext: Any = None):
        self.db = db
        self.rpc = rpc
        self.exchanges = exchanges
        self.heuristic = exchange_tx_heuristic
        self.ext = ext   # ExternalHub: метки бирж (Solscan, GMGN) исключают кошелёк из кластеризации
        self.stats = ParseStats()

    async def is_exchange(self, wallet: str) -> bool:
        if wallet in self.exchanges:
            return True
        row = await self.db.fetchrow("SELECT is_exchange, checked_at, labels_at FROM wallets WHERE address=$1", wallet)
        if row and row["is_exchange"]:
            return True
        if self.ext is not None and not (row and row["labels_at"]):
            labeled = await self.ext.is_exchange(wallet)
            if labeled:
                return True
        if row and row["checked_at"]:
            return row["is_exchange"]
        try:
            sigs = await self.rpc.signatures_for(wallet, limit=self.heuristic)
        except RpcError:
            return False
        busy = False
        if len(sigs) >= self.heuristic:
            times = [s.get("blockTime") for s in sigs if s.get("blockTime")]
            busy = bool(times) and (max(times) - min(times)) < 3600
        await self.db.execute(
            """INSERT INTO wallets(address, is_exchange, checked_at) VALUES ($1, $2, now())
               ON CONFLICT (address) DO UPDATE SET is_exchange=EXCLUDED.is_exchange, checked_at=now()""",
            wallet, busy)
        return busy

    async def transfers_around(self, wallet: str, start: datetime, end: datetime,
                               limit: int = 40, max_tx: int = 12) -> list[Any]:
        """Переводы SOL кошелька в окне [start, end] через RPC (с кэшем в sol_transfers)."""
        try:
            sigs = await self.rpc.signatures_for(wallet, limit=limit)
        except RpcError:
            return []
        out = []
        picked = [s for s in sigs if not s.get("err") and s.get("blockTime")
                  and start.timestamp() <= s["blockTime"] <= end.timestamp()][:max_tx]
        for s in picked:
            try:
                tx = await self.rpc.transaction(s["signature"])
            except RpcError:
                continue
            if not tx:
                continue
            ctx = TxContext(s["signature"], s.get("slot"), s.get("blockTime"), None, "rpc")
            out.extend(sol_transfers(tx, ctx))
        if out:
            await self.db.p.executemany(
                "INSERT INTO sol_transfers(signature, src, dst, sol, slot, ts) VALUES ($1,$2,$3,$4,$5,$6) "
                "ON CONFLICT DO NOTHING", [(t.signature, t.src, t.dst, t.sol, t.slot, t.ts) for t in out])
        return out

    async def resolve_deployer(self, deployer: str, created_at: datetime, funding_window_min: float,
                               image: str | None = None, fresh_cluster: int | None = None,
                               economy: bool = False) -> tuple[Resolution, list[tuple[str, str]]]:
        """Кластер деплоера + кошельки, профинансированные им перед запуском.

        Возвращает (резолюция, [(кошелёк, причина)]) для добавления в кластер запуска."""
        funded: list[tuple[str, str]] = []
        known = await cluster_of(self.db, deployer)
        if known and fresh_cluster is not None and known[0] == fresh_cluster:
            known = None  # только что созданный кластер-заглушка — ищем настоящую связь
        watch = await self.db.fetchrow(
            "SELECT cluster_id, hop, parent FROM withdrawal_watch WHERE wallet=$1 AND stopped IS NULL", deployer)

        win_start = created_at - timedelta(minutes=funding_window_min)
        transfers = await self.transfers_around(deployer, created_at - timedelta(hours=24), created_at,
                                                limit=20 if economy else 40, max_tx=5 if economy else 12)
        inbound = [t for t in transfers if t.dst == deployer and t.src != deployer]
        outbound = [t for t in transfers if t.src == deployer and t.dst != deployer and t.ts >= win_start
                    and t.sol >= 0.05]
        for t in outbound[:5]:
            # в экономном режиме не проверяем получателей на биржу (это ещё по запросу на каждого)
            if economy or not await self.is_exchange(t.dst):
                funded.append((t.dst, f"профинансирован деплоером до запуска ({t.sol:.2f} SOL)"))
        funder = max(inbound, key=lambda t: t.sol, default=None)
        if funder:
            await self.db.execute(
                """INSERT INTO wallets(address, funded_by, funded_at, funded_sol) VALUES ($1,$2,$3,$4)
                   ON CONFLICT (address) DO UPDATE SET funded_by=EXCLUDED.funded_by,
                   funded_at=EXCLUDED.funded_at, funded_sol=EXCLUDED.funded_sol""",
                deployer, funder.src, funder.ts, funder.sol)

        if watch:
            cid = watch["cluster_id"]
            await add_link(self.db, cid, deployer, f"получил SOL от кластера после дампа ({watch['hop']} переход)",
                           "high")
            return Resolution(cid, "high", "SOL после дампа пришли на этот кошелёк", known_dev_wallet=True), funded
        if known:
            return Resolution(known[0], known[1], known[2]), funded

        if funder and not await self.is_exchange(funder.src):
            fc = await cluster_of(self.db, funder.src)
            if fc:
                await add_link(self.db, fc[0], deployer, "профинансирован кошельком кластера", "high")
                return Resolution(fc[0], "high", "общий источник финансирования"), funded
            # другой деплоер кластера получал SOL с того же кошелька
            sib = await self.db.fetchrow(
                """SELECT l.cluster_id FROM wallets w JOIN cluster_links l ON l.wallet = w.address
                   AND l.status IN ('auto','confirmed') AND l.confidence <> 'low'
                   WHERE w.funded_by = $1 AND w.address <> $2 LIMIT 1""", funder.src, deployer)
            if sib:
                await add_link(self.db, sib["cluster_id"], deployer, "общий источник финансирования", "high")
                return Resolution(sib["cluster_id"], "high", "общий источник финансирования"), funded

        if image:
            row = await self.db.fetchrow(
                """SELECT cluster_id FROM tokens WHERE image=$1 AND cluster_id IS NOT NULL AND deployer <> $2
                   ORDER BY created_at DESC LIMIT 1""", image, deployer)
            if row:
                await add_link(self.db, row["cluster_id"], deployer, "повтор картинки из прошлых запусков", "medium")
                return Resolution(row["cluster_id"], "medium", "повтор картинки из прошлых запусков"), funded

        if fresh_cluster is not None:
            return Resolution(fresh_cluster, "high", "новый кластер", new_cluster=True), funded
        cid = await new_cluster(self.db, deployer)
        return Resolution(cid, "high", "новый кластер", new_cluster=True), funded

    async def link_by_metadata(self, cluster_id: int, mint: str, meta: dict[str, Any]) -> list[tuple[int, str]]:
        """Повтор картинки/ссылок/описания связывает с кластером (средняя уверенность)."""
        out = []
        for col, label in (("image", "картинки"), ("twitter", "ссылки X"), ("website", "сайта"),
                           ("telegram", "ссылки Telegram")):
            val = meta.get(col)
            if not val:
                continue
            rows = await self.db.fetch(
                f"""SELECT DISTINCT cluster_id FROM tokens WHERE {col} = $1 AND mint <> $2
                    AND cluster_id IS NOT NULL AND cluster_id <> $3 LIMIT 5""", val, mint, cluster_id)
            out.extend((r["cluster_id"], f"повтор {label}") for r in rows)
        return out

    async def promote_slot_buyers(self, cluster_id: int, buyers: list[str]) -> int:
        """Покупатели из слота создания → постоянная связь, если повторяются только у этого кластера."""
        added = 0
        for w in buyers:
            row = await self.db.fetchrow(
                """SELECT count(*) FILTER (WHERE k.cluster_id = $2) own,
                          count(DISTINCT k.cluster_id) FILTER (WHERE k.cluster_id <> $2) other
                   FROM trades t JOIN tokens k ON k.mint = t.mint
                   WHERE t.trader = $1 AND t.is_buy AND t.slot = k.created_slot""", w, cluster_id)
            if row and row["own"] >= 2 and row["other"] <= 1:
                await add_link(self.db, cluster_id, w, "покупал в слоте создания в нескольких запусках", "high")
                added += 1
        return added


async def cluster_wallets(db: Database, cluster_id: int) -> list[tuple[str, str, str]]:
    rows = await db.fetch(
        """SELECT wallet, reason, confidence FROM cluster_links
           WHERE cluster_id=$1 AND status IN ('auto','confirmed') AND confidence <> 'low'""", cluster_id)
    return [(r["wallet"], r["reason"], r["confidence"]) for r in rows]


def utc(dt: datetime | None = None) -> datetime:
    return dt or datetime.now(timezone.utc)
