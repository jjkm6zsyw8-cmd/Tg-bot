"""Режим «Тренды»: монеты Solana, которые прямо сейчас продвигают и покупают.

Источники (официальные публичные API, без ключей):
  * DexScreener — свежие бусты, топ бустов, новые профили токенов, данные пар (объём/покупки/цена за 5 мин);
  * GeckoTerminal — новые и трендовые пулы Solana (жёсткий лимит, опрашиваем редко);
  * Solana RPC — топ-10 холдеров (getTokenLargestAccounts) для отсечения монет «у одного кошелька».

Каждые 30 с: собрать кандидатов → пакетно получить пары → проверить импульс → алерт.
Сопровождение 60 минут по данным DexScreener: цель, разворот (экстренный выход), итог.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import aiohttp

from ..alerts import format as fmt
from ..ingest.rpc import RpcError
from ..solana import is_valid_address

if TYPE_CHECKING:
    from .engine import AlertEntry, Engine

log = logging.getLogger(__name__)

DEX = "https://api.dexscreener.com"
GECKO = "https://api.geckoterminal.com/api/v2/networks/solana"

DEFAULTS: dict[str, float] = {
    "trend_mc_min_usd": 15_000,
    "trend_mc_max_usd": 5_000_000,
    "trend_min_liq_usd": 8_000,
    "trend_min_vol_m5_usd": 2_000,
    "trend_min_buys_m5": 15,
    "trend_min_buy_ratio_m5": 1.3,     # покупок / продаж за 5 минут
    "trend_pc_m5_min": 3,              # цена за 5 мин, % — растёт
    "trend_pc_m5_max": 60,             # но не вершина свечи
    "trend_pc_h1_max": 500,
    "trend_max_age_h": 72,
    "trend_max_top10": 0.45,           # доля топ-10 холдеров без пула
    "trend_exit_drop": 0.20,           # падение от цены алерта → выход
    "trend_exit_m5_drop": 15,          # падение за 5 мин, % → выход
}


def p(cfg: dict[str, Any], key: str) -> float:
    v = cfg.get(key)
    return float(v) if v is not None else DEFAULTS[key]


@dataclass
class PairInfo:
    mint: str
    symbol: str
    name: str
    pair: str
    dex: str
    price: float
    mc_usd: float
    liq_usd: float
    vol_m5: float
    vol_h1: float
    buys_m5: int
    sells_m5: int
    pc_m5: float
    pc_h1: float
    age_h: float
    boosts: int
    socials: int
    liq_base: float

    @classmethod
    def from_pair(cls, d: dict[str, Any]) -> "PairInfo":
        tx = (d.get("txns") or {}).get("m5") or {}
        vol = d.get("volume") or {}
        pc = d.get("priceChange") or {}
        liq = d.get("liquidity") or {}
        info = d.get("info") or {}
        created = d.get("pairCreatedAt") or 0
        base = d.get("baseToken") or {}
        return cls(
            mint=base.get("address", ""), symbol=base.get("symbol", ""), name=base.get("name", ""),
            pair=d.get("pairAddress", ""), dex=d.get("dexId", ""), price=float(d.get("priceUsd") or 0),
            mc_usd=float(d.get("marketCap") or d.get("fdv") or 0), liq_usd=float(liq.get("usd") or 0),
            vol_m5=float(vol.get("m5") or 0), vol_h1=float(vol.get("h1") or 0),
            buys_m5=int(tx.get("buys") or 0), sells_m5=int(tx.get("sells") or 0),
            pc_m5=float(pc.get("m5") or 0), pc_h1=float(pc.get("h1") or 0),
            age_h=(time.time() - created / 1000) / 3600 if created else 1e9,
            boosts=int((d.get("boosts") or {}).get("active") or 0),
            socials=len(info.get("socials") or []) + len(info.get("websites") or []),
            liq_base=float(liq.get("base") or 0),
        )


def score(pi: PairInfo, cfg: dict[str, Any]) -> tuple[bool, str]:
    """Импульс: растёт, покупают больше, чем продают, есть ликвидность и объём, не вершина свечи."""
    if not (p(cfg, "trend_mc_min_usd") <= pi.mc_usd <= p(cfg, "trend_mc_max_usd")):
        return False, "mc"
    if pi.liq_usd < p(cfg, "trend_min_liq_usd"):
        return False, "liquidity"
    if pi.age_h > p(cfg, "trend_max_age_h"):
        return False, "old"
    if pi.vol_m5 < p(cfg, "trend_min_vol_m5_usd") or pi.buys_m5 < p(cfg, "trend_min_buys_m5"):
        return False, "volume"
    if pi.buys_m5 < p(cfg, "trend_min_buy_ratio_m5") * max(1, pi.sells_m5):
        return False, "pressure"
    if not (p(cfg, "trend_pc_m5_min") <= pi.pc_m5 <= p(cfg, "trend_pc_m5_max")):
        return False, "price_m5"
    if pi.pc_h1 > p(cfg, "trend_pc_h1_max"):
        return False, "price_h1"
    return True, ""


@dataclass
class Tracked:
    mint: str
    alert_price: float
    alert_ts: float
    target_price: float
    max_price: float = 0.0
    exit_price: float | None = None
    exit_reason: str | None = None
    flags: set[str] = field(default_factory=set)


class Trends:
    def __init__(self, engine: "Engine"):
        self.e = engine
        self.cands: dict[str, float] = {}      # mint → когда впервые увидели
        self.boosts: dict[str, int] = {}
        self.alerted_at: dict[str, float] = {}
        self.tracked: dict[str, Tracked] = {}
        self.tick = 0
        self.stats = {"candidates": 0, "checked": 0, "alerts": 0, "errors": 0}

    async def _get(self, http: aiohttp.ClientSession, url: str) -> Any:
        try:
            async with http.get(url, headers={"Accept": "application/json"}) as r:
                if r.status != 200:
                    self.stats["errors"] += 1
                    return None
                return await r.json(content_type=None)
        except Exception:  # noqa: BLE001
            self.stats["errors"] += 1
            return None

    async def discover(self, http: aiohttp.ClientSession) -> None:
        now = time.time()
        for path in ("/token-boosts/latest/v1", "/token-boosts/top/v1", "/token-profiles/latest/v1"):
            for x in await self._get(http, DEX + path) or []:
                if x.get("chainId") == "solana" and is_valid_address(x.get("tokenAddress")):
                    m = x["tokenAddress"]
                    self.cands.setdefault(m, now)
                    if x.get("totalAmount"):
                        self.boosts[m] = int(x["totalAmount"])
        if self.tick % 6 == 0:  # GeckoTerminal: ~30 запросов в минуту на всех — раз в 3 минуты
            for path in ("/new_pools", "/trending_pools"):
                data = await self._get(http, GECKO + path)
                for pool in (data or {}).get("data") or []:
                    rel = ((pool.get("relationships") or {}).get("base_token") or {}).get("data") or {}
                    tid = rel.get("id", "")
                    m = tid.split("_", 1)[1] if tid.startswith("solana_") else ""
                    if is_valid_address(m):
                        self.cands.setdefault(m, now)
        # свежие монеты из нашего потока, которые уже набрали покупателей (могли мигрировать)
        for st in list(self.e.states.values()):
            if st.migrated or (st.progress() or 0) > 0.8:
                self.cands.setdefault(st.mint, now)
        cut = now - 3 * 3600
        for m in [m for m, t in self.cands.items() if t < cut and m not in self.tracked]:
            self.cands.pop(m, None)
        self.stats["candidates"] = len(self.cands)

    async def pairs(self, http: aiohttp.ClientSession, mints: list[str]) -> dict[str, PairInfo]:
        out: dict[str, PairInfo] = {}
        for i in range(0, len(mints), 30):
            data = await self._get(http, f"{DEX}/tokens/v1/solana/{','.join(mints[i:i + 30])}")
            for d in data or []:
                pi = PairInfo.from_pair(d)
                if pi.mint in mints[i:i + 30] and (pi.mint not in out or pi.liq_usd > out[pi.mint].liq_usd):
                    out[pi.mint] = pi
            await asyncio.sleep(0.25)
        return out

    async def top10_share(self, pi: PairInfo) -> float | None:
        """Доля топ-10 холдеров без кошелька пула (RPC: 2 запроса)."""
        rpc = self.e.rpc
        try:
            supply = await rpc.call("getTokenSupply", [pi.mint])
            largest = await rpc.call("getTokenLargestAccounts", [pi.mint])
        except RpcError:
            return None
        total = float(((supply or {}).get("value") or {}).get("uiAmount") or 0)
        accs = [float(a.get("uiAmount") or 0) for a in ((largest or {}).get("value") or [])]
        if total <= 0 or not accs:
            return None
        if pi.liq_base and accs and abs(accs[0] - pi.liq_base) / max(pi.liq_base, 1) < 0.05:
            accs = accs[1:]          # первый — пул ликвидности
        elif pi.dex in ("pumpfun", "pump") and accs:
            accs = accs[1:]          # на кривой крупнейший — аккаунт bonding curve
        return sum(accs[:10]) / total

    async def run(self) -> None:
        await asyncio.sleep(20)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as http:
            while True:
                try:
                    cfg = self.e.cfg
                    if cfg.get("modes", {}).get("trend", True):
                        await self.discover(http)
                        await self.scan(http, cfg)
                    await self.follow(http, cfg)
                    await self.e.db.kv_set("health:trends", {**self.stats, "tracked": len(self.tracked)})
                except Exception:  # noqa: BLE001
                    log.exception("trends")
                self.tick += 1
                await asyncio.sleep(30)

    async def scan(self, http: aiohttp.ClientSession, cfg: dict[str, Any]) -> None:
        now = time.time()
        mints = [m for m in self.cands if now - self.alerted_at.get(m, 0) > 6 * 3600 and m not in self.tracked]
        info = await self.pairs(http, mints)
        self.stats["checked"] = len(info)
        for m, pi in info.items():
            ok, _why = score(pi, cfg)
            if not ok:
                continue
            row = await self.e.db.fetchrow(
                """SELECT k.cluster_id, c.blacklisted FROM tokens k LEFT JOIN clusters c ON c.id = k.cluster_id
                   WHERE k.mint=$1""", m)
            if row and row["blacklisted"]:
                continue
            st = self.e.states.get(m)
            if st is not None and (st.big_sell_ts or any(not t.is_buy and t.trader == st.deployer for t in st.trades)):
                continue  # дев уже сливал — не входим
            top10 = await self.top10_share(pi)
            if top10 is not None and top10 > p(cfg, "trend_max_top10"):
                continue
            await self.alert(pi, cfg, top10, row["cluster_id"] if row else None)

    async def alert(self, pi: PairInfo, cfg: dict[str, Any], top10: float | None, cluster_id: int | None) -> None:
        sol = self.e.price.value or 150
        thr = float(cfg.get("min_net_profit", 0.3))
        costs = self.e.costs()
        f = costs.fee_bps / 10_000 + costs.extra_slippage_bps / 10_000
        target_mult = (1 + thr) / max(0.5, (1 - f) ** 2)
        mc_sol = pi.mc_usd / sol
        self.alerted_at[pi.mint] = time.time()
        self.tracked[pi.mint] = Tracked(pi.mint, pi.price, time.time(), pi.price * target_mult, max_price=pi.price)
        self.stats["alerts"] += 1
        boosts = self.boosts.get(pi.mint) or pi.boosts
        level = "strong" if boosts and pi.buys_m5 >= 2 * max(1, pi.sells_m5) and pi.pc_m5 >= 10 else "normal"
        data = {
            "symbol": pi.symbol, "mc": mc_sol, "age_s": pi.age_h * 3600, "signal": "импульс",
            "entry_mc": mc_sol, "entry_max_mc": mc_sol * 1.1, "target_mc": mc_sol * target_mult,
            "metrics": {"top10": top10} if top10 is not None else {}, "violations": [],
            "cluster_id": cluster_id, "stage": "мигрировала" if pi.dex != "pumpfun" else "на кривой",
            "exit_rule": "Выход при развороте (продажи > покупок)",
            "extra_reasons": [
                f"5 мин: объём {fmt.usd(pi.vol_m5)}, покупок {pi.buys_m5} / продаж {pi.sells_m5}, цена {pi.pc_m5:+.0f}%",
                f"1 ч: объём {fmt.usd(pi.vol_h1)}, цена {pi.pc_h1:+.0f}%",
                f"Ликвидность {fmt.usd(pi.liq_usd)} · {pi.dex} · пара {fmt.age(pi.age_h * 3600)}"
                + (f" · буст DexScreener ×{boosts}" if boosts else "") + (" · есть соцсети" if pi.socials else ""),
            ],
        }
        await self.e.compose("trend", pi.mint, level, data, signal="impulse")

    async def follow(self, http: aiohttp.ClientSession, cfg: dict[str, Any]) -> None:
        if not self.tracked:
            return
        info = await self.pairs(http, list(self.tracked))
        now = time.time()
        for m, tr in list(self.tracked.items()):
            pi = info.get(m)
            e = self.e.alerts.get(m)
            if pi and pi.price > 0:
                tr.max_price = max(tr.max_price, pi.price)
                if e is not None:
                    e.data["mc"] = pi.mc_usd / (self.e.price.value or 150)
                    await self.e._edit(e)
                if tr.exit_price is None:
                    if pi.price >= tr.target_price and "target" not in tr.flags:
                        tr.flags.add("target")
                        await self._msg(m, pi, "target")
                    drop = pi.price <= tr.alert_price * (1 - p(cfg, "trend_exit_drop"))
                    rev = pi.pc_m5 <= -p(cfg, "trend_exit_m5_drop") or (
                        pi.sells_m5 > 2 * max(1, pi.buys_m5) and pi.pc_m5 < 0)
                    if drop or rev:
                        tr.exit_price, tr.exit_reason = pi.price, "emergency"
                        await self._msg(m, pi, "trend_exit", emergency=True,
                                        extra=f"5 мин: цена {pi.pc_m5:+.0f}%, покупок {pi.buys_m5} / продаж {pi.sells_m5}")
            if now - tr.alert_ts > 2 * 3600:   # итог подводит engine через finalize(); это страховка от утечки
                self.tracked.pop(m, None)

    async def _msg(self, mint: str, pi: PairInfo, kind: str, emergency: bool = False, extra: str = "") -> None:
        mc_sol = pi.mc_usd / (self.e.price.value or 150)
        text = fmt.render_follow(kind, {"mint": mint, "symbol": pi.symbol, "mc": mc_sol}, self.e.price.value, extra)
        kb = fmt.exit_keyboard(mint, self.e.settings.links(), self.e.env.redirect_base) if emergency else None
        await self.e.db.enqueue("follow", {"mint": mint, "text": text, "keyboard": kb, "emergency": emergency,
                                           "kind": kind}, priority=0 if emergency else 5)

    async def finalize(self, e: "AlertEntry") -> None:
        """Итог алерта «Тренды» по ценам DexScreener."""
        tr = self.tracked.pop(e.mint, None)
        if tr is None:
            return
        costs = self.e.costs()
        f = costs.fee_bps / 10_000 + costs.extra_slippage_bps / 10_000
        entry = tr.alert_price
        exit_p = tr.exit_price or tr.max_price
        if tr.exit_price is None and "target" in tr.flags:
            exit_p, reason = tr.target_price, "target"
        elif tr.exit_price is not None:
            reason = tr.exit_reason or "emergency"
        else:
            reason = "timeout"
        net = (exit_p / entry) * (1 - f) ** 2 - 1 if entry else 0.0
        best = (tr.max_price / entry) * (1 - f) ** 2 - 1 if entry else 0.0
        thr = float(self.e.cfg.get("min_net_profit", 0.3))
        auto = "great" if best >= max(1.0, 2 * thr) else "good" if best >= thr else "fail"
        sol = self.e.price.value or 150
        outcome = {"max_mc": (e.data.get("mc") or 0) * tr.max_price / entry if entry else None,
                   "exit_mc": None, "exit_reason": reason, "net_result": net, "achievable_profit": best,
                   "auto_mark": auto}
        for aid in e.alert_ids.values():
            await self.e.db.execute(
                """INSERT INTO alert_outcomes(alert_id, max_mc, exit_reason, net_result, achievable_profit, auto_mark)
                   VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (alert_id) DO UPDATE SET max_mc=EXCLUDED.max_mc,
                   exit_reason=EXCLUDED.exit_reason, net_result=EXCLUDED.net_result,
                   achievable_profit=EXCLUDED.achievable_profit, auto_mark=EXCLUDED.auto_mark, computed_at=now()""",
                aid, outcome["max_mc"], reason, net, best, auto)
            await self.e.db.execute("UPDATE alerts SET closed_at=now() WHERE id=$1", aid)
        main_id = next(iter(e.alert_ids.values()), None)
        text = fmt.render_final({**e.data, "mint": e.mint}, outcome, sol)
        await self.e.db.enqueue("final", {"mint": e.mint, "text": text,
                                          "keyboard": fmt.marks_keyboard(main_id) if main_id else None}, priority=5)
