"""Состояние монеты в реальном времени: сделки, балансы, кластер дева, крупные продажи.

Один и тот же класс используется для живого потока, для /token по запросу и для
бэктеста (воспроизведение сделок из БД), поэтому результаты совпадают.
"""
from __future__ import annotations

import bisect
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

from ..solana import INITIAL_V_SOL, INITIAL_V_TOKENS, TOTAL_SUPPLY

INITIAL_MC = INITIAL_V_SOL / INITIAL_V_TOKENS * TOTAL_SUPPLY   # ≈ 27.96 SOL
from .curve import curve_progress


@dataclass(slots=True)
class TradeRow:
    ts: float
    slot: int | None
    trader: str
    is_buy: bool
    sol: float
    tokens: float
    mc: float
    signature: str = ""
    new_balance: float | None = None
    real_tokens: float | None = None

    @classmethod
    def from_record(cls, r: Any) -> "TradeRow":
        return cls(
            ts=r["ts"].timestamp(), slot=r["slot"], trader=r["trader"], is_buy=r["is_buy"],
            sol=r["sol_amount"], tokens=r["token_amount"], mc=r["mc_sol"], signature=r["signature"],
            new_balance=r["new_balance"], real_tokens=r["real_tokens"],
        )


@dataclass(slots=True)
class Link:
    reason: str
    confidence: str  # high | medium | low


@dataclass
class TradeEffects:
    cluster_sell: bool = False
    big_sell: bool = False
    cluster_buy_more: bool = False
    sold_fraction: float = 0.0
    mc_before: float = 0.0


@dataclass
class TokenState:
    mint: str
    deployer: str
    created_ts: float
    created_slot: int | None = None
    creator: str = ""
    symbol: str = ""
    name: str = ""
    uri: str = ""
    meta: dict[str, Any] = field(default_factory=dict)
    initial_mc: float = 0.0
    initial_buy_sol: float = 0.0
    cluster_id: int | None = None
    migrated: bool = False
    big_sell_single: float = 0.30
    big_sell_window: float = 0.50
    big_sell_window_s: float = 60.0

    trades: list[TradeRow] = field(default_factory=list)
    balances: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    cluster: dict[str, Link] = field(default_factory=dict)
    mc: float = 0.0
    peak_mc: float = 0.0
    real_tokens: float | None = None
    cluster_peak_hold: float = 0.0
    first_cluster_sell_ts: float | None = None
    big_sell_ts: float | None = None
    dump_mc: float | None = None
    sold_share_at_dump: float | None = None
    _cluster_sells: list[tuple[float, float, float]] = field(default_factory=list)  # ts, sold, hold_before
    _sigs: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        if not self.initial_mc or self.initial_mc <= 0:
            self.initial_mc = INITIAL_MC
        if self.deployer and self.deployer not in self.cluster:
            self.cluster[self.deployer] = Link("исходный деплоер", "high")
        self.mc = self.mc or self.initial_mc
        self.peak_mc = max(self.peak_mc, self.mc)

    # ------------------------------------------------------------- кластер
    def cluster_wallets(self) -> set[str]:
        """Кошельки кластера для расчётов (низкая уверенность не участвует)."""
        return {w for w, l in self.cluster.items() if l.confidence != "low"}

    def add_cluster_wallet(self, wallet: str, reason: str, confidence: str = "high") -> bool:
        cur = self.cluster.get(wallet)
        rank = {"low": 0, "medium": 1, "high": 2}
        if cur and rank[cur.confidence] >= rank[confidence]:
            return False
        self.cluster[wallet] = Link(reason, confidence)
        self.replay()
        return True

    def cluster_hold(self) -> float:
        return sum(max(0.0, self.balances.get(w, 0.0)) for w in self.cluster_wallets())

    # ------------------------------------------------------------- сделки
    def add_trade(self, t: TradeRow) -> TradeEffects:
        key = f"{t.signature}:{t.trader}:{int(t.is_buy)}"
        if t.signature and key in self._sigs:
            return TradeEffects()
        if t.signature:
            self._sigs.add(key)
        if t.ts < self.created_ts:
            # время создания пришло с задержкой (время приёма, а не блока): сделка не может быть раньше
            self.created_ts = t.ts
        # сделки приходят почти по порядку; редкие опоздания вставляем на место и пересчитываем
        if self.trades and t.ts < self.trades[-1].ts - 0.5:
            idx = bisect.bisect_right([x.ts for x in self.trades], t.ts)
            self.trades.insert(idx, t)
            self.replay()
            return TradeEffects()
        self.trades.append(t)
        return self._apply(t)

    def _apply(self, t: TradeRow) -> TradeEffects:
        eff = TradeEffects(mc_before=self.mc)
        cw = self.cluster_wallets()
        # покупка в слоте создания — часть кластера по определению ТЗ
        if (t.is_buy and self.created_slot is not None and t.slot == self.created_slot
                and t.trader not in self.cluster):
            self.cluster[t.trader] = Link("купил в слоте создания", "high")
            cw.add(t.trader)
        in_cluster = t.trader in cw
        hold_before = self.cluster_hold() if in_cluster else 0.0
        if t.new_balance is not None:
            self.balances[t.trader] = t.new_balance
        else:
            self.balances[t.trader] += t.tokens if t.is_buy else -t.tokens
            if self.balances[t.trader] < 0:
                self.balances[t.trader] = 0.0
        if t.mc > 0:
            self.mc = t.mc
            self.peak_mc = max(self.peak_mc, t.mc)
        if t.real_tokens is not None:
            self.real_tokens = t.real_tokens
        if in_cluster:
            hold_now = self.cluster_hold()
            self.cluster_peak_hold = max(self.cluster_peak_hold, hold_now)
            if not t.is_buy and hold_before > 0:
                frac = min(1.0, t.tokens / hold_before)
                eff.cluster_sell = True
                eff.sold_fraction = frac
                if self.first_cluster_sell_ts is None:
                    self.first_cluster_sell_ts = t.ts
                self._cluster_sells.append((t.ts, t.tokens, hold_before))
                if self.big_sell_ts is None and self._is_big_sell(t.ts, frac):
                    self.big_sell_ts = t.ts
                    self.dump_mc = eff.mc_before or t.mc  # MC непосредственно перед первой крупной продажей
                    peak = self.cluster_peak_hold or hold_before
                    self.sold_share_at_dump = 1 - hold_now / peak if peak else None
                    eff.big_sell = True
            elif t.is_buy and t.ts - self.created_ts > 5 and t.trader == self.deployer:
                eff.cluster_buy_more = True
        return eff

    def _is_big_sell(self, ts: float, frac: float) -> bool:
        if frac >= self.big_sell_single:
            return True
        window = [s for s in self._cluster_sells if ts - s[0] <= self.big_sell_window_s]
        if not window:
            return False
        base = window[0][2]
        return base > 0 and sum(s[1] for s in window) / base >= self.big_sell_window

    def replay(self) -> None:
        """Пересчёт с нуля (после изменения кластера или слотов)."""
        trades = sorted(self.trades, key=lambda x: x.ts)
        self.trades = []
        self.balances = defaultdict(float)
        self.mc = self.initial_mc
        self.peak_mc = self.initial_mc
        self.cluster_peak_hold = 0.0
        self.first_cluster_sell_ts = self.big_sell_ts = self.dump_mc = self.sold_share_at_dump = None
        self._cluster_sells = []
        for t in trades:
            self.trades.append(t)
            self._apply(t)

    def apply_slots(self, slots: dict[str, int], created_slot: int | None = None) -> bool:
        changed = False
        if created_slot is not None and self.created_slot != created_slot:
            self.created_slot = created_slot
            changed = True
        for t in self.trades:
            if t.slot is None and t.signature in slots:
                t.slot = slots[t.signature]
                changed = True
        if changed:
            self.replay()
        return changed

    # ------------------------------------------------------------- MC во времени
    def mc_at(self, ts: float) -> float:
        """MC на момент ts (по последней сделке не позже ts)."""
        if not self.trades or ts < self.trades[0].ts:
            return self.initial_mc
        idx = bisect.bisect_right([x.ts for x in self.trades], ts) - 1
        return self.trades[idx].mc if idx >= 0 else self.initial_mc

    def mc_at_age(self, age_s: float) -> float | None:
        if self.trades and self.trades[-1].ts < self.created_ts + age_s:
            return None  # ещё не дожили
        return self.mc_at(self.created_ts + age_s)

    def max_mc_between(self, t0: float, t1: float) -> float:
        vals = [x.mc for x in self.trades if t0 <= x.ts <= t1]
        return max(vals) if vals else self.mc_at(t0)

    def growth(self, now: float, window_s: float = 60.0) -> float:
        base = self.mc_at(now - window_s) if now - window_s > self.created_ts else self.initial_mc
        return self.mc / base - 1 if base > 0 else 0.0

    def creation_mc(self) -> float:
        """MC сразу после транзакции создания (включая покупку дева в ней). Нулевой MC — битые данные."""
        for t in self.trades:
            if t.ts - self.created_ts > 1.5:
                break
            if t.trader == self.deployer and t.is_buy and t.mc > 0:
                return t.mc
        return self.initial_mc if self.initial_mc > 0 else INITIAL_MC

    def growth_rate_per_s(self, now: float, window_s: float = 10.0, min_span_s: float = 3.0) -> float:
        """Наблюдаемый относительный рост MC в секунду за последние window_s.

        База — MC после создания: покупка дева в транзакции создания ростом не считается."""
        start = max(self.created_ts + 1.5, now - window_s)
        span = now - start
        if span < min_span_s:
            return 0.0
        base = max(self.mc_at(start), self.creation_mc())
        cur = self.mc_at(now)
        if base <= 0 or cur <= base:
            return 0.0
        return (cur / base) ** (1 / span) - 1

    # ------------------------------------------------------------- метрики безопасности
    def trades_count(self) -> int:
        """Сделки после транзакции создания (покупка дева в ней не считается)."""
        return sum(1 for t in self.trades if not (t.trader == self.deployer and t.ts - self.created_ts <= 1.5))

    def holders(self) -> dict[str, float]:
        return {w: b for w, b in self.balances.items() if b > 1e-6}

    def share(self, wallets: Iterable[str]) -> float:
        return sum(max(0.0, self.balances.get(w, 0.0)) for w in set(wallets)) / TOTAL_SUPPLY

    def slot_buyers(self) -> dict[int, set[str]]:
        out: dict[int, set[str]] = defaultdict(set)
        for t in self.trades:
            if t.is_buy and t.slot is not None:
                out[t.slot].add(t.trader)
        return out

    def bundle_wallets(self, min_txs: int = 4) -> set[str]:
        """Покупатели из групп 4+ транзакций в одном блоке."""
        sig_by_slot: dict[int, set[str]] = defaultdict(set)
        wallets_by_slot: dict[int, set[str]] = defaultdict(set)
        for t in self.trades:
            if t.is_buy and t.slot is not None:
                sig_by_slot[t.slot].add(t.signature or f"{t.trader}{t.ts}")
                wallets_by_slot[t.slot].add(t.trader)
        out: set[str] = set()
        for slot, sigs in sig_by_slot.items():
            if len(sigs) >= min_txs:
                out |= wallets_by_slot[slot]
        return out

    def sniper_wallets(self) -> set[str]:
        if self.created_slot is None:
            return set()
        return {t.trader for t in self.trades
                if t.is_buy and t.slot == self.created_slot and t.trader != self.deployer}

    def insider_wallets(self) -> set[str]:
        return {w for w, l in self.cluster.items()
                if w != self.deployer and l.confidence != "low" and ("финанс" in l.reason or "перевод" in l.reason)}

    def top10_share(self) -> float:
        bal = sorted(self.holders().values(), reverse=True)[:10]
        return sum(bal) / TOTAL_SUPPLY

    def wash_share(self) -> float:
        cw = self.cluster_wallets()
        total = sum(t.sol for t in self.trades)
        mine = sum(t.sol for t in self.trades if t.trader in cw)
        return mine / total if total > 0 else 0.0

    def early_buyers(self, window_s: float = 30.0) -> int:
        return len({t.trader for t in self.trades if t.is_buy and t.ts - self.created_ts <= window_s})

    def dev_buy_sol(self) -> float:
        sol = self.initial_buy_sol
        for t in self.trades:
            if t.ts - self.created_ts > 3:
                break
            if t.is_buy and t.trader == self.deployer:
                sol = max(sol, t.sol)
        return sol

    def progress(self) -> float | None:
        if self.migrated:
            return 1.0
        return curve_progress(self.real_tokens)

    def slots_known(self) -> bool:
        return self.created_slot is not None and all(
            t.slot is not None for t in self.trades if t.ts - self.created_ts < 10)

    def metrics(self, fresh_share: float | None = None) -> dict[str, Any]:
        cw = self.cluster_wallets()
        return {
            "cluster": self.share(cw),
            "bundles": self.share(self.bundle_wallets()),
            "snipers": self.share(self.sniper_wallets()),
            "insiders": self.share(self.insider_wallets()),
            "top10": self.top10_share(),
            "fresh_wallets": fresh_share,
            "growth_1m": self.growth(self.trades[-1].ts if self.trades else self.created_ts),
            "progress": self.progress(),
            "wash_volume": self.wash_share(),
            "holders": len(self.holders()),
            "slots_known": self.slots_known(),
        }
