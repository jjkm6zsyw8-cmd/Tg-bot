"""Проверки режима «Тренды» против ботов и рагов (по разбору «How to avoid scams 101»).

  1. Боты в потоке сделок — GeckoTerminal даёт число РАЗНЫХ покупателей и продавцов за 5 минут:
     много покупок от малого числа кошельков = накрутка; продавцов почти столько же, сколько покупателей = нет спроса.
  2. «Лесенка» и ровный объём — по минутным свечам GeckoTerminal: рост без откатов и красных свечей,
     одинаковый объём каждую минуту — так выглядит работа бота, а не живые покупатели.
  3. Холдеры — топ-10 кошельков: доля свежих (созданы < 24 ч), общий источник денег, пополнены в одно время.
  4. Вход на откате — после сигнала ждём отката и отскока, а не покупаем вершину свечи.

Чистые функции (staircase, bot_flow, pullback_step, holder_stats) покрыты тестами; сеть — в trends.py.
"""
from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

DEFAULTS: dict[str, float] = {
    "trend_min_buyers_m5": 12,          # разных покупателей за 5 мин
    "trend_max_buys_per_buyer": 2.5,    # покупок на одного покупателя за 5 мин
    "trend_min_buyer_ratio": 1.2,       # разных покупателей / разных продавцов
    "trend_stair_rise": 0.15,           # рост за ~20 минутных свечей, после которого проверяем «лесенку»
    "trend_stair_max_red": 1,           # красных свечей (−1% и больше) не больше
    "trend_stair_max_dd": 0.03,         # самый глубокий откат по закрытиям меньше 3% — подозрительно ровно
    "trend_flat_volume_cv": 0.25,       # разброс объёма по минутам меньше — объём рисует бот
    "trend_max_fresh": 0.5,             # доля свежих кошельков в топ-10
    "trend_max_same_funder": 0.3,       # доля кошельков топ-10 с одним источником денег
    "trend_max_same_time": 0.5,         # доля кошельков топ-10, пополненных в одно окно 10 минут
    "trend_pullback_min": 0.05,         # откат от пика после сигнала
    "trend_pullback_max": 0.20,         # глубже — это уже обвал, не входим
    "trend_bounce": 0.02,               # отскок от дна отката
    "trend_pullback_wait_s": 300,       # сколько ждём отката
}

REASONS = {
    "bots": "боты в сделках", "staircase": "лесенка", "flat_volume": "ровный объём (бот)",
    "holders_fresh": "свежие кошельки", "holders_funder": "общий источник денег",
    "holders_time": "пополнены в одно время", "no_pullback": "не было отката", "fell": "обвал после сигнала",
    "no_data": "нет данных GeckoTerminal", "bad_price": "цена не сходится с Jupiter", "top10": "топ-10 держат много",
}


def q(cfg: dict[str, Any], key: str) -> float:
    v = cfg.get(key)
    return float(v) if v is not None else DEFAULTS[key]


# ---------------------------------------------------------------------- 1. боты
def bot_flow(tx: dict[str, Any] | None, cfg: dict[str, Any]) -> str | None:
    """tx — attributes.transactions пула GeckoTerminal. Причина отказа или None."""
    m5 = (tx or {}).get("m5") or {}
    buys, buyers = int(m5.get("buys") or 0), int(m5.get("buyers") or 0)
    sellers = int(m5.get("sellers") or 0)
    if buyers < q(cfg, "trend_min_buyers_m5"):
        return f"покупателей за 5 мин {buyers}"
    if buys / max(1, buyers) > q(cfg, "trend_max_buys_per_buyer"):
        return f"{buys} покупок от {buyers} кошельков"
    if buyers < q(cfg, "trend_min_buyer_ratio") * max(1, sellers):
        return f"покупателей {buyers} / продавцов {sellers}"
    return None


# ---------------------------------------------------------------------- 2. лесенка
def candles_of(ohlcv: list[list[float]] | None, n: int = 20) -> list[list[float]]:
    """GeckoTerminal ohlcv_list ([ts, o, h, l, c, v], новые первыми) → последние n свечей по времени."""
    return sorted(ohlcv or [], key=lambda x: x[0])[-n:]


def staircase(ohlcv: list[list[float]] | None, cfg: dict[str, Any]) -> tuple[str, str] | None:
    """(код, пояснение) или None. Мало свечей — не судим."""
    cs = candles_of(ohlcv)
    if len(cs) < 10:
        return None
    closes = [c[4] for c in cs]
    if closes[0] <= 0:
        return None
    rise = closes[-1] / closes[0] - 1
    red = sum(1 for c in cs if c[1] > 0 and c[4] < c[1] * 0.99)
    peak, dd = closes[0], 0.0
    for c in closes:
        peak = max(peak, c)
        dd = max(dd, 1 - c / peak if peak > 0 else 0.0)
    if rise >= q(cfg, "trend_stair_rise") and red <= q(cfg, "trend_stair_max_red") and dd < q(cfg, "trend_stair_max_dd"):
        return "staircase", f"+{rise:.0%} за {len(cs)} мин без откатов"
    vols = [c[5] for c in cs[-15:]]
    full_minutes = cs[-1][0] - cs[-15][0] <= 15 * 60 if len(cs) >= 15 else False
    if full_minutes and len(vols) >= 15:
        mean = sum(vols) / len(vols)
        if mean > 0:
            cv = (sum((v - mean) ** 2 for v in vols) / len(vols)) ** 0.5 / mean
            if cv < q(cfg, "trend_flat_volume_cv"):
                return "flat_volume", f"объём каждую минуту одинаковый (разброс {cv:.0%})"
    return None


# ---------------------------------------------------------------------- 3. холдеры
@dataclass
class HolderInfo:
    wallet: str
    created: datetime | None
    funder: str | None
    funded_at: datetime | None


def holder_stats(hs: list[HolderInfo], services: set[str], now: datetime | None = None) -> dict[str, Any]:
    """Доли среди проверенных кошельков: свежие, общий источник (без бирж и сервисов), пополнены в одно окно."""
    now = now or datetime.now(timezone.utc)
    n = len(hs)
    if n == 0:
        return {"checked": 0}
    fresh = sum(1 for h in hs if h.created and h.created > now - timedelta(hours=24))
    funders = Counter(h.funder for h in hs if h.funder and h.funder not in services)
    top_funder, same = (funders.most_common(1)[0] if funders else (None, 0))
    times = sorted(h.funded_at for h in hs if h.funded_at)
    window = 0
    for i, t in enumerate(times):
        j = i
        while j < len(times) and times[j] - t <= timedelta(minutes=10):
            j += 1
        window = max(window, j - i)
    return {"checked": n, "fresh": fresh / n, "same_funder": (same / n) if same >= 2 else 0.0,
            "funder": top_funder if same >= 2 else None, "same_time": (window / n) if window >= 3 else 0.0}


def holder_verdict(stats: dict[str, Any], cfg: dict[str, Any]) -> tuple[str, str] | None:
    if not stats or stats.get("checked", 0) < 5:
        return None
    if stats["fresh"] > q(cfg, "trend_max_fresh"):
        return "holders_fresh", f"свежих кошельков в топ-10 {stats['fresh']:.0%}"
    if stats["same_funder"] > q(cfg, "trend_max_same_funder"):
        return "holders_funder", f"{stats['same_funder']:.0%} топ-10 пополнены с одного кошелька"
    if stats["same_time"] > q(cfg, "trend_max_same_time"):
        return "holders_time", f"{stats['same_time']:.0%} топ-10 пополнены в одни 10 минут"
    return None


def funding_of(tx: dict[str, Any] | None, wallet: str) -> tuple[str | None, float]:
    """Кто прислал SOL кошельку в его первой транзакции (jsonParsed getTransaction)."""
    if not tx:
        return None, 0.0
    msg = (tx.get("transaction") or {}).get("message") or {}
    ixs = list(msg.get("instructions") or [])
    for inner in ((tx.get("meta") or {}).get("innerInstructions") or []):
        ixs += inner.get("instructions") or []
    for ix in ixs:
        parsed = ix.get("parsed") if isinstance(ix, dict) else None
        if not isinstance(parsed, dict) or parsed.get("type") not in ("transfer", "transferWithSeed", "createAccount"):
            continue
        info = parsed.get("info") or {}
        if (info.get("destination") or info.get("newAccount")) == wallet and info.get("source"):
            return info["source"], float(info.get("lamports") or 0) / 1e9
    return None, 0.0


# ---------------------------------------------------------------------- 4. вход на откате
@dataclass
class Pending:
    mint: str
    ts: float
    peak: float
    signal_price: float
    low: float | None = None
    dipped: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


def pullback_step(pd: Pending, price: float, now: float, cfg: dict[str, Any]) -> str | None:
    """"enter" — откат и отскок, входим; "fell" / "no_pullback" — отказ; None — ждём дальше."""
    if price <= 0:
        return None
    pd.peak = max(pd.peak, price)
    if price <= pd.peak * (1 - q(cfg, "trend_pullback_max")):
        return "fell"
    if price <= pd.peak * (1 - q(cfg, "trend_pullback_min")):
        pd.dipped = True
        pd.low = price if pd.low is None else min(pd.low, price)
    if pd.dipped and pd.low is not None and price >= pd.low * (1 + q(cfg, "trend_bounce")):
        return "enter"
    if now - pd.ts > q(cfg, "trend_pullback_wait_s"):
        return "no_pullback"
    return None


# ---------------------------------------------------------------------- теневой учёт
@dataclass
class Shadow:
    mint: str
    symbol: str
    reason: str
    detail: str
    ts: float
    price: float
    max_price: float
    last_price: float


class RateBudget:
    """Не больше n вызовов за окно (GeckoTerminal: 30 в минуту на всех)."""

    def __init__(self, n: int, window_s: float = 60.0):
        self.n, self.window = n, window_s
        self.calls: list[float] = []

    def take(self) -> bool:
        now = time.monotonic()
        self.calls = [t for t in self.calls if now - t < self.window]
        if len(self.calls) >= self.n:
            return False
        self.calls.append(now)
        return True


async def shadow_summary(db: Any, start: datetime, end: datetime) -> list[str]:
    """Отсеянные «Трендами» монеты: что было с ними через час — и для сравнения алерты за тот же период."""
    # медиана, а не среднее: одна монета со сбойной ценой (×10 000) делала «среднее» бессмысленным;
    # записи с ростом больше ×20 за час считаем сбоем данных и не учитываем
    clean = "price > 0 AND max_price < price * 20 AND last_price < price * 20"
    rows = await db.fetch(
        f"""SELECT reason, count(*) AS n,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY last_price / price - 1) AS after,
                   count(*) FILTER (WHERE max_price >= price * 1.3) AS up30,
                   count(*) FILTER (WHERE last_price <= price * 0.7) AS down30
            FROM trend_shadow WHERE created_at >= $1 AND created_at < $2 AND {clean}
            GROUP BY 1 ORDER BY 2 DESC""", start, end)
    if not rows:
        return []
    total = sum(r["n"] for r in rows)
    up = sum(r["up30"] for r in rows)
    down = sum(r["down30"] for r in rows)
    out = [f"🧹 <b>Тренды: отсеяно {total}</b> · через час: упали на 30%+ {down} ({down / total:.0%}), "
           f"на пике было +30%↑ {up} ({up / total:.0%})"]
    for r in rows:
        out.append(f"  {REASONS.get(r['reason'], r['reason'])}: {r['n']} · медиана через час {(r['after'] or 0):+.0%} · "
                   f"+30%↑ {r['up30']} · −30%↓ {r['down30']}")
    a = await db.fetchrow(
        """SELECT count(*) AS n, count(*) FILTER (WHERE o.max_mc >= a.entry_mc * 1.3) AS up30,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY o.net_result) AS med
           FROM alerts a JOIN alert_outcomes o ON o.alert_id = a.id
           WHERE a.mode = 'trend' AND a.created_at >= $1 AND a.created_at < $2
             AND a.entry_mc > 0 AND o.max_mc < a.entry_mc * 20""", start, end)
    if a and a["n"]:
        out.append(f"  для сравнения, алерты: {a['n']} · +30%↑ {a['up30']} ({a['up30'] / a['n']:.0%}) · "
                   f"медиана итога {(a['med'] or 0):+.0%}")
    return out
