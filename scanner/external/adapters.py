"""Адаптеры внешних источников. Все возвращают нормализованные словари (или None) и не бросают
наружу ничего, кроме Unavailable из base.request.

Документация (проверено 02.11.2026):
* DexScreener  https://docs.dexscreener.com/api/reference         — без ключа; 60 и 300 запросов/мин
* RugCheck     https://api.rugcheck.xyz/swagger/index.html         — отчёт и сводка публичные
* GMGN         https://github.com/GMGNAI/gmgn-skills (OpenAPI)     — X-APIKEY, ключ на gmgn.ai/ai; Free 5/5
* Birdeye      https://docs.birdeye.so                              — X-API-KEY, Standard: 1 rps, 30k CU/мес
* Jupiter      https://developers.jup.ag/docs/price/v3              — без ключа 0.5 rps, ключ: 1 rps
* Solscan Pro  https://pro-api.solscan.io/pro-api-docs/v2.0         — заголовок token; метки — тариф Level 2
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from .base import ExternalSource

SOL_MINT = "So11111111111111111111111111111111111111112"
# надёжные котировки: у пар к экзотическим токенам (токенизированные акции, мемы) DexScreener
# иногда считает цену и MC с ошибкой в тысячи раз
GOOD_QUOTES = {SOL_MINT, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",   # USDC
               "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}                # USDT


def good_quote(d: dict[str, Any]) -> bool:
    return ((d.get("quoteToken") or {}).get("address")) in GOOD_QUOTES

TTL_LABELS = 24 * 3600
TTL_SECURITY = 600
TTL_PRICE = 15

EXCHANGE_WORDS = ("exchange", "cex", "binance", "coinbase", "okx", "bybit", "kraken", "kucoin", "gate.io",
                  "mexc", "bitget", "htx", "huobi", "crypto.com", "upbit", "bitfinex")


def _f(v: Any) -> float | None:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


# ====================================================================== DexScreener
class DexScreener(ExternalSource):
    name = "dexscreener"
    rate_per_s = 4.5     # пары/токены: 300/мин
    capacity = 5

    async def pair(self, mint: str, ttl: float = TTL_PRICE) -> dict[str, Any] | None:
        """ttl — сколько секунд годится кеш; для открытых позиций — 3 с."""
        async def fetch() -> dict[str, Any] | None:
            data = await self.request("GET", f"https://api.dexscreener.com/tokens/v1/solana/{mint}",
                                      path_key="tokens")
            return parse_dex_pairs(data, mint)
        return await self.cached(f"pair{int(ttl)}", mint, ttl, fetch)

    async def pairs_many(self, mints: list[str], ttl: float = 2.0) -> dict[str, dict[str, Any] | None]:
        """Цены пачкой: до 30 монет за один запрос (для открытых бумажных позиций). Кладёт в кеш pair{ttl}."""
        out: dict[str, dict[str, Any] | None] = {}
        need: list[str] = []
        for m in dict.fromkeys(mints):
            hit, v = self.cache.get(f"pair{int(ttl)}:{m}")
            if hit:
                self.stats.cache_hits += 1
                out[m] = v
            else:
                need.append(m)
        for i in range(0, len(need), 30):
            chunk = need[i:i + 30]
            data = await self.request("GET", f"https://api.dexscreener.com/tokens/v1/solana/{','.join(chunk)}",
                                      path_key="tokens")
            for m in chunk:
                out[m] = parse_dex_pairs(data, m)
                self.cache.set(f"pair{int(ttl)}:{m}", out[m], ttl)
        return out

    async def get_json(self, url: str, path_key: str, timeout_s: float = 8.0) -> Any:
        """Для режима «Тренды» (опрос раз в 30 с) — через общий лимит адаптера."""
        return await self.request("GET", url, path_key=path_key, timeout_s=timeout_s)


def parse_dex_pairs(data: Any, mint: str) -> dict[str, Any] | None:
    best = None
    rank: tuple[bool, float] = (False, -1.0)
    # пара pump.fun (кривая) у DexScreener без ликвидности, и любой левый пул PumpSwap с $100 «ликвидности»
    # её перебивал: Piper — кривая $37k, левый пул $1.7k → бумага «входила» по $1.7k. Пул миграции создаётся
    # после кривой; пул PumpSwap старше пары кривой — не настоящий, его не берём
    curve_at = min((int(d.get("pairCreatedAt") or 0) for d in data or []
                    if d.get("dexId") == "pumpfun" and ((d.get("baseToken") or {}).get("address")) == mint
                    and d.get("pairCreatedAt")), default=0)
    for d in data or []:
        if ((d.get("baseToken") or {}).get("address")) != mint:
            continue
        if (curve_at and d.get("dexId") == "pumpswap" and d.get("pairCreatedAt")
                and int(d["pairCreatedAt"]) < curve_at):
            continue
        liq = float((d.get("liquidity") or {}).get("usd") or 0)
        r = (good_quote(d), liq)            # сначала пары к SOL/USDC/USDT, потом — по ликвидности
        if best is None or r > rank:
            rank = r
            tx = (d.get("txns") or {}).get("m5") or {}
            best = {
                "dex": d.get("dexId"), "pair": d.get("pairAddress"), "price_usd": _f(d.get("priceUsd")),
                "symbol": (d.get("baseToken") or {}).get("symbol"),
                "market_cap_usd": _f(d.get("marketCap") or d.get("fdv")), "liquidity_usd": liq,
                "volume_m5": _f((d.get("volume") or {}).get("m5")), "volume_h1": _f((d.get("volume") or {}).get("h1")),
                "buys_m5": int(tx.get("buys") or 0), "sells_m5": int(tx.get("sells") or 0),
                "price_change_m5": _f((d.get("priceChange") or {}).get("m5")),
                "price_change_h1": _f((d.get("priceChange") or {}).get("h1")),
                "boosts": int((d.get("boosts") or {}).get("active") or 0),
                "good_quote": r[0],
            }
    return best


# ====================================================================== RugCheck
class RugCheck(ExternalSource):
    name = "rugcheck"
    rate_per_s = 1.0     # лимит не опубликован — держимся скромно
    capacity = 3

    async def report(self, mint: str) -> dict[str, Any] | None:
        async def fetch() -> dict[str, Any] | None:
            data = await self.request("GET", f"https://api.rugcheck.xyz/v1/tokens/{mint}/report", path_key="report")
            return parse_rugcheck(data)
        return await self.cached("report", mint, TTL_SECURITY, fetch)


def parse_rugcheck(d: Any) -> dict[str, Any] | None:
    if not isinstance(d, dict):
        return None
    known = d.get("knownAccounts") or {}
    service = {a for a, v in known.items() if (v or {}).get("type") in ("AMM", "LOCKER", "PROGRAM")}
    holders = [h for h in d.get("topHolders") or []
               if h.get("owner") not in service and h.get("address") not in service]
    risks = [{"name": r.get("name"), "level": r.get("level"), "value": r.get("value")} for r in d.get("risks") or []]
    return {
        "mint_authority": d.get("mintAuthority"),
        "freeze_authority": d.get("freezeAuthority"),
        "score_normalised": _f(d.get("score_normalised")),
        "risks": risks,
        "danger_risks": [r["name"] for r in risks if (r.get("level") or "").lower() == "danger"],
        "top10_pct": sum(float(h.get("pct") or 0) for h in holders[:10]) / 100 if holders else None,
        "insiders_in_top": sum(1 for h in holders[:20] if h.get("insider")),
        "insiders_detected": d.get("graphInsidersDetected"),
        "rugged": bool(d.get("rugged")),
        "total_holders": d.get("totalHolders"),
        "creator": d.get("creator"),
    }


# ====================================================================== GMGN OpenAPI
class Gmgn(ExternalSource):
    name = "gmgn"
    needs_key = True
    rate_per_s = 5.0     # Free: leaky bucket 5/5 (веса: security 1, holders 5, wallet_stats 3)
    capacity = 5
    host = "https://openapi.gmgn.ai"

    def headers(self) -> dict[str, str]:
        return {"X-APIKEY": self.key, "Content-Type": "application/json"}

    async def _get(self, path: str, params: dict[str, Any], cost: float) -> Any:
        q = {**params, "timestamp": int(time.time()), "client_id": str(uuid.uuid4())}
        body = await self.request("GET", self.host + path, path_key=path, params=q, cost=cost)
        if not isinstance(body, dict) or body.get("code") != 0:
            err = body.get("error") if isinstance(body, dict) else "bad response"
            self.stats.error(f"{path}: {err}")
            return None
        return body.get("data")

    async def token_security(self, mint: str) -> dict[str, Any] | None:
        async def fetch() -> dict[str, Any] | None:
            return parse_gmgn_security(await self._get("/v1/token/security", {"chain": "sol", "address": mint}, 1))
        return await self.cached("security", mint, TTL_SECURITY, fetch)

    async def holder_tags(self, mint: str) -> dict[str, Any] | None:
        """Сколько смарт-мани и KOL среди холдеров (теги smart_degen, renowned)."""
        async def fetch() -> dict[str, Any] | None:
            out: dict[str, Any] = {}
            for tag, key in (("smart_degen", "smart"), ("renowned", "kol")):
                data = await self._get("/v1/market/token_top_holders",
                                       {"chain": "sol", "address": mint, "tag": tag, "limit": 50}, 5)
                rows = (data or {}).get("list") if isinstance(data, dict) else data
                out[key] = len(rows or [])
                out[f"{key}_wallets"] = [r.get("address") for r in rows or [] if r.get("address")][:20]
            return out
        return await self.cached("holder_tags", mint, TTL_SECURITY, fetch)

    async def wallet_stats(self, wallet: str, period: str = "30d") -> dict[str, Any] | None:
        async def fetch() -> dict[str, Any] | None:
            data = await self._get("/v1/user/wallet_stats", {"chain": "sol", "wallet_address": wallet,
                                                               "period": period}, 3)
            return parse_gmgn_wallet(data)
        return await self.cached(f"wallet_{period}", wallet, TTL_LABELS, fetch)

    async def trending(self, interval: str = "1h", limit: int = 20) -> list[dict[str, Any]]:
        """Монеты в тренде (GMGN rank, вес 3): адрес, символ, MC — источник кандидатов разведки."""
        async def fetch() -> list[dict[str, Any]]:
            data = await self._get("/v1/market/rank", {"chain": "sol", "interval": interval, "limit": limit}, 3)
            rows = (data or {}).get("rank") if isinstance(data, dict) else data
            return [{"mint": r.get("address"), "symbol": r.get("symbol") or "", "mc": _f(r.get("market_cap"))}
                    for r in rows or [] if isinstance(r, dict) and r.get("address")]
        return await self.cached("rank", interval, 300, fetch) or []

    async def top_traders(self, mint: str, limit: int = 30) -> list[dict[str, Any]]:
        """Лучшие трейдеры монеты по реализованной прибыли (GMGN token_top_traders, вес 5)."""
        async def fetch() -> list[dict[str, Any]]:
            data = await self._get("/v1/market/token_top_traders",
                                   {"chain": "sol", "address": mint, "limit": limit, "order_by": "profit",
                                    "direction": "desc"}, 5)
            rows = (data or {}).get("list") if isinstance(data, dict) else data
            return [parse_gmgn_trader(r) for r in rows or [] if isinstance(r, dict) and r.get("address")]
        return await self.cached("top_traders", mint, 1800, fetch) or []

    async def created_tokens(self, wallet: str) -> Any:
        async def fetch() -> Any:
            return await self._get("/v1/user/created_tokens", {"chain": "sol", "wallet_address": wallet}, 1)
        return await self.cached("created", wallet, TTL_SECURITY, fetch)


def parse_gmgn_security(d: Any) -> dict[str, Any] | None:
    if not isinstance(d, dict):
        return None
    return {
        "mint_renounced": d.get("renounced_mint"),
        "freeze_renounced": d.get("renounced_freeze_account"),
        "top10_rate": _f(d.get("top_10_holder_rate")),
        "rug_ratio": _f(d.get("rug_ratio")),
        "sniper_count": d.get("sniper_count"),
        "creator_token_status": d.get("creator_token_status"),
    }


def parse_gmgn_trader(r: dict[str, Any]) -> dict[str, Any]:
    """Строка топ-трейдеров монеты: прибыль в $, доля проданного, сделки, метки."""
    tags = [str(t) for t in (r.get("tags") or []) + (r.get("maker_token_tags") or []) if t]
    bought, sold = _f(r.get("history_bought_cost")) or 0.0, _f(r.get("history_sold_income")) or 0.0
    profit = _f(r.get("realized_profit"))
    return {"address": r.get("address"), "profit_usd": profit if profit is not None else sold - bought,
            "bought_usd": bought, "sold_share": _f(r.get("sell_amount_percentage")) or 0.0,
            "buys": int(_f(r.get("buy_tx_count_cur")) or 0), "sells": int(_f(r.get("sell_tx_count_cur")) or 0),
            "exchange": str(r.get("addr_type")) == "2", "suspicious": bool(r.get("is_suspicious")), "tags": tags}


def parse_gmgn_wallet(d: Any) -> dict[str, Any] | None:
    if isinstance(d, list):
        d = d[0] if d else None
    if not isinstance(d, dict):
        return None
    common = d.get("common") or {}
    tags = list(common.get("tags") or [])
    if common.get("tag") and common["tag"] not in tags:
        tags.append(common["tag"])
    pnl = d.get("pnl_stat") or {}
    winrate = _f(pnl.get("winrate"))
    return {
        "tags": tags, "name": common.get("name"), "twitter": common.get("twitter_username"),
        "followers": common.get("followers_count"), "fund_from": common.get("fund_from"),
        "pnl": _f(d.get("realized_profit_pnl", d.get("pnl"))), "realized_profit": _f(d.get("realized_profit")),
        "winrate": winrate if winrate is not None else _f(d.get("winrate")),
        "buy_count": d.get("buy_count", d.get("buy")), "sell_count": d.get("sell_count", d.get("sell")),
        # для разведки: сколько монет торговал, среднее удержание, распределение итогов по монетам
        "token_num": int(_f(pnl.get("token_num")) or 0), "avg_hold_s": _f(pnl.get("avg_holding_period")),
        "big_losses": int(_f(pnl.get("pnl_lt_nd5_num")) or 0), "x5": int(_f(pnl.get("pnl_gt_5x_num")) or 0),
        "created_tokens": int(_f(common.get("created_token_count")) or 0),
        "is_smart": any(t in ("smart_degen", "smart_money") for t in tags),
        "is_kol": any(t in ("kol", "renowned") for t in tags),
        "is_exchange": any(w in " ".join(map(str, tags + [common.get("fund_from") or ""])).lower()
                           for w in ("cex", "exchange")),
    }


# ====================================================================== Birdeye
class Birdeye(ExternalSource):
    name = "birdeye"
    needs_key = True
    rate_per_s = 1.0     # Standard: 1 rps на весь аккаунт
    capacity = 1

    def headers(self) -> dict[str, str]:
        return {"X-API-KEY": self.key, "x-chain": "solana"}

    async def price(self, mint: str) -> float | None:
        async def fetch() -> float | None:
            d = await self.request("GET", "https://public-api.birdeye.so/defi/price", path_key="price",
                                   params={"address": mint})
            return _f(((d or {}).get("data") or {}).get("value"))
        return await self.cached("price", mint, TTL_PRICE, fetch)

    async def wallet_pnl(self, wallet: str) -> dict[str, Any] | None:
        """PnL кошелька. По документации — с тарифа Starter; на Standard вернёт 403 и выключится на сутки."""
        async def fetch() -> dict[str, Any] | None:
            d = await self.request("GET", "https://public-api.birdeye.so/wallet/v2/pnl/summary",
                                   path_key="pnl_summary", params={"wallet": wallet})
            s = ((d or {}).get("data") or {}).get("summary") or (d or {}).get("data") or {}
            pnl = s.get("pnl") if isinstance(s.get("pnl"), dict) else s
            return {"realized_usd": _f(pnl.get("realized_profit_usd")),
                    "unrealized_usd": _f(pnl.get("unrealized_usd")),
                    "total_usd": _f(pnl.get("total_usd")), "win_rate": _f((s.get("counts") or {}).get("win_rate"))}
        return await self.cached("pnl", wallet, TTL_LABELS, fetch)


# ====================================================================== Jupiter
class Jupiter(ExternalSource):
    name = "jupiter"
    rate_per_s = 0.5     # без ключа; с ключом 1 rps
    capacity = 1

    def headers(self) -> dict[str, str]:
        return {"x-api-key": self.key} if self.key else {}

    @property
    def base(self) -> str:
        return "https://api.jup.ag/price/v3" if self.key else "https://lite-api.jup.ag/price/v3"

    async def prices(self, mints: list[str], ttl: float = TTL_PRICE) -> dict[str, float]:
        """ttl — сколько секунд годится кеш (бумага для решений берёт 1–4 с, сверка — 15 с)."""
        key = ",".join(sorted(mints))[:500]

        async def fetch() -> dict[str, float]:
            d = await self.request("GET", self.base, path_key="price", params={"ids": ",".join(mints[:50])})
            return {m: float(v["usdPrice"]) for m, v in (d or {}).items() if isinstance(v, dict) and v.get("usdPrice")}
        return await self.cached(f"prices{int(ttl)}", key, ttl, fetch)

    async def sol_usd(self) -> float | None:
        return (await self.prices([SOL_MINT])).get(SOL_MINT)


# ====================================================================== Solscan Pro
class Solscan(ExternalSource):
    name = "solscan"
    needs_key = True
    rate_per_s = 5.0     # платные тарифы: 1000 запросов / 60 с
    capacity = 5

    def headers(self) -> dict[str, str]:
        return {"token": self.key}

    async def label(self, wallet: str) -> dict[str, Any] | None:
        async def fetch() -> dict[str, Any] | None:
            d = await self.request("GET", "https://pro-api.solscan.io/v2.0/account/metadata",
                                   path_key="account_metadata", params={"address": wallet})
            data = (d or {}).get("data") or {}
            label = data.get("account_label") or ""
            tags = list(data.get("account_tags") or [])
            typ = data.get("account_type") or ""
            text = " ".join([label, typ, *map(str, tags)]).lower()
            return {"label": label or None, "tags": tags, "type": typ or None,
                    "is_exchange": any(w in text for w in EXCHANGE_WORDS)}
        return await self.cached("label", wallet, TTL_LABELS, fetch)
