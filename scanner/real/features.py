"""25.11 (владелец): признаки входа трендов — на каждом входе trend / trendj (и в тени, и в бою) бот пишет в
real_positions.extra.feat снимок монеты, чтобы через 200+ сделок сравнить падающие и растущие.

Поля (доли — 0…1, время — часы, MC — рост в долях: 0.12 = +12%):
  dev_team_hold   доля выпуска у команды (кластера) дева, GMGN stat.dev_team_hold_rate
  creator_hold    доля у самого создателя
  top10           доля топ-10 холдеров (GMGN); sig_top10 — наш расчёт трендов по RPC на момент сигнала
  dev_sold        доля проданного дева из купленного (GMGN, метка dev); dev_status — hold / sell
  insiders_*      инсайдеры (GMGN rat_trader) — «кластер» вокруг дева: держат сейчас, продали, кошельков
  bundlers_*      бандлы: держат сейчас, продали, кошельков, доля объёма (bundler_volume)
  snipers_*       снайперы: держат сейчас, продали, кошельков
  holders         число холдеров (GMGN; запасной — RugCheck)
  pc_m5, pc_m30   рост цены (= MC) за 5 и 30 мин до входа (минутные свечи GeckoTerminal пула сигнала;
                  запасной для 5 мин — GMGN price_5m); pc_h1 — за час (GMGN)
  vol_m5_usd      объём за 5 мин, $; pool_sol — SOL в пуле (кривая / пул PumpSwap — из сети, иначе GMGN);
  vol_m5_to_pool  объём за 5 мин в SOL / SOL в пуле
  coin_age_h      возраст монеты; since_migr_h — сколько прошло с переезда (None — ещё на кривой / не знаем);
  on_curve        монета ещё на кривой pump.fun
Источник у каждого поля — в extra.feat.src (что ответило). Нет ответа — поле None, вход от этого не зависит.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

GECKO = "https://api.geckoterminal.com/api/v2/networks/solana"
TAGS = (("dev", "dev"), ("insiders", "rat_trader"), ("bundlers", "bundler"), ("snipers", "sniper"))
SOL_SYMBOLS = {"SOL", "WSOL"}


def _f(v: Any) -> float | None:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def candle_growth(rows: list[list[float]], now: float) -> dict[str, float | None]:
    """Минутные свечи GeckoTerminal [ts, open, high, low, close, volume_usd] → рост за 5 и 30 мин и объём за 5 мин.
    Цена «N мин назад» — закрытие последней свечи не позже now − N мин; свечей так далеко нет (пул моложе или в
    данных пропуск дольше 2 мин) — None."""
    rows = sorted((r for r in rows if isinstance(r, (list, tuple)) and len(r) >= 6), key=lambda r: r[0])
    if not rows:
        return {"pc_m5": None, "pc_m30": None, "vol_m5_usd": None}
    last = float(rows[-1][4])

    def back(sec: float) -> float | None:
        t0 = now - sec
        before = [r for r in rows if r[0] <= t0]
        if before:
            return float(before[-1][4])
        first = rows[0]
        return float(first[1]) if first[0] <= t0 + 120 else None

    out: dict[str, float | None] = {}
    for key, sec in (("pc_m5", 300), ("pc_m30", 1800)):
        p0 = back(sec)
        out[key] = last / p0 - 1 if p0 and p0 > 0 and last > 0 else None
    out["vol_m5_usd"] = sum(float(r[5] or 0) for r in rows if r[0] > now - 300)
    return out


async def gecko_candles(pool: str, http: aiohttp.ClientSession | None = None) -> list[list[float]] | None:
    url = f"{GECKO}/pools/{pool}/ohlcv/minute?aggregate=1&limit=40&currency=usd"
    own = http is None
    http = http or aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8))
    try:
        async with http.get(url, headers={"accept": "application/json"}) as r:
            if r.status != 200:
                return None
            data = await r.json()
        return ((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
    except Exception as exc:  # noqa: BLE001
        log.debug("признаки: свечи %s: %s", pool, exc)
        return None
    finally:
        if own:
            await http.close()


def build(data: dict[str, Any], info: dict[str, Any] | None, sec: dict[str, Any] | None,
          tagged: dict[str, dict[str, Any] | None], candles: list[list[float]] | None, sol_usd: float, now: float,
          chain_sol: float | None = None, on_curve: bool | None = None, rug: dict[str, Any] | None = None
          ) -> dict[str, Any]:
    """Признаки из ответов источников (каждый может быть None)."""
    tr = data.get("trend") or {}
    info, sec, rug = info or {}, sec or {}, rug or {}
    f: dict[str, Any] = {"at": round(now), "src": []}
    # на момент сигнала (DexScreener / RPC трендов)
    f.update(sig_pc_m5=_f(tr.get("pc_m5")), sig_pc_h1=_f(tr.get("pc_h1")), sig_vol_m5_usd=_f(tr.get("vol_m5")),
             sig_liq_usd=_f(tr.get("liq")), sig_top10=_f((data.get("metrics") or {}).get("top10")),
             sig_buyers=tr.get("buyers"), sig_sellers=tr.get("sellers"), cluster_id=data.get("cluster_id"),
             pair_age_h=_f(tr.get("age_h")), dex=tr.get("dex"))
    if info:
        f["src"].append("gmgn_info")
    if sec:
        f["src"].append("gmgn_security")
    # 1) кластер дева и топ-10
    f["dev_team_hold"] = info.get("dev_team_hold") if info.get("dev_team_hold") is not None \
        else sec.get("dev_team_hold_rate")
    f["creator_hold"] = info.get("creator_hold") if info.get("creator_hold") is not None \
        else sec.get("creator_balance_rate")
    f["insider_hold_rate"] = sec.get("insider_hold_rate")
    f["top10"] = info.get("top10") if info.get("top10") is not None else sec.get("top10_rate")
    # 2) продажи дева и кластера, 3) бандлы и снайперы
    f["dev_status"] = info.get("creator_status") or sec.get("creator_token_status")
    for name, _tag in TAGS:
        t = tagged.get(name)
        if t is None:
            continue
        f["src"].append(f"gmgn_{name}")
        f[f"{name}_n"], f[f"{name}_hold"], f[f"{name}_sold"], f[f"{name}_exited"] = \
            t.get("n"), t.get("hold"), t.get("sold"), t.get("exited")
    for name in ("snipers", "bundlers", "insiders"):
        if f.get(f"{name}_n") is None and info.get(f"{name}_n") is not None:
            f[f"{name}_n"] = info.get(f"{name}_n")
    f["bundler_volume"] = info.get("bundler_volume") if info.get("bundler_volume") is not None \
        else sec.get("bundler_volume_rate")
    f["insider_volume"] = info.get("insider_volume") if info.get("insider_volume") is not None \
        else sec.get("insider_volume_rate")
    f["snipers_count"] = sec.get("sniper_count")
    f["fresh_rate"] = info.get("fresh_rate")
    # 4) холдеры
    f["holders"] = info.get("holders") if info.get("holders") is not None else rug.get("total_holders")
    # 5) рост за 5 и 30 мин до входа
    g = candle_growth(candles, now) if candles else {}
    if candles:
        f["src"].append("gecko")
    pc5 = g.get("pc_m5")
    if pc5 is None and _f(info.get("price")) and _f(info.get("price_5m")):
        pc5 = float(info["price"]) / float(info["price_5m"]) - 1
    f["pc_m5"], f["pc_m30"] = pc5, g.get("pc_m30")
    f["pc_h1"] = (float(info["price"]) / float(info["price_1h"]) - 1
                  if _f(info.get("price")) and _f(info.get("price_1h")) else None)
    # 6) объём за 5 мин / SOL в пуле
    vol = g.get("vol_m5_usd") if g.get("vol_m5_usd") is not None else _f(info.get("volume_5m"))
    pool_sol = chain_sol
    if pool_sol is None:
        if str(info.get("pool_quote") or "").upper() in SOL_SYMBOLS and _f(info.get("pool_quote_reserve")):
            pool_sol = float(info["pool_quote_reserve"])
        elif _f(info.get("pool_quote_usd")) and sol_usd > 0:
            pool_sol = float(info["pool_quote_usd"]) / sol_usd
    f["vol_m5_usd"], f["pool_sol"] = vol, pool_sol
    f["vol_m5_to_pool"] = vol / sol_usd / pool_sol if vol is not None and pool_sol and sol_usd > 0 else None
    # 7) возраст монеты и сколько прошло с переезда
    created = _f(info.get("created_ts")) or _f(info.get("open_ts"))
    f["coin_age_h"] = (now - created) / 3600 if created else None
    migrated = info.get("launchpad_status") == 2 or (on_curve is False and info.get("launchpad") == "pump")
    if on_curve is True:
        f["on_curve"], f["since_migr_h"] = True, None
    else:
        f["on_curve"] = False if migrated or on_curve is False else None
        t_migr = None
        if migrated and info.get("pool_created_ts") and (not info.get("migrated_pool")
                                                         or info.get("migrated_pool") == info.get("pool")):
            t_migr = float(info["pool_created_ts"])
        f["since_migr_h"] = (now - t_migr) / 3600 if t_migr else (
            f["pair_age_h"] if migrated and str(f.get("dex") or "").lower() in ("pumpswap", "pump-amm") else None)
    f["launchpad"] = info.get("launchpad")
    f["pool_exchange"] = info.get("pool_exchange")
    return f


async def collect(ext: Any, mint: str, data: dict[str, Any], sol_usd: float, *, chain_sol: float | None = None,
                  on_curve: bool | None = None, rug: dict[str, Any] | None = None) -> dict[str, Any]:
    """Опросить источники параллельно и собрать признаки. Ошибка источника — его поля None."""
    now = time.time()
    pair = (data.get("trend") or {}).get("pair")

    async def none() -> None:
        return None

    calls = [ext.gmgn_token_info(mint) if hasattr(ext, "gmgn_token_info") else none(),
             ext.gmgn_security(mint) if hasattr(ext, "gmgn_security") else none(),
             *(ext.gmgn_tagged(mint, tag) if hasattr(ext, "gmgn_tagged") else none() for _n, tag in TAGS),
             gecko_candles(pair) if pair else none()]
    res = await asyncio.gather(*calls, return_exceptions=True)
    res = [None if isinstance(x, BaseException) else x for x in res]
    info, sec, *rest = res
    tagged = {name: rest[i] for i, (name, _tag) in enumerate(TAGS)}
    candles = rest[len(TAGS)]
    return build(data, info, sec, tagged, candles, sol_usd, now, chain_sol=chain_sol, on_curve=on_curve, rug=rug)
