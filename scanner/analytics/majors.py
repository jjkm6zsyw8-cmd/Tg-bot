"""«Крупные монеты» (24.11, владелец: «надо проверять» — после разбора отчёта: на свежих монетах из платных промо
DexScreener почти всё — скам или клоны, у заблокированных пулов плюс давали клоны с 9–31 держателем).

Источник — списки Jupiter (tokens/v2): «в тренде» за 5 мин и за 1 ч и «органические» за 5 мин, по 100 монет. Берём
только монеты с живой аудиторией и глубоким пулом (quality): от MIN_HOLDERS держателей, ликвидность от MIN_LIQ_USD,
старше MIN_AGE_H, объём «органический» (Jupiter отличает настоящих покупателей от накрутки), допечатка и заморозка
выключены, топ-холдеры не больше MAX_TOP_HOLDERS. Вход — монета растёт прямо сейчас (trigger): за 5 мин +1.5…+10%,
за час в плюсе, покупают больше, чем продают. Сигнал — в реальный бот (метод «majorj»): только тень, по настоящим
котировкам Jupiter; продажа — правила E / F под небольшие движения (settings.EXTRA_RULES).
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

MAJOR_TARGET = "MJ"
LISTS = (("toptrending", "5m"), ("toptrending", "1h"), ("toporganicscore", "5m"))
EVERY_S = 60.0
COOLDOWN_S = 45 * 60                  # одна монета — не чаще раза в 45 мин
MIN_HOLDERS, MIN_LIQ_USD, MIN_AGE_H, MAX_TOP_HOLDERS = 1000, 100_000.0, 24.0, 50.0
ORGANIC_OK = ("high", "medium")
PC5_MIN, PC5_MAX = 1.5, 10.0          # рост за 5 мин, %
BUY_SELL_MIN = 1.3                    # объём покупок / продаж за 5 мин
MIN_BUYS_5M, MIN_TRADERS_5M = 10, 8
EXCLUDE = {"SOL", "WSOL", "USDC", "USDT", "USD1", "PYUSD", "USDS", "USDG", "EURC", "USDE", "FDUSD"}


def _f(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def age_h(x: dict[str, Any], now: float | None = None) -> float | None:
    created = (x.get("firstPool") or {}).get("createdAt") or x.get("createdAt")
    if not created:
        return None
    try:
        t = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
    except ValueError:
        return None
    return ((now or time.time()) - t.timestamp()) / 3600


def quality(x: dict[str, Any], now: float | None = None) -> str | None:
    """None — монета «крупная» (живая аудитория, глубокий пул, без прав у создателя), иначе — почему нет."""
    sym = str(x.get("symbol") or "").upper()
    tags = {str(t).lower() for t in x.get("tags") or []}
    if sym in EXCLUDE or tags & {"lst", "stablecoin"}:
        return "стейблкоин / SOL"
    if _f(x.get("holderCount")) < MIN_HOLDERS:
        return "мало держателей"
    if _f(x.get("liquidity")) < MIN_LIQ_USD:
        return "мелкий пул"
    a = age_h(x, now)
    if a is None or a < MIN_AGE_H:
        return "моложе суток"
    if str(x.get("organicScoreLabel") or "").lower() not in ORGANIC_OK:
        return "объём накручен (Jupiter: низкий органический)"
    audit = x.get("audit") or {}
    if not (audit.get("mintAuthorityDisabled") is True or not x.get("mintAuthority")):
        return "создатель может допечатать"
    if not (audit.get("freezeAuthorityDisabled") is True or not x.get("freezeAuthority")):
        return "создатель может заморозить"
    top = audit.get("topHoldersPercentage")
    if top is not None and _f(top) > MAX_TOP_HOLDERS:
        return "топ-холдеры держат больше половины"
    return None


def trigger(x: dict[str, Any]) -> str | None:
    """None — растёт прямо сейчас (вход), иначе — почему не сейчас."""
    s5, s1 = x.get("stats5m") or {}, x.get("stats1h") or {}
    pc5 = _f(s5.get("priceChange"))
    if not PC5_MIN <= pc5 <= PC5_MAX:
        return "за 5 мин не растёт" if pc5 < PC5_MIN else "за 5 мин уже улетела"
    if _f(s1.get("priceChange")) <= 0:
        return "за час не в плюсе"
    if _f(s5.get("buyVolume")) < BUY_SELL_MIN * max(1.0, _f(s5.get("sellVolume"))):
        return "покупают не больше, чем продают"
    if _f(s5.get("numBuys")) < MIN_BUYS_5M or _f(s5.get("numTraders")) < MIN_TRADERS_5M:
        return "мало покупок"
    return None


class Majors:
    """Раз в минуту — списки Jupiter → отбор → сигнал «крупная монета растёт» в реальный бот (тень)."""

    def __init__(self, engine: Any):
        self.e = engine
        self.cool: dict[str, float] = {}
        self.stats: dict[str, int] = {"ticks": 0, "tokens": 0, "quality": 0, "signals": 0}

    async def run(self) -> None:
        await asyncio.sleep(30)
        while True:
            try:
                await self.tick()
            except Exception:  # noqa: BLE001 — сканер не должен ронять аналитику
                log.exception("крупные монеты")
            await asyncio.sleep(EVERY_S)

    async def tick(self, now: float | None = None) -> list[str]:
        ext, real = getattr(self.e, "ext", None), getattr(self.e, "real", None)
        if ext is None or real is None or not hasattr(ext, "jup_tokens"):
            return []
        now = now or time.time()
        toks: dict[str, dict[str, Any]] = {}
        for cat, interval in LISTS:
            for x in await ext.jup_tokens(cat, interval) or []:
                if isinstance(x, dict) and x.get("id"):
                    toks.setdefault(str(x["id"]), x)
        self.stats["ticks"] += 1
        self.stats["tokens"] = len(toks)
        sent = []
        good = 0
        for m, x in toks.items():
            if quality(x, now):
                continue
            good += 1
            if trigger(x) or now - self.cool.get(m, 0.0) < COOLDOWN_S:
                continue
            self.cool[m] = now
            a = age_h(x, now) or 0.0
            data = {"modes": ["major"], "symbol": str(x.get("symbol") or "")[:20], "trend_at": now, "trigger_at": now,
                    "age_s": a * 3600,
                    "trend": {"liq": _f(x.get("liquidity")), "holders": int(_f(x.get("holderCount"))),
                              "organic": x.get("organicScoreLabel"), "mcap": _f(x.get("mcap")),
                              "pc_m5": _f((x.get("stats5m") or {}).get("priceChange")),
                              "pc_h1": _f((x.get("stats1h") or {}).get("priceChange"))}}
            asyncio.create_task(real.on_signal(m, "normal", None, data, None, {MAJOR_TARGET}))
            sent.append(m)
        self.stats["quality"] = good
        self.stats["signals"] += len(sent)
        for k in [k for k, t in self.cool.items() if now - t > 6 * 3600]:
            self.cool.pop(k, None)
        return sent
