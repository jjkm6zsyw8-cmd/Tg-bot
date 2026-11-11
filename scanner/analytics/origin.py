"""Откуда кошелёк: Axiom, Pump.fun, GMGN — помечается при добавлении (/addwallet, /import) или командой /origin;
кошельки разведки и автоотбора smart-money бот помечает сам. По пометке отчёты показывают, какой источник даёт
кошельки, на которых бумага в плюсе.

Выбросы: одна сделка ×3 тянет среднее сильнее десятка обычных. В оценках (рейтинг кошельков, автофильтр, «что
отличает плюсовые колы») результат сделки ограничивается +100% (OUTLIER); сами иксы в отчётах показываются отдельно.
"""
from __future__ import annotations

from typing import Any

ORIGINS = {"axiom": "Axiom", "pumpfun": "Pump.fun", "gmgn": "GMGN", "other": "другое"}
ALIASES = {"axiom": "axiom", "аксиом": "axiom", "акси": "axiom",
           "pump": "pumpfun", "pumpfun": "pumpfun", "pump.fun": "pumpfun", "помп": "pumpfun", "помпфан": "pumpfun",
           "пампфан": "pumpfun", "памп": "pumpfun",
           "gmgn": "gmgn", "гмгн": "gmgn",
           "other": "other", "другое": "other"}
AUTO_SMART = "бот сам (smart)"
UNMARKED = "не помечен"
OUTLIER = 1.0          # +100%: выше — «икс», в оценках считается как +100%


def capped(p: float) -> float:
    return min(float(p), OUTLIER)


def parse_origin(word: str | None) -> str | None:
    return ALIASES.get((word or "").strip().lower().lstrip("#")) if word else None


async def set_origin(db: Any, addresses: list[str], origin: str) -> None:
    await db.p.executemany(
        """INSERT INTO wallet_origin(address, origin, updated_at) VALUES ($1, $2, now())
           ON CONFLICT (address) DO UPDATE SET origin = EXCLUDED.origin, updated_at = now()""",
        [(a, origin) for a in addresses])


async def origins(db: Any) -> dict[str, str]:
    """Адрес → подпись источника. Ручная пометка важнее; дальше — разведка и автоотбор smart."""
    out: dict[str, str] = {}
    for r in await db.fetch("SELECT address FROM traders WHERE NOT manual AND (active OR qualified)"):
        out[r["address"]] = AUTO_SMART
    for r in await db.fetch("SELECT address, source FROM scout WHERE status IN ('promoted', 'probation', 'passed')"):
        src = "GMGN" if str(r["source"] or "").lower().startswith("gmgn") else "своя база"
        out[r["address"]] = f"разведка ({src})"
    for r in await db.fetch("SELECT address, origin FROM wallet_origin"):
        out[r["address"]] = ORIGINS.get(r["origin"], r["origin"])
    return out


async def own_wallets(db: Any) -> list[str]:
    """Свой список: добавленные вручную и импортом."""
    rows = await db.fetch("""SELECT address FROM watchlist WHERE kind = 'trader'
                             UNION SELECT address FROM traders WHERE manual""")
    return [r["address"] for r in rows]
