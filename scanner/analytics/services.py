"""Служебные адреса, через которые склеиваются кластеры (блок A5).

Источники склейки, найденные на данных 3 ноября 2026 (кластер №3 — 3014 запусков за день):
* общий «финансист» — биржа, обменник, бот-кошелёк, сервис запуска финансирует сотни деплоеров;
* сборщики комиссий и чаевых — деплоеры переводят им SOL перед запуском (Jito tips, боты, сервисы);
* сервис запуска как подписант — один кошелёк создаёт тысячи монет за чужих создателей;
* одна картинка у копий популярной монеты — повтор метаданных у разных девов;
* служебные аккаунты Pump.fun (global, fee recipients, event authority, migration).

Такие адреса помечаются в wallets.role = 'service' | 'exchange' с причиной и источником и не
участвуют в кластеризации.
"""
from __future__ import annotations

import logging
from typing import Any

from ..db.pool import Database

log = logging.getLogger(__name__)

# Служебные аккаунты Pump.fun и чаевые Jito (публично известные адреса)
STATIC_SERVICES: dict[str, str] = {
    "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf": "Pump.fun global",
    "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1": "Pump.fun event authority",
    "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM": "Pump.fun fee recipient",
    "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM": "Pump.fun mint authority",
    "39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg": "Pump.fun migration authority",
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5": "Jito tip",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe": "Jito tip",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY": "Jito tip",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49": "Jito tip",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh": "Jito tip",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt": "Jito tip",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL": "Jito tip",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT": "Jito tip",
    "11111111111111111111111111111111": "System program",
}

# Пороги динамического распознавания (за 7 дней)
FUNDER_MAX_DEPLOYERS = 7        # финансист больше чем 7 деплоеров — сервис/биржа
COLLECTOR_MIN_DEPLOYERS = 8     # получает SOL от 8+ деплоеров — сборщик комиссий
LAUNCH_SERVICE_PER_DAY = 30     # создаёт 30+ монет в день за других создателей
GENERIC_IMAGE_DEPLOYERS = 3     # картинка у 3+ разных девов — копии, не признак связи
HUB_MIN_CLUSTERS = 3            # кошелёк-член 3+ кластеров — узел склейки


async def mark(db: Database, address: str, role: str, reason: str, source: str = "heuristic") -> None:
    await db.execute(
        """INSERT INTO wallets(address, role, role_reason, role_source, is_exchange, checked_at)
           VALUES ($1, $2, $3, $4, $5, now())
           ON CONFLICT (address) DO UPDATE SET
               role = CASE WHEN wallets.role IN ('exchange', 'service') AND EXCLUDED.role = 'service'
                           THEN wallets.role ELSE EXCLUDED.role END,
               role_reason = CASE WHEN wallets.role_reason LIKE 'сервис запуска%' THEN wallets.role_reason
                                  ELSE EXCLUDED.role_reason END,
               role_source = EXCLUDED.role_source,
               is_exchange = wallets.is_exchange OR EXCLUDED.is_exchange""",
        address, role, reason[:200], source, role == "exchange")


async def service_set(db: Database) -> set[str]:
    rows = await db.fetch("SELECT address FROM wallets WHERE role IN ('service', 'exchange') OR is_exchange")
    return set(STATIC_SERVICES) | {r["address"] for r in rows}


async def launch_services(db: Database) -> set[str]:
    rows = await db.fetch("SELECT address FROM wallets WHERE role = 'service' AND role_reason LIKE 'сервис запуска%'")
    return {r["address"] for r in rows}


async def is_service(db: Database, address: str | None) -> bool:
    if not address:
        return False
    if address in STATIC_SERVICES:
        return True
    row = await db.fetchrow("SELECT role, is_exchange FROM wallets WHERE address=$1", address)
    return bool(row and (row["role"] in ("service", "exchange") or row["is_exchange"]))


async def generic_image(db: Database, image: str | None) -> bool:
    if not image:
        return True
    n = await db.fetchval(
        "SELECT count(DISTINCT deployer) FROM tokens WHERE image=$1 AND created_at > now() - interval '7 days'", image)
    return (n or 0) >= GENERIC_IMAGE_DEPLOYERS


async def detect(db: Database) -> list[dict[str, Any]]:
    """Находит служебные адреса по данным базы и помечает их. Возвращает список найденных."""
    found: list[dict[str, Any]] = []
    for addr, name in STATIC_SERVICES.items():
        await mark(db, addr, "service", name, "static")
    # 1) финансисты многих деплоеров
    for r in await db.fetch(
            """SELECT w.funded_by addr, count(DISTINCT w.address) n FROM wallets w
               JOIN tokens t ON t.deployer = w.address
               WHERE w.funded_by IS NOT NULL AND t.created_at > now() - interval '7 days'
               GROUP BY 1 HAVING count(DISTINCT w.address) > $1""", FUNDER_MAX_DEPLOYERS):
        found.append({"address": r["addr"], "role": "service", "reason": f"финансировал {r['n']} деплоеров"})
    # 2) сборщики: получают SOL от многих деплоеров (комиссии ботов, сервисы, чаевые)
    for r in await db.fetch(
            """SELECT s.dst addr, count(DISTINCT s.src) n FROM sol_transfers s
               WHERE s.ts > now() - interval '7 days'
                 AND s.src IN (SELECT DISTINCT deployer FROM tokens WHERE created_at > now() - interval '7 days')
               GROUP BY 1 HAVING count(DISTINCT s.src) >= $1""", COLLECTOR_MIN_DEPLOYERS):
        found.append({"address": r["addr"], "role": "service", "reason": f"получал SOL от {r['n']} деплоеров"})
    # 3) сервисы запуска: один подписант, много монет за разных создателей
    for r in await db.fetch(
            """SELECT deployer addr, count(*) n, count(DISTINCT current_owner) owners FROM tokens
               WHERE created_at > now() - interval '1 day' GROUP BY 1 HAVING count(*) >= $1""",
            LAUNCH_SERVICE_PER_DAY):
        if r["owners"] > 1:
            found.append({"address": r["addr"], "role": "service",
                          "reason": f"сервис запуска: {r['n']} монет/сутки за {r['owners']} создателей"})
    # 4) узлы склейки: автоматический член многих кластеров
    for r in await db.fetch(
            """SELECT wallet addr, count(DISTINCT cluster_id) n FROM cluster_links
               WHERE status = 'auto' AND reason NOT IN ('исходный деплоер', 'деплоер', 'создатель')
               GROUP BY 1 HAVING count(DISTINCT cluster_id) >= $1""", HUB_MIN_CLUSTERS):
        found.append({"address": r["addr"], "role": "service", "reason": f"связан с {r['n']} кластерами"})
    # биржи из внешних меток и эвристики по числу транзакций уже лежат в wallets.is_exchange
    for f in found:
        await mark(db, f["address"], f["role"], f["reason"])
    return found
