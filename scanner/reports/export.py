"""/export и ежедневная выгрузка: один архив со всем, что нужно для разбора.

Внутри: сделки бумаги за сутки и за 30 дней (с отметкой самопроверки и путём цены), файл по кошелькам
(свои, разведка, кандидаты, smart-money с итогом копий), журнал копитрейда за 7 дней и памятка по колонкам.
Открывается в Numbers / Excel; целиком можно отправить на разбор.
"""
from __future__ import annotations

import base64
import csv
import io
import zipfile
from datetime import datetime, timezone
from typing import Any

from .wallets import wallets_csv

README = """Выгрузка бота «Pump.fun Dev Scanner»

paper_24h.csv   — все закрытые сделки бумаги за сутки (по портфелям A–H)
paper_30d.csv   — то же за 30 дней
wallets.csv     — все кошельки: свои, найденные разведкой, на испытании, отсеянные, smart-money;
                  итог их копий в бумаге и вердикт (сначала худшие)
copylog_7d.csv  — журнал копитрейда за 7 дней: что скопировано, что пропущено и почему

Колонки сделок:
  «проверка по рынку» — ок / ⚠ не в итоге (цена не сошлась с минутной свечой GeckoTerminal) / нет свечей / ещё нет
  «рост за 30 мин», «просадка до пика», «цена через 30 мин» — путь цены после входа (по свечам), для /calib
Портфели: G эталон (всё на +25%, стоп −8%), A стоп −15%, B 70% на +25% + трейлинг, C не продаёт за трейдером,
E всё на +40%, F только smart-money, D лотерея, H испытательный срок кандидатов разведки (по $10).
Файлы в UTF-8 с разделителем «;».
"""


async def copylog_csv(db: Any, days: int = 7) -> str:
    out = io.StringIO()
    w = csv.writer(out, delimiter=";")
    w.writerow(["время (UTC)", "кошелёк", "метка", "монета", "адрес монеты", "сторона", "SOL цели", "MC цели, SOL",
                "действие", "причина", "MC входа, SOL", "дельта входа %"])
    labels = {r["address"]: r["label"] for r in await db.fetch(
        "SELECT address, label FROM watchlist UNION ALL SELECT address, label FROM traders WHERE label IS NOT NULL")}
    for r in await db.fetch(
            "SELECT * FROM copy_log WHERE ts > now() - make_interval(days => $1) ORDER BY ts", days):
        w.writerow([r["ts"].strftime("%Y-%m-%d %H:%M:%S"), r["wallet"], labels.get(r["wallet"]) or "",
                    r["symbol"] or "", r["mint"], r["side"], round(r["sol"] or 0, 4), round(r["mc"] or 0, 2),
                    r["action"], r["reason"] or "", round(r["entry_mc"], 2) if r["entry_mc"] else "",
                    round(r["delta"] * 100, 1) if r["delta"] is not None else ""])
    return out.getvalue()


async def build_export(db: Any, paper: Any) -> tuple[bytes, str]:
    """(zip, подпись). paper — PaperTrader (бота или аналитики): им строятся CSV сделок."""
    sol_usd = paper.e.price.value or 150
    day = await paper.trades_csv(24)
    month = await paper.trades_csv(24 * 30)
    wallets, n_wallets = await wallets_csv(db, sol_usd, str(paper.cfg.get("paper_reset_tz") or "Asia/Tbilisi"))
    copylog = await copylog_csv(db)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, text in (("paper_24h.csv", day), ("paper_30d.csv", month), ("wallets.csv", wallets),
                           ("copylog_7d.csv", copylog), ("README.txt", README)):
            z.writestr(name, ("﻿" if name.endswith(".csv") else "") + text)   # BOM — Excel видит кириллицу
    n_day, n_month = max(0, day.count("\n") - 1), max(0, month.count("\n") - 1)
    caption = (f"сделок за сутки {n_day}, за 30 дней {n_month}, кошельков {n_wallets}. "
               "Можно открыть в Numbers / Excel или отправить на разбор целиком.")
    return buf.getvalue(), caption


async def enqueue_export(db: Any, paper: Any, title: str) -> None:
    data, caption = await build_export(db, paper)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await db.enqueue("zip", {"topic": "paper", "b64": base64.b64encode(data).decode(),
                             "filename": f"scanner_{stamp}.zip", "caption": f"{title}: {caption}"}, priority=6)
