"""/wallets — файл по всем кошелькам: свои, найденные разведкой, на испытании, отсеянные, smart-money.

По каждому: откуда взялся, статистика GMGN за 7 дней (если есть ключ), своя статистика по базе сделок и главное —
итог копий в бумаге (G — эталон для своих и разведки, F — для smart-money, H — испытательный срок).
Колонка «вердикт»: что с кошельком делать по итогу копий.
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

STATUS = {"queued": "в очереди на испытание", "probation": "на испытании", "promoted": "в своём списке (разведка)",
          "passed": "прошёл испытание, список полон", "rejected": "отсеян", "few": "мало сделок на испытании",
          "demoted": "убран: копии в минусе", "removed": "убран вручную"}
HEAD = ["тип", "статус", "вердикт", "метка", "адрес", "откуда", "почему попал", "найден", "решение",
        "копий в бумаге", "портфель", "в плюсе", "винрейт копий %", "итог копий SOL", "итог копий $",
        "средняя копия %", "лучшая %", "худшая %", "копий на испытании (H)", "итог испытания %",
        "GMGN винрейт 7д %", "GMGN прибыль 7д $", "GMGN монет 7д", "GMGN удержание, с", "GMGN сливов хуже −50%",
        "сделок 30д (своя база)", "копируемость 30д %", "винрейт 30д %", "медиана удержания, с",
        "последняя активность", "GMGN"]


def _pct(v: Any) -> Any:
    return "" if v is None else round(float(v) * 100, 1)


def _num(v: Any, nd: int = 4) -> Any:
    return "" if v is None else round(float(v), nd)


def verdict_of(kind: str, n: int, wins: int, pnl: float) -> str:
    if n < 5:
        return "мало копий — ждём"
    if pnl > 0 and wins / n >= 0.4:
        return "✅ держать"
    if pnl > 0:
        return "🟡 в плюсе за счёт редких иксов"
    return "❌ шлак — убрать" if kind == "свой" else "❌ в минусе"


async def wallets_csv(db: Any, sol_usd: float, tz: str = "Asia/Tbilisi") -> tuple[str, int]:
    try:
        zone = ZoneInfo(tz)
    except Exception:  # noqa: BLE001
        zone = ZoneInfo("UTC")
    when = lambda t: t.astimezone(zone).strftime("%Y-%m-%d %H:%M") if isinstance(t, datetime) else ""
    wallets: dict[str, dict[str, Any]] = {}
    for r in await db.fetch("SELECT kind, address, label, created_at FROM watchlist WHERE kind IN ('trader', 'dev')"):
        wallets.setdefault(r["address"], {"kind": "свой" if r["kind"] == "trader" else "свой дев",
                                          "label": r["label"], "status": "в своём списке", "found": r["created_at"]})
    for r in await db.fetch("SELECT * FROM traders WHERE active OR manual"):
        w = wallets.setdefault(r["address"], {"kind": "свой" if r["manual"] else "smart-money",
                                              "status": "в своём списке" if r["manual"] else "smart-money (автоотбор)",
                                              "found": r["qualified_at"]})
        w["label"] = w.get("label") or r["label"]
        w["traders"] = r
    for r in await db.fetch(
            """SELECT * FROM scout WHERE status IN ('probation', 'promoted', 'passed', 'queued')
                  OR decided_at > now() - interval '14 days'"""):
        w = wallets.setdefault(r["address"], {})
        w.update({"source": r["source"], "reason": r["reason"], "decision": r["decision"], "gmgn": r["stats"] or {}})
        if r["status"] == "promoted" or not str(w.get("kind") or "").startswith("свой"):
            # добавленный вручную кошелёк остаётся «своим», даже если разведка его когда-то смотрела
            w.update({"kind": "разведка" if r["status"] == "promoted" else "кандидат",
                      "status": STATUS.get(r["status"], r["status"]), "found": r["found_at"]})
    papers: dict[tuple[str, str], dict[str, Any]] = {}
    for r in await db.fetch(
            """SELECT source_wallet w, portfolio, count(*) n, count(*) FILTER (WHERE pnl_sol > 0) wins,
                      sum(pnl_sol) pnl, sum(cost_sol) cost, avg(pnl_pct) avg, max(pnl_pct) best, min(pnl_pct) worst
               FROM paper_positions WHERE source_wallet IS NOT NULL AND closed_at IS NOT NULL
                 AND exit_reason <> 'reset' AND NOT suspect AND portfolio IN ('G', 'F', 'H')
               GROUP BY 1, 2"""):
        papers[(r["w"], r["portfolio"])] = dict(r)
    out = io.StringIO()
    wr = csv.writer(out, delimiter=";")
    wr.writerow(HEAD)
    rows = []
    for addr, w in wallets.items():
        port = "F" if w.get("kind") == "smart-money" else "G"
        p = papers.get((addr, port)) or papers.get((addr, "F" if port == "G" else "G")) or {}
        h = papers.get((addr, "H")) or {}
        n, wins, pnl = int(p.get("n") or 0), int(p.get("wins") or 0), float(p.get("pnl") or 0)
        t = w.get("traders") or {}
        g = w.get("gmgn") or {}
        kind = w.get("kind") or "кандидат"
        rows.append((pnl, [
            kind, w.get("status") or "", verdict_of(kind, n, wins, pnl) if kind != "кандидат" else "",
            w.get("label") or "", addr, w.get("source") or ("вручную" if kind.startswith("свой") else "автоотбор"),
            w.get("reason") or "", when(w.get("found")), w.get("decision") or "",
            n, p.get("portfolio") or "", wins, _pct(wins / n) if n else "", _num(pnl), round(pnl * sol_usd, 2),
            _pct(p.get("avg")), _pct(p.get("best")), _pct(p.get("worst")),
            int(h.get("n") or 0), _pct(float(h["pnl"]) / float(h["cost"])) if h.get("cost") else "",
            _pct(g.get("winrate")), _num(g.get("realized_profit"), 0), g.get("token_num") or "",
            _num(g.get("avg_hold_s"), 0), g.get("big_losses") if g.get("big_losses") is not None else "",
            t.get("trades_30d") or "", _pct(t.get("copy_30d")), _pct(t.get("win_rate")),
            _num(t.get("median_hold_s"), 0), when(t.get("last_active")), f"https://gmgn.ai/sol/address/{addr}",
        ]))
    for _, row in sorted(rows, key=lambda x: x[0]):          # сначала худшие — их убирать
        wr.writerow(row)
    return out.getvalue(), len(rows)
