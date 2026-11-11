"""Рейтинг кошельков, за которыми бот следит и которых копирует (свои, найденные разведкой, smart-money).

Оценка — по тому, что дали копии этого кошелька на бумаге: последние 20 закрытых копий в эталонных портфелях
(G и испытательный H — одна и та же схема), только сделки, цена которых сошлась с рынком (не ⚠).
score = сумма результатов / (копий + 4): средний результат копии, «стянутый» к нулю, пока копий мало, — один
удачный икс не делает кошелёк лучшим, одна неудача не отправляет на паузу. Результат копии выше +100% считается
как +100% (выброс не решает за весь кошелёк).

Уровни и что они меняют (пороги — в /settings):
  🟢 топ    — score ≥ +4% на копию и 5+ копий: позиция ×1.5;
  🟡 норм   — score ≥ 0: обычная позиция;
  ⚪ новый  — меньше 3 копий: позиция ×0.75, пока не наберётся история;
  🟠 слабый — score от −3% до 0: позиция ×0.5;
  🔴 пауза  — score ниже −3% и 3+ копии: в боевые портфели не копируется, только в теневой H (по $10) —
              если там исправится, вернётся сам.
Пороги проверены «вперёд по времени» на 8–9.11 (решение — только по прошлым копиям): кошельки с оценкой ниже нуля
дальше теряли −8.7% на копию (−$90 из −$108 всех одиночных копий), остальные — около нуля.
Ручная правка: /rating адрес топ|норм|пауза|авто — «авто» возвращает автоматическую оценку.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from ..alerts.format import esc

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger(__name__)

REF_PORTFOLIOS = ("G", "H")
WINDOW = 20
SHRINK = 4
EVERY_S = 300
DEFAULTS: dict[str, Any] = {
    "rating_top": 0.04,          # score для «топ» (и 5+ копий)
    "rating_ok": 0.0,            # ниже — «слабый»
    "rating_pause": -0.03,       # ниже — пауза
    "rating_min_copies": 3,      # копий для паузы (до этого — «новый»)
    "rating_top_mult": 1.5,
    "rating_new_mult": 0.75,
    "rating_weak_mult": 0.5,
}
TIERS = {"top": "🟢 топ", "ok": "🟡 норм", "new": "⚪ новый", "weak": "🟠 слабый", "paused": "🔴 пауза"}
MANUAL = {"топ": "top", "top": "top", "норм": "ok", "ok": "ok", "пауза": "paused", "pause": "paused",
          "слабый": "weak", "weak": "weak"}


def cfg_get(cfg: dict[str, Any], key: str) -> float:
    v = cfg.get(key)
    return float(DEFAULTS[key] if v is None else v)


def score_of(pcts: list[float]) -> float:
    return sum(pcts) / (len(pcts) + SHRINK)


TOP_MIN_COPIES = 5


def tier_of(pcts: list[float], cfg: dict[str, Any]) -> str:
    n, s = len(pcts), score_of(pcts)
    need = int(cfg_get(cfg, "rating_min_copies"))
    if n >= need and s < cfg_get(cfg, "rating_pause"):
        return "paused"
    if n < need:
        return "new"
    if n >= TOP_MIN_COPIES and s >= cfg_get(cfg, "rating_top"):
        return "top"
    if s >= cfg_get(cfg, "rating_ok"):
        return "ok"
    return "weak"


def mult_of(tier: str, cfg: dict[str, Any]) -> float:
    return {"top": cfg_get(cfg, "rating_top_mult"), "ok": 1.0, "new": cfg_get(cfg, "rating_new_mult"),
            "weak": cfg_get(cfg, "rating_weak_mult"), "paused": 0.0}.get(tier, 1.0)


async def compute(db: Any, cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Рейтинг всех кошельков с копиями: адрес → {tier, mult, score, n, wins, sum, manual}."""
    rows = await db.fetch(
        """SELECT source_wallet w, pnl_pct FROM (
               SELECT source_wallet, pnl_pct, row_number() OVER (PARTITION BY source_wallet ORDER BY closed_at DESC) rn
               FROM paper_positions
               WHERE source_wallet IS NOT NULL AND portfolio = ANY($1::text[]) AND closed_at IS NOT NULL
                 AND exit_reason <> 'reset' AND NOT suspect AND closed_at > now() - interval '14 days') x
           WHERE rn <= $2""", list(REF_PORTFOLIOS), WINDOW)
    from .origin import capped
    by: dict[str, list[float]] = {}
    for r in rows:                      # икс выше +100% считается как +100%: одна ракета не делает кошелёк «топом»
        by.setdefault(r["w"], []).append(capped(float(r["pnl_pct"] or 0)))
    manual = {r["address"]: r["manual_tier"] for r in await db.fetch(
        "SELECT address, manual_tier FROM wallet_rating WHERE manual_tier IS NOT NULL")}
    out: dict[str, dict[str, Any]] = {}
    for w in set(by) | set(manual):
        pcts = by.get(w, [])
        auto = tier_of(pcts, cfg)
        tier = manual.get(w) or auto
        out[w] = {"tier": tier, "auto": auto, "mult": mult_of(tier, cfg), "score": score_of(pcts), "n": len(pcts),
                  "wins": sum(p > 0 for p in pcts), "sum": sum(pcts), "manual": w in manual}
    return out


async def save(db: Any, rating: dict[str, dict[str, Any]]) -> None:
    await db.p.executemany(
        """INSERT INTO wallet_rating(address, tier, auto_tier, score, copies, wins, sum_pct, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, now())
           ON CONFLICT (address) DO UPDATE SET tier=EXCLUDED.tier, auto_tier=EXCLUDED.auto_tier, score=EXCLUDED.score,
               copies=EXCLUDED.copies, wins=EXCLUDED.wins, sum_pct=EXCLUDED.sum_pct, updated_at=now()""",
        [(w, r["tier"], r["auto"], r["score"], r["n"], r["wins"], r["sum"]) for w, r in rating.items()])


class Rating:
    def __init__(self, engine: "Engine"):
        self.e = engine
        self.db = engine.db
        self.last: dict[str, str] = {}

    async def refresh(self) -> list[str]:
        rating = await compute(self.db, self.e.cfg)
        await save(self.db, rating)
        self.e.rating = rating
        changes = []
        for w, r in rating.items():
            old = self.last.get(w)
            if old and old != r["tier"] and "paused" in (old, r["tier"]):
                label = (self.e.traders.get(w) or {}).get("label") if hasattr(self.e, "traders") else None
                changes.append(f"{TIERS[r['tier']]} ← {TIERS.get(old, old)}: {esc(label or w[:4] + '…' + w[-4:])} "
                               f"({r['n']} копий, в среднем {r['score']:+.0%})")
        self.last = {w: r["tier"] for w, r in rating.items()}
        return changes

    async def loop(self) -> None:
        await asyncio.sleep(60)
        first = True
        while True:
            try:
                changes = await self.refresh()
                if changes and not first:
                    await self.db.system("📊 <b>Рейтинг кошельков</b>\n" + "\n".join(changes[:20]))
                first = False
            except Exception:  # noqa: BLE001 — рейтинг не должен ронять аналитику
                log.exception("рейтинг кошельков")
            await asyncio.sleep(EVERY_S)


async def rating_report(db: Any, cfg: dict[str, Any], limit: int = 40) -> str:
    rating = await compute(db, cfg)
    labels = {r["address"]: r["label"] for r in await db.fetch(
        """SELECT address, label FROM watchlist WHERE label IS NOT NULL
           UNION ALL SELECT address, label FROM traders WHERE label IS NOT NULL""")}
    order = {"top": 0, "ok": 1, "new": 2, "weak": 3, "paused": 4}
    rows = sorted(rating.items(), key=lambda kv: (order.get(kv[1]["tier"], 9), -kv[1]["score"]))
    cnt: dict[str, int] = {}
    for _, r in rows:
        cnt[r["tier"]] = cnt.get(r["tier"], 0) + 1
    lines = ["📊 <b>Рейтинг кошельков</b> · по последним 20 копиям (G и H, только сверенные с рынком)",
             " · ".join(f"{TIERS[t]} {cnt[t]}" for t in order if t in cnt) or "копий пока нет"]
    for w, r in rows[:limit]:
        name = esc(labels.get(w) or f"{w[:4]}…{w[-4:]}")
        lines.append(f"{TIERS[r['tier']]}{' ✋' if r['manual'] else ''} {name}: {r['n']} копий, в плюсе {r['wins']}, "
                     f"в среднем {r['score']:+.1%} → позиция ×{r['mult']:g}"
                     + (f"\n   <code>{w}</code>" if r["tier"] in ("paused", "weak") else ""))
    lines.append("<i>Пауза — копии только в теневой H, вернётся сам. Вручную: /rating адрес топ|норм|пауза|авто</i>")
    return "\n".join(lines)
