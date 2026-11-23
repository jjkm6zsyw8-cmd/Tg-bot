"""Автонастройка реального бота (23.11, просьба владельца: «бот должен сам собирать данные, менять методы и присылать
отчёт, почему он это сделал; говно должен фильтровать сам»).

Раз в 6 часов — только по тени (настоящие цены в сети) за последние 3 дня:
  1. Метод в минусе при любом правиле продажи (пробы A/B/G/R/W/L на тех же входах) — новые входы метода
     останавливаются на 3 дня (и в тени, и в бою), потом метод снова проверяется: «а вдруг пойдёт».
  2. Источник в минусе — дев («мой дев») или кошелёк (копии, smart, ракеты) с 8+ закрытыми сделками, итог < 0 и в
     среднем хуже −5% — его сигналы не берутся 3 дня. Кошельки, за которыми ты сам включил «за ним», не трогаются.
  3. Правило продажи метода: если по пробам другое правило заметно лучше (на 2+ п.п. на сделку, 30+ сделок, в плюсе),
     метод переходит на него — только когда метод не торгует в бою (в бою — только совет в отчёте).
  4. «Новый дев»: окно цены входа — из корзин капитализации, где итог закрытых сделок в плюсе.
Отчёт «что поменял и почему» — в тему «Реальный бот». Последний — /autotune.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from ..alerts.format import esc
from . import settings as S

log = logging.getLogger(__name__)

BLOCK_KEY = "real:autotune_block"     # {"kinds": {kind: {...}}, "sources": {addr: {...}}}
LAST_KEY = "real:autotune_last"       # {"at": iso, "text": отчёт}
BAND_KEY = "real:newdev_band"         # [мин, макс] капитализации входа «нового дева», SOL
EVERY_S = 6 * 3600
WINDOW_D = 3
BLOCK_S = 3 * 86400
MIN_KIND_N = 40                       # метод: закрытых сделок для решения
MIN_RULE_N = 30                       # правило: проб для решения
MIN_SRC_N = 8                         # источник: закрытых сделок для решения
SRC_BAD_AVG = -0.05
RULE_EDGE = 0.02
BANDS = ((0, 35), (35, 45), (45, 57), (57, 80), (80, 100))
MIN_BAND_N = 10


def _pct(v: float) -> str:
    return f"{v * 100:+.1f}%"


async def run(db: Any, cfg: dict[str, Any], live_kinds: set[str], now: float | None = None) -> list[str]:
    """Один проход: решения записываются (kv и общие настройки), возвращаются строки отчёта."""
    now = now or time.time()
    since = datetime.fromtimestamp(now, timezone.utc) - timedelta(days=WINDOW_D)
    block = dict(await db.kv_get(BLOCK_KEY) or {})
    kinds_bl: dict[str, Any] = {k: v for k, v in (block.get("kinds") or {}).items() if float(v.get("until", 0)) > now}
    srcs_bl: dict[str, Any] = {k: v for k, v in (block.get("sources") or {}).items() if float(v.get("until", 0)) > now}
    expired = [k for k in (block.get("kinds") or {}) if k not in kinds_bl]
    expired_src = len(block.get("sources") or {}) - len(srcs_bl)
    lines: list[str] = []

    # ---------------------------------------------------------------- 1, 3: методы и правила продажи
    # итог метода — только сделки по его нынешнему правилу продажи: сменили подход (например «мой дев» → D) —
    # старый минус новый подход не останавливает
    main = {}
    for kind in S.METHODS:
        r = await db.fetchrow(
            """SELECT count(*) n, COALESCE(avg(pnl_pct), 0) a, COALESCE(sum(pnl_sol), 0) s FROM real_positions
               WHERE mode = 'shadow' AND closed_at IS NOT NULL AND closed_at > $1 AND kind = $2
                 AND COALESCE(extra->>'rule', '') = $3""", since, kind, S.get(cfg, S.SCHEME_KEYS[kind]))
        if r and r["n"]:
            main[kind] = r
    trials: dict[str, dict[str, Any]] = {}
    for r in await db.fetch(
            """SELECT kind, rule, count(*) n, COALESCE(avg(pnl_pct), 0) a FROM real_rule_trials
               WHERE closed_at IS NOT NULL AND closed_at > $1 GROUP BY kind, rule""", since):
        trials.setdefault(r["kind"], {})[r["rule"]] = r
    for kind in S.METHODS:
        m = main.get(kind)
        tr = {k: v for k, v in (trials.get(kind) or {}).items() if v["n"] >= MIN_RULE_N}
        name = S.KIND_NAMES.get(kind, kind)
        if kind in kinds_bl:
            continue
        if m and m["n"] >= MIN_KIND_N and float(m["a"]) < -0.03 and tr and all(float(v["a"]) < 0 for v in tr.values()):
            best = max(tr.values(), key=lambda v: float(v["a"]))
            kinds_bl[kind] = {"until": now + BLOCK_S, "why": f"{m['n']} сд в среднем {_pct(float(m['a']))}, лучшее "
                              f"правило {best['rule']} {_pct(float(best['a']))}"}
            lines.append(f"⛔ <b>{name}</b>: остановлен на 3 дня — в минусе при любом правиле продажи "
                         f"({m['n']} сд, в среднем {_pct(float(m['a']))}; лучшее правило {best['rule']} "
                         f"{_pct(float(best['a']))}). Через 3 дня — снова проверка.")
            continue
        if not tr:
            continue
        cur = S.get(cfg, S.SCHEME_KEYS[kind])
        best = max(tr.values(), key=lambda v: float(v["a"]))
        cur_a = float(tr[cur]["a"]) if cur in tr else None
        if best["rule"] != cur and float(best["a"]) > 0 and (cur_a is None or float(best["a"]) - cur_a >= RULE_EDGE):
            why = (f"{best['rule']} {_pct(float(best['a']))} на сделку ({best['n']} проб) против {cur} "
                   + (_pct(cur_a) if cur_a is not None else "— мало проб"))
            if kind in live_kinds:
                lines.append(f"💡 <b>{name}</b>: правило продажи лучше {best['rule']} — {why}. Метод в бою — сам не меняю, "
                             "переключи в 🎯 Методы.")
            else:
                await db.set_override(S.SCHEME_KEYS[kind], best["rule"])
                lines.append(f"🔁 <b>{name}</b>: продаю по правилу {best['rule']} вместо {cur} — {why}.")
    for k in expired:
        lines.append(f"🔄 <b>{S.KIND_NAMES.get(k, k)}</b>: 3 дня прошли — снова проверяется в тени.")

    # ---------------------------------------------------------------- 2: источники (девы и кошельки)
    follow = dict(await db.kv_get("real:follow") or {})
    auto = set(await db.kv_get("real:follow_auto") or [])
    mine = {a for a in follow if a not in auto}
    bad = []
    for r in await db.fetch(
            """SELECT trader, kind, count(*) n, COALESCE(avg(pnl_pct), 0) a, COALESCE(sum(pnl_sol), 0) s
               FROM real_positions WHERE mode = 'shadow' AND closed_at IS NOT NULL AND closed_at > $1
                 AND trader IS NOT NULL AND kind <> 'newdev'
               GROUP BY trader, kind HAVING count(*) >= $2""", since, MIN_SRC_N):
        if r["trader"] in srcs_bl or r["trader"] in mine:
            continue
        if float(r["s"]) < 0 and float(r["a"]) <= SRC_BAD_AVG:
            srcs_bl[r["trader"]] = {"until": now + BLOCK_S, "kind": r["kind"],
                                    "why": f"{r['n']} сд в среднем {_pct(float(r['a']))}"}
            bad.append(r)
    if bad:
        bad.sort(key=lambda r: float(r["s"]))
        shown = ", ".join(f"{r['trader'][:4]}…{r['trader'][-4:]} ({S.KIND_NAMES.get(r['kind'], r['kind'])}, "
                          f"{r['n']} сд, {_pct(float(r['a']))})" for r in bad[:8])
        lines.append(f"🚫 Источники в минусе — не беру их сигналы 3 дня: {len(bad)}. {shown}"
                     + (f" и ещё {len(bad) - 8}" if len(bad) > 8 else ""))
    if expired_src > 0:
        lines.append(f"🔄 Источников снова на проверке (3 дня прошли): {expired_src}")

    # ---------------------------------------------------------------- 4: окно цены входа «нового дева»
    rows = await db.fetch(
        """SELECT entry_mc, pnl_pct FROM real_positions WHERE kind = 'newdev' AND mode = 'shadow'
           AND closed_at IS NOT NULL AND closed_at > $1 AND entry_mc IS NOT NULL""", since)
    if rows:
        good = []
        stats = []
        for lo, hi in BANDS:
            xs = [float(r["pnl_pct"] or 0) for r in rows if lo <= float(r["entry_mc"]) < hi]
            if len(xs) >= MIN_BAND_N:
                avg = sum(xs) / len(xs)
                stats.append(f"{lo}–{hi}: {len(xs)} сд {_pct(avg)}")
                if avg > 0:
                    good.append((lo, hi))
        old = await db.kv_get(BAND_KEY)
        if good:
            band = [min(g[0] for g in good), max(g[1] for g in good)]
            if band != old:
                await db.kv_set(BAND_KEY, band)
                lines.append(f"🎯 <b>Новый дев</b>: покупаю только при капитализации {band[0]}–{band[1]} SOL — там итог в "
                             f"плюсе ({'; '.join(stats)}).")
        elif stats and old:
            await db.kv_set(BAND_KEY, None)
            lines.append(f"🎯 <b>Новый дев</b>: окна в плюсе нет ({'; '.join(stats)}) — меряю все цены входа.")

    await db.kv_set(BLOCK_KEY, {"kinds": kinds_bl, "sources": srcs_bl})
    head = (f"🤖 <b>Автонастройка</b> (тень за {WINDOW_D} дня): остановлено методов {len(kinds_bl)}, источников "
            f"{len(srcs_bl)}")
    text = "\n".join([head, *(lines or ["Изменений нет — данных для решений мало или всё в норме."])])
    await db.kv_set(LAST_KEY, {"at": datetime.fromtimestamp(now, timezone.utc).isoformat(), "text": text})
    return [text] if lines else []


async def last_text(db: Any) -> str:
    last = await db.kv_get(LAST_KEY) or {}
    block = await db.kv_get(BLOCK_KEY) or {}
    if not last:
        return "🤖 Автонастройка ещё не запускалась (первый раз — через 10 минут после старта, дальше раз в 6 ч)."
    kinds = block.get("kinds") or {}
    tail = ""
    if kinds:
        tail = "\n\nСейчас остановлены: " + "; ".join(
            f"{esc(S.KIND_NAMES.get(k, k))} — {esc(str(v.get('why')))}" for k, v in kinds.items())
    return f"{last.get('text', '')}\n\n<i>Последний проход: {str(last.get('at', ''))[:16].replace('T', ' ')} UTC</i>" + tail
