"""Автофильтр сигналов: бот сам учится на сделках отслеживаемых трейдеров, какие сигналы копировать, а какие — шлак.

Обучение: каждый закрытый сигнал копий и smart в эталонном портфеле G (только сверенные с рынком) — пример:
признаки монеты в момент сигнала (как их видел бот) → чем кончилась копия. Признаки: капитализация, возраст монеты,
рост за минуту до сигнала, холдеры, доля топ-10, снайперы, бандлы, инсайдеры, свежие кошельки, накрутка объёма,
прогресс кривой, доля кластера дева, сумма покупки трейдера, сколько smart-кошельков уже в монете, час суток, тип
сигнала. По каждому признаку — 4 диапазона (по квантилям истории) и средний результат копии в каждом, «стянутый»
к общему среднему, пока примеров мало. Прогноз сигнала = общее среднее + сумма отклонений его диапазонов.

Защита от самообмана: модель проверяется «вперёд по времени» — учится на днях до дня D и решает по сигналам дня D.
Фильтр включается, только если на такой проверке отсеянные сигналы были в минусе и хуже взятых. Отсеивает только в
боевом M; A–G берут всё — G и показывает, что дали отсеянные (/filter).
"""
from __future__ import annotations

import asyncio
import logging
import statistics as st
from datetime import datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger(__name__)

TZ = ZoneInfo("Asia/Tbilisi")
NUM = ("mc", "age_s", "growth_1m", "holders", "top10", "snipers", "bundles", "insiders", "fresh_wallets",
       "wash_volume", "progress", "cluster", "buy_sol", "smart_in_coin", "hour")
NAMES = {"mc": "капитализация", "age_s": "возраст монеты", "growth_1m": "рост за минуту до сигнала",
         "holders": "холдеры", "top10": "доля топ-10", "snipers": "снайперы", "bundles": "бандлы",
         "insiders": "инсайдеры", "fresh_wallets": "свежие кошельки", "wash_volume": "накрутка объёма",
         "progress": "прогресс кривой", "cluster": "доля кластера дева", "buy_sol": "покупка трейдера, SOL",
         "smart_in_coin": "smart в монете", "hour": "час суток", "kind": "тип сигнала"}
BINS = 4
SHRINK = 20.0          # примеров, при которых отклонение диапазона берётся наполовину
MIN_TRAIN = 60
EVERY_S = 6 * 3600


def features(payload: dict[str, Any], level: str, mode: str, when: datetime | None) -> dict[str, Any]:
    m = payload.get("metrics") or {}
    traders = payload.get("traders") or []
    x: dict[str, Any] = {k: m.get(k) for k in ("growth_1m", "holders", "top10", "snipers", "bundles", "insiders",
                                              "fresh_wallets", "wash_volume", "progress", "cluster")}
    x["mc"] = payload.get("mc")
    x["age_s"] = payload.get("age_s")
    x["buy_sol"] = max((float(t.get("buy_sol") or 0) for t in traders if isinstance(t, dict)), default=None) \
        if traders else payload.get("copy_sol")
    x["smart_in_coin"] = payload.get("smart_in_coin")
    x["hour"] = when.astimezone(TZ).hour if when else None
    strong = level == "strong" or len(traders) >= 2
    x["kind"] = f"{'копия' if mode == 'copy' else 'smart'} {'сильная' if strong else 'одна'}"
    return {k: (float(v) if k != "kind" and v is not None else v) for k, v in x.items()}


def _edges(vals: list[float]) -> list[float]:
    v = sorted(vals)
    return sorted({v[int(len(v) * q / BINS)] for q in range(1, BINS)})


def _bin(edges: list[float], v: float) -> int:
    return sum(v >= e for e in edges)


def fit(rows: list[tuple[dict[str, Any], float]]) -> dict[str, Any]:
    """rows: (признаки, результат копии). Возвращает модель: общее среднее и отклонения по диапазонам."""
    ys = [y for _, y in rows]
    base = st.mean(ys) if ys else 0.0
    model: dict[str, Any] = {"base": base, "n": len(rows), "num": {}, "cat": {}}
    for f in NUM:
        vals = [x[f] for x, _ in rows if x.get(f) is not None]
        if len(vals) < MIN_TRAIN // 2:
            continue
        edges = _edges(vals)
        groups: dict[int, list[float]] = {}
        for x, y in rows:
            if x.get(f) is not None:
                groups.setdefault(_bin(edges, x[f]), []).append(y)
        model["num"][f] = {"edges": edges, "delta": {str(b): (st.mean(g) - base) * len(g) / (len(g) + SHRINK)
                                                      for b, g in groups.items()},
                           "n": {str(b): len(g) for b, g in groups.items()}}
    groups_c: dict[str, list[float]] = {}
    for x, y in rows:
        groups_c.setdefault(str(x.get("kind")), []).append(y)
    model["cat"]["kind"] = {k: (st.mean(g) - base) * len(g) / (len(g) + SHRINK) for k, g in groups_c.items()}
    return model


def predict(model: dict[str, Any], x: dict[str, Any]) -> float:
    p = float(model.get("base", 0.0))
    for f, spec in (model.get("num") or {}).items():
        if x.get(f) is not None:
            p += float(spec["delta"].get(str(_bin(spec["edges"], x[f])), 0.0))
    p += float(((model.get("cat") or {}).get("kind") or {}).get(str(x.get("kind")), 0.0))
    return p


def walk_forward(rows: list[tuple[dict[str, Any], float, str]], threshold: float = 0.0) -> dict[str, Any]:
    """Учимся на днях до D, решаем по дню D. rows: (признаки, результат, день)."""
    days = sorted({d for _, _, d in rows})
    taken: list[float] = []
    skipped: list[float] = []
    for d in days:
        train = [(x, y) for x, y, dd in rows if dd < d]
        if len(train) < MIN_TRAIN:
            continue
        model = fit(train)
        for x, y, dd in rows:
            if dd == d:
                (skipped if predict(model, x) < threshold else taken).append(y)
    res = {"taken": len(taken), "skipped": len(skipped),
           "taken_avg": st.mean(taken) if taken else None, "skipped_avg": st.mean(skipped) if skipped else None,
           "all_avg": st.mean(taken + skipped) if taken or skipped else None}
    res["useful"] = bool(skipped and taken and res["skipped_avg"] < 0 and res["skipped_avg"] < res["taken_avg"]
                         and len(skipped) >= 10)
    return res


async def dataset(db: Any, days: int = 14) -> list[tuple[dict[str, Any], float, str]]:
    rows = await db.fetch(
        """SELECT p.opened_at, p.pnl_pct, p.level, a.mode, a.payload FROM paper_positions p
           JOIN alerts a ON a.id = p.alert_id
           WHERE p.portfolio = 'G' AND p.closed_at IS NOT NULL AND NOT p.suspect AND p.exit_reason <> 'reset'
             AND a.mode IN ('copy', 'smart') AND p.opened_at > now() - make_interval(days => $1)
           ORDER BY p.opened_at""", days)
    out = []
    for r in rows:
        payload = r["payload"] if isinstance(r["payload"], dict) else {}
        out.append((features(payload, r["level"] or "", r["mode"], r["opened_at"]), float(r["pnl_pct"] or 0),
                    r["opened_at"].astimezone(TZ).strftime("%Y-%m-%d")))
    return out


async def train(db: Any) -> dict[str, Any]:
    rows = await dataset(db)
    model = fit([(x, y) for x, y, _ in rows]) if len(rows) >= MIN_TRAIN else {"base": 0.0, "n": len(rows)}
    check = walk_forward(rows) if len(rows) >= MIN_TRAIN else {"useful": False}
    model["check"] = check
    model["active"] = bool(check.get("useful"))
    model["trained_at"] = datetime.now(TZ).isoformat(timespec="minutes")
    return model


class AutoFilter:
    def __init__(self, engine: "Engine"):
        self.e = engine
        self.db = engine.db

    async def refresh(self) -> dict[str, Any]:
        model = await train(self.db)
        await self.db.kv_set("autofilter:model", model)
        self.e.autofilter = model
        return model

    async def loop(self) -> None:
        saved = await self.db.kv_get("autofilter:model")
        if saved:
            self.e.autofilter = saved
        await asyncio.sleep(300)
        while True:
            try:
                model = await self.refresh()
                c = model.get("check") or {}
                await self.db.system(
                    f"🧠 Автофильтр переобучен: {model.get('n', 0)} сигналов · "
                    + ("включён" if model.get("active") else "пока выключен (проверка не доказала пользу)")
                    + (f" · проверка: отсеял бы {c['skipped']} (в среднем {c['skipped_avg']:+.1%}), "
                       f"взял {c['taken']} ({c['taken_avg']:+.1%})" if c.get("skipped_avg") is not None
                       and c.get("taken_avg") is not None else ""))
            except Exception:  # noqa: BLE001 — фильтр не должен ронять аналитику
                log.exception("автофильтр")
            await asyncio.sleep(EVERY_S)


async def filter_report(db: Any) -> str:
    model = await db.kv_get("autofilter:model") or {}
    if not model:
        return "🧠 Автофильтр ещё не обучался (первое обучение — через 5 минут после запуска, дальше раз в 6 часов)."
    c = model.get("check") or {}
    lines = [f"🧠 <b>Автофильтр сигналов</b> · обучен {model.get('trained_at', '?')} на {model.get('n', 0)} сигналах "
             f"(копии и smart в G, сверенные с рынком) · средний результат копии {model.get('base', 0):+.1%}",
             "Статус: " + ("✅ включён — M не входит в сигналы с прогнозом ниже нуля" if model.get("active")
                           else "⏸ выключен — проверка «вперёд по времени» пока не доказала пользу; M берёт всё по своим правилам")]
    if c.get("taken_avg") is not None:
        lines.append(f"Проверка вперёд по времени: взял бы {c['taken']} (в среднем {c['taken_avg']:+.1%}), "
                     f"отсеял бы {c['skipped']}" + (f" (в среднем {c['skipped_avg']:+.1%})" if c.get("skipped_avg")
                                                    is not None else ""))
    effects = []
    for f, spec in (model.get("num") or {}).items():
        edges = spec["edges"]
        for b, d in spec["delta"].items():
            n = spec["n"].get(b, 0)
            i = int(b)
            lo = f"{edges[i - 1]:.3g}" if i > 0 else "…"
            hi = f"{edges[i]:.3g}" if i < len(edges) else "…"
            effects.append((d, f"{NAMES.get(f, f)} {lo}–{hi}", n))
    for k, d in ((model.get("cat") or {}).get("kind") or {}).items():
        effects.append((d, f"тип: {k}", 0))
    if effects:
        effects.sort()
        lines.append("\n<b>Что тянет вниз</b> (отклонение от среднего):")
        lines += [f"• {name}: {d:+.1%}" + (f" ({n} сигн.)" if n else "") for d, name, n in effects[:6]]
        lines.append("<b>Что тянет вверх</b>:")
        lines += [f"• {name}: {d:+.1%}" + (f" ({n} сигн.)" if n else "") for d, name, n in effects[::-1][:6]]
    logs = await db.kv_get("autofilter:log") or []
    if logs:
        lines.append(f"\nПоследние решения в M: отсеяно {sum(1 for x in logs if x.get('skip'))} из {len(logs)}")
    return "\n".join(lines)
