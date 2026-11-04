"""Автоподбор фильтров режима «Тренды» по истории алертов (обучение / проверка).

Как работает:
  * берём закрытые тренд-алерты с итогом (net_result) по времени;
  * первые 2/3 — обучение, последние 1/3 — проверка (честно: проверка «из будущего»);
  * на обучении жадно добавляем до 3 ужесточений порогов (цена за 5 мин, давление покупок, объём,
    ликвидность, MC, рост за час) и «плохие часы» — каждое должно заметно поднять суммарный результат;
  * принимаем новые фильтры, только если на проверке средний результат на сделку вырос, сумма не упала
    и осталось ≥ 10 сделок. Иначе остаются фильтры по умолчанию.

Пороги можно только ужесточать относительно базовых (по отсечённым монетам истории нет).
Ручные настройки пользователя всегда главнее подобранных.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timezone
from typing import Any

from .trends import DEFAULTS

log = logging.getLogger(__name__)

MIN_SAMPLES = 100
KV_KEY = "trends:tuned"

# признак → (ключ настройки, направление): "min" — отсекаем значения ниже порога, "max" — выше
CUTS: list[tuple[str, str, str]] = [
    ("pc_m5", "trend_pc_m5_min", "min"),
    ("pc_m5", "trend_pc_m5_max", "max"),
    ("ratio", "trend_min_buy_ratio_m5", "min"),
    ("vol_m5", "trend_min_vol_m5_usd", "min"),
    ("liq", "trend_min_liq_usd", "min"),
    ("mc_usd", "trend_mc_min_usd", "min"),
    ("mc_usd", "trend_mc_max_usd", "max"),
    ("pc_h1", "trend_pc_h1_max", "max"),
]
LABELS = {
    "trend_pc_m5_min": ("рост за 5 мин от", "%"), "trend_pc_m5_max": ("рост за 5 мин до", "%"),
    "trend_min_buy_ratio_m5": ("покупок/продаж от", "×"), "trend_min_vol_m5_usd": ("объём 5 мин от", "$"),
    "trend_min_liq_usd": ("ликвидность от", "$"), "trend_mc_min_usd": ("MC от", "$"),
    "trend_mc_max_usd": ("MC до", "$"), "trend_pc_h1_max": ("рост за час до", "%"),
}


@dataclass
class Sample:
    net: float
    hour: int
    f: dict[str, float]


@dataclass
class TuneResult:
    adopted: bool
    n: int
    n_train: int
    n_test: int
    base_test_mean: float = 0.0
    new_test_mean: float = 0.0
    base_test_sum: float = 0.0
    new_test_sum: float = 0.0
    kept_test: int = 0
    params: dict[str, Any] = field(default_factory=dict)
    note: str = ""

    def text(self, base_cfg: dict[str, Any] | None = None) -> str:
        base_cfg = base_cfg or {}
        if self.n < MIN_SAMPLES:
            return (f"🎛 Автоподбор «Трендов»: мало истории — {self.n} закрытых сделок из {MIN_SAMPLES} нужных. "
                    "Фильтры по умолчанию.")
        head = f"🎛 Автоподбор «Трендов» · {self.n} сделок (обучение {self.n_train} / проверка {self.n_test})"
        if not self.adopted:
            return f"{head}\nУлучшения на проверке нет — оставил фильтры по умолчанию. {self.note}".strip()
        lines = [head,
                 f"Проверка: в среднем {self.base_test_mean:+.0%} → <b>{self.new_test_mean:+.0%}</b> на сделку, "
                 f"сделок {self.kept_test} из {self.n_test}"]
        for key, val in self.params.items():
            if key == "trend_bad_hours":
                lines.append("• пропускать часы (UTC): " + ", ".join(f"{h:02d}" for h in val))
                continue
            label, unit = LABELS.get(key, (key, ""))
            was = base_cfg.get(key, DEFAULTS.get(key))
            lines.append(f"• {label} {_fmt(val, unit)} (было {_fmt(was, unit)})")
        return "\n".join(lines)


def _fmt(v: Any, unit: str) -> str:
    if v is None:
        return "—"
    v = float(v)
    if unit == "$":
        return f"${v / 1000:.0f}K" if v >= 1000 else f"${v:.0f}"
    if unit == "×":
        return f"{v:.2f}×"
    return f"{v:.0f}%"


def sample_from(payload: dict[str, Any], hour: int, net: float) -> Sample | None:
    t = payload.get("trend") or {}
    if not t:
        return None
    f = {
        "pc_m5": float(t.get("pc_m5") or 0), "ratio": float(t.get("buys") or 0) / max(1.0, float(t.get("sells") or 0)),
        "vol_m5": float(t.get("vol_m5") or 0), "liq": float(t.get("liq") or 0), "pc_h1": float(t.get("pc_h1") or 0),
    }
    if t.get("mc_usd"):
        f["mc_usd"] = float(t["mc_usd"])
    return Sample(float(net), hour, f)


def _passes(s: Sample, cuts: dict[str, tuple[str, str, float]], bad_hours: set[int]) -> bool:
    if s.hour in bad_hours:
        return False
    for feat, direction, thr in cuts.values():
        v = s.f.get(feat)
        if v is None:
            continue           # у старых алертов признака нет — не отсекаем
        if direction == "min" and v < thr or direction == "max" and v > thr:
            return False
    return True


def _total(samples: list[Sample], cuts: dict[str, tuple[str, str, float]], bad_hours: set[int]) -> tuple[float, int]:
    kept = [s.net for s in samples if _passes(s, cuts, bad_hours)]
    return sum(kept), len(kept)


def _quantiles(vals: list[float], qs: list[float]) -> list[float]:
    vals = sorted(vals)
    if not vals:
        return []
    return sorted({vals[min(len(vals) - 1, int(q * len(vals)))] for q in qs})


def tune(samples: list[Sample], base_cfg: dict[str, Any] | None = None, max_cuts: int = 3) -> TuneResult:
    """samples — по времени (старые первыми). Возвращает подобранные пороги или «не принимать»."""
    base_cfg = base_cfg or {}
    n = len(samples)
    n_train = n * 2 // 3
    res = TuneResult(adopted=False, n=n, n_train=n_train, n_test=n - n_train)
    if n < MIN_SAMPLES:
        return res
    train, test = samples[:n_train], samples[n_train:]
    min_keep = max(30, int(0.35 * len(train)))
    cuts: dict[str, tuple[str, str, float]] = {}
    cur, _ = _total(train, cuts, set())
    for _ in range(max_cuts):
        best: tuple[float, str, tuple[str, str, float]] | None = None
        for feat, key, direction in CUTS:
            if key in cuts:
                continue
            vals = [s.f[feat] for s in train if feat in s.f]
            if len(vals) < min_keep:
                continue
            qs = [0.1, 0.2, 0.3, 0.4, 0.5] if direction == "min" else [0.5, 0.6, 0.7, 0.8, 0.9]
            base = base_cfg.get(key, DEFAULTS.get(key))
            for thr in _quantiles(vals, qs):
                if base is not None and (direction == "min" and thr <= float(base)
                                         or direction == "max" and thr >= float(base)):
                    continue   # только ужесточение
                trial = {**cuts, key: (feat, direction, thr)}
                tot, kept = _total(train, trial, set())
                if kept >= min_keep and (best is None or tot > best[0]):
                    best = (tot, key, (feat, direction, thr))
        # шаг должен дать заметный прирост: ≥ 2 п.п. на каждую сделку обучения в сумме
        if best is None or best[0] - cur < 0.02 * len(train):
            break
        cur, key, cut = best
        cuts[key] = cut
    # плохие часы: на обучении ≥ 8 сделок в час и средний результат хуже −5%
    by_hour: dict[int, list[float]] = {}
    for s in train:
        if _passes(s, cuts, set()):
            by_hour.setdefault(s.hour, []).append(s.net)
    bad = sorted((h for h, v in by_hour.items() if len(v) >= 8 and sum(v) / len(v) < -0.05),
                 key=lambda h: sum(by_hour[h]) / len(by_hour[h]))[:8]
    bad_hours = set(bad)
    if bad_hours and _total(train, cuts, bad_hours)[1] < min_keep:
        bad_hours = set()
    # проверка на «будущих» сделках
    b_sum, b_n = _total(test, {}, set())
    t_sum, t_n = _total(test, cuts, bad_hours)
    res.base_test_sum, res.new_test_sum, res.kept_test = b_sum, t_sum, t_n
    res.base_test_mean = b_sum / b_n if b_n else 0.0
    res.new_test_mean = t_sum / t_n if t_n else 0.0
    if not cuts and not bad_hours:
        res.note = "Порогов, которые заметно улучшают результат, не нашлось."
        return res
    if t_n >= 10 and res.new_test_mean > res.base_test_mean and t_sum >= b_sum:
        res.adopted = True
        res.params = {key: round(thr, 4) for key, (_f, _d, thr) in cuts.items()}
        if bad_hours:
            res.params["trend_bad_hours"] = sorted(bad_hours)
    else:
        res.note = (f"Подбор на обучении не подтвердился на проверке "
                    f"({res.base_test_mean:+.0%} → {res.new_test_mean:+.0%}, сделок {t_n}).")
    return res


async def load_samples(db: Any) -> list[Sample]:
    rows = await db.fetch(
        """SELECT a.created_at, a.payload, o.net_result FROM alerts a JOIN alert_outcomes o ON o.alert_id = a.id
           WHERE a.mode = 'trend' AND o.net_result IS NOT NULL ORDER BY a.created_at""")
    out = []
    for r in rows:
        s = sample_from(r["payload"] or {}, r["created_at"].astimezone(timezone.utc).hour, r["net_result"])
        if s is not None:
            out.append(s)
    return out


async def tune_trends(db: Any, base_cfg: dict[str, Any]) -> TuneResult:
    """Подобрать и сохранить (или сбросить) пороги. Режим «Тренды» перечитывает их раз в 5 минут."""
    res = tune(await load_samples(db), base_cfg)
    if res.n >= MIN_SAMPLES:
        await db.kv_set(KV_KEY, res.params if res.adopted else None)
    return res
