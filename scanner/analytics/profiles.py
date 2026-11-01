"""Профиль кластера дева и рейтинг повторяемости (разд. 3 ТЗ).

Профиль строится по завершённым запускам кластера. Для сравнения — медианы,
чтобы один выброс не искажал профиль. Свежие запуски (30 дней) весят вдвое больше.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable

FIELDS = ("initial_mc", "peak_mc", "dump_mc", "time_to_dump_s", "sold_share", "cluster_share_start",
          "early_buyers", "achievable_profit", "mc_30s", "mc_60s", "mc_120s", "dev_buy_sol",
          "bundle_wallets", "bundle_share")


def _med(values: Iterable[float | None], weights: Iterable[float] | None = None) -> float | None:
    """Медиана (взвешенная, если заданы веса): один выброс не искажает профиль."""
    pairs = [(float(v), w) for v, w in zip(values, weights or _ones()) if v is not None]
    if not pairs:
        return None
    if weights is None:
        return median(v for v, _ in pairs)
    pairs.sort()
    total = sum(w for _, w in pairs)
    acc = 0.0
    for i, (v, w) in enumerate(pairs):
        acc += w
        if acc > total / 2:
            return v
        if acc == total / 2:
            return (v + pairs[i + 1][0]) / 2 if i + 1 < len(pairs) else v
    return pairs[-1][0]


def _ones() -> Iterable[float]:
    while True:
        yield 1.0


def _within(value: float | None, ref: float | None, tol: float) -> bool | None:
    if value is None or ref is None or ref == 0:
        return None
    return abs(value - ref) / abs(ref) <= tol


def is_pump_dump(launch: dict[str, Any]) -> bool:
    """Запуск повторил сценарий «запуск, рост, дамп»."""
    init, peak = launch.get("initial_mc"), launch.get("peak_mc")
    return bool(launch.get("time_to_dump_s") is not None and init and peak and peak / init >= 1.2)


@dataclass
class Profile:
    cluster_id: int | None
    launches: int
    pattern_launches: int
    similar_launches: int
    repeatability: float
    medians: dict[str, float | None]
    growth_rate: float | None          # медианный относительный рост MC в секунду на старте
    migrated: int
    instant_rugs: int
    instant_rugger: bool
    last_launch_at: str | None
    weights_note: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def dump_mc(self) -> float | None:
        return self.medians.get("dump_mc")

    @property
    def time_to_dump(self) -> float | None:
        return self.medians.get("time_to_dump_s")

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, d: dict[str, Any] | None) -> "Profile | None":
        if not d:
            return None
        known = {k: d.get(k) for k in cls.__dataclass_fields__}
        known["medians"] = known.get("medians") or {}
        known["extra"] = known.get("extra") or {}
        return cls(**known)


def build_profile(launches: list[dict[str, Any]], cfg: dict[str, Any], cluster_id: int | None = None,
                  now: datetime | None = None, pattern_weight: float = 1.0) -> Profile:
    now = now or datetime.now(timezone.utc)
    recent_cut = now - timedelta(days=float(cfg.get("recent_days", 30)))
    recent_w = float(cfg.get("recent_weight", 2.0))
    mc_tol = float(cfg.get("mc_tolerance", 0.30))
    t_tol = float(cfg.get("time_tolerance", 0.50))
    rug_s = float(cfg.get("instant_rug_s", 15))

    def weight(l: dict[str, Any]) -> float:
        created = l.get("created_at")
        w = recent_w if isinstance(created, datetime) and created >= recent_cut else 1.0
        return w * float(l.get("weight") or 1.0)

    pattern = [l for l in launches if is_pump_dump(l)]
    base = pattern or launches
    ws = [weight(l) for l in base]
    meds = {f: _med([l.get(f) for l in base], ws) for f in FIELDS}
    growth = _med([(l.get("features") or {}).get("early_growth_rate") for l in base], ws)

    total_w = sim_w = 0.0
    similar = 0
    for l in launches:
        w = weight(l)
        total_w += w
        if not is_pump_dump(l):
            continue
        checks = [
            _within(l.get("dump_mc"), meds["dump_mc"], mc_tol),
            _within(l.get("time_to_dump_s"), meds["time_to_dump_s"], t_tol),
            _within(l.get("mc_60s"), meds["mc_60s"], mc_tol),
        ]
        if all(c is not False for c in checks) and any(c is True for c in checks):
            sim_w += w
            similar += 1
    rep = (sim_w / total_w if total_w else 0.0) * pattern_weight
    rugs = sum(1 for l in launches if l.get("time_to_dump_s") is not None and l["time_to_dump_s"] < rug_s)
    med_ttd = meds["time_to_dump_s"]
    last = max((l["created_at"] for l in launches if isinstance(l.get("created_at"), datetime)), default=None)
    return Profile(
        cluster_id=cluster_id,
        launches=len(launches),
        pattern_launches=len(pattern),
        similar_launches=similar,
        repeatability=max(0.0, min(1.0, rep)),
        medians=meds,
        growth_rate=growth,
        migrated=sum(1 for l in launches if l.get("migrated")),
        instant_rugs=rugs,
        instant_rugger=bool(len(launches) >= int(cfg.get("min_launches", 3)) and med_ttd is not None and med_ttd < rug_s),
        last_launch_at=last.isoformat() if last else None,
    )


def similar_to_reference(features: dict[str, Any], ref: dict[str, Any], cfg: dict[str, Any]) -> tuple[bool, list[str]]:
    """Похож ли ранний запуск на эталон (уровень «Наблюдение», без связи кошельков)."""
    tol = float(cfg.get("mc_tolerance", 0.30))
    reasons: list[str] = []
    checks = []
    for key, label in (("dev_buy_sol", "покупка дева"), ("cluster_share_start", "доля кластера"),
                       ("mc_30s", "MC через 30 с")):
        ok = _within(features.get(key), ref.get(key), tol)
        if ok is not None:
            checks.append(ok)
            if ok:
                reasons.append(label)
    fa, fr = features.get("features") or {}, ref.get("features") or {}
    if fa.get("ticker_style") and fa.get("ticker_style") == fr.get("ticker_style"):
        reasons.append("стиль тикера")
        checks.append(True)
    if fa.get("desc_template") and fa.get("desc_template") == fr.get("desc_template"):
        reasons.append("шаблон описания")
        checks.append(True)
    same_socials = all(fa.get(k) == fr.get(k) for k in ("has_twitter", "has_telegram", "has_website"))
    checks.append(same_socials)
    return (len(checks) >= 3 and all(checks)), reasons
