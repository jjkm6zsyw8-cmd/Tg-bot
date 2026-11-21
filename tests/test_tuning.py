"""Автоподбор фильтров «Трендов»: обучение / проверка, только ужесточение, отказ без улучшения."""
from __future__ import annotations

import random

from scanner.analytics.tuning import MIN_SAMPLES, Sample, sample_from, tune


def make(n: int, seed: int = 1, signal: bool = True) -> list[Sample]:
    """Свечи с ростом за 5 мин > 30% (вершина) проигрывают, остальные — в плюсе (если signal)."""
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        pc = rnd.uniform(3, 60)
        net = (-0.25 if pc > 30 else 0.15) if signal else rnd.choice([-0.1, 0.1])
        net += rnd.uniform(-0.05, 0.05)
        out.append(Sample(net, i % 24, {"pc_m5": pc, "ratio": rnd.uniform(1.3, 4), "vol_m5": rnd.uniform(2e3, 5e4),
                                        "liq": rnd.uniform(8e3, 2e5), "pc_h1": rnd.uniform(0, 400),
                                        "mc_usd": rnd.uniform(2e4, 2e6)}))
    return out


def test_needs_history():
    r = tune(make(MIN_SAMPLES - 1))
    assert not r.adopted and "мало истории" in r.text()


def test_finds_top_of_candle_cut_and_checks_on_test():
    r = tune(make(240))
    assert r.adopted
    assert 20 <= r.params["trend_pc_m5_max"] <= 40              # отсечь «вершину свечи»
    assert r.new_test_mean > r.base_test_mean and r.kept_test >= 10
    txt = r.text({"trend_pc_m5_max": 60})
    assert "рост за 5 мин до" in txt and "было 60%" in txt


def test_no_signal_keeps_defaults():
    r = tune(make(240, signal=False))
    assert not r.adopted and not r.params


def test_only_tightens():
    r = tune(make(240), {"trend_pc_m5_max": 20})                # уже строже, чем найденная граница
    assert "trend_pc_m5_max" not in r.params


def test_sample_from_payload():
    s = sample_from({"trend": {"pc_m5": 12, "buys": 30, "sells": 10, "vol_m5": 5000, "liq": 20000}}, 5, 0.1)
    assert s is not None and s.f["ratio"] == 3 and "mc_usd" not in s.f and s.hour == 5
    assert sample_from({}, 5, 0.1) is None
