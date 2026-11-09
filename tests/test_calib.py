"""/calib: перебор «тейк × стоп» по пути цены после входа."""
from __future__ import annotations

from scanner.reports.calib import FEE, SLIP, best, group_line, sim


def row(mfe, mae, dip, p30):
    return {"mfe": mfe, "mae": mae, "dip": dip, "p30": p30}


def test_sim_take_stop_hold():
    r = row(0.6, -0.3, -0.05, -0.3)                  # дошла до +60%, до пика проседала на 5%, потом слила
    assert sim(r, 0.25, 0.08) == 0.25 - FEE           # тейк до стопа
    r = row(0.6, -0.3, -0.12, -0.3)                  # до пика просела на 12% — стоп 8% выбивает раньше
    assert sim(r, 0.25, 0.08) == -0.08 - SLIP - FEE
    assert sim(r, 0.25, 0.15) == 0.25 - FEE           # со стопом 15% пережила просадку
    assert sim(row(0.1, -0.05, -0.05, 0.04), 0.25, 0.08) == 0.04 - FEE   # ни тейка, ни стопа — цена через 30 мин


def test_best_prefers_wider_stop_when_dips_before_pump():
    rows = [row(1.0, -0.12, -0.12, 0.5)] * 10 + [row(0.05, -0.4, -0.02, -0.4)] * 10
    tp, sl, avg = best(rows)
    assert sl >= 0.15 and tp == 1.0 and avg > 0      # просадка −12% перед ×2 — стоп −8% всё режет


def test_group_line_needs_data():
    assert "мало данных" in group_line("копии", [row(0.3, -0.1, -0.05, 0.1)] * 5)
    text = group_line("копии", [row(0.3, -0.1, -0.05, 0.1)] * 25)
    assert "+25% у 100%" in text and "лучше всего" in text
