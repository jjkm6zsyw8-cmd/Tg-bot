"""Общие фикстуры: дневной лимит бумаги зависит от часов (08–18 по Батуми) — в тестах он выключен,
иначе результат тестов зависел бы от времени запуска. Сам лимит проверяет test_no_reentry_after_loss_and_day_cap."""
from __future__ import annotations

import pytest

from scanner.analytics import paper as pp

REAL_DAY_CAP = pp.PaperTrader.day_cap_sol


@pytest.fixture(autouse=True)
def _no_day_cap(monkeypatch):
    monkeypatch.setattr(pp.PaperTrader, "day_cap_sol", lambda self, now=None: None)
