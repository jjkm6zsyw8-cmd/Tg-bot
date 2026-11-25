"""Общие фикстуры: дневной лимит бумаги зависит от часов (08–18 по Батуми) — в тестах он выключен,
иначе результат тестов зависел бы от времени запуска. Сам лимит проверяет test_no_reentry_after_loss_and_day_cap.

25.11: убранные методы (scanner/real/settings.py, RETIRED) и бумажная торговля (scanner/analytics/paper.py,
PAPER_TRADING) у владельца выключены, но их код остался — им же торгуются «за ним», смарт-ракеты на pump.fun и
маршрутизация сигналов. Тесты механики гоняют этот код как раньше: здесь RETIRED пустой, а бумага включена. Тесты с
меткой policy проверяют настоящее поведение — без подмены."""
from __future__ import annotations

import pytest

from scanner.analytics import paper as pp
from scanner.real import settings as S

REAL_DAY_CAP = pp.PaperTrader.day_cap_sol


@pytest.fixture(autouse=True)
def _no_day_cap(monkeypatch):
    monkeypatch.setattr(pp.PaperTrader, "day_cap_sol", lambda self, now=None: None)


@pytest.fixture(autouse=True)
def _mechanics(request, monkeypatch):
    if request.node.get_closest_marker("policy") is None:
        monkeypatch.setattr(S, "RETIRED", frozenset())
        monkeypatch.setattr(pp, "PAPER_TRADING", True)


@pytest.fixture(autouse=True)
def _no_gecko(monkeypatch):
    """25.11: признаки входа трендов берут свечи GeckoTerminal — в тестах без сети."""
    from scanner.real import features

    async def no_candles(pool, http=None):
        return None
    monkeypatch.setattr(features, "gecko_candles", no_candles)
