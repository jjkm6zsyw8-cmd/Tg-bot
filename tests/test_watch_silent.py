"""«Наблюдение» в чат не идёт: ни алерт, ни продолжения, ни итог — иначе очередь Telegram росла до 900+."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from scanner.analytics.engine import AlertEntry, Engine


class Q:
    def __init__(self):
        self.items = []

    async def enqueue(self, kind, payload, priority=5):
        self.items.append(kind)


async def test_follow_messages_only_for_alerts_shown_in_chat():
    eng = Engine.__new__(Engine)
    eng.db = Q()
    eng.price = SimpleNamespace(value=90.0)
    eng.settings = SimpleNamespace(links=lambda: {})
    eng.env = SimpleNamespace(redirect_base="https://example.com")
    st = SimpleNamespace(symbol="X", mc=30.0)
    silent = AlertEntry(mint="M", created_mono=0.0, created_wall=datetime.now(timezone.utc), level="watch")
    await eng._follow_msg(silent, "migrated", st)
    assert eng.db.items == []                                     # алерта в чате не было — продолжения нет
    shown = AlertEntry(mint="M", created_mono=0.0, created_wall=datetime.now(timezone.utc), has_message=True)
    await eng._follow_msg(shown, "migrated", st)
    assert eng.db.items == ["follow"]
