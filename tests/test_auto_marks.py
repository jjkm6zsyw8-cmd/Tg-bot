"""21.11: «Сопровождение монет» — отметки руками. Бот ставит оценку сам; «скам» применяется без сообщения."""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from scanner.analytics import engine as eng_mod
from scanner.analytics.feedback import Feedback
from scanner.db.pool import Database

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")


async def test_silent_scam_mark_blacklists_without_message():
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        aid = await db.fetchval("INSERT INTO alerts(mint, mode, level) VALUES ('M', 'dev', 'normal') RETURNING id")
        fb = Feedback(SimpleNamespace(db=db, cfg={}, settings=SimpleNamespace(yaml=SimpleNamespace(section=lambda _s: {}))))
        await fb.on_mark(aid, "scam", silent=True)
        assert await db.fetchval("SELECT count(*) FROM outbox") == 0
        await fb.on_mark(aid, "scam")
        assert await db.fetchval("SELECT count(*) FROM outbox") == 1
    finally:
        await db.close()


def test_auto_mark_names():
    assert eng_mod.AUTO_MARK_NAMES["scam"].startswith("☠️") and set(eng_mod.AUTO_MARK_NAMES) == {"great", "good", "fail",
                                                                                                "scam", "nodata"}
