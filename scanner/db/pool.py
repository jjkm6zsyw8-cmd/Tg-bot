"""Пул соединений PostgreSQL и общие операции."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import asyncpg

log = logging.getLogger(__name__)
SCHEMA = Path(__file__).with_name("schema.sql")


async def _init_conn(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
    await conn.set_type_codec("json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self, min_size: int = 2, max_size: int = 10) -> "Database":
        self.pool = await asyncpg.create_pool(self.dsn, min_size=min_size, max_size=max_size, init=_init_conn)
        return self

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def migrate(self) -> None:
        async with self.p.acquire() as conn:
            await conn.execute("SELECT pg_advisory_lock(424242)")
            try:
                await conn.execute(SCHEMA.read_text(encoding="utf-8"))
            finally:
                await conn.execute("SELECT pg_advisory_unlock(424242)")

    @property
    def p(self) -> asyncpg.Pool:
        assert self.pool is not None, "Database.connect() не вызван"
        return self.pool

    # --- тонкие обёртки ---
    async def fetch(self, q: str, *a: Any) -> list[asyncpg.Record]:
        return await self.p.fetch(q, *a)

    async def fetchrow(self, q: str, *a: Any) -> asyncpg.Record | None:
        return await self.p.fetchrow(q, *a)

    async def fetchval(self, q: str, *a: Any) -> Any:
        return await self.p.fetchval(q, *a)

    async def execute(self, q: str, *a: Any) -> str:
        return await self.p.execute(q, *a)

    # --- kv / settings ---
    async def kv_get(self, key: str, default: Any = None) -> Any:
        v = await self.fetchval("SELECT value FROM kv WHERE key=$1", key)
        return default if v is None else v

    async def kv_set(self, key: str, value: Any) -> None:
        if value is None:
            await self.execute("DELETE FROM kv WHERE key=$1", key)
            return
        await self.execute(
            "INSERT INTO kv(key, value, updated_at) VALUES ($1, $2, now()) "
            "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=now()",
            key, value,
        )

    async def get_overrides(self) -> dict[str, Any]:
        v = await self.fetchval("SELECT value FROM settings WHERE key='user'")
        return v or {}

    async def set_override(self, key: str, value: Any) -> dict[str, Any]:
        cur = await self.get_overrides()
        if value is None:
            cur.pop(key, None)
        else:
            cur[key] = value
        await self.execute(
            "INSERT INTO settings(key, value, updated_at) VALUES ('user', $1, now()) "
            "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=now()",
            cur,
        )
        await self.execute("SELECT pg_notify('settings', '1')")
        return cur

    # --- outbox ---
    async def enqueue(self, kind: str, payload: dict[str, Any], priority: int = 5) -> int:
        oid = await self.fetchval(
            "INSERT INTO outbox(kind, payload, priority) VALUES ($1, $2, $3) RETURNING id",
            kind, payload, priority,
        )
        await self.execute("SELECT pg_notify('outbox', $1)", str(oid))
        return oid

    async def system(self, text: str) -> None:
        log.warning("SYSTEM: %s", text)
        await self.enqueue("system", {"text": text}, priority=3)

    async def job(self, kind: str, payload: dict[str, Any] | None = None) -> int:
        jid = await self.fetchval("INSERT INTO jobs(kind, payload) VALUES ($1, $2) RETURNING id", kind, payload or {})
        await self.execute("SELECT pg_notify('jobs', $1)", str(jid))
        return jid
