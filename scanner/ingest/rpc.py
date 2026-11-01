"""Клиент Solana JSON-RPC: досбор пропущенных транзакций, слоты, переводы SOL."""
from __future__ import annotations

import asyncio
import itertools
import logging
import time
from typing import Any

import aiohttp

log = logging.getLogger(__name__)


class RpcError(RuntimeError):
    pass


class RateLimiter:
    def __init__(self, rps: float):
        self.interval = 1.0 / rps if rps > 0 else 0.0
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        if not self.interval:
            return
        async with self._lock:
            now = time.monotonic()
            if self._next > now:
                await asyncio.sleep(self._next - now)
            self._next = max(now, self._next) + self.interval


class SolanaRpc:
    def __init__(self, url: str, rps: float = 20, concurrency: int = 8, timeout: float = 10):
        self.url = url
        self.limiter = RateLimiter(rps)
        self.sem = asyncio.Semaphore(concurrency)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._ids = itertools.count(1)
        self._session: aiohttp.ClientSession | None = None

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self) -> None:
        if self._session:
            await self._session.close()

    async def call(self, method: str, params: list[Any], retries: int = 3) -> Any:
        body = {"jsonrpc": "2.0", "id": next(self._ids), "method": method, "params": params}
        delay = 0.5
        for attempt in range(retries + 1):
            await self.limiter.wait()
            try:
                async with self.sem:
                    s = await self.session()
                    async with s.post(self.url, json=body) as resp:
                        if resp.status == 429:
                            raise RpcError("rate limited")
                        data = await resp.json(content_type=None)
                if "error" in data:
                    raise RpcError(str(data["error"]))
                return data.get("result")
            except (aiohttp.ClientError, asyncio.TimeoutError, RpcError) as exc:
                if attempt >= retries:
                    raise RpcError(f"{method}: {exc}") from exc
                await asyncio.sleep(delay)
                delay *= 2
        return None

    async def signature_slots(self, signatures: list[str]) -> dict[str, int]:
        """Слот для каждой подписи (getSignatureStatuses, до 256 за вызов)."""
        out: dict[str, int] = {}
        for i in range(0, len(signatures), 256):
            chunk = signatures[i : i + 256]
            res = await self.call("getSignatureStatuses", [chunk, {"searchTransactionHistory": False}])
            for sig, st in zip(chunk, (res or {}).get("value") or []):
                if st and st.get("slot") is not None:
                    out[sig] = int(st["slot"])
        return out

    async def signatures_for(self, address: str, limit: int = 100, before: str | None = None,
                             until: str | None = None) -> list[dict[str, Any]]:
        cfg: dict[str, Any] = {"limit": limit, "commitment": "confirmed"}
        if before:
            cfg["before"] = before
        if until:
            cfg["until"] = until
        return await self.call("getSignaturesForAddress", [address, cfg]) or []

    async def transaction(self, signature: str) -> dict[str, Any] | None:
        return await self.call(
            "getTransaction",
            [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}],
        )

    async def block_time(self, slot: int) -> int | None:
        try:
            return await self.call("getBlockTime", [slot], retries=1)
        except RpcError:
            return None
