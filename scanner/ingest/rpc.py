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
        self.calls = 0
        self.rate_limited = 0
        self.pause_until = 0.0          # общая пауза после 429: запросы ждут, а не долбят RPC
        self._consecutive_429 = 0
        self.day = time.strftime("%Y-%m-%d", time.gmtime())
        self.day_calls = 0
        self.budget: int | None = None      # запросов в сутки (UTC) на «необязательные» задачи; None — без лимита

    def over_budget(self) -> bool:
        """Дневной лимит исчерпан: фоновые задачи (слежка за выводами, холдеры, кластеры) ждут следующих суток."""
        self._roll_day()
        return self.budget is not None and self.day_calls >= self.budget

    def _roll_day(self) -> None:
        today = time.strftime("%Y-%m-%d", time.gmtime())
        if today != self.day:
            self.day, self.day_calls = today, 0

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self) -> None:
        if self._session:
            await self._session.close()

    async def call(self, method: str, params: list[Any], retries: int = 2) -> Any:
        body = {"jsonrpc": "2.0", "id": next(self._ids), "method": method, "params": params}
        delay = 1.0
        for attempt in range(retries + 1):
            wait = self.pause_until - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)          # во время паузы после 429 запросов нет
            await self.limiter.wait()
            try:
                async with self.sem:
                    s = await self.session()
                    self.calls += 1
                    self._roll_day()
                    self.day_calls += 1
                    async with s.post(self.url, json=body) as resp:
                        if resp.status == 429:
                            self._on_429(resp.headers.get("Retry-After"))
                            raise RpcError("rate limited")
                        data = await resp.json(content_type=None)
                self._consecutive_429 = 0
                if "error" in data:
                    err = data["error"]
                    if isinstance(err, dict) and err.get("code") in (429, -32429):
                        self._on_429(None)
                        raise RpcError("rate limited")
                    raise RpcError(str(err))   # ошибки запроса не повторяем
                return data.get("result")
            except (aiohttp.ClientError, asyncio.TimeoutError, RpcError) as exc:
                retryable = not isinstance(exc, RpcError) or "rate limited" in str(exc)
                if attempt >= retries or not retryable:
                    raise RpcError(f"{method}: {exc}") from exc
                await asyncio.sleep(delay)
                delay *= 2
        return None

    def _on_429(self, retry_after: str | None) -> None:
        self.rate_limited += 1
        self._consecutive_429 += 1
        wait = min(60.0, 2.0 ** self._consecutive_429)
        try:
            if retry_after:
                wait = max(wait, float(retry_after))
        except ValueError:
            pass
        self.pause_until = max(self.pause_until, time.monotonic() + wait)
        log.warning("RPC 429: пауза %.0f с", wait)

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
