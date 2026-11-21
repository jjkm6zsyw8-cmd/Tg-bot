"""Общий каркас внешних источников: ограничитель запросов, пауза после 429, кеш, тайм-аут, статистика.

Правила (из ТЗ на внешние источники):
* у каждого источника свой лимит под его тариф (token bucket с «весом» запроса);
* 429 → нарастающая пауза (или время из X-RateLimit-Reset / Retry-After); во время паузы запросов нет;
* 403 «нужен тариф выше» → эндпоинт выключается на сутки, не долбим его;
* тайм-аут по умолчанию 1 с: не ответил — возвращаем None, вызывающий помечает «нет данных»;
* кеш по ключу с TTL, отдельный для каждого вида данных.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any

import aiohttp

log = logging.getLogger(__name__)


class TokenBucket:
    """rate — пополнение в секунду, capacity — максимальный «вес» в моменте."""

    def __init__(self, rate: float, capacity: float):
        self.rate = max(rate, 1e-6)
        self.capacity = max(capacity, 1.0)
        self.tokens = self.capacity
        self.updated = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
        self.updated = now

    def wait_time(self, cost: float = 1.0) -> float:
        self._refill()
        cost = min(cost, self.capacity)
        return 0.0 if self.tokens >= cost else (cost - self.tokens) / self.rate

    def take(self, cost: float = 1.0) -> None:
        self._refill()
        self.tokens -= min(cost, self.capacity)

    @property
    def remaining(self) -> float:
        self._refill()
        return self.tokens


class TTLCache:
    def __init__(self, max_items: int = 20_000):
        self.max_items = max_items
        self.data: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def get(self, key: str) -> tuple[bool, Any]:
        item = self.data.get(key)
        if item is None:
            return False, None
        if item[0] < time.monotonic():
            self.data.pop(key, None)
            return False, None
        self.data.move_to_end(key)
        return True, item[1]

    def set(self, key: str, value: Any, ttl: float) -> None:
        self.data[key] = (time.monotonic() + ttl, value)
        self.data.move_to_end(key)
        while len(self.data) > self.max_items:
            self.data.popitem(last=False)


@dataclass
class SourceStats:
    requests: int = 0
    ok: int = 0
    cache_hits: int = 0
    errors: deque = field(default_factory=lambda: deque(maxlen=5000))   # monotonic-время ошибок
    rate_limited: int = 0
    last_error: str = ""
    last_ok_at: float = 0.0
    remaining_header: str | None = None

    def error(self, msg: str) -> None:
        self.errors.append(time.monotonic())
        self.last_error = msg[:200]

    def errors_last_hour(self) -> int:
        cut = time.monotonic() - 3600
        return sum(1 for t in self.errors if t >= cut)


class Unavailable(Exception):
    """Источник выключен, в паузе или эндпоинт недоступен на тарифе."""


class ExternalSource:
    name = "base"
    #: значения по умолчанию; перекрываются config/external.yaml
    rate_per_s: float = 1.0
    capacity: float = 1.0
    needs_key: bool = False

    def __init__(self, key: str = "", enabled: bool = True, rate_per_s: float | None = None,
                 capacity: float | None = None, timeout_s: float = 1.0):
        self.key = key
        self.enabled = enabled and (bool(key) or not self.needs_key)
        self.disabled_reason = "" if self.enabled else ("нет ключа" if self.needs_key and not key else "выключен")
        self.bucket = TokenBucket(rate_per_s or self.rate_per_s, capacity or self.capacity)
        self.timeout_s = timeout_s
        self.cache = TTLCache()
        self.stats = SourceStats()
        self.ban_until = 0.0
        self.consecutive_429 = 0
        self.blocked_paths: dict[str, float] = {}   # путь → до какого времени не вызывать (403 тариф)
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------ http
    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers={"Accept": "application/json",
                                                           "User-Agent": "pumpfun-dev-scanner/1.0"})
        return self._session

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()

    def headers(self) -> dict[str, str]:
        return {}

    def status(self) -> str:
        if not self.enabled:
            return self.disabled_reason
        if time.monotonic() < self.ban_until:
            return f"пауза после 429 ещё {int(self.ban_until - time.monotonic())} с"
        if self.stats.errors_last_hour() >= 20 and self.stats.last_ok_at < time.monotonic() - 300:
            return "ошибки"
        return "ok"

    async def request(self, method: str, url: str, *, path_key: str, params: dict[str, Any] | None = None,
                      json_body: Any = None, cost: float = 1.0, timeout_s: float | None = None) -> Any:
        """Один запрос с соблюдением лимита. Возвращает разобранный JSON или бросает Unavailable."""
        if not self.enabled:
            raise Unavailable(self.disabled_reason)
        now = time.monotonic()
        if now < self.ban_until:
            raise Unavailable("пауза после 429")          # во время бана — никаких повторов
        if self.blocked_paths.get(path_key, 0) > now:
            raise Unavailable("эндпоинт недоступен на тарифе")
        timeout = timeout_s if timeout_s is not None else self.timeout_s
        wait = self.bucket.wait_time(cost)
        if wait > timeout:
            raise Unavailable("лимит запросов")
        if wait:
            await asyncio.sleep(wait)
        self.bucket.take(cost)
        self.stats.requests += 1
        s = await self.session()
        try:
            async with s.request(method, url, params=params, json=json_body, headers=self.headers(),
                                 timeout=aiohttp.ClientTimeout(total=max(0.1, timeout - wait))) as r:
                rem = r.headers.get("X-RateLimit-Remaining") or r.headers.get("x-ratelimit-remaining")
                if rem is not None:
                    self.stats.remaining_header = rem
                if r.status == 429:
                    self._on_429(r.headers)
                    raise Unavailable("429")
                if r.status in (401, 403):
                    body = (await r.text())[:300]
                    self.stats.error(f"{r.status} {path_key}: {body}")
                    if r.status == 401:
                        self.enabled = False
                        self.disabled_reason = "ключ не принят (401)"
                    else:
                        self.blocked_paths[path_key] = now + 86400   # тариф не позволяет — сутки не вызываем
                    raise Unavailable(f"HTTP {r.status}")
                if r.status >= 400:
                    self.stats.error(f"HTTP {r.status} {path_key}")
                    raise Unavailable(f"HTTP {r.status}")
                data = await r.json(content_type=None)
        except asyncio.TimeoutError as exc:
            self.stats.error(f"тайм-аут {path_key}")
            raise Unavailable("тайм-аут") from exc
        except aiohttp.ClientError as exc:
            self.stats.error(f"{type(exc).__name__} {path_key}")
            raise Unavailable("сеть") from exc
        self.consecutive_429 = 0
        self.stats.ok += 1
        self.stats.last_ok_at = time.monotonic()
        return data

    def _on_429(self, headers: Any) -> None:
        self.stats.rate_limited += 1
        self.consecutive_429 += 1
        wait = 2.0 ** min(self.consecutive_429, 8)        # 2, 4, 8 … 256 с
        reset = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")
        retry = headers.get("Retry-After")
        try:
            if reset:
                r = float(reset)
                wait = max(wait, r - time.time() if r > 1e9 else r)
            elif retry:
                wait = max(wait, float(retry))
        except ValueError:
            pass
        self.ban_until = time.monotonic() + min(wait, 3600)
        self.stats.error(f"429, пауза {int(wait)} с")
        log.warning("%s: 429, пауза %.0f с", self.name, wait)

    async def cached(self, kind: str, key: str, ttl: float, fetch: Any) -> Any:
        """Кеш вокруг fetch() (корутина). Unavailable пробрасывается, неудачи не кешируются."""
        hit, value = self.cache.get(f"{kind}:{key}")
        if hit:
            self.stats.cache_hits += 1
            return value
        value = await fetch()
        self.cache.set(f"{kind}:{key}", value, ttl)
        return value

    def health(self) -> dict[str, Any]:
        return {
            "status": self.status(), "enabled": self.enabled, "requests": self.stats.requests,
            "ok": self.stats.ok, "cache_hits": self.stats.cache_hits,
            "errors_hour": self.stats.errors_last_hour(), "rate_limited": self.stats.rate_limited,
            "remaining": self.stats.remaining_header if self.stats.remaining_header is not None
            else round(self.bucket.remaining, 1),
            "last_error": self.stats.last_error,
        }
