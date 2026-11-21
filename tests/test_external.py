"""Внешние источники: лимиты, 429, 403, тайм-аут, кеш, разбор ответов, фильтры, приоритет ончейна."""
from __future__ import annotations

import asyncio
import time

import pytest
from aiohttp import web

from scanner.external.adapters import (parse_dex_pairs, parse_gmgn_security, parse_gmgn_wallet, parse_rugcheck)
from scanner.external.base import ExternalSource, TokenBucket, Unavailable
from scanner.external.hub import ExternalHub, TokenCheck


class Dummy(ExternalSource):
    name = "dummy"
    rate_per_s = 100
    capacity = 100


@pytest.fixture
async def server():
    state = {"n": 0, "mode": "ok"}

    async def handler(request: web.Request) -> web.Response:
        state["n"] += 1
        mode = state["mode"]
        if mode == "429":
            return web.Response(status=429, headers={"Retry-After": "30"})
        if mode == "403":
            return web.json_response({"error": "PLAN_UPGRADE_REQUIRED"}, status=403)
        if mode == "slow":
            await asyncio.sleep(2)
        return web.json_response({"ok": True, "n": state["n"]})

    app = web.Application()
    app.router.add_get("/x", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}/x", state
    await runner.cleanup()


async def test_429_pauses_and_no_retry_during_ban(server):
    url, state = server
    src = Dummy()
    state["mode"] = "429"
    with pytest.raises(Unavailable):
        await src.request("GET", url, path_key="x")
    assert src.ban_until > time.monotonic() + 25           # Retry-After 30 с
    state["mode"] = "ok"
    n = state["n"]
    with pytest.raises(Unavailable):
        await src.request("GET", url, path_key="x")
    assert state["n"] == n                                  # во время бана запрос не ушёл
    assert "пауза" in src.status()
    await src.close()


async def test_backoff_grows():
    src = Dummy()
    src._on_429({})
    first = src.ban_until - time.monotonic()
    src._on_429({})
    assert src.ban_until - time.monotonic() > first


async def test_403_blocks_endpoint_for_a_day(server):
    url, state = server
    src = Dummy()
    state["mode"] = "403"
    with pytest.raises(Unavailable):
        await src.request("GET", url, path_key="pnl")
    state["mode"] = "ok"
    n = state["n"]
    with pytest.raises(Unavailable):
        await src.request("GET", url, path_key="pnl")
    assert state["n"] == n
    assert (await src.request("GET", url, path_key="other"))["ok"]   # другие эндпоинты работают
    await src.close()


async def test_timeout_one_second(server):
    url, state = server
    src = Dummy(timeout_s=1.0)
    state["mode"] = "slow"
    t = time.monotonic()
    with pytest.raises(Unavailable):
        await src.request("GET", url, path_key="x")
    assert time.monotonic() - t < 1.5
    await src.close()


async def test_cache_ttl(server):
    url, state = server
    src = Dummy()

    async def fetch():
        return await src.request("GET", url, path_key="x")
    a = await src.cached("k", "1", 15, fetch)
    b = await src.cached("k", "1", 15, fetch)
    assert a == b and state["n"] == 1 and src.stats.cache_hits == 1
    await src.close()


def test_bucket_weights():
    b = TokenBucket(rate=5, capacity=5)
    assert b.wait_time(5) == 0
    b.take(5)
    assert b.wait_time(5) == pytest.approx(1.0, abs=0.05)


def test_no_key_disables_source():
    from scanner.external.adapters import Gmgn
    g = Gmgn(key="")
    assert not g.enabled and g.status() == "нет ключа"


def test_parsers():
    rc = parse_rugcheck({
        "mintAuthority": None, "freezeAuthority": "F1", "score_normalised": 12, "rugged": False,
        "knownAccounts": {"POOL": {"type": "AMM"}},
        "topHolders": [{"owner": "POOL", "pct": 40}, {"owner": "A", "pct": 10, "insider": True}, {"owner": "B", "pct": 5}],
        "risks": [{"name": "Freeze Authority still enabled", "level": "danger"}]})
    assert rc["top10_pct"] == pytest.approx(0.15) and rc["insiders_in_top"] == 1
    assert rc["freeze_authority"] == "F1" and rc["danger_risks"] == ["Freeze Authority still enabled"]
    gm = parse_gmgn_security({"renounced_mint": 1, "renounced_freeze_account": 1, "top_10_holder_rate": "0.2",
                              "rug_ratio": "0.4"})
    assert gm["rug_ratio"] == 0.4 and gm["top10_rate"] == 0.2
    w = parse_gmgn_wallet({"common": {"tags": ["smart_degen", "kol"], "twitter_username": "x"}, "winrate": 0.6})
    assert w["is_smart"] and w["is_kol"] and w["twitter"] == "x"
    p = parse_dex_pairs([{"baseToken": {"address": "M"}, "liquidity": {"usd": 5}, "priceUsd": "1"},
                         {"baseToken": {"address": "M"}, "liquidity": {"usd": 50}, "priceUsd": "2"}], "M")
    assert p["price_usd"] == 2.0


def _hub() -> ExternalHub:
    from scanner.config import Env
    return ExternalHub(Env(), db=None)   # type: ignore[arg-type]


def test_external_filters_block_and_onchain_priority():
    hub = _hub()
    chk = TokenCheck(facts={"rugcheck": {"mint_authority": None, "freeze_authority": "F", "rugged": False,
                                         "danger_risks": [], "top10_pct": 0.6, "score_normalised": 5},
                            "gmgn": {"mint_renounced": 1, "freeze_renounced": 1, "rug_ratio": 0.5}})
    hub._evaluate(chk, {"top10": 0.2})
    assert any("freeze" in b for b in chk.block) and any("rug_ratio" in b for b in chk.block)
    assert not any("топ-10" in b for b in chk.block)          # свой ончейн топ-10 в приоритете
    assert chk.facts["discrepancy"]["top10"]["onchain"] == 0.2
    clean = TokenCheck(facts={"rugcheck": {"mint_authority": None, "freeze_authority": None, "rugged": False,
                                           "danger_risks": ["Low Liquidity"], "top10_pct": 0.2}},
                       missing=["gmgn"])
    hub._evaluate(clean, {})
    assert not clean.block and any("нет данных: GMGN" in line for line in clean.lines)
    assert any("Low Liquidity" in line for line in clean.lines)   # опасные риски выделены, но не отсекают


async def test_disabled_source_is_not_missing():
    hub = _hub()
    hub.disable_all()
    chk = await hub.token_checks("M", timeout=0.5)
    assert chk.missing == [] and chk.block == []


def test_dex_pair_prefers_sol_quote_over_exotic():
    """Пара к экзотическому токену (токенизированная акция) с большей «ликвидностью» не должна выигрывать."""
    from scanner.external.adapters import SOL_MINT
    data = [
        {"baseToken": {"address": "M"}, "quoteToken": {"address": "AMDxxx"}, "liquidity": {"usd": 9e9},
         "priceUsd": "0.3", "marketCap": 306e6},
        {"baseToken": {"address": "M"}, "quoteToken": {"address": SOL_MINT}, "liquidity": {"usd": 40_000},
         "priceUsd": "0.00002", "marketCap": 20_000},
    ]
    p = parse_dex_pairs(data, "M")
    assert p["market_cap_usd"] == 20_000 and p["good_quote"]
    only_exotic = parse_dex_pairs(data[:1], "M")
    assert only_exotic["market_cap_usd"] == 306e6 and not only_exotic["good_quote"]


def test_dex_pairs_ignore_fake_pumpswap_pool_older_than_curve():
    """Piper: кривая pump.fun $37k (у DexScreener без ликвидности) и левый пул PumpSwap $1.7k, созданный раньше."""
    data = [
        {"baseToken": {"address": "M"}, "quoteToken": {"symbol": "SOL", "address": "So11111111111111111111111111111111111111112"},
         "dexId": "pumpswap", "pairCreatedAt": 1000, "liquidity": {"usd": 300}, "marketCap": 1725, "priceUsd": "0.0000017"},
        {"baseToken": {"address": "M"}, "quoteToken": {"symbol": "SOL", "address": "So11111111111111111111111111111111111111112"},
         "dexId": "pumpfun", "pairCreatedAt": 5000, "marketCap": 37061, "priceUsd": "0.000037"},
    ]
    assert parse_dex_pairs(data, "M")["market_cap_usd"] == 37061
    data[0]["pairCreatedAt"] = 9000                       # пул после кривой — миграция, берём его
    assert parse_dex_pairs(data, "M")["market_cap_usd"] == 1725
