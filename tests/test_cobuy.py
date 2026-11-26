"""Соседи лучшего кошелька по блокчейну: кто покупает те же монеты раньше него, в том же блоке, после."""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scanner.analytics import cobuy as CB
from scanner.analytics.scout import Scout

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
BEST = "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5"
LEADER = "LeaderxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxLD1"
SAMEBLK = "SameBlkxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxSB1"
COPIER = "CopierxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxCP1"
POOL = "PoolAuthxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxPA1"
WSOL = CB.WSOL


def swap_tx(wallet: str, mint: str, side: str, sol: float, tokens: float = 1000.0) -> dict:
    """Покупка/продажа на PumpSwap: SOL кошелька меняется, токены приходят на его счёт; пул (не подписант) — обратно."""
    sign = 1 if side == "buy" else -1
    return {"meta": {"err": None, "preBalances": [int(10e9), 0], "postBalances": [int(10e9 - sign * sol * 1e9), 0],
                     "preTokenBalances": [
                         {"accountIndex": 2, "mint": mint, "owner": wallet, "uiTokenAmount": {"uiAmount": 0 if side == "buy" else tokens}},
                         {"accountIndex": 3, "mint": mint, "owner": POOL, "uiTokenAmount": {"uiAmount": 1e6}}],
                     "postTokenBalances": [
                         {"accountIndex": 2, "mint": mint, "owner": wallet, "uiTokenAmount": {"uiAmount": tokens if side == "buy" else 0}},
                         {"accountIndex": 3, "mint": mint, "owner": POOL, "uiTokenAmount": {"uiAmount": 1e6 - sign * tokens}}]},
            "transaction": {"message": {"accountKeys": [{"pubkey": wallet, "signer": True},
                                                        {"pubkey": POOL, "signer": False}]}}}


class FakeRpc:
    def __init__(self):
        self.sigs: dict[str, list[dict]] = {}
        self.txs: dict[str, dict] = {}

    def add(self, mint, wallet, side, sol, slot, when):
        sig = f"s{len(self.txs)}"
        self.txs[sig] = swap_tx(wallet, mint, side, sol)
        self.sigs.setdefault(mint, []).append({"signature": sig, "slot": slot, "blockTime": int(when), "err": None})
        self.sigs[mint].sort(key=lambda s: -s["slot"])

    async def signatures_for(self, address, limit=100, before=None, until=None):
        out, take = [], before is None
        for s in self.sigs.get(address, []):
            if take:
                out.append(s)
            elif s["signature"] == before:
                take = True
        return out[:limit]

    async def transaction(self, sig):
        return self.txs.get(sig)


def test_swaps_of_ignores_pool_and_non_signers():
    tx = swap_tx(LEADER, "M1", "buy", 1.5)
    assert CB.swaps_of(tx, "M1") == [(LEADER, "buy", pytest.approx(1.5))]
    assert CB.swaps_of(swap_tx(LEADER, "M1", "sell", 2.0), "M1") == [(LEADER, "sell", pytest.approx(2.0))]
    assert CB.swaps_of(tx, "OTHER") == [] and CB.swaps_of({**tx, "meta": {**tx["meta"], "err": 1}}, "M1") == []


async def test_neighbours_and_queue():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        rpc = FakeRpc()
        now = time.time()
        for i in range(5):
            mint, t, slot = f"M{i}", now - 3600 * (i + 1), 1000 + i * 10_000
            await db.execute("""INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol,
                                    slot, ts, pool) VALUES ($1, $2, $3, true, 1.8, 1000, 300, $4, $5, 'pump-amm')""",
                             f"b{i}", mint, BEST, slot, datetime.fromtimestamp(t, timezone.utc))
            rpc.add(mint, BEST, "buy", 1.8, slot, t)
            rpc.add(mint, LEADER, "buy", 1.2, slot - 20, t - 8)          # раньше него
            if i < 3:
                rpc.add(mint, SAMEBLK, "buy", 0.9, slot, t)              # в том же блоке
            rpc.add(mint, COPIER, "buy", 0.5, slot + 3, t + 1)           # сразу после
            rpc.add(mint, "FarAwayxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxFA1", "buy", 1.0, slot - 400, t - 160)  # вне окна
            rpc.add(mint, "7BNaqaMdrW7r5kAw8r5bGzTLvWe9CpP3qNMjMP79LGH5", "buy", 1.0, slot - 5, t - 2)       # двойник
        res = await CB.neighbours(db, rpc, BEST, coins=10)
        assert res.coins == 5 and res.found == 5
        assert res.near[LEADER].lead == 5 and len(res.near[LEADER].before) == 5
        assert len(res.near[SAMEBLK].same) == 3 and res.near[SAMEBLK].lead == 3
        assert res.near[COPIER].lead == 0 and len(res.near[COPIER].after) == 5
        assert not any(w.startswith("FarAway") or w.startswith("7BNaqa") for w in res.near)
        text = CB.report(res)
        assert f"{LEADER} | 5 / 0 / 0 | 1.20" in text and f"{COPIER} | после на 5 монетах" in text
        eng = SimpleNamespace(db=db, cfg={}, price=SimpleNamespace(value=100.0), ext=None, scouts=set(),
                              _load_traders=None)
        queued = await CB.queue_leaders(Scout(eng), res)  # type: ignore[arg-type]
        rows = {r["address"]: r for r in await db.fetch("SELECT * FROM scout")}
        assert set(rows) == {LEADER, SAMEBLK} and len(queued) == 2               # копировщик — не кандидат
        assert rows[LEADER]["score"] > rows[SAMEBLK]["score"] and "раньше него" in rows[LEADER]["reason"]
        assert await CB.best_wallet(db) is None                                  # бумаги нет — взять не из чего
    finally:
        await db.close()
