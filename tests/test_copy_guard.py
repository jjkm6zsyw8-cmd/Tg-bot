"""26.11 (сводное ТЗ, C): защита копирования — пул от 30 SOL, ставка ≤10% его покупки и ≤3% SOL пула, мы крупнейший
покупатель после него → меньше ставка, лимит SOL на один кошелёк."""
from __future__ import annotations

import time
from types import SimpleNamespace

from tests.real_helpers import FakeChain, pk
from tests.test_real import _fast, db, fire, live, shadow, skipped  # noqa: F401 — фикстуры

GUARD = {"real_min_pool_sol": 30.0, "real_copy_max_share": 0.10, "real_max_depth_share": 0.03,
         "real_follow_max_sol": 0.5, "real_copy_largest": True, "real_stake_usd": 20, "real_min_stake_usd": 1,
         "real_max_open": 10, "real_max_impact": 0.5}


async def test_small_pool_is_skipped_and_big_pool_enters(db):
    chain = FakeChain()
    small, big = pk("cg-small"), pk("cg-big")
    chain.add_curve(small, 40.0)                       # в кривой ~10 SOL
    chain.add_curve(big, 200.0)                        # ~50 SOL
    tr = shadow(db, chain, **GUARD)
    await tr.start()
    await fire(tr, chain, mint=small)
    assert not tr.positions and skipped(tr, "мелкий пул, не входим")
    await fire(tr, chain, mint=big, alert_id=2)
    assert len(tr.positions) == 1


async def test_stake_is_cut_by_his_buy_and_depth(db, monkeypatch):
    chain = FakeChain()
    m = pk("cg-share")
    chain.add_curve(m, 200.0)
    tr = live(db, chain, monkeypatch, **GUARD)           # 27.11: урезания ставки — только в бою
    await tr.start()
    await fire(tr, chain, mint=m, copy_sol=0.5)        # он купил на 0.5 SOL → мы не больше 0.05
    row = await db.fetchrow("SELECT (extra->>'stake_sol')::float AS s FROM real_positions")
    assert row["s"] <= 0.0500001


async def test_largest_buyer_after_him_cuts_stake(db, monkeypatch):
    chain = FakeChain()
    m = pk("cg-largest")
    chain.add_curve(m, 200.0)
    tr = live(db, chain, monkeypatch, **GUARD)
    now = time.time()
    tr.e.states = {str(m): SimpleNamespace(trades=[
        SimpleNamespace(ts=now - 0.5, trader="OTHER1", is_buy=True, sol=0.03),
        SimpleNamespace(ts=now - 0.4, trader="TRADER", is_buy=True, sol=2.0),     # сам трейдер — не в счёт
        SimpleNamespace(ts=now - 30, trader="OLD", is_buy=True, sol=5.0)])}       # до его покупки — не в счёт
    await tr.start()
    await fire(tr, chain, mint=m, trigger_ts=now - 0.8)
    row = await db.fetchrow("SELECT (extra->>'stake_sol')::float AS s FROM real_positions")
    assert row["s"] <= 0.0300001                       # не больше крупнейшей чужой покупки после него


async def test_wallet_limit(db, monkeypatch):
    chain = FakeChain()
    tr = live(db, chain, monkeypatch, **{**GUARD, "real_follow_max_sol": 0.2})
    await tr.start()
    for i in range(4):
        m = pk(f"cg-lim{i}")
        chain.add_curve(m, 200.0)
        await fire(tr, chain, mint=m, alert_id=10 + i)
    spent = sum(p.cost_sol for p in tr.positions.values())
    assert spent <= 0.2 + 0.01 and skipped(tr, "на этот кошелёк уже открыто")


async def test_shadow_stake_is_always_fixed(db):
    """27.11 (владелец): в тени ставка всегда 0.14 SOL — ни 10% его покупки, ни доля пула, ни крупнейший покупатель,
    ни лимит на кошелёк её не урезают; мелкий пул — по-прежнему не входим (как в модели)."""
    chain = FakeChain()
    tr = shadow(db, chain, **{**GUARD, "real_follow_max_sol": 0.2, "real_shadow_stake_sol": 0.14})
    await tr.start()
    for i in range(3):
        m = pk(f"cg-fix{i}")
        chain.add_curve(m, 200.0)
        await fire(tr, chain, mint=m, alert_id=20 + i, copy_sol=0.5)
    stakes = [float(r["s"]) for r in await db.fetch("SELECT (extra->>'stake_sol')::float AS s FROM real_positions")]
    assert stakes == [0.14, 0.14, 0.14]
    small = pk("cg-fix-small")
    chain.add_curve(small, 40.0)
    await fire(tr, chain, mint=small, alert_id=30)
    assert skipped(tr, "мелкий пул, не входим")


def test_coin_once_shadow_does_not_block_money_and_other_bot_wallet(db):
    """27.11: несколько кошельков купили одну монету — входим один раз. Тень по монете не мешает бою; бой по монете
    занимает её и для тени; бой в другом кошельке бота занимает её для боя."""
    tr, other = shadow(db, FakeChain()), shadow(db, FakeChain())
    tr._busy, other._busy = {}, {}
    tr.positions = {1: SimpleNamespace(mint="X", extra={"mode": "shadow"}, trader="A")}
    other.positions = {2: SimpleNamespace(mint="Z", extra={"mode": "live"}, trader="B")}
    tr.peers = other.peers = {1: tr, 2: other}
    assert tr.coin_taken("X", "shadow") == "уже есть позиция по монете"
    assert tr.coin_taken("X", "live") is None
    tr.positions[3] = SimpleNamespace(mint="X", extra={"mode": "live"}, trader="C")
    assert tr.coin_taken("X", "live") == "уже есть позиция по монете"
    assert tr.coin_taken("Z", "live") == "монету уже купил другой кошелёк бота"
    assert tr.coin_taken("Z", "shadow") is None                 # тень меряется в своём кошельке
    other._busy["Q"] = "live"
    assert tr.coin_taken("Q", "live") == "монету уже купил другой кошелёк бота"
