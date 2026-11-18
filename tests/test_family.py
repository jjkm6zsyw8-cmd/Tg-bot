"""Семья лучших кошельков: та же цепочка, что нашлась 18.11 в блокчейне вручную —
7BNax → копилка Ce8vr → обменник EnvMK ← ещё копилки Askojg и F6DxYF ← торговый Ar2Y6o1 (торгует сейчас)."""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scanner.analytics import family as FAM
from scanner.analytics.scout import Scout

DSN = os.environ.get("TEST_DATABASE_URL", "postgresql://scanner:scanner@localhost:5432/scanner_test")
BEST = "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5"
TWIN = "7BNaqaMdrW7r5kAw8r5bGzTLvWe9CpP3qNMjMP79LGH5"           # отравление адреса: те же 4 + 4 знака
STASH = "Ce8vrXaKQ9zwA6L24X1TBAMofJPvnkNRpqN9AkMJDBEo"
CASHOUT = "EnvMKia59oSpTQWwVGE6gC5nD3xYLp1vKsDfqjCakYUr"
STASH2 = "F6DxYFixzRXRz8s6vMtcJX7cy9SyVRomdR5X5LMZsmGc"
STASH2_TWIN = "F6DxU6sRn2D3z5jukdE5Ga4UTBbgycNTxNEi7xNcsmGc"
STASH3 = "AskojgRevbJEF3cNs1vjNFZUibVYggubE5SYphTjt9ZW"
SIBLING = "Ar2Y6o1QmrRAskjii1cRfijeKugHH13ycxW5cd7rro1x"           # торгует сейчас
SIBLING_TWIN = "Ar2Y2ck5YiKChGnjE9x4GJprNAdHCHexujBsiSotro1x"
OLD_BOT = "5aLY85pyxiuX3fd4RgM3Yc1e3MAL6b7UgaZz6MS3JUfG"            # торговал 7–8.11, давно молчит
EXCHANGE = "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhLu2Pa1"


def tx(*moves: tuple[str, str, float]) -> dict:
    return {"meta": {"err": None, "innerInstructions": []},
            "transaction": {"message": {"instructions": [
                {"program": "system", "parsed": {"type": "transfer", "info": {
                    "source": s, "destination": d, "lamports": int(sol * 1e9)}}} for s, d, sol in moves]}}}


class FakeRpc:
    """Адрес → транзакции (новые первыми), как getSignaturesForAddress; подпись → транзакция."""

    def __init__(self):
        self.sigs: dict[str, list[dict]] = {}
        self.txs: dict[str, dict] = {}
        self.calls = 0

    def add(self, when: float, src: str, dst: str, sol: float) -> None:
        sig = f"sig{len(self.txs)}"
        self.txs[sig] = tx((src, dst, sol))
        for a in (src, dst):
            self.sigs.setdefault(a, []).insert(0, {"signature": sig, "blockTime": int(when), "err": None})
            self.sigs[a].sort(key=lambda s: -s["blockTime"])

    async def signatures_for(self, address, limit=100, before=None, until=None):
        self.calls += 1
        out = []
        for s in self.sigs.get(address, []):
            if s["signature"] == until:
                break
            out.append(s)
        return out[:limit]

    async def transaction(self, sig):
        self.calls += 1
        return self.txs.get(sig)


def test_transfers_and_twins():
    t = tx((BEST, STASH, 421.5), (TWIN, STASH, 0.0), ("X", "Y", 1.0))
    assert FAM.transfers(t, STASH) == [(BEST, STASH, 421.5), (TWIN, STASH, 0.0)]
    assert FAM.transfers({**t, "meta": {"err": {"x": 1}}}, STASH) == []
    assert FAM.twin(TWIN, BEST) and FAM.twin(SIBLING_TWIN, SIBLING) and not FAM.twin(BEST, BEST)
    assert not FAM.twin(STASH, BEST)


async def test_family_chain_finds_the_owners_active_wallet():
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        now = time.time()
        # лучший кошелёк: 12 сделок бумаги A в плюсе, последняя — 5 часов назад (замолчал)
        for i in range(12):
            await db.execute(
                """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                       tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet)
                   VALUES ('A', $1, $2, $2, 300, 0.1, 1, 1, 'take_profit', 0.025, 0.25, $3)""",
                f"M{i}", datetime.now(timezone.utc) - timedelta(hours=5 + i), BEST)
        rpc = FakeRpc()
        day = 86400
        rpc.add(now - 40 * day, STASH, CASHOUT, 2524.0)                      # копилка → обменник (давно)
        rpc.add(now - 40 * day + 60, STASH3, CASHOUT, 3664.0)               # в ту же минуту — ещё две копилки
        rpc.add(now - 40 * day + 90, STASH2, CASHOUT, 1686.0)
        rpc.add(now - 40 * day + 95, STASH2_TWIN, CASHOUT, 0.0)             # двойник — 0 SOL
        rpc.add(now - 10 * day, OLD_BOT, STASH3, 20.0)                      # старый бот — давно
        rpc.add(now - 6 * 3600, BEST, STASH, 8.0)                           # прибыль в копилку
        rpc.add(now - 6 * 3600 + 120, TWIN, STASH, 0.0)                     # отравление адреса
        rpc.add(now - 5 * 3600, BEST, STASH, 421.5)                         # всё — в копилку и тишина
        rpc.add(now - 2 * 3600, SIBLING, STASH2, 2.4)                       # торгует сейчас, прибыль в копилку 2
        rpc.add(now - 2 * 3600 + 60, SIBLING_TWIN, STASH2, 0.0)
        rpc.add(now - 1 * 3600, SIBLING, EXCHANGE, 3.0)                     # и на биржу — не важно
        settings = SimpleNamespace(exchanges=lambda: {EXCHANGE})
        eng = SimpleNamespace(db=db, cfg={}, price=SimpleNamespace(value=100.0), ext=None, scouts=set(),
                              _load_traders=None, rpc=rpc, settings=settings)
        sc = Scout(eng)  # type: ignore[arg-type]
        fam = sc.family

        events = await fam.run(force=True)        # копилка → (её первый разбор) обменник → копилки владельца
        roles = {r["address"]: r["role"] for r in await db.fetch("SELECT * FROM wallet_family")}
        assert roles == {STASH: "stash", CASHOUT: "cashout", STASH2: "stash", STASH3: "stash"}
        assert TWIN not in roles and STASH2_TWIN not in roles
        assert any("Копилка лучшего кошелька" in e for e in events) and any("Обменник" in e for e in events)
        assert any(STASH2 in e for e in events)
        assert await db.fetchval("SELECT count(*) FROM scout") == 0       # старые переводы — не кандидаты

        events = await fam.run(force=True)        # копилки 2 и 3: первый разбор — свежая прибыль от SIBLING
        r = await db.fetchrow("SELECT * FROM scout WHERE address=$1", SIBLING)
        assert r is not None and r["status"] == "queued" and r["score"] == pytest.approx(FAM.SCORE + 12 * 0.25 * 100)
        assert r["source"] == f"семья {FAM.short(BEST)}" and "копилку F6Dx…smGc" in r["reason"]
        assert await db.fetchval("SELECT count(*) FROM scout") == 1        # OLD_BOT давно молчит, двойники — нет
        assert any("👪" in e and SIBLING in e for e in events)

        # новый перевод с копилки 7BNax — новый кошелёк владельца
        rpc.add(now + 60, STASH, "NEWWALLETxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx1", 30.0)
        events = await fam.run(force=True)
        assert await db.fetchval("SELECT status FROM scout WHERE address=$1",
                                 "NEWWALLETxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx1") == "queued"
        calls = rpc.calls
        assert await fam.run() == []                                        # чаще раза в 15 минут не ходит
        assert rpc.calls == calls
        text = "\n".join(await FAM.report_lines(db))
        assert "🏦 копилка Ce8v…DBEo" in text and "🔁 обменник EnvM…kYUr" in text and "перевёл 421.5 SOL" in text
        assert "владелец 7BNa…LGH5" in text
        # сводка /scout показывает семью
        from scanner.analytics.scout import scout_report
        assert "Семья лучших кошельков" in await scout_report(db)
    finally:
        await db.close()


async def test_shared_address_is_not_a_stash_and_strongest_family_goes_first():
    """18.11: у GZVS «копилка» 94zb… получала прибыль от 5+ разных кошельков — это общий адрес, а не личная копилка:
    за ней не следим, её кандидатов убираем из очереди. Семья сильнейшего владельца (7BNax) — первой в очереди."""
    from scanner.db.pool import Database
    try:
        db = await Database(DSN).connect(min_size=1, max_size=3)
    except Exception:  # noqa: BLE001
        pytest.skip("PostgreSQL недоступен")
    try:
        await db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await db.migrate()
        await db.execute("INSERT INTO wallet_family(address, role, parent, root) VALUES ('SHARED', 'stash', 'GZVS', 'GZVS')")
        await db.execute("INSERT INTO wallet_family(address, role, parent, root) VALUES ('F6Dx', 'stash', 'EnvMK', $1)", BEST)
        for i in range(5):
            await db.execute("INSERT INTO wallet_family(address, role, parent, root) VALUES ($1, 'child', 'SHARED', 'GZVS')",
                             f"U{i}")
            await db.execute("INSERT INTO scout(address, status, source, score) VALUES ($1, $2, 'семья GZVS', 10000)",
                             f"U{i}", "probation" if i == 0 else "queued")
        await db.execute("INSERT INTO wallet_family(address, role, parent, root) VALUES ($1, 'child', 'F6Dx', $2)",
                         SIBLING, BEST)
        await db.execute("INSERT INTO scout(address, status, source, score) VALUES ($1, 'queued', 'семья 7BNa', 10000)",
                         SIBLING)
        for i in range(20):                                   # 7BNax: 20 сделок бумаги A по +28%
            await db.execute(
                """INSERT INTO paper_positions(portfolio, mint, opened_at, closed_at, entry_mc, cost_sol, tokens,
                       tokens_initial, exit_reason, pnl_sol, pnl_pct, source_wallet)
                   VALUES ('A', $1, now(), now(), 300, 0.1, 1, 1, 'take_profit', 0.028, 0.28, $2)""", f"M{i}", BEST)
        eng = SimpleNamespace(db=db, cfg={}, price=SimpleNamespace(value=100.0), ext=None, scouts=set(),
                              _load_traders=None, rpc=FakeRpc(), settings=None)
        fam = Scout(eng).family  # type: ignore[arg-type]
        events = await fam.check_services()
        assert await db.fetchval("SELECT role FROM wallet_family WHERE address='SHARED'") == "service"
        assert await db.fetchval("SELECT role FROM wallet_family WHERE address='F6Dx'") == "stash"    # 1 кошелёк — личная
        left = {r["address"]: r["status"] for r in await db.fetch("SELECT address, status FROM scout")}
        assert left == {"U0": "probation", SIBLING: "queued"}       # на испытании — доиспытывается
        assert any("общий адрес" in e and "убрано 4" in e for e in events)
        await fam.reprioritize()
        assert await db.fetchval("SELECT score FROM scout WHERE address=$1", SIBLING) == pytest.approx(10000 + 560)
        text = "\n".join(await FAM.report_lines(db))
        assert "⚠️ общий адрес SHAR" in text.replace("SHARED", "SHAR") and "кошелёк U1" not in text   # не семья
    finally:
        await db.close()
