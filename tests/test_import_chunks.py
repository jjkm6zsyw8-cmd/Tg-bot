"""22.11: отчёт импорта — частями по 5 кошельков, завис один — остальные не ждут."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

from scanner.analytics import walletcheck as wc


async def test_import_report_in_chunks_with_timeout(monkeypatch):
    sent = []

    class Db:
        async def enqueue(self, kind, payload, priority=5):
            sent.append(payload["text"])

        async def execute(self, *a):
            pass

    async def backfill(e, addr, collect=None):
        if addr == "SLOW":
            await asyncio.sleep(10)
        return 0, None

    stats = SimpleNamespace(trades=12, copyability=0.1, median_hold_s=300, win_rate=0.6)

    async def compute(db, addr, cfg, costs):
        return stats, stats, []

    async def nothing(*a, **k):
        return None

    async def no_rules(*a, **k):
        return {"n": 0, "rules": {}, "best": None}

    async def no_link(*a):
        return False

    monkeypatch.setattr(wc, "backfill_wallet", backfill)
    monkeypatch.setattr(wc, "compute_trader", compute)
    monkeypatch.setattr(wc, "save_trader", nothing)
    monkeypatch.setattr(wc, "linked_to_dev", no_link)
    monkeypatch.setattr(wc, "qualifies", lambda *a: (True, []))
    monkeypatch.setattr(wc, "IMPORT_WALLET_S", 0.05)
    from scanner.analytics import wallet_rules as WR
    monkeypatch.setattr(WR, "evaluate", no_rules)

    async def autofollow(*a, **k):
        return []
    monkeypatch.setattr(WR, "autofollow", autofollow)
    e = SimpleNamespace(cfg={}, costs=lambda: None, price=SimpleNamespace(value=150), db=Db(), _load_traders=nothing)
    wallets = [[f"W{i}" + "x" * 40] for i in range(6)] + [["SLOW"]]
    await wc.import_report(e, wallets)
    assert len(sent) == 2
    assert sent[0].startswith("📥 <b>Импорт: проверено 5 из 7</b>") and sent[0].count("\n✅") + sent[0].count("\n⚪") == 5
    assert sent[1].startswith("📥 <b>Импорт: 7 — готово</b>") and "SLOW: не успел проверить" in sent[1]
