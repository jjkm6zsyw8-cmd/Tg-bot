"""Проверка импортированных кошельков: история сделок через RPC → статистика → вердикт «копировать или нет».

Для каждого кошелька: последние ~150 транзакций (getSignaturesForAddress + getTransaction), из них — сделки
Pump.fun/PumpSwap этого кошелька. Копия оценивается по капитализации сразу после его покупки и сразу после его
продажи (без нашей задержки — это верхняя граница; бумажная торговля покажет честную цифру с задержкой).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ..alerts import format as fmt
from ..ingest.pump_parser import ParseStats, parse_transaction
from ..ingest.rpc import RpcError
from ..models import Trade
from .curve import mc_sol
from .copytrade import compute_trader, linked_to_dev, qualifies, save_trader

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger(__name__)


async def backfill_wallet(e: "Engine", wallet: str, max_tx: int = 150) -> tuple[int, float | None]:
    """Сделки кошелька из последних max_tx транзакций → таблица trades. (сколько добавлено, время самой старой)."""
    rpc, db = e.rpc, e.db
    try:
        sigs = await rpc.signatures_for(wallet, limit=max_tx)
    except RpcError:
        return 0, None
    sigs = [s for s in sigs if not s.get("err")]
    if not sigs:
        return 0, None
    oldest = min((s.get("blockTime") or 0) for s in sigs) or None
    have = {r["signature"] for r in await db.fetch(
        "SELECT DISTINCT signature FROM trades WHERE trader = $1 AND signature = ANY($2)",
        wallet, [s["signature"] for s in sigs])}
    pool_map = {r["amm_pool"]: r["mint"] for r in await db.fetch(
        "SELECT amm_pool, mint FROM tokens WHERE amm_pool IS NOT NULL AND created_at > now() - interval '30 days'")}
    stats = ParseStats()
    found: list[Trade] = []
    for s in sigs:
        if s["signature"] in have:
            continue
        if e.rpc_over_budget():
            break
        try:
            tx = await rpc.transaction(s["signature"])
        except RpcError:
            continue
        if not tx:
            continue
        for ev in parse_transaction(tx, s["signature"], s.get("slot"), "rpc", stats, pool_map):
            if isinstance(ev, Trade) and ev.trader == wallet:
                found.append(ev)
    if found:
        await db.p.executemany(
            """INSERT INTO trades(signature, mint, trader, is_buy, sol_amount, token_amount, mc_sol, v_sol, v_tokens,
                   real_tokens, new_balance, fee_sol, pool, slot, ts, received_at, source)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
               ON CONFLICT (signature, mint, trader, is_buy) DO NOTHING""",
            [(t.signature, t.mint, t.trader, t.is_buy, t.sol_amount, t.token_amount, mc_sol(t.v_sol, t.v_tokens),
              t.v_sol, t.v_tokens, t.real_tokens, t.new_token_balance, t.fee_sol, t.pool, t.slot, t.ts,
              t.received_at, "rpc") for t in found])
        await db.p.executemany(
            """INSERT INTO tokens(mint, signature, deployer, current_owner, created_at, initial_mc_sol, source)
               VALUES ($1, $2, '', '', $3, $4, 'stub') ON CONFLICT (mint) DO NOTHING""",
            [(t.mint, f"stub:{t.mint}", t.ts, mc_sol(t.v_sol, t.v_tokens)) for t in found])
    return len(found), oldest


def verdict(trades: int, copy: float, hold_s: float) -> tuple[str, str]:
    if trades < 5:
        return "⚪", "мало сделок в истории — решит бумага"
    if hold_s < 30:
        return "⚠️", f"держит ~{fmt.ago(hold_s)} — за ним не успеть"
    if copy <= 0:
        return "⚠️", "копия в минусе"
    return "✅", "копировать"


async def import_report(e: "Engine", wallets: list[list[Any]]) -> None:
    """Фоновая задача после импорта: история, статистика, сохранение в трейдеры (свой список), отчёт в «Смарт-мани»."""
    cfg, costs = e.cfg, e.costs()
    lines: list[str] = []
    good = 0
    for item in wallets:
        addr, label = item[0], (item[1] if len(item) > 1 else None)
        try:
            added, oldest = await backfill_wallet(e, addr)
            s7, s30, _trips = await compute_trader(e.db, addr, cfg, costs)
            linked = await linked_to_dev(e.db, addr)
            ok, _fails = qualifies(s7, s30, linked, cfg)
            await save_trader(e.db, addr, s7, s30, ok, linked, label=label, manual=True)
            await e.db.execute("UPDATE traders SET active=true WHERE address=$1", addr)
        except Exception:  # noqa: BLE001
            log.exception("проверка кошелька %s", addr)
            lines.append(f"❔ {fmt.esc(label or addr[:6])}: не удалось проверить")
            continue
        mark, why = verdict(s30.trades, s30.copyability, s30.median_hold_s)
        good += mark == "✅"
        span = ""
        if oldest:
            days = (datetime.now(timezone.utc).timestamp() - oldest) / 86400
            span = f" · история {days:.0f} д" if days >= 1 else f" · история {days * 24:.0f} ч"
        name = fmt.esc(label or f"{addr[:4]}…{addr[-4:]}")
        if s30.trades:
            lines.append(f"{mark} <b>{name}</b> · {s30.trades} сд · прибыльных {s30.win_rate:.0%} · "
                         f"копия {s30.copyability:+.0%}/сд · держит ~{fmt.ago(s30.median_hold_s)}{span} — {why}")
        else:
            lines.append(f"{mark} <b>{name}</b> · сделок Pump.fun не найдено{span} — {why}")
    await e._load_traders()
    head = (f"📥 <b>Импорт кошельков: {len(wallets)}</b> · стоит копировать {good}\n"
            "Слежу за всеми в реальном времени (Helius). Бумага копирует покупки от "
            f"{float(cfg.get('copy_min_buy_sol', 0.1)):g} SOL; выход — когда кошелёк продаёт или по схемам A–G.\n"
            "<i>Копия — без нашей задержки, это потолок; честную цифру покажет бумага.</i>")
    text = head + "\n\n" + "\n".join(lines)
    for i in range(0, len(text), 3800):
        await e.db.enqueue("text", {"topic": "smart", "text": text[i:i + 3800]}, priority=4)
