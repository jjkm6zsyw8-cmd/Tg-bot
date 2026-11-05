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
from ..models import TokenCreated, Trade
from .curve import mc_sol
from .copytrade import compute_trader, linked_to_dev, qualifies, save_trader

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger(__name__)


DEV_HINTS = ("дев", "dev", "твинк", "twink", "кап")


def sell_mc_before(t: Trade) -> float:
    """MC непосредственно перед продажей: резервы в событии — уже после сделки (SOL ушёл, токены вернулись)."""
    v_tok = t.v_tokens - t.token_amount
    if v_tok <= 0:
        return mc_sol(t.v_sol, t.v_tokens)
    return mc_sol(t.v_sol + t.sol_amount, v_tok)


def dev_profile(wallet: str, creates: list[TokenCreated], trades: list[Trade]) -> dict[str, Any]:
    """Почерк дева: на каком MC и через сколько после запуска он впервые продаёт свою монету."""
    sells: dict[str, list[Trade]] = {}
    for t in trades:
        if t.trader == wallet and not t.is_buy and t.pool == "pump":
            sells.setdefault(t.mint, []).append(t)
    rows: list[tuple[float, float, bool]] = []
    mine = [c for c in creates if wallet in (c.deployer, c.creator)]
    for c in mine:
        ss = sorted(sells.get(c.mint, []), key=lambda x: x.ts)
        if not ss:
            continue
        first = ss[0]
        start = mc_sol(c.v_sol, c.v_tokens) or 27.96
        mc = sell_mc_before(first)
        rows.append((mc, max(0.0, (first.ts - c.ts).total_seconds()), mc >= 1.5 * start))
    out: dict[str, Any] = {"launches": len(mine), "sold": len(rows)}
    if rows:
        mcs = sorted(r[0] for r in rows)
        secs = sorted(r[1] for r in rows)

        def q(xs: list[float], f: float) -> float:
            return xs[min(len(xs) - 1, int(f * (len(xs) - 1) + 0.5))]
        out.update(med_sell_mc=q(mcs, 0.5), p25_sell_mc=q(mcs, 0.25), p75_sell_mc=q(mcs, 0.75),
                   med_sell_s=q(secs, 0.5), hit_rate=sum(1 for r in rows if r[2]) / len(rows))
    return out


def is_dev(label: str | None, profile: dict[str, Any]) -> bool:
    return profile.get("launches", 0) >= 1 or any(h in (label or "").lower() for h in DEV_HINTS)


async def backfill_wallet(e: "Engine", wallet: str, max_tx: int = 150,
                          collect: list[Any] | None = None) -> tuple[int, float | None]:
    """Сделки кошелька из последних max_tx транзакций → таблица trades. (сколько добавлено, время самой старой).
    collect — сюда же складываются все события кошелька (создания и сделки) для почерка дева."""
    rpc, db = e.rpc, e.db
    try:
        sigs = await rpc.signatures_for(wallet, limit=max_tx)
    except RpcError:
        return 0, None
    sigs = [s for s in sigs if not s.get("err")]
    if not sigs:
        return 0, None
    oldest = min((s.get("blockTime") or 0) for s in sigs) or None
    have = set() if collect is not None else {r["signature"] for r in await db.fetch(
        "SELECT DISTINCT signature FROM trades WHERE trader = $1 AND signature = ANY($2)",
        wallet, [s["signature"] for s in sigs])}
    pool_map = {r["amm_pool"]: r["mint"] for r in await db.fetch(
        "SELECT amm_pool, mint FROM tokens WHERE amm_pool IS NOT NULL AND created_at > now() - interval '30 days'")}
    stats = ParseStats()
    found: list[Trade] = []
    creates: list[TokenCreated] = []
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
                if collect is not None:
                    collect.append(ev)
            elif isinstance(ev, TokenCreated) and wallet in (ev.deployer, ev.creator):
                creates.append(ev)
                if collect is not None:
                    collect.append(ev)
    if creates:
        await db.p.executemany(
            """INSERT INTO tokens(mint, signature, deployer, current_owner, name, symbol, uri, bonding_curve,
                   created_at, initial_mc_sol, initial_buy_sol, source)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'rpc') ON CONFLICT (mint) DO NOTHING""",
            [(c.mint, c.signature, c.deployer, c.creator or c.deployer, c.name[:200], c.symbol[:50], c.uri[:500],
              c.bonding_curve, c.ts, mc_sol(c.v_sol, c.v_tokens) or 27.96, c.initial_buy_sol) for c in creates])
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


async def save_dev(e: "Engine", addr: str, label: str | None, prof: dict[str, Any]) -> None:
    db = e.db
    await db.execute("DELETE FROM watchlist WHERE kind='trader' AND address=$1", addr)
    await db.execute("INSERT INTO watchlist(kind, address, label) VALUES ('dev', $1, $2) "
                     "ON CONFLICT (kind, address) DO UPDATE SET label=COALESCE(EXCLUDED.label, watchlist.label)",
                     addr, label)
    await db.execute("UPDATE traders SET active=false WHERE address=$1", addr)   # своих монет дев не «копируем»
    await db.execute(
        """INSERT INTO my_devs(address, launches, sold, med_sell_mc, p25_sell_mc, p75_sell_mc, med_sell_s, hit_rate,
               updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8, now())
           ON CONFLICT (address) DO UPDATE SET launches=EXCLUDED.launches, sold=EXCLUDED.sold,
               med_sell_mc=EXCLUDED.med_sell_mc, p25_sell_mc=EXCLUDED.p25_sell_mc, p75_sell_mc=EXCLUDED.p75_sell_mc,
               med_sell_s=EXCLUDED.med_sell_s, hit_rate=EXCLUDED.hit_rate, updated_at=now()""",
        addr, prof.get("launches", 0), prof.get("sold", 0), prof.get("med_sell_mc"), prof.get("p25_sell_mc"),
        prof.get("p75_sell_mc"), prof.get("med_sell_s"), prof.get("hit_rate"))


def dev_line(name: str, prof: dict[str, Any], sol_usd: float) -> str:
    if not prof.get("sold"):
        n = prof.get("launches", 0)
        return (f"👻 <b>{name}</b> · запусков в истории {n} — продаж дева не видно, цели по его почерку пока нет"
                if n else f"👻 <b>{name}</b> · запусков в последних транзакциях нет — алерт придёт при первом запуске")
    return (f"👻 <b>{name}</b> · запусков {prof['launches']}, продаёт на "
            f"{fmt.mc_usd(prof['p25_sell_mc'], sol_usd)}–{fmt.mc_usd(prof['p75_sell_mc'], sol_usd)} "
            f"через ~{fmt.ago(prof['med_sell_s'])} · по схеме ×1.5+ {prof['hit_rate']:.0%}")


async def import_report(e: "Engine", wallets: list[list[Any]]) -> None:
    """Фоновая задача после импорта: история каждого кошелька → дев или трейдер → отчёт в «Копитрейд»."""
    cfg, costs = e.cfg, e.costs()
    sol_usd = e.price.value or 150
    dev_lines: list[str] = []
    trader_lines: list[str] = []
    good = 0
    for item in wallets:
        addr, label = item[0], (item[1] if len(item) > 1 else None)
        name = fmt.esc(label or f"{addr[:4]}…{addr[-4:]}")
        try:
            events: list[Any] = []
            _added, oldest = await backfill_wallet(e, addr, collect=events)
            creates = [x for x in events if isinstance(x, TokenCreated)]
            trades = [x for x in events if isinstance(x, Trade)]
            prof = dev_profile(addr, creates, trades)
            if is_dev(label, prof):
                await save_dev(e, addr, label, prof)
                dev_lines.append(dev_line(name, prof, sol_usd))
                continue
            s7, s30, _trips = await compute_trader(e.db, addr, cfg, costs)
            linked = await linked_to_dev(e.db, addr)
            ok, _fails = qualifies(s7, s30, linked, cfg)
            await save_trader(e.db, addr, s7, s30, ok, linked, label=label, manual=True)
            await e.db.execute("UPDATE traders SET active=true WHERE address=$1", addr)
        except Exception:  # noqa: BLE001
            log.exception("проверка кошелька %s", addr)
            trader_lines.append(f"❔ {name}: не удалось проверить")
            continue
        mark, why = verdict(s30.trades, s30.copyability, s30.median_hold_s)
        good += mark == "✅"
        span = ""
        if oldest:
            days = (datetime.now(timezone.utc).timestamp() - oldest) / 86400
            span = f" · история {days:.0f} д" if days >= 1 else f" · история {days * 24:.0f} ч"
        if s30.trades:
            trader_lines.append(f"{mark} <b>{name}</b> · {s30.trades} сд · прибыльных {s30.win_rate:.0%} · "
                                f"копия {s30.copyability:+.0%}/сд · держит ~{fmt.ago(s30.median_hold_s)}{span} — {why}")
        else:
            trader_lines.append(f"{mark} <b>{name}</b> · сделок Pump.fun не найдено{span} — {why}")
    await e._load_traders()
    parts = [f"📥 <b>Импорт: {len(wallets)}</b> · девов {len(dev_lines)}, трейдеров {len(trader_lines)} "
             f"(стоит копировать {good})"]
    if dev_lines:
        parts.append("\n<b>Девы</b> — алерт в момент запуска монеты: вход и цель по почерку дева, "
                     "выход при его первой продаже\n" + "\n".join(dev_lines))
    if trader_lines:
        parts.append("\n<b>Трейдеры</b> — алерт и бумажная копия при покупке от "
                     f"{float(cfg.get('copy_min_buy_sol', 0.1)):g} SOL, выход при их продаже\n" + "\n".join(trader_lines)
                     + "\n<i>Копия — без нашей задержки, это потолок; честную цифру покажет бумага.</i>")
    text = "\n".join(parts)
    for i in range(0, len(text), 3800):
        await e.db.enqueue("text", {"topic": "copy", "text": text[i:i + 3800]}, priority=4)
