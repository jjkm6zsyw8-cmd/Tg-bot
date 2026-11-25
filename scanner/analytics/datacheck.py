"""26.11 (сводное ТЗ, проверки): одна команда для сервера — python -m scanner datacheck [--hours 24] [--days 3].

1. Резервы (B1): монет и изменений по часам, отказы узла (429), ошибки подписки и RPC, лимиты.
2. Резервы против логов (B2): цена из резервов и из сделок (не PumpPortal на PumpSwap) в одних и тех же блоках.
3. Резервы против тени (B2): цена входа тени и ближайшая цена резервов (±3 с).
4. Копии без цены за последние часы (проверка 3): у отслеживаемых кошельков и охотника — цель ≤ 30%.
5. Модель против тени на одних сделках (проверка 4): копия «как бот» (задержка, правило позиции) и итог тени —
   разница в среднем, цель ≤ 5 п.п.
"""
from __future__ import annotations

import statistics
import time
from datetime import datetime, timezone
from typing import Any

from . import copymodel as CM
from .paper import SCHEMES


def _med(xs: list[float]) -> float | None:
    return statistics.median(xs) if xs else None


async def reserves_part(db: Any, hours: float) -> list[str]:
    rows = await db.fetch(
        """SELECT date_trunc('hour', ts) AS h, count(DISTINCT mint) AS coins, count(*) AS ticks FROM reserve_ticks
           WHERE ts > now() - make_interval(secs => $1) GROUP BY 1 ORDER BY 1 DESC LIMIT 6""", hours * 3600)
    out = ["1) Резервы по часам (UTC): " + (", ".join(f"{r['h']:%H}ч — {r['coins']} мон. / {r['ticks']} изм."
                                                       for r in rows) or "изменений нет")]
    h = ((await db.kv_get("health:ingest")) or {}).get("reserves") or {}
    if h:
        out.append(f"   соединение {'есть' if h.get('connected') else 'НЕТ'}, отказов узла (429) {h.get('refused', 0)}, "
                   f"ошибок подписки {h.get('sub_errors', 0)}, ошибок RPC {h.get('rpc_error', 0)}, добавлено монет "
                   f"{h.get('added', 0)}, без счёта {h.get('no_account', 0)}, лимит монеты {h.get('capped_coins', 0)}, "
                   f"за сутки {h.get('mb_today', 0)} МБ{' — ЛИМИТ' if h.get('capped') else ''}"
                   + (", ПУБЛИЧНЫЙ УЗЕЛ" if h.get("url_public") else "")
                   + (f"; последняя ошибка: {h.get('last_error')}" if h.get("last_error") else ""))
    return out


async def price_part(db: Any, hours: float) -> list[str]:
    rows = await db.fetch(
        """SELECT r.mc_sol AS rm, t.mc_sol AS tm FROM reserve_ticks r
           JOIN trades t ON t.mint = r.mint AND t.slot = r.slot AND t.mc_sol > 0
                        AND NOT (t.source = 'pumpportal' AND t.pool = 'pump-amm')
           WHERE r.ts > now() - make_interval(secs => $1) AND r.slot IS NOT NULL LIMIT 20000""", hours * 3600)
    d = [abs(float(r["rm"]) / float(r["tm"]) - 1) for r in rows if r["tm"]]
    out = [f"2) Резервы против логов (те же блоки): {len(d)} пар, медиана расхождения "
           + (f"{_med(d):.2%}, в пределах 2%: {sum(1 for x in d if x <= 0.02) / len(d):.0%}" if d else "—")]
    sh = await db.fetch(
        """SELECT p.entry_mc AS pm,
                  (SELECT r.mc_sol FROM reserve_ticks r WHERE r.mint = p.mint
                     AND r.ts BETWEEN p.opened_at - interval '3 seconds' AND p.opened_at + interval '3 seconds'
                   ORDER BY abs(extract(epoch FROM r.ts - p.opened_at)) LIMIT 1) AS rm
           FROM real_positions p WHERE p.mode = 'shadow' AND p.opened_at > now() - make_interval(secs => $1)
             AND p.entry_mc > 0 AND COALESCE(p.venue, '') <> 'jupiter'""", hours * 3600)
    d2 = [abs(float(r["pm"]) / float(r["rm"]) - 1) for r in sh if r["rm"]]
    out.append(f"3) Резервы против цены входа тени (±3 с): {len(d2)} из {len(sh)} входов, медиана расхождения "
               + (f"{_med(d2):.2%}, в пределах 5%: {sum(1 for x in d2 if x <= 0.05) / len(d2):.0%}" if d2 else "—"))
    out.append("   Логи по монетам отключать можно, только если в п.2 медиана < 1% и в п.3 большинство в пределах 5%.")
    return out


async def noprice_part(db: Any, hours: float, delay: float) -> tuple[list[str], float | None]:
    now = time.time()
    since = now - hours * 3600
    wallets = [r["address"] for r in await db.fetch(
        """SELECT address FROM traders WHERE active OR manual
           UNION SELECT address FROM hunter_wallets WHERE list <> 'archive' AND status <> 'ignored'""")]
    total = nop = small = 0
    for w in wallets:
        trades = await CM.wallet_trades(db, w, since - 120, now)
        if not any(d["is_buy"] for d in trades):
            continue
        p = await CM.prepare(db, w, trades, since, now - 35 * 60, limit=500)     # выход ещё мог не наступить
        for _e, c in CM.run(p, SCHEMES["A"], entry_delay_s=delay):
            total += 1
            nop += c.res is None and c.why != CM.SMALL_POOL
            small += c.why == CM.SMALL_POOL
    share = nop / max(1, total - small) if total else None
    ok = "✅" if share is not None and share <= 0.30 else "❌"
    return [f"4) Копии без цены за {hours:g} ч (свой список и охотник, {len(wallets)} кошельков): "
            + (f"{nop} из {total - small} — {share:.0%} {ok} (цель ≤ 30%)" if share is not None else "покупок нет")], share


async def model_vs_shadow(db: Any, days: float, delay: float) -> tuple[list[str], float | None]:
    rows = await db.fetch(
        """SELECT p.id, p.mint, p.trader, extract(epoch FROM p.opened_at) AS ts, p.pnl_pct,
                  COALESCE(p.extra->>'rule', 'A') AS rule
           FROM real_positions p WHERE p.mode = 'shadow' AND p.trader IS NOT NULL AND p.closed_at IS NOT NULL
             AND p.pnl_pct IS NOT NULL AND p.opened_at > now() - make_interval(days => $1)
             AND COALESCE(p.venue, '') <> 'jupiter' AND COALESCE(p.exit_reason, '') NOT IN ('reset', 'manual')
           ORDER BY p.id DESC LIMIT 400""", days)
    if not rows:
        return ["5) Модель против тени: закрытых сделок тени за кошельками нет"], None
    buys = await db.fetch(
        """SELECT p.id, t.slot, extract(epoch FROM t.ts) AS ts, t.sol_amount FROM unnest($1::bigint[], $2::text[],
               $3::text[], $4::float8[]) AS p(id, mint, trader, ts)
           JOIN LATERAL (SELECT slot, ts, sol_amount FROM trades WHERE mint = p.mint AND trader = p.trader AND is_buy
                         AND ts BETWEEN to_timestamp(p.ts - 60) AND to_timestamp(p.ts + 1)
                         ORDER BY ts DESC LIMIT 1) t ON true""",
        [r["id"] for r in rows], [r["mint"] for r in rows], [r["trader"] for r in rows], [float(r["ts"]) for r in rows])
    his = {b["id"]: b for b in buys}
    win = {}
    for r in rows:
        if r["id"] in his:
            a, b = float(his[r["id"]]["ts"]) - 5, float(his[r["id"]]["ts"]) + 1900
            w = win.get(r["mint"])
            win[r["mint"]] = (min(a, w[0]), max(b, w[1])) if w else (a, b)
    ticks = await CM.load_ticks(db, win)
    cover = await CM.load_cover(db, win)
    meta = {m["mint"]: (m["created"], m["deployer"]) for m in await db.fetch(
        "SELECT mint, extract(epoch FROM created_at) AS created, deployer FROM tokens WHERE mint = ANY($1::text[])",
        list(win))}
    diffs, both = [], 0
    for r in rows:
        b = his.get(r["id"])
        if b is None:
            continue
        e = CM.Entry(r["mint"], float(b["ts"]), b["slot"], float(b["sol_amount"] or 0))
        created, dev = meta.get(r["mint"], (None, None))
        sc = SCHEMES.get(r["rule"]) or SCHEMES["A"]
        c = CM.copy_trade(e, [t for t in ticks.get(r["mint"], []) if e.ts - 5 <= t.ts <= e.ts + 1900], r["trader"],
                          dev, float(created) if created is not None else None, sc, covered=cover.get(r["mint"]),
                          entry_delay_s=delay)
        if c.res is None:
            continue
        both += 1
        diffs.append(float(c.res) - float(r["pnl_pct"]))
    if not diffs:
        return [f"5) Модель против тени: {len(rows)} сделок тени, у модели цены нет ни для одной"], None
    mean = statistics.fmean(diffs)
    ok = "✅" if abs(mean) <= 0.05 else "❌"
    return [f"5) Модель против тени на одних сделках ({days:g} дн): {both} из {len(rows)}, разница в среднем "
            f"{mean * 100:+.1f} п.п. {ok} (цель ≤ 5), медиана |разницы| {_med([abs(x) for x in diffs]) * 100:.1f} п.п."], mean


async def run(db: Any, hours: float = 24.0, days: float = 3.0, delay: float = 1.36) -> str:
    lines = [f"Проверка данных · {datetime.now(timezone.utc):%d.%m %H:%M} UTC"]
    lines += await reserves_part(db, hours)
    lines += await price_part(db, hours)
    lines += (await noprice_part(db, hours, delay))[0]
    lines += (await model_vs_shadow(db, days, delay))[0]
    return "\n".join(lines)
