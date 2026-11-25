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
    pdh = ((await db.kv_get("health:ingest")) or {}).get("pumpdata")
    if pdh:
        n = await db.fetchval("SELECT count(*) FROM market_trades WHERE ts > now() - interval '1 hour'")
        out.append(f"   pumpdata (весь рынок, сделки от {pdh.get('min_sol')} SOL): соединение "
                   f"{'есть' if pdh.get('connected') else 'НЕТ'}, за час записано {n:,}, очередь {pdh.get('queue', 0)}, "
                   f"потеряно {pdh.get('dropped', 0)}, переподключений {pdh.get('reconnects', 0)}"
                   + (f"; ошибка: {pdh.get('last_error')}" if pdh.get("last_error") else ""))
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
    total = nop = small = nop_nr = 0
    for w in wallets:
        trades = await CM.wallet_trades(db, w, since - 120, now)
        if not any(d["is_buy"] for d in trades):
            continue
        p = await CM.prepare(db, w, trades, since, now - 35 * 60, limit=500)     # выход ещё мог не наступить
        for _e, c in CM.run(p, SCHEMES["A"], entry_delay_s=delay):
            total += 1
            nop += c.res is None and c.why != CM.SMALL_POOL
            small += c.why == CM.SMALL_POOL
        # 27.11 (владелец: нужны ли резервы при pumpdata?): те же копии без цены из резервов
        for _e, c in CM.run(CM.without_reserves(p), SCHEMES["A"], entry_delay_s=delay):
            nop_nr += c.res is None and c.why != CM.SMALL_POOL
    share = nop / max(1, total - small) if total else None
    ok = "✅" if share is not None and share <= 0.30 else "❌"
    return [f"4) Копии без цены за {hours:g} ч (свой список и охотник, {len(wallets)} кошельков): "
            + (f"{nop} из {total - small} — {share:.0%} {ok} (цель ≤ 30%); без резервов было бы {nop_nr} — "
               f"{nop_nr / max(1, total - small):.0%}" if share is not None else "покупок нет")], share


GAP_RULES = ("A", "B", "G")             # правила до 30 мин — модель их считает; R / F / W / тренды — дольше
# 27.11: модель охотника — копия покупки кошелька; «мой дев» (вход на запуске после покупки дева при создании — там
# снайперы того же блока, на выгрузке +21.5 п.п.) и тренды — не она, их в сверке модели не считаем
MODEL_KINDS = ("copy", "smart", "bigcopy", "swing")


def effective_rule(extra: dict[str, Any]) -> str:
    """27.11: правило, по которому тень на самом деле вышла (executor.scheme_of): у позиции «за ним» — его правило
    (best_exit: выбранное владельцем или лучшее по истории), иначе правило метода (extra.rule)."""
    be = extra.get("best_exit")
    if be in ("A", "B", "G", "R", "F", "W") and not extra.get("trial"):
        return str(be)
    return str(extra.get("rule") or "A")


async def shadow_pairs(db: Any, days: float, delay: float, limit: int = 600,
                       exit_delay: float | None = None) -> dict[str, Any]:
    """Сделки тени за кошельками и копия модели на той же покупке — по тому же правилу. Две копии: с задержкой
    delay (как оценивает охотник) и с настоящей задержкой тени (от его покупки до нашего входа)."""
    import json as _json
    rows = [dict(r) for r in await db.fetch(
        """SELECT p.id, p.mint, p.trader, extract(epoch FROM p.opened_at) AS ts, extract(epoch FROM p.closed_at) AS cts,
                  p.pnl_pct, p.entry_mc, p.exit_mc, p.exit_reason, p.venue, p.kind, p.extra
           FROM real_positions p WHERE p.mode = 'shadow' AND p.trader IS NOT NULL AND p.closed_at IS NOT NULL
             AND p.pnl_pct IS NOT NULL AND p.opened_at > now() - make_interval(days => $1)
             AND COALESCE(p.venue, '') <> 'jupiter' AND COALESCE(p.exit_reason, '') NOT IN ('reset', 'manual')
           ORDER BY p.id DESC LIMIT $2""", days, limit)]
    excluded: dict[str, int] = {}
    keep = []
    for r in rows:
        ex = r["extra"] if isinstance(r["extra"], dict) else _json.loads(r["extra"] or "{}")
        rule = effective_rule(ex)
        if rule not in GAP_RULES:
            excluded[rule] = excluded.get(rule, 0) + 1
            continue
        r["rule"] = rule
        keep.append(r)
    out: dict[str, Any] = {"total": len(rows), "excluded": excluded, "pairs": [], "no_buy": 0, "no_model": 0}
    if not keep:
        return out
    buys = await db.fetch(
        """SELECT p.id, t.slot, extract(epoch FROM t.ts) AS ts, t.sol_amount FROM unnest($1::bigint[], $2::text[],
               $3::text[], $4::float8[]) AS p(id, mint, trader, ts)
           JOIN LATERAL (SELECT slot, ts, sol_amount FROM trades WHERE mint = p.mint AND trader = p.trader AND is_buy
                         AND ts BETWEEN to_timestamp(p.ts - 60) AND to_timestamp(p.ts + 1)
                         ORDER BY ts DESC LIMIT 1) t ON true""",
        [r["id"] for r in keep], [r["mint"] for r in keep], [r["trader"] for r in keep], [float(r["ts"]) for r in keep])
    his = {b["id"]: b for b in buys}
    win: dict[str, tuple[float, float]] = {}
    for r in keep:
        if r["id"] in his:
            a, b = float(his[r["id"]]["ts"]) - 5, float(his[r["id"]]["ts"]) + 1900
            w = win.get(r["mint"])
            win[r["mint"]] = (min(a, w[0]), max(b, w[1])) if w else (a, b)
    ticks = await CM.load_ticks(db, win)
    cover = await CM.load_cover(db, win)
    meta = {m["mint"]: (m["created"], m["deployer"]) for m in await db.fetch(
        "SELECT mint, extract(epoch FROM created_at) AS created, deployer FROM tokens WHERE mint = ANY($1::text[])",
        list(win))}
    for r in keep:
        b = his.get(r["id"])
        if b is None:
            out["no_buy"] += 1
            continue
        e = CM.Entry(r["mint"], float(b["ts"]), b["slot"], float(b["sol_amount"] or 0))
        created, dev = meta.get(r["mint"], (None, None))
        tk = [t for t in ticks.get(r["mint"], []) if e.ts - 5 <= t.ts <= e.ts + 1900]
        lag = max(0.0, float(r["ts"]) - e.ts)

        def model(d: float, xd: float | None = None, e=e, tk=tk, dev=dev, created=created, r=r) -> CM.Copy:
            return CM.copy_trade(e, tk, r["trader"], dev, float(created) if created is not None else None,
                                 SCHEMES[r["rule"]], covered=cover.get(r["mint"]), entry_delay_s=d, exit_delay_s=xd)
        m, ml = model(delay, exit_delay), model(lag, exit_delay)
        if m.res is None:
            out["no_model"] += 1
            continue
        out["pairs"].append({"redo": model, "e_ts": e.ts, "s_close": float(r["cts"]),
                             "rule": r["rule"], "venue": r["venue"] or "pump", "kind": r["kind"], "lag": lag, "m": m,
                             "ml": ml if ml.res is not None else None, "s_res": float(r["pnl_pct"]),
                             "s_entry": float(r["entry_mc"] or 0), "s_exit": float(r["exit_mc"] or 0),
                             "s_reason": r["exit_reason"] or "?", "s_hold": float(r["cts"]) - float(r["ts"])})
    return out


async def model_gap(db: Any, days: float = 3.0, delay: float = 1.36) -> list[str]:
    """27.11 (владелец: модель против тени +15.4 п.п. — оценки охотника завышены): разница по частям при одном и том
    же правиле. Части — в логарифмических пунктах (≈ п.п. при небольших разницах), в сумме дают разницу итогов:
    цена входа ln(вход тени / вход модели), цена выхода ln(выход модели / выход тени), издержки и доли продаж — остаток."""
    import math
    d = await shadow_pairs(db, days, delay)
    every = d["pairs"]
    ps = [p for p in every if p["kind"] in MODEL_KINDS]
    exc = ", ".join(f"{k} {v}" for k, v in sorted(d["excluded"].items())) or "нет"
    head = [f"6) Модель против тени по частям ({days:g} дн, одно правило на паре, копии кошельков — "
            f"{'/'.join(MODEL_KINDS)}): сделок тени {d['total']}, сверено {len(ps)}; не сверялись — правило дольше 30 "
            f"мин ({exc}), нет его покупки в базе {d['no_buy']}, у модели нет цены {d['no_model']}, другие методы "
            f"{len(every) - len(ps)}"]
    if not ps:
        return head
    def mean(xs: list[float]) -> float:
        return statistics.fmean(xs) if xs else 0.0
    diff = [p["m"].res - p["s_res"] for p in ps]
    full = [p for p in ps if p["s_entry"] > 0 and p["s_exit"] > 0 and p["m"].entry_mc > 0 and p["m"].exit_mc > 0
            and 1 + p["s_res"] > 0 and 1 + p["m"].res > 0]
    ent = [math.log(p["s_entry"] / p["m"].entry_mc) for p in full]
    ext = [math.log(p["m"].exit_mc / p["s_exit"]) for p in full]
    tot = [math.log((1 + p["m"].res) / (1 + p["s_res"])) for p in full]
    cst = [t - a - b for t, a, b in zip(tot, ent, ext)]
    lagged = [p["ml"].res - p["s_res"] for p in ps if p["ml"] is not None]
    lags = [p["lag"] for p in ps]
    lines = head + [
        f"   разница итогов: в среднем {mean(diff) * 100:+.1f} п.п., медиана {_med(diff) * 100:+.1f}",
        f"   по частям ({len(full)} сделок с ценами входа и выхода, в среднем): вход {mean(ent) * 100:+.1f} · выход "
        f"{mean(ext) * 100:+.1f} · издержки и частичные продажи {mean(cst) * 100:+.1f} (сумма {mean(tot) * 100:+.1f})",
        f"   задержка тени от его покупки: медиана {_med(lags):.1f} с (модель — {delay:g} с); модель с задержкой тени: "
        + (f"разница {mean(lagged) * 100:+.1f} п.п. на {len(lagged)}" if lagged else "—"),
        f"   удержание: модель медиана {_med([p['m'].hold_s for p in ps]) / 60:.1f} мин, тень "
        f"{_med([p['s_hold'] for p in ps]) / 60:.1f} мин"]
    # 27.11 (владелец: тейк → тейк +7.1 п.п. — тень продаёт позже?): на выходах по тейку у обеих — через сколько после
    # решения модели тень закрыла сделку; та же модель с этой задержкой продажи — на тейках и на всех копиях
    takes = [p for p in ps if p["m"].reason == "take_profit" and p["s_reason"] == "take_profit"]
    if takes:
        lags_x = [p["s_close"] - (p["e_ts"] + CM.ENTRY_DELAY_S + p["m"].hold_s) for p in takes]
        xd = max(0.0, round(_med(lags_x), 1))
        before = mean([p["m"].res - p["s_res"] for p in takes])
        after_t = [p["redo"](delay, xd) for p in takes]
        after = mean([c.res - p["s_res"] for c, p in zip(after_t, takes) if c.res is not None])
        after_all = [(p["redo"](delay, xd), p) for p in ps]
        after_all_d = mean([c.res - p["s_res"] for c, p in after_all if c.res is not None])
        lines.append(f"   тейк → тейк ({len(takes)}): тень закрывает через {xd:g} с после решения модели (медиана; модель "
                     f"продаёт через {CM.EXIT_DELAY_S:g} с); модель с продажей через {xd:g} с: на тейках "
                     f"{before * 100:+.1f} → {after * 100:+.1f} п.п., на всех копиях {mean(diff) * 100:+.1f} → "
                     f"{after_all_d * 100:+.1f} — в настройку hunter_exit_delay_s: {xd:g}")
        # пробел в ценах у модели: она не видела, когда цена прошла тейк, и «продала» по первой следующей сделке выше
        late = [p for p in takes if p["m"].hold_s - p["s_hold"] > 20]   # тень смотрит цену раз в 4 с
        rest = [p["m"].res - p["s_res"] for p in takes if p not in late]
        lines.append(f"   из них модель держала дольше тени на 20+ с (пробел в ценах — продала позже и выше): {len(late)}"
                     + (f", на них {mean([p['m'].res - p['s_res'] for p in late]) * 100:+.1f} п.п." if late else "")
                     + (f"; остальные тейки {mean(rest) * 100:+.1f} п.п." if rest else ""))
    pairs: dict[tuple[str, str], list[float]] = {}
    for p, x in zip(ps, diff):
        pairs.setdefault((p["m"].reason or "?", p["s_reason"]), []).append(x)
    lines.append("   причина выхода (модель → тень): " + "; ".join(
        f"{a} → {b}: {len(v)}, {mean(v) * 100:+.1f}" for (a, b), v in sorted(pairs.items(), key=lambda kv: -len(kv[1]))[:8]))
    for name, key in (("метод (все, вкл. не модель)", "kind"), ("правило", "rule"), ("биржа", "venue")):
        groups: dict[str, list[float]] = {}
        for p in (every if key == "kind" else ps):
            groups.setdefault(str(p[key]), []).append(p["m"].res - p["s_res"])
        lines.append(f"   {name}: " + "; ".join(f"{k} {len(v)}, {mean(v) * 100:+.1f}" for k, v in sorted(groups.items())))
    return lines


async def model_vs_shadow(db: Any, days: float, delay: float) -> tuple[list[str], float | None]:
    """Проверка 4: модель против тени на одних сделках. 27.11: правило — то, по которому тень вышла на самом деле
    (effective_rule), и только правила до 30 мин (A/B/G); разложение по частям — model_gap (datacheck --gap)."""
    d = await shadow_pairs(db, days, delay, limit=400)
    ps = [p for p in d["pairs"] if p["kind"] in MODEL_KINDS]
    if not d["total"]:
        return ["5) Модель против тени: закрытых сделок тени за кошельками нет"], None
    if not ps:
        return [f"5) Модель против тени: {d['total']} сделок тени, сверить нечего (правило дольше 30 мин, нет его "
                "покупки в базе или у модели нет цены)"], None
    diffs = [p["m"].res - p["s_res"] for p in ps]
    mean = statistics.fmean(diffs)
    ok = "✅" if abs(mean) <= 0.05 else "❌"
    return [f"5) Модель против тени на копиях кошельков ({days:g} дн, то же правило): {len(ps)} из {d['total']}, "
            f"разница в среднем {mean * 100:+.1f} п.п. {ok} (цель ≤ 5), медиана |разницы| "
            f"{_med([abs(x) for x in diffs]) * 100:.1f} п.п. — по частям: datacheck --gap"], mean


PP_SOL_PER_MSG = 0.01 / 10_000          # PumpPortal: 0.01 SOL за 10 тыс. сообщений сделок (документация 27.11)


async def pp_cost_text(db: Any, days: int = 30) -> str:
    """27.11 (владелец: «сколько PumpPortal тратил в SOL в день, когда работал»): сделки, пришедшие от PumpPortal, по
    дням (source_stats — приёмник пишет каждый час) × 0.01 SOL за 10 тыс. Создания и миграции бесплатны."""
    rows = await db.fetch(
        """SELECT (hour AT TIME ZONE 'UTC')::date AS day, sum(trades) AS trades,
                  count(*) FILTER (WHERE trades > 0) AS hours
           FROM source_stats WHERE source = 'pumpportal' AND hour > now() - make_interval(days => $1)
           GROUP BY 1 ORDER BY 1""", days)
    if not rows:
        return "PumpPortal: в source_stats нет записей."
    price = float(((await db.kv_get("sol_usd", {})) or {}).get("price") or 0) if hasattr(db, "kv_get") else 0.0
    lines = ["PumpPortal по дням (UTC): сделок пришло · часов со сделками · стоимость (0.01 SOL за 10 тыс.)"]
    for r in rows:
        sol = int(r["trades"]) * PP_SOL_PER_MSG
        lines.append(f"  {r['day']:%d.%m}: {int(r['trades']):>10,} · {int(r['hours']):>2} ч · {sol:.3f} SOL"
                     + (f" (${sol * price:,.0f})" if price else ""))
    # 27.11: сырые сообщения по видам подписки (приёмник считает каждое пришедшее, source_stats pp_raw_*)
    raw = await db.fetch(
        """SELECT (hour AT TIME ZONE 'UTC')::date AS day, substr(source, 8) AS kind, sum(trades) AS n
           FROM source_stats WHERE source LIKE 'pp\\_raw\\_%' AND hour > now() - make_interval(days => $1)
           GROUP BY 1, 2 ORDER BY 1""", days)
    if raw:
        by: dict[Any, dict[str, int]] = {}
        for r in raw:
            by.setdefault(r["day"], {})[r["kind"]] = int(r["n"])
        lines.append("Все сообщения PumpPortal (с 27.11): сделки монет · сделки кошельков · служебные · создания+миграции "
                     "→ платные (сделки)")
        for d, k in sorted(by.items()):
            paid = k.get("coin", 0) + k.get("wallet", 0)
            lines.append(f"  {d:%d.%m}: {k.get('coin', 0):,} · {k.get('wallet', 0):,} · {k.get('service', 0):,} · "
                         f"{k.get('create', 0) + k.get('migrate', 0):,} → {paid * PP_SOL_PER_MSG:.3f} SOL"
                         + (f" (${paid * PP_SOL_PER_MSG * price:,.0f})" if price else "")
                         + (f"; со служебными {(paid + k.get('service', 0)) * PP_SOL_PER_MSG:.3f}" if k.get("service")
                            else ""))
    full = [r for r in rows if int(r["hours"]) >= 20]
    if full:
        avg = sum(int(r["trades"]) for r in full) / len(full)
        lines.append(f"В полные дни (сделки шли 20+ ч, {len(full)} дн): в среднем {avg:,.0f} сделок ≈ "
                     f"{avg * PP_SOL_PER_MSG:.3f} SOL в день" + (f" (${avg * PP_SOL_PER_MSG * price:,.0f})" if price else ""))
    else:
        lines.append("Полных дней (сделки шли 20+ ч) нет — PumpPortal отказывал большую часть времени.")
    return "\n".join(lines)


async def run(db: Any, hours: float = 24.0, days: float = 3.0, delay: float = 1.36) -> str:
    lines = [f"Проверка данных · {datetime.now(timezone.utc):%d.%m %H:%M} UTC"]
    lines += await reserves_part(db, hours)
    lines += await price_part(db, hours)
    lines += (await noprice_part(db, hours, delay))[0]
    lines += (await model_vs_shadow(db, days, delay))[0]
    return "\n".join(lines)
