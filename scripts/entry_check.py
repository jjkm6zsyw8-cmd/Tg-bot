"""Точная проверка: что дал бы каждый сигнал копий и smart при входе через 0.4…10 с после покупки трейдера.

Без минутных свечей и без допущений о цене внутри минуты: путь цены — сделки монеты из своей базы (таблица trades,
слоты блоков ≈ 0.4 с), вход — по последней сделке к моменту «покупка трейдера + задержка», выход — по сделкам после
входа: тейк, стоп, нет роста за 5 минут, продажа трейдера, 30 минут максимум. Комиссии pump.fun 1.25% на сторону
и приоритет 0.0005 SOL на транзакцию (позиция 0.15 SOL).

Сверка с фактом: для копий своих кошельков в журнале copy_log есть цена трейдера и реальная цена входа бумаги
(Jupiter, через 5 с) — расчёт «через 5 с» должен с ней сходиться (строка «сверка»).

Запуск на сервере (одна строка, без пересборки):
    cd /opt/tg-bot && docker compose exec -T analytics python - < scripts/entry_check.py
Переменные: EC_DAYS — за сколько дней (по умолчанию 7), EC_EXIT_DELAY — через сколько секунд после сигнала
выхода ложится продажа (по умолчанию 1).
"""
from __future__ import annotations

import asyncio
import collections
import math
import os
import statistics as st
from typing import Any

FEE = 0.0125
PRIO = 0.0005 / 0.15                 # приоритет на одну транзакцию, доля позиции 0.15 SOL
SLOT_S = 0.4
EXIT_DELAY_S = float(os.environ.get("EC_EXIT_DELAY", 1.0))   # бот видит сделку и продаёт не мгновенно
DELAYS = (0.4, 1.0, 2.0, 3.0, 5.0, 10.0)
HORIZON_S = 1800
# перебор выходов: тейк × стоп × таймер (выход через 10 мин, если не было +5%) — на сигналах с видимыми первыми секундами
GRID_TP = (0.05, 0.07, 0.10, 0.15, 0.20, 0.25, 0.40, 0.60)
GRID_STOP = (0.05, 0.08, 0.10, 0.15, 0.20, 0.30, None)
GRID_TIME = (None, 600)
GRID_DELAYS = (0.4, 1.0, 2.0, 3.0)
# реалистичный быстрый бот: поток транзакций (gRPC) + быстрая отправка — покупка ложится через 1–3 блока после
# покупки трейдера, продажа — через 1–2 блока после сделки, пересёкшей стоп/тейк
REAL_ENTRY = (0.4, 0.8, 1.2)
REAL_EXIT = (0.4, 0.8)
SCHEMES = {
    # G — всё на +25% чистыми, стоп −8%, 5 минут на +10%, выход за трейдером
    "G": dict(tp=0.25, share=1.0, stop=0.08, time_s=300, gain=0.10, trail=None),
    # M — сильные копии: 85% на +25%, 15% — раннер с трейлингом 30% (за трейдером не продаётся)
    "M": dict(tp=0.25, share=0.85, stop=0.08, time_s=300, gain=0.10, trail=0.30),
}


def net(ratio: float) -> float:
    """Чистый результат при покупке и продаже с отношением цен ratio (комиссии и приоритет туда-обратно)."""
    return ratio * (1 - FEE) ** 2 - 1 - 2 * PRIO


def entry_at(path: list[tuple[int, float, float, str, bool]], s0: int, delay: float) -> tuple[int, float] | None:
    """Индекс и MC входа: последняя сделка к слоту s0 + delay/0.4 (наша покупка ложится после неё)."""
    target = s0 + max(1, math.ceil(delay / SLOT_S))
    idx = None
    for i, (slot, _ts, _mc, _tr, _b) in enumerate(path):
        if slot <= target:
            idx = i
        else:
            break
    return (idx, path[idx][2]) if idx is not None else None


def fill_after(path: list[tuple[int, float, float, str, bool]], j: int, delay: float | None = None) -> float:
    """Цена продажи: последняя сделка к слоту «сигнал выхода + задержка» (бот продаёт не мгновенно)."""
    target = path[j][0] + math.ceil((EXIT_DELAY_S if delay is None else delay) / SLOT_S)
    mc = path[j][2]
    for slot, _ts, m, _tr, _b in path[j + 1:]:
        if slot > target:
            break
        mc = m
    return mc


def simulate(path: list[tuple[int, float, float, str, bool]], i0: int, mc0: float, trader: str,
             sc: dict[str, Any], exit_delay: float | None = None) -> tuple[float, str]:
    """Выход по сделкам после входа. path: (слот, время, MC, трейдер, покупка)."""
    t0 = path[i0][1]
    rem, got, peak, tp_done, max_net = 1.0, 0.0, mc0, False, -1.0
    tp_level = mc0 * (1 + sc["tp"] + 2 * PRIO) / (1 - FEE) ** 2
    last_mc = mc0
    for j in range(i0 + 1, len(path)):
        slot, ts, mc, tr, is_buy = path[j]
        if ts - t0 > HORIZON_S:
            break
        if sc["time_s"] and ts - t0 >= sc["time_s"] and max_net < sc["gain"] and not tp_done:
            got += rem * last_mc
            return net(got / mc0), "нет роста"
        last_mc = mc
        if tr == trader and not is_buy and not tp_done:
            got += rem * fill_after(path, j, exit_delay)
            return net(got / mc0), "трейдер продал"
        if sc["stop"] is not None and mc <= mc0 * (1 - sc["stop"]):
            got += rem * fill_after(path, j, exit_delay)
            return net(got / mc0), "стоп"
        if tp_done and sc["trail"] and mc <= peak * (1 - sc["trail"]):
            got += rem * fill_after(path, j, exit_delay)
            return net(got / mc0), "трейлинг"
        if not tp_done and mc >= tp_level:
            sell = rem if sc["share"] >= 1 else sc["share"]
            got += sell * fill_after(path, j, exit_delay)
            rem -= sell
            tp_done = True
            if rem <= 1e-9:
                return net(got / mc0), "тейк"
        peak = max(peak, mc)
        max_net = max(max_net, net(mc / mc0))
    got += rem * last_mc
    return net(got / mc0), "30 минут" if rem else "тейк"


async def main(dsn: str | None = None) -> None:
    import asyncpg
    conn = await asyncpg.connect(dsn or os.environ["DATABASE_URL"])
    days = float(os.environ.get("EC_DAYS", 7))
    sigs = await conn.fetch(
        """SELECT DISTINCT ON (p.mint, date_trunc('minute', p.opened_at))
                  p.mint, p.opened_at, p.level, p.source_wallet, a.mode, a.trader AS alert_trader
           FROM paper_positions p JOIN alerts a ON a.id = p.alert_id
           WHERE a.mode IN ('copy', 'smart') AND p.opened_at > now() - make_interval(days => $1)
           ORDER BY p.mint, date_trunc('minute', p.opened_at), p.opened_at""", int(math.ceil(days)))
    res: dict[str, dict[float, list[float]]] = collections.defaultdict(lambda: collections.defaultdict(list))
    resm: dict[float, list[float]] = collections.defaultdict(list)
    slip: dict[float, list[float]] = collections.defaultdict(list)
    by_day: dict[str, dict[float, list[float]]] = collections.defaultdict(lambda: collections.defaultdict(list))
    wallets: dict[str, list[float]] = collections.defaultdict(list)
    seen_early, used, no_trigger, no_path = 0, 0, 0, 0
    grid_paths: list[tuple[list[tuple[int, float, float, str, bool]], int, str, bool]] = []
    lags: list[float] = []
    for s in sigs:
        who = s["source_wallet"] or s["alert_trader"]
        trig = await conn.fetchrow(
            """SELECT slot, ts, mc_sol FROM trades WHERE mint=$1 AND trader=$2 AND is_buy AND slot IS NOT NULL
                 AND ts <= $3::timestamptz AND ts > $3::timestamptz - interval '2 minutes'
               ORDER BY ts DESC, slot DESC LIMIT 1""",
            s["mint"], who, s["opened_at"])
        if not trig or not who:
            no_trigger += 1
            continue
        lags.append((s["opened_at"] - trig["ts"]).total_seconds())
        rows = await conn.fetch(
            """SELECT slot, extract(epoch FROM ts) t, mc_sol, trader, is_buy FROM trades
               WHERE mint=$1 AND slot >= $2 AND ts <= $3::timestamptz + interval '31 minutes' AND mc_sol > 0
               ORDER BY slot, id""", s["mint"], trig["slot"], trig["ts"])
        path = [(int(r["slot"]), float(r["t"]), float(r["mc_sol"]), r["trader"], r["is_buy"]) for r in rows]
        if len(path) < 3:
            no_path += 1
            continue
        used += 1
        early = any(trig["slot"] < p[0] <= trig["slot"] + 8 for p in path)   # видели сделки первых ~3 с
        seen_early += early
        if not early:
            # подписка на монету могла включиться позже покупки трейдера — первых секунд в базе нет, и быстрый
            # вход посчитался бы по слишком хорошей цене; такие сигналы — только в отдельной строке
            for d in DELAYS:
                e = entry_at(path, int(trig["slot"]), d)
                if e:
                    res["(без первых секунд)"][d].append(simulate(path, e[0], e[1], who, SCHEMES["G"])[0])
            continue
        kind = f"{'копия' if s['mode'] == 'copy' else 'smart'} {'сильная' if s['level'] == 'strong' else 'одна'}"
        day = s["opened_at"].strftime("%d.%m")
        m_sig = (s["mode"] == "copy" and s["level"] == "strong") or (s["mode"] == "smart" and s["level"] != "strong")
        grid_paths.append((path, int(trig["slot"]), who, m_sig))
        for d in DELAYS:
            e = entry_at(path, int(trig["slot"]), d)
            if not e:
                continue
            i0, mc0 = e
            r, _why = simulate(path, i0, mc0, who, SCHEMES["G"])
            res[kind][d].append(r)
            slip[d].append(mc0 / float(trig["mc_sol"]) - 1)
            by_day[day][d].append(r)
            if d == 1.0:
                wallets[who].append(r)
            if s["level"] == "strong" or s["mode"] == "smart":
                rm, _ = simulate(path, i0, mc0, who, SCHEMES["M"] if s["mode"] == "copy" else SCHEMES["G"])
                if (s["mode"] == "copy" and s["level"] == "strong") or (s["mode"] == "smart" and s["level"] != "strong"):
                    resm[d].append(rm)
    # сверка с фактом: реальный вход бумаги через 5 с (Jupiter) против расчёта по своим сделкам
    seen = await conn.fetchrow(
        """SELECT count(*) n,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - ts)) med,
                  percentile_cont(0.9) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - ts)) p90
           FROM trades WHERE source = 'watch' AND ts > now() - interval '2 days'
             AND trader IN (SELECT address FROM traders WHERE active OR manual
                            UNION SELECT address FROM watchlist WHERE kind = 'trader')""")
    fact = await conn.fetch(
        """SELECT delta FROM copy_log WHERE delta IS NOT NULL AND ts > now() - make_interval(days => $1)""",
        int(math.ceil(days)))
    await conn.close()

    def cell(v: list[float]) -> str:
        return f"{st.mean(v):+5.1%} ({sum(x > 0 for x in v) / len(v):.0%} в плюсе, {len(v)})" if v else "—"
    print(f"Продажа через {EXIT_DELAY_S:g} с после сигнала выхода (EC_EXIT_DELAY)")
    print(f"Точная проверка входа за {days:g} дн. · сигналов {len(sigs)}, посчитано {used} "
          f"(нет сделки трейдера {no_trigger}, нет пути цены {no_path}) · с видимыми сделками первых 3 с: {seen_early} "
          f"— таблица по ним")
    print("\nСхема G, % на сделку при входе через N секунд после покупки трейдера:")
    print("  " + " | ".join(f"{d:g} с" for d in DELAYS))
    for kind in sorted(res):
        print(f"  {kind:<14} " + " | ".join(cell(res[kind][d]) for d in DELAYS))
    print("\nНасколько дороже трейдера входим (медиана):  "
          + " · ".join(f"{d:g} с: {st.median(slip[d]):+.0%}" for d in DELAYS if slip[d]))
    if fact:
        print(f"Сверка: реальный вход бумаги через 5 с (copy_log, Jupiter) — медиана {st.median(r['delta'] for r in fact):+.0%} "
              f"к цене трейдера ({len(fact)} копий); расчёт по сделкам через 5 с — {st.median(slip[5.0]):+.0%}" if slip[5.0] else "")
    print("\nM (сильные копии + одиночный smart), % на сделку: "
          + " · ".join(f"{d:g} с: {cell(resm[d])}" for d in DELAYS))
    print("\nПо дням, все сигналы G: через 1 с | через 5 с")
    for day in sorted(by_day):
        print(f"  {day}: {cell(by_day[day][1.0])} | {cell(by_day[day][5.0])}")
    if grid_paths:
        print(f"\nПеребор выходов ({len(grid_paths)} сигналов, $ при $10 на сделку, тейк/стоп/таймер 10 мин):")
        for d in GRID_DELAYS:
            entries = [(p, entry_at(p, s0, d), who) for p, s0, who, _m in grid_paths]
            entries = [(p, e, who) for p, e, who in entries if e]
            rows_g = []
            for tp in GRID_TP:
                for stop in GRID_STOP:
                    for tm in GRID_TIME:
                        sc = dict(tp=tp, share=1.0, stop=stop, time_s=tm, gain=0.05, trail=None)
                        rs = [simulate(p, e[0], e[1], who, sc)[0] for p, e, who in entries]
                        rows_g.append((sum(rs) * 10, tp, stop, tm, sum(r > 0 for r in rs) / len(rs) if rs else 0))
            rows_g.sort(key=lambda r: -r[0])
            name = lambda r: (f"+{r[1]:.0%}/{('−%d%%' % round(r[2] * 100)) if r[2] else 'без стопа'}"
                              f"{'/10м' if r[3] else ''}")
            pos_n = sum(1 for r in rows_g if r[0] > 0)
            pick = {(0.05, None, 600), (0.25, 0.08, None), (0.60, 0.20, None)}
            print(f"  через {d:g} с: в плюсе {pos_n} из {len(rows_g)} · лучшие: "
                  + "; ".join(f"{name(r)} {r[0]:+.0f}$ ({r[4]:.0%})" for r in rows_g[:4])
                  + " · для сравнения: " + "; ".join(f"{name(r)} {r[0]:+.0f}$" for r in rows_g
                                                      if (r[1], r[2], r[3]) in pick))
    if grid_paths:
        print(f"\nРеалистичный быстрый бот ({len(grid_paths)} сигналов, $ при $10): покупка ложится через N блоков "
              f"после трейдера, продажа — через M блоков после сигнала выхода (1 блок ≈ 0.4 с)")
        for d in REAL_ENTRY:
            for xd in REAL_EXIT:
                entries = [(p, entry_at(p, s0, d), who, m) for p, s0, who, m in grid_paths]
                entries = [(p, e, who, m) for p, e, who, m in entries if e]
                rows_r = []
                for tp in GRID_TP:
                    for stop in GRID_STOP:
                        for tm in GRID_TIME:
                            sc = dict(tp=tp, share=1.0, stop=stop, time_s=tm, gain=0.05, trail=None)
                            rs = [simulate(p, e[0], e[1], who, sc, xd)[0] for p, e, who, _m in entries]
                            rows_r.append((sum(rs) * 10, tp, stop, tm))
                rows_r.sort(key=lambda r: -r[0])
                g = [simulate(p, e[0], e[1], who, SCHEMES["G"], xd)[0] for p, e, who, _m in entries]
                mm = [simulate(p, e[0], e[1], who, SCHEMES["M"], xd)[0] for p, e, who, m in entries if m]
                name = lambda r: (f"+{r[1]:.0%}/{('−%d%%' % round(r[2] * 100)) if r[2] else 'без стопа'}"
                                  f"{'/10м' if r[3] else ''}")
                print(f"  покупка +{round(d / SLOT_S)} бл., продажа +{round(xd / SLOT_S)} бл.: в плюсе "
                      f"{sum(1 for r in rows_r if r[0] > 0)} из {len(rows_r)} · лучшие: "
                      + "; ".join(f"{name(r)} {r[0]:+.0f}$" for r in rows_r[:3])
                      + f" · схема G {sum(g) * 10:+.0f}$ · M {cell(mm)}")
    print("\nСкорость нашего бота сейчас:")
    if seen and seen["n"]:
        print(f"  видит покупку отслеживаемого кошелька через ≈{seen['med']:.1f} с после блока (медиана, точность "
              f"±0.5 с; 90% — до {seen['p90']:.1f} с), {seen['n']} сделок за 2 дня")
    if lags:
        print(f"  бумага входит через ≈{st.median(lags):.1f} с после блока трейдера (медиана по {len(lags)} сигналам)")
    ranked = sorted(((st.mean(v), w, len(v)) for w, v in wallets.items() if len(v) >= 3), reverse=True)
    if ranked:
        print("\nКошельки при входе через 1 с (3+ сигнала), лучшие и худшие:")
        show = ranked if len(ranked) <= 10 else ranked[:5] + ranked[-5:]
        for m, w, n in show:
            print(f"  {w[:6]}…{w[-4:]}: {m:+.1%} на сделку, {n} сигн.")


if __name__ == "__main__":
    asyncio.run(main())
