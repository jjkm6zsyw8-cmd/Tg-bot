"""Точная проверка: что дал бы каждый сигнал копий и smart при входе через 0.4…10 с после покупки трейдера.

Без минутных свечей и без допущений о цене внутри минуты: путь цены — сделки монеты из своей базы (таблица trades,
слоты блоков ≈ 0.4 с), вход — по последней сделке к моменту «покупка трейдера + задержка», выход — по сделкам после
входа: тейк, стоп, нет роста за 5 минут, продажа трейдера, 30 минут максимум. Комиссии pump.fun 1.25% на сторону
и приоритет 0.0005 SOL на транзакцию (позиция 0.15 SOL).

Сверка с фактом: для копий своих кошельков в журнале copy_log есть цена трейдера и реальная цена входа бумаги
(Jupiter, через 5 с) — расчёт «через 5 с» должен с ней сходиться (строка «сверка»).

Первые секунды после покупки трейдера: если подписка на монету включилась позже, этих сделок в базе нет — они
догружаются через RPC (getSignaturesForAddress по монете между покупкой трейдера и первой своей сделкой +
getTransaction) и кешируются в таблице ec_gap; таблица trades не трогается (иначе аналитика приняла бы старые
сделки за новые). Итог — в Telegram (топик отчётов) и в вывод.

Запуск на сервере (одна строка, без пересборки; в фоне — результат придёт в Telegram):
    cd /opt/tg-bot && (nohup docker compose exec -T analytics python - < scripts/entry_check.py > /tmp/ec.log 2>&1 &)
Переменные: EC_DAYS — за сколько дней (по умолчанию 30), EC_EXIT_DELAY — продажа через N с после сигнала выхода
(по умолчанию 1), EC_BACKFILL=0 — без дозагрузки, EC_RPS — запросов RPC в секунду (4), EC_RPC_MAX — не больше
стольких запросов за запуск (20000), EC_TG=0 — не слать в Telegram.
"""
from __future__ import annotations

import asyncio
import collections
import html
import json
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
# перебор выходов: тейк × стоп × таймер (выход через 10 мин, если не было +5%)
GRID_TP = (0.05, 0.07, 0.10, 0.15, 0.20, 0.25, 0.40, 0.60)
GRID_STOP = (0.05, 0.08, 0.10, 0.15, 0.20, 0.30, None)
GRID_TIME = (None, 600)
GRID = [dict(tp=tp, share=1.0, stop=stop, time_s=tm, gain=0.05, trail=None)
        for tp in GRID_TP for stop in GRID_STOP for tm in GRID_TIME]
# реалистичный быстрый бот: поток транзакций (gRPC) + быстрая отправка — покупка ложится через 1–3 блока после
# покупки трейдера, продажа — через 1–2 блока после сделки, пересёкшей стоп/тейк
REAL = [(0.4, 0.4), (0.8, 0.4), (0.8, 0.8), (1.2, 0.8)]
MAIN_REAL = (0.8, 0.4)               # «хороший бот»: покупка через 2 блока, продажа через 1
SIZES_USD = (10, 20, 50, 100, 200)   # размер ставки: своя покупка и продажа двигают цену кривой
K_CURVE = 30 * 1.073e9 / 1e9         # виртуальные резервы кривой: v_sol = sqrt(K × MC) (MC в SOL, 1 млрд токенов)
EARLY_SLOTS = 8                      # первые ~3 с после покупки трейдера — без них быстрый вход не посчитать
GAP_FULL = 60                        # дыру до первой своей сделки догружаем целиком, если в ней ≤ 60 транзакций
GAP_WINDOW_MAX = 40                  # иначе — только первые ~3 с (если там ≤ 40 транзакций)
SCHEMES = {
    # G — всё на +25% чистыми, стоп −8%, 5 минут на +10%, выход за трейдером
    "G": dict(tp=0.25, share=1.0, stop=0.08, time_s=300, gain=0.10, trail=None),
    # M — сильные копии: 85% на +25%, 15% — раннер с трейлингом 30% (за трейдером не продаётся)
    "M": dict(tp=0.25, share=0.85, stop=0.08, time_s=300, gain=0.10, trail=0.30),
}


def vsol(mc: float) -> float:
    return math.sqrt(K_CURVE * max(mc, 1e-9))


def net_sized(ratio: float, mc0: float, size_sol: float) -> float:
    """Как net, но со своим влиянием на цену: покупка на size_sol дороже на size/v_sol, продажа — дешевле;
    приоритет 0.0005 SOL на транзакцию — доля от своей ставки."""
    imp = (1 + size_sol / vsol(mc0)) * (1 + size_sol * ratio / vsol(mc0 * ratio))
    return ratio / imp * (1 - FEE) ** 2 - 1 - 2 * 0.0005 / size_sol


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
             sc: dict[str, Any], exit_delay: float | None = None, size_sol: float | None = None,
             force_slot: int | None = None) -> tuple[float, str]:
    """Выход по сделкам после входа. path: (слот, время, MC, трейдер, покупка). size_sol — со своим влиянием на цену.
    force_slot — выйти, как только путь дошёл до этого блока (правило «трейдер мелко докупил — выходим»)."""
    def nf(ratio: float) -> float:
        return net_sized(ratio, mc0, size_sol) if size_sol else net(ratio)
    t0 = path[i0][1]
    rem, got, peak, tp_done = 1.0, 0.0, mc0, False
    stop_mc = mc0 * (1 - sc["stop"]) if sc["stop"] is not None else -1.0
    tp_level = mc0 * (1 + sc["tp"] + 2 * PRIO) / (1 - FEE) ** 2
    last_mc = mc0
    for j in range(i0 + 1, len(path)):
        slot, ts, mc, tr, is_buy = path[j]
        if ts - t0 > HORIZON_S:
            break
        if sc["time_s"] and ts - t0 >= sc["time_s"] and not tp_done and net(peak / mc0) < sc["gain"]:
            got += rem * last_mc
            return nf(got / mc0), "нет роста"
        last_mc = mc
        if force_slot is not None and slot >= force_slot:
            got += rem * fill_after(path, j, exit_delay)
            return nf(got / mc0), "мелкая докупка"
        if tr == trader and not is_buy and not tp_done:
            got += rem * fill_after(path, j, exit_delay)
            return nf(got / mc0), "трейдер продал"
        if mc <= stop_mc:
            got += rem * fill_after(path, j, exit_delay)
            return nf(got / mc0), "стоп"
        if tp_done and sc["trail"] and mc <= peak * (1 - sc["trail"]):
            got += rem * fill_after(path, j, exit_delay)
            return nf(got / mc0), "трейлинг"
        if not tp_done and mc >= tp_level:
            sell = rem if sc["share"] >= 1 else sc["share"]
            got += sell * fill_after(path, j, exit_delay)
            rem -= sell
            tp_done = True
            if rem <= 1e-9:
                return nf(got / mc0), "тейк"
        if mc > peak:
            peak = mc
    got += rem * last_mc
    return nf(got / mc0), "30 минут" if rem else "тейк"


class Gapper:
    """Догрузка первых секунд монеты после покупки трейдера через RPC (с кешем в ec_gap)."""

    def __init__(self, conn: Any, rpc: Any = None):
        self.conn = conn
        self.rpc = rpc
        self.calls = 0
        self.max_calls = int(os.environ.get("EC_RPC_MAX", 20000))
        self.stats = collections.Counter()

    async def setup(self) -> None:
        await self.conn.execute(
            """CREATE TABLE IF NOT EXISTS ec_gap (mint text NOT NULL, trig_sig text NOT NULL, status text NOT NULL,
                   rows jsonb NOT NULL DEFAULT '[]', created_at timestamptz NOT NULL DEFAULT now(),
                   PRIMARY KEY (mint, trig_sig))""")
        if self.rpc is None:
            from scanner.config import Env
            from scanner.ingest.rpc import SolanaRpc
            env = Env()
            self.rpc = SolanaRpc(env.rpc_endpoint, rps=float(os.environ.get("EC_RPS", 4)), concurrency=4,
                                 fallback=env.rpc_fallback_url or None)

    async def close(self) -> None:
        if self.rpc is not None and hasattr(self.rpc, "close"):
            await self.rpc.close()

    async def fill(self, mint: str, trig: Any, first_after_sig: str | None,
                   first_after_slot: int | None) -> tuple[str, list[tuple[int, float, float, str, bool, str]]]:
        """('full'|'window'|отказ, сделки дыры). full — дыра до первой своей сделки целиком, window — первые ~3 с."""
        cached = await self.conn.fetchrow("SELECT status, rows FROM ec_gap WHERE mint=$1 AND trig_sig=$2",
                                          mint, trig["signature"])
        if cached:
            rows = cached["rows"] if isinstance(cached["rows"], list) else json.loads(cached["rows"])
            self.stats["из кеша"] += 1
            return cached["status"], [tuple(r) for r in rows]  # type: ignore[misc]
        status, rows = await self._fetch(mint, trig, first_after_sig, first_after_slot)
        if status in ("full", "window", "много сделок", "нет пула"):
            await self.conn.execute(
                "INSERT INTO ec_gap(mint, trig_sig, status, rows) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                mint, trig["signature"], status, json.dumps(rows))
        return status, rows

    async def _fetch(self, mint: str, trig: Any, before: str | None,
                     first_after_slot: int | None) -> tuple[str, list[Any]]:
        from scanner.analytics.curve import mc_sol
        from scanner.ingest.pump_parser import ParseStats, parse_transaction
        from scanner.ingest.rpc import RpcError
        from scanner.models import Trade
        if self.calls >= self.max_calls:
            return "лимит запросов", []
        pool_map: dict[str, str] = {}
        if trig["pool"] != "pump":
            amm = await self.conn.fetchval("SELECT amm_pool FROM tokens WHERE mint=$1", mint)
            if not amm:
                return "нет пула", []
            pool_map[amm] = mint
        s0 = int(trig["slot"])
        sigs: list[dict[str, Any]] = []
        try:
            for _ in range(10):                       # от первой своей сделки назад до покупки трейдера
                self.calls += 1
                page = await self.rpc.signatures_for(mint, limit=1000, before=before, until=trig["signature"])
                sigs += page
                if len(page) < 1000:
                    break
                before = page[-1]["signature"]
            else:
                return "много сделок", []
        except RpcError:
            return "ошибка RPC", []
        sigs = [x for x in sigs if not x.get("err") and x.get("slot") is not None and int(x["slot"]) >= s0]
        if len(sigs) <= GAP_FULL:
            status, want = "full", sigs
        else:
            want = [x for x in sigs if int(x["slot"]) <= s0 + EARLY_SLOTS]
            if len(want) > GAP_WINDOW_MAX:
                return "много сделок", []
            status = "window"
        stats = ParseStats()

        async def one(x: dict[str, Any]) -> list[Any]:
            self.calls += 1
            try:
                tx = await self.rpc.transaction(x["signature"])
            except RpcError:
                return [None]
            if not tx:
                return [None]
            out = []
            for ev in parse_transaction(tx, x["signature"], int(x["slot"]), "rpc", stats, pool_map):
                if isinstance(ev, Trade) and ev.mint == mint:
                    mc = mc_sol(ev.v_sol, ev.v_tokens)
                    if mc > 0:
                        out.append([int(x["slot"]), ev.ts.timestamp(), mc, ev.trader, ev.is_buy, x["signature"]])
            return out
        got = await asyncio.gather(*(one(x) for x in want))
        if any(r == [None] for r in got):
            return "ошибка RPC", []
        rows = sorted((r for rs in got for r in rs), key=lambda r: r[0])
        return status, rows


# трейлинг всей позиции без тейка: продажа только по откату от пика (включается сразу — от безубытка — или после +X%)
TRAIL_GRID = [dict(tp=act, share=1e-9, stop=stop, time_s=None, gain=0, trail=tr)
              for act in (0.0, 0.10, 0.25) for tr in (0.10, 0.15, 0.20, 0.30, 0.40, 0.50) for stop in (0.10, 0.20, None)]


def tname(sc: dict[str, Any]) -> str:
    start = "сразу" if not sc["tp"] else "после +{:.0%}".format(sc["tp"])
    stop = ", стоп −{:.0%}".format(sc["stop"]) if sc["stop"] else ", без стопа"
    return "трейл {:.0%} {}{}".format(sc["trail"], start, stop)


def pick_by_past(res: list[list[float]], days: list[str], fallback: list[float]) -> tuple[float, str]:
    """Вариант на день D — лучший по дням до D (30+ сигналов истории), иначе fallback. Итог в $ при $10, название
    варианта последнего дня."""
    tot, last = 0.0, "—"
    for dd in sorted(set(days)):
        past = [i for i, x in enumerate(days) if x < dd]
        idx = [i for i, x in enumerate(days) if x == dd]
        if len(past) >= 30:
            k = max(range(len(res[0])), key=lambda k: sum(res[i][k] for i in past))
            tot += sum(res[i][k] for i in idx) * 10
            last = str(k)
        else:
            tot += sum(fallback[i] for i in idx) * 10
    return tot, last


def trail_line(paths: list[tuple[Any, int, float, str, str]], xd: float) -> str:
    """paths: (путь, индекс входа, MC входа, чья продажа — выход, день). Трейлинг всей позиции: по прошлым дням,
    лучший задним числом, три постоянных варианта."""
    if not paths:
        return "—"
    res = [[simulate(p, i0, mc0, who, sc, xd)[0] for sc in TRAIL_GRID] for p, i0, mc0, who, _d in paths]
    g = [simulate(p, i0, mc0, who, SCHEMES["G"], xd)[0] for p, i0, mc0, who, _d in paths]
    wf, last = pick_by_past(res, [x[4] for x in paths], g)
    best = max(range(len(TRAIL_GRID)), key=lambda k: sum(r[k] for r in res))
    fixed = [TRAIL_GRID.index(dict(tp=a, share=1e-9, stop=st_, time_s=None, gain=0, trail=t))
             for a, t, st_ in ((0.0, 0.20, 0.10), (0.10, 0.30, 0.20), (0.25, 0.40, None))]
    return (f"по прошлым дням {wf:+.0f}$" + (f" (последний выбор: {tname(TRAIL_GRID[int(last)])})" if last != "—" else "")
            + f" · лучший задним числом {tname(TRAIL_GRID[best])} {sum(r[best] for r in res) * 10:+.0f}$ · "
            + " · ".join(f"{tname(TRAIL_GRID[k])} {sum(r[k] for r in res) * 10:+.0f}$" for k in fixed)
            + f" · для сравнения G {sum(g) * 10:+.0f}$ · вариантов в плюсе "
              f"{sum(1 for k in range(len(TRAIL_GRID)) if sum(r[k] for r in res) > 0)} из {len(TRAIL_GRID)}")


# часть на уровне «обычного роста» монеты, остаток — трейлинг: тейк 65–70% на +10…+40%, остаток трейл 15/30%
PART_GRID = [dict(tp=tp, share=sh, stop=stop, time_s=None, gain=0, trail=tr)
             for tp in (0.10, 0.15, 0.20, 0.25, 0.30, 0.40) for sh in (0.65, 0.70) for tr in (0.15, 0.30)
             for stop in (0.08, 0.15, 0.30, None)]


def pname(sc: dict[str, Any]) -> str:
    stop = "стоп −{:.0%}".format(sc["stop"]) if sc["stop"] else "без стопа"
    return "{:.0%} на +{:.0%}, остаток трейл {:.0%}, {}".format(sc["share"], sc["tp"], sc["trail"], stop)


def partial_line(paths: list[tuple[Any, int, float, str, str]], xd: float) -> str:
    """Тейк части на уровне роста, остаток трейлинг. По прошлым дням: и сам вариант, и «уровень = медиана роста
    монет по прошлым дням» (как ты предложил), лучший задним числом, разбивка по стопам."""
    if not paths:
        return "—"
    res = [[simulate(p, i0, mc0, who, sc, xd)[0] for sc in PART_GRID] for p, i0, mc0, who, _d in paths]
    g = [simulate(p, i0, mc0, who, SCHEMES["G"], xd)[0] for p, i0, mc0, who, _d in paths]
    days = [x[4] for x in paths]
    wf, _ = pick_by_past(res, days, g)
    # уровень тейка = медиана роста монет (пик за 30 мин) по прошлым дням, 70%, остаток трейл 15%, стоп по прошлым
    peaks = []
    for p, i0, mc0, _w, _d in paths:
        t0 = p[i0][1]
        peaks.append(max((q[2] for q in p[i0:] if q[1] - t0 <= HORIZON_S), default=mc0) / mc0 - 1)
    med_tot = 0.0
    for dd in sorted(set(days)):
        past = [i for i, x in enumerate(days) if x < dd]
        idx = [i for i, x in enumerate(days) if x == dd]
        if len(past) < 30:
            med_tot += sum(g[i] for i in idx) * 10
            continue
        lvl = max(0.05, st.median(peaks[i] for i in past))
        best = None
        for stop in (0.08, 0.15, 0.30, None):
            sc = dict(tp=lvl, share=0.70, stop=stop, time_s=None, gain=0, trail=0.15)
            v = sum(simulate(paths[i][0], paths[i][1], paths[i][2], paths[i][3], sc, xd)[0] for i in past)
            if best is None or v > best[0]:
                best = (v, sc)
        med_tot += sum(simulate(paths[i][0], paths[i][1], paths[i][2], paths[i][3], best[1], xd)[0]
                       for i in idx) * 10
    best_k = max(range(len(PART_GRID)), key=lambda k: sum(r[k] for r in res))
    by_stop = {}
    for stop in (0.08, 0.15, 0.30, None):
        ks = [k for k, sc in enumerate(PART_GRID) if sc["stop"] == stop]
        by_stop[stop] = max(sum(r[k] for r in res) for k in ks) * 10
    return (f"уровень = медиана роста по прошлым дням (70%, трейл 15%) {med_tot:+.0f}$ · вариант по прошлым дням "
            f"{wf:+.0f}$ · лучший задним числом {pname(PART_GRID[best_k])} {sum(r[best_k] for r in res) * 10:+.0f}$ · "
            "лучший при стопе: " + ", ".join(f"{('−%d%%' % round(k * 100)) if k else 'без'} {v:+.0f}$"
                                             for k, v in by_stop.items())
            + f" · G {sum(g) * 10:+.0f}$ · медиана роста монет {st.median(peaks):+.0%}")


# вход частями: первая покупка на сигнале, вторая — если цена откатилась на dip; выход от средней цены
BUDGET_SOL = 0.12                    # $10 при SOL ≈ $84: приоритет 0.0005 SOL на транзакцию — доля от этого
DCA_GRID = [dict(first=fs, dip=dp, tp=tp, stop=stp) for fs in (0.5, 0.33, 0.0)
            for dp in (0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40)
            for tp in (0.10, 0.15, 0.25, 0.40, 0.60) for stp in (0.10, 0.20, 0.30, None)]


def simulate_dca(path: list[tuple[int, float, float, str, bool]], i0: int, mc0: float, sc: dict[str, Any],
                 exit_delay: float | None = None) -> float:
    """Результат на весь бюджет ($10 = 1.0): доля first покупается на входе; если цена откатилась на dip от входа —
    докупка остатка (через задержку, как продажа); выход всего — когда чистый результат от средней цены ≥ tp, или стоп
    −stop от средней (только после докупки; до неё ждём отката), или конец окна 30 мин. Без докупки вложено first."""
    prio = 0.0005 / BUDGET_SOL
    t0 = path[i0][1]
    tokens = sc["first"] * (1 - FEE) / mc0
    spent = sc["first"] + prio if sc["first"] > 0 else 0.0      # first = 0 — ждём отката и заходим на всё
    added = False
    last = mc0
    for j in range(i0 + 1, len(path)):
        _slot, ts, mc, _tr, _b = path[j]
        if ts - t0 > HORIZON_S:
            break
        last = mc
        if not added and mc <= mc0 * (1 - sc["dip"]):
            px = fill_after(path, j, exit_delay)
            tokens += (1 - sc["first"]) * (1 - FEE) / px
            spent += (1 - sc["first"]) + prio
            added = True
            continue
        if spent <= 0:
            continue
        value = tokens * mc * (1 - FEE) - prio
        if value / spent - 1 >= sc["tp"]:
            return tokens * fill_after(path, j, exit_delay) * (1 - FEE) - prio - spent
        if added and sc["stop"] is not None and value / spent - 1 <= -sc["stop"]:
            return tokens * fill_after(path, j, exit_delay) * (1 - FEE) - prio - spent
    if spent <= 0:
        return 0.0                                  # отката не было — сделки нет
    return tokens * last * (1 - FEE) - prio - spent


def dca_name(sc: dict[str, Any]) -> str:
    if not sc["first"]:
        stop = "стоп −{:.0%}".format(sc["stop"]) if sc["stop"] else "без стопа"
        return "ждать −{:.0%}, весь $10, выход +{:.0%}, {}".format(sc["dip"], sc["tp"], stop)
    first = 10 * sc["first"]
    stop = "стоп −{:.0%}".format(sc["stop"]) if sc["stop"] else "без стопа"
    return "${:.1f} сразу + ${:.1f} на −{:.0%}, выход +{:.0%}, {}".format(first, 10 - first, sc["dip"], sc["tp"], stop)


def dca_line(paths: list[tuple[Any, int, float, str, str]], xd: float) -> list[str]:
    if not paths:
        return []
    g = [simulate(p, i0, mc0, who, SCHEMES["G"], xd)[0] for p, i0, mc0, who, _d in paths]
    res = [[simulate_dca(p, i0, mc0, sc, xd) for sc in DCA_GRID] for p, i0, mc0, _w, _d in paths]
    days = [x[4] for x in paths]
    wf, _ = pick_by_past(res, days, g)
    # откат до пика: насколько монета проседает от входа, прежде чем дойти до максимума (за 30 мин)
    dips = []
    for p, i0, mc0, _w, _d in paths:
        t0 = p[i0][1]
        win = [q for q in p[i0:] if q[1] - t0 <= HORIZON_S]
        k = max(range(len(win)), key=lambda k: win[k][2])
        dips.append(min(q[2] for q in win[:k + 1]) / mc0 - 1)
    # «откат = медиана откатов по прошлым дням», $5 + $5, выход и стоп — лучшие по прошлым дням
    med_tot = 0.0
    for dd in sorted(set(days)):
        past = [i for i, x in enumerate(days) if x < dd]
        idx = [i for i, x in enumerate(days) if x == dd]
        if len(past) < 30:
            med_tot += sum(g[i] for i in idx) * 10
            continue
        dip = max(0.03, -st.median(dips[i] for i in past))
        best = None
        for tp in (0.10, 0.15, 0.25, 0.40):
            for stp in (0.10, 0.20, 0.30, None):
                sc = dict(first=0.5, dip=dip, tp=tp, stop=stp)
                v = sum(simulate_dca(paths[i][0], paths[i][1], paths[i][2], sc, xd) for i in past)
                if best is None or v > best[0]:
                    best = (v, sc)
        med_tot += sum(simulate_dca(paths[i][0], paths[i][1], paths[i][2], best[1], xd) for i in idx) * 10
    top = sorted(range(len(DCA_GRID)), key=lambda k: -sum(r[k] for r in res))[:3]
    wait_k = [k for k, sc in enumerate(DCA_GRID) if not sc["first"]]
    wait_top = sorted(wait_k, key=lambda k: -sum(r[k] for r in res))[:3]
    hit = " · ".join(f"−{x:.0%}: {sum(dv <= -x for dv in dips) / len(dips):.0%}" for x in (0.05, 0.10, 0.20, 0.30))
    return [f"откат до пика: медиана {st.median(dips):+.0%}; монет, проседавших до пика на {hit}",
            f"откат = медиана по прошлым дням ($5+$5) {med_tot:+.0f}$ · вариант по прошлым дням {wf:+.0f}$ · "
            f"вариантов в плюсе {sum(1 for k in range(len(DCA_GRID)) if sum(r[k] for r in res) > 0)} из "
            f"{len(DCA_GRID)} · G {sum(g) * 10:+.0f}$",
            "лучшие задним числом: " + " · ".join(f"{dca_name(DCA_GRID[k])} {sum(r[k] for r in res) * 10:+.0f}$"
                                                for k in top),
            "только «ждать отката, весь $10» по прошлым дням: " + "{:+.0f}$".format(
                pick_by_past([[r[k] for k in wait_k] for r in res], days, g)[0])
            + " · лучшие задним числом: " + " · ".join(
                f"{dca_name(DCA_GRID[k])} {sum(r[k] for r in res) * 10:+.0f}$ "
                f"({sum(1 for r in res if r[k] != 0)} сд.)" for k in wait_top)]


# твои тактики — фиксированные, без подбора: факт по каждому дню на сделках из базы (порядок блоков)
FACT_TACTICS: list[tuple[str, str, dict[str, Any] | None]] = [
    ("Сейчас: G (тейк +25%, стоп −8%)", "sim", None),
    ("Тейк +25%, без стопа", "sim", dict(tp=0.25, share=1.0, stop=None, time_s=None, gain=0, trail=None)),
    ("$5+$5: докупка −30%, выход +15%, без стопа", "dca", dict(first=0.5, dip=0.30, tp=0.15, stop=None)),
    ("$5+$5: докупка −30%, выход +25%, без стопа", "dca", dict(first=0.5, dip=0.30, tp=0.25, stop=None)),
    ("$5+$5: докупка −20%, выход +25%, без стопа", "dca", dict(first=0.5, dip=0.20, tp=0.25, stop=None)),
    ("Ждать −25%, весь $10, выход +25%", "dca", dict(first=0.0, dip=0.25, tp=0.25, stop=None)),
    ("Ждать −30%, весь $10, выход +40%", "dca", dict(first=0.0, dip=0.30, tp=0.40, stop=None)),
    ("Ждать −40%, весь $10, выход +60%", "dca", dict(first=0.0, dip=0.40, tp=0.60, stop=None)),
    ("Трейл 20% сразу, стоп −10%", "sim", dict(tp=0.0, share=1e-9, stop=0.10, time_s=None, gain=0, trail=0.20)),
    ("Трейл 40% после +25%, без стопа", "sim", dict(tp=0.25, share=1e-9, stop=None, time_s=None, gain=0, trail=0.40)),
    ("70% на +25%, 30% трейл 15%, без стопа", "sim", dict(tp=0.25, share=0.70, stop=None, time_s=None, gain=0,
                                                          trail=0.15)),
]


FACT_CSV: list[list[Any]] = []          # та же таблица для файла


def fact_table(groups: list[tuple[str, list[tuple[Any, int, float, str, str]]]], xd: float) -> list[str]:
    """Одна таблица: тактика → $ по дням (копии | smart), всего, сделок, в плюсе."""
    out = []
    days = sorted({p[4] for _l, ps in groups for p in ps})
    FACT_CSV.clear()
    FACT_CSV.append(["тактика", "сигналы"] + [f"{d[3:]}.{d[:2]}, $" for d in days]
                    + ["всего, $", "сделок", "в плюсе, %", "на сделку, $"])
    out.append("ФАКТ ПО ТАКТИКАМ — сделки из базы в порядке блоков, $10 на сигнал, покупка через 2 блока, продажа "
               "через 1; по дням " + " / ".join(f"{d[3:]}.{d[:2]}" for d in days))
    for name, kind, sc in FACT_TACTICS:
        parts = []
        for label, ps in groups:
            vals = []
            for p, i0, mc0, who, day in ps:
                if kind == "dca":
                    v = simulate_dca(p, i0, mc0, sc, xd)  # type: ignore[arg-type]
                    if sc["first"] == 0 and v == 0.0:  # type: ignore[index]
                        continue                        # отката не было — сделки нет
                else:
                    v = simulate(p, i0, mc0, who, sc or SCHEMES["G"], xd)[0]
                vals.append((day, v))
            byd = [sum(v for d, v in vals if d == dd) * 10 for dd in days]
            n = len(vals)
            parts.append(f"{label} {sum(byd):+.0f}$ [" + " / ".join(f"{b:+.0f}" for b in byd) + f"] {n} сд., "
                         f"в плюсе {sum(v > 0 for _d, v in vals) / n if n else 0:.0%}")
            FACT_CSV.append([name, label] + [round(b, 2) for b in byd]
                            + [round(sum(byd), 2), n, round(100 * sum(v > 0 for _d, v in vals) / n, 1) if n else "",
                               round(sum(byd) / n, 2) if n else ""])
        out.append(f"• {name}: " + " | ".join(parts))
    return out


OTHER_MODES = {"dev": "запуски девов (лотерея D)", "mydev": "мой дев", "new": "новые монеты",
               "new_dev_wallet": "новый кошелёк дева"}
LOTTERY_D = dict(tp=1.0, share=0.34, stop=0.15, time_s=600, gain=0.10, trail=0.30)   # D: ⅓ на ×2, остаток трейл 30%
ROCKETS = [
    ("½ на +25%, остаток трейл 30%", dict(tp=0.25, share=0.5, stop=0.15, time_s=None, gain=0, trail=0.30)),
    ("держать: стоп −30%, трейл 40%", dict(tp=0.0, share=1e-9, stop=0.30, time_s=None, gain=0, trail=0.40)),
    ("тейк ×2, стоп −30%", dict(tp=1.0, share=1.0, stop=0.30, time_s=None, gain=0, trail=None)),
    ("тейк ×3, стоп −50%", dict(tp=2.0, share=1.0, stop=0.50, time_s=None, gain=0, trail=None)),
]


async def check_other(conn: Any, gapper: Any, say: Any) -> None:
    """Сигналы, которые бумага не торгует (или только D): запуски девов, новые монеты, мой дев — на скорости бота.
    Вход — через 2 блока после последней сделки монеты к моменту алерта (у запуска — после создания монеты),
    выход — через 1 блок после сигнала, по сделкам. Первые секунды догружаются так же, как у копий."""
    days = float(os.environ.get("EC_OTHER_DAYS", 3))
    alerts = await conn.fetch(
        """SELECT a.mint, a.mode, a.created_at, a.payload->'metrics'->>'cluster' AS share,
                  t.deployer, t.created_slot, t.signature AS create_sig, t.initial_mc_sol
           FROM alerts a JOIN tokens t ON t.mint = a.mint
           WHERE a.mode = ANY($1::text[]) AND a.created_at > now() - make_interval(days => $2)
             AND a.level <> 'watch'
           ORDER BY a.created_at DESC LIMIT $3""", list(OTHER_MODES), int(math.ceil(days)),
        int(os.environ.get("EC_OTHER_MAX", 1500)))
    alerts = list(reversed(alerts))
    rpc_cap = (gapper.calls if gapper else 0) + int(os.environ.get("EC_OTHER_RPC", 4000))
    if not alerts:
        return
    d, xd = MAIN_REAL
    groups: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    lost = collections.Counter()
    for a in alerts:
        trig = await conn.fetchrow(
            """SELECT slot, ts, mc_sol, signature, pool FROM trades WHERE mint=$1 AND slot IS NOT NULL AND mc_sol > 0
                 AND ts <= $2::timestamptz AND ts > $2::timestamptz - interval '30 seconds'
               ORDER BY slot DESC, id DESC LIMIT 1""", a["mint"], a["created_at"])
        if trig is None and a["mode"] == "dev" and a["created_slot"] and a["initial_mc_sol"]:
            trig = {"slot": a["created_slot"], "ts": a["created_at"], "mc_sol": a["initial_mc_sol"],
                    "signature": a["create_sig"], "pool": "pump"}
        if trig is None:
            lost["нет сделок до алерта"] += 1
            continue
        rows = await conn.fetch(
            """SELECT slot, extract(epoch FROM ts) t, mc_sol, trader, is_buy, signature FROM trades
               WHERE mint=$1 AND slot >= $2 AND ts <= $3::timestamptz + interval '31 minutes' AND mc_sol > 0
               ORDER BY slot, id""", a["mint"], trig["slot"], trig["ts"])
        s0 = int(trig["slot"])
        full = [(int(r["slot"]), float(r["t"]), float(r["mc_sol"]), r["trader"], r["is_buy"], r["signature"])
                for r in rows]
        if len(full) < 2:
            lost["нет цены после"] += 1
            continue
        if not any(s0 < q[0] <= s0 + EARLY_SLOTS for q in full):
            if not gapper:
                lost["без дозагрузки"] += 1
                continue
            if gapper.calls >= rpc_cap:
                lost["лимит запросов"] += 1
                continue
            after = next((q for q in full if q[0] > s0), None)
            status, extra = await gapper.fill(a["mint"], trig, after[5] if after else None, after[0] if after else None)
            if status not in ("full", "window"):
                lost[status] += 1
                continue
            have = {q[5] for q in full}
            full = sorted(full + [tuple(r) for r in extra if r[5] not in have], key=lambda q: q[0])
        path = [q[:5] for q in full]
        e = entry_at(path, s0, d)
        if not e:
            continue
        name = OTHER_MODES[a["mode"]]
        try:
            share = float(a["share"]) if a["share"] is not None else None
        except ValueError:
            share = None
        if a["mode"] == "dev" and share is not None and 0.030 <= share < 0.035:
            name += ", дев купил ~1 SOL (как D)"
        t0 = path[e[0]][1]
        peak = max((q[2] for q in path[e[0]:] if q[1] - t0 <= HORIZON_S), default=e[1]) / e[1]
        res = {"G": simulate(path, e[0], e[1], a["deployer"], SCHEMES["G"], xd)[0],
               "D": simulate(path, e[0], e[1], a["deployer"], LOTTERY_D, xd)[0],
               "peak": peak, "day": a["created_at"].strftime("%m.%d"),
               "grid": [simulate(path, e[0], e[1], a["deployer"], sc, xd)[0] for sc in GRID]}
        for label, sc in ROCKETS:
            res[label] = simulate(path, e[0], e[1], a["deployer"], sc, xd)[0]
        res["trail_path"] = (path, e[0], e[1], a["deployer"], res["day"])
        groups[name].append(res)
    say("")
    say(f"ДРУГИЕ СИГНАЛЫ НА СКОРОСТИ БОТА ({days:g} дн., $10 на сделку; покупка через {round(d / SLOT_S)} бл. после "
        f"последней сделки к алерту, у запуска — после создания; продажа через {round(xd / SLOT_S)} бл.; "
        f"цена видна 30 мин). Сейчас в бумагу идут только запуски девов «~1 SOL» (D).")
    if lost:
        say("  не посчитано: " + ", ".join(f"{k} {v}" for k, v in lost.items()))
    for name, rs in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        n = len(rs)
        hit = " · ".join(f"×{x}: {sum(r['peak'] >= x for r in rs) / n:.0%}" for x in (2, 5, 10))
        best = max(range(len(GRID)), key=lambda k: sum(r["grid"][k] for r in rs))
        # схема выхода по прошлым дням (честно)
        days_l = sorted({r["day"] for r in rs})
        wf = 0.0
        for dd in days_l:
            past = [r for r in rs if r["day"] < dd]
            k = max(range(len(GRID)), key=lambda k: sum(r["grid"][k] for r in past)) if len(past) >= 30 else None
            wf += sum((r["grid"][k] if k is not None else r["G"]) for r in rs if r["day"] == dd) * 10
        say(f"• {name}: {n} сигн. · дошли до {hit} · в плюсе (G) {sum(r['G'] > 0 for r in rs) / n:.0%}")
        say("    по дням, G: " + " · ".join(
            f"{dd[3:]}.{dd[:2]} {sum(r['G'] for r in rs if r['day'] == dd) * 10:+.0f}$ "
            f"({sum(r['day'] == dd for r in rs)} сд., в плюсе "
            f"{sum(r['G'] > 0 for r in rs if r['day'] == dd) / sum(r['day'] == dd for r in rs):.0%})"
            for dd in days_l))
        say(f"    G {sum(r['G'] for r in rs) * 10:+.0f}$ · D {sum(r['D'] for r in rs) * 10:+.0f}$ · "
            + " · ".join(f"{lbl} {sum(r[lbl] for r in rs) * 10:+.0f}$" for lbl, _ in ROCKETS)
            + f" · по прошлым дням {wf:+.0f}$ · лучшая задним числом {sname(GRID[best])} "
              f"{sum(r['grid'][best] for r in rs) * 10:+.0f}$")
        say("    трейлинг всей позиции: " + trail_line([r["trail_path"] for r in rs], xd))


async def send_files(conn: Any, text: str | None) -> None:
    """Файлы в Telegram (топик отчётов): таблица тактик CSV (открывается в Excel/Numbers) и полный отчёт TXT."""
    import csv as _csv
    import io as _io
    stamp = __import__("datetime").datetime.now().strftime("%Y-%m-%d_%H%M")
    files = []
    if FACT_CSV and text is None:
        buf = _io.StringIO()
        _csv.writer(buf, delimiter=";").writerows(FACT_CSV)
        files.append((f"tactics_{stamp}.csv", buf.getvalue(), "📊 Факт по тактикам: $ по дням, копии и smart"))
    if text is not None:
        files.append((f"check_{stamp}.txt", text, "🔬 Полный отчёт проверки"))
    for name, body, caption in files:
        oid = await conn.fetchval("INSERT INTO outbox(kind, payload, priority) VALUES ('file', $1, 4) RETURNING id",
                                  json.dumps({"csv": body, "filename": name, "caption": caption, "topic": "daily"}))
        await conn.execute("SELECT pg_notify('outbox', $1)", str(oid))


async def notify(conn: Any, text: str) -> None:
    """Итог — в Telegram (топик отчётов) через очередь бота, кусками до 3500 знаков."""
    chunks, cur = [], ""
    for line in text.splitlines():
        if len(cur) + len(line) + 1 > 3500:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    if cur.strip():
        chunks.append(cur)
    for i, ch in enumerate(chunks):
        title = "🔬 <b>Проверка «быстрый бот»</b>" + (f" ({i + 1}/{len(chunks)})" if len(chunks) > 1 else "")
        oid = await conn.fetchval("INSERT INTO outbox(kind, payload, priority) VALUES ('text', $1, 4) RETURNING id",
                                  json.dumps({"text": f"{title}\n<pre>{html.escape(ch)}</pre>", "topic": "daily"}))
        await conn.execute("SELECT pg_notify('outbox', $1)", str(oid))


def sname(sc: dict[str, Any]) -> str:
    return (f"+{sc['tp']:.0%}/{('−%d%%' % round(sc['stop'] * 100)) if sc['stop'] else 'без стопа'}"
            f"{'/10м' if sc['time_s'] else ''}")


async def main(dsn: str | None = None, rpc: Any = None) -> str:
    import asyncpg
    conn = await asyncpg.connect(dsn or os.environ["DATABASE_URL"])
    days = float(os.environ.get("EC_DAYS", 30))
    sigs = await conn.fetch(
        """SELECT DISTINCT ON (p.mint, date_trunc('minute', p.opened_at))
                  p.mint, p.opened_at, p.level, p.source_wallet, a.mode, a.trader AS alert_trader, a.payload
           FROM paper_positions p JOIN alerts a ON a.id = p.alert_id
           WHERE a.mode IN ('copy', 'smart') AND p.opened_at > now() - make_interval(days => $1)
           ORDER BY p.mint, date_trunc('minute', p.opened_at), p.opened_at""", int(math.ceil(days)))
    gapper = Gapper(conn, rpc) if os.environ.get("EC_BACKFILL", "1") != "0" else None
    if gapper:
        await gapper.setup()
    res: dict[str, dict[float, list[float]]] = collections.defaultdict(lambda: collections.defaultdict(list))
    slip: dict[float, list[float]] = collections.defaultdict(list)
    wallets: dict[str, list[float]] = collections.defaultdict(list)
    used, no_trigger, no_path, lost = 0, 0, 0, collections.Counter()
    origin = collections.Counter()
    good: list[dict[str, Any]] = []          # сигналы с известными первыми секундами
    lags: list[float] = []
    for n_sig, s in enumerate(sigs, 1):
        if n_sig % 100 == 0:
            print(f"… {n_sig}/{len(sigs)} сигналов, запросов RPC {gapper.calls if gapper else 0}", flush=True)
        who = s["source_wallet"] or s["alert_trader"]
        trig = await conn.fetchrow(
            """SELECT slot, ts, mc_sol, signature, pool, sol_amount FROM trades WHERE mint=$1 AND trader=$2 AND is_buy
                 AND slot IS NOT NULL AND ts <= $3::timestamptz AND ts > $3::timestamptz - interval '2 minutes'
               ORDER BY ts DESC, slot DESC LIMIT 1""",
            s["mint"], who, s["opened_at"]) if who else None
        if not trig:
            no_trigger += 1
            continue
        lags.append((s["opened_at"] - trig["ts"]).total_seconds())
        rows = await conn.fetch(
            """SELECT slot, extract(epoch FROM ts) t, mc_sol, trader, is_buy, signature FROM trades
               WHERE mint=$1 AND slot >= $2 AND ts <= $3::timestamptz + interval '31 minutes' AND mc_sol > 0
               ORDER BY slot, id""", s["mint"], trig["slot"], trig["ts"])
        s0 = int(trig["slot"])
        full = [(int(r["slot"]), float(r["t"]), float(r["mc_sol"]), r["trader"], r["is_buy"], r["signature"])
                for r in rows]
        if len(full) < 2:
            no_path += 1
            continue
        used += 1
        how = "видно сразу"
        if not any(s0 < p[0] <= s0 + EARLY_SLOTS for p in full):
            if not gapper:
                lost["без дозагрузки"] += 1
                continue
            after = next((p for p in full if p[0] > s0), None)
            status, extra = await gapper.fill(s["mint"], trig, after[5] if after else None,
                                              after[0] if after else None)
            if status not in ("full", "window"):
                lost[status] += 1
                continue
            have = {p[5] for p in full}
            full = sorted(full + [tuple(r) for r in extra if r[5] not in have], key=lambda p: p[0])
            how = "догружено целиком" if status == "full" else "догружены первые 3 с"
        origin[how] += 1
        path = [p[:5] for p in full]
        kind = f"{'копия' if s['mode'] == 'copy' else 'smart'} {'сильная' if s['level'] == 'strong' else 'одна'}"
        m_sig = (s["mode"] == "copy" and s["level"] == "strong") or (s["mode"] == "smart" and s["level"] != "strong")
        pl = s["payload"]
        pl = (json.loads(pl) if isinstance(pl, str) else pl) or {}
        buy = pl.get("copy_sol")
        if buy is None:
            buy = max((float(t.get("buy_sol") or 0) for t in pl.get("traders") or [] if isinstance(t, dict)),
                      default=None)
        tb = await conn.fetch(
            """SELECT slot, is_buy, sol_amount FROM trades WHERE mint=$1 AND trader=$2 AND slot > $3
                 AND ts <= $4::timestamptz + interval '30 minutes' ORDER BY slot, id""", s["mint"], who, s0, trig["ts"])
        good.append({"path": path, "s0": s0, "who": who, "m": m_sig, "how": how, "mint": s["mint"],
                     "t_sig": s["opened_at"],
                     "day": s["opened_at"].strftime("%m.%d"), "kind": kind,
                     "tsol": float(trig["sol_amount"] or 0),
                     "tbuys": [(int(x["slot"]), float(x["sol_amount"] or 0)) for x in tb if x["is_buy"]],
                     "buy": float(buy) if buy is not None else None,
                     "mc": float(pl["mc"]) if pl.get("mc") is not None else None,
                     "growth": float(((pl.get("metrics") or {}).get("growth_1m")) or 0)})
        for d in DELAYS:
            e = entry_at(path, s0, d)
            if not e:
                continue
            r, _why = simulate(path, e[0], e[1], who, SCHEMES["G"])
            res[kind][d].append(r)
            slip[d].append(e[1] / float(trig["mc_sol"]) - 1)
            if d == 1.0:
                wallets[who].append(r)
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

    out: list[str] = []
    say = out.append

    def cell(v: list[float]) -> str:
        return f"{st.mean(v):+5.1%} ({sum(x > 0 for x in v) / len(v):.0%} в плюсе, {len(v)})" if v else "—"
    say(f"За {days:g} дн.: сигналов копий и smart {len(sigs)}, посчитано {len(good)} "
        f"({', '.join(f'{k} {v}' for k, v in origin.items()) or '—'})")
    skipped = no_trigger + no_path + sum(lost.values())
    if skipped:
        say(f"Не посчитано {skipped}: нет сделки трейдера в базе {no_trigger}, нет цены после {no_path}"
            + "".join(f", {k} {v}" for k, v in lost.items()))
    if gapper:
        say(f"Запросов RPC за запуск: {gapper.calls}" + (f" (из кеша {gapper.stats['из кеша']})"
                                                       if gapper.stats["из кеша"] else ""))

    d0, xd0 = MAIN_REAL
    fact_groups = []
    for label, pick in (("копии", lambda g: not g["kind"].startswith("smart")),
                        ("smart", lambda g: g["kind"].startswith("smart"))):
        ps = []
        for g in good:
            if pick(g):
                e = entry_at(g["path"], g["s0"], d0)
                if e:
                    ps.append((g["path"], e[0], e[1], g["who"], g["day"]))
        fact_groups.append((label, ps))
    say("")
    fact_lines = fact_table(fact_groups, xd0)
    for ln in fact_lines:
        say(ln)
    if os.environ.get("EC_TG", "1") != "0":          # главное — сразу, не дожидаясь долгих разделов
        await notify(conn, "\n".join(out[:4] + [""] + fact_lines))
        await send_files(conn, None)
    # главный расчёт: реалистичный бот, все схемы выхода, каждый сигнал → результат
    mat: dict[tuple[float, float], list[list[float | None]]] = {}
    for d, xd in REAL:
        rows_m: list[list[float | None]] = []
        for g in good:
            e = entry_at(g["path"], g["s0"], d)
            rows_m.append([simulate(g["path"], e[0], e[1], g["who"], sc, xd)[0] if e else None for sc in GRID]
                          + [simulate(g["path"], e[0], e[1], g["who"], SCHEMES["G"], xd)[0] if e else None,
                             simulate(g["path"], e[0], e[1], g["who"], SCHEMES["M"], xd)[0] if e and g["m"] else None])
        mat[(d, xd)] = rows_m
    gi, mi = len(GRID), len(GRID) + 1
    days_l = sorted({g["day"] for g in good})

    def total(rows_m: list[list[float | None]], k: int, idx: list[int] | None = None) -> tuple[float, int, int]:
        v = [rows_m[i][k] for i in (idx if idx is not None else range(len(rows_m))) if rows_m[i][k] is not None]
        return sum(v) * 10, len(v), sum(x > 0 for x in v)   # type: ignore[operator]

    def walk_forward(rows_m: list[list[float | None]],
                     sel: set[int] | None = None) -> tuple[float, list[tuple[str, int, float, str]]]:
        """Схема выхода дня D выбирается только по дням до D (первый день — схема G). sel — только эти сигналы."""
        per, tot = [], 0.0
        for day in days_l:
            idx = [i for i, g in enumerate(good) if g["day"] == day and (sel is None or i in sel)]
            past = [i for i, g in enumerate(good) if g["day"] < day and (sel is None or i in sel)]
            if len(past) >= 30:
                k = max(range(len(GRID)), key=lambda k: total(rows_m, k, past)[0])
                name = sname(GRID[k])
            else:
                k, name = gi, "схема G"
            usd = total(rows_m, k, idx)[0]
            tot += usd
            per.append((day, len(idx), usd, name))
        return tot, per

    say("")
    say("БЫСТРЫЙ БОТ ПО ВСЕМ ДНЯМ ($10 на сделку). Покупка через N блоков после трейдера, продажа через M блоков "
        "после сигнала выхода (1 блок ≈ 0.4 с). «По прошлым дням» — схема выхода дня выбрана только по "
        "предыдущим дням (честно); «лучшая задним числом» — подогнана под все дни (завышена).")
    for (d, xd), rows_m in mat.items():
        wf, _per = walk_forward(rows_m)
        best = max(range(len(GRID)), key=lambda k: total(rows_m, k)[0])
        bt = total(rows_m, best)
        g_t = total(rows_m, gi)
        m_t = total(rows_m, mi)
        pos = sum(1 for k in range(len(GRID)) if total(rows_m, k)[0] > 0)
        say(f"• покупка +{round(d / SLOT_S)} бл., продажа +{round(xd / SLOT_S)} бл.: по прошлым дням {wf:+.0f}$ · "
            f"схема G {g_t[0]:+.0f}$ ({g_t[1]} сд.) · M {m_t[0]:+.0f}$ ({m_t[1]} сд.) · лучшая задним числом "
            f"{sname(GRID[best])} {bt[0]:+.0f}$ · вариантов в плюсе {pos} из {len(GRID)}")
    rows_m = mat[MAIN_REAL]
    wf, per = walk_forward(rows_m)
    say("")
    say(f"По дням, покупка +{round(MAIN_REAL[0] / SLOT_S)} бл., продажа +{round(MAIN_REAL[1] / SLOT_S)} бл. "
        f"(сделок · по прошлым дням · схема G · M):")
    cum = 0.0
    for day, n, usd, name in per:
        idx = [i for i, g in enumerate(good) if g["day"] == day]
        cum += usd
        g_t, m_t = total(rows_m, gi, idx), total(rows_m, mi, idx)
        say(f"  {day[3:]}.{day[:2]}: {n} · {usd:+.0f}$ ({name}) · G {g_t[0]:+.0f}$ · M {m_t[0]:+.0f}$ ({m_t[1]}) "
            f"· итого {cum:+.0f}$")
    try:
        raw = await conn.fetchval("SELECT value FROM kv WHERE key='sol_usd'")
        sol_usd = float((json.loads(raw) if isinstance(raw, str) else raw or {}).get("price") or 0)
    except Exception:  # noqa: BLE001
        sol_usd = 0.0
    sol_usd = sol_usd or 150.0
    d, xd = MAIN_REAL
    sized = []
    for usd in SIZES_USD:
        size = usd / sol_usd
        rows_s: list[list[float | None]] = []
        for g in good:
            e = entry_at(g["path"], g["s0"], d)
            rows_s.append([simulate(g["path"], e[0], e[1], g["who"], sc, xd, size)[0] if e else None for sc in GRID]
                          + [simulate(g["path"], e[0], e[1], g["who"], SCHEMES["G"], xd, size)[0] if e else None,
                             None])
        wf_s, _ = walk_forward(rows_s)
        n_tr = sum(1 for r in rows_s if r[0] is not None)
        usd_total = wf_s * usd / 10
        sized.append(f"${usd}: {usd_total:+.0f}$ ({usd_total / (n_tr * usd) if n_tr else 0:+.1%} на сделку, "
                     f"G {total(rows_s, gi)[0] * usd / 10:+.0f}$)")
    say(f"Размер ставки (покупка +{round(d / SLOT_S)} бл., продажа +{round(xd / SLOT_S)} бл., схема по прошлым дням; "
        f"учтено, что своя покупка и продажа двигают цену; SOL ${sol_usd:.0f}):")
    say("  " + " · ".join(sized))
    # ракеты: как часто монета после входа делала ×2/×5/×10 (в пределах 30 мин) и что дают схемы «держать»
    rockets = [
        ("½ на +25%, остаток — трейлинг 30%", dict(tp=0.25, share=0.5, stop=0.15, time_s=None, gain=0, trail=0.30)),
        ("держать: стоп −30%, трейлинг 40% от пика", dict(tp=0.0, share=1e-9, stop=0.30, time_s=None, gain=0,
                                                         trail=0.40)),
        ("тейк ×2, стоп −30%", dict(tp=1.0, share=1.0, stop=0.30, time_s=None, gain=0, trail=None)),
        ("тейк ×3, стоп −50%", dict(tp=2.0, share=1.0, stop=0.50, time_s=None, gain=0, trail=None)),
    ]
    say("")
    say(f"Ракеты (покупка +{round(d / SLOT_S)} бл., продажа +{round(xd / SLOT_S)} бл.; цена видна 30 мин после входа):")
    for label, pick in (("smart", lambda g: g["kind"].startswith("smart")),
                        ("копии", lambda g: not g["kind"].startswith("smart"))):
        grp = [(g, entry_at(g["path"], g["s0"], d)) for g in good if pick(g)]
        grp = [(g, e) for g, e in grp if e]
        if not grp:
            continue
        peaks = []
        for g, e in grp:
            t0 = g["path"][e[0]][1]
            peaks.append(max((p[2] for p in g["path"][e[0]:] if p[1] - t0 <= HORIZON_S), default=e[1]) / e[1])
        hit = " · ".join(f"×{x}: {sum(pk >= x for pk in peaks) / len(peaks):.0%}" for x in (2, 5, 10))
        g_usd = sum(simulate(g["path"], e[0], e[1], g["who"], SCHEMES["G"], xd)[0] for g, e in grp) * 10
        parts = [f"{name} {sum(simulate(g['path'], e[0], e[1], g['who'], sc, xd)[0] for g, e in grp) * 10:+.0f}$"
                 for name, sc in rockets]
        say(f"  {label} ({len(grp)}): дошли до {hit} · схема G {g_usd:+.0f}$ · " + " · ".join(parts))
    say("")
    say(f"Трейлинг всей позиции — без тейка, продажа только по откату от пика (покупка +{round(d / SLOT_S)} бл., "
        f"продажа +{round(xd / SLOT_S)} бл.; цена видна 30 мин, потом продажа по последней цене):")
    part_lines: list[str] = []
    dca_paths: list[tuple[str, list[Any]]] = []
    for label, pick in (("smart", lambda g: g["kind"].startswith("smart")),
                        ("копии", lambda g: not g["kind"].startswith("smart"))):
        paths = []
        for g in good:
            if pick(g):
                e = entry_at(g["path"], g["s0"], d)
                if e:
                    paths.append((g["path"], e[0], e[1], g["who"], g["day"]))
        if paths:
            say(f"  {label} ({len(paths)}): " + trail_line(paths, xd))
            part_lines.append(f"  {label} ({len(paths)}): " + partial_line(paths, xd))
            dca_paths.append((label, paths))
    say("")
    say("Часть на уровне обычного роста (65–70%), остаток — трейлинг 15/30% (тейк +10…+40%, стопы −8/−15/−30%/без):")
    for ln in part_lines:
        say(ln)
    say("")
    say("Вход частями: часть сразу, остаток — на откате (−5…−30% от входа); выход всего от средней цены (+10…+40%), "
        "стоп после докупки −10/−20/−30%/без; $10 на сигнал, до отката вложено только первая часть:")
    for label, paths in dca_paths:
        for i, ln in enumerate(dca_line(paths, xd)):
            say(f"  {label + ' (' + str(len(paths)) + ')' if i == 0 else ' ' * (len(label) + 3 + len(str(len(paths))))}"
                f": {ln}")
    # отбор без подгонки: smart — все; копии — только кошельки, у которых по ПРОШЛЫМ дням 3+ сигнала и плюс
    say("")
    say(f"Отбор без подгонки (покупка +{round(d / SLOT_S)} бл., продажа +{round(xd / SLOT_S)} бл., схема G): smart — "
        "все; копии — только кошельки, бывшие в плюсе по прошлым дням (3+ сигнала):")
    rows_m = mat[MAIN_REAL]
    cum, tot_s, tot_c, n_s, n_c = 0.0, 0.0, 0.0, 0, 0
    for day in days_l:
        past: dict[str, list[float]] = collections.defaultdict(list)
        for i, g in enumerate(good):
            if g["day"] < day and rows_m[i][gi] is not None:
                past[g["who"]].append(rows_m[i][gi])  # type: ignore[arg-type]
        ok = {w for w, v in past.items() if len(v) >= 3 and st.mean(v) > 0}
        sm = [rows_m[i][gi] for i, g in enumerate(good) if g["day"] == day and g["kind"].startswith("smart")
              and rows_m[i][gi] is not None]
        cp = [rows_m[i][gi] for i, g in enumerate(good) if g["day"] == day and not g["kind"].startswith("smart")
              and g["who"] in ok and rows_m[i][gi] is not None]
        ds, dc = sum(sm) * 10, sum(cp) * 10        # type: ignore[arg-type]
        cum += ds + dc
        tot_s, tot_c, n_s, n_c = tot_s + ds, tot_c + dc, n_s + len(sm), n_c + len(cp)
        say(f"  {day[3:]}.{day[:2]}: smart {len(sm)} · {ds:+.0f}$ | копии {len(cp)} (кошельков {len(ok)}) · "
            f"{dc:+.0f}$ | итого {cum:+.0f}$")
    say(f"  Всего: smart {n_s} сд. {tot_s:+.0f}$ ({tot_s / (10 * n_s) if n_s else 0:+.1%} на сделку) · копии "
        f"{n_c} сд. {tot_c:+.0f}$ ({tot_c / (10 * n_c) if n_c else 0:+.1%} на сделку)")
    # холдеры в момент сигнала: RugCheck totalHolders (проверка монеты перед первым алертом; своё число по своему
    # потоку сделок занижено — подписка на монету включается после покупки трейдера)
    hold_rows = await conn.fetch(
        """SELECT entity, value, fetched_at FROM external_facts
           WHERE key = 'total_holders' AND source = 'rugcheck' AND entity = ANY($1::text[])""",
        [f"token:{g['mint']}" for g in good])
    hmap = {}
    for r in hold_rows:
        try:
            v = r["value"]
            v = json.loads(v) if isinstance(v, str) else v
            hmap[r["entity"][6:]] = (float(v), r["fetched_at"])
        except (TypeError, ValueError):
            continue
    say("")
    say(f"Холдеры в момент сигнала (RugCheck, проверка перед алертом; покупка +{round(d / SLOT_S)} бл., продажа "
        f"+{round(xd / SLOT_S)} бл., $10 на сигнал):")
    sa_h = dict(tp=0.25, share=1.0, stop=None, time_s=None, gain=0, trail=None)
    buckets = ((0, 10), (10, 30), (30, 60), (60, 100), (100, 200), (200, 1e9))
    for label, pick in (("smart", lambda g: g["kind"].startswith("smart")),
                        ("копии", lambda g: not g["kind"].startswith("smart"))):
        grp = []
        for g in good:
            if not pick(g) or g["mint"] not in hmap:
                continue
            hv, at = hmap[g["mint"]]
            if abs((at - g["t_sig"]).total_seconds()) > 600:
                continue                                    # число снято не в момент сигнала — не берём
            e = entry_at(g["path"], g["s0"], d)
            if e:
                grp.append((g, e, hv))
        if not grp:
            say(f"  {label}: нет данных RugCheck")
            continue
        parts = []
        for lo, hi in buckets:
            sub = [(g, e) for g, e, hv in grp if lo <= hv < hi]
            if not sub:
                continue
            ra = [simulate(g["path"], e[0], e[1], g["who"], sa_h, xd)[0] for g, e in sub]
            rg = [simulate(g["path"], e[0], e[1], g["who"], SCHEMES["G"], xd)[0] for g, e in sub]
            name = f"{lo}–{hi:g}" if hi < 1e9 else f"от {lo}"
            parts.append(f"{name}: {len(sub)} сд. · A {sum(ra) * 10:+.0f}$ (в плюсе {sum(x > 0 for x in ra) / len(ra):.0%})"
                         f" · G {sum(rg) * 10:+.0f}$ (в плюсе {sum(x > 0 for x in rg) / len(rg):.0%})")
        say(f"  {label} ({len(grp)} с числом холдеров):")
        for ptxt in parts:
            say("    " + ptxt)
    # копии: совмещение фильтров — холдеры (RugCheck) × сумма первой покупки трейдера × выход при мелкой докупке.
    # Перебор комбинаций всегда находит «лучшую задним числом» — она завышена; честная цифра — комбинация на день D
    # выбирается только по дням до D
    say("")
    say(f"Копии: совмещение фильтров — холдеры × сумма покупки трейдера × выход при мелкой докупке (≤0.06 SOL) "
        f"(покупка +{round(d / SLOT_S)} бл., продажа +{round(xd / SLOT_S)} бл., $10 на сигнал):")
    sa_c = dict(tp=0.25, share=1.0, stop=None, time_s=None, gain=0, trail=None)
    cs = []
    for g in good:
        if g["kind"].startswith("smart"):
            continue
        e = entry_at(g["path"], g["s0"], d)
        if not e:
            continue
        hv = None
        if g["mint"] in hmap and abs((hmap[g["mint"]][1] - g["t_sig"]).total_seconds()) <= 600:
            hv = hmap[g["mint"]][0]
        ent = g["path"][e[0]][0]
        fs = next((sl for sl, sol in g["tbuys"] if sl > ent and sol <= 0.06), None)
        cs.append({"day": g["day"], "h": hv, "buy": g["tsol"],
                   "A": simulate(g["path"], e[0], e[1], g["who"], sa_c, xd)[0],
                   "A+": simulate(g["path"], e[0], e[1], g["who"], sa_c, xd, force_slot=fs)[0],
                   "G": simulate(g["path"], e[0], e[1], g["who"], SCHEMES["G"], xd)[0],
                   "G+": simulate(g["path"], e[0], e[1], g["who"], SCHEMES["G"], xd, force_slot=fs)[0]})
    hf = [("любые", lambda h: True), ("от 10", lambda h: h is not None and h >= 10),
          ("от 30", lambda h: h is not None and h >= 30), ("30–60", lambda h: h is not None and 30 <= h < 60),
          ("от 60", lambda h: h is not None and h >= 60)]
    bf = [("любая", 0.0), ("от 0.5", 0.5), ("от 1", 1.0), ("от 2", 2.0), ("от 5", 5.0)]
    combos = []
    for hn, hfun in hf:
        for bn, bmin in bf:
            for rule in ("", "+"):
                combos.append((f"холдеры {hn} · покупка {bn} SOL" + (" · выход при мелкой докупке" if rule else ""),
                               hfun, bmin, rule))

    def pick(c: tuple[Any, ...], x: dict[str, Any]) -> bool:
        return c[1](x["h"]) and x["buy"] >= c[2]
    rows_c = []
    for c in combos:
        sub = [x for x in cs if pick(c, x)]
        if len(sub) < 15:
            continue
        ra = [x["A" + c[3]] for x in sub]
        rg = [x["G" + c[3]] for x in sub]
        days_pos = sum(1 for dd in {x["day"] for x in sub} if sum(x["A" + c[3]] for x in sub if x["day"] == dd) > 0)
        rows_c.append((sum(ra) * 10, c[0], len(sub), sum(v > 0 for v in ra) / len(ra), sum(rg) * 10, days_pos,
                       len({x["day"] for x in sub})))
    rows_c.sort(key=lambda r: -r[0])
    say(f"  все копии: {len(cs)} сд. · A {sum(x['A'] for x in cs) * 10:+.0f}$ · G {sum(x['G'] for x in cs) * 10:+.0f}$")
    say(f"  комбинаций с 15+ сделками: {len(rows_c)}, в плюсе (A): {sum(1 for r in rows_c if r[0] > 0)}")
    for r in rows_c[:8]:
        say(f"    {r[1]}: {r[2]} сд. · A {r[0]:+.0f}$ (в плюсе {r[3]:.0%}) · G {r[4]:+.0f}$ · дней в плюсе {r[5]} из {r[6]}")
    # честно: комбинация на день — лучшая по прошлым дням (15+ сделок в прошлом)
    days_c = sorted({x["day"] for x in cs})
    wf_tot, wf_n, picks = 0.0, 0, []
    for dd in days_c:
        past = [x for x in cs if x["day"] < dd]
        best = None
        for c in combos:
            sub = [x for x in past if pick(c, x)]
            if len(sub) >= 15:
                v = sum(x["A" + c[3]] for x in sub)
                if best is None or v > best[0]:
                    best = (v, c)
        if best is None:
            continue
        today = [x for x in cs if x["day"] == dd and pick(best[1], x)]
        usd = sum(x["A" + best[1][3]] for x in today) * 10
        wf_tot += usd
        wf_n += len(today)
        picks.append(f"{dd[3:]}.{dd[:2]} {usd:+.0f}$ ({len(today)})")
    say(f"  честно — фильтр на день выбран по прошлым дням: {wf_tot:+.0f}$ на {wf_n} сделках · " + " · ".join(picks))
    # докупки трейдера: размер первой покупки, мелкая докупка (≈$1–2.5) — выход, крупная (≈$84+) — что дальше
    say("")
    say(f"Покупки и докупки трейдера (покупка +{round(d / SLOT_S)} бл., продажа +{round(xd / SLOT_S)} бл., $10 на "
        f"сигнал; 1 SOL ≈ $84):")
    sa = dict(tp=0.25, share=1.0, stop=None, time_s=None, gain=0, trail=None)        # A: +25% без стопа
    for label, pick in (("smart", lambda g: g["kind"].startswith("smart")),
                        ("копии", lambda g: not g["kind"].startswith("smart"))):
        grp = [(g, entry_at(g["path"], g["s0"], d)) for g in good if pick(g)]
        grp = [(g, e) for g, e in grp if e]
        if not grp:
            continue
        say(f"  {label} ({len(grp)}):")
        # 1) размер первой покупки
        parts = []
        for lo, hi in ((0, 0.5), (0.5, 1), (1, 2), (2, 5), (5, 1e9)):
            sub = [(g, e) for g, e in grp if lo <= g["tsol"] < hi]
            if sub:
                ra = sum(simulate(g["path"], e[0], e[1], g["who"], sa, xd)[0] for g, e in sub) * 10
                rg = sum(simulate(g["path"], e[0], e[1], g["who"], SCHEMES["G"], xd)[0] for g, e in sub) * 10
                parts.append(f"{lo:g}–{hi:g} SOL: {len(sub)} сд. A {ra:+.0f}$ G {rg:+.0f}$" if hi < 1e9
                             else f"от {lo:g} SOL: {len(sub)} сд. A {ra:+.0f}$ G {rg:+.0f}$")
        say("    первая покупка трейдера → " + " · ".join(parts))
        # 2) мелкая докупка после нашего входа — выходить сразу?
        for thr in (0.015, 0.03, 0.06):
            hit = []
            for g, e in grp:
                ent = g["path"][e[0]][0]
                fs = next((sl for sl, sol in g["tbuys"] if sl > ent and sol <= thr), None)
                if fs is not None:
                    hit.append((g, e, fs))
            if not hit:
                continue
            base_a = sum(simulate(g["path"], e[0], e[1], g["who"], sa, xd)[0] for g, e, _f in hit) * 10
            rule_a = sum(simulate(g["path"], e[0], e[1], g["who"], sa, xd, force_slot=fs)[0] for g, e, fs in hit) * 10
            base_g = sum(simulate(g["path"], e[0], e[1], g["who"], SCHEMES["G"], xd)[0] for g, e, _f in hit) * 10
            rule_g = sum(simulate(g["path"], e[0], e[1], g["who"], SCHEMES["G"], xd, force_slot=fs)[0]
                         for g, e, fs in hit) * 10
            after = []
            for g, e, fs in hit:
                j = next((k for k, q in enumerate(g["path"]) if q[0] >= fs), None)
                if j is None:
                    continue
                t1 = g["path"][j][1]
                later = [q[2] for q in g["path"][j:] if q[1] - t1 <= 300]
                if later:
                    after.append(later[-1] / g["path"][j][2] - 1)
            say(f"    мелкая докупка ≤{thr:g} SOL (≈${thr * 84:.1f}) после входа: {len(hit)} сд. · цена через 5 мин после "
                f"неё (медиана) {st.median(after) if after else 0:+.0%} · A: держать {base_a:+.0f}$ / выйти сразу "
                f"{rule_a:+.0f}$ · G: {base_g:+.0f}$ / {rule_g:+.0f}$")
        # 3) крупная докупка после входа — хороший знак?
        for thr in (1.0, 3.0):
            yes = [(g, e) for g, e in grp if any(sl > g["path"][e[0]][0] and sol >= thr for sl, sol in g["tbuys"])]
            no = [(g, e) for g, e in grp if (g, e) not in yes]
            if yes:
                ya = sum(simulate(g["path"], e[0], e[1], g["who"], sa, xd)[0] for g, e in yes) * 10
                na = sum(simulate(g["path"], e[0], e[1], g["who"], sa, xd)[0] for g, e in no) * 10
                say(f"    крупная докупка ≥{thr:g} SOL (≈${thr * 84:.0f}) после входа: {len(yes)} сд., A {ya:+.0f}$ "
                    f"({ya / len(yes):+.2f}$ на сделку) · без неё: {len(no)} сд., A {na:+.0f}$ "
                    f"({na / len(no) if no else 0:+.2f}$ на сделку)")
    # как улучшить smart: копии через фильтры smart; схема выхода только для smart; какие smart-кошельки дают плюс
    def usd_of(idx: list[int]) -> str:
        v = [rows_m[i][gi] for i in idx if rows_m[i][gi] is not None]
        return (f"{len(v)} сд. {sum(v) * 10:+.0f}$ ({sum(v) / len(v):+.1%}, в плюсе {sum(x > 0 for x in v) / len(v):.0%})"
                if v else "0 сд.")  # type: ignore[operator]
    copies = [i for i, g in enumerate(good) if not g["kind"].startswith("smart")]
    f_buy = lambda g: g["buy"] is not None and g["buy"] >= 0.5
    f_mc = lambda g: g["mc"] is not None and 20 <= g["mc"] <= 600
    f_gr = lambda g: g["growth"] <= 1.0
    say("")
    say(f"Копии через фильтры smart (покупка ≥0.5 SOL, MC 20–600 SOL, рост за минуту ≤100%; схема G, "
        f"+{round(d / SLOT_S)}/+{round(xd / SLOT_S)} бл.):")
    passed = [i for i in copies if f_buy(good[i]) and f_mc(good[i]) and f_gr(good[i])]
    say(f"  прошли бы все: {usd_of(passed)} · не прошли: {usd_of([i for i in copies if i not in set(passed)])}")
    for name, f in (("покупка ≥0.5 SOL", f_buy), ("MC 20–600 SOL", f_mc), ("рост ≤100%", f_gr)):
        say(f"  {name}: да {usd_of([i for i in copies if f(good[i])])} · нет "
            f"{usd_of([i for i in copies if not f(good[i])])}")
    pdays = collections.defaultdict(list)
    for i in passed:
        pdays[good[i]["day"]].append(i)
    say("  прошли все, по дням: " + " · ".join(
        f"{dd[3:]}.{dd[:2]} {sum(rows_m[i][gi] or 0 for i in ix) * 10:+.0f}$ ({len(ix)})"  # type: ignore[operator]
        for dd, ix in sorted(pdays.items())))
    smart_i = {i for i, g in enumerate(good) if g["kind"].startswith("smart")}
    if smart_i:
        wf_sm, per_sm = walk_forward(rows_m, smart_i)
        say(f"Smart: схема выхода по прошлым дням (только smart) {wf_sm:+.0f}$ · схема G {usd_of(sorted(smart_i))}")
        say("  по дням: " + " · ".join(f"{dd[3:]}.{dd[:2]} {u:+.0f}$ ({nm})" for dd, n, u, nm in per_sm if n))
        by_w = collections.defaultdict(list)
        for i in smart_i:
            if rows_m[i][gi] is not None:
                by_w[good[i]["who"]].append(rows_m[i][gi])
        rk = sorted(((sum(v) * 10, w, len(v)) for w, v in by_w.items()), reverse=True)  # type: ignore[arg-type]
        say(f"  smart-кошельков с сигналами: {len(rk)}; больше всех дали: "
            + ", ".join(f"{w[:5]}…{w[-4:]} {u:+.0f}$ ({n})" for u, w, n in rk[:4])
            + "; меньше всех: " + ", ".join(f"{w[:5]}…{w[-4:]} {u:+.0f}$ ({n})" for u, w, n in rk[-3:]))
    by_how = collections.defaultdict(list)
    for i, g in enumerate(good):
        by_how[g["how"]].append(i)
    say("По происхождению данных, схема G: " + " · ".join(
        f"{h} {total(rows_m, gi, idx)[0]:+.0f}$ ({len(idx)})" for h, idx in by_how.items()))
    by_kind = collections.defaultdict(list)
    for i, g in enumerate(good):
        by_kind[g["kind"]].append(i)
    say("По типу сигнала, схема G: " + " · ".join(
        f"{k} {total(rows_m, gi, idx)[0]:+.0f}$ ({len(idx)})" for k, idx in sorted(by_kind.items())))

    say("")
    say("Схема G, % на сделку при входе через N с после покупки трейдера (продажа через "
        f"{EXIT_DELAY_S:g} с):")
    say("  " + " | ".join(f"{d:g} с" for d in DELAYS))
    for kind in sorted(res):
        say(f"  {kind}: " + " | ".join(cell(res[kind][d]) for d in DELAYS))
    if any(slip.values()):
        say("Насколько дороже трейдера входим (медиана): "
            + " · ".join(f"{d:g} с: {st.median(slip[d]):+.0%}" for d in DELAYS if slip[d]))
    if fact and slip[5.0]:
        say(f"Сверка: реальный вход бумаги через 5 с (Jupiter) — медиана {st.median(r['delta'] for r in fact):+.0%} "
            f"к цене трейдера ({len(fact)} копий); расчёт по сделкам через 5 с — {st.median(slip[5.0]):+.0%}")
    say("")
    say("Скорость нашего бота сейчас:")
    if seen and seen["n"]:
        say(f"  видит покупку отслеживаемого кошелька через ≈{seen['med']:.1f} с после блока (медиана, точность "
            f"±0.5 с; 90% — до {seen['p90']:.1f} с), {seen['n']} сделок за 2 дня")
    if lags:
        say(f"  бумага входит через ≈{st.median(lags):.1f} с после блока трейдера (медиана по {len(lags)} сигналам)")
    ranked = sorted(((st.mean(v), w, len(v)) for w, v in wallets.items() if len(v) >= 3), reverse=True)
    if ranked:
        say("")
        say("Кошельки при входе через 1 с (3+ сигнала), лучшие и худшие:")
        show = ranked if len(ranked) <= 10 else ranked[:5] + ranked[-5:]
        for m, w, n in show:
            say(f"  {w[:6]}…{w[-4:]}: {m:+.1%} на сделку, {n} сигн.")
    if os.environ.get("EC_OTHER", "1") != "0":
        await check_other(conn, gapper, say)
    text = "\n".join(out)
    if os.environ.get("EC_TG", "1") != "0":
        await notify(conn, text)
        await send_files(conn, text)
    if gapper:
        await gapper.close()
    await conn.close()
    print(text)
    return text


if __name__ == "__main__":
    asyncio.run(main())
