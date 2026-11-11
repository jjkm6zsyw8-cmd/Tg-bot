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
             sc: dict[str, Any], exit_delay: float | None = None, size_sol: float | None = None) -> tuple[float, str]:
    """Выход по сделкам после входа. path: (слот, время, MC, трейдер, покупка). size_sol — со своим влиянием на цену."""
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
            """SELECT slot, ts, mc_sol, signature, pool FROM trades WHERE mint=$1 AND trader=$2 AND is_buy
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
        good.append({"path": path, "s0": s0, "who": who, "m": m_sig, "how": how,
                     "day": s["opened_at"].strftime("%m.%d"), "kind": kind,
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
    text = "\n".join(out)
    if os.environ.get("EC_TG", "1") != "0":
        await notify(conn, text)
    if gapper:
        await gapper.close()
    await conn.close()
    print(text)
    return text


if __name__ == "__main__":
    asyncio.run(main())
