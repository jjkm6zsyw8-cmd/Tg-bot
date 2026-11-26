"""Вход вокруг миграции на PumpSwap: что было бы, если покупать монету у конца кривой или сразу после переезда.

Зачем (20.11): по ресёрчу владельца после миграции ~20% ликвидности идёт на автопокупки в первые 5 минут (BOOST) —
предсказуемый спрос. Такого метода у бота нет. Проверка по своей базе (сделки первых 30 минут монеты, обе площадки):

  вход «на кривой»: первая сделка, где MC ≥ X SOL (250/300/350/380; миграция — около 410). Дойдёт ли монета до
  миграции, в этот момент неизвестно, поэтому в счёт идут и те, что не дошли;
  вход «после миграции»: первая сделка на PumpSwap (сразу) и первая сделка через минуту после неё;
  вход — через LAG_S по худшей цене этих секунд; выход — тейк +X% цены (ровно по уровню), стоп −S% (по цене
  сделки, которая его пробила), иначе через 5 или 15 минут; комиссии FEE.

Цены PumpSwap от PumpPortal — без виртуальных резервов пула (на 4–26% ниже настоящей): такие сделки в путь цены не
берутся. Сделка, где после миграции других цен нет или 30 минут записи монеты кончились раньше выхода, в итог не идёт
(их число — в отчёте).

Сервер: docker compose run --rm bot migration [--days 3] [--no-send] — итог в Telegram и Excel со всеми правилами.
"""
from __future__ import annotations

import asyncio
import base64
import io
import re
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from .dips import Acc, num, pct, usd
from .patterns import BATCH, FEE, LAG_S, TZ, WINDOW_S, load_coins, split

CURVE_MC = (250.0, 300.0, 350.0, 380.0)        # вход на кривой при MC от, SOL
AFTER_S = (0.0, 60.0)                          # вход после миграции: сразу и через минуту
TAKES = (0.1, 0.2, 0.3, 0.5)
STOPS = (0.1, 0.2)
HOLDS = (300.0, 900.0)
MIN_N = 30
FIRST_PRICE_S = 10.0                           # первая надёжная цена PumpSwap не позже 10 с после миграции

Rule = tuple[str, float, float, float]          # (вход, тейк, стоп, удержание с)


def entry_name(key: str) -> str:
    if key.startswith("curve"):
        return f"на кривой при MC от {key[5:]} SOL"
    s = float(key[3:])
    return "сразу после миграции" if s == 0 else f"через {s:.0f} с после миграции"


def rule_name(rule: Rule) -> str:
    key, take, stop, hold = rule
    return f"{entry_name(key)} · тейк +{take:.0%} · стоп −{stop:.0%} · до {hold / 60:.0f} мин"


async def load_trades(db: Any, mints: list[str]) -> dict[str, list[Any]]:
    rows = await db.fetch(
        """SELECT mint, ts, mc_sol, pool, source FROM trades WHERE mint = ANY($1::text[]) ORDER BY mint, ts, id""",
        mints)
    out: dict[str, list[Any]] = {}
    for r in rows:
        out.setdefault(r["mint"], []).append(r)
    return out


@dataclass
class Path:
    ts: list[float]                 # секунды с создания
    mc: list[float]
    amm: list[bool]                 # сделка на PumpSwap (с надёжной ценой)
    migrated_s: float | None        # первая сделка на PumpSwap (любая), секунды с создания
    amm_price: bool                 # после миграции есть надёжные цены


def coin_path(tok: dict[str, Any], rows: Iterable[Any]) -> Path | None:
    created = tok["created_at"].timestamp()
    ts: list[float] = []
    mc: list[float] = []
    amm: list[bool] = []
    mig = None
    for r in rows:
        t = r["ts"].timestamp() - created
        m = float(r["mc_sol"] or 0)
        if t > WINDOW_S or m <= 0:
            continue
        on_amm = r["pool"] == "pump-amm"
        if on_amm and mig is None:
            mig = t
        if on_amm and r["source"] == "pumpportal":
            continue                # цена без виртуальных резервов пула — в путь не берём
        ts.append(t)
        mc.append(m)
        amm.append(on_amm)
    if not mc or amm[0] or not 20 <= mc[0] <= 150:
        return None                 # пара не к SOL или монета не с кривой pump.fun
    return Path(ts, mc, amm, mig, any(amm))


def simulate(p: Path, i: int, take: float, stop: float, hold: float) -> tuple[float, str] | None:
    """Сделка со входа по сигналу на точке i. None — посчитать нельзя: запись монеты кончилась раньше выхода или
    монета переехала во время сделки, а надёжных цен PumpSwap нет."""
    t_in = p.ts[i] + LAG_S
    if t_in + hold > WINDOW_S:
        return None
    e, j = p.mc[i], i + 1
    while j < len(p.mc) and p.ts[j] <= t_in:
        e = max(e, p.mc[j])
        j += 1
    if p.migrated_s is not None and t_in < p.migrated_s <= t_in + hold and not p.amm_price:
        return None
    last = e
    for k in range(j, len(p.mc)):
        if p.ts[k] > t_in + hold:
            break
        last = p.mc[k]
        if last >= e * (1 + take):
            return (1 + take) * (1 - FEE) - 1, "тейк"
        if last <= e * (1 - stop):
            return last / e * (1 - FEE) - 1, "стоп"
    return last / e * (1 - FEE) - 1, "время"


def triggers(p: Path) -> dict[str, int]:
    """Точка входа по каждому сигналу (индекс сделки) — только по прошлому."""
    out: dict[str, int] = {}
    for x in CURVE_MC:
        for i in range(len(p.mc)):
            if p.amm[i] or (p.migrated_s is not None and p.ts[i] >= p.migrated_s):
                break
            if p.mc[i] >= x:
                out[f"curve{x:g}"] = i
                break
    if p.migrated_s is not None and p.amm_price:
        for s in AFTER_S:
            k = next((i for i in range(len(p.mc)) if p.amm[i] and p.ts[i] >= p.migrated_s + s), None)
            if k is not None and p.ts[k] - (p.migrated_s + s) <= FIRST_PRICE_S:
                out[f"amm{s:g}"] = k
    return out


@dataclass
class Study:
    coins: int = 0
    first: datetime | None = None
    last: datetime | None = None
    reached: dict[str, int] = field(default_factory=dict)        # дошли до MC X на кривой
    reached_mig: dict[str, int] = field(default_factory=dict)    # …и переехали за 30 минут
    migrated: int = 0
    with_price: int = 0
    no_price: int = 0                                            # сделок, которые посчитать нельзя
    after5: list[float] = field(default_factory=list)            # цена через 5 мин после миграции к первой цене
    max5: list[float] = field(default_factory=list)              # максимум за 5 мин к первой цене
    rules: dict[Rule, Acc] = field(default_factory=dict)

    def add_coin(self, tok: dict[str, Any], rows: Iterable[Any]) -> None:
        p = coin_path(tok, rows)
        if p is None:
            return
        self.coins += 1
        born = tok["created_at"]
        self.first = born if self.first is None or born < self.first else self.first
        self.last = born if self.last is None or born > self.last else self.last
        day = born.astimezone(TZ).strftime("%d.%m")
        trig = triggers(p)
        for key in trig:
            if key.startswith("curve"):
                self.reached[key] = self.reached.get(key, 0) + 1
                if p.migrated_s is not None:
                    self.reached_mig[key] = self.reached_mig.get(key, 0) + 1
        if p.migrated_s is not None:
            self.migrated += 1
            if p.amm_price:
                self.with_price += 1
                k0 = trig.get("amm0")
                if k0 is not None and p.ts[k0] + 300 <= WINDOW_S:
                    base = p.mc[k0]
                    window = [p.mc[k] for k in range(k0, len(p.mc)) if p.ts[k] <= p.ts[k0] + 300]
                    self.after5.append(window[-1] / base - 1)
                    self.max5.append(max(window) / base - 1)
        for key, i in trig.items():
            for take in TAKES:
                for stop in STOPS:
                    for hold in HOLDS:
                        res = simulate(p, i, take, stop, hold)
                        if res is None:
                            self.no_price += 1
                            continue
                        self.rules.setdefault((key, take, stop, hold), Acc()).add(res[0], res[1], day)


def rule_line(rule: Rule, acc: Acc) -> str:
    dp, dn = acc.days_plus()
    return (f"{rule_name(rule)}: сделок {num(acc.n)} · в среднем {pct(acc.avg)} · в плюсе {acc.wins / acc.n:.0%} · "
            f"итог по $10 {usd(acc)} · дней в плюсе {dp} из {dn}")


def report(st: Study, days: float, min_n: int = MIN_N) -> str:
    fmt = lambda d: d.astimezone(TZ).strftime("%d.%m %H:%M") if d else "—"   # noqa: E731
    lines = ["<b>Покупка вокруг миграции на PumpSwap — проверка по своей базе</b>",
             f"Монеты pump.fun за {days:g} дн ({fmt(st.first)} – {fmt(st.last)}, время Тбилиси): {num(st.coins)}. "
             "В базе — первые 30 минут жизни монеты.",
             "Вход через 2 с по худшей цене этих секунд; тейк и стоп — от цены входа; комиссии 3% вычтены; ставка $10.",
             ""]
    lines.append("<b>1. Сколько монет доходит до миграции</b>")
    for x in CURVE_MC:
        key = f"curve{x:g}"
        n = st.reached.get(key, 0)
        m = st.reached_mig.get(key, 0)
        lines.append(f"• дошли до MC {x:g} SOL: {num(n)}" + (f", из них переехали за 30 минут — {num(m)} ({m / n:.0%})"
                                                         if n else ""))
    lines.append(f"Переехали на PumpSwap за первые 30 минут: {num(st.migrated)}; с надёжной ценой после переезда: "
                 f"{num(st.with_price)}.")
    if st.after5:
        up = sum(1 for x in st.after5 if x > 0) / len(st.after5)
        lines.append(f"Цена через 5 минут после переезда (к первой цене на PumpSwap): медиана "
                     f"{pct(statistics.median(st.after5))}; выше первой — у {up:.0%}; максимум за эти 5 минут — "
                     f"медиана {pct(statistics.median(st.max5))} ({num(len(st.after5))} монет).")
    lines += ["", "<b>2. Лучшее правило для каждого входа</b> (где сделок не меньше " + str(min_n) + ")"]
    keys = [f"curve{x:g}" for x in CURVE_MC] + [f"amm{s:g}" for s in AFTER_S]
    for key in keys:
        cand = [(r, a) for r, a in st.rules.items() if r[0] == key and a.n >= min_n]
        if cand:
            r, a = max(cand, key=lambda ra: ra[1].avg)
            lines.append(f"• {rule_line(r, a)}")
        else:
            n = max((a.n for r, a in st.rules.items() if r[0] == key), default=0)
            lines.append(f"• {entry_name(key)}: сделок мало ({n})")
    good = [(r, a) for r, a in st.rules.items() if a.n >= min_n]
    plus = [(r, a) for r, a in good if a.avg > 0]
    lines += ["", f"<b>3. Лучшие из {len(st.rules)} правил</b> (сделок не меньше {min_n}; в плюсе — {len(plus)})"]
    for r, a in sorted(good, key=lambda ra: -ra[1].avg)[:8]:
        lines.append(f"• {rule_line(r, a)}")
    if plus:
        lines.append("Правило в плюсе только в один-два дня — случайность: смотрите «дней в плюсе».")
    if st.no_price:
        lines.append(f"Не посчитано сделок: {num(st.no_price)} (запись монеты кончилась раньше выхода или нет "
                     "надёжной цены после переезда).")
    lines.append("Все правила с итогами по дням — в Excel.")
    return "\n".join(lines)


def to_xlsx(st: Study, text: str) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
    wb = Workbook()
    ws = wb.active
    ws.title = "Итог"
    for ln in re.sub(r"</?b>", "", text).split("\n"):
        ws.append([ln])
    ws.column_dimensions["A"].width = 150
    sh = wb.create_sheet("Правила")
    days = sorted({d for a in st.rules.values() for d in a.days}, key=lambda d: (d[3:], d[:2]))
    sh.append(["Вход", "Тейк", "Стоп", "Держать до, мин", "Сделок", "Средний итог", "Медиана", "В плюсе",
               "Тейк сработал", "Стоп сработал", "По времени", "Итог по $10", *[f"{d}, средний" for d in days],
               "Дней в плюсе"])
    for cell in sh[1]:
        cell.font = Font(bold=True)
    green, red = PatternFill("solid", fgColor="E6F4EA"), PatternFill("solid", fgColor="FCE8E6")
    for (key, take, stop, hold), a in sorted(st.rules.items(), key=lambda ra: -ra[1].avg):
        per_day = [(a.days[d][0] / a.days[d][1]) if d in a.days and a.days[d][1] else None for d in days]
        sh.append([entry_name(key), take, stop, hold / 60, a.n, a.avg, statistics.median(a.nets), a.wins / a.n,
                   a.how.get("тейк", 0) / a.n, a.how.get("стоп", 0) / a.n, a.how.get("время", 0) / a.n, a.total * 10,
                   *per_day, a.days_plus()[0]])
    fmts = ["@", "0%", "0%", "0", "0", "+0.0%;-0.0%", "+0.0%;-0.0%", "0%", "0%", "0%", "0%", "+#,##0;-#,##0",
            *["+0.0%;-0.0%"] * len(days), "0"]
    for row in sh.iter_rows(min_row=2):
        for cell, f in zip(row, fmts):
            cell.number_format = f
        row[5].fill = green if (row[5].value or 0) > 0 else red
    sh.freeze_panes = "E2"
    sh.column_dimensions["A"].width = 30
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


async def run(db: Any, days: float = 3.0, send: bool = True, progress: Callable[[str], Any] | None = None,
              now: datetime | None = None, min_n: int = MIN_N) -> str:
    now = now or datetime.now(timezone.utc)
    until = now - timedelta(seconds=WINDOW_S + 300)
    since = until - timedelta(days=days)
    coins = await load_coins(db, since, until)
    st = Study()
    for k in range(0, len(coins), BATCH):
        chunk = coins[k:k + BATCH]
        trades = await load_trades(db, [c["mint"] for c in chunk])
        for c in chunk:
            st.add_coin(c, trades.get(c["mint"], []))
        if progress is not None and (k // BATCH) % 25 == 0:
            progress(f"монет {min(k + BATCH, len(coins))} из {len(coins)}")
        await asyncio.sleep(0)
    text = report(st, days, min_n)
    if send:
        for part in split(text):
            await db.system(part)
        if st.rules:
            await db.enqueue("zip", {"topic": "system", "b64": base64.b64encode(to_xlsx(st, text)).decode(),
                                     "filename": f"migration_{now:%Y%m%d_%H%M}.xlsx",
                                     "caption": "Покупка вокруг миграции: все правила, итоги по дням"}, priority=3)
    return text
