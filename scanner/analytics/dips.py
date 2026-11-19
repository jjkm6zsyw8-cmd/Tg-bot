"""Вход после падения: что было бы, если покупать монету, когда она упала на D% от своего максимума.

Зачем (19.11, идея пользователя): «у выросших монет перед ростом была просадка — входить после −30%, стоп −5…−15%,
тейк +30…+40% или трейлинг». Смотреть только на выросшие монеты нельзя: такая же просадка бывает у монет, которые
дальше падают, и их больше. Поэтому правило проверяется целиком, на ВСЕХ монетах pump.fun за N дней (своя база: все
сделки первых 30 минут на кривой):

  сигнал — цена упала на D% от максимума монеты (с создания), а максимум к этому моменту был не ниже G× от старта;
  вход — через LAG_S по худшей цене этих секунд, как у живого бота; одна сделка на монету на правило;
  выход — стоп −S% цены, тейк +X% цены или трейлинг (после +A% продать, если цена откатилась на B% от максимума после
  входа), иначе через HOLD_S. Комиссии туда-обратно FEE вычитаются. Стоп и трейлинг — по цене сделки, которая пробила
  уровень (проскок вниз считается), тейк — ровно по уровню.

Плюс две простые справки: какая просадка была перед пиком у монет, выросших в 3+ раза, и что было со всеми монетами
после падения на 30% от максимума.

Сервер: docker compose run --rm bot dips [--days 3] [--no-send] — итог в Telegram и Excel со всеми правилами.
"""
from __future__ import annotations

import asyncio
import base64
import bisect
import io
import re
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from .patterns import BATCH, FEE, HOLD_S, LAG_S, TZ, WINDOW_S, load_coins, load_trades, split

DROPS = (0.2, 0.3, 0.4, 0.5)            # падение от максимума монеты
GROWS = (1.0, 1.5, 2.0, 3.0)            # максимум к моменту падения — во столько раз выше старта (1 — любой)
STOPS = (0.05, 0.1, 0.15, 0.2, 0.3)     # стоп: цена ниже входа на S
TAKES = (0.2, 0.3, 0.4, 0.6, 1.0)       # тейк: цена выше входа на X — продать всё
TRAILS = ((0.2, 0.1), (0.3, 0.15))      # трейлинг: после +A продать, если цена ушла на B ниже максимума после входа
EXITS: tuple[tuple[str, float, float], ...] = (*(("take", x, 0.0) for x in TAKES),
                                               *(("trail", a, b) for a, b in TRAILS))
MIN_N = 100                             # в «лучшие» — только правила, где сделок не меньше
LAST_ENTRY_S = WINDOW_S - HOLD_S - LAG_S  # позже 15 минут удержания не уместятся в записанные 30 минут монеты
BIG_GROWTH = 3.0                        # «выросла хорошо» — максимум за 30 минут в 3+ раза выше старта
# правило пользователя из вопроса 19.11 — в отчёте отдельно
ASKED = ((0.3, 1.0, 0.05, ("take", 0.3, 0.0)), (0.3, 1.0, 0.05, ("take", 0.4, 0.0)),
         (0.3, 1.0, 0.15, ("take", 0.3, 0.0)), (0.3, 1.0, 0.15, ("take", 0.4, 0.0)),
         (0.3, 1.0, 0.05, ("trail", 0.3, 0.15)), (0.3, 1.0, 0.15, ("trail", 0.2, 0.1)))

Rule = tuple[float, float, float, tuple[str, float, float]]


def exit_name(ex: tuple[str, float, float]) -> str:
    kind, a, b = ex
    return f"тейк +{a:.0%}" if kind == "take" else f"трейлинг: после +{a:.0%} откат {b:.0%}"


def rule_name(rule: Rule) -> str:
    drop, grow, stop, ex = rule
    g = "" if grow <= 1 else f", перед этим выросла в {grow:g}+ раза"
    return f"упала на {drop:.0%}{g} · стоп −{stop:.0%} · {exit_name(ex)}"


# ---------------------------------------------------------------------------------------------- путь цены
def coin_path(tok: dict[str, Any], rows: Iterable[Any]) -> tuple[list[float], list[float]] | None:
    """Сделки монеты на кривой за первые 30 минут: (секунды с создания, MC в SOL). Пусто — не наша кривая (пара не к
    SOL: первая сделка вне 20–150 SOL) или сделок нет."""
    created = tok["created_at"].timestamp()
    ts, mc = [], []
    for r in rows:
        m = float(r["mc_sol"] or 0)
        t = r["ts"].timestamp() - created
        if m > 0 and t <= WINDOW_S:
            ts.append(t)
            mc.append(m)
    if not mc or not 20 <= mc[0] <= 150:
        return None
    return ts, mc


def trigger(ts: list[float], mc: list[float], drop: float, grow: float, last_s: float = LAST_ENTRY_S) -> int | None:
    """Первая сделка, где цена на drop ниже максимума монеты, а максимум к этому моменту — не ниже grow× от старта.
    Знает только прошлое: максимум считается по сделкам до этой."""
    start, top = mc[0], mc[0]
    for i in range(1, len(mc)):
        if ts[i] > last_s:
            return None
        if top >= start * grow and mc[i] <= top * (1 - drop):
            return i
        top = max(top, mc[i])
    return None


def entry(ts: list[float], mc: list[float], i: int) -> tuple[float, int]:
    """Вход через LAG_S после сигнала по худшей (самой высокой) цене этих секунд: (цена, первая сделка после входа)."""
    e, j = mc[i], i + 1
    while j < len(mc) and ts[j] <= ts[i] + LAG_S:
        e = max(e, mc[j])
        j += 1
    return e, j


def outcomes(ts: list[float], mc: list[float],
             i: int) -> dict[tuple[float, tuple[str, float, float]], tuple[float, str]]:
    """Итог сделки со входа по сигналу на сделке i — сразу для всех стопов и выходов: {(стоп, выход): (чистыми, как)}.
    Один проход: первые пересечения уровней тейка и стопа; трейлинг — отдельным проходом с момента включения."""
    e, j = entry(ts, mc, i)
    t_end = ts[i] + LAG_S + HOLD_S
    end = bisect.bisect_right(ts, t_end) - 1                       # последняя сделка до конца удержания
    up = {x: None for x in {*TAKES, *(a for a, _b in TRAILS)}}     # первая сделка с ценой ≥ e·(1+x)
    dn: dict[float, int | None] = {s: None for s in STOPS}         # первая сделка с ценой ≤ e·(1−s)
    pending = len(up) + len(dn)
    for k in range(j, end + 1):
        p = mc[k]
        for x in up:
            if up[x] is None and p >= e * (1 + x):
                up[x] = k
                pending -= 1
        for s in dn:
            if dn[s] is None and p <= e * (1 - s):
                dn[s] = k
                pending -= 1
        if not pending:
            break
    last = mc[end] if end >= j else e
    net = lambda p: p / e * (1 - FEE) - 1                          # noqa: E731
    trail_exit: dict[tuple[float, float], int | None] = {}
    for a, b in TRAILS:
        act, out = up[a], None
        if act is not None:
            peak = mc[act]
            for k in range(act + 1, end + 1):
                peak = max(peak, mc[k])
                if mc[k] <= peak * (1 - b):
                    out = k
                    break
        trail_exit[(a, b)] = out
    res: dict[tuple[float, tuple[str, float, float]], tuple[float, str]] = {}
    for s in STOPS:
        stop_k = dn[s]
        for ex in EXITS:
            kind, a, b = ex
            if kind == "take":
                hit = up[a]
                if hit is not None and (stop_k is None or hit < stop_k):
                    res[(s, ex)] = ((1 + a) * (1 - FEE) - 1, "тейк")
                elif stop_k is not None:
                    res[(s, ex)] = (net(mc[stop_k]), "стоп")
                else:
                    res[(s, ex)] = (net(last), "время")
            else:
                act = up[a]
                if stop_k is not None and (act is None or stop_k < act):
                    res[(s, ex)] = (net(mc[stop_k]), "стоп")
                elif act is not None and trail_exit[(a, b)] is not None:
                    res[(s, ex)] = (net(mc[trail_exit[(a, b)]]), "трейлинг")
                else:
                    res[(s, ex)] = (net(last), "время")
    return res


def max_dip_before_peak(mc: list[float]) -> tuple[float, float]:
    """(рост до пика за 30 минут во сколько раз от старта, самая глубокая просадка от максимума до этого пика)."""
    p = max(range(len(mc)), key=mc.__getitem__)
    top, dip = mc[0], 0.0
    for k in range(p + 1):
        top = max(top, mc[k])
        dip = max(dip, 1 - mc[k] / top)
    return mc[p] / mc[0], dip


def after_fall(ts: list[float], mc: list[float], drop: float = 0.3,
               rise: float = 0.3) -> tuple[bool, bool, bool] | None:
    """Монета упала на drop от максимума (до 15-й минуты) — что было за 15 минут после этого: (выросла на rise от этой
    цены, обновила прежний максимум, упала ещё на drop). None — не падала."""
    i = trigger(ts, mc, drop, 1.0)
    if i is None:
        return None
    top = max(mc[:i])
    later = [mc[k] for k in range(i + 1, len(mc)) if ts[k] <= ts[i] + HOLD_S]
    return (any(p >= mc[i] * (1 + rise) for p in later), any(p > top for p in later),
            any(p <= mc[i] * (1 - drop) for p in later))


# ---------------------------------------------------------------------------------------------- подсчёт
@dataclass
class Acc:
    n: int = 0
    total: float = 0.0
    wins: int = 0
    how: dict[str, int] = field(default_factory=dict)
    days: dict[str, list[float]] = field(default_factory=dict)     # день → [сумма, сделок]
    nets: list[float] = field(default_factory=list)

    def add(self, net: float, how: str, day: str) -> None:
        self.n += 1
        self.total += net
        self.wins += net > 0
        self.how[how] = self.how.get(how, 0) + 1
        d = self.days.setdefault(day, [0.0, 0])
        d[0] += net
        d[1] += 1
        self.nets.append(net)

    @property
    def avg(self) -> float:
        return self.total / self.n if self.n else 0.0

    def days_plus(self) -> tuple[int, int]:
        return sum(1 for s, c in self.days.values() if c and s > 0), len(self.days)


@dataclass
class Study:
    coins: int = 0
    first: datetime | None = None
    last: datetime | None = None
    rules: dict[Rule, Acc] = field(default_factory=dict)
    big: list[float] = field(default_factory=list)                 # просадки перед пиком у выросших в 3+ раза
    fell: list[tuple[bool, bool, bool]] = field(default_factory=list)

    def add_coin(self, tok: dict[str, Any], rows: Iterable[Any]) -> None:
        path = coin_path(tok, rows)
        if path is None:
            return
        ts, mc = path
        self.coins += 1
        born = tok["created_at"]
        self.first = born if self.first is None or born < self.first else self.first
        self.last = born if self.last is None or born > self.last else self.last
        day = born.astimezone(TZ).strftime("%d.%m")
        grew, dip = max_dip_before_peak(mc)
        if grew >= BIG_GROWTH:
            self.big.append(dip)
        fall = after_fall(ts, mc)
        if fall is not None:
            self.fell.append(fall)
        cache: dict[int, dict] = {}
        for drop in DROPS:
            for grow in GROWS:
                i = trigger(ts, mc, drop, grow)
                if i is None:
                    continue
                if i not in cache:
                    cache[i] = outcomes(ts, mc, i)
                for (stop, ex), (net, how) in cache[i].items():
                    self.rules.setdefault((drop, grow, stop, ex), Acc()).add(net, how, day)


# ---------------------------------------------------------------------------------------------- отчёт
def num(n: float) -> str:
    return f"{n:,.0f}".replace(",", "\u00a0")


def usd(acc: Acc) -> str:
    v = acc.total * 10
    return f"{'+' if v > 0 else '−' if v < 0 else ''}${num(abs(v))}"


def pct(v: float) -> str:
    return f"{v * 100:+.1f}%".replace("-", "−")


def rule_line(rule: Rule, acc: Acc) -> str:
    dp, dn = acc.days_plus()
    return (f"{rule_name(rule)}: сделок {num(acc.n)} · в среднем {pct(acc.avg)} · в плюсе {acc.wins / acc.n:.0%} · "
            f"итог по $10 {usd(acc)} · дней в плюсе {dp} из {dn}")


def report(st: Study, days: float, min_n: int = MIN_N) -> str:
    fmt = lambda d: d.astimezone(TZ).strftime("%d.%m %H:%M") if d else "—"   # noqa: E731
    lines = ["<b>Вход после падения — проверка на всех монетах</b>",
             f"Монеты pump.fun за {days:g} дн ({fmt(st.first)} – {fmt(st.last)}, время Тбилиси): {num(st.coins)}.",
             "Правило: монета упала на D% от своего максимума → бот покупает через 2 с по худшей цене этих секунд → "
             "продаёт по стопу, тейку или трейлингу, иначе через 15 минут. Комиссии 3% вычтены, ставка $10.",
             "Стоп — по цене сделки, которая его пробила: если цена рухнула сразу на 50%, убыток −50%, а не −5%.", ""]
    lines.append("<b>1. Какая просадка была перед ростом у выросших монет</b>")
    if st.big:
        b = sorted(st.big)
        small = sum(1 for d in b if d < 0.1) / len(b)
        deep = sum(1 for d in b if d >= 0.3) / len(b)
        lines.append(f"Выросли в {BIG_GROWTH:g}+ раза за первые 30 минут: {num(len(b))} из {num(st.coins)} монет. "
                     f"Самая глубокая "
                     f"просадка перед пиком: медиана −{statistics.median(b):.0%}; не глубже −10% — у {small:.0%}; "
                     f"−30% и глубже — у {deep:.0%}.")
    else:
        lines.append("Монет, выросших в 3+ раза, нет.")
    lines += ["", "<b>2. Что было со всеми монетами после падения на 30% от максимума</b>"]
    if st.fell:
        n = len(st.fell)
        rose, high, more = (sum(1 for f in st.fell if f[k]) / n for k in range(3))
        lines.append(f"Таких монет: {num(n)} из {num(st.coins)}. За 15 минут после падения: "
                     f"выросли на +30% от этой цены — "
                     f"{rose:.0%}; обновили прежний максимум — {high:.0%}; упали ещё на 30% — {more:.0%}.")
    else:
        lines.append("Таких монет нет.")
    lines += ["", "<b>3. Ваше правило</b> (упала на 30% от максимума)"]
    for rule in ASKED:
        acc = st.rules.get(rule)
        lines.append(f"• {rule_line(rule, acc)}" if acc and acc.n else f"• {rule_name(rule)}: сделок нет")
    total = len(st.rules)
    good = [(r, a) for r, a in st.rules.items() if a.n >= min_n]
    plus = [(r, a) for r, a in good if a.avg > 0]
    lines += ["", f"<b>4. Лучшие из {total} правил</b> (где сделок не меньше {min_n}; в плюсе — {len(plus)})"]
    for rule, acc in sorted(good, key=lambda ra: -ra[1].avg)[:10]:
        lines.append(f"• {rule_line(rule, acc)}")
    if plus:
        lines.append("Правило в плюсе только в один-два дня из всех — случайность: проверяйте «дней в плюсе».")
    lines.append("")
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
    head = ["Упала от максимума", "Перед этим выросла, раз", "Стоп", "Выход", "Сделок", "Средний итог", "Медиана",
            "В плюсе", "Тейк/трейлинг", "Стоп сработал", "По времени", "Итог по $10", *[f"{d}, средний" for d in days],
            "Дней в плюсе"]
    sh.append(head)
    for cell in sh[1]:
        cell.font = Font(bold=True)
    green, red = PatternFill("solid", fgColor="E6F4EA"), PatternFill("solid", fgColor="FCE8E6")
    for (drop, grow, stop, ex), acc in sorted(st.rules.items(), key=lambda ra: -ra[1].avg):
        if not acc.n:
            continue
        win_exit = acc.how.get("тейк", 0) + acc.how.get("трейлинг", 0)
        per_day = [(acc.days[d][0] / acc.days[d][1]) if d in acc.days and acc.days[d][1] else None for d in days]
        sh.append([drop, grow, stop, exit_name(ex), acc.n, acc.avg, statistics.median(acc.nets), acc.wins / acc.n,
                   win_exit / acc.n, acc.how.get("стоп", 0) / acc.n, acc.how.get("время", 0) / acc.n, acc.total * 10,
                   *per_day, acc.days_plus()[0]])
    fmts = ["0%", "0.0", "0%", "@", "0", "+0.0%;-0.0%", "+0.0%;-0.0%", "0%", "0%", "0%", "0%", "+#,##0;-#,##0",
            *["+0.0%;-0.0%"] * len(days), "0"]
    for row in sh.iter_rows(min_row=2):
        for cell, f in zip(row, fmts):
            cell.number_format = f
        row[5].fill = green if (row[5].value or 0) > 0 else red
    sh.freeze_panes = "E2"
    sh.column_dimensions["D"].width = 30
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


async def run(db: Any, days: float = 3.0, send: bool = True, progress: Callable[[str], Any] | None = None,
              now: datetime | None = None, min_n: int = MIN_N) -> str:
    now = now or datetime.now(timezone.utc)
    until = now - timedelta(seconds=WINDOW_S + 300)          # у монет до этого времени окно в 30 минут закрыто
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
                                     "filename": f"dips_{now:%Y%m%d_%H%M}.xlsx",
                                     "caption": "Вход после падения: все правила, итоги по дням"}, priority=3)
    return text
