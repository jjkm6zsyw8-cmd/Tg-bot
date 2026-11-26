"""Оценка копии кошелька «как торгует бот» (25.11, задание владельца по разбору выгрузки).

Старая копируемость (copytrade.apply_copy_prices) входила через 5 с, выходила только вслед за трейдером, ставка 0.5 SOL,
сдвиг цены считала по кривой pump.fun даже на PumpSwap, а без цены выхода брала цену входа. Здесь — ровно путь бота:

1. Вход — блок +2 после покупки трейдера (худшая для покупки цена этих блоков; блока нет — цена через ~1 с).
2. Выход — та же decide_exit, что у бумаги и реального бота, по правилу A (всё на +25% чистыми, до 30 мин) с общими
   выходами: трейдер продал, дев продал (экстренно), мелкая докупка трейдера. Продажа — по цене через ~1 с.
3. Ставка 0.14 SOL; издержки — как у бота: комиссия 1.25% в каждую сторону (pump.FEE_BPS / pumpswap.FEE_BPS),
   приоритет + чаевые + подпись на каждую транзакцию (настройки реального бота). Круг при 0.14 SOL — 3.2–3.8%, как
   в тени.
4. Сдвиг цены своей сделкой: на кривой — по виртуальным резервам кривой, на PumpSwap — по пулу (постоянное
   произведение; глубина пула по MC от состояния на миграции: 85 SOL и 206.9 млн токенов).
5. Цены выхода нет (сделок монеты после нашего входа в базе нет, а монета не в окне полной записи) — сделка не идёт
   в оценку; доля таких — в итоге (no_price).
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any

from .paper import SCHEMES, TINY_REBUY_SOL, Position, decide_exit
from ..solana import INITIAL_V_SOL, INITIAL_V_TOKENS, TOTAL_SUPPLY

STAKE_SOL = 0.14
ENTRY_BLOCKS = 2
ENTRY_DELAY_S = 1.0            # вход, если блок сделки неизвестен
EXIT_DELAY_S = 1.0             # продажа «садится» через ~1 с после решения
FEE_BPS = 125                  # как pump.FEE_BPS и pumpswap.FEE_BPS у реального бота
FIXED_SOL = 0.0003 + 0.00001 + 0.000005   # приоритет + чаевые + подпись (real_priority_sol, real_tip_sol, BASE_FEE)
CURVE_K = INITIAL_V_SOL * INITIAL_V_TOKENS
POOL_SOL0, POOL_TOK0 = 85.005, 206_900_000.0   # пул PumpSwap сразу после миграции (MC 410.88 SOL)
POOL_K = POOL_SOL0 * POOL_TOK0
FULL_WINDOW_S = 30 * 60        # приёмник пишет все сделки новой монеты первые 30 мин (trade_window_min)
# 25.11: сделки PumpSwap основной поток (logsSubscribe по программе pump.fun) не видит — они приходят только через
# подписку на монету, а у неё лимит. Поэтому на PumpSwap и вне окна полной записи тишина дольше GAP_S между входом и
# выходом — это пробел в данных, а не «цена стояла»: такая копия не идёт в оценку
GAP_S = 90.0
HOLE_S = 4.0    # 27.11: дольше бот без цены не бывает (опрос раз в 4 с) — тейк после такой дыры продаётся по уровню тейка
# монета на кривой молчала в окне полной записи не меньше DEAD_S до его конца — её бросили: цена дальше та же (иначе
# такие копии — а это убыточные — выпадали бы из оценки и завышали её)
DEAD_S = 600.0
COVER_SLACK_S = 10.0             # 25.11: подписка на резервы начинается через 1–3 с после покупки трейдера
RULE = "A"
AMM = "pump-amm"


@dataclass(slots=True)
class Tick:
    ts: float
    mc: float
    slot: int | None = None
    trader: str = ""
    is_buy: bool = True
    sol: float = 0.0
    pool: str = "pump"
    # 25.11: сделки PumpPortal на PumpSwap несут MC без виртуальных резервов пула (на 4–26% ниже, state.TradeRow) —
    # как цена они не годятся, только как событие (трейдер / дев продал)
    price_ok: bool = True
    src: str = ""                        # 27.11: откуда цена (pumpportal / watch / rpc / pumpdata / reserve) — для разборов


@dataclass(slots=True)
class Entry:
    mint: str
    ts: float
    slot: int | None = None
    sol: float = 0.0


def reserves(mc: float, venue: str) -> tuple[float, float, float]:
    """(SOL, токены, k) резервов, при которых цена даёт этот MC: кривая — виртуальные резервы, пул — после миграции."""
    k = POOL_K if venue == AMM else CURVE_K
    sol = math.sqrt(max(mc, 1e-9) * k / TOTAL_SUPPLY)
    return sol, k / sol, k


def buy_tokens(mc: float, venue: str, sol_in: float, fee_bps: float = FEE_BPS) -> float:
    s, t, k = reserves(mc, venue)
    net = sol_in * 10_000 / (10_000 + fee_bps)
    return t - k / (s + net)


def sell_sol(mc: float, venue: str, tokens: float, fee_bps: float = FEE_BPS) -> float:
    s, t, k = reserves(mc, venue)
    gross = s - k / (t + tokens)
    return gross * (1 - fee_bps / 10_000)


def roundtrip_cost(mc: float, venue: str = "pump", stake: float = STAKE_SOL) -> float:
    """Сколько стоит круг «купил и сразу продал» при той же цене (доля ставки)."""
    tokens = buy_tokens(mc, venue, stake)
    back = sell_sol(mc, venue, tokens) - FIXED_SOL
    return 1 - back / (stake + FIXED_SOL)


class CopyPos(Position):
    """Позиция копии: «если продать сейчас» — по своей бирже (кривая / пул) и с затратами бота."""

    def net_if_sold(self, mc: float, fee_bps: float, priority_sol: float) -> float:
        venue = str(self.extra.get("venue") or "pump")
        value = (sell_sol(mc, venue, self.tokens) - FIXED_SOL) if self.tokens > 0 else 0.0
        return (self.proceeds_sol + value) / self.cost_sol - 1 if self.cost_sol else 0.0


def _price_at(ticks: list[Tick], ts: float) -> Tick | None:
    last = None
    for t in ticks:
        if t.ts > ts:
            break
        if t.price_ok:
            last = t
    return last


def take_level(pos: Position, sc: dict[str, Any], lo: float, hi: float) -> float:
    """27.11: цена (MC), на которой чистый итог позиции ровно доходит до тейка — между прошлой ценой lo и ценой hi,
    на которой тейк сработал (итог растёт с ценой: поиск делением пополам)."""
    tp = float(sc["tp_net"])
    if pos.net_if_sold(lo, FEE_BPS, FIXED_SOL) >= tp:
        return lo
    for _ in range(40):
        mid = (lo + hi) / 2
        if pos.net_if_sold(mid, FEE_BPS, FIXED_SOL) >= tp:
            hi = mid
        else:
            lo = mid
    return hi


def entry_by_delay(e: Entry, ticks: list[Tick], delay_s: float) -> Tick | None:
    """26.11: вход с нашей настоящей задержкой (медиана из боя 1.36 с от его покупки до нашей): худшая для покупки
    цена сделок за (его покупка; + delay]; сделок нет — цена после его покупки."""
    ok = [t for t in ticks if t.price_ok]
    win = [t for t in ok if e.ts < t.ts <= e.ts + delay_s]
    if win:
        return max(win, key=lambda t: t.mc)
    return _price_at(ok, e.ts + delay_s)


def pool_sol(mc: float, venue: str) -> float:
    """Настоящих SOL в кривой / пуле при этом MC (кривая: виртуальный резерв минус стартовые 30 SOL)."""
    s = reserves(mc, venue)[0]
    return s if venue == AMM else max(0.0, s - INITIAL_V_SOL)


def entry_tick(e: Entry, ticks: list[Tick]) -> Tick | None:
    """Цена входа бота: худшая для покупки цена блоков (его блок; его блок + 2]; сделок там нет — цена после последней
    сделки до блока + 2 (это его же покупка). Блок неизвестен — цена через ENTRY_DELAY_S."""
    ticks = [t for t in ticks if t.price_ok]
    if e.slot:
        win = [t for t in ticks if t.slot is not None and e.slot < t.slot <= e.slot + ENTRY_BLOCKS]
        if win:
            return max(win, key=lambda t: t.mc)
        before = [t for t in ticks if t.slot is not None and t.slot <= e.slot + ENTRY_BLOCKS and t.ts >= e.ts - 1]
        if before:
            return before[-1]
    return _price_at(ticks, e.ts + ENTRY_DELAY_S)


@dataclass
class Copy:
    res: float | None
    why: str | None = None               # None — посчитана; «нет цены входа» / «нет цены выхода»
    reason: str | None = None            # причина выхода
    entry_mc: float = 0.0
    exit_mc: float = 0.0
    venue: str = "pump"
    hold_s: float = 0.0
    stake: float = STAKE_SOL             # 26.11: ставка копии, SOL (для $ в день)
    burst: bool = False                  # 26.11: цена от нашего входа дала +BURST_UP за BURST_S


SMALL_POOL = "мелкий пул"             # копия не в оценке: бот в такой пул не входит (C1, real_min_pool_sol)
BURST_UP, BURST_S = 0.25, 30.0


def copy_trade(e: Entry, ticks: list[Tick], trader: str, dev: str | None, created_ts: float | None,
               scheme: dict[str, Any] | None = None, stake: float = STAKE_SOL,
               covered: list[tuple[float, float]] | None = None, entry_delay_s: float | None = None,
               stake_share: float | None = None, min_pool_sol: float = 0.0, exit_delay_s: float | None = None,
               stake_cap: float | None = None, his_share: float | None = None) -> Copy:
    """Копия одной покупки трейдера: вход как у бота, выход по decide_exit (правило A и общие выходы).
    covered — окна слежения за резервами монеты (reserve_cover): внутри окна тишина значит «цена не менялась».
    26.11: entry_delay_s — вход через нашу задержку (иначе блок +2); stake_share — ставка как доля SOL пула (3%
    глубины) вместо stake; min_pool_sol — в пул мельче бот не входит (копия не в оценке). 27.11: exit_delay_s —
    продажа «садится» через столько секунд после решения (по умолчанию EXIT_DELAY_S; настоящую задержку тени меряет
    datacheck --gap)."""
    xd = EXIT_DELAY_S if exit_delay_s is None else float(exit_delay_s)
    sc = scheme or SCHEMES[RULE]
    ticks = sorted(ticks, key=lambda t: t.ts)
    et = entry_by_delay(e, ticks, entry_delay_s) if entry_delay_s is not None else entry_tick(e, ticks)
    if et is None or et.mc <= 0:
        return Copy(None, "нет цены входа")
    venue = AMM if et.pool == AMM else "pump"
    depth = pool_sol(et.mc, venue)
    if min_pool_sol and depth < min_pool_sol:
        return Copy(None, SMALL_POOL, entry_mc=et.mc, venue=venue)
    if stake_share:
        stake = max(0.001, stake_share * reserves(et.mc, venue)[0])
    # 27.11 (владелец): ставка и при 3% пула — как в защите копирования боя: не больше доли его покупки и лимита на кошелёк
    if his_share and e.sol > 0:
        stake = min(stake, max(0.001, his_share * e.sol))
    if stake_cap:
        stake = min(stake, stake_cap)
    opened = e.ts + ENTRY_DELAY_S
    tokens = buy_tokens(et.mc, venue, stake)
    pos = CopyPos(id=0, portfolio="copy", mint=e.mint, opened_ts=opened, entry_mc=et.mc, cost_sol=stake + FIXED_SOL,
                  tokens=tokens, tokens_initial=tokens, target_mc=None, max_mc=et.mc, extra={"venue": venue})
    burst = any(t.price_ok and opened < t.ts <= opened + BURST_S and t.mc >= et.mc * (1 + BURST_UP) for t in ticks)
    deadline = opened + float(sc.get("max_hold_s") or 1800)
    entry_slot = et.slot

    trusted_until = (created_ts + FULL_WINDOW_S) if created_ts is not None else -1.0
    last_seen = opened
    last_px_ts, last_px_mc = opened, et.mc            # последняя известная цена (для дыр перед тейком)

    def in_cover(a: float, b: float) -> bool:
        return any(s <= a + COVER_SLACK_S and b <= e_ for s, e_ in covered or ())

    def gap(upto: float) -> bool:
        """Пробел в данных между прошлой сделкой и upto: на PumpSwap или вне окна полной записи тишина > GAP_S
        (25.11: кроме окна слежения за резервами — там цена известна на каждом изменении)."""
        if upto - last_seen <= GAP_S or in_cover(last_seen, upto):
            return False
        return venue == AMM or upto > trusted_until

    def close(at: float, why: str, mc_at: float | None = None) -> Copy:
        px = _price_at(ticks, at + xd)
        mc = mc_at if mc_at is not None else (px.mc if px is not None else et.mc)
        v = venue if mc_at is not None else (AMM if (px is not None and px.pool == AMM) else venue)
        pos.proceeds_sol += sell_sol(mc, v, pos.tokens) - FIXED_SOL
        return Copy(pos.proceeds_sol / pos.cost_sol - 1, None, why, et.mc, mc, venue, at - opened, stake, burst)

    for t in ticks:
        if t.ts <= opened:
            continue
        if t.ts > deadline:
            if gap(deadline):
                return Copy(None, "нет цены выхода", entry_mc=et.mc, venue=venue)
            return close(deadline, "max_hold")       # бот продаёт по времени по таймеру, не дожидаясь сделки
        if gap(t.ts):
            return Copy(None, "нет цены выхода", entry_mc=et.mc, venue=venue)
        if t.pool == AMM:
            venue = AMM
        reason = None
        if dev and t.trader == dev and not t.is_buy:
            reason = "emergency"
        elif t.trader == trader and not t.is_buy:
            reason = "trader_sell"
        elif (t.trader == trader and t.is_buy and 0 < t.sol <= TINY_REBUY_SOL
              and (t.slot is None or entry_slot is None or t.slot > entry_slot)):
            reason = "tiny_rebuy"
        if not t.price_ok:
            if reason:                               # событие есть, цены у сделки нет — продажа по последней цене
                return close(t.ts, reason)
            continue
        # 27.11 (владелец): дыра в ценах (бот без цены не бывает дольше HOLE_S, вне окна слежения за резервами) — тейк
        # продаётся по уровню тейка: бот увидел бы его раньше, а не по первой цене после дыры (раздувало кошельки
        # со всплеском: выгрузка — +87% у модели против +25% у тени после 4 мин без цен)
        hole = t.ts - last_px_ts > HOLE_S and not in_cover(last_px_ts, t.ts)
        prev_mc = last_px_mc
        last_seen = last_px_ts = t.ts
        last_px_mc = t.mc
        d = decide_exit(pos, t.mc, t.ts, sc, FEE_BPS, FIXED_SOL, emergency=reason)
        if d is None:
            continue
        share = float(d[1])
        at_take = take_level(pos, sc, prev_mc, t.mc) if d[0] == "take_profit" and hole else None
        if share < 1 and pos.tokens > 0:
            part = pos.tokens * share
            px = _price_at(ticks, t.ts + xd)
            mc_sell = at_take if at_take is not None else (px.mc if px else t.mc)
            pos.proceeds_sol += sell_sol(mc_sell, venue, part) - FIXED_SOL
            pos.tokens -= part
            pos.tp_done = True
            continue
        return close(t.ts, d[0], at_take)
    # сделок до конца удержания больше нет: цена известна, только если монета ещё в окне полной записи (сделок нет —
    # значит, их не было), иначе — пробел в данных
    if venue != AMM and created_ts is not None and (deadline <= trusted_until or trusted_until - last_seen >= DEAD_S):
        return close(deadline, "max_hold")
    if in_cover(last_seen, deadline):
        return close(deadline, "max_hold")      # резервы слушали до конца — цена с последнего изменения та же
    return Copy(None, "нет цены выхода", entry_mc=et.mc, venue=venue)


def entries_of(trades: list[dict[str, Any]]) -> list[Entry]:
    """Входы трейдера, которые бот скопировал бы: первая покупка монеты и каждая новая после того, как он продал
    90%+ купленного. trades: dict(mint, ts, is_buy, sol, tokens, slot) по времени."""
    out: list[Entry] = []
    state: dict[str, tuple[float, float]] = {}       # монета → (остаток, пик)
    for t in sorted(trades, key=lambda x: x["ts"]):
        bal, peak = state.get(t["mint"], (0.0, 0.0))
        if t["is_buy"]:
            if bal <= 0.1 * peak or peak == 0:
                out.append(Entry(t["mint"], float(t["ts"]), t.get("slot"), float(t.get("sol") or 0)))
                bal, peak = 0.0, 0.0
            bal += float(t.get("tokens") or 0)
            peak = max(peak, bal)
        else:
            bal = max(0.0, bal - float(t.get("tokens") or 0))
        state[t["mint"]] = (bal, peak)
    return out


@dataclass
class Summary:
    n: int = 0                              # входов
    priced: int = 0                         # посчитано
    no_entry: int = 0
    no_exit: int = 0
    results: list[float] = field(default_factory=list)
    reasons: dict[str, int] = field(default_factory=dict)

    @property
    def no_price(self) -> float:
        return (self.no_entry + self.no_exit) / self.n if self.n else 0.0

    @property
    def avg(self) -> float:
        return statistics.fmean(self.results) if self.results else 0.0

    @property
    def win(self) -> float:
        return sum(1 for x in self.results if x > 0) / len(self.results) if self.results else 0.0

    @property
    def t(self) -> float:
        from .copytrade import t_stat
        return t_stat(self.results)

    def add(self, c: Copy) -> None:
        self.n += 1
        if c.res is None:
            if c.why == "нет цены входа":
                self.no_entry += 1
            else:
                self.no_exit += 1
            return
        self.priced += 1
        self.results.append(c.res)
        self.reasons[c.reason or "?"] = self.reasons.get(c.reason or "?", 0) + 1

    def text(self) -> str:
        if not self.n:
            return "входов нет"
        return (f"{self.priced} из {self.n} копий посчитано (без цены {self.no_price:.0%}: входа {self.no_entry}, "
                f"выхода {self.no_exit}) · в среднем {self.avg:+.1%} · в плюсе {self.win:.0%} · t={self.t:.2f}")


def spread(items: list[Any], limit: int) -> list[Any]:
    """До limit элементов равномерно по времени (порядок — по времени, последний — всегда)."""
    if limit <= 0 or len(items) <= limit:
        return list(items)
    step = len(items) / limit
    idx = sorted({len(items) - 1 - int(i * step) for i in range(limit)})
    return [items[i] for i in idx]


# ---------------------------------------------------------------------- база
async def load_cover(db: Any, windows: dict[str, tuple[float, float]]) -> dict[str, list[tuple[float, float]]]:
    """Окна слежения за резервами монет (reserve_cover), пересекающие окна копий."""
    if not windows:
        return {}
    mints = list(windows)
    rows = await db.fetch(
        """SELECT p.mint, extract(epoch FROM c.started_at) AS s, extract(epoch FROM c.ended_at) AS e
           FROM unnest($1::text[], $2::float8[], $3::float8[]) AS p(mint, lo, hi)
           JOIN reserve_cover c ON c.mint = p.mint AND c.ended_at >= to_timestamp(p.lo)
                               AND c.started_at <= to_timestamp(p.hi)""",
        mints, [windows[m][0] for m in mints], [windows[m][1] for m in mints])
    out: dict[str, list[tuple[float, float]]] = {}
    for r in rows:
        out.setdefault(r["mint"], []).append((float(r["s"]), float(r["e"])))
    return out


async def load_ticks(db: Any, windows: dict[str, tuple[float, float]], reserves: bool = True) -> dict[str, list[Tick]]:
    if not windows:
        return {}
    mints = list(windows)
    rows = await db.fetch(
        """SELECT p.mint, extract(epoch FROM t.ts) AS ts, t.mc_sol, t.slot, t.trader, t.is_buy, t.sol_amount, t.pool,
                  t.source
           FROM unnest($1::text[], $2::float8[], $3::float8[]) AS p(mint, lo, hi)
           JOIN trades t ON t.mint = p.mint AND t.ts BETWEEN to_timestamp(p.lo) AND to_timestamp(p.hi) AND t.mc_sol > 0
           UNION ALL
           SELECT p.mint, extract(epoch FROM m.ts), m.mc_sol, m.slot, m.trader, m.is_buy, m.sol_amount, m.pool,
                  'pumpdata'
           FROM unnest($1::text[], $2::float8[], $3::float8[]) AS p(mint, lo, hi)
           JOIN market_trades m ON m.mint = p.mint AND m.ts BETWEEN to_timestamp(p.lo) AND to_timestamp(p.hi)
                AND m.mc_sol > 0
           ORDER BY 1, 2""", mints, [windows[m][0] for m in mints], [windows[m][1] for m in mints])
    out: dict[str, list[Tick]] = {}
    for r in rows:
        pool = r["pool"] or "pump"
        out.setdefault(r["mint"], []).append(Tick(float(r["ts"]), float(r["mc_sol"]), r["slot"], r["trader"],
                                                  bool(r["is_buy"]), float(r["sol_amount"] or 0), pool,
                                                  not (r["source"] == "pumpportal" and pool == AMM),
                                                  str(r["source"] or "")))
    # 25.11: цена из резервов (подписка на кривую / хранилища пула) — без сделки, только цена
    for r in [] if not reserves else await db.fetch(
            """SELECT p.mint, extract(epoch FROM r.ts) AS ts, r.mc_sol, r.slot, r.venue
               FROM unnest($1::text[], $2::float8[], $3::float8[]) AS p(mint, lo, hi)
               JOIN reserve_ticks r ON r.mint = p.mint AND r.ts BETWEEN to_timestamp(p.lo) AND to_timestamp(p.hi)
               WHERE r.mc_sol > 0""", mints, [windows[m][0] for m in mints], [windows[m][1] for m in mints]):
        out.setdefault(r["mint"], []).append(Tick(float(r["ts"]), float(r["mc_sol"]), r["slot"], "", True, 0.0,
                                                  r["venue"] or "pump", True, "reserve"))
    for m in out:
        out[m].sort(key=lambda t: t.ts)
    return out


@dataclass
class Prepared:
    """26.11: входы кошелька и данные их монет (сделки, резервы, окна слежения, создание и дев) — загружаются один раз,
    дальше копии считаются по разным правилам / задержкам / ставкам без новых запросов к базе."""
    address: str
    ents: list[Entry]
    ticks: dict[str, list[Tick]]
    cover: dict[str, list[tuple[float, float]]]
    meta: dict[str, tuple[Any, Any]]
    hold: float


async def prepare(db: Any, address: str, trades: list[dict[str, Any]], since: float, until: float | None = None,
                  limit: int = 120, recent_from: float | None = None, recent_limit: int = 60,
                  hold_s: float = 1800.0) -> Prepared:
    all_e = [e for e in entries_of(trades) if e.ts >= since and (until is None or e.ts <= until)]
    pick = {id(e): e for e in spread(all_e, limit)}
    if recent_from is not None:
        for e in [x for x in all_e if x.ts >= recent_from][-recent_limit:]:
            pick[id(e)] = e
    ents = sorted(pick.values(), key=lambda e: e.ts)
    if not ents:
        return Prepared(address, [], {}, {}, {}, hold_s)
    win: dict[str, tuple[float, float]] = {}
    for e in ents:
        a, b = e.ts - 5, e.ts + ENTRY_DELAY_S + hold_s + 60
        w = win.get(e.mint)
        win[e.mint] = (min(a, w[0]), max(b, w[1])) if w else (a, b)
    ticks = await load_ticks(db, win)
    cover = await load_cover(db, win)
    meta = {r["mint"]: (r["created"], r["deployer"]) for r in await db.fetch(
        "SELECT mint, extract(epoch FROM created_at) AS created, deployer FROM tokens WHERE mint = ANY($1::text[])",
        list(win))}
    return Prepared(address, ents, ticks, cover, meta, hold_s)


def without_reserves(p: Prepared) -> Prepared:
    """27.11: те же входы без цены из резервов (у тиков резервов нет кошелька) и без окон их слежения — сколько копий
    останется без цены, если подписку на резервы выключить."""
    return Prepared(p.address, p.ents, {m: [t for t in ts if t.trader] for m, ts in p.ticks.items()}, {}, p.meta,
                    p.hold)


def run(p: Prepared, scheme: dict[str, Any] | None = None, **kw: Any) -> list[tuple[Entry, Copy]]:
    """Копии всех входов p по правилу scheme (kw — как у copy_trade: entry_delay_s, stake_share, min_pool_sol…)."""
    sc = scheme or SCHEMES[RULE]
    hold = min(float(sc.get("max_hold_s") or 1800), p.hold)
    out = []
    for e in p.ents:
        created, dev = p.meta.get(e.mint, (None, None))
        hi = e.ts + ENTRY_DELAY_S + hold + 60
        c = copy_trade(e, [t for t in p.ticks.get(e.mint, []) if e.ts - 5 <= t.ts <= hi], p.address, dev or None,
                       float(created) if created is not None else None, sc, covered=p.cover.get(e.mint), **kw)
        out.append((e, c))
    return out


async def evaluate_trades(db: Any, address: str, trades: list[dict[str, Any]], since: float,
                          until: float | None = None, limit: int = 120, recent_from: float | None = None,
                          recent_limit: int = 60, scheme: dict[str, Any] | None = None) -> list[tuple[Entry, Copy]]:
    """Копии входов кошелька из его сделок trades (dict: mint, ts, is_buy, sol, tokens, slot): за [since, until] до
    limit входов равномерно по времени и ещё до recent_limit последних с recent_from (неделя — для оценки за 7 дней)."""
    sc = scheme or SCHEMES[RULE]
    p = await prepare(db, address, trades, since, until, limit, recent_from, recent_limit,
                      float(sc.get("max_hold_s") or 1800))
    return run(p, sc)


def summarize(copies: list[tuple[Entry, Copy]], since: float | None = None) -> Summary:
    s = Summary()
    for e, c in copies:
        if since is None or e.ts >= since:
            s.add(c)
    return s


async def wallet_trades(db: Any, address: str, since: float, until: float | None = None) -> list[dict[str, Any]]:
    from datetime import datetime, timezone
    lo = datetime.fromtimestamp(since, timezone.utc)
    hi = datetime.fromtimestamp(until, timezone.utc) if until else datetime.now(timezone.utc)
    # 27.11: плюс весь рынок из pumpdata.fun (market_trades); та же сделка из обоих источников — один раз (по блоку)
    rows = await db.fetch(
        """SELECT mint, extract(epoch FROM ts) AS ts, is_buy, sol_amount, token_amount, slot FROM trades
           WHERE trader = $1 AND ts >= $2 AND ts <= $3
           UNION ALL
           SELECT m.mint, extract(epoch FROM m.ts), m.is_buy, m.sol_amount, m.token_amount, m.slot FROM market_trades m
           WHERE m.trader = $1 AND m.ts >= $2 AND m.ts <= $3 AND NOT EXISTS (
               SELECT 1 FROM trades t WHERE t.trader = $1 AND t.mint = m.mint AND t.slot = m.slot AND t.is_buy = m.is_buy)
           ORDER BY 2""", address, lo, hi)
    return [{"mint": r["mint"], "ts": float(r["ts"]), "is_buy": r["is_buy"], "sol": r["sol_amount"],
             "tokens": r["token_amount"], "slot": r["slot"]} for r in rows]


async def evaluate(db: Any, address: str, since: float, until: float | None = None, limit: int = 120,
                   scheme: dict[str, Any] | None = None) -> tuple[Summary, list[tuple[Entry, Copy]]]:
    """Копии покупок кошелька за [since, until] (unix): до limit входов равномерно по времени."""
    copies = await evaluate_trades(db, address, await wallet_trades(db, address, since, until), since, until, limit,
                                   scheme=scheme)
    return summarize(copies), copies


# ---------------------------------------------------------------------- кандидаты: соседи по покупкам
async def cobuyers(db: Any, refs: list[str], window_s: float = 120.0, days: float = 30.0, min_shared: int = 2,
                   limit: int = 100) -> list[tuple[str, int]]:
    """Кошельки, которые покупали те же монеты, что refs, в пределах ±window_s от их покупки: (адрес, общих монет),
    больше общих — выше. Сами refs и дев монеты — не в счёт."""
    if not refs:
        return []
    rows = await db.fetch(
        """WITH r AS (SELECT DISTINCT ON (trader, mint) trader, mint, ts FROM trades
                      WHERE trader = ANY($1::text[]) AND is_buy AND ts > now() - make_interval(days => $3::int)
                      ORDER BY trader, mint, ts)
           SELECT t.trader, count(DISTINCT t.mint) n FROM r
           JOIN trades t ON t.mint = r.mint AND t.is_buy AND t.trader <> ALL($1::text[])
                AND t.ts BETWEEN r.ts - make_interval(secs => $2) AND r.ts + make_interval(secs => $2)
           LEFT JOIN tokens k ON k.mint = t.mint
           WHERE t.trader IS DISTINCT FROM k.deployer
           GROUP BY t.trader HAVING count(DISTINCT t.mint) >= $4 ORDER BY 2 DESC LIMIT $5""",
        list(refs), float(window_s), int(math.ceil(days)), int(min_shared), int(limit))
    return [(r["trader"], int(r["n"])) for r in rows]


# ---------------------------------------------------------------------- контроль против бумаги A
EXIT_GROUPS = (("тейк", ("take_profit", "target", "trailing")), ("за трейдером", ("trader_sell", "tiny_rebuy")),
               ("по таймеру", ("max_hold", "time_stop")), ("дев / стоп", ("emergency", "stop")))


def exit_group(reason: str | None) -> str:
    for name, rs in EXIT_GROUPS:
        if reason in rs:
            return name
    return "прочее"


def exits_table(pairs: list[tuple[float, str | None, float, str | None]]) -> list[str]:
    """Одни и те же сделки: (модель %, причина модели, бумага %, причина бумаги) → разбивка выходов и где расходятся."""
    names = [n for n, _ in EXIT_GROUPS] + ["прочее"]
    rows = [f"{'выход':<14}{'модель':>14}{'бумага':>14}"]
    for n in names:
        mm = [a for a, ra, _b, _rb in pairs if exit_group(ra) == n]
        pp = [b for _a, _ra, b, rb in pairs if exit_group(rb) == n]
        if not mm and not pp:
            continue
        cell = lambda xs: f"{len(xs)} · {statistics.fmean(xs):+.0%}" if xs else "—"   # noqa: E731
        rows.append(f"{n:<14}{cell(mm):>14}{cell(pp):>14}")
    diff: dict[tuple[str, str], int] = {}
    for _a, ra, _b, rb in pairs:
        k = (exit_group(ra), exit_group(rb))
        if k[0] != k[1]:
            diff[k] = diff.get(k, 0) + 1
    out = ["Выходы на одних и тех же сделках (сделок · средний итог):", *rows]
    if diff:
        out.append("Расходятся: " + "; ".join(f"модель «{a}» ↔ бумага «{b}» — {n}"
                                               for (a, b), n in sorted(diff.items(), key=lambda kv: -kv[1])))
    return out


async def control_text(db: Any, address: str, since: float, until: float) -> str:
    """Новая оценка кошелька за период против бумаги A на тех же монетах (контроль модели)."""
    summ, copies = await evaluate(db, address, since, until, limit=100_000)
    from datetime import datetime, timezone
    lo, hi = datetime.fromtimestamp(since, timezone.utc), datetime.fromtimestamp(until, timezone.utc)
    paper = await db.fetch(
        """SELECT mint, extract(epoch FROM opened_at) AS ts, pnl_pct, exit_reason FROM paper_positions
           WHERE portfolio = 'A' AND source_wallet = $1 AND opened_at BETWEEN $2 AND $3 AND closed_at IS NOT NULL
             AND NOT suspect AND exit_reason IS DISTINCT FROM 'reset' AND pnl_pct IS NOT NULL""", address, lo, hi)
    lines = [f"Кошелёк {address}", f"Период: {lo:%d.%m %H:%M} – {hi:%d.%m %H:%M} UTC",
             f"Новая оценка (вход блок +{ENTRY_BLOCKS}, правило A, ставка {STAKE_SOL} SOL): " + summ.text(),
             "Причины выхода: " + (", ".join(f"{k} {v}" for k, v in sorted(summ.reasons.items(), key=lambda kv: -kv[1]))
                                    or "—")]
    if paper:
        pa = [float(r["pnl_pct"]) for r in paper]
        lines.append(f"Бумага A за период: {len(pa)} сд, в среднем {statistics.fmean(pa):+.1%}, "
                     f"в плюсе {sum(1 for x in pa if x > 0) / len(pa):.0%}")
        by = {}
        for r in paper:
            by.setdefault(r["mint"], []).append((float(r["ts"]), float(r["pnl_pct"]), r["exit_reason"]))
        pairs = []
        for e, c in copies:
            if c.res is None:
                continue
            near = [p for p in by.get(e.mint, []) if abs(p[0] - e.ts) <= 120]
            if near:
                pairs.append((c.res, c.reason, near[0][1], near[0][2]))
        if pairs:
            lines.append(f"На одних и тех же сделках ({len(pairs)}): модель {statistics.fmean(p[0] for p in pairs):+.1%},"
                         f" бумага {statistics.fmean(p[2] for p in pairs):+.1%}")
            lines += exits_table(pairs)
    else:
        lines.append("Бумага A по этому кошельку за период: сделок нет")
    lines.append("Без цены — копии, где в базе нет сделок монеты до выхода (пробел дольше "
                 f"{GAP_S:g} с на PumpSwap или после первых 30 мин монеты, вне окна слежения за резервами): в оценку "
                 "не идут.")
    return "\n".join(lines)
