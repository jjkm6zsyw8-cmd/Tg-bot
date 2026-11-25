"""Режим «охотник» — бот сам находит кошельки, на которых НАШ бот зарабатывает: заходит вместе с кошельком (с нашей
настоящей задержкой) и выходит НАШИМ лучшим правилом. Сколько держит сам кошелёк — неважно. Цель — от $40 в день.

25.11 — первая версия; 26.11 (сводное ТЗ владельца):
A1. Вход в модели — через нашу задержку из боя (hunter_entry_delay_s, медиана 1.36 с от его покупки до нашей);
    рядом считается и вход «блок +2».
A2. Правило выхода для каждого кошелька (A / B / G): лучшее выбирается на первой половине его сделок, проверяется на
    второй: там хуже A — остаётся A; на второй в минусе — не горячий.
A3. $ в день = сделок в день × ставка × средний итог копии после издержек; при 0.14 SOL и при максимальной ставке
    (3% SOL пула, сдвиг цены своей покупкой учтён).
A4. «Всплеск» — доля его покупок, после которых цена от нашего входа дала +25% за 30 с.
A5. Списки по НОВЫМ сделкам после находки (модель, правило кошелька, задержка): «в бой» — от $40/день и 50+ копий;
    «наблюдение» — от $15/день (не выкидываем); архив — с причиной.
A6. «Остыл» (за кем повторяем): по нашим сделкам с начала повтора t ≤ −2 на 10+ сделках, или у него нет покупок
    12 ч — алерт и стоп копирования.
A7. Алерт о находке: адрес, монеты, средний %, % в плюсе, $ в день, всплеск, удержание, активен ли; кнопки «Тень» /
    «Бой 0.14» / «Игнор».
A8. /hunt [часы] — кого нашёл, в каком списке, $ в день, что дали копии после находки.
A9. Раз в неделю — отчёт: сколько найдено, сколько в плюсе после находки.
A10/D5. Бой — только список «в бой» после 24 ч тени на новых сделках (hunter:battle_ok), ставка 0.14 SOL, по кнопке.
C1. В пул меньше real_min_pool_sol (30 SOL) бот не входит — такие копии не в оценке.
C2. «Сливает на копировщиков»: доля его продаж в первые 30 с после всплеска чужих покупок (3+ покупателя за 10 с
    после него); выросла — алерт и стоп.

Поиск раз в час — только если владелец включил (/hunter on). Слежение за найденными (списки, «остыл», слив,
допуск к бою) работает всегда. Результаты охотника и копии после находки хранятся бессрочно (hunter_wallets,
hunter_copies). Исторический прогон: python -m scanner hunt --history [--from --to --step --wallet …].
"""
from __future__ import annotations

import asyncio
import logging
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from ..alerts import format as fmt
from . import copymodel as CM
from .paper import SCHEMES

log = logging.getLogger(__name__)

WINDOW_H = 72


@dataclass(frozen=True)
class Bar:
    """Планка «горячего» (25.11): 50+ купленных монет за 72 ч и t ≥ 3, в среднем от +10%, в плюсе от 60%."""
    min_coins: int = 50
    min_avg: float = 0.10
    min_win: float = 0.60
    max_noprice: float = 0.5
    min_t: float = 3.0


BAR = Bar()
RULES = ("A", "B", "G")             # правила выхода с удержанием до 30 мин (R держит сутки — цены нет)
ENTRY_DELAY_S = 1.36                 # медиана из боя: от его покупки до нашей
MAX_STAKE_SHARE = 0.03               # максимальная ставка — 3% SOL пула
MIN_POOL_SOL = 30.0                  # C1: в пул мельче не входим
BUNDLE_SHARE = 0.3                   # доля покупок в блоке создания монеты — от неё кошелёк считается бандлом
ACTIVE_S = 3600                      # «активен сейчас»: последняя покупка не старше часа
BATTLE_USD, BATTLE_N, WATCH_USD, ARCHIVE_MIN_N = 40.0, 50, 15.0, 20
COOL_MIN_N, COOL_T, COOL_IDLE_H = 10, -2.0, 12
SHADOW_BEFORE_LIVE_H, SHADOW_MIN_N = 24, 5
DUMP_BUYERS, DUMP_WIN_S, DUMP_SELL_S, DUMP_ALERT, DUMP_RISE = 3, 10.0, 30.0, 0.3, 0.1
STAKE_SOL = 0.14
MAX_WALLETS = 300
EVAL_LIMIT = 200
REALERT_COOLED_D = 7
TOPIC = "copy"
DAY_KEY, WEEK_KEY, BATTLE_KEY = "hunter:day_at", "hunter:week_at", "hunter:battle_ok"
LIST_NAMES = {"new": "найден", "battle": "в бой", "watch": "наблюдение", "archive": "архив"}


def t_stat(xs: list[float]) -> float:
    if not xs:
        return 0.0
    m = statistics.fmean(xs)
    sd = statistics.stdev(xs) if len(xs) > 1 else 0.0
    return m / (sd / len(xs) ** 0.5) if sd > 0 else (99.0 if m > 0 else (-99.0 if m < 0 else 0.0))


@dataclass
class Stats:
    address: str
    coins: int = 0                   # разных монет с ценой копии
    coins_all: int = 0               # разных монет, которые он купил (планка — по ним)
    n: int = 0                       # копий с ценой
    total: int = 0                   # копий в оценке (без «мелкий пул»)
    avg: float = 0.0
    win: float = 0.0
    noprice: float = 0.0
    hold_s: float | None = None      # медиана удержания у самого трейдера
    last_buy: float | None = None
    own: int = 0
    bundled: float = 0.0
    t: float = 0.0
    top: list[tuple[str, float]] = field(default_factory=list)
    rule: str = "A"                  # A2: наше лучшее правило на первой половине
    rule_train: float | None = None
    rule_test: float | None = None   # то же правило на второй половине
    per_day: float = 0.0             # копий в день (в оценке)
    usd_day: float = 0.0             # A3: при 0.14 SOL
    usd_day_max: float = 0.0         # A3: при 3% глубины
    burst: float = 0.0               # A4
    block2_avg: float | None = None  # A1: то же правило со входом «блок +2»
    dump: float = 0.0                # C2
    small_pool: int = 0              # копий мимо (пул < 30 SOL)

    def hot(self, bar: Bar | None = None) -> str | None:
        """None — горячий, иначе — почему нет (планка — BAR, если не задана)."""
        bar = bar or BAR
        if self.own:
            return "покупает свои монеты (дев)"
        if self.bundled >= BUNDLE_SHARE:
            return f"бандл: {self.bundled:.0%} покупок в блоке создания"
        if self.coins_all < bar.min_coins:
            return f"монет {self.coins_all} < {bar.min_coins}"
        if self.noprice > bar.max_noprice:
            return f"без цены {self.noprice:.0%}"
        if self.avg < bar.min_avg:
            return f"в среднем {self.avg:+.1%}"
        if self.win < bar.min_win:
            return f"в плюсе {self.win:.0%}"
        if self.t < bar.min_t:
            return f"t {self.t:.1f} < {bar.min_t:g}"
        if self.rule_test is not None and self.rule_test <= 0:
            return f"правило {self.rule} на второй половине {self.rule_test:+.1%}"
        if self.dump >= DUMP_ALERT:
            return f"сливает на копировщиков ({self.dump:.0%})"
        return None


def stats_of(address: str, copies: list[tuple[CM.Entry, CM.Copy]], trades: list[dict[str, Any]],
             symbols: dict[str, str] | None = None) -> Stats:
    s = Stats(address)
    s.small_pool = sum(1 for _e, c in copies if c.why == CM.SMALL_POOL)
    copies = [(e, c) for e, c in copies if c.why != CM.SMALL_POOL]
    s.total = len(copies)
    priced = [(e, c) for e, c in copies if c.res is not None]
    s.n = len(priced)
    s.coins = len({e.mint for e, _c in priced})
    s.coins_all = len({e.mint for e, _c in copies})
    if priced:
        res = [c.res for _e, c in priced]
        s.avg = statistics.fmean(res)
        s.win = sum(1 for x in res if x > 0) / len(res)
        s.t = t_stat(res)
        s.burst = sum(1 for _e, c in priced if c.burst) / len(priced)
    s.noprice = 1 - s.n / s.total if s.total else 0.0
    first_buy: dict[str, float | None] = {}
    holds = []
    for t in sorted(trades, key=lambda x: x["ts"]):
        if t["is_buy"]:
            first_buy.setdefault(t["mint"], t["ts"])
            s.last_buy = max(s.last_buy or 0.0, t["ts"])
        elif first_buy.get(t["mint"]) is not None:
            holds.append(t["ts"] - first_buy[t["mint"]])
            first_buy[t["mint"]] = None
    s.hold_s = statistics.median(holds) if holds else None
    best = sorted(priced, key=lambda x: -abs(x[1].res))[:6]
    s.top = [((symbols or {}).get(e.mint) or e.mint[:4], c.res) for e, c in best]
    return s


def pick_rule(by_rule: dict[str, list[tuple[CM.Entry, CM.Copy]]]) -> tuple[str, float | None, float | None]:
    """A2: правило с лучшим средним на первой половине входов (по времени) и его итог на второй."""
    any_rule = next(iter(by_rule.values()), [])
    ents = sorted({e.ts for e, _c in any_rule})
    if len(ents) < 4:
        return "A", None, None
    cut = ents[len(ents) // 2]
    best, best_train = "A", None
    for rule, copies in by_rule.items():
        tr = [c.res for e, c in copies if e.ts < cut and c.res is not None]
        if len(tr) >= 3 and (best_train is None or statistics.fmean(tr) > best_train):
            best, best_train = rule, statistics.fmean(tr)
    def half2(rule: str) -> float | None:
        xs = [c.res for e, c in by_rule[rule] if e.ts >= cut and c.res is not None]
        return statistics.fmean(xs) if xs else None
    test = half2(best)
    base = half2("A") if "A" in by_rule else None
    if best != "A" and base is not None and (test is None or test < base):
        # проверка на второй половине не прошла: выбранное правило там хуже A — остаёмся на A (основное правило)
        a_train = [c.res for e, c in by_rule["A"] if e.ts < cut and c.res is not None]
        return "A", (statistics.fmean(a_train) if a_train else None), base
    return best, best_train, test


def dump_share(p: CM.Prepared, trades: list[dict[str, Any]], since: float = 0.0) -> float:
    """C2: доля его монет, где после его покупки был всплеск чужих покупок (DUMP_BUYERS разных за DUMP_WIN_S), а он
    продал в первые DUMP_SELL_S после начала всплеска (продаёт в копировщиков)."""
    sells: dict[str, list[float]] = {}
    for d in trades:
        if not d["is_buy"]:
            sells.setdefault(d["mint"], []).append(d["ts"])
    bursts = dumps = 0
    for e in p.ents:
        if e.ts < since:
            continue
        buyers: dict[str, float] = {}
        for t in p.ticks.get(e.mint, []):
            if e.ts < t.ts <= e.ts + DUMP_WIN_S and t.is_buy and t.trader and t.trader != p.address:
                buyers.setdefault(t.trader, t.ts)
        if len(buyers) < DUMP_BUYERS:
            continue
        bursts += 1
        start = sorted(buyers.values())[DUMP_BUYERS - 1]
        if any(start <= s <= start + DUMP_SELL_S for s in sells.get(e.mint, [])):
            dumps += 1
    return dumps / bursts if bursts else 0.0


async def candidates(db: Any, since: float, until: float, min_coins: int | None = None,
                     limit: int = MAX_WALLETS) -> list[dict[str, Any]]:
    """Кошельки с покупками от min_coins разных монет за окно (по умолчанию — планка BAR); свои монеты и бандлы
    считаем тут же."""
    min_coins = BAR.min_coins if min_coins is None else min_coins
    rows = await db.fetch(
        """WITH b AS (SELECT trader, mint, min(slot) AS slot, min(ts) AS ts FROM trades
                      WHERE is_buy AND ts BETWEEN to_timestamp($1) AND to_timestamp($2) GROUP BY 1, 2)
           SELECT b.trader, count(*) AS coins, count(*) FILTER (WHERE k.deployer = b.trader) AS own,
                  count(*) FILTER (WHERE k.created_slot IS NOT NULL AND b.slot IS NOT NULL
                                   AND b.slot <= k.created_slot) AS bundled,
                  extract(epoch FROM max(b.ts)) AS last_buy
           FROM b LEFT JOIN tokens k ON k.mint = b.mint
           GROUP BY b.trader HAVING count(*) >= $3 ORDER BY count(*) DESC LIMIT $4""",
        since, until, int(min_coins), int(limit))
    return [dict(r) for r in rows]


@dataclass
class Params:
    delay: float = ENTRY_DELAY_S
    min_pool: float = MIN_POOL_SOL
    sol_usd: float = 150.0


async def params_of(db: Any, cfg: dict[str, Any] | None = None) -> Params:
    cfg = cfg or {}
    price = (await db.kv_get("sol_usd", {}) or {}).get("price") if hasattr(db, "kv_get") else None
    return Params(float(cfg.get("hunter_entry_delay_s", ENTRY_DELAY_S)),
                  float(cfg.get("real_min_pool_sol", MIN_POOL_SOL)), float(price or 150.0))


def assess(p: CM.Prepared, trades: list[dict[str, Any]], prm: Params, days: float,
           symbols: dict[str, str] | None = None, rules: tuple[str, ...] = RULES) -> tuple[Stats, dict[str, Any]]:
    """Оценка кошелька на входах p: правило (A2), итог, $ в день (A3), всплеск (A4), блок +2 (A1), слив (C2)."""
    by_rule = {r: CM.run(p, SCHEMES[r], entry_delay_s=prm.delay, min_pool_sol=prm.min_pool) for r in rules}
    rule, train, test = pick_rule(by_rule)
    copies = by_rule[rule]
    s = stats_of(p.address, copies, trades, symbols)
    s.rule, s.rule_train, s.rule_test = rule, train, test
    s.per_day = s.total / max(days, 1 / 24)
    s.usd_day = s.per_day * STAKE_SOL * s.avg * prm.sol_usd
    mx = [c for _e, c in CM.run(p, SCHEMES[rule], entry_delay_s=prm.delay, min_pool_sol=prm.min_pool,
                                stake_share=MAX_STAKE_SHARE) if c.res is not None]
    if mx:
        s.usd_day_max = s.per_day * statistics.fmean(c.stake for c in mx) * statistics.fmean(c.res for c in mx) \
            * prm.sol_usd
    b2 = [c.res for _e, c in CM.run(p, SCHEMES[rule], min_pool_sol=prm.min_pool) if c.res is not None]
    s.block2_avg = statistics.fmean(b2) if b2 else None
    s.dump = dump_share(p, trades)
    return s, {"by_rule": by_rule}


async def evaluate(db: Any, address: str, since: float, until: float, prm: Params, limit: int = EVAL_LIMIT,
                   cand: dict[str, Any] | None = None) -> Stats:
    trades = await CM.wallet_trades(db, address, since, until)
    p = await CM.prepare(db, address, trades, since, until, limit)
    syms = {r["mint"]: r["symbol"] for r in await db.fetch(
        "SELECT mint, symbol FROM tokens WHERE mint = ANY($1::text[])", list({e.mint for e in p.ents}))}
    s, _ = assess(p, trades, prm, (until - since) / 86400, syms)
    if cand:
        s.own = int(cand.get("own") or 0)
        s.bundled = int(cand.get("bundled") or 0) / max(1, int(cand.get("coins") or 1))
    return s


def ago(sec: float | None) -> str:
    return fmt.ago(sec) if sec is not None else "?"


def alert_text(s: Stats, now: float, prm: Params | None = None) -> tuple[str, list[list[dict[str, Any]]]]:
    prm = prm or Params()
    active = s.last_buy is not None and now - s.last_buy <= ACTIVE_S
    coins = ", ".join(f"${fmt.esc(sym)} {res:+.0%}" for sym, res in s.top)
    test = f"{s.rule_test:+.1%}" if s.rule_test is not None else "?"
    b2 = f"{s.block2_avg:+.1%}" if s.block2_avg is not None else "?"
    text = (f"🎯 <b>Охотник: горячий кошелёк</b> <code>{s.address}</code>\n"
            f"За {WINDOW_H} ч, наш бот за ним (вход через {prm.delay:g} с, правило <b>{s.rule}</b>, издержки тени): "
            f"<b>{s.coins_all} монет</b> ({s.coins} с ценой), в среднем <b>{s.avg:+.1%}</b>, в плюсе "
            f"<b>{s.win:.0%}</b>, t={s.t:.1f} ({s.n} копий, без цены {s.noprice:.0%})\n"
            f"Правило {s.rule}: на второй половине {test}; со входом «блок +2» {b2}\n"
            f"<b>${s.usd_day:,.0f} в день</b> при {STAKE_SOL:g} SOL · ${s.usd_day_max:,.0f} при 3% пула · "
            f"{s.per_day:.0f} сделок в день\n"
            f"Всплеск (+25% за 30 с): {s.burst:.0%} · слив на копировщиков: {s.dump:.0%}\n"
            f"Удержание у него: медиана {ago(s.hold_s)} · последняя покупка "
            f"{ago(now - s.last_buy) if s.last_buy else '?'} назад — {'🟢 активен' if active else '⚪ сейчас не торгует'}\n"
            f"Монеты: {coins}\n"
            f"Бой — только после 24 ч тени и списка «в бой» (от ${BATTLE_USD:g}/день на {BATTLE_N}+ новых сделках).")
    kb = [[{"text": "👻 Тень", "callback": f"hn:s:{s.address}"},
           {"text": f"💵 Бой {STAKE_SOL:g}", "callback": f"hn:l:{s.address}", "style": "danger"},
           {"text": "🙈 Игнор", "callback": f"hn:i:{s.address}"}]]
    return text, kb


def live_confirm(addr: str, cap_usd: float, rule: str = "A") -> tuple[str, list[list[dict[str, Any]]]]:
    text = (f"💵 <b>Бой за {addr[:4]}…{addr[-4:]}?</b>\nКаждая его покупка → покупка ботом на {STAKE_SOL:g} SOL "
            f"(не больше потолка сервера ${cap_usd:g}), выход — правило {rule}. Только у кошельков бота в режиме «бой»; "
            "действуют лимит позиций, лимит на кошелёк, дневной и общий стоп, kill switch. Остынет — бот снимет сам.")
    return text, [[{"text": "Да, в бой", "callback": f"hn:L:{addr}", "style": "danger"},
                   {"text": "Нет", "callback": f"hn:x:{addr}"}]]


# ---------------------------------------------------------------------- база
async def upsert(db: Any, s: Stats) -> None:
    await db.execute(
        """INSERT INTO hunter_wallets(address, found_at, status, coins, avg_pct, win, hold_s, n_copies, noprice,
               last_alert_at, list, rule, rule_train, rule_test, usd_day, usd_day_max, burst, block2_avg, dump_share,
               list_at)
           VALUES ($1, now(), 'new', $2, $3, $4, $5, $6, $7, now(), 'new', $8, $9, $10, $11, $12, $13, $14, $15, now())
           ON CONFLICT (address) DO UPDATE SET found_at = now(), status = 'new', coins = $2, avg_pct = $3, win = $4,
               hold_s = $5, n_copies = $6, noprice = $7, last_alert_at = now(), followed_at = NULL, cooled_at = NULL,
               cool_reason = NULL, list = 'new', rule = $8, rule_train = $9, rule_test = $10, usd_day = $11,
               usd_day_max = $12, burst = $13, block2_avg = $14, dump_share = $15, list_at = now(),
               list_reason = NULL, n_after = NULL, avg_after = NULL, usd_day_after = NULL""",
        s.address, s.coins_all, s.avg, s.win, s.hold_s, s.n, s.noprice, s.rule, s.rule_train, s.rule_test, s.usd_day,
        s.usd_day_max, s.burst, s.block2_avg, s.dump)


async def follow(db: Any, cfg: dict[str, Any], costs: Any, addr: str, mode: str | None) -> None:
    """«Тень» / «Бой 0.14» / снять: кошелёк — в свой список (его покупки слушаем), «за ним» по его правилу."""
    from ..bot import board
    from .copytrade import compute_trader, linked_to_dev, qualifies, save_trader
    if mode:
        if not await db.fetchval("SELECT manual FROM traders WHERE address=$1", addr):
            s7, s30, _ = await compute_trader(db, addr, cfg, costs)
            linked = await linked_to_dev(db, addr)
            ok, _f = qualifies(s7, s30, linked, cfg)
            await save_trader(db, addr, s7, s30, ok, linked, label="охотник", manual=True)
        await db.execute("UPDATE traders SET active=true, manual=true WHERE address=$1", addr)
        rule = await db.fetchval("SELECT rule FROM hunter_wallets WHERE address = $1", addr)
        await board.set_follow_rule(db, addr, rule if rule in ("A", "B", "G", "R") else "A", None)
    await board.set_follow(db, addr, mode)
    stakes = dict(await db.kv_get("real:follow_stake") or {})
    if mode == "live":
        stakes[addr] = STAKE_SOL
    else:
        stakes.pop(addr, None)
    await db.kv_set("real:follow_stake", stakes or None)
    if mode:
        # время начала тени не сбрасываем при переходе в бой (24 ч тени уже отсчитаны)
        await db.execute("UPDATE hunter_wallets SET status = $2, followed_at = COALESCE(followed_at, now()) "
                         "WHERE address = $1", addr, mode)
    await db.job("reload")


async def battle_ready(db: Any, addr: str, now: float | None = None) -> str | None:
    """None — можно в бой; иначе — почему нельзя (A10/D5)."""
    now = now or time.time()
    r = await db.fetchrow("SELECT list, status, followed_at FROM hunter_wallets WHERE address = $1", addr)
    if r is None:
        return "кошелёк не из охотника"
    if r["list"] != "battle":
        return f"не в списке «в бой» (сейчас: {LIST_NAMES.get(r['list'], r['list'])})"
    if r["status"] not in ("shadow", "live") or r["followed_at"] is None:
        return "сначала 24 ч тени"
    if now - r["followed_at"].timestamp() < SHADOW_BEFORE_LIVE_H * 3600:
        left = SHADOW_BEFORE_LIVE_H * 3600 - (now - r["followed_at"].timestamp())
        return f"тень идёт меньше {SHADOW_BEFORE_LIVE_H} ч (осталось {ago(left)})"
    res = await our_results(db, addr, r["followed_at"])
    if res["n"] < SHADOW_MIN_N:
        return f"в тени за ним {res['n']} сделок (нужно от {SHADOW_MIN_N})"
    if res["avg"] < 0:
        return f"в тени за ним в минусе ({res['avg']:+.1%})"
    return None


async def our_results(db: Any, addr: str, since: datetime, until: datetime | None = None) -> dict[str, Any]:
    """Наши сделки «за ним» (тень и бой) с момента, как начали повторять."""
    rows = await db.fetch(
        """SELECT pnl_pct, pnl_sol, extract(epoch FROM opened_at) AS ts FROM real_positions
           WHERE trader = $1 AND opened_at >= $2 AND ($3::timestamptz IS NULL OR opened_at < $3)
             AND closed_at IS NOT NULL AND pnl_pct IS NOT NULL ORDER BY opened_at""", addr, since, until)
    pct = [float(r["pnl_pct"]) for r in rows]
    return {"n": len(pct), "avg": statistics.fmean(pct) if pct else 0.0, "t": t_stat(pct),
            "sol": sum(float(r["pnl_sol"] or 0) for r in rows), "win": sum(1 for x in pct if x > 0),
            "last": float(rows[-1]["ts"]) if rows else None}


def cool_reason(n: int, t: float, last_buy: float | None, since: float, now: float) -> str | None:
    """A6: «остыл» — t ≤ −2 на 10+ наших сделках с начала повтора, или у него нет покупок 12 ч."""
    if n >= COOL_MIN_N and t <= COOL_T:
        return f"{n} сделок за ним, устойчивый минус (t={t:.1f})"
    if now - max(last_buy or 0.0, since) >= COOL_IDLE_H * 3600:
        return f"нет покупок {COOL_IDLE_H} ч"
    return None


def list_for(n_after: int, usd_after: float) -> tuple[str, str | None]:
    """A5: список по новым сделкам после находки."""
    if n_after >= BATTLE_N and usd_after >= BATTLE_USD:
        return "battle", None
    if usd_after >= WATCH_USD:
        return "watch", None
    if n_after >= ARCHIVE_MIN_N:
        return "archive", f"после находки ${usd_after:,.0f}/день < ${WATCH_USD:g} ({n_after} копий)"
    return "new", None


class Hunter:
    def __init__(self, engine: Any):
        self.e = engine
        self.stats: dict[str, Any] = {}

    async def say(self, text: str, kb: list[list[dict[str, Any]]] | None = None) -> None:
        await self.e.db.enqueue("text", {"topic": TOPIC, "text": text, "keyboard": kb}, priority=5)

    async def loop(self) -> None:
        await asyncio.sleep(120)
        while True:
            try:
                await self.tick(time.time())
            except Exception:  # noqa: BLE001
                log.exception("охотник")
            await asyncio.sleep(3600)

    async def tick(self, now: float) -> None:
        prm = await params_of(self.e.db, self.e.cfg)
        if self.e.cfg.get("hunter_on"):
            await self.scan(now, prm)
        await self.track(now, prm)
        await self.watch(now)
        await self.battle_list(now)
        await self.daily(now)
        await self.weekly(now)

    # ------------------------------------------------------------------ поиск (A1–A4, A7)
    async def scan(self, now: float, prm: Params | None = None) -> list[Stats]:
        db = self.e.db
        prm = prm or await params_of(db, self.e.cfg)
        since = now - WINDOW_H * 3600
        known = {r["address"]: r for r in await db.fetch("SELECT address, status, list, cooled_at FROM hunter_wallets")}
        hot = []
        cands = await candidates(db, since, now, BAR.min_coins)
        for c in cands:
            k = known.get(c["trader"])
            if k is not None and (k["status"] in ("new", "shadow", "live", "ignored") or k["list"] in ("battle", "watch")
                                  or (k["cooled_at"] is not None and k["cooled_at"] > datetime.now(timezone.utc)
                                      - timedelta(days=REALERT_COOLED_D))):
                continue
            if c["own"] or int(c["bundled"]) / max(1, int(c["coins"])) >= BUNDLE_SHARE:
                continue
            s = await evaluate(db, c["trader"], since, now, prm, cand=c)
            if s.hot() is None:
                hot.append(s)
                await upsert(db, s)
                await self.say(*alert_text(s, now, prm))
        self.stats = {"at": now, "candidates": len(cands), "hot": len(hot)}
        return hot

    # ------------------------------------------------------------------ после находки (A5, C2, B4)
    async def track(self, now: float, prm: Params | None = None) -> None:
        db = self.e.db
        prm = prm or await params_of(db, self.e.cfg)
        rows = await db.fetch("SELECT * FROM hunter_wallets WHERE list <> 'archive' AND status <> 'ignored'")
        for r in rows:
            addr, start = r["address"], r["found_at"].timestamp()
            trades = await CM.wallet_trades(db, addr, start - 120, now)
            p = await CM.prepare(db, addr, trades, start, now, limit=100_000)
            rule = r["rule"] if r["rule"] in SCHEMES else "A"
            copies = CM.run(p, SCHEMES[rule], entry_delay_s=prm.delay, min_pool_sol=prm.min_pool)
            await save_copies(db, addr, rule, copies)
            done = [(e, c) for e, c in copies if c.why != CM.SMALL_POOL]
            res = [c.res for _e, c in done if c.res is not None]
            end = r["cooled_at"].timestamp() if r["cooled_at"] is not None else now
            days = max((end - start) / 86400, 1 / 24)
            avg = statistics.fmean(res) if res else 0.0
            usd = len(done) / days * STAKE_SOL * avg * prm.sol_usd
            new_list, reason = list_for(len(res), usd)
            dump = dump_share(p, trades)
            if dump >= DUMP_ALERT and dump >= float(r["dump_share"] or 0) + DUMP_RISE:
                new_list, reason = "archive", f"сливает на копировщиков: {dump:.0%} продаж в первые 30 с всплеска"
                if r["status"] in ("shadow", "live"):
                    await follow(db, self.e.cfg, self.e.costs(), addr, None)
                    await db.execute("UPDATE hunter_wallets SET status = 'cooled', cooled_at = now(), cool_reason = $2 "
                                     "WHERE address = $1", addr, reason)
                await self.say(f"🚨 <b>Сливает на копировщиков</b> <code>{addr}</code>: {reason}. Копирование "
                               "остановлено, кошелёк — в архив.")
            await db.execute(
                """UPDATE hunter_wallets SET n_after = $2, avg_after = $3, usd_day_after = $4,
                       list = $5, list_reason = $6, list_at = CASE WHEN list <> $5 THEN now() ELSE list_at END
                   WHERE address = $1""", addr, len(res), avg, usd, new_list, reason)
            if new_list == "battle" and r["list"] != "battle":
                await self.say(f"🎯 <code>{addr}</code> → список <b>«{LIST_NAMES[new_list]}»</b>: после находки "
                               f"{len(res)} копий, в среднем {avg:+.1%}, ${usd:,.0f}/день при {STAKE_SOL:g} SOL. "
                               "В бой — после 24 ч тени, кнопкой.")
            elif new_list == "archive" and r["list"] != "archive" and reason and "сливает" not in reason:
                await self.say(f"🗄 <code>{addr}</code> → архив: {reason}.")

    # ------------------------------------------------------------------ остыл (A6)
    async def watch(self, now: float) -> list[str]:
        db = self.e.db
        cooled = []
        for r in await db.fetch("SELECT address, status, followed_at FROM hunter_wallets "
                                "WHERE status IN ('shadow', 'live') AND followed_at IS NOT NULL"):
            addr = r["address"]
            res = await our_results(db, addr, r["followed_at"])
            last_buy = await db.fetchval("SELECT extract(epoch FROM max(ts)) FROM trades WHERE trader = $1 AND is_buy",
                                         addr)
            why = cool_reason(res["n"], res["t"], float(last_buy) if last_buy else None,
                              r["followed_at"].timestamp(), now)
            if why is None:
                continue
            await follow(db, self.e.cfg, self.e.costs(), addr, None)
            await db.execute("UPDATE hunter_wallets SET status = 'cooled', cooled_at = now(), cool_reason = $2 "
                             "WHERE address = $1", addr, why)
            await self.say(f"🧊 <b>Остыл</b> <code>{addr}</code>: {why} — копирование "
                           f"({'бой' if r['status'] == 'live' else 'тень'}) остановлено.\nЗа всё время за ним: "
                           f"{res['n']} сд, в среднем {res['avg']:+.1%}, {res['sol']:+.3f} SOL.")
            cooled.append(addr)
        return cooled

    # ------------------------------------------------------------------ допуск к бою (A10, D5)
    async def battle_list(self, now: float) -> dict[str, float]:
        db = self.e.db
        old = dict(await db.kv_get(BATTLE_KEY) or {})
        ok: dict[str, float] = {}
        for r in await db.fetch("SELECT address FROM hunter_wallets WHERE list = 'battle' AND status IN ('shadow', 'live')"):
            if await battle_ready(db, r["address"], now) is None:
                ok[r["address"]] = old.get(r["address"], now)
        for addr in set(old) - set(ok):
            live = await db.fetchval("SELECT status FROM hunter_wallets WHERE address = $1", addr) == "live"
            if live:
                await self.say(f"⚠️ <code>{addr}</code> больше не в допуске к бою ({await battle_ready(db, addr, now)}) — "
                               "бот за ним только в тени.")
        await db.kv_set(BATTLE_KEY, ok or None)
        if ok != old:
            await db.job("reload")
        return ok

    async def daily(self, now: float) -> None:
        db = self.e.db
        if now - float(await db.kv_get(DAY_KEY) or 0) < 86400:
            return
        rows = await db.fetch("SELECT address, status, followed_at FROM hunter_wallets "
                              "WHERE status IN ('shadow', 'live') AND followed_at IS NOT NULL")
        await db.kv_set(DAY_KEY, now)
        if not rows:
            return
        lines = ["🎯 <b>Охотник: итог за 24 ч</b> по кошелькам, за которыми повторяем"]
        for r in rows:
            d = await our_results(db, r["address"], max(r["followed_at"],
                                                        datetime.fromtimestamp(now - 86400, timezone.utc)))
            lines.append(f"{'💵' if r['status'] == 'live' else '👻'} <code>{r['address'][:4]}…{r['address'][-4:]}</code>: "
                         f"{d['n']} сд, в среднем {d['avg']:+.1%}, {d['sol']:+.3f} SOL")
        await self.say("\n".join(lines))

    async def weekly(self, now: float) -> str | None:
        db = self.e.db
        if now - float(await db.kv_get(WEEK_KEY) or 0) < 7 * 86400:
            return None
        await db.kv_set(WEEK_KEY, now)
        text = await weekly_text(db, now)
        await self.say(text)
        return text


async def save_copies(db: Any, addr: str, rule: str, copies: list[tuple[CM.Entry, CM.Copy]]) -> None:
    """B4: копии после находки — бессрочно (hunter_copies)."""
    rows = [(addr, e.mint, datetime.fromtimestamp(e.ts, timezone.utc), rule, c.res, c.why or c.reason, c.entry_mc,
             c.exit_mc, c.burst, c.stake) for e, c in copies]
    if rows:
        await db.p.executemany(
            """INSERT INTO hunter_copies(address, mint, entry_at, rule, res, reason, entry_mc, exit_mc, burst, stake)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
               ON CONFLICT (address, mint, entry_at) DO UPDATE SET rule = EXCLUDED.rule, res = EXCLUDED.res,
                   reason = EXCLUDED.reason, entry_mc = EXCLUDED.entry_mc, exit_mc = EXCLUDED.exit_mc,
                   burst = EXCLUDED.burst, stake = EXCLUDED.stake""", rows)


async def weekly_text(db: Any, now: float) -> str:
    since = datetime.fromtimestamp(now - 7 * 86400, timezone.utc)
    rows = await db.fetch("SELECT * FROM hunter_wallets WHERE found_at >= $1 OR followed_at >= $1 ORDER BY found_at",
                          since)
    found = [r for r in rows if r["found_at"] >= since]
    by: dict[str, int] = {}
    for r in found:
        by[r["list"]] = by.get(r["list"], 0) + 1
    lines = [f"🎯 <b>Охотник, неделя</b>: найдено горячих {len(found)}"
             + (" · " + ", ".join(f"{LIST_NAMES.get(k, k)} {v}" for k, v in by.items()) if by else "")]
    after = [r for r in found if r["n_after"]]
    if after:
        plus = sum(1 for r in after if float(r["avg_after"] or 0) > 0)
        lines.append(f"После находки (модель, новые сделки): в плюсе {plus} из {len(after)} · "
                     f"всего {sum(int(r['n_after']) for r in after)} копий")
    ours = []
    for r in rows:
        if r["followed_at"] is None:
            continue
        res = await our_results(db, r["address"], r["followed_at"], r["cooled_at"])
        if res["n"]:
            ours.append(res)
    if ours:
        lines.append(f"За ними после начала повтора (наши сделки): в плюсе {sum(1 for x in ours if x['sol'] > 0)} из "
                     f"{len(ours)} кошельков · {sum(x['n'] for x in ours)} сд, {sum(x['sol'] for x in ours):+.3f} SOL")
    else:
        lines.append("За найденными ещё не повторяли (ни тени, ни боя).")
    return "\n".join(lines)


async def hunt_text(db: Any, hours: float = 24.0) -> str:
    """A8: кого нашёл за hours часов и все из списков «в бой» и «наблюдение»."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = await db.fetch(
        """SELECT * FROM hunter_wallets WHERE found_at >= $1 OR list IN ('battle', 'watch')
           ORDER BY (list = 'battle') DESC, (list = 'watch') DESC, usd_day_after DESC NULLS LAST, found_at DESC
           LIMIT 40""", since)
    ok = set(await db.kv_get(BATTLE_KEY) or {})
    if not rows:
        return f"🎯 Охотник: за {hours:g} ч никого не нашёл, в списках пусто."
    status = {"new": "без решения", "shadow": "👻 тень", "live": "💵 бой", "ignored": "игнор", "cooled": "🧊 остыл"}
    lines = [f"🎯 <b>Охотник</b>: найдены за {hours:g} ч и списки"]
    for r in rows:
        after = (f"после находки {int(r['n_after'] or 0)} коп., {float(r['avg_after'] or 0):+.1%}, "
                 f"${float(r['usd_day_after'] or 0):,.0f}/день" if r["n_after"] else "после находки копий ещё нет")
        lines.append(f"<b>{LIST_NAMES.get(r['list'], r['list'])}</b> · <code>{r['address']}</code> · "
                     f"{status.get(r['status'], r['status'])}{' · ✅ допуск к бою' if r['address'] in ok else ''}\n"
                     f"  найден {r['found_at']:%d.%m %H:%M} UTC: ${float(r['usd_day'] or 0):,.0f}/день, правило "
                     f"{r['rule'] or 'A'}, всплеск {float(r['burst'] or 0):.0%} · {after}"
                     + (f" · {r['list_reason']}" if r["list_reason"] else "")
                     + (f" · остыл: {r['cool_reason']}" if r["cool_reason"] else ""))
    return "\n".join(lines)


# ---------------------------------------------------------------------- исторический прогон
async def history(db: Any, days: float = 7.0, step_h: float = 6.0, max_wallets: int = 500,
                  until: float | None = None, bar: Bar | None = None, trace: str | None = None,
                  start: float | None = None, prm: Params | None = None, rules: tuple[str, ...] = RULES) -> str:
    """Как охотник сработал бы на прошлых данных: в каждой точке (шаг step_h) — горячие за 72 ч до неё (правило — на
    первой половине окна, проверка — на второй); первый раз найденный ведём вперёд по модели до «остыл» (t ≤ −2 на
    10+ копиях после находки или 12 ч без покупок) или до конца данных; список — по $ в день после находки."""
    bar = bar or BAR
    prm = prm or await params_of(db)
    until = until or time.time()
    start = start if start is not None else until - days * 86400
    lo = start - WINDOW_H * 3600
    cands = await candidates(db, lo, until, bar.min_coins, max_wallets)
    cands = [c for c in cands if not c["own"] and int(c["bundled"]) / max(1, int(c["coins"])) < BUNDLE_SHARE]
    n_cands = len(cands)
    if trace and not any(c["trader"] == trace for c in cands):
        cands.append({"trader": trace, "coins": 0, "own": 0, "bundled": 0})
    found: list[dict[str, Any]] = []
    tr_lines: list[str] = []
    for c in cands:
        trades = await CM.wallet_trades(db, c["trader"], lo, until)
        p = await CM.prepare(db, c["trader"], trades, lo, until, limit=100_000)
        if not p.ents:
            if c["trader"] == trace:
                tr_lines.append("  копий нет: в базе нет его покупок за период")
            continue
        by_rule = {r: CM.run(p, SCHEMES[r], entry_delay_s=prm.delay, min_pool_sol=prm.min_pool) for r in rules}
        t = start
        while t <= until:
            win = {r: [(e, x) for e, x in cs if t - WINDOW_H * 3600 <= e.ts < t] for r, cs in by_rule.items()}
            rule, train, test = pick_rule(win)
            wtr = [d for d in trades if t - WINDOW_H * 3600 <= d["ts"] < t]
            s = stats_of(c["trader"], win[rule], wtr)
            s.rule, s.rule_train, s.rule_test = rule, train, test
            s.per_day = s.total / (WINDOW_H / 24)
            s.usd_day = s.per_day * STAKE_SOL * s.avg * prm.sol_usd
            why = s.hot(bar)
            if c["trader"] == trace and s.total:
                half = f" (2-я половина {test:+.1%})" if test is not None else ""
                tr_lines.append(f"  {datetime.fromtimestamp(t, timezone.utc):%d.%m %H:%M}: {s.coins_all} мон. "
                                f"({s.coins} с ценой), {s.n}/{s.total} коп., правило {rule} {s.avg:+.1%}{half}, "
                                f"в плюсе {s.win:.0%}, t={s.t:.1f}, без цены {s.noprice:.0%} → "
                                f"{'🔥 ГОРЯЧИЙ' if why is None else why}")
            if why is None:
                fwd = forward(by_rule[rule], [d for d in trades if d["ts"] >= t], t, until)
                b2 = [x.res for e, x in CM.run(p, SCHEMES[rule], min_pool_sol=prm.min_pool)
                      if e.ts >= t and x.res is not None and (fwd["end"] is None or e.ts <= fwd["end"])]
                fdays = max(((fwd["end"] or until) - t) / 86400, 1 / 24)
                usd = (fwd["n"] + fwd["noprice"]) / fdays * STAKE_SOL * (statistics.fmean(fwd["res"]) if fwd["res"]
                                                                          else 0.0) * prm.sol_usd
                lst, reason = list_for(fwd["n"], usd)
                if lst == "new":
                    lst, reason = "archive", f"мало копий после находки ({fwd['n']})"
                found.append({"address": c["trader"], "at": t, "stats": s, **fwd, "usd": usd, "list": lst,
                              "reason": reason, "b2": b2})
                break
            t += step_h * 3600
    by_list: dict[str, list[dict[str, Any]]] = {}
    for f in found:
        by_list.setdefault(f["list"], []).append(f)
    lines = [f"Исторический прогон охотника: {datetime.fromtimestamp(start, timezone.utc):%d.%m %H:%M} – "
             f"{datetime.fromtimestamp(until, timezone.utc):%d.%m %H:%M} UTC, шаг {step_h:g} ч, вход через "
             f"{prm.delay:g} с, правила {'/'.join(rules)} (выбор на 1-й половине окна), пул от {prm.min_pool:g} SOL; "
             f"планка: {bar.min_coins}+ монет, ≥ {bar.min_avg:+.0%}, в плюсе ≥ {bar.min_win:.0%}, t ≥ {bar.min_t:g}; "
             f"кошельков с {bar.min_coins}+ монетами (не дев, не бандл): {n_cands}",
             f"Горячих нашлось бы: {len(found)} · в бой {len(by_list.get('battle', []))}, наблюдение "
             f"{len(by_list.get('watch', []))}, архив {len(by_list.get('archive', []))}"]
    with_fwd = [f for f in found if f["n"]]
    if with_fwd:
        allres = [r for f in with_fwd for r in f["res"]]
        lines.append(f"После находки (модель, пока не остыл): копий {len(allres)}, в среднем "
                     f"{statistics.fmean(allres):+.1%}, в плюсе {sum(1 for x in allres if x > 0) / len(allres):.0%}; "
                     f"кошельков в плюсе {sum(1 for f in with_fwd if f['sum'] > 0)} из {len(with_fwd)}; итог "
                     f"{sum(f['sum'] for f in with_fwd) * STAKE_SOL:+.3f} SOL при {STAKE_SOL:g} SOL на копию; "
                     f"без цены {sum(f['noprice'] for f in with_fwd)} копий")
        for f in sorted(with_fwd, key=lambda x: -x["usd"])[:20]:
            s = f["stats"]
            lines.append(f"  {LIST_NAMES[f['list']]:<11} {f['address'][:4]}…{f['address'][-4:]} найден "
                         f"{datetime.fromtimestamp(f['at'], timezone.utc):%d.%m %H:%M} ({s.coins_all} мон., {s.rule} "
                         f"{s.avg:+.0%}, t={s.t:.1f}) → {f['n']} коп., {statistics.fmean(f['res']):+.1%}, "
                         f"${f['usd']:,.0f}/день" + (f" · остыл: {f['cooled']}" if f["cooled"] else ""))
    elif found:
        lines.append("После находки копий с ценой нет — данных после находки не хватает.")
    if trace:
        f = next((x for x in found if x["address"] == trace), None)
        lines += ["", f"Кошелёк {trace}:"] + (tr_lines or ["  за период не было окна с его копиями"])
        if f:
            res = f["res"]
            lines.append(f"  Найден {datetime.fromtimestamp(f['at'], timezone.utc):%d.%m %H:%M}, правило "
                         f"{f['stats'].rule}; после находки: {len(res)} копий с ценой"
                         + (f", в среднем {statistics.fmean(res):+.1%}, в плюсе "
                            f"{sum(1 for x in res if x > 0) / len(res):.0%}" if res else "")
                         + f", без цены {f['noprice']}, ${f['usd']:,.0f}/день → {LIST_NAMES[f['list']]}"
                         + (f" · остыл: {f['cooled']}" if f["cooled"] else ""))
            if f["b2"]:
                lines.append(f"  Для сравнения вход «блок +2» на тех же: {len(f['b2'])} копий, "
                             f"{statistics.fmean(f['b2']):+.1%}")
        else:
            lines.append("  Не найден.")
    return "\n".join(lines)


def forward(copies: list[tuple[CM.Entry, CM.Copy]], trades: list[dict[str, Any]], t0: float,
            until: float) -> dict[str, Any]:
    """Копии после находки t0 — пока не «остыл» (t ≤ −2 на 10+ копиях после находки или 12 ч без покупок)."""
    res: list[float] = []
    noprice = 0
    cooled = None
    end = None
    buys = sorted(d["ts"] for d in trades if d["is_buy"] and d["ts"] >= t0)
    last = t0
    for e, c in sorted(((e, c) for e, c in copies if e.ts >= t0 and c.why != CM.SMALL_POOL), key=lambda x: x[0].ts):
        if e.ts - last >= COOL_IDLE_H * 3600:
            cooled, end = f"нет покупок {COOL_IDLE_H} ч", last + COOL_IDLE_H * 3600
            break
        if len(res) >= COOL_MIN_N and t_stat(res) <= COOL_T:
            cooled, end = f"устойчивый минус на {len(res)} копиях (t={t_stat(res):.1f})", e.ts
            break
        last = e.ts
        if c.res is None:
            noprice += 1
            continue
        res.append(c.res)
    else:
        if (buys and until - buys[-1] >= COOL_IDLE_H * 3600) or (not buys and until - t0 >= COOL_IDLE_H * 3600):
            cooled = f"нет покупок {COOL_IDLE_H} ч"
            end = (buys[-1] if buys else t0) + COOL_IDLE_H * 3600
    return {"n": len(res), "res": res, "sum": sum(res), "noprice": noprice, "cooled": cooled, "end": end}
