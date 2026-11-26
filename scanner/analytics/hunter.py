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

27.11 (владелец): мягкая планка — 15+ монет за 72 ч (≥ +10%, в плюсе ≥ 60%, без требования к t; hunter_min_coins /
hunter_min_t в config.local.yaml). Всех найденных — сразу в тень и список «наблюдение» (без денег); шум отсеивает тень, а
не планка: архив (и стоп тени) — 20+ копий после находки и меньше $15/день, «остыл» — как раньше. В «в бой» — по итогу
после находки (от $40/день на 50+ копиях); когда допуск к бою готов (24 ч тени), бот присылает кнопку «Бой 0.14».

Поиск раз в час — только если владелец включил (/hunter on). Слежение за найденными (списки, «остыл», слив,
допуск к бою) работает всегда. Результаты охотника и копии после находки хранятся бессрочно (hunter_wallets,
hunter_copies). Исторический прогон: python -m scanner hunt --history [--from --to --step --wallet …].
"""
from __future__ import annotations

import asyncio
import logging
import re
import statistics
import time
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from ..alerts import format as fmt
from . import copymodel as CM
from . import farm as FARM
from .paper import SCHEMES

log = logging.getLogger(__name__)

WINDOW_H = 72


@dataclass(frozen=True)
class Bar:
    """Планка «горячего». 27.11 (владелец): мягкая — 15+ купленных монет за 72 ч, в среднем от +10%, в плюсе от 60%,
    t не требуется: найденные идут только в тень и «наблюдение», шум отсеивает тень. Строгая (25.11) — STRICT_BAR."""
    min_coins: int = 15
    min_avg: float = 0.10
    min_win: float = 0.60
    max_noprice: float = 0.5
    min_t: float = 0.0


BAR = Bar()
STRICT_BAR = Bar(min_coins=50, min_t=3.0)      # 25.11: 50+ монет и t ≥ 3 — находит на сутки позже (контроль 7BNaxx)


def bar_of(cfg: dict[str, Any] | None = None) -> Bar:
    """Планка из настроек (hunter_min_coins, hunter_min_t), остальное — BAR."""
    cfg = cfg or {}
    return replace(BAR, min_coins=int(cfg.get("hunter_min_coins", BAR.min_coins)),
                   min_t=float(cfg.get("hunter_min_t", BAR.min_t)))

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
BOT_TRADES_DAY, BOT_LOW_BALANCE = 100, 0.05   # 27.11: метка «бот» (не отсев — решает модель копирования)
STAKE_SOL = 0.14
MAX_WALLETS = 600                    # мягкая планка — кандидатов больше
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
    trades_day: float = 0.0          # 27.11: его сделок (покупки и продажи) в день за окно — метка «бот»
    balance: float | None = None     # 27.11: баланс SOL при находке (RPC)

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
                     limit: int = MAX_WALLETS, min_sol: float = 0.0) -> list[dict[str, Any]]:
    """Кошельки с покупками от min_coins разных монет за окно (по умолчанию — планка BAR); свои монеты и бандлы
    считаем тут же. 27.11 (Fn9n…: найден по «покупкам 0.00 SOL, MC 0.0» из pumpdata): мусор pumpdata и покупки
    мельче min_sol (бот их не копирует) — не в счёт, иначе пылевые боты с тысячами монет занимают места в лимите."""
    min_coins = BAR.min_coins if min_coins is None else min_coins
    rows = await db.fetch(
        """WITH s AS (SELECT trader, mint, slot, ts FROM trades
                      WHERE is_buy AND ts BETWEEN to_timestamp($1) AND to_timestamp($2) AND sol_amount >= $5
                            AND NOT (source = 'pumpdata' AND (sol_amount < 0.001 OR mc_sol < 1
                                     OR (pool = 'pump' AND mc_sol NOT BETWEEN 20 AND 600)))
                      UNION ALL                       -- 27.11: весь рынок из pumpdata.fun
                      SELECT trader, mint, slot, ts FROM market_trades
                      WHERE is_buy AND ts BETWEEN to_timestamp($1) AND to_timestamp($2) AND sol_amount >= $5
                            AND sol_amount >= 0.001 AND mc_sol >= 1 AND (pool <> 'pump' OR mc_sol BETWEEN 20 AND 600)),
                b AS (SELECT trader, mint, min(slot) AS slot, min(ts) AS ts FROM s GROUP BY 1, 2)
           SELECT b.trader, count(*) AS coins, count(*) FILTER (WHERE k.deployer = b.trader) AS own,
                  count(*) FILTER (WHERE k.created_slot IS NOT NULL AND b.slot IS NOT NULL
                                   AND b.slot <= k.created_slot) AS bundled,
                  extract(epoch FROM max(b.ts)) AS last_buy
           FROM b LEFT JOIN tokens k ON k.mint = b.mint
           GROUP BY b.trader HAVING count(*) >= $3 ORDER BY count(*) DESC LIMIT $4""",
        since, until, int(min_coins), int(limit), float(min_sol))
    return [dict(r) for r in rows]


@dataclass
class Params:
    delay: float = ENTRY_DELAY_S
    min_pool: float = MIN_POOL_SOL
    sol_usd: float = 150.0
    exit_delay: float = CM.EXIT_DELAY_S   # 27.11: продажа «садится» через столько секунд (hunter_exit_delay_s)
    max_stake: float = 0.5               # 27.11: ставка «при 3% пула» — не больше лимита на кошелёк (real_follow_max_sol)
    copy_share: float = 0.10             # и не больше доли его покупки (real_copy_max_share)
    min_buy: float = 0.1                 # 27.11: его покупки мельче бот не копирует (copy_min_buy_sol) — и модель


async def params_of(db: Any, cfg: dict[str, Any] | None = None) -> Params:
    cfg = cfg or {}
    price = (await db.kv_get("sol_usd", {}) or {}).get("price") if hasattr(db, "kv_get") else None
    return Params(float(cfg.get("hunter_entry_delay_s", ENTRY_DELAY_S)),
                  float(cfg.get("real_min_pool_sol", MIN_POOL_SOL)), float(price or 150.0),
                  float(cfg.get("hunter_exit_delay_s", CM.EXIT_DELAY_S)), float(cfg.get("real_follow_max_sol", 0.5)),
                  float(cfg.get("real_copy_max_share", 0.10)), float(cfg.get("copy_min_buy_sol", 0.1)))


def assess(p: CM.Prepared, trades: list[dict[str, Any]], prm: Params, days: float,
           symbols: dict[str, str] | None = None, rules: tuple[str, ...] = RULES) -> tuple[Stats, dict[str, Any]]:
    """Оценка кошелька на входах p: правило (A2), итог, $ в день (A3), всплеск (A4), блок +2 (A1), слив (C2)."""
    by_rule = {r: CM.run(p, SCHEMES[r], exit_delay_s=prm.exit_delay, entry_delay_s=prm.delay, min_pool_sol=prm.min_pool) for r in rules}
    rule, train, test = pick_rule(by_rule)
    copies = by_rule[rule]
    s = stats_of(p.address, copies, trades, symbols)
    s.rule, s.rule_train, s.rule_test = rule, train, test
    s.per_day = s.total / max(days, 1 / 24)
    s.trades_day = len(trades) / max(days, 1 / 24)
    s.usd_day = s.per_day * STAKE_SOL * s.avg * prm.sol_usd
    mx = [c for _e, c in CM.run(p, SCHEMES[rule], exit_delay_s=prm.exit_delay, entry_delay_s=prm.delay, min_pool_sol=prm.min_pool,
                                stake_share=MAX_STAKE_SHARE, stake_cap=prm.max_stake, his_share=prm.copy_share)
          if c.res is not None]
    if mx:
        s.usd_day_max = s.per_day * statistics.fmean(c.stake for c in mx) * statistics.fmean(c.res for c in mx) \
            * prm.sol_usd
    b2 = [c.res for _e, c in CM.run(p, SCHEMES[rule], exit_delay_s=prm.exit_delay, min_pool_sol=prm.min_pool) if c.res is not None]
    s.block2_avg = statistics.fmean(b2) if b2 else None
    s.dump = dump_share(p, trades)
    return s, {"by_rule": by_rule}


async def evaluate(db: Any, address: str, since: float, until: float, prm: Params, limit: int = EVAL_LIMIT,
                   cand: dict[str, Any] | None = None) -> Stats:
    trades = await CM.wallet_trades(db, address, since, until)
    p = await CM.prepare(db, address, trades, since, until, limit, min_buy=prm.min_buy)
    syms = {r["mint"]: r["symbol"] for r in await db.fetch(
        "SELECT mint, symbol FROM tokens WHERE mint = ANY($1::text[])", list({e.mint for e in p.ents}))}
    s, _ = assess(p, trades, prm, (until - since) / 86400, syms)
    if cand:
        s.own = int(cand.get("own") or 0)
        s.bundled = int(cand.get("bundled") or 0) / max(1, int(cand.get("coins") or 1))
    return s


def ago(sec: float | None) -> str:
    return fmt.ago(sec) if sec is not None else "?"


UNCHECKED = " ⚠️ оценка не сверена с тенью"


SHADOW_OFF = ("⚠️ Все кошельки бота выключены — тени за найденными нет, только оценка модели. Включить: «💵 Реальный "
              "бот» → «👛 Мои кошельки» → кошелёк → «Тень» (методы можно не включать).")


async def bot_watching(db: Any) -> bool | None:
    """27.11 (владелец думал, что охотник считает тень сам): тень «за ним» идёт, только если хоть один кошелёк бота в
    режиме «тень» или «бой» (real:desk). False — все выключены; None — состояние неизвестно."""
    d = await db.kv_get("real:desk") if hasattr(db, "kv_get") else None
    if not d:
        return None
    ws = d.get("wallets") or []
    if not ws:
        return None if d.get("common_accepts") else False
    return any(w.get("mode") in ("shadow", "live") for w in ws)


def unchecked(cfg: dict[str, Any] | None) -> str:
    """27.11 (владелец: модель против тени +15.4 п.п. на 366 сделках): пока разница не исправлена
    (hunter_model_checked: false), у $ в день — пометка."""
    return "" if (cfg or {}).get("hunter_model_checked") else UNCHECKED


def bot_line(s: Stats) -> str:
    """27.11 (владелец): ботов жёстко не отсеивать — в алерте метка «бот: N сделок/день, баланс ~0»."""
    bal = "?" if s.balance is None else ("~0" if s.balance < BOT_LOW_BALANCE else f"{s.balance:,.2f} SOL")
    if s.trades_day >= BOT_TRADES_DAY:
        return f"🤖 <b>бот: {s.trades_day:,.0f} сделок/день, баланс {bal}</b>"
    return f"Сделок в день: {s.trades_day:,.0f} · баланс {bal}"


def alert_text(s: Stats, now: float, prm: Params | None = None,
               note: str = UNCHECKED) -> tuple[str, list[list[dict[str, Any]]]]:
    prm = prm or Params()
    active = s.last_buy is not None and now - s.last_buy <= ACTIVE_S
    coins = ", ".join(f"${fmt.esc(sym)} {res:+.0%}" for sym, res in s.top)
    test = f"{s.rule_test:+.1%}" if s.rule_test is not None else "?"
    b2 = f"{s.block2_avg:+.1%}" if s.block2_avg is not None else "?"
    text = (f"🎯 <b>Охотник: горячий кошелёк</b> <code>{s.address}</code> → 👻 тень и «наблюдение»\n"
            f"За {WINDOW_H} ч, наш бот за ним (вход через {prm.delay:g} с, правило <b>{s.rule}</b>, издержки тени): "
            f"<b>{s.coins_all} монет</b> ({s.coins} с ценой), в среднем <b>{s.avg:+.1%}</b>, в плюсе "
            f"<b>{s.win:.0%}</b>, t={s.t:.1f} ({s.n} копий, без цены {s.noprice:.0%})\n"
            f"Правило {s.rule}: на второй половине {test}; со входом «блок +2» {b2}\n"
            f"<b>${s.usd_day:,.0f} в день</b> при {STAKE_SOL:g} SOL · ${s.usd_day_max:,.0f} при 3% пула (не больше "
            f"{prm.max_stake:g} SOL и {prm.copy_share:.0%} его покупки) · "
            f"{s.per_day:.0f} сделок в день{note}\n"
            f"Всплеск (+25% за 30 с): {s.burst:.0%} · слив на копировщиков: {s.dump:.0%}\n"
            f"Удержание у него: медиана {ago(s.hold_s)} · последняя покупка "
            f"{ago(now - s.last_buy) if s.last_buy else '?'} назад — {'🟢 активен' if active else '⚪ сейчас не торгует'}\n"
            f"Монеты: {coins}\n"
            f"{bot_line(s)}\n"
            f"Тень за ним включена (без денег). В «в бой» — по итогу после находки: от ${BATTLE_USD:g}/день на "
            f"{BATTLE_N}+ новых копиях; после 24 ч тени пришлю кнопку «Бой {STAKE_SOL:g}». Архив и снятие тени — "
            f"{ARCHIVE_MIN_N}+ копий и меньше ${WATCH_USD:g}/день.")
    kb = [[{"text": "🙈 Игнор (снять тень)", "callback": f"hn:i:{s.address}"}]]
    return text, kb


def ready_text(addr: str, res: dict[str, Any], usd_after: float | None,
               note: str = UNCHECKED) -> tuple[str, list[list[dict[str, Any]]]]:
    """Допуск к бою готов: кнопка «Бой 0.14» (дальше — подтверждение)."""
    usd = f", ${usd_after:,.0f}/день по модели{note}" if usd_after is not None else ""
    text = (f"✅ <b>Можно в бой</b> <code>{addr}</code>: список «в бой»{usd}; в тени за ним {res['n']} сд, в среднем "
            f"{res['avg']:+.1%}, {res['sol']:+.3f} SOL. Бой — только по кнопке, {STAKE_SOL:g} SOL на покупку.")
    return text, [[{"text": f"💵 Бой {STAKE_SOL:g}", "callback": f"hn:l:{addr}", "style": "danger"},
                   {"text": "🙈 Игнор", "callback": f"hn:i:{addr}"}]]


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
           VALUES ($1, now(), 'new', $2, $3, $4, $5, $6, $7, now(), 'watch', $8, $9, $10, $11, $12, $13, $14, $15, now())
           ON CONFLICT (address) DO UPDATE SET found_at = now(), status = 'new', coins = $2, avg_pct = $3, win = $4,
               hold_s = $5, n_copies = $6, noprice = $7, last_alert_at = now(), followed_at = NULL, cooled_at = NULL,
               cool_reason = NULL, list = 'watch', rule = $8, rule_train = $9, rule_test = $10, usd_day = $11,
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


async def archive_farm(db: Any, addr: str, why: str) -> None:
    """Ферма — в архив навсегда (охотник её больше не предложит)."""
    await db.execute("""UPDATE hunter_wallets SET list = 'archive', status = 'ignored', list_reason = $2, farm = $2,
                            farm_at = now(), list_at = now() WHERE address = $1""", addr, why)


def idle_reason(last_buy: float | None, now: float) -> str | None:
    """27.11: при находке кошелёк должен торговать — последняя покупка не старше COOL_IDLE_H (как в «остыл»): 25.11
    первый поиск нашёл 4 кошелька по окну 72 ч, у которых после находки не было ни одной сделки."""
    if last_buy is None or now - float(last_buy) > COOL_IDLE_H * 3600:
        return f"нет покупок {COOL_IDLE_H} ч"
    return None


def list_for(n_after: int, usd_after: float) -> tuple[str, str | None]:
    """A5 (27.11): список по новым сделкам после находки. Найденный сразу в «наблюдении» (и в тени); «в бой» — от
    $40/день на 50+ копиях; архив (тень снимается) — 20+ копий и меньше $15/день; иначе — «наблюдение»."""
    if n_after >= BATTLE_N and usd_after >= BATTLE_USD:
        return "battle", None
    if n_after >= ARCHIVE_MIN_N and usd_after < WATCH_USD:
        return "archive", f"после находки ${usd_after:,.0f}/день < ${WATCH_USD:g} ({n_after} копий)"
    return "watch", None


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
        farms: list[tuple[str, str]] = []
        not_checked = 0
        bar = bar_of(self.e.cfg)
        cands = await candidates(db, since, now, bar.min_coins, min_sol=prm.min_buy)
        for c in cands:
            k = known.get(c["trader"])
            if k is not None and (k["status"] in ("new", "shadow", "live", "ignored") or k["list"] in ("battle", "watch")
                                  or (k["cooled_at"] is not None and k["cooled_at"] > datetime.now(timezone.utc)
                                      - timedelta(days=REALERT_COOLED_D))):
                continue
            if c["own"] or int(c["bundled"]) / max(1, int(c["coins"])) >= BUNDLE_SHARE:
                continue
            if idle_reason(c.get("last_buy"), now):
                continue                    # 27.11: уже не торгует — «остыл» снял бы его через 12 ч без единой копии
            s = await evaluate(db, c["trader"], since, now, prm, cand=c)
            if s.hot(bar) is None:
                # 27.11 (владелец): отсев ферм обязателен — создавал монеты, получал комиссию создателя или
                # профинансировал больше 20 кошельков → архив «ферма»; не проверен (RPC) — в следующий час
                farm, checked = await self.farm_check(c["trader"])
                if farm:
                    await upsert(db, s)
                    await archive_farm(db, c["trader"], farm)
                    farms.append((c["trader"], farm))
                    continue
                if not checked:
                    not_checked += 1
                    continue
                s.balance = await self.balance(c["trader"])
                hot.append(s)
                await upsert(db, s)
                await db.execute("UPDATE hunter_wallets SET farm_at = now() WHERE address = $1", c["trader"])
                # 27.11 (владелец): всех найденных — сразу в тень (без денег) и «наблюдение»
                await follow(db, self.e.cfg, self.e.costs(), c["trader"], "shadow")
                text, kb = alert_text(s, now, prm, unchecked(self.e.cfg))
                if await bot_watching(db) is False:
                    text += "\n" + SHADOW_OFF
                await self.say(text, kb)
        if farms:
            await self.say(f"🚜 <b>Охотник: отсеяно ферм {len(farms)}</b> — в архив, за ними не повторяем:\n"
                           + "\n".join(f"<code>{a}</code> — {fmt.esc(w)}" for a, w in farms[:10]))
        self.stats = {"at": now, "candidates": len(cands), "hot": len(hot), "farms": len(farms),
                      "farm_unchecked": not_checked}
        return hot

    async def balance(self, addr: str) -> float | None:
        rpc = getattr(self.e, "rpc", None)
        if rpc is None or not hasattr(rpc, "call"):
            return None
        try:
            res = await rpc.call("getBalance", [addr, {"commitment": "confirmed"}], retries=1)
            value = res.get("value") if isinstance(res, dict) else res
            return int(value) / 1e9 if value is not None else None
        except Exception:  # noqa: BLE001 — баланс в алерте не обязателен
            return None

    async def farm_check(self, addr: str) -> tuple[str | None, bool]:
        rpc = getattr(self.e, "rpc", None)
        over = getattr(self.e, "rpc_over_budget", None)
        return await FARM.check(self.e.db, rpc, addr, over)

    # ------------------------------------------------------------------ после находки (A5, C2, B4)
    async def track(self, now: float, prm: Params | None = None) -> None:
        db = self.e.db
        prm = prm or await params_of(db, self.e.cfg)
        rows = await db.fetch("SELECT * FROM hunter_wallets WHERE list <> 'archive' AND status <> 'ignored'")
        for r in rows:
            addr, start = r["address"], r["found_at"].timestamp()
            if r["farm_at"] is None:
                # 27.11: найденные до отсева ферм — проверить сейчас (ферма — снять тень, в архив)
                farm, checked = await self.farm_check(addr)
                if farm:
                    from ..bot import board
                    mine = addr in (await db.kv_get(board.UNTIL_KEY) or {})   # ты сам поставил /follow — не трогаю
                    stopped = r["status"] in ("shadow", "live") and not mine
                    if stopped:
                        await follow(db, self.e.cfg, self.e.costs(), addr, None)
                    await archive_farm(db, addr, farm)
                    await self.say(f"🚜 <code>{addr}</code> — {fmt.esc(farm)}: в архив"
                                   + (", копирование за ним снято." if stopped else
                                      "; тень по твоему /follow оставил — сними сам, если согласен." if mine else "."))
                    continue
                if checked:
                    await db.execute("UPDATE hunter_wallets SET farm_at = now() WHERE address = $1", addr)
            trades = await CM.wallet_trades(db, addr, start - 120, now)
            p = await CM.prepare(db, addr, trades, start, now, limit=100_000, min_buy=prm.min_buy)
            rule = r["rule"] if r["rule"] in SCHEMES else "A"
            copies = CM.run(p, SCHEMES[rule], exit_delay_s=prm.exit_delay, entry_delay_s=prm.delay, min_pool_sol=prm.min_pool)
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
                               f"{len(res)} копий, в среднем {avg:+.1%}, ${usd:,.0f}/день при {STAKE_SOL:g} SOL"
                               f"{unchecked(self.e.cfg)}. В бой — после 24 ч тени, кнопкой.")
            elif new_list == "archive" and r["list"] != "archive" and reason and "сливает" not in reason:
                # 27.11: шум отсеивает тень — в архиве копирование (тень и бой) снимается
                stopped = r["status"] in ("shadow", "live")
                if stopped:
                    await follow(db, self.e.cfg, self.e.costs(), addr, None)
                    await db.execute("UPDATE hunter_wallets SET status = 'cooled', cooled_at = now(), cool_reason = $2 "
                                     "WHERE address = $1", addr, reason)
                await self.say(f"🗄 <code>{addr}</code> → архив: {reason}."
                               + (" Копирование за ним снято." if stopped else ""))

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
        for r in await db.fetch("SELECT address, status FROM hunter_wallets "
                                "WHERE list = 'battle' AND status IN ('shadow', 'live')"):
            if await battle_ready(db, r["address"], now) is None:
                ok[r["address"]] = old.get(r["address"], now)
                if r["address"] not in old and r["status"] != "live":
                    w = await db.fetchrow("SELECT followed_at, usd_day_after FROM hunter_wallets WHERE address = $1",
                                          r["address"])
                    res = await our_results(db, r["address"], w["followed_at"])
                    await self.say(*ready_text(r["address"], res, float(w["usd_day_after"])
                                               if w["usd_day_after"] is not None else None, unchecked(self.e.cfg)))
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
             c.exit_mc, c.burst, c.stake, c.hold_s) for e, c in copies]
    if rows:
        await db.p.executemany(
            """INSERT INTO hunter_copies(address, mint, entry_at, rule, res, reason, entry_mc, exit_mc, burst, stake,
                                         hold_s)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               ON CONFLICT (address, mint, entry_at) DO UPDATE SET rule = EXCLUDED.rule, res = EXCLUDED.res,
                   reason = EXCLUDED.reason, entry_mc = EXCLUDED.entry_mc, exit_mc = EXCLUDED.exit_mc,
                   burst = EXCLUDED.burst, stake = EXCLUDED.stake, hold_s = EXCLUDED.hold_s""", rows)


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


def once_per_coin(items: list[tuple[float, float, str, bool, Any]]) -> tuple[list[tuple[float, float, str, bool, Any]],
                                                                             int]:
    """27.11 (владелец): несколько кошельков купили одну монету — входим один раз. Как в боте (executor.coin_taken):
    пока копия по монете открыта, такая же покупка другого кошелька — мимо; живую (бой) занимает только живая, тень
    занимает любая. items: (вход, выход, монета, бой ли, данные). Возвращает оставшиеся по времени и сколько убрано."""
    live_until: dict[str, float] = {}
    any_until: dict[str, float] = {}
    out, dup = [], 0
    for it in sorted(items, key=lambda i: i[0]):
        t0, t1, mint, live = it[0], it[1], it[2], it[3]
        if (live_until if live else any_until).get(mint, float("-inf")) > t0:
            dup += 1
            continue
        if live:
            live_until[mint] = max(live_until.get(mint, t1), t1)
        any_until[mint] = max(any_until.get(mint, t1), t1)
        out.append(it)
    return out, dup


async def shadow_total(db: Any, now: float | None = None) -> list[str]:
    """27.11 (/hunt): итог по всем кошелькам, за которыми повторяем (тень и бой): модель — копии после находки (монета
    у нескольких — один раз), $ в день — сумма по кошелькам; и настоящие сделки бота за ними."""
    now = now or time.time()
    ws = await db.fetch("SELECT address, status, found_at, followed_at FROM hunter_wallets "
                        "WHERE status IN ('shadow', 'live') AND followed_at IS NOT NULL")
    if not ws:
        return []
    prm = await params_of(db)
    ok = set(await db.kv_get(BATTLE_KEY) or {})
    found = {r["address"]: r["found_at"].timestamp() for r in ws}
    live = {r["address"] for r in ws if r["status"] == "live" and r["address"] in ok}
    rows = await db.fetch(
        """SELECT address, mint, extract(epoch FROM entry_at) AS ts, res, hold_s FROM hunter_copies
           WHERE address = ANY($1::text[]) AND reason IS DISTINCT FROM $2""", list(found), CM.SMALL_POOL)
    items = [(float(r["ts"]), float(r["ts"]) + float(r["hold_s"] if r["hold_s"] is not None else EXIT_LAG_S),
              r["mint"], r["address"] in live, r) for r in rows if float(r["ts"]) >= found[r["address"]]]
    kept, dup = once_per_coin(items)
    res = [float(it[4]["res"]) for it in kept if it[4]["res"] is not None]
    usd_day = 0.0
    for addr, t0 in found.items():
        mine = [float(it[4]["res"]) for it in kept if it[4]["address"] == addr and it[4]["res"] is not None]
        usd_day += sum(mine) * STAKE_SOL * prm.sol_usd / max((now - t0) / 86400, 1 / 24)
    noprice = sum(1 for it in kept if it[4]["res"] is None)
    lines = [f"Σ <b>Итого за {len(ws)} кошельками</b> (тень{' и бой' if live else ''}), модель после находки: "
             + (f"{len(res)} копий, в среднем {statistics.fmean(res):+.1%}, {sum(res) * STAKE_SOL:+.3f} SOL при "
                f"{STAKE_SOL:g} SOL, ≈${usd_day:,.0f}/день" if res else "копий с ценой ещё нет")
             + (f" · без цены {noprice}" if noprice else "")
             + (f" · монета у нескольких — один раз (убрано {dup})" if dup else "")]
    n = sol = 0.0
    pct: list[float] = []
    usd_ours = 0.0
    for r in ws:
        d = await our_results(db, r["address"], r["followed_at"])
        if d["n"]:
            n += d["n"]
            sol += d["sol"]
            pct.append(d["avg"] * d["n"])
            usd_ours += d["sol"] * prm.sol_usd / max((now - r["followed_at"].timestamp()) / 86400, 1 / 24)
    lines.append("   Бот за ними (настоящие сделки тени и боя): "
                 + (f"{int(n)} сд, в среднем {sum(pct) / n:+.1%}, {sol:+.3f} SOL, ≈${usd_ours:,.0f}/день" if n
                    else "сделок ещё нет"))
    return lines


def _reason(text: str | None) -> str:
    """Причина без чисел конкретной сделки — чтобы одинаковые складывались."""
    t = re.sub(r"[-+]?\d[\d.,]*\s*%?", "…", (text or "без причины").split(";")[0]).strip()
    return t[:70]


async def why_text(db: Any, prm: Params | None = None, wallet: str | None = None, now: float | None = None) -> str:
    """27.11 (владелец: «найдены в 17:52, к 23:42 ни одной копии»): по каждому кошельку охотника в тени — его покупки
    после находки и что с каждой стало: модель (с ценой / мелкий пул < real_min_pool_sol / нет цены), журнал копий
    (сигнал взят или причина отказа), бот (позиция тени или причина пропуска из real_skips по той же монете ±3 мин), и
    сколько не дошли ни до журнала, ни до бота (кошелёк не в слежке / сделка не записана)."""
    now = now or time.time()
    prm = prm or await params_of(db)
    rows = await db.fetch("SELECT * FROM hunter_wallets WHERE address = $1", wallet) if wallet else await db.fetch(
        "SELECT * FROM hunter_wallets WHERE status IN ('shadow', 'live') ORDER BY found_at")
    if not rows:
        return "Кошельков охотника в тени нет." if not wallet else "Такого кошелька у охотника нет."
    out = [f"🔎 <b>Охотник: что стало с покупками после находки</b> (вход через {prm.delay:g} с, пул от "
           f"{prm.min_pool:g} SOL; время UTC)"]
    for r in rows:
        addr, found = r["address"], r["found_at"]
        since = found.timestamp()
        trades = await CM.wallet_trades(db, addr, since - 120, now)
        buys = [d for d in trades if d["is_buy"] and d["ts"] >= since]
        before = await db.fetchval(
            """SELECT extract(epoch FROM max(ts)) FROM (SELECT ts FROM trades WHERE trader = $1 AND is_buy AND ts < $2
                   UNION ALL SELECT ts FROM market_trades WHERE trader = $1 AND is_buy AND ts < $2) x""", addr, found)
        rule = r["rule"] if r["rule"] in SCHEMES else "A"
        p = await CM.prepare(db, addr, trades, since, now, limit=100_000, min_buy=prm.min_buy)
        cs = CM.run(p, SCHEMES[rule], exit_delay_s=prm.exit_delay, entry_delay_s=prm.delay, min_pool_sol=prm.min_pool)
        model = Counter("с ценой" if c.res is not None else (c.why or "?") for _e, c in cs)
        nopool = sum(1 for _e, c in CM.run(p, SCHEMES[rule], exit_delay_s=prm.exit_delay, entry_delay_s=prm.delay) if c.res is not None)
        small = [CM.pool_sol(c.entry_mc, c.venue) for _e, c in cs if c.why == CM.SMALL_POOL and c.entry_mc]
        head = (f"<code>{addr}</code> · найден {found:%d.%m %H:%M}, {r['status']}, правило {rule}\n"
                f"  покупок после находки: {len(buys)} (монет {len({d['mint'] for d in buys})})"
                + (f", из них мельче {prm.min_buy:g} SOL (бот не копирует) — {tiny}" if (tiny := sum(
                    1 for d in buys if float(d["sol"] or 0) < prm.min_buy)) else "")
                + (f", последняя {datetime.fromtimestamp(buys[-1]['ts'], timezone.utc):%d.%m %H:%M}" if buys else "")
                + (f"; все сделки в базе до {datetime.fromtimestamp(trades[-1]['ts'], timezone.utc):%d.%m %H:%M}"
                   if trades else "; его сделок в базе нет")
                + (f"\n  до находки последняя покупка {datetime.fromtimestamp(float(before), timezone.utc):%d.%m %H:%M}"
                   f" — за {(since - float(before)) / 3600:.0f} ч до находки"
                   + (" (уже не торговал)" if since - float(before) > COOL_IDLE_H * 3600 else "")
                   if before is not None else "\n  до находки его покупок в базе нет"))
        out.append(head)
        if not cs:
            continue
        out.append("  модель: " + ", ".join(f"{k} {v}" for k, v in model.most_common())
                   + (f" (пул у мелких: медиана {statistics.median(small):.0f} SOL)" if small else "")
                   + f"; без фильтра пула с ценой было бы {nopool}")
        lo = datetime.fromtimestamp(since - 5, timezone.utc)
        logs = await db.fetch("""SELECT mint, extract(epoch FROM ts) AS ts, action, reason FROM copy_log
                                 WHERE wallet = $1 AND side = 'buy' AND ts >= $2""", addr, lo)
        pos = await db.fetch("""SELECT mint, extract(epoch FROM opened_at) AS ts FROM real_positions
                                WHERE trader = $1 AND opened_at >= $2""", addr, lo)
        skips = await db.fetch("""SELECT mint, extract(epoch FROM at) AS ts, mode, why FROM real_skips
                                  WHERE mint = ANY($1::text[]) AND at >= $2""", list({e.mint for e, _c in cs}), lo)
        fate: Counter[str] = Counter()
        for e, _c in cs:
            near = lambda xs: [x for x in xs if x["mint"] == e.mint and e.ts - 5 <= float(x["ts"]) <= e.ts + 180]  # noqa: E731
            sk, lg = near(skips), near(logs)
            if near(pos):
                fate["позиция тени"] += 1
            elif sk:
                fate["бот: " + _reason(sk[-1]["why"])] += 1
            elif lg and lg[-1]["action"] == "skipped":
                fate["сигнал: " + _reason(lg[-1]["reason"])] += 1
            elif lg:
                fate["сигнал есть, у бота ни позиции, ни причины"] += 1
            else:
                fate["ни сигнала, ни пропуска бота"] += 1
        out.append("  бот: " + "; ".join(f"{k} — {v}" for k, v in fate.most_common(6)))
    return "\n".join(out)


async def entry_gap_text(db: Any, prm: Params, wallet: str, hours: float = 72.0, top: int = 3) -> str:
    """27.11 (владелец: у Fn9n… «блок +2: +378%» при +12.5% с задержкой 1.36 с — ошибка цены или весь плюс в первую
    секунду?): копии с наибольшим разрывом между входом «блок +2» и входом через delay — его покупка, оба входа (блок,
    время, MC, источник цены), сделки первых 3 с после него и выход."""
    now = time.time()
    trades = await CM.wallet_trades(db, wallet, now - hours * 3600 - 120, now)
    p = await CM.prepare(db, wallet, trades, now - hours * 3600, now, limit=100_000, min_buy=prm.min_buy)
    rule = await db.fetchval("SELECT rule FROM hunter_wallets WHERE address = $1", wallet) or "A"
    sc = SCHEMES[rule if rule in SCHEMES else "A"]
    syms = {r["mint"]: r["symbol"] for r in await db.fetch(
        "SELECT mint, symbol FROM tokens WHERE mint = ANY($1::text[])", list({e.mint for e in p.ents}))}
    rows = []
    for e in p.ents:
        tk = [t for t in p.ticks.get(e.mint, []) if e.ts - 5 <= t.ts <= e.ts + 1900]
        created, dev = p.meta.get(e.mint, (None, None))
        kw = dict(covered=p.cover.get(e.mint), min_pool_sol=prm.min_pool, exit_delay_s=prm.exit_delay)
        cr = float(created) if created is not None else None
        b2 = CM.copy_trade(e, tk, wallet, dev or None, cr, sc, **kw)
        dl = CM.copy_trade(e, tk, wallet, dev or None, cr, sc, entry_delay_s=prm.delay, **kw)
        if b2.res is not None and dl.res is not None:
            rows.append((b2.res - dl.res, e, tk, b2, dl))
    tiny = sum(1 for e in CM.entries_of(trades) if e.ts >= now - hours * 3600 and e.sol < prm.min_buy)
    tiny_s = (f"\nЕго входов мельче {prm.min_buy:g} SOL: {tiny} — бот такие не копирует, в расчёт не идут"
              if tiny else "")
    if not rows:
        return f"У {wallet} за {hours:g} ч нет копий с ценой в обоих вариантах." + tiny_s
    rows.sort(key=lambda r: -r[0])
    b2s, dls = [r[3].res for r in rows], [r[4].res for r in rows]
    rest = rows[top:]
    out = [f"🔬 <b>Вход «блок +2» против входа через {prm.delay:g} с</b> — <code>{wallet}</code>, правило {rule}, "
           f"{hours:g} ч: {len(rows)} копий, блок +2 {statistics.fmean(b2s):+.1%}, {prm.delay:g} с "
           f"{statistics.fmean(dls):+.1%}"
           + (f"; без {top} крайних — {statistics.fmean([r[3].res for r in rest]):+.1%} / "
              f"{statistics.fmean([r[4].res for r in rest]):+.1%}" if rest else "") + tiny_s]

    def tick(t: Any, e: Any) -> str:
        sl = f"блок {t.slot - e.slot:+d}" if t.slot is not None and e.slot else "блок ?"
        return f"{t.ts - e.ts:+.2f} с, {sl}, MC {t.mc:,.1f} ({t.src or '?'})"
    for gap, e, tk, b2, dl in rows[:top]:
        et2, etd = CM.entry_tick(e, tk), CM.entry_by_delay(e, tk, prm.delay)
        own = next((t for t in tk if t.trader == wallet and t.is_buy and abs(t.ts - e.ts) <= 2), None)
        out.append(f"\n<b>${fmt.esc(syms.get(e.mint) or e.mint[:6])}</b> <code>{e.mint}</code> — разрыв {gap * 100:+.0f} п.п.\n"
                   f"  его покупка {datetime.fromtimestamp(e.ts, timezone.utc):%d.%m %H:%M:%S}, блок {e.slot}, "
                   f"{e.sol:.2f} SOL" + (f", MC {own.mc:,.1f} ({own.src or '?'})" if own else "") + "\n"
                   f"  блок +2: вход " + (tick(et2, e) if et2 else "?") + f" → {b2.reason} MC {b2.exit_mc:,.1f} → "
                   f"{b2.res:+.0%}\n"
                   f"  {prm.delay:g} с: вход " + (tick(etd, e) if etd else "?") + f" → {dl.reason} MC {dl.exit_mc:,.1f}"
                   f" → {dl.res:+.0%}")
        first = [t for t in tk if e.ts - 0.5 <= t.ts <= e.ts + 3][:10]
        out.append("  сделки до +3 с: " + "; ".join(
            f"{t.ts - e.ts:+.2f} с {'покупка' if t.is_buy else 'продажа'} {t.sol:.2f} SOL MC {t.mc:,.1f} "
            f"({t.src or '?'}{', его' if t.trader == wallet else ''})" for t in first))
    return "\n".join(out)


async def hunt_text(db: Any, hours: float = 24.0, note: str = UNCHECKED) -> str:
    """A8: кого нашёл за hours часов и все из списков «в бой» и «наблюдение»."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = await db.fetch(
        """SELECT * FROM hunter_wallets WHERE found_at >= $1 OR list IN ('battle', 'watch')
           ORDER BY (list = 'battle') DESC, (list = 'watch') DESC, usd_day_after DESC NULLS LAST, found_at DESC
           LIMIT 40""", since)
    ok = set(await db.kv_get(BATTLE_KEY) or {})
    if not rows:
        off = await bot_watching(db) is False
        return f"🎯 Охотник: за {hours:g} ч никого не нашёл, в списках пусто." + (f"\n{SHADOW_OFF}" if off else "")
    status = {"new": "без решения", "shadow": "👻 тень", "live": "💵 бой", "ignored": "игнор", "cooled": "🧊 остыл"}
    lines = [f"🎯 <b>Охотник</b>: найдены за {hours:g} ч и списки"
             + (f"\n$/день — оценка модели:{note}" if note else "")
             + (f"\n{SHADOW_OFF}" if await bot_watching(db) is False else "")] + await shadow_total(db)
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
                  start: float | None = None, prm: Params | None = None, rules: tuple[str, ...] = RULES,
                  rpc: Any = None) -> str:
    """Как охотник сработал бы на прошлых данных: в каждой точке (шаг step_h) — горячие за 72 ч до неё (правило — на
    первой половине окна, проверка — на второй); первый раз найденный ведём вперёд по модели до «остыл» (t ≤ −2 на
    10+ копиях после находки или 12 ч без покупок) или до конца данных; список — по $ в день после находки."""
    bar = bar or BAR
    prm = prm or await params_of(db)
    until = until or time.time()
    start = start if start is not None else until - days * 86400
    lo = start - WINDOW_H * 3600
    cands = await candidates(db, lo, until, bar.min_coins, max_wallets, min_sol=prm.min_buy)
    cands = [c for c in cands if not c["own"] and int(c["bundled"]) / max(1, int(c["coins"])) < BUNDLE_SHARE]
    n_cands = len(cands)
    if trace and not any(c["trader"] == trace for c in cands):
        cands.append({"trader": trace, "coins": 0, "own": 0, "bundled": 0})
    found: list[dict[str, Any]] = []
    farms: list[tuple[str, str]] = []
    unchecked = 0
    tr_lines: list[str] = []
    for c in cands:
        trades = await CM.wallet_trades(db, c["trader"], lo, until)
        p = await CM.prepare(db, c["trader"], trades, lo, until, limit=100_000, min_buy=prm.min_buy)
        if not p.ents:
            if c["trader"] == trace:
                tr_lines.append("  копий нет: в базе нет его покупок за период")
            continue
        by_rule = {r: CM.run(p, SCHEMES[r], exit_delay_s=prm.exit_delay, entry_delay_s=prm.delay, min_pool_sol=prm.min_pool) for r in rules}
        t = start
        while t <= until:
            win = {r: [(e, x) for e, x in cs if t - WINDOW_H * 3600 <= e.ts < t] for r, cs in by_rule.items()}
            rule, train, test = pick_rule(win)
            wtr = [d for d in trades if t - WINDOW_H * 3600 <= d["ts"] < t]
            s = stats_of(c["trader"], win[rule], wtr)
            s.rule, s.rule_train, s.rule_test = rule, train, test
            s.per_day = s.total / (WINDOW_H / 24)
            s.usd_day = s.per_day * STAKE_SOL * s.avg * prm.sol_usd
            why = s.hot(bar) or idle_reason(s.last_buy, t)
            if c["trader"] == trace and s.total:
                half = f" (2-я половина {test:+.1%})" if test is not None else ""
                tr_lines.append(f"  {datetime.fromtimestamp(t, timezone.utc):%d.%m %H:%M}: {s.coins_all} мон. "
                                f"({s.coins} с ценой), {s.n}/{s.total} коп., правило {rule} {s.avg:+.1%}{half}, "
                                f"в плюсе {s.win:.0%}, t={s.t:.1f}, без цены {s.noprice:.0%} → "
                                f"{'🔥 ГОРЯЧИЙ' if why is None else why}")
            if why is None:
                # 27.11: отсев ферм, как при находке (база и RPC — нынешняя история кошелька: ферма остаётся фермой)
                farm, checked = await FARM.check(db, rpc, c["trader"])
                if farm:
                    farms.append((c["trader"], farm))
                    if c["trader"] == trace:
                        tr_lines.append(f"  → {farm}: в архив, за ним не повторяем")
                    break
                unchecked += not checked
                j = journey(by_rule[rule], [d for d in trades if d["ts"] >= t], t, until, prm.sol_usd)
                b2 = [x.res for e, x in CM.run(p, SCHEMES[rule], exit_delay_s=prm.exit_delay, min_pool_sol=prm.min_pool)
                      if t <= e.ts <= j["end"] and x.res is not None]
                found.append({"address": c["trader"], "at": t, "stats": s, **j, "b2": b2})
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
             f"Горячих по планке: {len(found) + len(farms)}, из них ферм {len(farms)}"
             + (f" (не проверены через RPC: {unchecked})" if unchecked else ""),
             f"Горячих нашлось бы: {len(found)} · в бой {len(by_list.get('battle', []))}, наблюдение "
             f"{len(by_list.get('watch', []))}, архив {len(by_list.get('archive', []))}"]
    if farms:
        lines.append("Фермы (в архив, после находки не считаются): " + "; ".join(
            f"{a[:4]}…{a[-4:]} — {w.removeprefix('ферма: ')}" for a, w in farms[:20]))
    with_fwd = [f for f in found if f["n"]]
    if with_fwd:
        # 27.11: монета у нескольких кошельков — один раз (как бот: пока копия открыта, вторая — мимо)
        kept, dup = once_per_coin([it for f in found for it in f["items"]])
        allres = [it[4][0] for it in kept if it[4][0] is not None]
        lines.append(f"После находки (модель, пока не остыл и не в архиве; монета у нескольких — один раз, убрано "
                     f"{dup}): копий {len(allres)}, в среднем {statistics.fmean(allres):+.1%}, в плюсе "
                     f"{sum(1 for x in allres if x > 0) / len(allres):.0%}; кошельков в плюсе "
                     f"{sum(1 for f in with_fwd if f['sum'] > 0)} из {len(with_fwd)}; итог "
                     f"{sum(allres) * STAKE_SOL:+.3f} SOL при {STAKE_SOL:g} SOL на копию; без цены "
                     f"{sum(1 for it in kept if it[4][0] is None)} копий")
        moved = [f for f in found if f["battle_at"] is not None]
        pre = [it[4][0] for it in kept if it[4][1] == "pre" and it[4][0] is not None]
        bat = [it[4][0] for it in kept if it[4][1] == "bat" and it[4][0] is not None]
        lines.append(f"Переведены в «в бой» (от ${BATTLE_USD:g}/день на {BATTLE_N}+ копиях после находки, 24 ч тени): "
                     f"{len(moved)} из {len(found)}")
        lines.append("  до перевода — тень, денег нет: " + phase_text(pre, prm.sol_usd))
        lines.append("  в бою (с перевода, пока в списке «в бой»): " + phase_text(bat, prm.sol_usd))
        for f in sorted(with_fwd, key=lambda x: -x["usd"])[:20]:
            s = f["stats"]
            bt = (f" · в бой {datetime.fromtimestamp(f['battle_at'], timezone.utc):%d.%m %H:%M}: до {len(f['pre'])} коп."
                  f", в бою {len(f['bat'])} коп. ${sum(f['bat']) * STAKE_SOL * prm.sol_usd:+,.0f}"
                  if f["battle_at"] is not None else "")
            lines.append(f"  {LIST_NAMES[f['list']]:<11} {f['address'][:4]}…{f['address'][-4:]} найден "
                         f"{datetime.fromtimestamp(f['at'], timezone.utc):%d.%m %H:%M} ({s.coins_all} мон., {s.rule} "
                         f"{s.avg:+.0%}, t={s.t:.1f}) → {f['n']} коп., {statistics.fmean(f['res']):+.1%}, "
                         f"${f['usd']:,.0f}/день{bt}" + (f" · остыл: {f['cooled']}" if f["cooled"] else "")
                         + (f" · архив: {f['reason']}" if f["list"] == "archive" and f["reason"] else ""))
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
            lines.append(f"  До перевода в «в бой» (тень, денег нет): " + phase_text(f["pre"], prm.sol_usd, f["pre_np"]))
            if f["battle_at"] is not None:
                bdays = max((f["end"] - f["battle_at"]) / 86400, 1 / 24)
                lines.append(f"  В «в бой» — {datetime.fromtimestamp(f['battle_at'], timezone.utc):%d.%m %H:%M} "
                             f"(через {(f['battle_at'] - f['at']) / 3600:.0f} ч после находки)")
                lines.append(f"  В бою: " + phase_text(f["bat"], prm.sol_usd, f["bat_np"])
                             + f", ≈${sum(f['bat']) * STAKE_SOL * prm.sol_usd / bdays:,.0f}/день за {bdays * 24:.0f} ч"
                             + (" (выпадал из «в бой»: " + str(f["out"]) + " коп. в это время — только тень)"
                                if f["out"] else ""))
            else:
                lines.append(f"  В «в бой» не перешёл до {datetime.fromtimestamp(f['end'], timezone.utc):%d.%m %H:%M}"
                             f" — {f['why_not']}")
            if f["b2"]:
                lines.append(f"  Для сравнения вход «блок +2» на тех же: {len(f['b2'])} копий, "
                             f"{statistics.fmean(f['b2']):+.1%}")
        else:
            lines.append("  Не найден.")
    return "\n".join(lines)


EXIT_LAG_S = 1800.0                  # правила A/B/G держат до 30 мин: итог копии известен не раньше


def phase_text(res: list[float], sol_usd: float, noprice: int | None = None) -> str:
    if not res:
        return "копий с ценой нет" + (f" (без цены {noprice})" if noprice else "")
    usd = sum(res) * STAKE_SOL * sol_usd
    return (f"{len(res)} коп., в среднем {statistics.fmean(res):+.1%}, в плюсе "
            f"{sum(1 for x in res if x > 0) / len(res):.0%}, ${usd:+,.0f} при {STAKE_SOL:g} SOL"
            + (f", без цены {noprice}" if noprice else ""))


def journey(copies: list[tuple[CM.Entry, CM.Copy]], trades: list[dict[str, Any]], t0: float, until: float,
            sol_usd: float, check_s: float = 3600.0) -> dict[str, Any]:
    """27.11: после находки t0 — как в боте. Сразу тень и «наблюдение»; раз в час — список по копиям после находки,
    итог которых уже известен (list_for, как Hunter.track); «в бой» — когда список «в бой», прошло 24 ч тени и в тени
    5+ копий не в минусе (как battle_ready) — с этого часа копии идут на деньги; выпал из «в бой» — снова только тень;
    архив — копирование снято; «остыл» — как forward. Копии делятся на «до перевода» (pre) и «в бою» (bat)."""
    fwd = forward(copies, trades, t0, until)
    end = fwd["end"] if fwd["end"] is not None else until
    seq = sorted(((e, c) for e, c in copies if t0 <= e.ts <= end and c.why != CM.SMALL_POOL), key=lambda x: x[0].ts)

    def state(t: float) -> tuple[str, str | None, float, bool, int, float]:
        done = [c for e, c in seq if e.ts + EXIT_LAG_S <= t]
        res = [c.res for c in done if c.res is not None]
        avg = statistics.fmean(res) if res else 0.0
        usd = len(done) / max((t - t0) / 86400, 1 / 24) * STAKE_SOL * avg * sol_usd
        lst, why = list_for(len(res), usd)
        ready = (lst == "battle" and t - t0 >= SHADOW_BEFORE_LIVE_H * 3600 and len(res) >= SHADOW_MIN_N and avg >= 0)
        return lst, why, usd, ready, len(res), avg

    pre: list[tuple[CM.Entry, CM.Copy]] = []
    bat: list[tuple[CM.Entry, CM.Copy]] = []
    battle_at = archived_at = None
    reason = None
    gap: list[tuple[CM.Entry, CM.Copy]] = []
    in_battle = False
    tick = t0 + check_s
    for e, c in seq:
        while tick <= e.ts and archived_at is None:
            lst, why, _usd, ready, _n, _avg = state(tick)
            if lst == "archive":
                archived_at, reason = tick, why
            else:
                in_battle = ready
                if ready and battle_at is None:
                    battle_at = tick
            tick += check_s
        if archived_at is not None:
            break
        if in_battle:
            bat.append((e, c))
        elif battle_at is None:
            pre.append((e, c))
        else:
            gap.append((e, c))                          # выпал из «в бой» — эти копии только в тени
    stop = archived_at if archived_at is not None else end
    lst, why, usd, _ready, n, avg = state(stop)
    if archived_at is not None:
        lst = "archive"
    elif lst == "battle" and battle_at is None:
        lst = "watch"                                   # «в бой» по итогу, но 24 ч тени ещё не прошло
    why_not = (reason or why) if lst == "archive" else (
        f"после находки {n} коп. с ценой, ${usd:,.0f}/день (нужно {BATTLE_N}+ и ${BATTLE_USD:g})"
        if battle_at is None else None)
    followed = pre + bat + gap
    res = [c.res for _e, c in followed if c.res is not None]
    return {"n": len(res), "res": res, "sum": sum(res),
            "noprice": sum(1 for _e, c in followed if c.res is None), "cooled": fwd["cooled"] if archived_at is None
            else None, "end": stop, "usd": usd, "list": lst, "reason": reason or why, "why_not": why_not,
            "battle_at": battle_at, "pre": [c.res for _e, c in pre if c.res is not None],
            "pre_np": sum(1 for _e, c in pre if c.res is None), "bat": [c.res for _e, c in bat if c.res is not None],
            "bat_np": sum(1 for _e, c in bat if c.res is None), "out": len(gap),
            "items": [(e.ts, e.ts + (c.hold_s or 0.0), e.mint, ph == "bat", (c.res, ph))
                      for ph, cs in (("pre", pre), ("bat", bat), ("gap", gap)) for e, c in cs]}


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
