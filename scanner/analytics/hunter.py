"""25.11 (владелец): режим «охотник» — горячие кошельки по копии «как бот».

1. Раз в час: все кошельки, купившие за 72 ч от HOT_MIN_COINS разных монет (из базы сделок), — копия copymodel (вход
   блок +2, правило A, ставка 0.14 SOL, издержки как в тени; цена — сделки и резервы монет).
2. «Горячий»: от 15 разных монет с ценой копии, в среднем от +10%, в плюсе от 60%, без цены не больше половины копий,
   не покупал свои монеты (не дев), не бандл (покупки в блоке создания монеты — меньше BUNDLE_SHARE).
3. Алерт владельцу: адрес, монеты, средний %, % в плюсе, удержание, активен ли сейчас; кнопки «Тень» / «Бой 0.14» /
   «Игнор» (бой — с подтверждением; только у кошельков бота в режиме «бой»; ставка — не выше потолка сервера).
4. Пока следим: итог за 24 ч; минус на 10+ сделках за 24 ч или у него нет покупок 12 ч — «остыл», копирование стоп.
5. Раз в неделю — отчёт: сколько горячих нашлось и сколько осталось в плюсе после того, как мы начали за ними
   повторять (наши сделки), и по модели — для всех найденных.

Включается владельцем: /hunter on (настройка hunter_on). Слежение за уже взятыми кошельками («остыл») работает всегда.
Исторический прогон: python -m scanner hunt --history [--days N] [--step H] [--max N].
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

log = logging.getLogger(__name__)

WINDOW_H = 72
HOT_MIN_COINS, HOT_MIN_AVG, HOT_MIN_WIN, HOT_MAX_NOPRICE = 15, 0.10, 0.60, 0.5
BUNDLE_SHARE = 0.3                 # доля покупок в блоке создания монеты — от неё кошелёк считается бандлом
ACTIVE_S = 3600                    # «активен сейчас»: последняя покупка не старше часа
COOL_MIN_N, COOL_IDLE_H = 10, 12
STAKE_SOL = 0.14
MAX_WALLETS = 300                  # за проход: кошельков с моделью (по числу монет за 72 ч)
EVAL_LIMIT = 200                   # копий на кошелёк за 72 ч
REALERT_COOLED_D = 7
TOPIC = "copy"
DAY_KEY, WEEK_KEY = "hunter:day_at", "hunter:week_at"


@dataclass
class Stats:
    address: str
    coins: int = 0                 # разных монет с ценой копии
    n: int = 0                     # копий с ценой
    total: int = 0                 # всех копий
    avg: float = 0.0
    win: float = 0.0
    noprice: float = 0.0
    hold_s: float | None = None    # медиана удержания у самого трейдера
    last_buy: float | None = None
    own: int = 0                   # покупок своих монет (он их дев)
    bundled: float = 0.0           # доля покупок в блоке создания монеты
    top: list[tuple[str, float]] = field(default_factory=list)   # (символ, итог копии) — для алерта

    def hot(self) -> str | None:
        """None — горячий, иначе — почему нет."""
        if self.own:
            return "покупает свои монеты (дев)"
        if self.bundled >= BUNDLE_SHARE:
            return f"бандл: {self.bundled:.0%} покупок в блоке создания"
        if self.coins < HOT_MIN_COINS:
            return f"монет с ценой {self.coins} < {HOT_MIN_COINS}"
        if self.noprice > HOT_MAX_NOPRICE:
            return f"без цены {self.noprice:.0%}"
        if self.avg < HOT_MIN_AVG:
            return f"в среднем {self.avg:+.1%}"
        if self.win < HOT_MIN_WIN:
            return f"в плюсе {self.win:.0%}"
        return None


def stats_of(address: str, copies: list[tuple[CM.Entry, CM.Copy]], trades: list[dict[str, Any]],
             symbols: dict[str, str] | None = None) -> Stats:
    s = Stats(address)
    s.total = len(copies)
    priced = [(e, c) for e, c in copies if c.res is not None]
    s.n = len(priced)
    s.coins = len({e.mint for e, _c in priced})
    if priced:
        res = [c.res for _e, c in priced]
        s.avg = statistics.fmean(res)
        s.win = sum(1 for x in res if x > 0) / len(res)
    s.noprice = 1 - s.n / s.total if s.total else 0.0
    first_buy: dict[str, float] = {}
    holds = []
    for t in sorted(trades, key=lambda x: x["ts"]):
        if t["is_buy"]:
            first_buy.setdefault(t["mint"], t["ts"])
            s.last_buy = max(s.last_buy or 0.0, t["ts"])
        elif t["mint"] in first_buy and first_buy[t["mint"]] is not None:
            holds.append(t["ts"] - first_buy[t["mint"]])
            first_buy[t["mint"]] = None          # первая продажа после первой покупки
    s.hold_s = statistics.median(holds) if holds else None
    best = sorted(priced, key=lambda x: -abs(x[1].res))[:6]
    s.top = [((symbols or {}).get(e.mint) or e.mint[:4], c.res) for e, c in best]
    return s


async def candidates(db: Any, since: float, until: float, min_coins: int = HOT_MIN_COINS,
                     limit: int = MAX_WALLETS) -> list[dict[str, Any]]:
    """Кошельки с покупками от min_coins разных монет за окно; своих монет и бандлы считаем тут же."""
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


async def evaluate(db: Any, address: str, since: float, until: float, limit: int = EVAL_LIMIT,
                   cand: dict[str, Any] | None = None) -> Stats:
    trades = await CM.wallet_trades(db, address, since, until)
    copies = await CM.evaluate_trades(db, address, trades, since, until, limit=limit)
    syms = {r["mint"]: r["symbol"] for r in await db.fetch(
        "SELECT mint, symbol FROM tokens WHERE mint = ANY($1::text[])", list({e.mint for e, _c in copies}))}
    s = stats_of(address, copies, trades, syms)
    if cand:
        s.own = int(cand.get("own") or 0)
        s.bundled = int(cand.get("bundled") or 0) / max(1, int(cand.get("coins") or 1))
    return s


def ago(sec: float | None) -> str:
    return fmt.ago(sec) if sec is not None else "?"


def alert_text(s: Stats, now: float) -> tuple[str, list[list[dict[str, Any]]]]:
    active = s.last_buy is not None and now - s.last_buy <= ACTIVE_S
    coins = ", ".join(f"${fmt.esc(sym)} {res:+.0%}" for sym, res in s.top)
    text = (f"🎯 <b>Охотник: горячий кошелёк</b> <code>{s.address}</code>\n"
            f"За {WINDOW_H} ч, копия «как бот» (блок +2, правило A, {STAKE_SOL:g} SOL, издержки тени): "
            f"<b>{s.coins} монет</b>, в среднем <b>{s.avg:+.1%}</b>, в плюсе <b>{s.win:.0%}</b> "
            f"({s.n} копий, без цены {s.noprice:.0%})\n"
            f"Удержание у него: медиана {ago(s.hold_s)} · последняя покупка {ago(now - s.last_buy) if s.last_buy else '?'}"
            f" назад — {'🟢 активен' if active else '⚪ сейчас не торгует'}\n"
            f"Монеты: {coins}")
    kb = [[{"text": "👻 Тень", "callback": f"hn:s:{s.address}"},
           {"text": f"💵 Бой {STAKE_SOL:g}", "callback": f"hn:l:{s.address}", "style": "danger"},
           {"text": "🙈 Игнор", "callback": f"hn:i:{s.address}"}]]
    return text, kb


def live_confirm(addr: str, cap_usd: float) -> tuple[str, list[list[dict[str, Any]]]]:
    text = (f"💵 <b>Бой за {addr[:4]}…{addr[-4:]}?</b>\nКаждая его покупка → покупка ботом на {STAKE_SOL:g} SOL "
            f"(не больше потолка сервера ${cap_usd:g}), выход — правило A. Только у кошельков бота в режиме «бой»; "
            "действуют лимит позиций, дневной и общий стоп. Остынет — бот снимет сам и напишет.")
    return text, [[{"text": "Да, в бой", "callback": f"hn:L:{addr}", "style": "danger"},
                   {"text": "Нет", "callback": f"hn:x:{addr}"}]]


# ---------------------------------------------------------------------- база
async def upsert(db: Any, s: Stats, status: str = "new") -> None:
    await db.execute(
        """INSERT INTO hunter_wallets(address, found_at, status, coins, avg_pct, win, hold_s, n_copies, noprice,
               last_alert_at)
           VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8, now())
           ON CONFLICT (address) DO UPDATE SET found_at = now(), status = $2, coins = $3, avg_pct = $4, win = $5,
               hold_s = $6, n_copies = $7, noprice = $8, last_alert_at = now(), followed_at = NULL, cooled_at = NULL,
               cool_reason = NULL""",
        s.address, status, s.coins, s.avg, s.win, s.hold_s, s.n, s.noprice)


async def follow(db: Any, cfg: dict[str, Any], costs: Any, addr: str, mode: str | None) -> None:
    """«Тень» / «Бой 0.14» / снять: кошелёк — в свой список (его покупки слушаем), «за ним» по правилу A."""
    from ..bot import board
    from .copytrade import compute_trader, linked_to_dev, qualifies, save_trader
    if mode:
        if not await db.fetchval("SELECT manual FROM traders WHERE address=$1", addr):
            s7, s30, _ = await compute_trader(db, addr, cfg, costs)
            linked = await linked_to_dev(db, addr)
            ok, _f = qualifies(s7, s30, linked, cfg)
            await save_trader(db, addr, s7, s30, ok, linked, label="охотник", manual=True)
        await db.execute("UPDATE traders SET active=true, manual=true WHERE address=$1", addr)
        await board.set_follow_rule(db, addr, "A", None)
    await board.set_follow(db, addr, mode)
    stakes = dict(await db.kv_get("real:follow_stake") or {})
    if mode == "live":
        stakes[addr] = STAKE_SOL
    else:
        stakes.pop(addr, None)
    await db.kv_set("real:follow_stake", stakes or None)
    if mode:
        await db.execute("UPDATE hunter_wallets SET status = $2, followed_at = COALESCE(followed_at, now()) "
                         "WHERE address = $1", addr, mode)
    await db.job("reload")


async def our_results(db: Any, addr: str, since: datetime, until: datetime | None = None) -> dict[str, Any]:
    """Наши сделки «за ним» (тень и бой) с момента, как начали повторять."""
    r = await db.fetchrow(
        """SELECT count(*) n, COALESCE(avg(pnl_pct), 0) a, COALESCE(sum(pnl_sol), 0) s,
                  count(*) FILTER (WHERE pnl_pct > 0) w, extract(epoch FROM max(opened_at)) AS last
           FROM real_positions WHERE trader = $1 AND opened_at >= $2 AND ($3::timestamptz IS NULL OR opened_at < $3)
             AND closed_at IS NOT NULL""", addr, since, until)
    return {"n": int(r["n"]), "avg": float(r["a"]), "sol": float(r["s"]), "win": int(r["w"]),
            "last": float(r["last"]) if r["last"] is not None else None}


def cool_reason(n24: int, avg24: float, last_buy: float | None, since: float, now: float) -> str | None:
    if n24 >= COOL_MIN_N and avg24 < 0:
        return f"за 24 ч {n24} сделок, в среднем {avg24:+.1%}"
    if now - max(last_buy or 0.0, since) >= COOL_IDLE_H * 3600:
        return f"нет покупок {COOL_IDLE_H} ч"
    return None


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
        if self.e.cfg.get("hunter_on"):
            await self.scan(now)
        await self.watch(now)
        await self.daily(now)
        await self.weekly(now)

    # ------------------------------------------------------------------ 1–3: поиск
    async def scan(self, now: float) -> list[Stats]:
        db = self.e.db
        since = now - WINDOW_H * 3600
        known = {r["address"]: r for r in await db.fetch(
            "SELECT address, status, cooled_at FROM hunter_wallets")}
        hot = []
        cands = await candidates(db, since, now)
        for c in cands:
            k = known.get(c["trader"])
            if k is not None and (k["status"] in ("new", "shadow", "live", "ignored") or (
                    k["cooled_at"] is not None and k["cooled_at"] > datetime.now(timezone.utc)
                    - timedelta(days=REALERT_COOLED_D))):
                continue
            if c["own"] or int(c["bundled"]) / max(1, int(c["coins"])) >= BUNDLE_SHARE:
                continue                              # дев своих монет / бандл — модель не считаем
            s = await evaluate(db, c["trader"], since, now, cand=c)
            if s.hot() is None:
                hot.append(s)
                await upsert(db, s)
                await self.say(*alert_text(s, now))
        self.stats = {"at": now, "candidates": len(cands), "hot": len(hot)}
        return hot

    # ------------------------------------------------------------------ 4: остыл
    async def watch(self, now: float) -> list[str]:
        db = self.e.db
        cooled = []
        for r in await db.fetch("SELECT address, status, followed_at FROM hunter_wallets "
                                "WHERE status IN ('shadow', 'live') AND followed_at IS NOT NULL"):
            addr = r["address"]
            day = await our_results(db, addr, max(r["followed_at"], datetime.fromtimestamp(now - 86400, timezone.utc)))
            last_buy = await db.fetchval("SELECT extract(epoch FROM max(ts)) FROM trades WHERE trader = $1 AND is_buy",
                                         addr)
            why = cool_reason(day["n"], day["avg"], float(last_buy) if last_buy else None,
                              r["followed_at"].timestamp(), now)
            if why is None:
                continue
            await follow(db, self.e.cfg, self.e.costs(), addr, None)
            await db.execute("UPDATE hunter_wallets SET status = 'cooled', cooled_at = now(), cool_reason = $2 "
                             "WHERE address = $1", addr, why)
            total = await our_results(db, addr, r["followed_at"])
            await self.say(f"🧊 <b>Остыл</b> <code>{addr}</code>: {why} — копирование ({'бой' if r['status'] == 'live' else 'тень'})"
                           f" остановлено.\nЗа всё время за ним: {total['n']} сд, в среднем {total['avg']:+.1%}, "
                           f"{total['sol']:+.3f} SOL.")
            cooled.append(addr)
        return cooled

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

    # ------------------------------------------------------------------ 5: неделя
    async def weekly(self, now: float) -> str | None:
        db = self.e.db
        if now - float(await db.kv_get(WEEK_KEY) or 0) < 7 * 86400:
            return None
        await db.kv_set(WEEK_KEY, now)
        text = await weekly_text(db, now)
        await self.say(text)
        return text


async def weekly_text(db: Any, now: float) -> str:
    since = datetime.fromtimestamp(now - 7 * 86400, timezone.utc)
    rows = await db.fetch("SELECT * FROM hunter_wallets WHERE found_at >= $1 OR followed_at >= $1 ORDER BY found_at",
                          since)
    found = [r for r in rows if r["found_at"] >= since]
    by = {}
    for r in found:
        by[r["status"]] = by.get(r["status"], 0) + 1
    names = {"new": "без решения", "shadow": "тень", "live": "бой", "ignored": "игнор", "cooled": "остыли"}
    lines = [f"🎯 <b>Охотник, неделя</b>: найдено горячих {len(found)}"
             + (" · " + ", ".join(f"{names.get(k, k)} {v}" for k, v in by.items()) if by else "")]
    ours = []
    for r in rows:
        if r["followed_at"] is None:
            continue
        res = await our_results(db, r["address"], r["followed_at"], r["cooled_at"])
        if res["n"]:
            ours.append(res)
    if ours:
        plus = sum(1 for x in ours if x["sol"] > 0)
        n = sum(x["n"] for x in ours)
        lines.append(f"За ними после начала повтора (наши сделки): в плюсе {plus} из {len(ours)} кошельков · "
                     f"{n} сд, {sum(x['sol'] for x in ours):+.3f} SOL")
    else:
        lines.append("За найденными ещё не повторяли (ни тени, ни боя).")
    model = []
    for r in found:
        start = r["found_at"].timestamp()
        trades = await CM.wallet_trades(db, r["address"], start, now)
        copies = await CM.evaluate_trades(db, r["address"], trades, start, now, limit=EVAL_LIMIT)
        res = [c.res for _e, c in copies if c.res is not None]
        if res:
            model.append(statistics.fmean(res))
    if model:
        lines.append(f"По модели после находки (все найденные, у кого были покупки): в плюсе "
                     f"{sum(1 for x in model if x > 0)} из {len(model)}, в среднем {statistics.fmean(model):+.1%}")
    return "\n".join(lines)


# ---------------------------------------------------------------------- исторический прогон
async def history(db: Any, days: float = 7.0, step_h: float = 6.0, max_wallets: int = 500,
                  until: float | None = None) -> str:
    """Как охотник сработал бы на прошлых данных: в каждой точке (шаг step_h) — горячие за 72 ч до неё; первый раз
    найденный кошелёк ведём вперёд по модели до «остыл» (минус на 10+ копиях за 24 ч или 12 ч без покупок) или до
    конца данных. Итог: сколько нашлось, сколько осталось в плюсе после находки."""
    until = until or time.time()
    start = until - days * 86400
    cands = await candidates(db, start, until, HOT_MIN_COINS, max_wallets)
    cands = [c for c in cands if not c["own"] and int(c["bundled"]) / max(1, int(c["coins"])) < BUNDLE_SHARE]
    found: list[dict[str, Any]] = []
    for c in cands:
        trades = await CM.wallet_trades(db, c["trader"], start, until)
        copies = await CM.evaluate_trades(db, c["trader"], trades, start, until, limit=100_000)
        if not copies:
            continue
        t = start + WINDOW_H * 3600
        while t <= until:
            win = [(e, x) for e, x in copies if t - WINDOW_H * 3600 <= e.ts < t]
            s = stats_of(c["trader"], win, [d for d in trades if t - WINDOW_H * 3600 <= d["ts"] < t])
            if s.hot() is None:
                fwd = forward(copies, [d for d in trades if d["ts"] >= t], t, until)
                found.append({"address": c["trader"], "at": t, "stats": s, **fwd})
                break
            t += step_h * 3600
    lines = [f"Исторический прогон охотника: {datetime.fromtimestamp(start, timezone.utc):%d.%m %H:%M} – "
             f"{datetime.fromtimestamp(until, timezone.utc):%d.%m %H:%M} UTC, шаг {step_h:g} ч, кошельков с "
             f"{HOT_MIN_COINS}+ монетами: {len(cands)}",
             f"Горячих нашлось бы: {len(found)}"]
    with_fwd = [f for f in found if f["n"]]
    if with_fwd:
        plus = [f for f in with_fwd if f["sum"] > 0]
        allres = [r for f in with_fwd for r in f["res"]]
        lines.append(f"После находки (модель, пока не остыл): копий {len(allres)}, в среднем "
                     f"{statistics.fmean(allres):+.1%}, в плюсе {sum(1 for x in allres if x > 0) / len(allres):.0%}; "
                     f"кошельков в плюсе {len(plus)} из {len(with_fwd)}; итог {sum(f['sum'] for f in with_fwd) * STAKE_SOL:+.3f} SOL "
                     f"при {STAKE_SOL:g} SOL на копию")
        lines.append(f"Без цены после находки: {sum(f['noprice'] for f in with_fwd)} копий "
                     "(до подписки на резервы цены PumpSwap в базе мало)")
        for f in sorted(with_fwd, key=lambda x: -x["sum"])[:15]:
            s = f["stats"]
            lines.append(f"  {f['address'][:4]}…{f['address'][-4:]} найден {datetime.fromtimestamp(f['at'], timezone.utc):%d.%m %H:%M}"
                         f" ({s.coins} мон., {s.avg:+.0%}, {s.win:.0%}) → {f['n']} коп., {statistics.fmean(f['res']):+.1%}"
                         f"{' · остыл: ' + f['cooled'] if f['cooled'] else ''}")
    elif found:
        lines.append("После находки копий с ценой нет — данных после находки не хватает.")
    return "\n".join(lines)


def forward(copies: list[tuple[CM.Entry, CM.Copy]], trades: list[dict[str, Any]], t0: float,
            until: float) -> dict[str, Any]:
    """Копии после находки t0 — пока не «остыл» (по модели: минус на 10+ копиях за 24 ч или 12 ч без покупок)."""
    res: list[float] = []
    noprice = 0
    cooled = None
    buys = sorted(d["ts"] for d in trades if d["is_buy"] and d["ts"] >= t0)
    last = t0
    done: list[tuple[float, float]] = []
    for e, c in sorted(((e, c) for e, c in copies if e.ts >= t0), key=lambda x: x[0].ts):
        if e.ts - last >= COOL_IDLE_H * 3600:
            cooled = f"нет покупок {COOL_IDLE_H} ч"
            break
        day = [r for ts, r in done if ts >= e.ts - 86400]
        if len(day) >= COOL_MIN_N and statistics.fmean(day) < 0:
            cooled = f"минус на {len(day)} копиях за 24 ч"
            break
        last = e.ts
        if c.res is None:
            noprice += 1
            continue
        res.append(c.res)
        done.append((e.ts, c.res))
    else:
        if (buys and until - buys[-1] >= COOL_IDLE_H * 3600) or (not buys and until - t0 >= COOL_IDLE_H * 3600):
            cooled = f"нет покупок {COOL_IDLE_H} ч"
    return {"n": len(res), "res": res, "sum": sum(res), "noprice": noprice, "cooled": cooled}
