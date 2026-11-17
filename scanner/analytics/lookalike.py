"""Поиск кошельков «по образцу smart A» — таких, как 7BNax.

Чем 7BNax отличается от остальных (бумага A, 14–17.11, честные входы): после его покупки цена почти сразу
растёт — копия по правилу A (вход через 2 блока после него, всё на +25%, выход за ним, если продал раньше) была
в плюсе в 81% сделок, в среднем +28%, тейк в среднем через 32 с. Обычная разведка ищет по прибыли самого кошелька
и проверяет копией в H (вход с задержкой копитрейда) — такой кошелёк там отсеивается: «дельта входа +43%».

Здесь меряется именно это свойство, по своей базе сделок (сделки каждой новой монеты первые 30 минут):
1. Активные кошельки за сутки: покупки 0.3–5 SOL, не дев монеты, не в первые 5 секунд (снайперы); 10–300 разных
   монет. Покупки — там, где в базе есть полная цена после них: на кривой pump.fun в первые 25 минут монеты и на
   PumpSwap (там торгует 7BNax), если монету записывали ещё до его покупки.
2. По каждому — до 40 последних покупок «копией A» на истории: вход — первая сделка через 2 блока после него,
   дальше 5 минут: цена дошла до +25% чистыми — тейк; он продал раньше — выход по его продаже; иначе — по цене
   через 5 минут. Комиссии — как у бумаги.
3. Кандидат: 10+ покупок, тейк в 60%+, в среднем +10%+ на копию, быстрых сбросов (продал за 20 с) не больше 30%.
4. Проверка вперёд — только на его покупках после находки: каждый час; как только набралось 10 новых (не раньше
   чем через 3 часа) — решение. Подтвердился — в smart (как кошельки разведки: метка 🧬, чистка по итогу копий
   в G); нет — отсев на 7 дней. За 3 дня не набрал 10 покупок — отсев.

Поиск — раз в 3 часа, проверка найденных — раз в час, с ограничением времени запросов.
"""
from __future__ import annotations

import logging
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from ..alerts.format import esc
from .copytrade import linked_to_dev

log = logging.getLogger(__name__)

SOURCE = "образец smart A"
STATUS = "lookalike"                    # статус в таблице scout на время проверки вперёд
DEFAULTS: dict[str, Any] = {
    "lk_enabled": True,
    "lk_every_h": 3.0,                  # как часто искать новых
    "lk_confirm_every_h": 1.0,          # как часто проверять найденных на новых сделках
    "lk_hours": 24.0,                   # за сколько часов брать покупки
    "lk_min_sol": 0.3, "lk_max_sol": 5.0,
    "lk_max_age_min": 25,               # кривая: покупки в первые N минут монеты (дальше данные о цене неполные)
    "lk_amm": True,                     # PumpSwap (там торгует 7BNax)
    "lk_amm_prior": 3,                  # PumpSwap: столько сделок других за 2 мин до покупки (монету уже записывали)
    "lk_min_coins": 10, "lk_max_coins": 300,
    "lk_traders": 400,                  # сколько активных кошельков проверять за раз
    "lk_sample": 40,                    # последних покупок на кошелёк
    "lk_tp": 0.25,                      # тейк чистыми, как у A
    "lk_fee": 0.03,                     # комиссии площадки и сети туда-обратно (доля)
    "lk_horizon_s": 300,
    "lk_min_n": 10, "lk_min_hit": 0.60, "lk_min_avg": 0.10, "lk_max_quick": 0.30,
    "lk_confirm_min_h": 3.0,            # решение — как только наберёт lk_min_n новых покупок, но не раньше
    "lk_max_promoted": 5,               # сколько кошельков «по образцу» держать в smart
    "lk_timeout_s": 120,
}
REJECT_DAYS = 7
QUICK_S = 20.0
WAIT_DAYS = 3.0
ENTRY_MAX_S = 10.0


def cfg_get(cfg: dict[str, Any], key: str) -> Any:
    v = cfg.get(key)
    return DEFAULTS[key] if v is None else v


def short(addr: str) -> str:
    return f"{addr[:4]}…{addr[-4:]}" if len(addr) > 10 else addr


@dataclass
class Copy:
    """Одна покупка кошелька, скопированная по правилу A на истории."""
    mint: str
    out: float           # итог копии, доля (0.25 = +25%)
    hit: bool            # тейк
    sold_s: float | None  # через сколько секунд он сам продал


@dataclass
class Eval:
    trader: str
    copies: list[Copy] = field(default_factory=list)

    @property
    def n(self) -> int:
        return len(self.copies)

    @property
    def hit(self) -> float:
        return sum(c.hit for c in self.copies) / self.n if self.n else 0.0

    @property
    def avg(self) -> float:
        return sum(c.out for c in self.copies) / self.n if self.n else 0.0

    @property
    def quick(self) -> float:
        return sum(1 for c in self.copies if c.sold_s is not None and c.sold_s < QUICK_S) / self.n if self.n else 0.0

    @property
    def hold(self) -> float | None:
        h = [c.sold_s for c in self.copies if c.sold_s is not None]
        return statistics.median(h) if h else None

    def good(self, cfg: dict[str, Any]) -> bool:
        return (self.n >= int(cfg_get(cfg, "lk_min_n")) and self.hit >= float(cfg_get(cfg, "lk_min_hit"))
                and self.avg >= float(cfg_get(cfg, "lk_min_avg")) and self.quick <= float(cfg_get(cfg, "lk_max_quick")))

    def text(self) -> str:
        if not self.n:
            return "покупок для проверки нет"
        hold = f", держит ~{self.hold:.0f} с" if self.hold is not None else ""
        return (f"{self.n} покупок: копия A в тейке {self.hit:.0%}, в среднем {self.avg:+.0%}, "
                f"быстрых сбросов {self.quick:.0%}{hold}")

    def stats(self) -> dict[str, Any]:
        return {"n": self.n, "hit": round(self.hit, 3), "avg": round(self.avg, 4), "quick": round(self.quick, 3),
                "hold_s": self.hold}


def copy_result(entry: float, hit_ts: float | None, sell_ts: float | None, sell_mc: float | None,
                last_mc: float | None, cfg: dict[str, Any]) -> tuple[float, bool]:
    """Итог копии по правилу A: тейк, если цена дошла до +tp чистыми раньше его продажи; иначе выход по его продаже
    или по цене в конце окна. Времена — секунды от входа."""
    tp, fee = float(cfg_get(cfg, "lk_tp")), float(cfg_get(cfg, "lk_fee"))
    if hit_ts is not None and (sell_ts is None or hit_ts <= sell_ts + 1.0):   # бумага видит его продажу через ~1 с
        return tp, True
    mc = sell_mc if sell_ts is not None and sell_mc else last_mc
    ratio = (mc / entry) if (mc and entry > 0) else 1.0
    return max(-1.0, ratio * (1 - fee) - 1), False


def hit_ratio(cfg: dict[str, Any]) -> float:
    """Во сколько раз должна вырасти цена, чтобы копия вышла на +tp чистыми."""
    return (1 + float(cfg_get(cfg, "lk_tp"))) / (1 - float(cfg_get(cfg, "lk_fee")))


def buys_sql(max_age_min: float, amm_prior: int | None) -> str:
    """Покупки, после которых в базе есть полная цена:
    • кривая pump.fun — в первые max_age_min минут монеты (сделки всех новых монет хранятся первые 30 минут);
    • PumpSwap (с 18.11: 7BNax торгует только там — 1080 покупок за 4 дня, ни одной на кривой, половина — в монеты
      старше 3 часов) — если монету уже записывали до его покупки: amm_prior+ сделок других кошельков за 2 минуты
      до неё. Сделки PumpSwap хранятся только у отслеживаемых монет; без этого условия попадали бы монеты, которые
      начали записывать из-за роста, — выборка сместилась бы к победителям. None — PumpSwap не смотреть."""
    if amm_prior is None:
        amm = "false"
    elif amm_prior <= 0:
        amm = "t.pool = 'pump-amm'"
    else:
        amm = (f"""t.pool = 'pump-amm' AND (SELECT count(*) FROM (SELECT 1 FROM trades p WHERE p.mint = t.mint
              AND p.ts >= t.ts - interval '120 seconds' AND p.ts < t.ts AND p.trader <> t.trader
              LIMIT {int(amm_prior)}) z) >= {int(amm_prior)}""")
    return f"""
    SELECT t.trader, t.mint, t.ts, t.slot
    FROM trades t JOIN tokens k ON k.mint = t.mint
    WHERE t.ts >= $1 AND t.ts < $2 AND t.is_buy
      AND t.sol_amount BETWEEN $3 AND $4
      AND t.ts > k.created_at + interval '5 seconds'
      AND t.trader <> k.deployer
      AND ((t.pool = 'pump' AND t.ts < k.created_at + make_interval(mins => {int(max_age_min)}))
           OR ({amm}))"""


def active_sql(max_age_min: float, amm_prior: int | None) -> str:
    return f"""
    SELECT trader, count(DISTINCT mint) coins FROM ({buys_sql(max_age_min, amm_prior)}) b
    GROUP BY 1 HAVING count(DISTINCT mint) BETWEEN $5 AND $6
    ORDER BY 2 DESC LIMIT $7"""


def copies_sql(max_age_min: float, amm_prior: int | None) -> str:
    return f"""
    WITH f AS (
        SELECT DISTINCT ON (trader, mint) * FROM ({buys_sql(max_age_min, amm_prior)} AND t.trader = ANY($5::text[])) b
        ORDER BY trader, mint, ts),
    b AS (SELECT *, row_number() OVER (PARTITION BY trader ORDER BY ts DESC) rn FROM f)
    SELECT b.trader, b.mint, e.mc entry, extract(epoch FROM e.ts) entry_ts,
           extract(epoch FROM h.hit_ts) hit_ts, extract(epoch FROM s.sell_ts) sell_ts, s.sell_mc, l.last_mc,
           extract(epoch FROM b.ts) buy_ts
    FROM b
    CROSS JOIN LATERAL (
        SELECT x.mc_sol mc, x.ts FROM trades x
        WHERE x.mint = b.mint AND x.ts >= b.ts AND x.ts <= b.ts + make_interval(secs => $8)
          AND CASE WHEN b.slot IS NOT NULL AND x.slot IS NOT NULL THEN x.slot >= b.slot + 2
                   ELSE x.ts >= b.ts + interval '800 milliseconds' END
        ORDER BY x.ts, x.id LIMIT 1) e
    LEFT JOIN LATERAL (
        SELECT min(y.ts) hit_ts FROM trades y
        WHERE y.mint = b.mint AND y.ts > e.ts AND y.ts <= e.ts + make_interval(secs => $7)
          AND y.mc_sol >= e.mc * $9) h ON true
    LEFT JOIN LATERAL (
        SELECT z.ts sell_ts, z.mc_sol sell_mc FROM trades z
        WHERE z.trader = b.trader AND z.mint = b.mint AND NOT z.is_buy AND z.ts > b.ts
          AND z.ts <= e.ts + make_interval(secs => $7)
        ORDER BY z.ts LIMIT 1) s ON true
    LEFT JOIN LATERAL (
        SELECT y.mc_sol last_mc FROM trades y
        WHERE y.mint = b.mint AND y.ts > e.ts AND y.ts <= e.ts + make_interval(secs => $7)
        ORDER BY y.ts DESC, y.id DESC LIMIT 1) l ON true
    WHERE b.rn <= $6 AND e.mc > 0"""


DIAG = """
    SELECT count(*) FILTER (WHERE t.is_buy) buys, count(*) FILTER (WHERE NOT t.is_buy) sells,
           count(*) FILTER (WHERE t.is_buy AND t.pool = 'pump') curve,
           count(*) FILTER (WHERE t.is_buy AND t.pool = 'pump-amm') amm,
           count(*) FILTER (WHERE t.is_buy AND t.sol_amount BETWEEN $4 AND $5) sized,
           count(*) FILTER (WHERE t.is_buy AND k.mint IS NOT NULL) known,
           count(DISTINCT t.mint) FILTER (WHERE t.is_buy) coins,
           percentile_disc(ARRAY[0.1, 0.25, 0.5, 0.75, 0.9]) WITHIN GROUP (
               ORDER BY extract(epoch FROM t.ts - k.created_at) / 60) FILTER (WHERE t.is_buy AND k.mint IS NOT NULL) age,
           percentile_disc(ARRAY[0.1, 0.5, 0.9]) WITHIN GROUP (ORDER BY t.sol_amount) FILTER (WHERE t.is_buy) size,
           min(t.ts) FILTER (WHERE t.is_buy) first_buy, max(t.ts) FILTER (WHERE t.is_buy) last_buy,
           string_agg(DISTINCT t.pool, ', ') pools
    FROM trades t LEFT JOIN tokens k ON k.mint = t.mint
    WHERE t.trader = $1 AND t.ts >= $2 AND t.ts < $3"""


async def diagnose(db: Any, cfg: dict[str, Any], wallet: str, until: datetime, hours: float) -> str:
    """Почему у кошелька мало покупок для проверки: что о нём есть в базе сделок."""
    r = await db.fetchrow(DIAG, wallet, until - timedelta(hours=hours), until,
                          float(cfg_get(cfg, "lk_min_sol")), float(cfg_get(cfg, "lk_max_sol")))
    if not r or not (r["buys"] or r["sells"]):
        return f"{wallet}: в базе сделок за {hours:g} ч нет ни одной его сделки"
    age = ", ".join(f"{a:.0f}" for a in (r["age"] or [])) or "—"
    size = ", ".join(f"{a:.2f}" for a in (r["size"] or [])) or "—"
    fmt = lambda ts: ts.strftime("%d.%m %H:%M") if ts else "—"  # noqa: E731
    return (f"{wallet}: покупок {r['buys']} (монет {r['coins']}), продаж {r['sells']}; на кривой pump {r['curve']}, "
            f"на PumpSwap {r['amm']}; "
            f"размер {cfg_get(cfg, 'lk_min_sol')}–{cfg_get(cfg, 'lk_max_sol')} SOL: {r['sized']}; монета в базе: "
            f"{r['known']}\n  возраст монеты при покупке, мин (10/25/50/75/90%): {age}\n"
            f"  размер покупки, SOL (10/50/90%): {size}\n  первая покупка {fmt(r['first_buy'])} UTC, последняя "
            f"{fmt(r['last_buy'])} UTC; площадки: {r['pools']}\n  в проверку идут покупки в первые "
            f"{cfg_get(cfg, 'lk_max_age_min')} мин монеты (--age N — шире) и PumpSwap, если монету записывали до "
            f"покупки (--all — все его покупки на PumpSwap)")


async def _fetch(db: Any, q: str, *a: Any, timeout_s: float = 120) -> list[Any]:
    """Тяжёлый запрос — с ограничением времени: при перегрузке базы отменится сам, а не будет мешать торговле."""
    async with db.p.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = {int(timeout_s * 1000)}")
            return list(await conn.fetch(q, *a))


async def evaluate(db: Any, cfg: dict[str, Any], until: datetime, hours: float | None = None,
                   wallets: list[str] | None = None) -> dict[str, Eval]:
    """Копии A на истории: для заданных кошельков или для всех активных за `hours` часов до `until`."""
    hours = float(hours or cfg_get(cfg, "lk_hours"))
    horizon = float(cfg_get(cfg, "lk_horizon_s"))
    t_to = until - timedelta(seconds=horizon + 60)             # у последних покупок окно 5 минут уже закрыто
    t_from = until - timedelta(hours=hours)
    lo, hi = float(cfg_get(cfg, "lk_min_sol")), float(cfg_get(cfg, "lk_max_sol"))
    timeout = float(cfg_get(cfg, "lk_timeout_s"))
    age = float(cfg_get(cfg, "lk_max_age_min"))
    prior = int(cfg_get(cfg, "lk_amm_prior")) if cfg_get(cfg, "lk_amm") else None
    if wallets is None:
        rows = await _fetch(db, active_sql(age, prior), t_from, t_to, lo, hi, int(cfg_get(cfg, "lk_min_coins")),
                            int(cfg_get(cfg, "lk_max_coins")), int(cfg_get(cfg, "lk_traders")), timeout_s=timeout)
        wallets = [r["trader"] for r in rows]
    out: dict[str, Eval] = {w: Eval(w) for w in wallets}
    ratio = hit_ratio(cfg)
    for i in range(0, len(wallets), 100):
        rows = await _fetch(db, copies_sql(age, prior), t_from, t_to, lo, hi, wallets[i:i + 100],
                            int(cfg_get(cfg, "lk_sample")),
                            horizon, ENTRY_MAX_S, ratio, timeout_s=timeout)
        for r in rows:
            e0 = float(r["entry_ts"])
            sell_ts = float(r["sell_ts"]) - e0 if r["sell_ts"] is not None else None
            hit_ts = float(r["hit_ts"]) - e0 if r["hit_ts"] is not None else None
            res, hit = copy_result(float(r["entry"]), hit_ts, sell_ts, r["sell_mc"], r["last_mc"], cfg)
            sold_s = float(r["sell_ts"]) - float(r["buy_ts"]) if r["sell_ts"] is not None else None
            out[r["trader"]].copies.append(Copy(r["mint"], res, hit, sold_s))
    return out


class Lookalike:
    """Раз в 3 часа ищет кандидатов; каждый час проверяет найденных на их новых сделках (после находки): набрали
    10 покупок и цифры повторились — в smart, не повторились — отсев."""

    def __init__(self, engine: Any):
        self.e = engine
        self.db = engine.db
        self.last: dict[str, Any] = {}

    @property
    def cfg(self) -> dict[str, Any]:
        return self.e.cfg

    async def due(self, now: datetime) -> bool:
        if not cfg_get(self.cfg, "lk_enabled"):
            return False
        last = await self.db.kv_get("lookalike:last")
        if not last:
            return True
        try:
            at = datetime.fromisoformat(last["at"])
        except Exception:  # noqa: BLE001
            return True
        return (now - at).total_seconds() >= float(cfg_get(self.cfg, "lk_every_h")) * 3600

    async def _excluded(self, cands: list[str]) -> set[str]:
        """Кого из кандидатов не брать: уже в списках, на проверке или в отсеве, девы и связанные с девами."""
        if not cands:
            return set()
        rows = await self.db.fetch(
            """SELECT address FROM traders WHERE address = ANY($2::text[]) AND (active OR manual OR pinned)
               UNION SELECT address FROM watchlist WHERE address = ANY($2::text[]) AND kind IN ('trader', 'dev')
               UNION SELECT address FROM scout WHERE address = ANY($2::text[])
                  AND (status IN ('probation', 'promoted', 'passed', $1)
                       OR (cooldown_until IS NOT NULL AND cooldown_until > now()))
               UNION SELECT deployer FROM tokens WHERE deployer = ANY($2::text[])
                  GROUP BY deployer HAVING count(*) >= 2""", STATUS, cands)
        out = {r["address"] for r in rows}
        for w in cands:
            if w not in out and await linked_to_dev(self.db, w):
                out.add(w)
        return out

    async def confirm_due(self, now: datetime) -> bool:
        if not cfg_get(self.cfg, "lk_enabled"):
            return False
        at = await self.db.kv_get("lookalike:confirm_at")
        try:
            return (now - datetime.fromisoformat(at)).total_seconds() >= \
                float(cfg_get(self.cfg, "lk_confirm_every_h")) * 3600
        except Exception:  # noqa: BLE001
            return True

    async def run(self, now: datetime | None = None, force: bool = False) -> list[str]:
        now = now or datetime.now(timezone.utc)
        events: list[str] = []
        if force or await self.confirm_due(now):          # найденные — каждый час, как только набрали покупок
            await self.db.kv_set("lookalike:confirm_at", now.isoformat())
            events += await self._confirm(now)
        if not force and not await self.due(now):
            return events
        t0 = time.monotonic()
        self.last = {}
        try:
            events += await self._discover(now)
        except Exception as exc:
            self.last["error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
            raise
        finally:
            self.last = {"at": now.isoformat(), "took_s": round(time.monotonic() - t0, 1), **self.last}
            await self.db.kv_set("lookalike:last", self.last)
        return events

    async def _discover(self, now: datetime) -> list[str]:
        evals = await evaluate(self.db, self.cfg, now)
        good = [w for w, ev in evals.items() if ev.good(self.cfg)]
        excluded = await self._excluded(good)
        found = [evals[w] for w in good if w not in excluded]
        found.sort(key=lambda ev: -ev.avg)
        # образец: как те же цифры выглядят у кошельков, которые уже в smart (для сверки в /scout)
        ref = await self._reference(now)
        self.last = {"checked": len(evals), "found": len(found),
                     "top": [{"w": ev.trader, **ev.stats()} for ev in sorted(evals.values(), key=lambda x: -x.avg)
                             if ev.n >= int(cfg_get(self.cfg, "lk_min_n"))][:10],
                     "reference": ref}
        events = []
        for ev in found[:20]:
            res = await self.db.execute(
                """INSERT INTO scout(address, status, source, reason, score, stats, found_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (address) DO UPDATE SET status=EXCLUDED.status, source=EXCLUDED.source,
                       reason=EXCLUDED.reason, score=EXCLUDED.score, stats=EXCLUDED.stats, found_at=EXCLUDED.found_at,
                       started_at=NULL, decided_at=NULL, decision=NULL, cooldown_until=NULL
                   WHERE scout.status IN ('queued', 'rejected', 'few', 'demoted', 'removed')
                     AND (scout.cooldown_until IS NULL OR scout.cooldown_until <= now())""",
                ev.trader, STATUS, SOURCE, ev.text(), round(ev.avg * 100, 2), {"found": ev.stats()}, now)
            if res.endswith("1"):
                events.append(f"🧬 похож на образец: <code>{ev.trader}</code> — {esc(ev.text())}. "
                              "Проверю на новых сделках через сутки.")
        return events

    async def _reference(self, now: datetime) -> list[dict[str, Any]]:
        # кошельки, давшие больше всего сделок бумаге A за неделю (7BNax и т. п.)
        rows = await self.db.fetch(
            """SELECT p.source_wallet address, max(t.label) label FROM paper_positions p
               LEFT JOIN traders t ON t.address = p.source_wallet
               WHERE p.portfolio = 'A' AND p.source_wallet IS NOT NULL AND p.opened_at > $1::timestamptz - interval '7 days'
               GROUP BY 1 ORDER BY count(*) DESC LIMIT 10""", now)
        if not rows:
            return []
        evals = await evaluate(self.db, self.cfg, now, wallets=[r["address"] for r in rows])
        labels = {r["address"]: r["label"] for r in rows}
        ref = [{"w": w, "label": labels.get(w), **ev.stats()} for w, ev in evals.items() if ev.n]
        return sorted(ref, key=lambda x: -x["avg"])[:8]

    async def _confirm(self, now: datetime) -> list[str]:
        rows = await self.db.fetch("SELECT address, found_at FROM scout WHERE status=$1", STATUS)
        due = [r for r in rows
               if (now - r["found_at"]).total_seconds() >= float(cfg_get(self.cfg, "lk_confirm_min_h")) * 3600]
        if not due:
            return []
        out: list[str] = []
        hours = float(cfg_get(self.cfg, "lk_hours"))
        for r in due:
            # только сделки после находки: проверка вперёд, а не тот же отрезок ещё раз
            since_h = min(hours, (now - r["found_at"]).total_seconds() / 3600)
            ev = (await evaluate(self.db, self.cfg, now, hours=since_h, wallets=[r["address"]]))[r["address"]]
            addr = r["address"]
            if ev.good(self.cfg):
                promoted = int(await self.db.fetchval(
                    "SELECT count(*) FROM scout WHERE status='promoted' AND source=$1", SOURCE) or 0)
                if promoted >= int(cfg_get(self.cfg, "lk_max_promoted")):
                    await self._close(addr, "passed", f"подтвердился ({ev.text()}), но мест для образца нет", 0, ev)
                    out.append(f"☑️ подтвердился, мест нет: <code>{addr}</code> — {esc(ev.text())}")
                    continue
                await self._promote(addr)
                await self._close(addr, "promoted", f"подтвердился на новых сделках: {ev.text()}", 0, ev)
                out.append(f"✅ в smart по образцу A: <code>{addr}</code> — {esc(ev.text())}")
            elif ev.n >= int(cfg_get(self.cfg, "lk_min_n")):
                await self._close(addr, "rejected", f"не подтвердился: {ev.text()}", REJECT_DAYS, ev)
                out.append(f"❌ не подтвердился {esc(short(addr))} — {esc(ev.text())}")
            elif (now - r["found_at"]).total_seconds() >= WAIT_DAYS * 86400:
                await self._close(addr, "few", f"за {WAIT_DAYS:.0f} дня мало покупок: {ev.text()}", REJECT_DAYS, ev)
        return out

    async def _close(self, addr: str, status: str, why: str, days: int, ev: Eval) -> None:
        await self.db.execute(
            """UPDATE scout SET status=$2, decided_at=now(), decision=$3,
                   stats = COALESCE(stats, '{}'::jsonb) || jsonb_build_object('confirm', $5::jsonb),
                   cooldown_until = CASE WHEN $4 > 0 THEN now() + make_interval(days => $4) END WHERE address=$1""",
            addr, status, why, days, ev.stats())

    async def _promote(self, addr: str) -> None:
        await self.db.execute(
            """INSERT INTO traders(address, label, manual, active, qualified, pinned, updated_at)
               VALUES ($1, $2, false, true, true, true, now())
               ON CONFLICT (address) DO UPDATE SET active=true, pinned=true, updated_at=now(),
                   label = CASE WHEN traders.label LIKE '🧬%' THEN traders.label ELSE $2 END""",
            addr, f"🧬 {short(addr)}")
        load = getattr(self.e, "_load_traders", None)
        if load is not None:
            await load()


async def report_lines(db: Any) -> list[str]:
    """Строки для /scout: что нашёл поиск по образцу и как те же цифры выглядят у кошельков smart."""
    last = await db.kv_get("lookalike:last") or {}
    rows = await db.fetch("SELECT address, reason, found_at FROM scout WHERE status=$1 ORDER BY score DESC", STATUS)
    lines = ["\n<b>🧬 Поиск по образцу smart A</b> (как 7BNax: после его покупки цена сразу растёт)"]
    if last.get("at"):
        lines.append(f"Последний поиск: {esc(str(last['at'])[:16].replace('T', ' '))} UTC за {last.get('took_s', '?')} с"
                     f" · проверено кошельков {last.get('checked', 0)}, подходят {last.get('found', 0)}")
    else:
        lines.append("Ещё не считался (раз в 3 часа).")
    if last.get("error"):
        lines.append(f"⚠️ Последний пересчёт не удался: {esc(last['error'])}")
    if rows:
        lines.append("На проверке (решение — когда наберёт 10 новых покупок, не раньше чем через 3 ч):")
    for r in rows[:8]:
        lines.append(f"• {esc(short(r['address']))} — {esc(r['reason'] or '')} · найден "
                     f"{r['found_at'].strftime('%d.%m %H:%M')} UTC")
    ref = last.get("reference") or []
    if ref:
        lines.append("Те же цифры у кошельков smart (за сутки):")
        for x in ref[:5]:
            lines.append(f"• {esc(x.get('label') or short(x['w']))}: {x['n']} пок., тейк {x['hit']:.0%}, "
                         f"в среднем {x['avg']:+.0%}")
    return lines


def format_cli(evals: dict[str, Eval], cfg: dict[str, Any], top: int = 25) -> str:
    rows = sorted(evals.values(), key=lambda ev: (-(ev.n >= int(cfg_get(cfg, "lk_min_n"))), -ev.avg))
    lines = [f"Проверено кошельков: {len(evals)}; подходят под образец: {sum(ev.good(cfg) for ev in evals.values())}",
             "кошелёк | покупок | тейк | в среднем | быстрых сбросов | держит, с | подходит"]
    for ev in rows[:top]:
        hold = f"{ev.hold:.0f}" if ev.hold is not None else "—"
        lines.append(f"{ev.trader} | {ev.n} | {ev.hit:.0%} | {ev.avg:+.1%} | {ev.quick:.0%} | {hold} | "
                     f"{'да' if ev.good(cfg) else 'нет'}")
    return "\n".join(lines)
