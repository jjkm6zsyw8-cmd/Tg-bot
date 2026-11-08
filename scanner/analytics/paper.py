"""Блок E: бумажная торговля 24/7 — без реальных денег, ключей и транзакций.

* Три портфеля по $30 на одних и тех же входах: A (стоп −6%), B (базовая схема), C (только экстренный
  выход и цель). Учёт в SOL, показ в SOL и USD.
* Вход по каждому алерту выбранных уровней («Сильный», «Обычный» — меняется в /settings): позиция 20%
  текущего баланса, не больше 3 открытых позиций и одной позиции на монету.
* Реалистичное исполнение: вход по цене через 5 с после алерта, выход через 2 с после сигнала;
  проскальзывание — по состоянию bonding curve с учётом размера позиции; комиссия Pump.fun — фактическая
  по сделкам (fee в TradeEvent), иначе из настроек; плюс приоритетная комиссия за каждую транзакцию.
Реальной торговли нет и не будет без отдельного решения пользователя.
"""
from __future__ import annotations

import asyncio
import csv
import io
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from ..alerts import format as fmt
from .curve import buy_tokens_out, reserves_for_mc, sell_sol_out

if TYPE_CHECKING:
    from .engine import Engine
    from .state import TokenState

log = logging.getLogger(__name__)

START_USD = 100.0                     # старт портфеля (paper_start_usd): $100 — доллары в масштабе реального банка
POSITION_SHARE = 0.20                 # по умолчанию; меняется в /settings (paper_position_share)
MAX_OPEN = 3
# Какие сигналы в какие портфели (разбор 6–7.11, без сбоев цены, на портфель): одиночные копии +$29, smart +$9,
# «Наблюдение» +$12 только за счёт 90CENT ×55 (без неё −$39), сильные копии −$32, «Мой дев» −$8, тренды −$7,
# новые −$5. Основные портфели — одиночные копии и smart; лотерея «Наблюдения» — отдельно в D и E: маленький
# билет, много мест и дневной стоп, чтобы плохой день стоил −$10, а не −$22, а ракета ×5…×55 давала плюс.
LOTTERY = "DE"
LOTTERY_SHARE = 0.10          # билет лотереи — 10% баланса…
LOTTERY_MIN_SOL = 0.02        # …но не меньше 0.02 SOL (~$1.8; 90CENT 6.11 куплена за 0.024)
LOTTERY_MAX_OPEN = 8
LOTTERY_DAY_STOP_USD = 35.0   # лотерея потеряла за день $35 (треть стартовых $100) — до 00:00/сброса новых билетов нет
# Проверка фильтра: D берёт только запуски, где дев купил ~1 SOL (3–3.5% монет у кластера дева в момент алерта),
# E — все. По 9008 алертам «Наблюдения»: у таких ракет ×3+ 4.2% против 2.1% у остальных (оба дня одинаково)
DAY_CAP_USD = 35.0            # днём в позициях лотереи не больше $35 (треть старта)
LOTTERY_FILTERED = "D"
LOTTERY_DEV_SHARE = (0.030, 0.035)
ENTRY_DELAY_S = 5.0
EXIT_DELAY_S = 2.0
MAX_HOLD_S = 2 * 3600          # страховка: позиция по «мёртвой» монете закрывается через 2 часа

SCHEMES: dict[str, dict[str, Any]] = {
    "A": {"name": "A: стоп −6%", "stop": 0.06, "tp_net": 0.30, "tp_share": 0.5, "trail": 0.15, "target": True,
          "time_stop_s": 600, "time_stop_gain": 0.10},
    "B": {"name": "B: базовая", "stop": 0.15, "tp_net": 0.30, "tp_share": 0.5, "trail": 0.15, "target": True,
          "time_stop_s": 600, "time_stop_gain": 0.10},
    "C": {"name": "C: только экстренный выход и цель", "stop": None, "tp_net": None, "tp_share": 0.0, "trail": None,
          "target": True, "time_stop_s": None, "time_stop_gain": None},
    "D": {"name": "D: тейк 50% на +20%, трейл 10%, стоп −10%", "stop": 0.10, "tp_net": 0.20, "tp_share": 0.5,
          "trail": 0.10, "target": True, "time_stop_s": 600, "time_stop_gain": 0.10},
    "E": {"name": "E: тейк 50% на +50%, трейл 20%, стоп −15%", "stop": 0.15, "tp_net": 0.50, "tp_share": 0.5,
          "trail": 0.20, "target": True, "time_stop_s": 900, "time_stop_gain": 0.10},
    "F": {"name": "F: без тейка, трейл 15% после +15%, стоп −10%", "stop": 0.10, "tp_net": None, "tp_share": 0.0,
          "trail": 0.15, "trail_after_net": 0.15, "target": True, "time_stop_s": 600, "time_stop_gain": 0.10},
    "G": {"name": "G: скальп — всё на +25%, стоп −8%, 5 мин", "stop": 0.08, "tp_net": 0.25, "tp_share": 1.0,
          "trail": None, "target": False, "time_stop_s": 300, "time_stop_gain": 0.10},
}
LEGEND = ("A, B, C, F, G — одиночные копии и smart: A стоп−6% · B база · C только цель · F трейл после +15% · "
          "G скальп +25% · D, E — лотерея «Наблюдение»: D тейк+20%, только если дев купил ~1 SOL · E тейк+50%, все · "
          "билет 10% (от $1.8), до 8 мест, дневной стоп −$35 · старт $100")
# Копии выходят по правилу своего портфеля. Было единое правило для всех (½ на +30%, трейл 15%, стоп −20%, без
# выхода по цели) — оно срезало ровно то, что давало плюс: 6.11 вечером копии в C (только цель) и F (трейл после
# +15%) дали +1.0…+1.1 SOL на портфель (CHICXU ×7, POOH ×2.3, blob ×2.3), а ночью 7.11 с единым правилом все
# портфели ушли в −0.15…−0.22 SOL.
START_MC_SOL = 32.0   # MC до ~32 SOL — стартовая цена кривой: источник ещё не видел сделок по монете
REENTRY_COOLDOWN_MIN = 60   # после убыточного выхода в ту же монету этот портфель час не входит
RUG_PCT = 0.5         # монета дала −50% и хуже — раг: больше не входим ни в неё, ни в монеты её дева (7 дней)
TICK_S = 2.0          # проверка открытых позиций (стоп/тейк/трейлинг) раз в 2 с
FRESH_S = 120.0       # цена из своего потока сделок годится, только если по монете были сделки за 2 минуты
JUMP_X = 5.0          # скачок цены больше чем в 5 раз за одну проверку — ждём подтверждения
MISMATCH_X = 2.0      # свой поток и DexScreener расходятся больше чем в 2 раза — верим DexScreener
ANOMALY_PCT = 10.0    # результат больше +1000% — проверяем цену выхода через Jupiter (⚠, если не подтвердилась)
LEVEL_SIZE = {"strong": 1.5, "normal": 0.75, "watch": 0.5}   # размер позиции по силе сигнала (× базовая доля)
PERIOD_NAMES = {1: "за 1 час", 2: "за 2 часа", 6: "за 6 часов", 24: "за 24 часа", 168: "за 7 дней",
                None: "за всё время"}
PERIOD_KEYBOARD = [
    [{"text": "1 ч", "callback": "pp:1"}, {"text": "2 ч", "callback": "pp:2"}, {"text": "6 ч", "callback": "pp:6"}],
    [{"text": "24 ч", "callback": "pp:24"}, {"text": "7 д", "callback": "pp:168"}, {"text": "Всё", "callback": "pp:all"}],
    [{"text": "Последние 20", "callback": "pp:hist"}, {"text": "Все сделки за 24 ч", "callback": "pp:day"}],
    [{"text": "📄 Файл всех сделок (Excel)", "callback": "pp:csv"}],
]
MODE_SHORT = {"trend": "тренд", "dev": "дев", "smart": "смарт", "new": "новая", "new_dev_wallet": "дев",
              "copy": "копия", "mydev": "мой дев"}


def wallet_name(r: Any) -> str:
    """Чья это сделка: подпись кошелька из своего списка или короткий адрес."""
    w = r.get("source_wallet") or ""
    return r.get("source_label") or (f"{w[:4]}…{w[-4:]}" if w else "")
EXIT_SHORT = {"emergency": "дев продал", "trader_sell": "трейдер продал", "stop": "стоп", "take_profit": "тейк",
              "target": "цель", "trailing": "трейлинг", "time_stop": "нет роста", "max_hold": "2ч без выхода"}
EXIT_NAMES = {"emergency": "экстренный выход (продажа кластера дева)", "trader_sell": "трейдер-источник продал",
              "stop": "стоп-лосс", "take_profit": "тейк-профит 50%", "target": "цель", "trailing": "трейлинг-стоп",
              "time_stop": "стоп по времени (нет +10% за 5–15 мин)", "max_hold": "максимальное удержание"}


# ---------------------------------------------------------------------- исполнение (чистые функции)
@dataclass
class Fill:
    sol: float          # вход: потрачено всего; выход: получено чистыми
    tokens: float
    fee_sol: float      # комиссия Pump.fun + приоритетная
    mc: float


def buy_fill(mc: float, sol_in: float, fee_bps: float, priority_sol: float) -> Fill:
    """Покупка на sol_in SOL: приоритет и комиссия кривой вычитаются, остаток идёт в кривую (проскальзывание)."""
    v_sol, v_tok = reserves_for_mc(mc)
    fee = (sol_in - priority_sol) * fee_bps / 10_000
    net = max(0.0, sol_in - priority_sol - fee)
    return Fill(sol=sol_in, tokens=buy_tokens_out(v_sol, v_tok, net), fee_sol=fee + priority_sol, mc=mc)


def sell_fill(mc: float, tokens: float, fee_bps: float, priority_sol: float) -> Fill:
    v_sol, v_tok = reserves_for_mc(mc)
    gross = sell_sol_out(v_sol, v_tok, tokens)
    fee = gross * fee_bps / 10_000
    return Fill(sol=max(0.0, gross - fee - priority_sol), tokens=tokens, fee_sol=fee + priority_sol, mc=mc)


@dataclass
class Position:
    id: int
    portfolio: str
    mint: str
    opened_ts: float
    entry_mc: float
    cost_sol: float
    tokens: float
    tokens_initial: float
    target_mc: float | None
    trader: str | None = None
    symbol: str = ""
    proceeds_sol: float = 0.0
    max_mc: float = 0.0
    max_net: float = -1.0
    tp_done: bool = False
    closing: bool = False
    level: str = "normal"
    alert_id: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    rule: str | None = None          # "copy" — копия своего кошелька (выход — по схеме портфеля)

    def net_if_sold(self, mc: float, fee_bps: float, priority_sol: float) -> float:
        value = sell_fill(mc, self.tokens, fee_bps, priority_sol).sol if self.tokens > 0 else 0.0
        return (self.proceeds_sol + value) / self.cost_sol - 1 if self.cost_sol else 0.0


def decide_exit(pos: Position, mc: float, now: float, scheme: dict[str, Any], fee_bps: float,
                priority_sol: float, emergency: str | None = None) -> tuple[str, float] | None:
    """Решение по позиции: (причина, доля остатка к продаже) или None."""
    if emergency:
        return emergency, 1.0
    pos.max_mc = max(pos.max_mc, mc)
    net = pos.net_if_sold(mc, fee_bps, priority_sol)
    pos.max_net = max(pos.max_net, net)
    if scheme.get("stop") is not None and mc <= pos.entry_mc * (1 - scheme["stop"]):
        return "stop", 1.0
    if scheme.get("target") and pos.target_mc and mc >= pos.target_mc:
        return "target", 1.0
    if scheme.get("tp_net") is not None and not pos.tp_done and net >= scheme["tp_net"]:
        return "take_profit", scheme["tp_share"]
    trail_on = pos.tp_done or (scheme.get("trail_after_net") is not None and pos.max_net >= scheme["trail_after_net"])
    if scheme.get("trail") is not None and trail_on and mc <= pos.max_mc * (1 - scheme["trail"]):
        return "trailing", 1.0
    if (scheme.get("time_stop_s") and now - pos.opened_ts >= scheme["time_stop_s"]
            and pos.max_net < scheme["time_stop_gain"] and not pos.tp_done):
        return "time_stop", 1.0
    if now - pos.opened_ts >= MAX_HOLD_S:
        return "max_hold", 1.0
    return None


# ---------------------------------------------------------------------- движок бумажной торговли
class PaperTrader:
    def __init__(self, engine: "Engine"):
        self.e = engine
        self.db = engine.db
        self.positions: dict[int, Position] = {}
        self.balance: dict[str, float] = {}
        self.paused: dict[str, bool] = {}
        self.skipped: dict[str, str] = {}          # причина, почему не было входов за час
        self.lock = asyncio.Lock()
        self._buf: list[str] = []                   # входы и выходы копятся и уходят одним сообщением раз в минуту
        self._exits: dict[str, list[tuple[str, float, str]]] = {}
        self.bad_prices = 0                         # отброшенных сбойных цен DexScreener
        self.false_exits = 0                        # ценовых выходов, не подтвердившихся через 2 с
        self.entry_fixed = 0                        # входов, где цену поправил Jupiter (свой поток отстал)
        self._last_flush = 0.0
        self.loss_exits: dict[tuple[str, str], float] = {}   # (портфель, монета) → время убыточного выхода
        self.rugged: dict[str, float] = {}         # монета → худший результат (−0.5 и хуже): больше не входим
        self.rug_devs: set[str] = set()            # девы этих монет: в их новые монеты тоже не входим
        self.day_pnl: dict[str, float] = {}        # итог закрытых сделок портфеля за сегодня (Батуми), SOL
        self._day_key = ""

    @property
    def cfg(self) -> dict[str, Any]:
        return self.e.cfg

    def tz(self) -> ZoneInfo:
        try:
            return ZoneInfo(str(self.cfg.get("timezone") or "UTC"))
        except Exception:  # noqa: BLE001
            return ZoneInfo("UTC")

    def state_price(self, mint: str) -> float | None:
        """MC из своего потока сделок — только если он свежий. Без сделок (PumpPortal не отдаёт)
        цена монеты застревает на стартовой (~28 SOL), и по ней нельзя ни входить, ни выходить."""
        st = self.e.states.get(mint)
        if st is None or not st.mc or st.mc <= 0:
            return None
        trades = getattr(st, "trades", None)
        if not trades or time.time() - trades[-1].ts > FRESH_S:
            return None
        return float(st.mc)

    async def jupiter_mc(self, mint: str) -> float | None:
        """MC по цене Jupiter, SOL (pump.fun: 1 млрд токенов) или None."""
        ext = getattr(self.e, "ext", None)
        jp = await ext.jup_price(mint) if ext is not None else None
        if not jp:
            return None
        return jp * 1e9 / (self.e.price.value or 150)

    async def arbitrate(self, mint: str, own: float, dex: float) -> str:
        """Свой поток и DexScreener расходятся больше чем вдвое — чья цена верна ("own" или "dex").
        Решает Jupiter (чья ближе). Без Jupiter: стартовая цена кривой (~28 SOL) — признак источника, который ещё
        не видел сделок: у своего потока — DexScreener прав, у DexScreener (свежая монета, у него 0–5 сделок) —
        прав свой поток. Иначе — DexScreener, как раньше."""
        jm = await self.jupiter_mc(mint)
        if jm:
            return "own" if abs(math.log(jm / own)) <= abs(math.log(jm / dex)) else "dex"
        if dex <= START_MC_SOL < own:
            return "own"
        return "dex"

    async def jupiter_check(self, mint: str, price_usd: float) -> bool | None:
        """True — Jupiter подтверждает цену (±2×), False — расходится, None — у Jupiter нет цены."""
        ext = getattr(self.e, "ext", None)
        jp = await ext.jup_price(mint) if ext is not None else None
        if not jp or price_usd <= 0:
            return None
        return 0.5 <= price_usd / jp <= 2.0

    async def jupiter_agrees(self, mint: str, price_usd: float) -> bool:
        ok = await self.jupiter_check(mint, price_usd) is True
        if not ok:
            log.warning("paper %s: цена %.3g $ не подтверждена Jupiter — скачок пропускаю", mint, price_usd)
        return ok

    async def dex_price(self, mint: str, ttl: float = 1.0) -> float | None:
        ext = getattr(self.e, "ext", None)
        if ext is None:
            return None
        pair = await ext.dex_pair(mint, timeout=2.0, ttl=ttl)
        if pair and pair.get("market_cap_usd"):
            return pair["market_cap_usd"] / (self.e.price.value or 150)
        return None

    def fee_bps(self) -> float:
        return float(self.e.fee_bps or self.e.settings.costs().get("curve_fee_bps", 125))

    def priority(self) -> float:
        return float(self.cfg.get("paper_priority_sol", 0.0005))

    def share(self) -> float:
        return min(0.5, max(0.05, float(self.cfg.get("paper_position_share") or POSITION_SHARE)))

    def share_for(self, level: str) -> float:
        """Базовая доля × сила сигнала: «Сильный» ×1.5, «Обычный» ×0.75 (если включено), не больше 50%."""
        base = self.share()
        if self.cfg.get("paper_size_by_level", True):
            base *= LEVEL_SIZE.get(level, 1.0)
        return min(0.5, base)

    def levels(self) -> set[str]:
        return set(self.cfg.get("paper_levels") or ["strong", "normal"])

    async def start(self) -> None:
        sol = self.e.price.value or 150
        for name, scheme in SCHEMES.items():
            await self.db.execute(
                """INSERT INTO paper_portfolios(name, scheme, start_usd, start_sol, balance_sol)
                   VALUES ($1, $2, $3, $4, $4) ON CONFLICT (name) DO NOTHING""",
                name, scheme, self.start_usd(), self.start_usd() / sol)
        for r in await self.db.fetch("SELECT * FROM paper_portfolios"):
            self.balance[r["name"]] = r["balance_sol"]
            self.paused[r["name"]] = r["paused"]
        for r in await self.db.fetch("SELECT * FROM paper_positions WHERE closed_at IS NULL"):
            self.positions[r["id"]] = Position(
                id=r["id"], portfolio=r["portfolio"], mint=r["mint"], opened_ts=r["opened_at"].timestamp(),
                entry_mc=r["entry_mc"], cost_sol=r["cost_sol"], tokens=r["tokens"], tokens_initial=r["tokens_initial"],
                target_mc=r["target_mc"], symbol=r["symbol"] or "", proceeds_sol=r["proceeds_sol"],
                max_mc=r["max_mc"] or r["entry_mc"], tp_done=r["tp_done"], level=r["level"] or "normal",
                alert_id=r["alert_id"], extra={"last_mc": r["last_mc"]} if r.get("last_mc") else {},
                rule=r.get("rule"))
        for r in await self.db.fetch(
                """SELECT portfolio, mint, max(closed_at) t FROM paper_positions WHERE pnl_sol < 0
                   AND exit_reason <> 'reset' AND closed_at > now() - interval '2 hours' GROUP BY 1, 2"""):
            self.loss_exits[(r["portfolio"], r["mint"])] = r["t"].timestamp()
        for r in await self.db.fetch(
                """SELECT p.mint, min(p.pnl_pct) pct, max(t.deployer) dev FROM paper_positions p
                   LEFT JOIN tokens t ON t.mint = p.mint
                   WHERE p.pnl_pct <= $1 AND p.exit_reason <> 'reset' AND NOT p.suspect
                     AND p.closed_at > now() - interval '7 days' GROUP BY p.mint""", -RUG_PCT):
            self.rugged[r["mint"]] = r["pct"]
            if r["dev"]:
                self.rug_devs.add(r["dev"])
        self._day_key = self.day_key()
        # дневной стоп лотереи считает итог с 00:00 (Батуми) или с последнего сброса — что позже:
        # ручной /paper reset должен снимать стоп (регрессия 8.11: лотерея простояла всю ночь после сброса)
        start = datetime.now(self.reset_tz()).replace(hour=0, minute=0, second=0, microsecond=0)
        last_reset = await self.db.fetchval("SELECT max(reset_at) FROM paper_portfolios")
        if last_reset and last_reset > start:
            start = last_reset
        for r in await self.db.fetch(
                """SELECT portfolio, sum(pnl_sol) s FROM paper_positions WHERE closed_at >= $1
                   AND exit_reason <> 'reset' AND NOT suspect GROUP BY portfolio""", start):
            self.day_pnl[r["portfolio"]] = float(r["s"] or 0)

    async def reset(self) -> None:
        async with self.lock:
            sol = self.e.price.value or 150
            await self.db.execute("UPDATE paper_positions SET closed_at=now(), exit_reason='reset' WHERE closed_at IS NULL")
            await self.db.execute("UPDATE paper_portfolios SET start_usd=$1, start_sol=$2, balance_sol=$2, paused=false, "
                                  "reset_at=now()", self.start_usd(), self.start_usd() / sol)
            self.positions.clear()
            self.day_pnl = {}                      # сброс — новый счёт и для дневного стопа лотереи
            for n in SCHEMES:
                self.balance[n] = self.start_usd() / sol
                self.paused[n] = False
            await self.db.kv_set("paper:compared", None)

    # ------------------------------------------------------------------ вход
    async def on_alert(self, mint: str, level: str, alert_id: int | None, data: dict[str, Any],
                       trader: str | None) -> None:
        if level not in self.levels():
            self.skipped.setdefault("*", f"уровень «{level}» не выбран для бумажной торговли")
            return
        if data.get("trend") and not self.cfg.get("paper_trends", False):
            self.skipped.setdefault("*", "«Тренды» не входят в бумажную торговлю (настройка)")
            return
        targets, why = self.route(data, level)
        if not targets:
            self.skipped["*"] = why
            return
        await asyncio.sleep(ENTRY_DELAY_S)          # реакция пользователя
        # «Тренды» — только по DexScreener (по нему дан алерт); остальное — свой свежий поток, сверка с DexScreener
        src = "dex" if data.get("trend") else "auto"
        own = None if src == "dex" else self.state_price(mint)
        dex = await self.dex_price(mint)            # цена через 5 с после алерта
        ext = getattr(self.e, "ext", None)
        if dex and ext is not None and not own:
            pair = await ext.dex_pair(mint, timeout=2.0, ttl=1.0)
            jp = await ext.jup_price(mint)
            pu = (pair or {}).get("price_usd")
            if jp and pu and not 0.5 <= pu / jp <= 2.0:
                if dex <= START_MC_SOL and src != "dex":
                    # DexScreener на стартовой цене кривой — он ещё не видел сделок; у Jupiter цена свежая
                    dex, src = jp * 1e9 / (self.e.price.value or 150), "jup"
                else:
                    log.warning("paper %s: DexScreener %.3g $ и Jupiter %.3g $ расходятся — не входим", mint, pu, jp)
                    self.skipped.setdefault("*", "цена DexScreener не сходится с Jupiter")
                    return
        if own and dex and max(own, dex) / min(own, dex) > MISMATCH_X:
            pick = await self.arbitrate(mint, own, dex)
            log.warning("paper %s: свой поток %.1f и DexScreener %.1f SOL расходятся — берём %s", mint, own, dex, pick)
            if pick == "dex":
                own, src = None, "dex"
        ref = data.get("copy_ref_mc")
        if not own and dex and dex <= START_MC_SOL and ref and ref > 1.5 * dex:
            # DexScreener ещё показывает стартовую цену кривой, а трейдер только что купил вдвое дороже —
            # DexScreener не видел сделок. Решает Jupiter; без него надёжной цены нет — не входим
            jm = await self.jupiter_mc(mint)
            if jm is None:
                self.skipped["*"] = "копия: DexScreener ещё на стартовой цене, Jupiter цену не знает"
                return
            dex, src = jm, "jup"
        mc = own or dex
        if mc is None and src == "dex":
            mc = data.get("mc")                     # цена алерта «Трендов» (тоже DexScreener)
        if not mc or mc <= 0:
            self.skipped.setdefault("*", "нет свежей цены монеты")
            return
        if src != "dex":
            # Цена входа — то, по чему можно купить сейчас. Свой поток видит не все сделки монеты: 8.11 у 15 из 22
            # прибыльных сделок вход был записан ниже всей минутной свечи (×1.1–1.7), плюс бумаги завышался
            # (+$182 против +$106 по рынку). Jupiter считает цену по текущему состоянию пула — ей и верим,
            # если она в разумных пределах (×3: дальше скорее сбой самого Jupiter)
            jm = await self.jupiter_mc(mint)
            if jm and 1 / 3 <= jm / mc <= 3 and abs(jm / mc - 1) > 0.10:
                log.info("paper %s: вход по Jupiter %.1f SOL вместо %.1f (свой поток отстал)", mint, jm, mc)
                mc = jm
                self.entry_fixed += 1
        guard = getattr(self.e, "copyguard", None)
        if guard is not None and data.get("copy_wallet") and data.get("copy_ref_mc"):
            # Price Delta: насколько дороже цели мы входим (+5 с реакции) — копить по кошельку
            delta = await guard.record_delta(data["copy_wallet"], mint, float(data["copy_ref_mc"]), float(mc))
            why = guard.entry_reason(delta)
            if why:                                  # эта копия: уже поздно — покупали бы на вершине его закупа
                self.skipped["*"] = "копия: " + why
                await guard.mark_skipped(data["copy_wallet"], mint, why)
                return
        async with self.lock:
            sizes = []
            for name in SCHEMES:
                size = await self._open(name, mint, level, alert_id, data, trader, mc, src) if name in targets else None
                sizes.append(f"{name} {size:.3f}" if size else f"{name} —")
        if any(not x.endswith("—") for x in sizes):
            sol_usd = self.e.price.value or 150
            await self._msg(f"📥 ${fmt.esc(data.get('symbol') or '?')} · MC {fmt.mc_usd(mc, sol_usd)} · "
                            + " · ".join(sizes) + " SOL")

    async def _open(self, name: str, mint: str, level: str, alert_id: int | None, data: dict[str, Any],
                    trader: str | None, mc: float, src: str = "auto") -> float | None:
        open_here = [p for p in self.positions.values() if p.portfolio == name]
        if any(p.mint == mint for p in open_here):
            self.skipped[name] = "уже есть позиция по этой монете"
            return None
        lottery = self.is_lottery(name)
        max_open = LOTTERY_MAX_OPEN if lottery else MAX_OPEN
        if len(open_here) >= max_open:
            self.skipped[name] = f"лимит {max_open} открытых позиций"
            return None
        if lottery and self.lottery_stopped(name):
            self.skipped[name] = (f"лотерея: за сегодня −${float(self.cfg.get('paper_lottery_day_stop_usd', LOTTERY_DAY_STOP_USD)):g}"
                                  " — дневной стоп до 00:00")
            return None
        if mint in self.rugged:
            # регрессия 6.11: D семь раз за 20 минут входил в раг NORTHKOREA (−56…−99% каждый раз)
            self.skipped[name] = f"монета уже рагнула ({self.rugged[mint]:+.0%}) — больше не входим"
            return None
        dev = getattr(self.e.states.get(mint), "deployer", None) if getattr(self.e, "states", None) else None
        if dev and dev in self.rug_devs and dev not in (getattr(self.e, "my_devs", None) or {}):
            self.skipped[name] = "дев этой монеты уже рагал — не входим"
            return None
        lost = self.loss_exits.get((name, mint))
        cool = float(self.cfg.get("paper_reentry_cooldown_min", REENTRY_COOLDOWN_MIN))
        if lost and cool > 0 and time.time() - lost < 60 * cool:
            # регрессия 6.11: D семь раз за 20 минут входил в один и тот же раг NORTHKOREA (−1.1 SOL за час)
            self.skipped[name] = f"монета уже дала убыток {fmt.ago(time.time() - lost)} назад — повторно не входим"
            return None
        size = self.balance[name] * (float(self.cfg.get("paper_lottery_share", LOTTERY_SHARE)) if lottery
                                     else self.share_for(level))
        # минимальная позиция: на ~$2 фиксированные комиссии (приоритет 2×0.0005 SOL + pump.fun) съедали ~7% сделки;
        # билет лотереи меньше — выигрыши там ×3…×55, а на дорогих билетах портфель вставал на паузу к вечеру
        min_size = max(float(self.cfg.get("paper_lottery_min_sol", LOTTERY_MIN_SOL)) if lottery
                       else float(self.cfg.get("paper_min_position_sol", 0.05)), 10 * self.priority())
        if data.get("copy_sol"):
            # Ratio: копия — доля от суммы покупки цели (её уверенность), потолок — обычная доля портфеля
            ratio = float(self.cfg.get("copy_ratio", 0.10))
            size = min(size, ratio * float(data["copy_sol"]))
        size = max(size, min_size)                    # не мельче минимума…
        size = min(size, self.balance[name] * 0.5)    # …но и не больше половины баланса
        cap = self.day_cap_sol() if lottery else None     # дневной лимит суммы — только на лотерею
        if cap is not None and size >= min_size:
            # днём торговля в минусе (6.11: копии 08–18 −0.03…−0.44 SOL во всех портфелях) — днём в позициях
            # не больше лимита, ночью и вечером — как обычно
            room = cap - sum(p.cost_sol for p in open_here)
            if room < min_size:
                self.skipped[name] = (f"днём не больше ${float(self.cfg.get('paper_day_cap_usd', DAY_CAP_USD)):g} "
                                      "в позициях — лимит занят")
                return None
            size = min(size, room)
        if size < min_size:
            if not self.paused.get(name):
                self.paused[name] = True
                await self.db.execute("UPDATE paper_portfolios SET paused=true WHERE name=$1", name)
                await self._msg(f"⏸ Портфель {name}: баланс {self.balance[name]:.4f} SOL — меньше двух минимальных "
                                f"позиций ({2 * min_size:.3f} SOL). Торговля на паузе, /paper reset — сброс.")
            self.skipped[name] = "пауза: мал баланс"
            return None
        fill = buy_fill(mc, size, self.fee_bps(), self.priority())
        if fill.tokens <= 0:
            return None
        self.balance[name] -= size
        target = data.get("target_mc")
        rule = "copy" if data.get("copy_wallet") else None
        now = datetime.now(timezone.utc)
        pid = await self.db.fetchval(
            """INSERT INTO paper_positions(portfolio, mint, alert_id, level, symbol, opened_at, entry_mc, cost_sol,
                   tokens, tokens_initial, max_mc, target_mc, rule, source_wallet)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$7,$10,$11,$12) RETURNING id""",
            name, mint, alert_id, level, data.get("symbol"), now, mc, size, fill.tokens, target, rule,
            data.get("copy_wallet") or data.get("source_wallet"))
        await self.db.execute(
            "INSERT INTO paper_trades(position_id, portfolio, side, ts, mc, sol, tokens, fee_sol, reason) "
            "VALUES ($1,$2,'buy',$3,$4,$5,$6,$7,'алерт')", pid, name, now, mc, size, fill.tokens, fill.fee_sol)
        await self.db.execute("UPDATE paper_portfolios SET balance_sol=$2 WHERE name=$1", name, self.balance[name])
        self.positions[pid] = Position(id=pid, portfolio=name, mint=mint, opened_ts=time.time(), entry_mc=mc,
                                       cost_sol=size, tokens=fill.tokens, tokens_initial=fill.tokens, target_mc=target,
                                       trader=trader, symbol=data.get("symbol") or "", max_mc=mc, level=level,
                                       alert_id=alert_id, extra={"src": src, "last_mc": mc}, rule=rule)
        self.skipped.pop(name, None)
        return size

    # ------------------------------------------------------------------ сопровождение
    async def on_trade(self, st: "TokenState", eff: Any, trader: str, is_buy: bool) -> None:
        ps = [p for p in self.positions.values() if p.mint == st.mint and not p.closing]
        if not ps:
            return
        emergency = None
        if eff is not None and eff.cluster_sell:
            emergency = "emergency"
        for p in ps:
            reason = emergency
            if reason is None and p.trader and trader == p.trader and not is_buy:
                reason = "trader_sell"
            if reason is None and p.extra.get("src") == "dex":
                continue          # цену «Трендов» ведём по DexScreener, из своего потока — только экстренные сигналы
            await self._check(p, st.mc, reason)

    async def tick_loop(self) -> None:
        last_report = time.time()
        while True:
            await asyncio.sleep(TICK_S)
            try:
                ps = list(self.positions.values())
                own = {p.id: (None if p.extra.get("src") == "dex" else self.state_price(p.mint)) for p in ps}
                # без свежего своего потока — одной пачкой в DexScreener (до 30 монет за запрос)
                need = [p.mint for p in ps if own[p.id] is None]
                ext = getattr(self.e, "ext", None)
                pairs = await ext.dex_pairs(need, timeout=3.0, ttl=TICK_S) if need and ext is not None else {}
                sol_usd = self.e.price.value or 150
                for p in ps:
                    mc = own[p.id]
                    price_usd = None
                    if mc is None:
                        pair = pairs.get(p.mint)
                        if pair and pair.get("market_cap_usd"):
                            mc = pair["market_cap_usd"] / sol_usd
                            price_usd = pair.get("price_usd")
                    if not mc:
                        # цены нет (монета без сделок и без пары на DexScreener): правила по времени всё равно
                        # должны срабатывать, иначе позиция зависает навсегда и занимает слот
                        mc = float(p.extra.get("last_mc") or p.entry_mc)
                    await self._check(p, mc, None, price_usd)
                if time.time() - self._last_flush >= 60:
                    self._last_flush = time.time()
                    await self.flush_messages()
                    # последняя цена открытых позиций — в БД: отчёт /paper строит бот сам, без аналитики
                    await self.db.kv_set("paper:skipped", dict(self.skipped) or None)   # причины — для отчёта из бота
                    marks = [(p.id, float(p.extra["last_mc"])) for p in self.positions.values() if p.extra.get("last_mc")]
                    if marks:
                        await self.db.p.executemany("UPDATE paper_positions SET last_mc=$2 WHERE id=$1", marks)
                if time.time() - last_report >= 3600:
                    last_report = time.time()
                    await self.report_hourly()
                await self.maybe_daily_reset()
                await self.maybe_compare()
            except Exception:  # noqa: BLE001
                log.exception("paper tick")

    async def _check(self, p: Position, mc: float, emergency: str | None, price_usd: float | None = None) -> None:
        """price_usd — цена DexScreener для этой проверки (None — цена из своего потока сделок)."""
        scheme = self.scheme_for(p)
        last = p.extra.get("last_mc")
        if emergency is None and last and (mc > JUMP_X * last or mc < last / JUMP_X):
            if price_usd is not None:
                # DexScreener может стабильно показывать неверную цену (пара к экзотическому токену):
                # скачок принимаем, только если его подтверждает независимый Jupiter
                if not await self.jupiter_agrees(p.mint, price_usd):
                    self.bad_prices += 1
                    return
            else:
                # свой поток: неверный пул или котировка дают СТАБИЛЬНО неверную цену — повтор её не опровергает.
                # Сверяем с Jupiter (pump.fun: 1 млрд токенов → цена = MC / 1e9); нет цены у Jupiter — ждём повтора
                verdict = await self.jupiter_check(p.mint, mc * (self.e.price.value or 150) / 1e9)
                if verdict is False:
                    self.bad_prices += 1
                    log.warning("paper %s: свой поток MC %.0f SOL не сходится с Jupiter — пропускаю", p.symbol, mc)
                    return
                if verdict is None:
                    pending = p.extra.get("jump")
                    if pending is None or not (0.5 < mc / pending < 2.0):
                        p.extra["jump"] = mc     # скачок в 5+ раз ждёт подтверждения следующей проверкой
                        return
        p.extra.pop("jump", None)
        p.extra["last_mc"] = mc
        decision = decide_exit(p, mc, time.time(), scheme, self.fee_bps(), self.priority(), emergency)
        if decision is None or p.closing:
            return
        reason, share = decision
        p.closing = True
        asyncio.create_task(self._exit_later(p, reason, share))

    async def _exit_later(self, p: Position, reason: str, share: float) -> None:
        await asyncio.sleep(EXIT_DELAY_S)            # реакция на сигнал выхода
        mc = await self.current_mc(p)                # ТЕКУЩАЯ цена через 2 с, а не максимум за позицию
        if not self.still_valid(p, reason, mc):
            # один «провальный» замер цены (смена пары у DexScreener, сбой) выбивал стоп во всех портфелях,
            # а продажа шла уже по нормальной цене: «−5% стоп» у схем со стопом −15%. Ценовой выход —
            # только если через 2 с условие всё ещё выполняется; экстренные выходы — без проверки
            p.closing = False
            self.false_exits += 1
            log.info("paper %s %s: %s не подтвердился (MC %.1f) — позиция остаётся", p.portfolio, p.symbol, reason, mc)
            return
        async with self.lock:
            await self._sell(p, reason, share, mc)

    @staticmethod
    def scheme_for(p: Position) -> dict[str, Any]:
        return SCHEMES[p.portfolio]

    def still_valid(self, p: Position, reason: str, mc: float) -> bool:
        scheme = self.scheme_for(p)
        if reason == "stop" and scheme.get("stop") is not None:
            return mc <= p.entry_mc * (1 - scheme["stop"])
        if reason == "trailing" and scheme.get("trail") is not None:
            return mc <= p.max_mc * (1 - scheme["trail"])
        if reason == "target" and p.target_mc:
            return mc >= p.target_mc
        if reason == "take_profit" and scheme.get("tp_net") is not None:
            return p.net_if_sold(mc, self.fee_bps(), self.priority()) >= scheme["tp_net"]
        return True

    async def current_mc(self, p: Position) -> float:
        """Текущий MC: свежий свой поток → DexScreener → последнее подтверждённое значение (не максимум).
        Цена выхода не может быть в 5+ раз выше последней подтверждённой (защита от сбоя данных)."""
        last = float(p.extra.get("last_mc") or p.entry_mc)
        mc = None if p.extra.get("src") == "dex" else self.state_price(p.mint)
        if mc is None:
            mc = await self.dex_price(p.mint)
        if mc is None:
            return last
        return last if mc > JUMP_X * last else mc

    async def _sell(self, p: Position, reason: str, share: float, mc: float) -> None:
        tokens = p.tokens * share if share < 1 else p.tokens
        fill = sell_fill(mc, tokens, self.fee_bps(), self.priority())
        p.tokens -= tokens
        p.proceeds_sol += fill.sol
        self.balance[p.portfolio] += fill.sol
        now = datetime.now(timezone.utc)
        await self.db.execute(
            "INSERT INTO paper_trades(position_id, portfolio, side, ts, mc, sol, tokens, fee_sol, reason) "
            "VALUES ($1,$2,'sell',$3,$4,$5,$6,$7,$8)", p.id, p.portfolio, now, mc, fill.sol, tokens, fill.fee_sol, reason)
        await self.db.execute("UPDATE paper_portfolios SET balance_sol=$2 WHERE name=$1", p.portfolio,
                              self.balance[p.portfolio])
        sol_usd = self.e.price.value or 150
        if share < 1 and p.tokens > 0:
            p.tp_done = True
            p.closing = False
            await self.db.execute("UPDATE paper_positions SET tokens=$2, proceeds_sol=$3, tp_done=true, max_mc=$4 "
                                  "WHERE id=$1", p.id, p.tokens, p.proceeds_sol, p.max_mc)
            await self._msg(f"💰 {p.portfolio} ${fmt.esc(p.symbol or '?')} тейк {share:.0%} · MC {fmt.mc_usd(mc, sol_usd)}")
            return
        p.max_mc = max(p.max_mc or 0.0, mc)           # максимум не ниже цены выхода (gris 8.11: выход 6.3k, «макс» 5.3k)
        pnl = p.proceeds_sol - p.cost_sol
        pct = pnl / p.cost_sol if p.cost_sol else 0.0
        suspect = False
        if pct > ANOMALY_PCT:
            # больше +1000%: настоящая ракета или сбой цены? Спрашиваем Jupiter про цену выхода
            suspect = await self.jupiter_check(p.mint, mc * (self.e.price.value or 150) / 1e9) is False
        await self.db.execute(
            """UPDATE paper_positions SET tokens=0, proceeds_sol=$2, closed_at=$3, exit_reason=$4, pnl_sol=$5,
                   pnl_pct=$6, max_mc=$7, suspect=$8 WHERE id=$1""", p.id, p.proceeds_sol, now, reason, pnl, pct,
            p.max_mc, suspect)
        self.positions.pop(p.id, None)
        if pnl < 0:
            self.loss_exits[(p.portfolio, p.mint)] = time.time()
        if not suspect:
            if self._day_key != self.day_key():
                self._day_key, self.day_pnl = self.day_key(), {}
            self.day_pnl[p.portfolio] = self.day_pnl.get(p.portfolio, 0.0) + pnl
        if pct <= -RUG_PCT and not suspect:
            self.rugged[p.mint] = min(pct, self.rugged.get(p.mint, 0.0))
            dev = getattr(self.e.states.get(p.mint), "deployer", None) if getattr(self.e, "states", None) else None
            if dev:
                self.rug_devs.add(dev)
        min_pos = max(float(self.cfg.get("paper_min_position_sol", 0.05)), 10 * self.priority())
        if self.paused.get(p.portfolio) and self.balance[p.portfolio] >= 2 * min_pos:
            self.paused[p.portfolio] = False
            await self.db.execute("UPDATE paper_portfolios SET paused=false WHERE name=$1", p.portfolio)
        self._exits.setdefault(p.symbol or "?", []).append((p.portfolio, pct, EXIT_SHORT.get(reason, reason)))

    def start_usd(self) -> float:
        return float(self.cfg.get("paper_start_usd") or START_USD)

    def is_lottery(self, name: str) -> bool:
        return name in str(self.cfg.get("paper_lottery_portfolios", LOTTERY))

    def route(self, data: dict[str, Any], level: str) -> tuple[set[str], str]:
        """В какие портфели идёт сигнал и почему никуда, если пусто. Без modes (старые вызовы) — во все."""
        modes = set(data.get("modes") or [])
        if not modes:
            return set(SCHEMES), ""
        lottery = {n for n in SCHEMES if self.is_lottery(n)}
        main = set(SCHEMES) - lottery
        out: set[str] = set()
        why = ""
        if "copy" in modes:
            if len(data.get("traders") or []) >= 2 and not self.cfg.get("paper_strong_copies", False):
                why = "сильная копия (2+ кошелька) — в бумагу не берём: за 2 дня −$32"
            else:
                out |= main
        if "smart" in modes:
            out |= main
        if "dev" in modes:
            lot = set(lottery)
            share = (data.get("metrics") or {}).get("cluster")
            lo = float(self.cfg.get("paper_lottery_dev_share_min", LOTTERY_DEV_SHARE[0]))
            hi = float(self.cfg.get("paper_lottery_dev_share_max", LOTTERY_DEV_SHARE[1]))
            if share is None or not lo <= float(share) < hi:
                for n in lot & set(str(self.cfg.get("paper_lottery_filtered", LOTTERY_FILTERED))):
                    lot.discard(n)
                    self.skipped[n] = (f"{n}: дев купил не ~1 SOL при запуске (доля {float(share or 0):.1%}, "
                                       f"нужно {lo:.1%}–{hi:.1%})")
            out |= lot
        if not out and not why:
            if "mydev" in modes:
                why = "«Мой дев» — в бумагу не берём: за 2 дня −$8"
            elif modes & {"new", "new_dev_wallet"}:
                why = "новые монеты — в бумагу не берём: за 2 дня −$5"
            else:
                why = "этот сигнал в бумагу не берём"
        return out, why

    def reset_tz(self) -> ZoneInfo:
        try:
            return ZoneInfo(str(self.cfg.get("paper_reset_tz") or "Asia/Tbilisi"))
        except Exception:  # noqa: BLE001
            return ZoneInfo("Asia/Tbilisi")

    def day_key(self) -> str:
        return datetime.now(self.reset_tz()).date().isoformat()

    def lottery_stopped(self, name: str) -> bool:
        """Лотерея портфеля за сегодня в минусе больше дневного стопа (по закрытым сделкам)."""
        stop_usd = float(self.cfg.get("paper_lottery_day_stop_usd", LOTTERY_DAY_STOP_USD) or 0)
        if stop_usd <= 0:
            return False
        if self._day_key != self.day_key():
            self._day_key, self.day_pnl = self.day_key(), {}
        return self.day_pnl.get(name, 0.0) * (self.e.price.value or 150) <= -stop_usd

    def day_cap_sol(self, now: datetime | None = None) -> float | None:
        """Лимит суммы в открытых позициях днём (SOL) или None — вне дневных часов или лимит выключен (0)."""
        cap_usd = float(self.cfg.get("paper_day_cap_usd", DAY_CAP_USD) or 0)
        if cap_usd <= 0:
            return None
        try:
            tz = ZoneInfo(str(self.cfg.get("paper_reset_tz") or "Asia/Tbilisi"))
        except Exception:  # noqa: BLE001
            tz = ZoneInfo("Asia/Tbilisi")
        hour = (now or datetime.now(timezone.utc)).astimezone(tz).hour
        start, end = int(self.cfg.get("paper_day_from_h", 8)), int(self.cfg.get("paper_day_to_h", 18))
        if not (start <= hour < end):
            return None
        return cap_usd / (self.e.price.value or 150)

    async def _msg(self, text: str) -> None:
        self._buf.append(text)

    async def flush_messages(self) -> None:
        # выходы по одной монете из всех портфелей — одной строкой: ✅ $SYM A +12% цель · B +8% трейлинг …
        for sym, xs in sorted(self._exits.items()):
            avg = sum(x[1] for x in xs) / len(xs)
            self._buf.append(f"{'✅' if avg > 0 else '❌'} ${fmt.esc(sym)} " + " · ".join(
                f"{n} {pct:+.0%} {why}" for n, pct, why in sorted(xs)))
        self._exits.clear()
        if not self._buf:
            return
        lines, self._buf = self._buf[:40], self._buf[40:]
        await self.db.enqueue("text", {"topic": "paper", "text": "\n".join(lines)}, priority=7)

    # ------------------------------------------------------------------ отчёты
    async def summary(self, name: str, since: datetime | None = None) -> dict[str, Any]:
        port = await self.db.fetchrow("SELECT * FROM paper_portfolios WHERE name=$1", name)
        rows = await self.db.fetch(
            "SELECT * FROM paper_positions WHERE portfolio=$1 AND closed_at IS NOT NULL AND exit_reason <> 'reset' "
            "AND opened_at >= $2 AND ($3::timestamptz IS NULL OR closed_at >= $3)", name, port["reset_at"], since)
        wins = [r["pnl_sol"] for r in rows if (r["pnl_sol"] or 0) > 0]
        losses = [r["pnl_sol"] for r in rows if (r["pnl_sol"] or 0) <= 0]
        open_ps = [p for p in self.positions.values() if p.portfolio == name]
        open_value = 0.0
        open_lines = []
        for p in open_ps:
            mc = float(p.extra.get("last_mc") or p.entry_mc)   # последняя подтверждённая цена, не максимум
            v = sell_fill(mc, p.tokens, self.fee_bps(), self.priority()).sol if p.tokens else 0.0
            open_value += v
            open_lines.append((p, (p.proceeds_sol + v) / p.cost_sol - 1 if p.cost_sol else 0.0))
        equity = self.balance.get(name, port["balance_sol"]) + open_value
        best = max(rows, key=lambda r: r["pnl_pct"] or 0, default=None)
        worst = min(rows, key=lambda r: r["pnl_pct"] or 0, default=None)
        return {"port": port, "trades": len(rows), "wins": len(wins),
                "anomalies": sum(1 for r in rows if r.get("suspect")), "pnl_sol": sum(r["pnl_sol"] or 0 for r in rows),
                "avg_win": sum(wins) / len(wins) if wins else 0.0, "avg_loss": sum(losses) / len(losses) if losses else 0.0,
                "equity": equity, "open": open_lines, "best": best, "worst": worst}

    async def _row(self, since: datetime | None) -> dict[str, dict[str, Any]]:
        return {name: await self.summary(name, since) for name in SCHEMES}

    async def render_period(self, hours: float | None, hourly: bool = False) -> str:
        """Все портфели одним компактным сообщением: итог за период, всего, баланс, сделки, открытые."""
        sol_usd = self.e.price.value or 150
        since = datetime.now(timezone.utc) - timedelta(hours=hours) if hours else None
        per = await self._row(since)
        tot = per if hours is None else await self._row(None)
        title = PERIOD_NAMES.get(hours) or f"за {hours:g} ч"   # f-строка в get() считалась и при hours=None
        lines = [f"📒 <b>Бумага · {title}</b> · позиция {self.share():.0%}"
                 + (" (×1.5 сильный / ×0.75 обычный)" if self.cfg.get("paper_size_by_level", True) else "")]

        no_trades = hours is not None and not any(per[n]["trades"] for n in SCHEMES)
        if no_trades:
            # сколько алертов было за период (по базе) и последняя причина, по которой бумага не вошла
            # (её пишет аналитика: бот, который строит этот отчёт, сам её не знает)
            cnt = await self.db.fetch(
                "SELECT mode, count(*) n FROM alerts WHERE created_at > now() - make_interval(mins => $1) GROUP BY 1",
                int(hours * 60))
            names = {"copy": "копии", "mydev": "мои девы", "dev": "девы", "new": "новые", "trend": "тренды",
                     "smart": "смарт"}
            parts = ", ".join(f"{names.get(r['mode'], r['mode'])} {r['n']}" for r in cnt)
            saved = await self.db.kv_get("paper:skipped", {}) or {}
            why = (self.skipped.get("*") or next((v for v in self.skipped.values()), "")
                   or saved.get("*") or next((v for v in saved.values()), ""))
            lines.append(f"⏸ сделок нет · алертов за период: {parts or '0'}" + (f" · бумага не вошла: {why}" if why else ""))
        # таблица: портфель · за период · всего · баланс · сделки (доля прибыльных); ★ — лидер по «всего»
        leader = max(SCHEMES, key=lambda n: tot[n]["equity"] / tot[n]["port"]["start_sol"])
        head = ("    " + (f"{'период':>7}" if hours is not None else "") + f"{'всего':>7}{'баланс':>8}  сделки")
        rows = [head]
        for n in SCHEMES:
            start_sol = tot[n]["port"]["start_sol"]
            cnt = per[n]["trades"]
            row = f"{n}{'★' if n == leader else ' '}  "
            if hours is not None:
                row += f"{per[n]['pnl_sol'] / per[n]['port']['start_sol']:>+7.1%}"
            row += f"{tot[n]['equity'] / start_sol - 1:>+7.0%}{'$' + format(tot[n]['equity'] * sol_usd, '.1f'):>8}  {cnt}"
            if cnt:
                row += f" ({per[n]['wins'] / cnt:.0%})"
            rows.append(row)
        lines.append("<pre>" + "\n".join(rows) + "</pre>")
        # открытые позиции — по монетам: 🟢 $SYM A +5% B +3% …
        by_mint: dict[str, list[str]] = {}
        for n in SCHEMES:
            for p, net in tot[n]["open"]:
                by_mint.setdefault(p.symbol or "?", []).append(f"{n} {net:+.0%}")
        for sym, xs in by_mint.items():
            lines.append(f"🟢 ${fmt.esc(sym)} " + " ".join(xs))
        odd = sum(per[n]["anomalies"] for n in SCHEMES)
        if odd:
            lines.append(f"⚠ {odd} сделок с ценой, которую не подтвердил Jupiter (вероятный сбой) — "
                         "см. «Последние 20 сделок»")
        if not hourly:
            lines.append(f"<i>{LEGEND}</i>")
        if hourly:
            self.skipped.clear()
        return "\n".join(lines)

    async def render(self, hourly: bool = False) -> str:
        return await self.render_period(1 if hourly else None, hourly=hourly)

    async def report_hourly(self) -> None:
        await self.flush_messages()
        await self.db.enqueue("text", {"topic": "paper", "text": await self.render(hourly=True),
                                       "keyboard": PERIOD_KEYBOARD}, priority=8)

    async def history(self, n: int = 20) -> str:
        rows = await self.db.fetch(
            """SELECT p.*, (SELECT t.mc FROM paper_trades t WHERE t.position_id = p.id AND t.side = 'sell'
                            ORDER BY t.ts DESC LIMIT 1) AS exit_mc
               FROM paper_positions p WHERE p.closed_at IS NOT NULL AND p.exit_reason <> 'reset'
               ORDER BY p.closed_at DESC LIMIT $1""", n)
        if not rows:
            return "📒 Закрытых сделок пока нет."
        sol_usd = self.e.price.value or 150
        out = [f"📒 <b>Последние {len(rows)} сделок</b>"]
        for r in rows:
            pct = r["pnl_pct"] or 0
            mark = "⚠" if r.get("suspect") else "✅" if (r["pnl_sol"] or 0) > 0 else "❌"
            mcs = (f" · {fmt.mc_usd(r['entry_mc'], sol_usd)}→{fmt.mc_usd(r['exit_mc'], sol_usd)}"
                   if r["entry_mc"] and r["exit_mc"] else "")
            out.append(f"{mark} {r['portfolio']} ${fmt.esc(r['symbol'] or '?')} {pct:+.0%} · "
                       f"{EXIT_SHORT.get(r['exit_reason'], r['exit_reason'])}{mcs} · "
                       f"{r['closed_at'].astimezone(self.tz()):%H:%M}")
        return "\n".join(out)

    async def _closed(self, since: datetime | None) -> list[Any]:
        return await self.db.fetch(
            """SELECT p.*, a.mode, (SELECT t.mc FROM paper_trades t WHERE t.position_id = p.id AND t.side = 'sell'
                                    ORDER BY t.ts DESC LIMIT 1) AS exit_mc,
                      (SELECT w.label FROM watchlist w WHERE w.address = p.source_wallet AND w.label IS NOT NULL
                       LIMIT 1) AS source_label
               FROM paper_positions p LEFT JOIN alerts a ON a.id = p.alert_id
               WHERE p.closed_at IS NOT NULL AND p.exit_reason <> 'reset'
                 AND ($1::timestamptz IS NULL OR p.closed_at >= $1) ORDER BY p.opened_at, p.portfolio""", since)

    async def trades_text(self, hours: float = 24) -> list[str]:
        """Все сделки за период: одна строка на монету (вход), в ней результаты всех портфелей.
        Возвращает части по ≤ 3800 символов — лимит сообщения Telegram."""
        rows = await self._closed(datetime.now(timezone.utc) - timedelta(hours=hours))
        if not rows:
            return [f"📒 За {hours:g} ч закрытых сделок нет."]
        sol_usd = self.e.price.value or 150
        groups: dict[tuple[str, Any], list[Any]] = {}
        for r in rows:
            groups.setdefault((r["mint"], r["alert_id"] or r["opened_at"].replace(second=0, microsecond=0)), []).append(r)
        lines = []
        for rs in groups.values():
            first = rs[0]
            avg = sum(r["pnl_pct"] or 0 for r in rs) / len(rs)
            odd = any(r.get("suspect") for r in rs)
            mark = "⚠" if odd else "✅" if avg > 0 else "❌"
            res = " · ".join(f"{r['portfolio']} {(r['pnl_pct'] or 0):+.0%} {EXIT_SHORT.get(r['exit_reason'], r['exit_reason'])}"
                             for r in sorted(rs, key=lambda x: x["portfolio"]))
            head_parts = [mark, f"{first['opened_at'].astimezone(self.tz()):%H:%M}",
                          f"${fmt.esc(first['symbol'] or '?')}", MODE_SHORT.get(first["mode"] or "", ""),
                          fmt.esc(f"({wallet_name(first)})") if first.get("source_wallet") else "",
                          fmt.mc_usd(first["entry_mc"], sol_usd)]
            lines.append(" ".join(x for x in head_parts if x) + f" · {res}")
        wins = sum(1 for r in rows if (r["pnl_sol"] or 0) > 0)
        head = (f"📒 <b>Все сделки за {hours:g} ч</b> · монет {len(groups)}, сделок {len(rows)} "
                f"(✅ {wins} / ❌ {len(rows) - wins})")
        chunks, cur = [], head
        for line in lines:
            if len(cur) + len(line) + 1 > 3800:
                chunks.append(cur)
                cur = "📒 <i>продолжение</i>"
            cur += "\n" + line
        chunks.append(cur)
        return chunks

    async def trades_csv(self, hours: float | None = None) -> str:
        """Все закрытые сделки с последнего сброса (или за hours) — файл для Excel / Numbers."""
        since = datetime.now(timezone.utc) - timedelta(hours=hours) if hours else None
        if since is None:
            since = await self.db.fetchval("SELECT max(reset_at) FROM paper_portfolios")
        sol_usd = self.e.price.value or 150
        out = io.StringIO()
        w = csv.writer(out, delimiter=";")
        tz = self.tz()
        w.writerow(["портфель", "монета", "адрес", "режим", "уровень", "кошелёк", "вход", "выход", "MC вход $",
                    "MC выход $", "MC максимум $", "вложено SOL", "получено SOL", "результат SOL", "результат %",
                    "причина выхода", "адрес кошелька"])
        for r in await self._closed(since):
            w.writerow([
                r["portfolio"], r["symbol"] or "", r["mint"], MODE_SHORT.get(r["mode"] or "", r["mode"] or ""),
                r["level"] or "", wallet_name(r) if r.get("source_wallet") else "",
                f"{r['opened_at'].astimezone(tz):%Y-%m-%d %H:%M:%S}",
                f"{r['closed_at'].astimezone(tz):%Y-%m-%d %H:%M:%S}",
                round((r["entry_mc"] or 0) * sol_usd), round((r["exit_mc"] or 0) * sol_usd),
                round((r["max_mc"] or 0) * sol_usd), round(r["cost_sol"] or 0, 5), round(r["proceeds_sol"] or 0, 5),
                round(r["pnl_sol"] or 0, 5), round((r["pnl_pct"] or 0) * 100, 1),
                EXIT_NAMES.get(r["exit_reason"], r["exit_reason"] or ""), r.get("source_wallet") or "",
            ])
        return out.getvalue()

    async def maybe_daily_reset(self) -> None:
        """В 00:00 по часовому поясу сброса (по умолчанию Батуми) — итог дня по каждой схеме и сброс до $30:
        все схемы каждый день торгуют в равных условиях, ни одна не стоит полдня на паузе."""
        if not self.cfg.get("paper_daily_reset", True):
            return
        try:
            tz = ZoneInfo(str(self.cfg.get("paper_reset_tz") or "Asia/Tbilisi"))
        except Exception:  # noqa: BLE001
            tz = ZoneInfo("Asia/Tbilisi")
        today = datetime.now(tz).date().isoformat()
        last = await self.db.kv_get("paper:daily_reset")
        if last == today:
            return
        await self.db.kv_set("paper:daily_reset", today)
        if last is None:
            return                                   # первый запуск — только запомнить день
        text = await self.day_summary(tz)
        await self.flush_messages()
        await self.db.enqueue("text", {"topic": "paper", "text": text}, priority=4)
        await self.reset()
        await self._msg(f"🔄 Новый день ({today}): все портфели снова по ${self.start_usd():.0f}")

    async def day_summary(self, tz: ZoneInfo) -> str:
        """Итог дня: схемы (с открытыми позициями по последней цене) и закрытые сделки по 6-часовым отрезкам."""
        sol_usd = self.e.price.value or 150
        tot = await self._row(None)
        res = sorted(((tot[n]["equity"] / tot[n]["port"]["start_sol"] - 1, n) for n in SCHEMES), reverse=True)
        lines = ["🏁 <b>Итог дня бумаги</b> (сброс в 00:00 по Батуми)"]
        for r, n in res:
            t = tot[n]
            wr = f", прибыльных {t['wins'] / t['trades']:.0%}" if t["trades"] else ""
            lines.append(f"{'🥇' if n == res[0][1] else '•'} {n}: <b>{r:+.0%}</b> · ${t['equity'] * sol_usd:.1f} · "
                         f"сделок {t['trades']}{wr}")
        rows = await self.db.fetch(
            """SELECT closed_at, pnl_sol FROM paper_positions p JOIN paper_portfolios f ON f.name = p.portfolio
               WHERE p.closed_at IS NOT NULL AND p.exit_reason <> 'reset' AND NOT p.suspect AND p.opened_at >= f.reset_at""")
        buckets: dict[int, list[float]] = {}
        for r in rows:
            buckets.setdefault(r["closed_at"].astimezone(tz).hour // 6, []).append(r["pnl_sol"] or 0.0)
        if buckets:
            names = {0: "ночь 00–06", 1: "утро 06–12", 2: "день 12–18", 3: "вечер 18–24"}
            lines.append("\n<b>По времени суток</b> (все схемы вместе)")
            for b in sorted(buckets):
                xs = buckets[b]
                lines.append(f"• {names[b]}: сделок {len(xs)}, прибыльных {sum(x > 0 for x in xs) / len(xs):.0%}, "
                             f"итог {sum(xs) * sol_usd:+.1f}$")
        return "\n".join(lines)

    async def maybe_compare(self) -> None:
        """Через 7 дней после сброса — сравнение портфелей и предложение лучшей схемы (один раз)."""
        port = await self.db.fetchrow("SELECT reset_at FROM paper_portfolios ORDER BY name LIMIT 1")
        if not port or datetime.now(timezone.utc) - port["reset_at"] < timedelta(days=7):
            return
        if await self.db.kv_get("paper:compared"):
            return
        res = []
        for name, scheme in SCHEMES.items():
            s = await self.summary(name)
            res.append((s["equity"] / s["port"]["start_sol"] - 1, name, scheme["name"], s["trades"]))
        res.sort(reverse=True)
        lines = ["🏁 <b>Бумажная торговля: итог 7 дней</b>"]
        lines += [f"{n}: {r:+.1%} ({t} сделок) — {fmt.esc(nm)}" for r, n, nm, t in res]
        best = res[0]
        lines.append(f"\nЛучшая схема: <b>{fmt.esc(best[2])}</b>." + (
            " Все портфели в минусе — схему выхода нужно менять, до реальной торговли рано." if best[0] <= 0 else
            " Реальная торговля — только после положительного итога за 2+ недели и вашего решения."))
        await self.db.enqueue("text", {"topic": "paper", "text": "\n".join(lines)}, priority=6)
        await self.db.kv_set("paper:compared", True)
