"""Исполнитель реального бота: сигнал → проверки → покупка → сопровождение → продажа.

Режимы (настройка real_mode, кнопки в /real):
  off     — новых входов нет (открытые позиции бот доводит до выхода по правилам);
  shadow  — «тень»: на каждый сигнал собирает настоящую транзакцию, проверяет её симуляцией на сети и ведёт
            учёт, как будто купил и продал, — без денег и без подписи. Показывает, что было бы у реального
            бота: прошла бы сделка, по какой цене, через сколько блоков после сигнала;
  live    — «бой»: подписывает и отправляет сделки. Включается только если на сервере задан REAL_LIVE=1,
            есть ключ кошелька и режим подтверждён кнопкой в Telegram.

Сигналы — те же, что у бумаги: smart (бумажный портфель M) и «мой дев» (портфель Y). Правила выхода — схемы
бумажных портфелей (real_scheme для smart, real_scheme_mydev для девов), решение принимает та же функция
decide_exit, что и у бумаги: логика бумаги и боя одна. Цена для решений — поток сделок монеты и, для боевых
позиций, состояние кривой/пула из блокчейна.

Защита (подробно — docs/REAL_BOT.md): фиксированная ставка с потолком, ограничение по влиянию на цену, лимит
открытых позиций, дневной и общий стоп, лимит проскальзывания (проверяет сама сеть), возраст сигнала в секундах
и блоках, «цена уже убежала», пауза после серии неудач, стоп-кран, белый список инструкций
(tx.check_instructions). Каждая боевая заявка пишется в real_orders ДО отправки: после перезапуска бот находит
сделки, которые легли без него.
"""
from __future__ import annotations

import asyncio
import collections
import contextvars
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from solders.pubkey import Pubkey

from ..alerts import format as fmt
from ..analytics.paper import (BIGCOPY, MAIN, MYDEV, SCHEMES, SCOUT, SMART_SET, SWING, TINY_REBUY_SOL, TREND,
                               Position, decide_exit, sell_fill)
from . import pump, pumpswap, settings as S, tx as rtx
from .keys import Wallet, load_wallet

if TYPE_CHECKING:
    from ..analytics.engine import Engine

log = logging.getLogger(__name__)

STATUS_KEY = "real:status"
TOPIC = "real"                        # отдельная тема Telegram «Реальный бот»
BASE_FEE = 5_000                      # лампортов за подпись
SIM_RESERVE = 6_000_000               # лампортов оставляем кошельку при симуляции (залоги счетов и комиссия)
SHADOW_MAX_OPEN = 30                  # тень-замер: столько позиций одновременно (это только симуляция)
# копии своего списка и кандидатов разведки в замер тени не идут (если метод выключен): 19.11 за 2.5 ч — 183 сделки,
# в среднем −6.7%, а бумага H на тех же сигналах −7.6% (вход дороже лишь на +0.3%) — бумага их меряет так же, а
# тень тратила на них ~1700 симуляций в сутки из бесплатных запросов Helius
SHADOW_SKIP = {"copy"}
NEWDEV = "Z"                     # сигнал «новый дев с крупной покупкой» (бумажного портфеля нет — только реальный бот)
JUP_VENUE = "jupiter"            # «тренды на других биржах»: позиция по котировке Jupiter (тень, без транзакции)
JUP_SLIP = 0.01                  # проскальзывание входа и выхода по котировке Jupiter (сверх комиссии)
OTHER_DEX = "не pump.fun и не PumpSwap"
SOL_MINT = "So11111111111111111111111111111111111111112"
NEWDEV_MAX_MC = 65.0             # дороже — лотерея не окупается (MC 65: окупаемость с 11% ухода на PumpSwap)
NEWDEV_SHADOW_MAX = 150          # «новый дев» в тени: своих позиций одновременно (держит до 1 ч)
NEWDEV_MAX_HOLD_S = 3600.0       # «новый дев»: любое правило — не дольше часа
KINDS = S.KIND_NAMES
SMART_TARGETS = {MAIN, *SMART_SET}         # smart-сигнал: в бумаге он идёт в боевой M и в A / B / G
# тренд — без покупки трейдера в блоке: возраст сигнала считаем от алерта (DexScreener опрашивается раз в 5 с,
# первый алерт монеты ещё ~1 с ждёт внешних проверок)
TREND_MAX_AGE_S = 15.0
# свежий запуск («мой дев»): узел ещё не видит монету, созданную доли секунды назад — повтор (21.11: пропуски
# «монета не найдена в сети», а бумага Y входит через 4 блока); «слишком много блоков» после повтора проверяется
NOT_FOUND = "монета не найдена в сети"
STATE_RETRIES = 3
STATE_RETRY_S = 0.35


def kinds_of(data: dict[str, Any], targets: set[str]) -> list[str]:
    """Какими методами можно торговать этот сигнал — по порядку приоритета (одна позиция на монету)."""
    modes = set(data.get("modes") or [])
    out = []
    if targets & SMART_TARGETS and "smart" in modes:
        out.append("smart")
    if MYDEV in targets and "mydev" in modes:
        out.append("mydev")
    if BIGCOPY in targets:
        out.append("bigcopy")
    if SCOUT in targets and modes & {"copy", "scout"}:
        out.append("copy")
    if SWING in targets and "swing" in modes:
        out.append("swing")
    if TREND in targets and "trend" in modes:
        out.append("trend")
    if NEWDEV in targets and "newdev" in modes:
        out.append("newdev")
    return out
MODE_ICON = {"live": "💵", "shadow": "👻"}
MODE_SHORT_RU = {"live": "💵 бой", "shadow": "👻 тень", "off": "выкл"}
# сигнал, который сейчас обрабатывается (монета, алерт): у каждой задачи свой — причина пропуска пишется к нему
_SIGNAL: contextvars.ContextVar[tuple[str, int | None] | None] = contextvars.ContextVar("real_signal", default=None)
EXIT_NAMES = {"take_profit": "тейк", "stop": "стоп", "trailing": "трейлинг", "time_stop": "нет роста",
              "max_hold": "время вышло", "trader_sell": "трейдер продал", "tiny_rebuy": "мелкая докупка",
              "emergency": "дев продаёт", "manual": "вручную", "sell_all": "продать всё", "gone": "токенов нет"}
SELL_SLIPPAGE_STEPS = (1.0, 2.0, 4.0, 6.0)      # множители лимита проскальзывания по попыткам продажи
BUY_CONFIRM_S, BUY_RESEND_UNTIL_S = 20.0, 5.0   # покупку шлём 5 с, дальше только ждём, пока жив её блокхэш
SELL_CONFIRM_S = 20.0
JUMP_X = 5.0                          # цена из потока скакнула в 5+ раз за одну сделку — ждём второго подтверждения
ORDER_DEAD_S = 75.0                   # заявка не найдена в сети дольше — уже не ляжет (20.11: блок ~0.21 с —
                                      # 280 в минуту, блокхэш живёт ~30 с; было 180 при блоке 0.4 с)
STATE_RETRY_S, UNTRADABLE_RETRY_S = 1.0, 2.0    # паузы между попытками продажи, если сеть не ответила / монета закрыта
SWEEP_TRIES = 3
# 20.11: «мой дев» через 2 блока после создания монеты — узел, который симулирует, иногда ещё не видит монету:
# открытие счёта токена отвечает IncorrectProgramId (монета «принадлежит» системной программе). Те же монеты позже
# симулируются без ошибок (проверено в сети) — повторяем симуляцию через пару блоков
SIM_RETRY_ERRORS = ("IncorrectProgramId", "InvalidAccountData", "AccountNotFound")
SIM_RETRIES, SIM_RETRY_S = 3, 0.4
NO_FUNDS = "не хватает SOL или токенов на счёте"


def paid_mc(lamports: int, tokens_raw: int) -> float:
    """Средняя цена покупки как капитализация монеты в SOL (без комиссии площадки)."""
    if tokens_raw <= 0:
        return 0.0
    return lamports * 10_000 / (10_000 + pump.FEE_BPS) / tokens_raw * pump.SUPPLY_RAW / 1e9


def make_chain(engine: Any) -> rtx.Chain | None:
    """Свой клиент RPC реального бота (без дневного лимита аналитики): чтение сети, отправка, подтверждение."""
    env = getattr(engine, "env", None)
    if env is None:
        return None
    from ..ingest.rpc import SolanaRpc
    return rtx.Chain(SolanaRpc(env.rpc_endpoint, rps=10, concurrency=8, timeout=6,
                               fallback=env.rpc_fallback_url or None))


class RealTrader:
    def __init__(self, engine: "Engine", chain: rtx.Chain | None = None, wallet: Wallet | None = None,
                 profile: dict[str, Any] | None = None, wallet_note: str | None = None):
        """profile — кошелёк из таблицы real_wallets ({id, name, settings}): у каждого кошелька свой режим, ставка,
        позиции, стопы и журнал. Без профиля — «общая полоса» (настройки /real, ключ из .env или без кошелька)."""
        self.e = engine
        self.db = engine.db
        self.profile = profile
        self.wid: int | None = int(profile["id"]) if profile else None
        self.name = str(profile.get("name") or "") if profile else ""
        self.accept_new = True                      # False — только доводит открытые позиции (полоса без кошелька)
        self._skip_buf: list[tuple[Any, ...]] = []  # причины пропусков по монетам — в real_skips на каждом тике
        self._skips_cleaned = 0.0
        self.external_poll = False                  # True — цену открытых позиций проверяет менеджер кошельков (desk)
        self._helius_key = getattr(getattr(engine, "env", None), "helius_key", "") or ""
        if wallet is not None or profile is not None or wallet_note is not None:
            self.wallet, self.wallet_note = wallet, wallet_note or ("задан" if wallet is not None else "не задан")
        else:
            self.wallet, self.wallet_note = load_wallet()
        self.chain = chain if chain is not None else self._make_chain()
        self.positions: dict[int, Position] = {}
        # пробы правил выхода (таблица real_rule_trials): тот же вход, выход по A / B / G / R — только расчёт, без
        # транзакций; в позиции бота, лимиты, стопы и итоги дня не входят
        self.trials: dict[int, Position] = {}
        self._trial_st: dict[str, tuple[float, Any]] = {}   # монета → (когда, запрос состояния): общий для проб
        self._trials_saved = time.time()
        # «за ним» с доски кошельков (kv real:follow): адрес → shadow | live — его покупки берём, даже если метод
        # сигнала выключен; live — только когда сам кошелёк бота в бою
        self.follow: dict[str, str] = {}
        self.follow_auto: set[str] = set()          # кого «тень за ним» включил сам бот (wallet_rules.autofollow)
        self._follow_at = 0.0
        self.balance: int | None = None             # лампорты на кошельке (кэш)
        self._balance_at = 0.0
        self.day_pnl: dict[str, float] = {}         # режим → итог закрытых сделок за сегодня, SOL
        self._day = ""
        self.live_total = 0.0                       # итог боя с момента включения, SOL
        self._armed_seen: Any = None
        self.fails = 0                              # неудачных транзакций подряд (бой)
        self.buys: collections.deque[float] = collections.deque(maxlen=500)
        self.skips: collections.Counter[str] = collections.Counter()
        self.lost: dict[str, float] = {}            # монета → время убыточного выхода
        self.to_close: dict[str, tuple[str, float, int]] = {}   # монета → (программа токена, когда, попытка)
        self._busy: dict[str, str] = {}             # монета → режим: по ней сейчас идёт вход
        self._sweeping = False
        self._checking = False
        self._last_poll = 0.0
        self._last_status = 0.0
        self._warned_live = False
        self._want_seen: str | None = None          # выбранный режим на прошлом тике — смена режима сообщением
        self.since = datetime.now(timezone.utc)

    # ------------------------------------------------------------------ окружение
    def _make_chain(self) -> rtx.Chain | None:
        return make_chain(self.e)

    @property
    def cfg(self) -> dict[str, Any]:
        """Действующие настройки: общие (/real → ⚙️) и поверх них — настройки этого кошелька."""
        if self.profile and self.profile.get("settings"):
            return {**self.e.cfg, **self.profile["settings"]}
        return self.e.cfg

    def get(self, key: str) -> Any:
        return S.get(self.cfg, key)

    def sol_usd(self) -> float:
        return float(self.e.price.value or 150)

    def mode(self) -> str:
        """Действующий режим: «бой» только при REAL_LIVE=1 на сервере и заданном ключе."""
        m = self.get("real_mode")
        if m == "live" and self.why_not_live():
            return "shadow"
        return m

    def why_not_live(self) -> str | None:
        if not S.live_allowed():
            return "на сервере не задан REAL_LIVE=1"
        if self.wallet is None or not self.wallet.can_sign:
            return "на сервере не задан ключ кошелька REAL_PRIVATE_KEY"
        return None

    def mode_for(self, kind: str) -> str:
        m = self.mode()
        if m == "off" or kind not in S.METHODS:
            return "off"
        measure = bool(self.get("real_shadow_all")) and kind not in SHADOW_SKIP   # выключенный метод — в тень, не на деньги
        if not self.get(S.METHODS[kind][1]):
            return "shadow" if measure else "off"
        if m == "live" and kind == "mydev" and not self.get("real_live_mydev"):
            return "shadow"                          # запуски девов в бою — только по отдельному разрешению
        return m

    def scheme_of(self, p: Position) -> dict[str, Any]:
        """Правила выхода позиции: у пробы — её правило, у позиции «за ним» — лучший выход кошелька, у остальных —
        правило метода."""
        if p.extra.get("best_exit") and not p.extra.get("trial"):
            from ..analytics.wallet_rules import exit_scheme
            es = exit_scheme(p.extra["best_exit"])
            if es:
                return es[1]
        rule = p.extra.get("rule") if p.extra.get("trial") else None
        if rule == "W":
            return {"stop": None, "tp_net": None, "tp_share": 0.0, "trail": None, "target": False, "time_stop_s": None,
                    "time_stop_gain": None, "max_hold_s": float(p.extra.get("w_hold") or 1.0)}
        sc = self.rule_scheme(rule, p.extra.get("kind")) if rule in S.SCHEMES else self.scheme_for(p.extra.get("kind"))
        if p.extra.get("kind") == "newdev" and float(sc.get("max_hold_s") or 1e9) > NEWDEV_MAX_HOLD_S:
            # запуск: монета, которая за час не ушла на PumpSwap, почти наверняка мертва; у R стоп −50% ниже «пола»
            # кривой — без предела мёртвая монета висела бы сутки и тратила запросы цены
            sc = {**sc, "max_hold_s": NEWDEV_MAX_HOLD_S}
        return sc

    def scheme_for(self, kind: str | None) -> dict[str, Any]:
        """Правила выхода позиции — схема бумажного портфеля (та же, по которой считается бумага): у smart и
        «мой дев» — выбранная (A / B / G), у копий, крупных копий и смарт-ракет — их портфелей H / K / R."""
        return self.rule_scheme(self.get(S.SCHEME_KEYS[kind or "smart"]), kind)

    @staticmethod
    def rule_scheme(name: str, kind: str | None) -> dict[str, Any]:
        """Схема продажи по букве правила: L — лотерея, D — почерк дева (только «мой дев»), U/V/P/Q — правила трендов,
        A/B/G/R — схемы бумаги."""
        if name == "L":
            return S.LOTTERY
        if name == "D":
            return S.DEV_SCHEME if kind == "mydev" else SCHEMES["G"]
        if name in S.EXTRA_RULES:
            return S.EXTRA_RULES[name]
        return SCHEMES[name]

    def paused(self) -> str:
        """Стоп-кран: общий (для всех кошельков) или этого кошелька."""
        own = (self.profile or {}).get("settings") or {}
        return str(self.e.cfg.get("real_paused") or "") or str(own.get("real_paused") or "")

    async def set_setting(self, key: str, value: Any) -> None:
        """Записать настройку туда, где она живёт: в профиль кошелька или в общие настройки."""
        if self.profile is None:
            await self.db.set_override(key, value)
            return
        st = dict(self.profile.get("settings") or {})
        if value is None:
            st.pop(key, None)
        else:
            st[key] = value
        self.profile["settings"] = st
        await self.db.execute("UPDATE real_wallets SET settings=$2 WHERE id=$1", self.wid, st)

    def status_key(self) -> str:
        return STATUS_KEY if self.wid is None else f"{STATUS_KEY}:{self.wid}"

    def day_key(self) -> str:
        paper = getattr(self.e, "paper", None)
        try:
            return paper.day_key() if paper is not None else datetime.now(timezone.utc).date().isoformat()
        except Exception:  # noqa: BLE001
            return datetime.now(timezone.utc).date().isoformat()

    def _day_of(self, ts: datetime) -> str:
        paper = getattr(self.e, "paper", None)
        try:
            return ts.astimezone(paper.reset_tz()).date().isoformat()
        except Exception:  # noqa: BLE001
            return ts.astimezone(timezone.utc).date().isoformat()

    def _roll_day(self) -> None:
        if self._day != self.day_key():
            self._day, self.day_pnl = self.day_key(), {}
            # новый день — заново: лучший плюс дня, ступени сообщений, замок прибыли, баланс на начало дня
            self.day_peak, self.day_note, self.day_locked = {}, {}, set()
            self.day_start_balance = self.balance

    def _armed_at(self) -> datetime | None:
        """Когда включили бой (от этого момента считается общий стоп)."""
        try:
            ts = datetime.fromisoformat(str(self.cfg.get("real_armed_at")))
        except (TypeError, ValueError):
            return None
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)

    def fees_sol(self) -> float:
        """Приоритет + чаевые + подпись на одну транзакцию, SOL."""
        return float(self.get("real_priority_sol")) + float(self.get("real_tip_sol")) + BASE_FEE / 1e9

    def failed_fee_sol(self) -> float:
        """Сколько стоит транзакция, отклонённая сетью: подпись и приоритет (чаевые при отказе не списываются)."""
        return float(self.get("real_priority_sol")) + BASE_FEE / 1e9

    def _urls(self) -> list[str]:
        """Куда, кроме обычного RPC, отправлять сделки: Helius Sender (нужны чаевые от 0.000005 SOL)."""
        tip = int(float(self.get("real_tip_sol")) * 1e9)
        return [rtx.sender_url(self._helius_key, tip, bool(self.get("real_mev_protect")))] if tip >= 5_000 else []

    # ------------------------------------------------------------------ запуск
    async def start(self) -> None:
        self._roll_day()
        try:
            self.follow = dict(await self.db.kv_get("real:follow") or {})
            self.follow_auto = set(await self.db.kv_get("real:follow_auto") or [])
            await self.load_autotune()
        except Exception:  # noqa: BLE001
            self.follow = {}
        for r in await self.db.fetch("SELECT * FROM real_positions WHERE closed_at IS NULL AND wallet_id IS NOT DISTINCT FROM $1", self.wid):
            ex = dict(r["extra"] or {})
            ex.update(mode=r["mode"], kind=r["kind"], venue=r["venue"], token_program=r["token_program"],
                      tokens_raw=int(r["tokens_raw"] or 0), rent=float(r["rent_sol"] or 0),
                      entry_slot=r["entry_slot"])
            ex.pop("note", None)
            self.positions[r["id"]] = Position(
                id=r["id"], portfolio="REAL", mint=r["mint"], opened_ts=r["opened_at"].timestamp(),
                entry_mc=float(r["entry_mc"] or 0), cost_sol=float(r["cost_sol"] or 0), tokens=float(r["tokens"] or 0),
                tokens_initial=float(r["tokens"] or 0), target_mc=ex.get("target_mc"), trader=r["trader"],
                symbol=r["symbol"] or "",
                proceeds_sol=float(r["proceeds_sol"] or 0), max_mc=float(r["max_mc"] or r["entry_mc"] or 0),
                tp_done=bool(r["tp_done"]), alert_id=r["alert_id"], extra=ex)
        for r in await self.db.fetch("SELECT * FROM real_rule_trials WHERE closed_at IS NULL "
                                     "AND wallet_id IS NOT DISTINCT FROM $1", self.wid):
            t = self._trial_position(r)
            self.trials[t.id] = t
        today = self.day_key()
        for r in await self.db.fetch("SELECT mode, pnl_sol, closed_at FROM real_positions WHERE closed_at IS NOT NULL "
                                     "AND closed_at > now() - interval '30 hours' AND wallet_id IS NOT DISTINCT FROM $1", self.wid):
            if self._day_of(r["closed_at"]) == today:
                self.day_pnl[r["mode"]] = self.day_pnl.get(r["mode"], 0.0) + float(r["pnl_sol"] or 0)
        for r in await self.db.fetch("SELECT fee_sol, created_at FROM real_orders WHERE mode = 'live' AND status = 'failed' "
                                     "AND fee_sol > 0 AND created_at > now() - interval '30 hours' AND wallet_id IS NOT DISTINCT FROM $1", self.wid):
            if self._day_of(r["created_at"]) == today:
                self.day_pnl["live"] = self.day_pnl.get("live", 0.0) - float(r["fee_sol"])
        self._armed_seen = self.cfg.get("real_armed_at")
        self.live_total = await self._live_total_db()
        for r in await self.db.fetch("SELECT mint, max(closed_at) t FROM real_positions WHERE pnl_sol < 0 "
                                     "AND closed_at > now() - interval '7 days' AND wallet_id IS NOT DISTINCT FROM $1 GROUP BY 1", self.wid):
            self.lost[r["mint"]] = r["t"].timestamp()
        for r in await self.db.fetch("SELECT DISTINCT mint, token_program FROM real_positions WHERE mode = 'live' "
                                     "AND closed_at > now() - interval '3 days' AND token_program IS NOT NULL "
                                     "AND wallet_id IS NOT DISTINCT FROM $1", self.wid):
            self.to_close[r["mint"]] = (r["token_program"], time.time() + 60, 1)   # не закрытые до остановки счета
        await self.refresh_balance(force=True)
        try:
            await self._recover()
        except Exception:  # noqa: BLE001
            log.exception("реальный бот: восстановление заявок")
        log.info("реальный бот: режим «%s», кошелёк %s (%s), открытых позиций %d", self.mode(),
                 self.wallet.pubkey if self.wallet else "—", self.wallet_note, len(self.positions))

    async def _live_total_db(self) -> float:
        """Итог боя с момента включения: закрытые сделки минус комиссии отклонённых сетью транзакций."""
        armed = self._armed_at()
        pnl = await self.db.fetchval(
            "SELECT COALESCE(sum(pnl_sol), 0) FROM real_positions WHERE mode = 'live' AND closed_at IS NOT NULL "
            "AND ($1::timestamptz IS NULL OR opened_at >= $1::timestamptz) AND wallet_id IS NOT DISTINCT FROM $2", armed, self.wid)
        fees = await self.db.fetchval(
            "SELECT COALESCE(sum(fee_sol), 0) FROM real_orders WHERE mode = 'live' AND status = 'failed' "
            "AND ($1::timestamptz IS NULL OR created_at >= $1::timestamptz) AND wallet_id IS NOT DISTINCT FROM $2", armed, self.wid)
        return float(pnl or 0) - float(fees or 0)

    async def _recover(self) -> None:
        """После перезапуска: заявки, отправленные перед остановкой. Покупка легла — берём позицию в учёт;
        продажа открытой позиции легла — закрываем её по факту."""
        if self.chain is None or self.wallet is None:
            return
        rows = await self.db.fetch("SELECT *, extract(epoch FROM now() - created_at) AS age_s FROM real_orders "
                                   "WHERE status = 'sent' AND side = 'buy' AND created_at > now() - interval '30 minutes' "
                                   "AND wallet_id IS NOT DISTINCT FROM $1", self.wid)
        for o in rows:
            await self._recover_buy(o)
        for p in [p for p in self.positions.values() if p.extra.get("mode") == "live"]:
            await self._prior_sell(p, minutes=24 * 60)

    async def _recover_buy(self, o: Any) -> None:
        base = dict(o["base"] or {})
        info = await self.chain.transaction(o["signature"], tries=1) if o["signature"] else None
        if info is None and float(o["age_s"] or 0) < ORDER_DEAD_S and o["signature"]:
            # блокхэш заявки ещё может быть жив — проверим позже, когда сделка точно либо легла, либо нет
            asyncio.create_task(self._recover_buy_later(o, ORDER_DEAD_S - float(o["age_s"] or 0)))
            return
        if not info or (info.get("meta") or {}).get("err") is not None or not base.get("mint"):
            await self._order_done(o["id"], "failed" if info else "expired",
                                   error="отклонена сетью" if info else "не легла в блок",
                                   fee_sol=self.failed_fee_sol() if info else None)
            return
        if any(p.mint == base["mint"] for p in self.positions.values()):
            return
        await self._adopt_buy(base, o["signature"], info, o["id"], None, late=True)

    async def _recover_buy_later(self, o: Any, delay_s: float) -> None:
        await asyncio.sleep(max(1.0, delay_s))
        try:
            await self._recover_buy({**dict(o), "age_s": ORDER_DEAD_S})
        except Exception:  # noqa: BLE001
            log.exception("реальный бот: восстановление покупки")

    async def refresh_balance(self, force: bool = False) -> None:
        if self.wallet is None or self.chain is None:
            return
        live = any(p.extra.get("mode") == "live" for p in self.positions.values()) or self.mode() == "live"
        if not force and time.time() - self._balance_at < (300 if live else 1800):
            return
        try:
            self.balance = await self.chain.balance(self.wallet.pubkey)
            self._balance_at = time.time()
        except Exception as exc:  # noqa: BLE001
            self._balance_at = time.time() - 240          # повтор через минуту, а не на каждом тике
            log.warning("баланс кошелька: %s", str(exc)[:160])

    # ------------------------------------------------------------------ вход
    async def load_autotune(self) -> None:
        """Решения автонастройки (scanner/real/autotune.py): остановленные методы и источники, окно цены «нового дева»."""
        from .autotune import BAND_KEY, BLOCK_KEY
        bl = await self.db.kv_get(BLOCK_KEY) or {}
        now = time.time()
        self.tune_kinds = {k: v for k, v in (bl.get("kinds") or {}).items() if float(v.get("until", 0)) > now}
        self.tune_sources = {k: v for k, v in (bl.get("sources") or {}).items() if float(v.get("until", 0)) > now}
        band = await self.db.kv_get(BAND_KEY)
        self.newdev_band = tuple(band) if isinstance(band, list) and len(band) == 2 else None

    def tune_block(self, kind: str, wallet: str | None) -> str | None:
        """Причина не входить по решению автонастройки или None."""
        k = (getattr(self, "tune_kinds", None) or {}).get(kind)
        if k:
            return f"автонастройка: метод остановлен — {k.get('why')}"
        s = (getattr(self, "tune_sources", None) or {}).get(wallet or "")
        if s:
            return f"автонастройка: источник в минусе — {s.get('why')}"
        return None

    def follow_mode(self, wallet: str | None) -> str | None:
        """«За ним» с доски кошельков: режим для его сигналов (live — только если этот кошелёк бота в бою)."""
        f = (getattr(self, "follow", None) or {}).get(wallet or "")
        if not f or self.mode() == "off":
            return None
        if self.get("real_only_follow") and wallet in (getattr(self, "follow_auto", None) or set()):
            return None                  # «только выбранные мной»: тень, которую бот включил сам, — не в счёт
        return "live" if f == "live" and self.mode() == "live" else "shadow"

    async def on_signal(self, mint: str, level: str, alert_id: int | None, data: dict[str, Any],
                        trader: str | None, targets: set[str]) -> None:
        """Сигнал бумаги: smart (идёт в боевой портфель M) или «мой дев» (портфель Y)."""
        wallet = data.get("copy_wallet") or data.get("source_wallet") or trader
        followed = self.follow_mode(wallet)
        if not self.accept_new or (data.get("paper_only") and not followed):
            return                       # paper_only — копия только на бумагу (свой кошелёк вне рамок smart), не деньги
        probe = not followed and bool(self.get("real_only_follow"))
        if probe and not {"newdev", "trend"} & set(data.get("modes") or []):
            return                       # «только выбранные мной кошельки»: методы и замер тени не торгуют
                                         # (кроме проб «новый дев» и трендов — они только в тени, пока проверяются)
        _SIGNAL.set((mint, alert_id))
        if followed:
            kind = next(iter(kinds_of(data, targets)), None)
            mode = "shadow" if data.get("shadow_only") else followed   # мимо анти-PVP — только тень
            data = {**data, "_followed": wallet}
        else:
            kind = next((k for k in kinds_of(data, targets) if self.mode_for(k) != "off"
                         and not (k == "smart" and self.get("real_smart_filter") == "m" and MAIN not in targets)), None)
            mode = self.mode_for(kind) if kind else "off"
            if probe and mode == "live":
                mode = "shadow"
        if kind is None:
            return
        manual = followed and wallet not in (getattr(self, "follow_auto", None) or set())
        # тренд: биржа станет известна после запроса состояния — его стоп проверяется в _enter (pump.fun — «trend»,
        # другие биржи — «trendj»)
        tb = None if manual or kind == "trend" else self.tune_block(kind, data.get("source_wallet") or trader)
        if tb:
            return self._skip(mode, kind, tb)
        if kind == "mydev" and level != "strong" and self.get("real_mydev_strong_only") and not manual:
            return self._skip(mode, kind, "у дева нет почерка: мало продаж или ×1.5 раньше его продажи реже 60%")
        if kind == "smart" and mode == "live" and data.get("unsteady") and not followed:
            # 20.11: на деньги — только кошельки с устойчивым плюсом копий; тень такие сигналы меряет
            return self._skip(mode, kind, str(data["unsteady"]))
        why = self.entry_block(mint, kind, mode, data)
        if why:
            return self._skip(mode, kind, why)
        self._busy[mint] = mode
        try:
            await self._enter(mint, kind, mode, alert_id, data, trader)
        except rtx.Forbidden as exc:
            log.error("реальный бот: транзакция не из белого списка — не подписана: %s", exc)
            self._skip(mode, kind, f"запрещённая инструкция: {exc}")
        except Exception as exc:  # noqa: BLE001 — сбой входа не должен задевать остальной бот
            log.exception("реальный бот: вход %s", mint)
            self._skip(mode, kind, f"сбой: {type(exc).__name__}")
        finally:
            self._busy.pop(mint, None)

    def entry_block(self, mint: str, kind: str, mode: str, data: dict[str, Any]) -> str | None:
        """Причина не входить (до запросов к сети) или None."""
        if self.paused():
            return f"входы остановлены: {self.paused()}"
        if kind == "trend":
            if not data.get("trend_at"):
                return "нет времени сигнала"
            if time.time() - float(data["trend_at"]) > TREND_MAX_AGE_S:
                return "сигнал опоздал"
        elif not data.get("trigger_at"):
            return "нет покупки трейдера в блоке"
        elif time.time() - float(data["trigger_at"]) > float(self.get("real_max_signal_age_s")):
            return "сигнал опоздал"
        if mint in self._busy or any(p.mint == mint for p in self.positions.values()):
            return "уже есть позиция по монете"
        free = mode == "shadow" and bool(self.get("real_shadow_free"))   # тень — замер: все сигналы, без стопа
        if kind == "newdev" and mode == "shadow":
            # свой лимит: лотерея — десятки позиций сразу, места остальных методов не занимает
            taken = sum(1 for p in self.positions.values() if p.extra.get("mode") == mode
                        and p.extra.get("kind") == "newdev")
            if taken >= NEWDEV_SHADOW_MAX:
                return "лимит открытых позиций"
        else:
            taken = sum(1 for p in self.positions.values() if p.extra.get("mode") == mode
                        and not (mode == "shadow" and p.extra.get("kind") == "newdev")) \
                + sum(1 for m in self._busy.values() if m == mode)
            if taken >= (SHADOW_MAX_OPEN if free else int(self.get("real_max_open"))):
                return "лимит открытых позиций"
        self._roll_day()
        sol_usd = self.sol_usd()
        if not free and -self.day_pnl.get(mode, 0.0) * sol_usd >= float(self.get("real_day_stop_usd")):
            return "дневной стоп"
        if mode == "live" and mode in (getattr(self, "day_locked", None) or set()):
            return "прибыль дня зафиксирована — новых входов до полуночи нет"
        hours = float(self.get("real_reentry_h"))
        if hours > 0 and time.time() - self.lost.get(mint, 0) < hours * 3600:
            return "монета уже дала убыток"
        paper = getattr(self.e, "paper", None)
        if paper is not None:
            if mint in getattr(paper, "rugged", {}):
                return "монета рагнула"
            states = getattr(self.e, "states", None) or {}
            dev = getattr(states.get(mint), "deployer", None)
            if dev and dev in getattr(paper, "rug_devs", set()) and dev not in (getattr(self.e, "my_devs", None) or {}):
                return "дев этой монеты уже рагал"
        if mode == "live":
            if self.wallet is None or not self.wallet.can_sign:
                return "нет ключа"
            now = time.time()
            if sum(1 for t in self.buys if now - t < 3600) >= int(self.get("real_max_buys_hour")):
                return "лимит покупок в час"
            need = float(self.get("real_min_stake_usd")) / sol_usd + float(self.get("real_reserve_sol"))
            # по кэшу — только если он свежий: после пополнения кошелька старый «0» не должен блокировать входы
            # (точный баланс читается вместе с состоянием монеты при входе)
            if self.balance is not None and time.time() - self._balance_at < 60 and self.balance / 1e9 < need:
                return "на кошельке мало SOL"
        return None

    def _skip(self, mode: str, kind: str, why: str) -> None:
        self.skips[f"{'бой' if mode == 'live' else 'тень'} · {KINDS[kind]}: {why}"] += 1
        sig = _SIGNAL.get()
        if sig is not None:
            self._skip_buf.append((datetime.now(timezone.utc), self.wid, sig[0], sig[1], mode, kind, why))

    async def _flush_skips(self) -> None:
        """Причины пропусков — в базу (для «Против бумаги»); старше 14 дней — удаляются раз в час."""
        pool = getattr(self.db, "pool", None)
        if pool is None:
            self._skip_buf.clear()
            return
        rows, self._skip_buf = self._skip_buf[:200], self._skip_buf[200:]
        try:
            if rows:
                await pool.executemany(
                    "INSERT INTO real_skips(at, wallet_id, mint, alert_id, mode, kind, why) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    rows)
            if time.time() - self._skips_cleaned > 3600:
                self._skips_cleaned = time.time()
                await self.db.execute("DELETE FROM real_skips WHERE at < now() - interval '14 days'")
        except Exception as exc:  # noqa: BLE001 — журнал пропусков не должен мешать торговле
            log.warning("журнал пропусков: %s", str(exc)[:160])

    def stake_lamports(self, v_quote: int, kind: str | None = None) -> tuple[int, str | None]:
        """Ставка: своя у метода или общая у кошелька, урезанная так, чтобы своя покупка двигала цену не сильнее
        лимита."""
        sol_usd = self.sol_usd()
        usd = S.method_stake(self.cfg, kind) or float(self.get("real_stake_usd"))
        stake = int(usd / sol_usd * 1e9)
        cap = int(v_quote * ((1 + float(self.get("real_max_impact"))) ** 0.5 - 1))
        stake = min(stake, cap)
        if stake < float(self.get("real_min_stake_usd")) / sol_usd * 1e9:
            return 0, "ставка меньше минимума: монета слишком мелкая для нашей суммы"
        return stake, None

    # ------------------------------------------------------------------ состояние монеты в сети
    @staticmethod
    def _keys(mint: Pubkey) -> list[Pubkey]:
        pool_k = pumpswap.pool_address(mint)
        return [pump.curve_address(mint), mint, pool_k, pump.ata(pool_k, mint, pump.TOKEN_2022),
                pump.ata(pool_k, mint, pump.TOKEN), pump.ata(pool_k, pump.WSOL, pump.TOKEN)]

    @staticmethod
    def _parse_state(mint: Pubkey, keys: list[Pubkey], acc: list[rtx.Account | None]) -> dict[str, Any]:
        """Где монета торгуется (кривая / PumpSwap), резервы, программа токена; why — почему торговать нельзя."""
        out: dict[str, Any] = {"venue": None, "why": None, "mc": 0.0}
        if acc[1] is None or acc[1].owner not in pump.TOKEN_PROGRAMS:
            out["why"] = NOT_FOUND
            return out
        tp = pump.TOKEN_PROGRAMS[acc[1].owner]
        out["token_program"] = tp
        curve = None
        if acc[0] is not None:
            try:
                curve = pump.parse_curve(acc[0].data)
            except ValueError:
                curve = None
        if curve is not None and curve.quote_mint:
            out["why"] = "монета в паре не к SOL"
            return out
        if curve is not None and not curve.complete:
            out.update(venue="pump", curve=curve, why=curve.why_not_tradable(), mc=curve.mc_sol, v_quote=curve.v_quote)
            return out
        vault, vault_key = (acc[3], keys[3]) if tp == pump.TOKEN_2022 else (acc[4], keys[4])
        if acc[2] is None or vault is None or acc[5] is None:
            out["why"] = ("кривая закрыта, пул PumpSwap ещё не создан" if curve is not None else
                          "не pump.fun и не PumpSwap — на других биржах бот не торгует")
            return out
        try:
            pool = pumpswap.parse_pool(str(keys[2]), acc[2].data)
            if pool.base_mint != str(mint) or pool.base_vault != str(vault_key) or pool.quote_vault != str(keys[5]):
                out["why"] = "пул PumpSwap нестандартный"
                return out
            pool = pumpswap.with_reserves(pool, pumpswap.token_amount(vault.data), pumpswap.token_amount(acc[5].data))
        except ValueError:
            out["why"] = "пул PumpSwap не разобран"
            return out
        out.update(venue="pump-amm", pool=pool, why=pool.why_not_tradable(), mc=pool.mc_sol, v_quote=pool.quote_effective)
        return out

    async def _states(self, mints: list[Pubkey], extra: list[Any] | None = None) -> tuple[list[dict[str, Any]], list[Any]]:
        """Состояние нескольких монет одним запросом (+ дополнительные счета, например свой кошелёк).
        В каждом состоянии slot — блок, на который узел отдал данные."""
        keys: list[Pubkey] = []
        for m in mints:
            keys += self._keys(m)
        slot, acc = await self.chain.accounts_at(keys + list(extra or []))
        out = []
        for i, m in enumerate(mints):
            st = self._parse_state(m, keys[i * 6:(i + 1) * 6], acc[i * 6:(i + 1) * 6])
            st["slot"] = slot
            out.append(st)
        return out, acc[len(mints) * 6:]

    async def _state(self, mint: Pubkey, extra: list[Any] | None = None) -> dict[str, Any]:
        sts, ex = await self._states([mint], extra)
        sts[0]["extra"] = ex
        return sts[0]

    @staticmethod
    def _quote_buy(st: dict[str, Any], lamports: int) -> int:
        return pump.quote_buy(st["curve"], lamports) if st["venue"] == "pump" else pumpswap.quote_buy(st["pool"], lamports)

    @staticmethod
    def _quote_sell(st: dict[str, Any], tokens_raw: int) -> int:
        return pump.quote_sell(st["curve"], tokens_raw) if st["venue"] == "pump" \
            else pumpswap.quote_sell(st["pool"], tokens_raw)

    def _prefix_ixs(self, venue: str, side: str) -> list[Any]:
        prio = int(float(self.get("real_priority_sol")) * 1e9)
        tip = int(float(self.get("real_tip_sol")) * 1e9)
        ixs = rtx.budget_ixs(rtx.CU_LIMIT[(venue, side)], prio)
        if tip > 0:
            ixs.append(rtx.tip_ix(self.wallet.pubkey, tip))
        return ixs

    def _buy_ixs(self, st: dict[str, Any], mint: Pubkey, lamports: int, min_out: int) -> list[Any]:
        user, tp = self.wallet.pubkey, st["token_program"]
        ixs = self._prefix_ixs(st["venue"], "buy")
        if st["venue"] == "pump":
            ixs += [pump.create_ata_ix(user, user, mint, tp), pump.buy_ix(user, mint, tp, lamports, min_out)]
        else:
            ixs += pumpswap.wrap_sol_ixs(user, lamports)
            ixs += [pump.create_ata_ix(user, user, mint, tp), pumpswap.buy_ix(user, st["pool"], tp, lamports, min_out),
                    pumpswap.unwrap_sol_ix(user)]
        rtx.check_instructions(ixs, user, lamports)
        return ixs

    def _sell_ixs(self, st: dict[str, Any], mint: Pubkey, tokens_raw: int, min_out: int) -> list[Any]:
        user, tp = self.wallet.pubkey, st["token_program"]
        ixs = self._prefix_ixs(st["venue"], "sell")
        if st["venue"] == "pump":
            ixs.append(pump.sell_ix(user, mint, tp, tokens_raw, min_out))
        else:
            ixs += pumpswap.wrap_sol_ixs(user, 0)
            ixs += [pumpswap.sell_ix(user, st["pool"], tp, tokens_raw, min_out), pumpswap.unwrap_sol_ix(user)]
        rtx.check_instructions(ixs, user, 0)
        return ixs

    # ------------------------------------------------------------------ покупка
    async def _enter(self, mint_s: str, kind: str, mode: str, alert_id: int | None, data: dict[str, Any],
                     trader: str | None) -> None:
        if self.chain is None:
            return self._skip(mode, kind, "нет доступа к сети")
        t0 = time.time()
        mint = Pubkey.from_string(mint_s)
        user = self.wallet.pubkey if self.wallet is not None else None
        bh = None
        if mode == "live":                           # блокхэш — тем же заходом, что и состояние: без лишней задержки
            st, bh = await asyncio.gather(self._state(mint, [user]), self.chain.blockhash())
        else:
            st = await self._state(mint, [user] if user is not None else None)
        for _ in range(STATE_RETRIES):
            if st["why"] != NOT_FOUND:
                break
            await asyncio.sleep(STATE_RETRY_S)
            st = await self._state(mint, [user] if user is not None else None)
        if kind == "trend":
            if st.get("venue") is None and OTHER_DEX in str(st.get("why") or ""):
                return await self._enter_jupiter(mint_s, alert_id, data, trader)
            tb = self.tune_block("trend", None)
            if tb:
                return self._skip(mode, kind, tb)
        if st["why"]:
            return self._skip(mode, kind, st["why"])
        if user is not None and st["extra"]:
            self.balance = st["extra"][0].lamports if st["extra"][0] is not None else 0
            self._balance_at = time.time()
        if kind == "mydev" and data.get("entry_max_mc") and float(st["mc"] or 0) > float(data["entry_max_mc"]):
            # как вручную: заходить на низах — дороже этой цены до его обычной продажи не остаётся запаса
            return self._skip(mode, kind, f"дороже, чем этот дев даёт заработать: MC {float(st['mc'] or 0):.0f} > "
                                          f"{float(data['entry_max_mc']):.0f}")
        cap = float(data.get("newdev_cap") or NEWDEV_MAX_MC)
        band = getattr(self, "newdev_band", None)
        if kind == "newdev" and band and not (band[0] <= float(st["mc"] or 0) < band[1]):
            return self._skip(mode, kind, f"вне окна цены автонастройки {band[0]:g}–{band[1]:g} SOL "
                                          f"(MC {float(st['mc'] or 0):.0f})")
        if kind == "newdev" and float(st["mc"] or 0) > cap:
            # окупается, только если вход дешёвый (лотерея до PumpSwap: потеря до «пола» кривой ~28 SOL)
            return self._skip(mode, kind, f"цена уже убежала: MC {float(st['mc']):.0f} SOL > {cap:.0f}")
        trend = kind == "trend"
        # у тренда нет покупки трейдера: блок и цена сигнала от другого алерта этой монеты ему не судьи
        trig_slot = 0 if trend else int(data.get("trigger_slot") or 0)
        if trig_slot and st["slot"] and st["slot"] - trig_slot > int(self.get("real_max_blocks")):
            return self._skip(mode, kind, "после сигнала прошло слишком много блоков")
        stake, why = self.stake_lamports(st["v_quote"], kind)
        if why:
            return self._skip(mode, kind, why)
        trig_mc = 0.0 if trend else float(data.get("trigger_mc") or 0)
        # тренд: цена сигнала — DexScreener (отстаёт на секунды) — «цена уже убежала» по ней не считаем; от переплаты
        # защищает проскальзывание покупки
        if data.get("trigger_create"):
            trig_mc = 0.0                             # у запуска цена сигнала — не цена монеты
        if data.get("trigger_mc_rough"):
            # 19.11: цена сигнала от PumpPortal на PumpSwap ниже настоящей на 4–26% (без виртуальных резервов пула) —
            # сравнение с ней давало ложное «цена уже убежала»; от переплаты защищает проскальзывание покупки
            trig_mc = 0.0
        if trig_mc > 0 and st["mc"] > trig_mc * (1 + float(self.get("real_max_runup"))):
            return self._skip(mode, kind, "цена уже убежала от цены сигнала")
        slip = float(self.get("real_buy_slippage"))
        expected = self._quote_buy(st, stake)
        min_out = int(expected * (1 - slip))
        if expected <= 0 or min_out <= 0:
            return self._skip(mode, kind, "покупать нечего")
        base = {"mint": mint_s, "kind": kind, "mode": mode, "alert_id": alert_id, "trader": trader,
                "symbol": str(data.get("symbol") or "")[:40], "venue": st["venue"],
                "token_program": str(st["token_program"]), "trigger_slot": trig_slot or None,
                "trigger_mc": trig_mc or None,
                "lag_s": round(time.time() - float((data.get("trend_at") if trend else None)
                                                    or data.get("trigger_ts") or data.get("trigger_at") or t0), 3),
                "stake": stake, "min_out": min_out, "slip": slip, "mc": st["mc"],
                "rule": self.get(S.SCHEME_KEYS[kind]),
                "filter": self.get("real_smart_filter") if kind == "smart" else "all",
                "best_exit": self.best_exit(data.get("_followed")), "followed": bool(data.get("_followed")),
                "fast": bool(data.get("fast")), "detect_ms": data.get("detect_ms"),
                "target_mc": float(data["target_mc"]) if kind == "mydev" and data.get("target_mc") else None,
                "age_s": (round(time.time() - float(data["trigger_ts"]), 3) if data.get("trigger_create")
                          and data.get("trigger_ts") else None)}
        if base["best_exit"]:
            base["rule"] = base["best_exit"]       # «за ним» — продаём по его лучшему выходу (wallet_rules)
        if mode == "live":
            await self._buy_live(st, mint, base, expected, bh)
        else:
            await self._buy_shadow(st, mint, base)

    async def _order(self, pid: int | None, base: dict[str, Any], side: str, status: str, **kw: Any) -> int | None:
        """Запись заявки в журнал. Возвращает её номер (None — запись не удалась)."""
        try:
            return await self.db.fetchval(
                """INSERT INTO real_orders(position_id, mode, side, mint, stake_sol, tokens_raw, min_out, slippage,
                       signature, status, error, slot, wait_ms, cu, fee_sol, attempt, note, base, wallet_id)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19) RETURNING id""",
                pid, base["mode"], side, base["mint"], kw.get("stake_sol"), kw.get("tokens_raw"), kw.get("min_out"),
                kw.get("slippage"), kw.get("signature"), status, (str(kw["error"])[:300] if kw.get("error") else None),
                kw.get("slot"), kw.get("wait_ms"), kw.get("cu"), kw.get("fee_sol"), int(kw.get("attempt") or 1),
                kw.get("note"), kw.get("payload") or {}, self.wid)
        except Exception:  # noqa: BLE001
            log.exception("real_orders")
            return None

    async def _order_done(self, oid: int | None, status: str, **kw: Any) -> None:
        if oid is None:
            return
        try:
            await self.db.execute(
                """UPDATE real_orders SET status=$2, error=$3, slot=COALESCE($4, slot), wait_ms=COALESCE($5, wait_ms),
                       fee_sol=COALESCE($6, fee_sol), tokens_raw=COALESCE($7, tokens_raw),
                       position_id=COALESCE($8, position_id), note=COALESCE($9, note) WHERE id=$1""",
                oid, status, (str(kw["error"])[:300] if kw.get("error") else None), kw.get("slot"), kw.get("wait_ms"),
                kw.get("fee_sol"), kw.get("tokens_raw"), kw.get("position_id"), kw.get("note"))
        except Exception:  # noqa: BLE001
            log.exception("real_orders")

    def _fee_loss(self, fee_sol: float) -> None:
        """Комиссия отклонённой сетью боевой транзакции — реальный расход: идёт в итог дня и в общий стоп."""
        self._roll_day()
        self.day_pnl["live"] = self.day_pnl.get("live", 0.0) - fee_sol
        self.live_total -= fee_sol

    async def _buy_shadow(self, st: dict[str, Any], mint: Pubkey, base: dict[str, Any]) -> None:
        """Тень: симуляция настоящей транзакции + цена на момент «посадки» (через real_land_s секунд)."""
        stake, min_out = base["stake"], base["min_out"]
        cu = None
        note = "без симуляции: кошелёк не задан"
        if self.wallet is not None:
            # для симуляции кошельку нужен SOL: при малом остатке симулируем уменьшенную сумму (проверяется сама
            # транзакция и цена), а учёт ведём на полную ставку по состоянию сети
            sim_stake = min(stake, max(0, (self.balance or 0) - SIM_RESERVE))
            if sim_stake >= 1_000_000:
                sim_min = max(1, int(self._quote_buy(st, sim_stake) * (1 - base["slip"])))
                ixs = self._buy_ixs(st, mint, sim_stake, sim_min)
                sim = await self.chain.simulate(self.wallet.unsigned(rtx.compile_message(self.wallet.pubkey, ixs)))
                for _ in range(SIM_RETRIES):
                    if sim.err is None or not any(e in str(sim.err) for e in SIM_RETRY_ERRORS) \
                            or (self.balance or 0) < sim_stake + SIM_RESERVE:
                        break                # на кошельке мало SOL — AccountNotFound ничего не ждёт
                    await asyncio.sleep(SIM_RETRY_S)
                    sim = await self.chain.simulate(self.wallet.unsigned(rtx.compile_message(self.wallet.pubkey, ixs)))
                cu = sim.units
                err = self._err_text(sim.err, st["venue"]) if sim.err is not None else None
                if err is None:
                    note = "симуляция прошла" + ("" if sim_stake == stake else f" (на {sim_stake / 1e9:.4f} SOL)")
                elif err == NO_FUNDS or "Insufficient" in err or "AccountNotFound" in err:
                    note = "без симуляции: на кошельке мало SOL"
                else:
                    await self._order(None, base, "buy", "failed", stake_sol=stake / 1e9, min_out=min_out,
                                      slippage=base["slip"], error=err, cu=cu, note="симуляция")
                    return self._skip("shadow", base["kind"], f"симуляция: {err}")
            else:
                note = "без симуляции: на кошельке мало SOL"
        await asyncio.sleep(float(self.get("real_land_s")))
        st2 = await self._state(mint)
        if st2["why"] or st2["venue"] is None:
            return self._skip("shadow", base["kind"], st2["why"] or "монета пропала")
        tokens = self._quote_buy(st2, stake)
        if tokens < min_out:
            await self._order(None, base, "buy", "failed", stake_sol=stake / 1e9, min_out=min_out, slippage=base["slip"],
                              error="цена ушла дальше лимита проскальзывания", cu=cu, note=note)
            return self._skip("shadow", base["kind"], "цена ушла дальше лимита проскальзывания")
        fee = self.fees_sol()
        await self._open(base, tokens_raw=tokens, cost_sol=stake / 1e9 + fee, rent_sol=0.0, sig=None,
                         slot=st2["slot"] or None, note=note, cu=cu, fee_sol=fee, wait_ms=None, order_id=None)

    async def _buy_live(self, st: dict[str, Any], mint: Pubkey, base: dict[str, Any], expected: int,
                        bh: tuple[Any, int]) -> None:
        stake, min_out = base["stake"], base["min_out"]
        reserve = int(float(self.get("real_reserve_sol")) * 1e9)
        if (self.balance or 0) - reserve < stake:
            return self._skip("live", base["kind"], "на кошельке мало SOL")
        blockhash, last_valid = bh
        ixs = self._buy_ixs(st, mint, stake, min_out)
        tx = self.wallet.sign(rtx.compile_message(self.wallet.pubkey, ixs, blockhash))
        sig = str(tx.signatures[0])
        # заявка пишется ДО отправки: если бот остановят в эту секунду, после запуска он найдёт сделку по подписи.
        # Журнал недоступен — покупку не отправляем вовсе.
        oid = await self._order(None, base, "buy", "sent", stake_sol=stake / 1e9, min_out=min_out, slippage=base["slip"],
                                signature=sig, payload=dict(base))
        if oid is None:
            return self._skip("live", base["kind"], "журнал заявок недоступен — покупка не отправлена")
        self.buys.append(time.time())
        urls = self._urls()
        t_send = time.time()
        await self.chain.send(tx, urls)
        landed = await self.chain.confirm(tx, timeout_s=BUY_CONFIRM_S, resend_until_s=BUY_RESEND_UNTIL_S, urls=urls,
                                          last_valid_height=last_valid or None)
        wait_ms = int((time.time() - t_send) * 1000)
        if landed.state != "landed":
            self.fails += 1
            failed = landed.state == "failed"
            err = self._err_text(landed.err, st["venue"]) if failed else "не легла в блок"
            fee = self.failed_fee_sol() if failed else None
            if failed:
                self._fee_loss(fee)
            await self._order_done(oid, landed.state, error=err, slot=landed.slot, wait_ms=wait_ms, fee_sol=fee)
            await self._say(f"⚠️ 💵 Покупка ${fmt.esc(base['symbol'] or '?')} не прошла: {fmt.esc(err)}")
            await self.refresh_balance(force=True)
            await self._check_fail_pause()
            return self._skip("live", base["kind"], f"покупка не прошла: {err}")
        self.fails = 0
        info = await self.chain.transaction(sig)
        await self._adopt_buy(base, sig, info, oid, wait_ms, expected=expected, slot=landed.slot)
        await self.refresh_balance(force=True)

    async def _adopt_buy(self, base: dict[str, Any], sig: str, info: dict[str, Any] | None, oid: int | None,
                         wait_ms: int | None, expected: int = 0, slot: int | None = None, late: bool = False) -> None:
        """Покупка легла: считаем по самой транзакции, сколько токенов пришло и сколько SOL ушло, и открываем позицию."""
        mint = Pubkey.from_string(base["mint"])
        tp = pump.TOKEN_PROGRAMS[base["token_program"]]
        user = str(self.wallet.pubkey)
        ata = pump.ata(self.wallet.pubkey, mint, tp)
        ata_s = str(ata)
        acc_s = str(pump.volume_accumulator(self.wallet.pubkey) if base["venue"] == "pump"
                    else pumpswap.volume_accumulator(self.wallet.pubkey))
        fill = rtx.fill_from_tx(info, user, [ata_s, acc_s]) if info else None
        stake = int(base["stake"])
        setup = 0.0
        if fill is not None and fill["tokens"].get(base["mint"], 0) > 0:
            tokens = int(fill["tokens"][base["mint"]])
            rent = fill["rents"].get(ata_s, 0) / 1e9
            setup = fill["rents"].get(acc_s, 0) / 1e9
            cost = (-fill["sol_delta"]) / 1e9 - rent - setup
            fee, slot = fill["fee"] / 1e9, fill["slot"] or slot
        else:
            # транзакция прошла, но узел её ещё не отдал: токены — по остатку на счёте, расход — по расчёту
            tokens = 0
            try:
                acc = (await self.chain.accounts([ata], "confirmed"))[0]
                tokens = pumpswap.token_amount(acc.data) if acc is not None and len(acc.data) >= 72 else 0
            except Exception as exc:  # noqa: BLE001
                log.warning("остаток токенов после покупки: %s", str(exc)[:160])
            tokens = tokens or expected
            if tokens <= 0:
                await self._order_done(oid, "failed", error="легла, но токены не пришли")
                return
            rent, fee = 0.0, self.fees_sol()
            cost = stake / 1e9 + fee
        note = " · ".join(x for x in (("разовый залог счёта объёма %.4f SOL" % setup) if setup else "",
                                      "найдена после перезапуска" if late else "") if x) or None
        await self._open(base, tokens_raw=tokens, cost_sol=cost, rent_sol=rent, sig=sig, slot=slot,
                         note=note, cu=None, fee_sol=fee, wait_ms=wait_ms, order_id=oid)

    async def _enter_jupiter(self, mint_s: str, alert_id: int | None, data: dict[str, Any],
                             trader: str | None) -> None:
        """23.11: тренд на монете не с pump.fun (Raydium, Meteora…) — только тень: цена входа — котировка Jupiter + 1%
        проскальзывания, сопровождение — цены Jupiter (desk.poll_prices), выход — по правилу метода «trendj»."""
        kind = "trendj"
        mode = self.mode_for(kind)
        if mode == "off":
            return self._skip("shadow", kind, "метод «тренды на других биржах» выключен")
        tb = self.tune_block(kind, None)
        if tb:
            return self._skip("shadow", kind, tb)
        ext = getattr(self.e, "ext", None)
        jp = await ext.jup_price(mint_s) if ext is not None else None
        if not jp:
            return self._skip("shadow", kind, "другая биржа: нет цены Jupiter")
        sol_usd = self.sol_usd()
        mc = float(jp) * 1e9 / sol_usd                 # условная капитализация: цена × 1 млрд (как у бумаги T)
        usd = S.method_stake(self.cfg, kind) or S.method_stake(self.cfg, "trend") or float(self.get("real_stake_usd"))
        stake = int(usd / sol_usd * 1e9)
        # настоящая цена исполнения: котировка Jupiter на нашу сумму (маршрут и сдвиг цены своей покупкой учтены);
        # котировки нет (лимит запросов) — цена Jupiter + 1%, с пометкой
        dec = ext.jup_decimals(mint_s) if hasattr(ext, "jup_decimals") else None
        q = await ext.jup_quote(SOL_MINT, mint_s, stake) if dec is not None and hasattr(ext, "jup_quote") else None
        if q and q["out"] > 0:
            raw = int(q["out"] / 10 ** dec * 1e6)       # в учёте бота — 6 знаков, как у pump.fun
            how = f"котировка Jupiter (сдвиг цены {q['impact'] * 100:.2f}%)"
        else:
            raw = int(stake * 1e6 / (mc * (1 + JUP_SLIP)))
            how = "котировки нет — цена Jupiter +1%"
        if raw <= 0:
            return self._skip("shadow", kind, "другая биржа: цена не разобрана")
        base = {"mint": mint_s, "kind": kind, "mode": "shadow", "alert_id": alert_id, "trader": trader,
                "symbol": str(data.get("symbol") or "")[:40], "venue": JUP_VENUE, "token_program": "",
                "trigger_slot": None, "trigger_mc": None,
                "lag_s": round(time.time() - float(data.get("trend_at") or time.time()), 3), "stake": stake,
                "min_out": raw, "slip": JUP_SLIP, "mc": mc, "rule": self.get(S.SCHEME_KEYS[kind]), "filter": "all",
                "best_exit": None, "followed": False, "fast": False, "detect_ms": None, "age_s": None,
                "target_mc": None, "dec": dec}
        fee = self.fees_sol()
        await self._open(base, tokens_raw=raw, cost_sol=stake / 1e9 + fee, rent_sol=0.0, sig=None, slot=None,
                         note=f"другая биржа: {how}, без транзакции", cu=None, fee_sol=fee, wait_ms=None,
                         order_id=None)

    async def _jup_sell(self, p: Position, raw6: int) -> tuple[float, float] | None:
        """Продажа позиции «другая биржа» по котировке Jupiter на точное число токенов: (SOL выручки, условная
        капитализация выхода) или None — котировки нет."""
        dec = p.extra.get("dec")
        ext = getattr(self.e, "ext", None)
        if dec is None or ext is None or not hasattr(ext, "jup_quote") or raw6 <= 0:
            return None
        amount = int(raw6 / 1e6 * 10 ** int(dec))
        key = (p.mint, amount)
        hit = self._jq_cache.get(key) if hasattr(self, "_jq_cache") else None
        if hit and time.time() - hit[0] < 2.0:
            q = hit[1]
        else:
            q = await ext.jup_quote(p.mint, SOL_MINT, amount)
            self._jq_cache = {k: v for k, v in (getattr(self, "_jq_cache", None) or {}).items()
                              if time.time() - v[0] < 2.0}
            self._jq_cache[key] = (time.time(), q)
        if not q or q["out"] <= 0:
            return None
        sol = q["out"] / 1e9
        return max(0.0, sol - self.fees_sol()), sol / (raw6 / 1e6) * 1e9

    async def _open(self, base: dict[str, Any], *, tokens_raw: int, cost_sol: float, rent_sol: float, sig: str | None,
                    slot: int | None, note: str | None, cu: int | None, fee_sol: float,
                    wait_ms: int | None, order_id: int | None) -> None:
        entry_mc = paid_mc(int(base["stake"]), tokens_raw)       # цена входа — средняя цена, по которой купили
        if base.get("venue") == JUP_VENUE and tokens_raw > 0:
            entry_mc = int(base["stake"]) * 1e6 / tokens_raw     # котировка Jupiter уже с комиссиями площадки
        blocks = (int(slot) - int(base["trigger_slot"])) if slot and base.get("trigger_slot") else None
        pid = await self.db.fetchval(
            """INSERT INTO real_positions(mode, kind, mint, symbol, trader, alert_id, venue, token_program, opened_at,
                   trigger_slot, trigger_mc, lag_s, entry_mc, entry_slot, cost_sol, tokens, tokens_raw, rent_sol,
                   buy_sig, max_mc, extra, wallet_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$13,$20,$21) RETURNING id""",
            base["mode"], base["kind"], base["mint"], base["symbol"], base["trader"], base["alert_id"], base["venue"],
            base["token_program"], datetime.now(timezone.utc), base.get("trigger_slot"), base.get("trigger_mc"),
            base.get("lag_s"), entry_mc, slot, cost_sol, tokens_raw / 1e6, tokens_raw, rent_sol, sig,
            {"note": note, "blocks_after": blocks, "stake_sol": base["stake"] / 1e9, "rule": base.get("rule"),
             "filter": base.get("filter"), "best_exit": base.get("best_exit"),
             "followed": base.get("followed", False), "fast": base.get("fast", False),
             "detect_ms": base.get("detect_ms"), "age_s": base.get("age_s"), "target_mc": base.get("target_mc"),
             "dec": base.get("dec")},
            self.wid)
        if order_id is not None:
            await self._order_done(order_id, "landed", slot=slot, wait_ms=wait_ms, fee_sol=fee_sol,
                                   tokens_raw=tokens_raw, position_id=pid, note=note)
        else:
            await self._order(pid, base, "buy", "simulated", stake_sol=base["stake"] / 1e9, tokens_raw=tokens_raw,
                              min_out=base["min_out"], slippage=base["slip"], slot=slot, cu=cu, fee_sol=fee_sol,
                              note=note)
        self.positions[pid] = Position(
            id=pid, portfolio="REAL", mint=base["mint"], opened_ts=time.time(), entry_mc=entry_mc, cost_sol=cost_sol,
            tokens=tokens_raw / 1e6, tokens_initial=tokens_raw / 1e6, target_mc=base.get("target_mc"),
            trader=base["trader"],
            symbol=base["symbol"], max_mc=entry_mc, alert_id=base["alert_id"],
            extra={"mode": base["mode"], "kind": base["kind"], "venue": base["venue"],
                   "token_program": base["token_program"], "tokens_raw": tokens_raw, "rent": rent_sol,
                   "last_mc": entry_mc, "entry_slot": slot, "best_exit": base.get("best_exit"),
                   "dec": base.get("dec")})
        if self.get("real_rule_trials"):
            try:
                await self._open_trials(base, pid, entry_mc, cost_sol - rent_sol, tokens_raw, slot)
            except Exception:  # noqa: BLE001 — пробы не должны мешать торговле
                log.exception("реальный бот: пробы правил %s", base["mint"])
        if base["mode"] == "live" or self.get("real_notify_shadow"):
            sol_usd = self.sol_usd()
            await self._say(f"{MODE_ICON[base['mode']]} Купил ${fmt.esc(base['symbol'] or '?')} "
                            f"({KINDS[base['kind']]}) на ${cost_sol * sol_usd:.2f} · MC {fmt.mc_usd(entry_mc, sol_usd)}"
                            + (f" · через {blocks} бл. после сигнала" if blocks is not None else
                               f" · через {float(base.get('lag_s') or 0):.1f} с после сигнала"))

    # ------------------------------------------------------------------ сопровождение
    async def on_trade(self, st: Any, eff: Any, trader: str, is_buy: bool, slot: int | None = None,
                       sol: float | None = None) -> None:
        """Каждая сделка монеты из потока: выход за трейдером, мелкая докупка, продажа дева, стоп/тейк по цене."""
        both = [*self.positions.values(), *self.trials.values()]
        for p in [p for p in both if p.mint == st.mint and not p.closing]:
            if p.extra.get("retry_at"):
                continue                              # продажа уже идёт повторами из цикла
            sc = self.scheme_of(p)
            reason = ("emergency" if eff is not None and getattr(eff, "cluster_sell", False)
                      and not sc.get("ignore_emergency") else None)
            if (reason is None and p.trader and trader == p.trader and not is_buy and not sc.get("ignore_trader_sell")
                    and not (p.tp_done and sc.get("follow_runner") is False)):
                reason = "trader_sell"
            tiny = float(self.cfg.get("paper_tiny_rebuy_sol", TINY_REBUY_SOL) or 0)
            entry_slot = p.extra.get("entry_slot")
            after_entry = int(slot) > int(entry_slot) if slot and entry_slot else time.time() - p.opened_ts > 1.0
            if (reason is None and p.trader and trader == p.trader and is_buy and sol is not None and tiny > 0
                    and 0 < float(sol) <= tiny and after_entry):
                reason = "tiny_rebuy"
            mc = float(getattr(st, "mc", 0) or 0)
            # сделки блоков ДО нашей покупки приходят из потока с опозданием — их цена входу не судья
            stale = bool(slot and entry_slot and int(slot) <= int(entry_slot))
            if reason is None and (mc <= 0 or stale):
                continue
            self._check(p, mc if mc > 0 and not stale else float(p.extra.get("last_mc") or p.entry_mc), reason)

    def _check(self, p: Position, mc: float, reason: str | None = None, confirmed: bool = False) -> None:
        """Решение по позиции на цене mc; если пора выходить — запускает продажу.
        confirmed — цена из блокчейна (а не из потока): проверка на скачок не нужна."""
        trial = bool(p.extra.get("trial"))
        if p.closing or p.id not in (self.trials if trial else self.positions) or mc <= 0:
            return
        last = float(p.extra.get("last_mc") or 0)
        if reason is None and not confirmed and last > 0 and (mc > JUMP_X * last or mc < last / JUMP_X):
            pending = p.extra.get("jump")
            if pending is None or not 0.5 < mc / pending < 2.0:
                p.extra["jump"] = mc                  # скачок в 5+ раз ждёт подтверждения следующей сделкой
                return
        p.extra.pop("jump", None)
        p.extra["last_mc"] = mc
        d = decide_exit(p, mc, time.time(), self.scheme_of(p), pump.FEE_BPS, self.fees_sol(), emergency=reason)
        if d is None:
            return
        if trial:
            p.closing = True
            asyncio.create_task(self._trial_exit_safe(p, d[0], float(d[1])))
        else:
            self._start_exit(p, d[0], float(d[1]))

    def _start_exit(self, p: Position, reason: str, share: float) -> None:
        p.closing = True
        p.extra["exit"] = [reason, share]
        p.extra.pop("retry_at", None)
        asyncio.create_task(self._exit_safe(p, reason, share))

    async def _exit_safe(self, p: Position, reason: str, share: float) -> None:
        try:
            if p.extra.get("mode") == "live":
                await self._sell_live(p, reason, share)
            else:
                await self._sell_shadow(p, reason, share)
        except rtx.Forbidden as exc:
            log.error("реальный бот: продажа не из белого списка — не подписана: %s", exc)
            p.closing = False
            p.extra["retry_at"] = time.time() + 60
        except Exception:  # noqa: BLE001
            log.exception("реальный бот: выход %s", p.mint)
            p.closing = False
            p.extra["retry_at"] = time.time() + 15

    @staticmethod
    def _sell_amount(p: Position, share: float) -> int:
        have = int(p.extra["tokens_raw"])
        return int(have * share) if share < 1 else have

    async def _sell_shadow(self, p: Position, reason: str, share: float) -> None:
        await asyncio.sleep(float(self.get("real_land_s")))
        raw = self._sell_amount(p, share)
        try:
            st = await self._state(Pubkey.from_string(p.mint))
        except Exception:  # noqa: BLE001
            st = {"why": "сеть не ответила", "venue": None}
        jq = await self._jup_sell(p, raw) if p.extra.get("venue") == JUP_VENUE else None
        if jq is not None:                           # другая биржа: настоящая котировка Jupiter на продажу
            proceeds, mc = jq
        elif st.get("venue") and not st.get("why"):
            proceeds = max(0.0, self._quote_sell(st, raw) / 1e9 - self.fees_sol())
            mc = st["mc"]
        else:                                        # состояние не получили — оценка по последней цене потока
            mc = float(p.extra.get("last_mc") or p.entry_mc)
            sell_mc = mc * (1 - JUP_SLIP) if p.extra.get("venue") == JUP_VENUE else mc
            proceeds = sell_fill(sell_mc, raw / 1e6, pump.FEE_BPS, self.fees_sol()).sol
        await self._settle(p, reason, share, raw, proceeds, mc, sig=None, slot=st.get("slot") or None, wait_ms=None,
                           fee=self.fees_sol())

    async def _prior_sell(self, p: Position, minutes: int = 10) -> bool:
        """Не легла ли одна из прошлых попыток продажи (с опозданием или пока бот был выключен)."""
        rows = await self.db.fetch(
            "SELECT id, signature, tokens_raw, base, extract(epoch FROM now() - created_at) AS age_s FROM real_orders "
            "WHERE position_id = $1 AND side = 'sell' AND signature IS NOT NULL AND status IN ('sent', 'expired') "
            "AND created_at > now() - make_interval(mins => $2) ORDER BY id", p.id, int(minutes))
        for o in rows:
            info = await self.chain.transaction(o["signature"], tries=1)
            if not info:
                if float(o["age_s"] or 0) > ORDER_DEAD_S:
                    await self._order_done(o["id"], "dead", error="не легла в блок")
                continue
            if (info.get("meta") or {}).get("err") is not None:
                await self._order_done(o["id"], "failed", error="отклонена сетью", fee_sol=self.failed_fee_sol())
                continue
            fill = rtx.fill_from_tx(info, str(self.wallet.pubkey), [])
            meta = dict(o["base"] or {})
            raw = min(int(o["tokens_raw"] or 0) or int(p.extra["tokens_raw"]), int(p.extra["tokens_raw"]))
            share = 1.0 if raw >= int(p.extra["tokens_raw"]) else float(meta.get("share") or 1.0)
            await self._settle(p, str(meta.get("reason") or "manual"), share, raw, fill["sol_delta"] / 1e9,
                               float(p.extra.get("last_mc") or p.entry_mc), sig=o["signature"], slot=fill["slot"],
                               wait_ms=None, fee=fill["fee"] / 1e9, order_id=o["id"])
            return True
        return False

    async def _sell_live(self, p: Position, reason: str, share: float) -> None:
        if self.wallet is None or not self.wallet.can_sign:
            p.closing = False
            p.extra["retry_at"] = time.time() + 60
            p.extra["stuck"] = int(p.extra.get("stuck") or 0) + 1
            if p.extra["stuck"] in (1, 30):
                await self._say(f"🆘 💵 Не могу продать ${fmt.esc(p.symbol or '?')}: на сервере нет ключа кошелька. "
                                f"Продайте вручную. Монета: <code>{p.mint}</code>")
            return
        mint = Pubkey.from_string(p.mint)
        base = {"mode": "live", "mint": p.mint}
        user = str(self.wallet.pubkey)
        urls = self._urls()
        meta = {"reason": reason, "share": share}
        if p.extra.get("stuck") and await self._prior_sell(p):
            return
        for attempt, k in enumerate(SELL_SLIPPAGE_STEPS, 1):
            if attempt > 1 and await self._prior_sell(p):
                return
            raw = self._sell_amount(p, share)
            try:
                st, (blockhash, _last_valid) = await asyncio.gather(self._state(mint), self.chain.blockhash())
            except Exception as exc:  # noqa: BLE001
                log.warning("реальный бот: состояние перед продажей %s: %s", p.mint, str(exc)[:160])
                await asyncio.sleep(STATE_RETRY_S)
                continue
            if st["venue"] is None or st["why"]:
                await self._order(p.id, base, "sell", "failed", tokens_raw=raw, error=st["why"], attempt=attempt)
                await asyncio.sleep(UNTRADABLE_RETRY_S)
                continue
            slip = min(0.95, float(self.get("real_sell_slippage")) * k)
            min_out = int(self._quote_sell(st, raw) * (1 - slip))
            tx = self.wallet.sign(rtx.compile_message(self.wallet.pubkey, self._sell_ixs(st, mint, raw, min_out),
                                                      blockhash))
            sig = str(tx.signatures[0])
            # продажу отправляем, даже если журнал недоступен: выйти из позиции важнее записи
            oid = await self._order(p.id, base, "sell", "sent", tokens_raw=raw, min_out=min_out, slippage=slip,
                                    signature=sig, attempt=attempt, payload=meta)
            t_send = time.time()
            await self.chain.send(tx, urls)
            landed = await self.chain.confirm(tx, timeout_s=SELL_CONFIRM_S, urls=urls)
            wait_ms = int((time.time() - t_send) * 1000)
            if landed.state == "landed":
                self.fails = 0
                info = await self.chain.transaction(sig)
                fill = rtx.fill_from_tx(info, user, []) if info else None
                proceeds = fill["sol_delta"] / 1e9 if fill else self._quote_sell(st, raw) / 1e9 - self.fees_sol()
                await self._settle(p, reason, share, raw, proceeds, st["mc"], sig=sig,
                                   slot=(fill or {}).get("slot") or landed.slot, wait_ms=wait_ms,
                                   fee=fill["fee"] / 1e9 if fill else self.fees_sol(), order_id=oid)
                await self.refresh_balance(force=True)
                return
            self.fails += 1
            failed = landed.state == "failed"
            err = self._err_text(landed.err, st["venue"]) if failed else "не легла в блок"
            fee = self.failed_fee_sol() if failed else None
            if failed:
                self._fee_loss(fee)
            await self._order_done(oid, landed.state, error=err, slot=landed.slot, wait_ms=wait_ms, fee_sol=fee)
            if failed and (err == NO_FUNDS or "токенов на счёте меньше" in err):
                if await self._prior_sell(p) or await self._tokens_gone(p, mint):
                    return
        # все попытки неудачны: не легла ли одна из них позже и есть ли вообще токены
        if await self._prior_sell(p) or await self._tokens_gone(p, mint):
            return
        p.closing = False
        p.extra["retry_at"] = time.time() + 20
        p.extra["stuck"] = int(p.extra.get("stuck") or 0) + 1
        if p.extra["stuck"] in (1, 5, 20):
            await self._say(f"🆘 💵 НЕ СМОГ ПРОДАТЬ ${fmt.esc(p.symbol or '?')} ({EXIT_NAMES.get(reason, reason)}): "
                            f"{len(SELL_SLIPPAGE_STEPS)} попытки не прошли. Повторяю каждые 20 с. "
                            f"Монета: <code>{p.mint}</code>")
        await self._check_fail_pause()

    # ------------------------------------------------------------------ пробы правил выхода
    @staticmethod
    def _trial_position(r: Any) -> Position:
        raw = int(r["tokens_raw"] or 0)
        p = Position(
            id=int(r["id"]), portfolio="TRIAL", mint=r["mint"], opened_ts=r["opened_at"].timestamp(),
            entry_mc=float(r["entry_mc"] or 0), cost_sol=float(r["cost_sol"] or 0), tokens=raw / 1e6,
            tokens_initial=raw / 1e6, target_mc=None, trader=r["trader"], symbol=r["symbol"] or "",
            proceeds_sol=float(r["proceeds_sol"] or 0), max_mc=float(r["max_mc"] or r["entry_mc"] or 0),
            tp_done=bool(r["tp_done"]), alert_id=r["alert_id"],
            extra={"trial": True, "rule": r["rule"], "mode": r["mode"], "kind": r["kind"], "tokens_raw": raw,
                   "last_mc": float(r["max_mc"] or r["entry_mc"] or 0), "position_id": r["position_id"],
                   "saved_max": float(r["max_mc"] or 0), "w_hold": r["hold_s"]})
        if p.max_mc > p.entry_mc:
            # после перезапуска: пик цены — из базы, иначе трейлинг R «забывал», что монета уже росла
            p.max_net = p.net_if_sold(p.max_mc, pump.FEE_BPS, 0.0)
        return p

    async def _flush_trials(self) -> None:
        """Пик цены открытых проб — в базу (раз в минуту): переживает перезапуск."""
        rows = [(t.id, float(t.max_mc)) for t in self.trials.values() if t.max_mc > float(t.extra.get("saved_max") or 0)]
        if not rows:
            return
        for tid, mx in rows:
            await self.db.execute("UPDATE real_rule_trials SET max_mc=$2 WHERE id=$1", tid, mx)
            if tid in self.trials:
                self.trials[tid].extra["saved_max"] = mx

    async def _open_trials(self, base: dict[str, Any], pid: int | None, entry_mc: float, cost_sol: float,
                           tokens_raw: int, slot: int | None) -> None:
        """На вход бота — по пробе на каждое правило выхода: тот же вход, те же токены и затраты."""
        w_hold = self.w_hold(base)
        for rule in S.TRIAL_RULES:
            if rule == "W" and w_hold is None:
                continue                         # обычное удержание трейдера неизвестно (или это не копия)
            r = await self.db.fetchrow(
                """INSERT INTO real_rule_trials(position_id, wallet_id, mode, kind, rule, mint, symbol, trader,
                       alert_id, opened_at, entry_mc, cost_sol, tokens_raw, max_mc, hold_s)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),$10,$11,$12,$10,$13) RETURNING *""",
                pid, self.wid, base["mode"], base["kind"], rule, base["mint"], base["symbol"], base["trader"],
                base["alert_id"], entry_mc, cost_sol, tokens_raw, w_hold if rule == "W" else None)
            t = self._trial_position(r)
            t.extra["entry_slot"] = slot
            if base.get("venue") == JUP_VENUE:
                t.extra["venue"] = JUP_VENUE          # пробы тоже сопровождаются ценами Jupiter
                t.extra["dec"] = base.get("dec")
            self.trials[t.id] = t

    def best_exit(self, wallet: str | None) -> str | None:
        """Лучший выход кошелька по его сделкам (traders.best_rule) — для позиций «за ним»."""
        if not wallet:
            return None
        t = (getattr(self.e, "traders", None) or {}).get(wallet) or {}
        try:
            b = t.get("best_rule")
        except AttributeError:
            return None
        return b if b in ("F", "A", "B", "G", "R", "W") else None

    def w_hold(self, base: dict[str, Any]) -> float | None:
        """Проба W: через сколько секунд после нашего входа продать — обычное удержание трейдера − 2 с − наше
        опоздание (от его покупки до нашей). None — трейдера нет или удержание неизвестно / дольше 30 мин."""
        t = (getattr(self.e, "traders", None) or {}).get(base.get("trader") or "")
        hold = float((t or {}).get("median_hold_s") or 0)
        if hold <= 0 or hold > 1800:
            return None
        late = float(base.get("lag_s") or 0) + float(self.get("real_land_s"))
        return max(1.0, hold - 2.0 - late)

    async def _trial_state(self, mint: str) -> dict[str, Any]:
        """Состояние монеты для продажи проб: пробы одной монеты выходят вместе (тейк A / B / G) — один запрос на всех."""
        now = time.time()
        hit = self._trial_st.get(mint)
        if hit is None or now - hit[0] > 1.0:
            hit = (now, asyncio.ensure_future(self._state(Pubkey.from_string(mint))))
            self._trial_st = {m: v for m, v in self._trial_st.items() if now - v[0] <= 1.0}
            self._trial_st[mint] = hit
        return await asyncio.shield(hit[1])

    async def _trial_exit_safe(self, p: Position, reason: str, share: float) -> None:
        try:
            await self._trial_exit(p, reason, share)
        except Exception:  # noqa: BLE001
            log.exception("реальный бот: проба правила %s", p.mint)
            p.closing = False

    async def _trial_exit(self, p: Position, reason: str, share: float) -> None:
        """Продажа пробы: как в тени — цена по состоянию кривой/пула через real_land_s (без транзакции)."""
        await asyncio.sleep(float(self.get("real_land_s")))
        have = int(p.extra["tokens_raw"])
        raw = int(have * share) if share < 1 else have
        st: dict[str, Any] = {}
        jq = await self._jup_sell(p, raw) if p.extra.get("venue") == JUP_VENUE else None
        if jq is None and self.chain is not None and p.extra.get("venue") != JUP_VENUE:
            try:
                st = await self._trial_state(p.mint)
            except Exception:  # noqa: BLE001
                st = {}
        if jq is not None:
            proceeds, mc = jq
        elif st.get("venue") and not st.get("why"):
            proceeds, mc = max(0.0, self._quote_sell(st, raw) / 1e9 - self.fees_sol()), float(st["mc"])
        else:
            mc = float(p.extra.get("last_mc") or p.entry_mc)
            sell_mc = mc * (1 - JUP_SLIP) if p.extra.get("venue") == JUP_VENUE else mc
            proceeds = sell_fill(sell_mc, raw / 1e6, pump.FEE_BPS, self.fees_sol()).sol
        p.proceeds_sol += proceeds
        p.extra["tokens_raw"] = have - raw
        p.tokens = p.extra["tokens_raw"] / 1e6
        p.max_mc = max(p.max_mc or 0.0, mc)
        if share < 1 and p.extra["tokens_raw"] > 0:
            p.tp_done, p.closing = True, False
            await self.db.execute("UPDATE real_rule_trials SET tokens_raw=$2, proceeds_sol=$3, tp_done=true, max_mc=$4 "
                                  "WHERE id=$1", p.id, p.extra["tokens_raw"], p.proceeds_sol, p.max_mc)
            return
        pnl = p.proceeds_sol - p.cost_sol
        await self.db.execute(
            """UPDATE real_rule_trials SET tokens_raw=0, proceeds_sol=$2, closed_at=now(), exit_reason=$3, exit_mc=$4,
                   pnl_sol=$5, pnl_pct=$6, max_mc=$7 WHERE id=$1""",
            p.id, p.proceeds_sol, reason, mc, pnl, pnl / p.cost_sol if p.cost_sol else 0.0, p.max_mc)
        self.trials.pop(p.id, None)

    async def _tokens_gone(self, p: Position, mint: Pubkey) -> bool:
        """Сколько токенов на самом деле на счёте: ноль (проданы или переведены вручную) — закрываем позицию без
        выручки; меньше учтённого — дальше продаём столько, сколько есть."""
        tp = pump.TOKEN_PROGRAMS.get(str(p.extra.get("token_program")))
        if tp is None:
            return False
        acc = (await self.chain.accounts([pump.ata(self.wallet.pubkey, mint, tp)], "confirmed"))[0]
        have = pumpswap.token_amount(acc.data) if acc is not None and len(acc.data) >= 72 else 0
        if have >= int(p.extra["tokens_raw"]):
            return False
        if have > 0:
            p.extra["tokens_raw"] = have
            p.tokens = have / 1e6
            return False
        await self._settle(p, "gone", 1.0, int(p.extra["tokens_raw"]), 0.0, float(p.extra.get("last_mc") or 0),
                           sig=None, slot=None, wait_ms=None, fee=0.0)
        return True

    async def _settle(self, p: Position, reason: str, share: float, raw: int, proceeds: float, mc: float, *,
                      sig: str | None, slot: int | None, wait_ms: int | None, fee: float,
                      order_id: int | None = None) -> None:
        """Продажа состоялась (в бою — подтверждена сетью): учёт выручки, закрытие позиции, итоги, сообщения."""
        mode = p.extra.get("mode", "shadow")
        p.proceeds_sol += proceeds
        p.extra["tokens_raw"] = max(0, int(p.extra["tokens_raw"]) - raw)
        p.tokens = p.extra["tokens_raw"] / 1e6
        p.max_mc = max(p.max_mc or 0.0, mc)
        p.extra.pop("stuck", None)
        note = EXIT_NAMES.get(reason, reason)
        if order_id is not None:
            await self._order_done(order_id, "landed", slot=slot, wait_ms=wait_ms, fee_sol=fee, note=note)
        else:
            await self._order(p.id, {"mode": mode, "mint": p.mint}, "sell", "landed" if mode == "live" else "simulated",
                              tokens_raw=raw, signature=sig, slot=slot, wait_ms=wait_ms, fee_sol=fee, note=note)
        if share < 1 and p.extra["tokens_raw"] > 0:
            p.tp_done, p.closing = True, False
            p.extra.pop("exit", None)
            await self.db.execute("UPDATE real_positions SET tokens=$2, tokens_raw=$3, proceeds_sol=$4, tp_done=true, "
                                  "max_mc=$5 WHERE id=$1", p.id, p.tokens, p.extra["tokens_raw"], p.proceeds_sol, p.max_mc)
            return
        pnl = p.proceeds_sol - p.cost_sol
        pct = pnl / p.cost_sol if p.cost_sol else 0.0
        await self.db.execute(
            """UPDATE real_positions SET tokens=0, tokens_raw=0, proceeds_sol=$2, closed_at=$3, exit_reason=$4,
                   exit_mc=$5, sell_sig=$6, pnl_sol=$7, pnl_pct=$8, max_mc=$9 WHERE id=$1""",
            p.id, p.proceeds_sol, datetime.now(timezone.utc), reason, mc, sig, pnl, pct, p.max_mc)
        self.positions.pop(p.id, None)
        self._roll_day()
        self.day_pnl[mode] = self.day_pnl.get(mode, 0.0) + pnl
        if pnl < 0:
            self.lost[p.mint] = time.time()
        sol_usd = self.sol_usd()
        if mode == "live":
            self.live_total += pnl
            if self.get("real_close_ata") and reason != "gone":
                self.to_close[p.mint] = (str(p.extra.get("token_program")), time.time() + 20, 1)
        if mode == "live" or self.get("real_notify_shadow"):
            await self._say(f"{'✅' if pnl > 0 else '❌'} {MODE_ICON.get(mode, '')} Продал "
                            f"${fmt.esc(p.symbol or '?')}: {pct:+.0%} ({pnl * sol_usd:+.2f}$) · {note} · "
                            f"за день {self.day_pnl[mode] * sol_usd:+.2f}$")
        await self._profit_watch(mode, pnl, p)
        if mode == "live":
            await self._check_stops()

    async def _profit_watch(self, mode: str, pnl: float, p: Position) -> None:
        """23.11: крупный плюс со сделки и ступени плюса за день — короткими сообщениями; в бою — замок прибыли дня:
        плюс дошёл до real_lock_after_usd и упал ниже real_lock_keep от лучшего за день — новых входов до полуночи нет."""
        if mode != "live" and not self.get("real_notify_shadow"):
            return
        if not hasattr(self, "day_peak"):
            self.day_peak, self.day_note, self.day_locked = {}, {}, set()
        sol_usd = self.sol_usd()
        usd, day = pnl * sol_usd, self.day_pnl.get(mode, 0.0) * sol_usd
        icon = MODE_ICON.get(mode, "")
        name = S.KIND_NAMES.get(str(p.extra.get("kind")), "")
        if usd >= float(self.get("real_big_win_usd")):
            await self._say(f"💰 {icon} +${usd:.0f} · ${fmt.esc(p.symbol or '?')} · {name}")
        step = float(self.get("real_day_note_usd"))
        level = int(day // step) if day > 0 else 0
        if level > int(self.day_note.get(mode, 0)):
            self.day_note[mode] = level
            start = getattr(self, "day_start_balance", None)
            grow = (f" · баланс ×{(start / 1e9 * sol_usd + day) / (start / 1e9 * sol_usd):.2f}"
                    if mode == "live" and start else "")
            await self._say(f"📈 {icon} За сегодня +${day:.0f}{grow}")
        self.day_peak[mode] = max(float(self.day_peak.get(mode, 0.0)), day)
        peak = self.day_peak[mode]
        if (mode == "live" and mode not in self.day_locked and peak >= float(self.get("real_lock_after_usd"))
                and day < float(self.get("real_lock_keep")) * peak):
            self.day_locked.add(mode)
            await self._say(f"🔒 {icon} Берегу прибыль: за день было +${peak:.0f}, сейчас +${day:.0f} — новых входов "
                            "нет до полуночи, открытые закроются по правилам.")

    # ------------------------------------------------------------------ защита
    async def _check_stops(self) -> None:
        sol_usd = self.sol_usd()
        if -self.live_total * sol_usd >= float(self.get("real_total_stop_usd")) and self.get("real_mode") == "live":
            await self.set_setting("real_mode_at", datetime.now(timezone.utc).isoformat())
            await self.set_setting("real_mode_by", f"общий стоп: убыток боя {self.live_total * sol_usd:+.2f}$")
            await self.set_setting("real_mode", "shadow")
            await self._say(f"🛑 💵 Общий стоп: убыток боя {self.live_total * sol_usd:+.2f}$ дошёл до лимита "
                            f"${self.get('real_total_stop_usd'):g}. Бой выключен, бот в режиме «тень». "
                            "Открытые позиции будут закрыты по правилам.")
        elif -self.day_pnl.get("live", 0.0) * sol_usd >= float(self.get("real_day_stop_usd")):
            await self._say(f"🛑 💵 Дневной стоп: за сегодня {self.day_pnl['live'] * sol_usd:+.2f}$ — "
                            "новых входов нет до полуночи.")

    async def _check_fail_pause(self) -> None:
        if self.fails >= int(self.get("real_fail_pause")) and not self.paused():
            await self.set_setting("real_paused", f"{self.fails} неудачных транзакций подряд")
            await self._say(f"⏸ 💵 {self.fails} неудачных транзакций подряд — входы остановлены. Открытые позиции "
                            "бот продолжает закрывать. Разберитесь и нажмите «▶️ Продолжить» в /real.")

    async def command(self, p: dict[str, Any]) -> None:
        """Команды из Telegram (через очередь заданий)."""
        cmd = p.get("cmd")
        if cmd == "sell_all":
            n = 0
            for pos in list(self.positions.values()):
                if not pos.closing and p.get("mode") in (None, pos.extra.get("mode")):
                    n += 1
                    self._start_exit(pos, "sell_all", 1.0)
            await self._say(f"Продаю всё: позиций {n}.")
        elif cmd == "sell":
            pos = self.positions.get(int(p.get("id") or 0))
            if pos is not None and not pos.closing:
                self._start_exit(pos, "manual", 1.0)
        elif cmd == "resume":
            self.fails = 0
        elif cmd == "check" and not self._checking:
            asyncio.create_task(self._selfcheck())
        await self.publish_status()

    async def _selfcheck(self) -> None:
        self._checking = True
        try:
            from .selfcheck import run
            text = await run(self)
        except Exception as exc:  # noqa: BLE001
            log.exception("реальный бот: проверка на сети")
            text = f"🧪 Проверка на сети не выполнена: {type(exc).__name__}"
        finally:
            self._checking = False
        await self._say(text)

    # ------------------------------------------------------------------ цикл
    async def loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            try:
                await self.tick()
            except Exception:  # noqa: BLE001 — цикл реального бота не должен ронять аналитику
                log.exception("реальный бот: цикл")

    async def tick(self) -> None:
        now = time.time()
        await self._flush_skips()
        if self.cfg.get("real_armed_at") != self._armed_seen:     # бой включили заново — общий стоп считается с нуля
            self._armed_seen = self.cfg.get("real_armed_at")
            self.live_total = await self._live_total_db()
        mode = self.mode()
        want = self.get("real_mode")
        if self._want_seen is not None and want != self._want_seen:
            # 18.11: бой «пропал» (сигналы smart пошли в тень), а почему — не было видно. Любая смена режима — сообщением
            by = str(self.cfg.get("real_mode_by") or "настройка кошелька изменена")
            await self._say(f"🔀 Режим: {MODE_SHORT_RU.get(self._want_seen, self._want_seen)} → "
                            f"<b>{MODE_SHORT_RU.get(want, want)}</b> ({fmt.esc(by)})")
        self._want_seen = want
        if want != "live":
            self._warned_live = False
        elif mode != "live" and not self._warned_live:
            self._warned_live = True
            await self._say(f"💵 Режим «бой» выбран, но не включён: {self.why_not_live()}. Работаю в режиме «тень».")
        for p in [p for p in self.positions.values() if not p.closing]:
            if p.extra.get("retry_at"):               # повтор зависшей продажи
                if now >= p.extra["retry_at"]:
                    reason, share = p.extra.get("exit") or ["manual", 1.0]
                    self._start_exit(p, str(reason), float(share))
                continue
            self._check(p, float(p.extra.get("last_mc") or p.entry_mc), None, confirmed=True)   # выходы по времени
        for t in [t for t in self.trials.values() if not t.closing]:
            self._check(t, float(t.extra.get("last_mc") or t.entry_mc), None, confirmed=True)
        if now - self._follow_at >= 30:
            self._follow_at = now
            try:
                self.follow = dict(await self.db.kv_get("real:follow") or {})
                self.follow_auto = set(await self.db.kv_get("real:follow_auto") or [])
                await self.load_autotune()
            except Exception:  # noqa: BLE001
                pass
        if self.trials and now - self._trials_saved >= 60:
            self._trials_saved = now
            try:
                await self._flush_trials()
            except Exception as exc:  # noqa: BLE001
                log.warning("реальный бот: пики проб: %s", str(exc)[:120])
        # цена открытых позиций — прямо из сети, и в бою, и в тени (17.11: тень видела цену только по сделкам из
        # потока, пропустила пик +32% и вышла на +19%, когда бумага взяла тейк)
        open_ps = self.watched()
        if open_ps and not self.external_poll and self.chain is not None \
                and now - self._last_poll >= float(self.get("real_poll_s")):
            self._last_poll = now
            await self._poll(open_ps)
        if self.to_close and not self._sweeping and self.wallet is not None and self.wallet.can_sign \
                and self.chain is not None:
            asyncio.create_task(self._sweep_safe())
        await self.refresh_balance()
        if now - self._last_status >= 5:
            await self.publish_status()

    def pollable(self) -> list[Position]:
        """Открытые позиции, цену которых надо проверить (не продаются прямо сейчас и не ждут повтора продажи)."""
        return [p for p in self.positions.values() if not p.closing and not p.extra.get("retry_at")]

    def watched(self) -> list[Position]:
        """Позиции и пробы правил, цену которых проверяем из сети."""
        return self.pollable() + [t for t in self.trials.values() if not t.closing]

    def on_state(self, p: Position, st: dict[str, Any]) -> None:
        """Цена позиции из сети (кривая / пул) — решение о выходе."""
        if st.get("venue") and st.get("mc"):
            p.extra["venue"] = st["venue"]
            self._check(p, float(st["mc"]), None, confirmed=True)

    async def _poll(self, ps: list[Position]) -> None:
        """Контроль цены открытых позиций: состояние кривой/пула из блокчейна одним запросом на 16 монет
        (по многим монетам сделки из потока не приходят или приходят с опозданием)."""
        mints = list(dict.fromkeys(p.mint for p in ps))
        by_mint: dict[str, dict[str, Any]] = {}
        for i in range(0, min(len(mints), 64), 16):           # пробы правил R живут до суток — монет бывает больше 16
            chunk = mints[i:i + 16]
            try:
                sts, _ = await self._states([Pubkey.from_string(m) for m in chunk])
            except Exception as exc:  # noqa: BLE001
                log.warning("реальный бот: состояние позиций: %s", str(exc)[:120])
                break
            by_mint.update(zip(chunk, sts))
        for p in ps:
            if p.mint in by_mint:
                self.on_state(p, by_mint[p.mint])

    async def _sweep_safe(self) -> None:
        self._sweeping = True
        try:
            await self._sweep()
        except Exception:  # noqa: BLE001
            log.exception("реальный бот: закрытие пустых счетов")
        finally:
            self._sweeping = False

    async def _sweep(self) -> None:
        """Закрыть пустые счета токенов проданных монет — вернуть залог (~0.0015 SOL с каждой)."""
        now = time.time()
        due = [(m, tp, n) for m, (tp, at, n) in self.to_close.items() if now >= at and tp in pump.TOKEN_PROGRAMS
               and not any(p.mint == m for p in self.positions.values())][:6]
        for m in [m for m, (tp, _at, _n) in self.to_close.items() if tp not in pump.TOKEN_PROGRAMS]:
            self.to_close.pop(m, None)
        if not due:
            return
        for m, _tp, _n in due:
            self.to_close.pop(m, None)
        user = self.wallet.pubkey
        accs = await self.chain.accounts([pump.ata(user, Pubkey.from_string(m), pump.TOKEN_PROGRAMS[tp])
                                          for m, tp, _n in due], "confirmed")
        ixs = rtx.budget_ixs(rtx.CU_LIMIT[("close", "close")] * len(due), 5_000)
        closing = []
        for (m, tp, n), acc in zip(due, accs):
            if acc is not None and len(acc.data) >= 72 and pumpswap.token_amount(acc.data) == 0:
                ixs.append(pump.close_ata_ix(user, Pubkey.from_string(m), pump.TOKEN_PROGRAMS[tp]))
                closing.append((m, tp, n))
        if not closing:
            return
        rtx.check_instructions(ixs, user, 0)
        blockhash, last_valid = await self.chain.blockhash()
        tx = self.wallet.sign(rtx.compile_message(user, ixs, blockhash))
        sig = await self.chain.send(tx)
        landed = await self.chain.confirm(tx, timeout_s=40.0, resend_s=3.0, last_valid_height=last_valid or None)
        await self._order(None, {"mode": "live", "mint": closing[0][0]}, "close", landed.state, signature=sig,
                          slot=landed.slot, error=landed.err, note=f"закрыто счетов: {len(closing)}")
        if landed.state != "landed":
            for m, tp, n in closing:                  # не вышло — ещё пара попыток позже
                if n < SWEEP_TRIES:
                    self.to_close[m] = (tp, time.time() + 300, n + 1)
        await self.refresh_balance(force=True)

    # ------------------------------------------------------------------ состояние и сообщения
    @staticmethod
    def _err_text(err: Any, venue: str) -> str:
        code = None
        try:
            code = err["InstructionError"][1]["Custom"]
        except Exception:  # noqa: BLE001
            code = None
        table = pump.ERRORS if venue == "pump" else pumpswap.ERRORS
        if code in table:
            return table[code]
        if code == 1:
            return NO_FUNDS
        return str(err)[:160]

    async def say_raw(self, text: str) -> None:
        await self._say(text)

    async def _say(self, text: str) -> None:
        if self.name:
            text = f"👛 <b>{fmt.esc(self.name)}</b> · {text}"
        try:
            await self.db.enqueue("text", {"topic": TOPIC, "text": text}, priority=8)
        except Exception:  # noqa: BLE001
            log.exception("реальный бот: сообщение")

    async def publish_status(self) -> None:
        self._last_status = time.time()
        self._roll_day()
        try:
            await self.db.kv_set(self.status_key(), {
                "wallet_id": self.wid, "name": self.name, "accept_new": self.accept_new,
                "at": datetime.now(timezone.utc).isoformat(), "mode": self.mode(), "want": self.get("real_mode"),
                "why_not_live": self.why_not_live(), "wallet": str(self.wallet.pubkey) if self.wallet else None,
                "can_sign": bool(self.wallet and self.wallet.can_sign), "wallet_note": self.wallet_note,
                "balance_sol": None if self.balance is None else self.balance / 1e9, "paused": self.paused(),
                "open": [{"id": p.id, "mint": p.mint, "symbol": p.symbol, "mode": p.extra.get("mode"),
                          "kind": p.extra.get("kind"), "cost": p.cost_sol,
                          "net": p.net_if_sold(float(p.extra.get("last_mc") or p.entry_mc), pump.FEE_BPS,
                                               self.fees_sol()),
                          "age_s": time.time() - p.opened_ts, "stuck": p.extra.get("stuck")}
                         for p in self.positions.values()],
                "day_pnl": self.day_pnl, "live_total": self.live_total, "fails": self.fails,
                "skips": dict(self.skips.most_common(14)), "since": self.since.isoformat(),
                "rpc_calls": getattr(self.chain, "calls", 0), "live_allowed": S.live_allowed(),
                "max_stake_usd": S.max_stake_usd(), "sol_usd": self.sol_usd()})
        except Exception:  # noqa: BLE001
            log.exception("реальный бот: статус")
