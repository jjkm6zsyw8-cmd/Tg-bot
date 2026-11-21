"""Разведка: бот сам ищет кошельки для копирования, проверяет их на испытательном сроке и чистит список.

Так же, как это делается руками (GMGN / Axiom → «Top traders» монеты → история кошелька → в список):

1. Кандидаты (раз в 15 минут; с 18.11 ещё «рядом со smart» — кто покупает те же монеты, что прибыльные smart, см.
   discover_cobuyers):
   • свои данные: кошелёк в топ-10 по прибыли на 2+ разных монетах за 3 дня (coin_top_traders — по каждой монете,
     которую бот вёл от запуска; кластер дева, бандлы и снайперы туда не попадают);
   • GMGN (если задан GMGN_API_KEY): монеты в тренде за час → их лучшие трейдеры по реализованной прибыли.
   Сразу отсеиваются: биржи и пулы, подозрительные, снайперы, бандлеры, инсайдеры, девы; кто ещё держит больше
   половины купленного (не трейдер, а холдер); уже в своём списке; отсеянные недавно. С GMGN — ещё статистика кошелька
   за 7 дней: винрейт, прибыль, сколько монет, среднее удержание (бот-снайпер держит секунды), доля сливов хуже −50%,
   запускает ли сам монеты.
2. Испытательный срок (до 4 дней, до 40 кошельков одновременно; за 6 часов меньше 2 копий — место следующему):
   бот подписывается на кошелёк и копирует каждую его
   покупку в бумажный портфель H по $10 — с той же задержкой, рыночной ценой и выходами, что настоящие копии (как G).
   Это главный фильтр: красивый PnL на GMGN не значит, что его можно повторить с задержкой в 2 секунды.
3. Решение: 6+ закрытых копий, итог в плюсе и прибыльных ≥ 40% → в свой список (копируется как свои кошельки,
   метка «🔍»). Иначе — отсев на 14 дней. 4 копии подряд в минусе — отсев сразу. 4 дня и меньше 6 копий — «мало
   сделок», отсев на 7 дней.
4. Чистка: кошелёк разведки в smart — по последним копиям в A (как боевой бот): от 6 копий и итог в минусе, либо
   3 последние в минусе в среднем −20% и хуже → убирается (14 дней). Винрейт сам по себе — не повод (с 19.11).
С 18.11 решение испытания — по копиям в S (правило боевого бота A), H (правило G) — для сравнения. С 19.11 (решение
владельца: «пох какой винрейт у кошеля, главное чтоб бот не терял бабки») в smart два пути — см. verdict_a.
   Кошельки, добавленные вручную, бот не трогает: в /wallets и в сводке они помечаются «шлак — убрать».

Всё, что разведка нашла и решила, лежит в таблице scout; файл — /wallets, сводка — /scout.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ..alerts.format import esc
from ..external.base import Unavailable
from . import dunescout as DS
from . import family as FAM
from . import lookalike as LK
from . import copytrade as CT
from .copytrade import linked_to_dev
from .paper import SCOUT, TREND, TRIAL

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger(__name__)

EVERY_S = 900
DEFAULTS: dict[str, Any] = {
    "scout_enabled": True,
    "scout_slots": 40,                 # кошельков на испытании одновременно (18.11: 20 → 40 — кандидатов
                                       # «рядом со smart» много, испытание было забито)
    "scout_max_auto": 15,              # сколько кошельков разведки держать в своём списке (0 — не добавлять)
    "scout_min_copies": 6,             # копий на испытании для решения
    "scout_probation_days": 4,
    "scout_cobuy_min_coins": 2,        # «рядом со smart»: столько разных монет вместе с прибыльным smart за 3 дня
    "scout_cobuy_active_h": 24,        # …и покупал за последние N часов (18.11: при 3 монетах и 6 ч кандидаты кончились)
    "scout_idle_days": 0.25,           # за 6 часов меньше 2 копий — неактивен, место следующему (18.11: из 40 на
                                       # испытании 34 за несколько часов не сделали ни одной покупки)
    # с 18.11 решение — по копиям в S (как торгует боевой бот: вход через 2 блока, всё на +25%, выход за трейдером):
    # владельцу нужны кошельки, на которых зарабатывает именно копия (у 7BNax копия — 81% в плюс, +28% на сделку)
    "scout_a_min_copies": 8,           # путь 1 «часто в плюсе»: 8+ копий, 65%+ в плюсе, в среднем +5%+
    "scout_a_min_winrate": 0.65,
    "scout_a_min_avg": 0.05,           # в среднем на копию, доля
    # путь 2 «в среднем» (с 19.11): винрейт любой, 10+ копий, в среднем +8%+ и без самой удачной копии — в плюсе
    # (одна случайная ракета не считается). +8%, а не +5%: в бою вход позже — 18.11 те же 20 сделок бой −0.6% в
    # среднем, бумага A +2.3%
    "scout_a_avg_copies": 10,
    "scout_a_avg_min": 0.08,
    "scout_a_max_copies": 20,          # столько копий и ни один путь не прошёл — отсев
    "scout_a_min_usd_day": 10,         # за сутки копии по $10 дали меньше $10 — торгует редко/мало, место следующему
    # редкий, но копируемый кошелёк (19.11): BojC — копируемость +52% за 30 дней по своей базе, сделка раз в сутки,
    # держит ~45 мин — снят через 6 ч как «почти не торгует», 32 сделки за месяц так и не скопированы. Таким — дольше:
    # «почти не торгует» не раньше 2 суток, без правила «$10 в сутки», испытание до 10 дней
    "scout_patient_min_copy": 0.10,    # копируемость за 30 дней (своя база) не ниже
    "scout_patient_min_trades": 15,    # и сделок за 30 дней не меньше
    "scout_patient_idle_days": 2.0,
    "scout_patient_probation_days": 10,
    # 20.11: в smart — только при устойчивом плюсе копий на испытании (среднее / (разброс / √n) ≥ 1.645): 8–10
    # удачных копий подряд бывают и случайно
    "scout_min_t": 1.645,
    "scout_min_winrate": 0.40,         # доля прибыльных копий
    "scout_min_profit_usd": 300,       # GMGN: прибыль на монете, $
    "scout_gmgn_min_winrate": 0.45,    # GMGN 7 дней
    "scout_gmgn_min_tokens": 10,
    "scout_gmgn_min_hold_s": 180,      # 20.11: было 30 с — на испытание шли скальперы (BYaf: продаёт через 10 с)
    # 20.11: на испытание — только кто держит монету от 3 минут (копия успевает войти и выйти; 7BNax держал ~7 мин);
    # скальперов (по своей базе или GMGN) — сразу в отсев, даже если уже на испытании. Держатели — первыми в очереди
    "scout_min_hold_s": 180,
    # держатели в своей базе: раз в 6 ч — кто за 2 дня закрыл в плюс 2+ монеты, держа их в медиане от 10 минут
    "scout_holders_every_h": 6,
    "scout_holders_min_hold_s": 600,
    "scout_holders_min_coins": 3,
    # 21.11: поиск по главному признаку — работает ли копия. Кошельки своей базы: копия за 30 дней (задержка 5 с,
    # комиссии) в плюсе, устойчиво (t от scout_min_t), 15+ сделок, держит от 3 минут → на испытание первыми
    "scout_copytest_enabled": True,
    "scout_copytest_every_h": 6,
    "scout_copytest_min_trades": 15,
    "scout_copytest_limit": 200,         # новых кошельков за раз (расчёт копии — по запросу на кошелёк)
    "scout_gmgn_max_big_losses": 0.35,
}
BAD_TAGS = {"sniper", "bundler", "rat_trader", "dev", "dex_bot", "insider", "creator", "sandwich_bot"}
REJECT_DAYS = 14
FEW_TRADES_DAYS = 7
IDLE_DAYS = 2                          # неактивный за сутки — можно проверить снова через 2 дня


def cfg_get(cfg: dict[str, Any], key: str) -> Any:
    v = cfg.get(key)
    return DEFAULTS[key] if v is None else v


def trader_ok(t: dict[str, Any], cfg: dict[str, Any]) -> tuple[bool, str]:
    """Строка топ-трейдеров монеты (GMGN): годится ли в кандидаты."""
    if t.get("exchange") or t.get("suspicious"):
        return False, "биржа / подозрительный"
    bad = BAD_TAGS & {str(x).lower() for x in t.get("tags") or []}
    if bad:
        return False, "метка " + ", ".join(sorted(bad))
    if (t.get("profit_usd") or 0) < float(cfg_get(cfg, "scout_min_profit_usd")):
        return False, "мало прибыли на монете"
    if (t.get("sold_share") or 0) < 0.5:
        return False, "держит больше половины — холдер, а не трейдер"
    if (t.get("buys") or 0) > 20:
        return False, "больше 20 покупок одной монеты — бот"
    return True, ""


def wallet_ok(w: dict[str, Any] | None, cfg: dict[str, Any]) -> tuple[bool, str]:
    """Статистика кошелька GMGN за 7 дней: годится ли на испытание."""
    if not w:
        return True, ""                 # нет данных — решит испытание
    if w.get("is_exchange"):
        return False, "биржа"
    if (w.get("created_tokens") or 0) >= 2:
        return False, "сам запускает монеты"
    n = w.get("token_num") or 0
    if n < int(cfg_get(cfg, "scout_gmgn_min_tokens")):
        return False, f"мало монет за 7 дней ({n})"
    wr = w.get("winrate")
    if wr is not None and wr < float(cfg_get(cfg, "scout_gmgn_min_winrate")):
        return False, f"винрейт {wr:.0%}"
    if (w.get("realized_profit") or 0) <= 0:
        return False, "за 7 дней в минусе"
    hold = w.get("avg_hold_s")
    if hold is not None and hold < float(cfg_get(cfg, "scout_gmgn_min_hold_s")):
        return False, f"держит в среднем {hold:.0f} с — бот, не повторить"
    if n and (w.get("big_losses") or 0) / n > float(cfg_get(cfg, "scout_gmgn_max_big_losses")):
        return False, "часто сливает хуже −50%"
    return True, ""


def score_of(w: dict[str, Any] | None, coins: int = 1, profit_usd: float = 0.0) -> float:
    """Очередь на испытание: сначала те, кто чаще в топе и с лучшим винрейтом и прибылью."""
    wr = (w or {}).get("winrate") or 0.5
    profit = max(profit_usd, (w or {}).get("realized_profit") or 0.0, 0.0)
    return round(coins * 10 + wr * 10 + math.log10(1 + profit), 3)


def verdict(n: int, wins: int, pnl: float, cost: float, age_days: float, cfg: dict[str, Any]) -> tuple[str | None, str]:
    """Итог испытания: ('promote' | 'reject' | 'few' | None, причина). pnl/cost — SOL по закрытым копиям в H."""
    need = int(cfg_get(cfg, "scout_min_copies"))
    res = f"{n} копий, в плюсе {wins}, итог {pnl / cost:+.0%}" if n and cost else f"{n} копий"
    if n >= need:
        if pnl > 0 and wins / n >= float(cfg_get(cfg, "scout_min_winrate")):
            return "promote", res
        return "reject", res
    if n >= 4 and wins == 0:
        return "reject", res + " — все в минусе"
    if n < 2 and age_days >= float(cfg_get(cfg, "scout_idle_days")):
        return "idle", res + f" за {age_days:.0f} дн. — почти не торгует, место следующему"
    if age_days >= float(cfg_get(cfg, "scout_probation_days")):
        return "few", res + f" за {age_days:.0f} дн. — мало сделок"
    return None, res


def verdict_a(n: int, wins: int, pnl: float, cost: float, age_days: float, cfg: dict[str, Any],
              best: float = 0.0, patient: bool = False, t: float | None = None) -> tuple[str | None, str]:
    """Итог испытания по копиям в S (правило боевого бота A: вход через 2 блока, всё на +25%, выход за трейдером).
    pnl/cost — SOL по закрытым копиям, best — SOL самой удачной копии.

    В smart (с 19.11 — два пути, решение владельца: важен заработок копии, а не винрейт кошелька):
      1) «часто в плюсе»: 8+ копий, 65%+ в плюсе, в среднем +5%+;
      2) «в среднем»: 10+ копий, в среднем +8%+ и без самой удачной копии — в плюсе; винрейт любой.
    Отсев (14 дней): 4 копии и ни одной в плюсе; 6+ копий и в среднем минус; 20 копий и ни один путь не прошёл.
    Место следующему (вернуться через 2 дня): за 6 ч меньше 2 копий; за сутки копии по $10 дали меньше $10.
    patient — редкий, но копируемый (копируемость за 30 дней ≥ 10%, 15+ сделок): «почти не торгует» не раньше 2 суток,
    без правила «$10 в сутки», испытание до 10 дней."""
    avg = pnl / cost if cost else 0.0
    wo = (pnl - best) / (cost * (n - 1) / n) if n >= 2 and cost else avg      # без самой удачной копии
    res = (f"копия как у бота (A): {n} сделок, в плюсе {wins} ({wins / n:.0%}), в среднем {avg:+.1%}"
           + (f", без лучшей {wo:+.1%}" if n >= 2 else "") if n else "копий ещё нет")
    if t is not None and n:
        res += f", устойчивость t={t:.2f}"
    steady = t is None or t >= float(cfg_get(cfg, "scout_min_t"))
    path1 = (n >= int(cfg_get(cfg, "scout_a_min_copies")) and wins / n >= float(cfg_get(cfg, "scout_a_min_winrate"))
             and avg >= float(cfg_get(cfg, "scout_a_min_avg")))
    path2 = n >= int(cfg_get(cfg, "scout_a_avg_copies")) and avg >= float(cfg_get(cfg, "scout_a_avg_min")) and wo > 0
    if path1 and steady:
        return "promote", res + " — часто в плюсе"
    if path2 and steady:
        return "promote", res + " — в среднем хорошо, винрейт не важен"
    if (path1 or path2) and n < int(cfg_get(cfg, "scout_a_max_copies")):
        return None, res + f" — плюс пока не устойчивый (нужно t ≥ {float(cfg_get(cfg, 'scout_min_t')):g}), ждём копий"
    if n >= int(cfg_get(cfg, "scout_a_max_copies")):
        return "reject", res + " — ни 65% в плюсе с +5% в среднем, ни +8% в среднем"
    if (n >= 4 and wins == 0) or (n >= 6 and avg < 0):
        # слабых — сразу: место на испытании нужно следующему кандидату (в очереди их десятки)
        return "reject", res + " — копия теряет деньги"
    idle = float(cfg_get(cfg, "scout_patient_idle_days" if patient else "scout_idle_days"))
    if n < 2 and age_days >= idle:
        return "idle", res + f" за {age_days * 24:.0f} ч — почти не торгует, место следующему" + (
            " (редкий, но копируемый — ждали дольше)" if patient else "")
    per_day = float(cfg_get(cfg, "scout_a_min_usd_day"))
    if per_day > 0 and age_days >= 1 and not patient:
        usd = avg * n * float(cfg.get("paper_scout_usd") or 10)
        if usd / age_days < per_day:
            return "idle", res + (f"; за {age_days:.1f} сут. копии по $10 дали {usd:+.1f}$ — меньше ${per_day:g} в "
                                  "сутки, место следующему")
    if age_days >= float(cfg_get(cfg, "scout_patient_probation_days" if patient else "scout_probation_days")):
        return "few", res + f" за {age_days:.0f} дн. — мало сделок"
    return None, res


def scalper_text(hold_s: float, need_s: float) -> str:
    return (f"скальпер: держит монету ~{hold_s:.0f} с (на испытание — от {need_s / 60:.0f} мин) — копия входит, "
            "когда он уже продаёт")


def trial_t(x: dict[str, Any]) -> float | None:
    """Устойчивость плюса копий на испытании: среднее / (разброс / √n); меньше 3 копий — не считаем."""
    n = int(x.get("n") or 0)
    sd, avg = x.get("sd"), x.get("apct")
    if n < 3 or avg is None or sd is None:
        return None
    if float(sd) <= 0:
        return 99.0 if float(avg) > 0 else 0.0
    return float(avg) / (float(sd) / n ** 0.5)


def demote_reason(pcts: list[float], pnl_sol: float) -> str | None:
    """Кошелёк разведки в smart (бот торгует его за деньги): когда убрать. pcts — итоги последних копий в A, новые
    первыми. 18.11: кошельки, пущенные в smart по старому мягкому правилу (6 копий, 40% в плюсе), за утро дали в бою
    −$14.76 (DKvR −$4.46 за 4 сделки, 46Xr −$6.95, yWyq −$3.35), а старое правило ждало 8 копий в минусе.
    С 19.11 — по деньгам, а не по винрейту: в smart теперь пускают и кошельки с редкими, но крупными плюсами."""
    n = len(pcts)
    if not n:
        return None
    wins = sum(1 for p in pcts if p > 0)
    res = f"последние {n} копий в A (как бот): в плюсе {wins} ({wins / n:.0%}), в среднем {sum(pcts) / n:+.0%}"
    if n >= 6 and pnl_sol < 0:
        return res + " — копия теряет деньги"
    if n >= 3 and all(p <= 0 for p in pcts[:3]) and sum(pcts[:3]) / 3 <= -0.2:
        return f"последние 3 копии в A в минусе, в среднем {sum(pcts[:3]) / 3:+.0%}"
    return None


def short(addr: str) -> str:
    return f"{addr[:4]}…{addr[-4:]}"


class Scout:
    def __init__(self, engine: "Engine"):
        self.e = engine
        self.db = engine.db
        self.stats = {"found": 0, "queued": 0, "started": 0, "promoted": 0, "rejected": 0, "demoted": 0,
                      "gmgn_errors": 0, "runs": 0}
        self._token_cursor = 0
        self._trend_done: set[str] = set()      # монеты трендов, по которым уже искали ранних покупателей
        self.lookalike = LK.Lookalike(engine)
        self.family = FAM.Family(engine, self)
        self.dune = DS.DuneScout(engine, self)
        self._dune_task: asyncio.Task[None] | None = None
        self._ct_task: asyncio.Task[None] | None = None
        self._ct_at = 0.0
        self._ct_seen: dict[str, float] = {}          # кошелёк → когда считали копию (повтор — не раньше суток)

    @property
    def cfg(self) -> dict[str, Any]:
        return self.e.cfg

    async def loop(self) -> None:
        await asyncio.sleep(180)
        while True:
            try:
                if cfg_get(self.cfg, "scout_enabled"):
                    await self.run_once()
            except Exception:  # noqa: BLE001 — разведка не должна ронять аналитику
                log.exception("разведка")
            await asyncio.sleep(EVERY_S)

    async def run_once(self) -> list[str]:
        self.stats["runs"] += 1
        events: list[str] = []
        await self.discover_own()
        await self.discover_cobuyers()
        await self.discover_gmgn()
        try:
            await self.discover_holders()
        except Exception:  # noqa: BLE001 — тяжёлый запрос мог отмениться по времени; в следующий раз
            log.exception("разведка: держатели")
        events += await self.decide()
        events += await self.demote()
        events += await self.requeue_patient()
        events += await self.fill_slots()
        try:
            events += await self.lookalike.run()          # по образцу smart A: поиск раз в 3 ч, проверка раз в час
        except Exception:  # noqa: BLE001 — тяжёлый запрос мог отмениться по времени; разведка продолжает работать
            log.exception("разведка: поиск по образцу")
        try:
            events += await self.family.run()             # семья лучших: копилки → обменники → кошельки владельца
        except Exception:  # noqa: BLE001 — узел мог не ответить; в следующий раз продолжит с того же места
            log.exception("разведка: семья лучших кошельков")
        if any("👪" in e for e in events):
            events += await self.fill_slots()             # кандидат «семья» — на испытание сразу, а не через 15 минут
        if cfg_get(self.cfg, "scout_copytest_enabled") and (self._ct_task is None or self._ct_task.done()) \
                and time.time() - self._ct_at >= float(cfg_get(self.cfg, "scout_copytest_every_h")) * 3600:
            self._ct_at = time.time()
            self._ct_task = asyncio.create_task(self._copytest_bg())   # расчёт копий — минуты, в фоне
        if self.dune.key and (self._dune_task is None or self._dune_task.done()):
            self._dune_task = asyncio.create_task(self._dune_bg())   # весь PumpSwap через Dune — раз в сутки, в фоне
        await self.load()
        if events:
            await self.db.system("🔍 <b>Разведка</b>\n" + "\n".join(events[:25]))
        return events

    async def _copytest_bg(self) -> None:
        try:
            n, added = await self.discover_copytest()
            if added:
                await self.db.system("🔍 <b>Разведка — проверка копированием</b>: посчитано кошельков "
                                     f"{n}, устойчиво в плюсе и на испытание — {len(added)}:\n" + "\n".join(added[:15]))
        except Exception:  # noqa: BLE001
            log.exception("разведка: проверка копированием")

    async def discover_copytest(self) -> tuple[int, list[str]]:
        """Кандидаты по главному признаку — работает ли копия (21.11). Остальные каналы ищут по прибыли кошелька,
        а она почти всегда сделана в первые секунды — копия опаздывает. Здесь — повтор всех сделок кошелька за 30
        дней по своей базе с задержкой 5 с и комиссиями: в плюсе, устойчиво (t от scout_min_t), 15+ сделок, держит от
        3 минут, не связан с девом → на испытание (решает испытание по цене блокчейна).
        1) уже посчитанные ежесуточным пересчётом трейдеров (таблица traders); 2) новые кошельки базы."""
        need_t = float(cfg_get(self.cfg, "scout_min_t"))
        min_n = int(cfg_get(self.cfg, "scout_copytest_min_trades"))
        min_hold = float(cfg_get(self.cfg, "scout_min_hold_s"))
        known = await self._known()
        good: dict[str, tuple[float, float, int, float]] = {}
        for r in await self.db.fetch(
                """SELECT address, copy_30d, copy_t_30d, trades_30d, median_hold_s FROM traders
                   WHERE copy_t_30d >= $1 AND copy_30d > 0 AND trades_30d >= $2 AND median_hold_s >= $3
                     AND NOT linked_to_dev AND last_active > now() - interval '2 days'
                   ORDER BY copy_t_30d DESC LIMIT 50""", need_t, min_n, min_hold):
            good[r["address"]] = (float(r["copy_30d"]), float(r["copy_t_30d"]), int(r["trades_30d"]),
                                  float(r["median_hold_s"]))
        computed = 0
        costs = self.e.costs() if callable(getattr(self.e, "costs", None)) else None
        if costs is not None:
            now = time.time()
            recent = {r["address"] for r in await self.db.fetch(
                "SELECT address FROM traders WHERE updated_at > now() - interval '1 day'")}
            rows = await LK._fetch(self.db, """
                SELECT trader FROM trades WHERE ts > now() - interval '7 days'
                GROUP BY trader HAVING count(DISTINCT mint) >= $1 AND max(ts) > now() - interval '2 days'
                ORDER BY count(DISTINCT mint) DESC LIMIT $2""", max(5, min_n // 2),
                int(cfg_get(self.cfg, "scout_copytest_limit")) * 3, timeout_s=120)
            todo = [r["trader"] for r in rows if r["trader"] not in known and r["trader"] not in recent
                    and r["trader"] not in good and now - self._ct_seen.get(r["trader"], 0.0) > 86400]
            for addr in todo[:int(cfg_get(self.cfg, "scout_copytest_limit"))]:
                self._ct_seen[addr] = now
                try:
                    _s7, s30, _trips = await CT.compute_trader(self.db, addr, self.cfg, costs)
                except Exception:  # noqa: BLE001
                    log.exception("проверка копированием %s", addr)
                    continue
                computed += 1
                if (s30.trades >= min_n and s30.copyability > 0 and s30.copy_t >= need_t
                        and (s30.median_hold_s or 0) >= min_hold and not await linked_to_dev(self.db, addr)):
                    good[addr] = (s30.copyability, s30.copy_t, s30.trades, float(s30.median_hold_s or 0))
            self._ct_seen = {a: t for a, t in self._ct_seen.items() if now - t <= 86400}
        added: list[str] = []
        for addr, (copy, t, n, hold) in sorted(good.items(), key=lambda kv: -kv[1][1]):
            if addr in known:
                continue
            reason = (f"копия за 30 дней {copy:+.0%} на сделку на {n} сделках, устойчиво (t={t:.1f}), "
                      f"держит ~{hold / 60:.0f} мин")
            if await self._queue(addr, "проверка копированием", reason, 60 + t * 10,
                                 {"copy_30d": copy, "copy_t": t, "trades": n, "hold_s": hold}):
                added.append(f"• <code>{addr[:4]}…{addr[-4:]}</code> — {reason}")
        self.stats["found"] += len(added)
        return computed, added

    async def _dune_bg(self) -> None:
        """Запрос к Dune идёт минуты — в фоне, чтобы разведка не ждала; найденные — в очередь и сообщение."""
        try:
            events = await self.dune.run()
            if events:
                await self.db.system("🔍 <b>Разведка</b>\n" + "\n".join(events[:25]))
        except Exception:  # noqa: BLE001 — нет ключа, лимит кредитов, другая таблица — пишем в /scout, не падаем
            log.exception("разведка: весь PumpSwap через Dune")

    async def load(self) -> None:
        """Кошельки на испытании — движку (копии в H) и приёму (подписка на их сделки)."""
        self.e.scouts = {r["address"] for r in await self.db.fetch(
            "SELECT address FROM scout WHERE status='probation'")}

    # ------------------------------------------------------------------ кандидаты
    async def _known(self) -> set[str]:
        rows = await self.db.fetch(
            """SELECT address FROM watchlist WHERE kind IN ('trader', 'dev')
               UNION SELECT address FROM traders WHERE manual OR pinned
               UNION SELECT address FROM scout WHERE status IN ('probation', 'promoted', 'passed', 'lookalike')
                  OR (cooldown_until IS NOT NULL AND cooldown_until > now())""")
        return {r["address"] for r in rows}

    async def _queue(self, addr: str, source: str, reason: str, score: float, stats: dict[str, Any] | None) -> bool:
        res = await self.db.execute(
            """INSERT INTO scout(address, status, source, reason, score, stats)
               VALUES ($1, 'queued', $2, $3, $4, $5)
               ON CONFLICT (address) DO UPDATE SET status='queued', source=EXCLUDED.source, reason=EXCLUDED.reason,
                   score=GREATEST(EXCLUDED.score, CASE WHEN scout.status='queued' THEN scout.score ELSE 0 END),
                   stats=COALESCE(EXCLUDED.stats, scout.stats), found_at=CASE WHEN scout.status='queued'
                   THEN scout.found_at ELSE now() END, started_at=NULL, decided_at=NULL, decision=NULL,
                   cooldown_until=NULL
               WHERE scout.status IN ('queued', 'rejected', 'few', 'demoted', 'removed')
                 AND (scout.cooldown_until IS NULL OR scout.cooldown_until <= now())""",
            addr, source, reason, score, stats)
        return res.endswith("1")

    async def discover_own(self) -> int:
        rows = await self.db.fetch(
            """SELECT trader, count(DISTINCT mint) n, sum(pnl_sol) pnl,
                      percentile_cont(0.5) WITHIN GROUP (ORDER BY hold_s) hold
               FROM coin_top_traders WHERE created_at > now() - interval '3 days' AND pnl_sol > 0
               GROUP BY 1 HAVING count(DISTINCT mint) >= 2 ORDER BY 2 DESC, 3 DESC LIMIT 60""")
        known = await self._known()
        added = 0
        sol_usd = self.e.price.value or 150
        for r in rows:
            if r["trader"] in known or (r["hold"] is not None
                                        and r["hold"] < float(cfg_get(self.cfg, "scout_min_hold_s"))):
                continue
            added += await self._queue(
                r["trader"], "свои данные", f"в топе по прибыли на {r['n']} монетах за 3 дня ({r['pnl']:+.2f} SOL)",
                score_of(None, int(r["n"]), float(r["pnl"] or 0) * sol_usd), None)
        self.stats["found"] += added
        return added

    async def discover_cobuyers(self) -> int:
        """Кандидаты рядом со smart: кто покупал те же монеты, что и прибыльные smart-кошельки (бумага A за неделю
        в плюсе), в пределах 10 минут от их покупки — на 2+ разных монетах за 3 дня, покупал за последние сутки.
        18.11: 7BNax торгует только на PumpSwap монетами старше часов — там своя база сделок неполная и «топ монеты» таких не видит; сделки
        вокруг покупок smart записываются (их монеты отслеживаются). Проверяет испытание в H — по цене блокчейна."""
        # 19.11: опора — не только smart в плюсе на A, но и кандидаты в плюсе на испытании S: поиск расходится от
        # любого кошелька, на котором копия бота зарабатывает (иначе без прибыльного smart он стоит)
        smart = [r["w"] for r in await self.db.fetch(
            """SELECT source_wallet w FROM paper_positions
               WHERE portfolio = ANY($1::text[]) AND source_wallet IS NOT NULL AND opened_at > now() - interval '7 days'
                 AND closed_at IS NOT NULL AND NOT suspect
               GROUP BY 1 HAVING sum(pnl_sol) > 0 AND count(*) >= 5""", ["A", TRIAL])]
        if not smart:
            return 0
        rows = await self.db.fetch(
            """WITH s AS (SELECT DISTINCT ON (mint, trader) mint, trader, ts FROM trades
                          WHERE is_buy AND ts > now() - interval '3 days' AND trader = ANY($1::text[])
                          ORDER BY mint, trader, ts)
               SELECT c.trader, count(DISTINCT c.mint) n, count(DISTINCT s.trader) with_smart,
                      percentile_cont(0.5) WITHIN GROUP (ORDER BY c.sol_amount) sol
               FROM s JOIN trades c ON c.mint = s.mint AND c.is_buy
                    AND c.ts BETWEEN s.ts - interval '10 minutes' AND s.ts + interval '10 minutes'
               WHERE c.trader <> ALL($1::text[]) AND c.sol_amount BETWEEN 0.3 AND 5
               GROUP BY 1 HAVING count(DISTINCT c.mint) >= $2
                  AND max(c.ts) > now() - make_interval(hours => $3)   -- торгует сейчас, а не три дня назад
               ORDER BY 2 DESC, max(c.ts) DESC LIMIT 80""", smart, int(cfg_get(self.cfg, "scout_cobuy_min_coins")),
            int(cfg_get(self.cfg, "scout_cobuy_active_h")))
        known = await self._known()
        added = 0
        for r in rows:
            if r["trader"] in known:
                continue
            added += await self._queue(
                r["trader"], "рядом со smart",
                f"покупал те же монеты, что кошельки в плюсе (smart и испытание): {r['n']} монет за 3 дня "
                f"(рядом с {r['with_smart']}), обычно {r['sol']:.2f} SOL",
                float(r["n"]) * 10 + 5, None)
        self.stats["found"] += added
        return added

    async def discover_gmgn(self, tokens_per_run: int = 6, wallets_per_run: int = 15) -> int:
        ext = getattr(self.e, "ext", None)
        gm = getattr(ext, "gmgn", None)
        if gm is None or not gm.enabled:
            return 0
        try:
            hot = await gm.trending("1h", 20)
        except Unavailable:
            self.stats["gmgn_errors"] += 1
            return 0
        # 20.11: монеты трендов (T), закрытые в плюс за сутки, — первыми: кто их рано купил и держал
        trend = [{"mint": r["mint"], "symbol": r["symbol"]} for r in await self.db.fetch(
            """SELECT mint, max(symbol) symbol FROM paper_positions WHERE portfolio = $1 AND closed_at IS NOT NULL
                 AND closed_at > now() - interval '1 day' AND pnl_sol > 0 AND NOT suspect
               GROUP BY mint ORDER BY max(pnl_pct) DESC LIMIT 3""", TREND)
            if r["mint"] not in self._trend_done]
        self._trend_done.update(t["mint"] for t in trend)
        if not hot and not trend:
            return 0
        i = self._token_cursor % len(hot) if hot else 0
        batch = trend + (hot[i:] + hot[:i])[:max(0, tokens_per_run - len(trend))]
        self._token_cursor += tokens_per_run
        known = await self._known()
        cands: dict[str, tuple[dict[str, Any], str]] = {}
        for tok in batch:
            try:
                tops = await gm.top_traders(tok["mint"], 30)
            except Unavailable:
                self.stats["gmgn_errors"] += 1
                break
            for t in tops:
                ok, _ = trader_ok(t, self.cfg)
                if ok and t["address"] not in known and t["address"] not in cands:
                    cands[t["address"]] = (t, tok.get("symbol") or "?")
        added = 0
        for addr, (t, sym) in list(cands.items())[:wallets_per_run]:
            try:
                w = await gm.wallet_stats(addr, "7d")
            except Unavailable:
                self.stats["gmgn_errors"] += 1
                break
            ok, _ = wallet_ok(w, self.cfg)
            if not ok:
                continue
            stats = {k: (w or {}).get(k) for k in ("winrate", "realized_profit", "token_num", "avg_hold_s",
                                                   "big_losses", "x5", "tags", "fund_from")}
            added += await self._queue(addr, "GMGN", f"топ-трейдер ${sym}: {t['profit_usd']:+,.0f}$ на монете",
                                       score_of(w, 1, float(t["profit_usd"] or 0)), stats)
        self.stats["found"] += added
        return added

    # ------------------------------------------------------------------ испытание
    async def fill_slots(self) -> list[str]:
        busy = await self.db.fetchval("SELECT count(*) FROM scout WHERE status='probation'")
        free = int(cfg_get(self.cfg, "scout_slots")) - int(busy or 0)
        await self.db.execute("DELETE FROM scout WHERE status='queued' AND found_at < now() - interval '3 days'")
        if free <= 0:
            return []
        rows = await self.db.fetch(
            "SELECT * FROM scout WHERE status='queued' ORDER BY score DESC NULLS LAST, found_at LIMIT $1", free * 5)
        holds = await self.hold_of([r["address"] for r in rows], {r["address"]: r["stats"] for r in rows})
        need = float(cfg_get(self.cfg, "scout_min_hold_s"))
        # держатели — первыми: 30+ мин, потом 10+ мин, потом остальные по очереди
        tier = lambda a: 2 if (holds.get(a) or 0) >= 1800 else 1 if (holds.get(a) or 0) >= 600 else 0  # noqa: E731
        rows = sorted(rows, key=lambda r: (-tier(r["address"]), -(r["score"] or 0)))
        started = []
        for r in rows:
            addr = r["address"]
            h = holds.get(addr)
            if h is not None and h < need:
                # скальпера — из очереди сразу, даже если мест сейчас нет: не ждёт своей очереди зря
                await self._close(addr, "rejected", scalper_text(h, need), REJECT_DAYS)
                self.stats["rejected"] += 1
                continue
            if len(started) >= free:
                continue
            launched = await self.db.fetchval("SELECT count(*) FROM tokens WHERE deployer=$1", addr)
            if launched and launched >= 2 or await linked_to_dev(self.db, addr):
                await self._close(addr, "rejected", "дев или связан с кластером дева", REJECT_DAYS)
                continue
            await self.db.execute("UPDATE scout SET status='probation', started_at=now() WHERE address=$1", addr)
            started.append(f"{esc(short(addr))} ({esc(r['source'] or '')}: {esc(r['reason'] or '')})")
        self.stats["started"] += len(started)
        if not started:
            return []
        return [f"🧪 на испытание {len(started)}: " + "; ".join(started[:8]) + (" …" if len(started) > 8 else "")]

    async def hold_of(self, addrs: list[str], stats: dict[str, Any] | None = None) -> dict[str, float]:
        """Сколько кошелёк держит монету (медиана, с): по своей базе — пересчёт трейдеров или сделки за 30 дней
        (от первой покупки до последней продажи; 3+ монеты), иначе — средняя GMGN из данных кандидата."""
        if not addrs:
            return {}
        out = {r["address"]: float(r["median_hold_s"]) for r in await self.db.fetch(
            """SELECT address, median_hold_s FROM traders WHERE address = ANY($1::text[])
                 AND median_hold_s IS NOT NULL AND trades_30d >= 3""", addrs)}
        rest = [a for a in addrs if a not in out]
        if rest:
            try:
                rows = await LK._fetch(self.db, """
                    SELECT trader, percentile_cont(0.5) WITHIN GROUP (ORDER BY hold) hold FROM (
                        SELECT trader, mint, extract(epoch FROM max(ts) FILTER (WHERE NOT is_buy)
                                                         - min(ts) FILTER (WHERE is_buy)) hold
                        FROM trades WHERE trader = ANY($1::text[]) AND ts > now() - interval '30 days'
                        GROUP BY trader, mint) x
                    WHERE hold > 0 GROUP BY trader HAVING count(*) >= 3""", rest, timeout_s=30)
                out.update({r["trader"]: float(r["hold"]) for r in rows})
            except Exception:  # noqa: BLE001 — база занята: решит испытание
                log.warning("разведка: удержание кандидатов не посчитано")
        for a in addrs:
            if a in out:
                continue
            st = (stats or {}).get(a)
            if isinstance(st, str):
                try:
                    import json
                    st = json.loads(st)
                except ValueError:
                    st = None
            if isinstance(st, dict) and st.get("avg_hold_s") is not None:
                out[a] = float(st["avg_hold_s"])
        return out

    async def discover_holders(self) -> int:
        """Держатели в своей базе (раз в 6 ч): кто за 2 дня закрыл 2+ монеты, держа их в медиане от 10 минут, и в
        сумме по закрытым — в плюсе; покупал за последние сутки. Таких можно копировать без гонки скорости."""
        now = time.time()
        every = float(cfg_get(self.cfg, "scout_holders_every_h")) * 3600
        if now - getattr(self, "_holders_at", 0.0) < every:
            return 0
        self._holders_at = now
        rows = await LK._fetch(self.db, """
            WITH p AS (
                SELECT trader, mint, min(ts) FILTER (WHERE is_buy) b_ts, max(ts) FILTER (WHERE NOT is_buy) s_ts,
                       sum(sol_amount) FILTER (WHERE is_buy) b_sol, sum(sol_amount) FILTER (WHERE NOT is_buy) s_sol
                FROM trades WHERE ts > now() - interval '2 days' AND pool = 'pump' GROUP BY trader, mint)
            SELECT trader, count(*) n, count(*) FILTER (WHERE s_ts > b_ts) closed,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM s_ts - b_ts))
                       FILTER (WHERE s_ts > b_ts) hold,
                   sum(s_sol - b_sol) FILTER (WHERE s_ts > b_ts) pnl
            FROM p WHERE b_sol BETWEEN 0.3 AND 5
            GROUP BY trader
            HAVING count(*) >= $1 AND count(*) FILTER (WHERE s_ts > b_ts) >= 2
               AND max(b_ts) > now() - interval '24 hours'
            ORDER BY pnl DESC NULLS LAST LIMIT 300""", int(cfg_get(self.cfg, "scout_holders_min_coins")),
            timeout_s=120)
        need = float(cfg_get(self.cfg, "scout_holders_min_hold_s"))
        known = await self._known()
        sol_usd = self.e.price.value or 150
        added = 0
        for r in rows:
            if r["trader"] in known or r["hold"] is None or float(r["hold"]) < need or float(r["pnl"] or 0) <= 0:
                continue
            added += await self._queue(
                r["trader"], "держатели (своя база)",
                f"держит монету ~{float(r['hold']) / 60:.0f} мин, за 2 дня закрыл {r['closed']} монет в плюс "
                f"{float(r['pnl']):+.2f} SOL", score_of(None, int(r["closed"]), float(r["pnl"]) * sol_usd) + 50, None)
            if added >= 20:
                break
        self.stats["found"] += added
        return added

    async def _close(self, addr: str, status: str, why: str, days: float) -> None:
        await self.db.execute(
            """UPDATE scout SET status=$2, decided_at=now(), decision=$3,
                   cooldown_until = CASE WHEN $4 > 0 THEN now() + make_interval(days => $4) END WHERE address=$1""",
            addr, status, why, int(days))

    async def results(self, portfolio: str, addrs: list[str], since_col: str) -> dict[str, dict[str, Any]]:
        """Закрытые копии кошельков в портфеле с начала испытания (H) или после добавления (G)."""
        if not addrs:
            return {}
        rows = await self.db.fetch(
            f"""SELECT p.source_wallet w, count(*) n, count(*) FILTER (WHERE p.pnl_sol > 0) wins,
                       COALESCE(sum(p.pnl_sol), 0) pnl, COALESCE(sum(p.cost_sol), 0) cost,
                       COALESCE(max(p.pnl_sol), 0) best, avg(p.pnl_pct) apct, stddev_samp(p.pnl_pct) sd
                FROM paper_positions p JOIN scout s ON s.address = p.source_wallet
                WHERE p.portfolio = $1 AND p.source_wallet = ANY($2::text[]) AND p.closed_at IS NOT NULL
                  AND p.exit_reason <> 'reset' AND NOT p.suspect AND p.opened_at >= s.{since_col}
                GROUP BY 1""", portfolio, addrs)
        return {r["w"]: dict(r) for r in rows}

    async def patient_set(self, addrs: list[str]) -> set[str]:
        """Редкие, но копируемые: копируемость за 30 дней по своей базе ≥ порога и сделок достаточно."""
        if not addrs:
            return set()
        return {r["address"] for r in await self.db.fetch(
            """SELECT address FROM traders WHERE address = ANY($1::text[]) AND copy_30d >= $2 AND trades_30d >= $3""",
            addrs, float(cfg_get(self.cfg, "scout_patient_min_copy")),
            int(cfg_get(self.cfg, "scout_patient_min_trades")))}

    async def requeue_patient(self) -> list[str]:
        """Разово (19.11): вернуть в очередь редких, но копируемых, которых раньше сняли за неактивность через 6 ч или
        за «мало сделок» за 4 дня, — теперь у них испытание дольше. Первыми в очереди (по копируемости)."""
        if await self.db.kv_get("scout:patient_requeue_v1"):
            return []
        rows = await self.db.fetch(
            """UPDATE scout s SET status='queued', cooldown_until=NULL, decided_at=NULL, decision=NULL, started_at=NULL,
                   found_at=now(), score=GREATEST(COALESCE(s.score, 0), 9000) + t.copy_30d * 100
               FROM traders t
               WHERE t.address = s.address AND s.status = 'few' AND s.decided_at > now() - interval '14 days'
                 AND t.copy_30d >= $1 AND t.trades_30d >= $2
               RETURNING s.address, t.copy_30d""",
            float(cfg_get(self.cfg, "scout_patient_min_copy")), int(cfg_get(self.cfg, "scout_patient_min_trades")))
        await self.db.kv_set("scout:patient_requeue_v1", True)
        if not rows:
            return []
        names = ", ".join(f"{esc(short(r['address']))} ({float(r['copy_30d']):+.0%})" for r in rows[:10])
        return [f"🔁 снова в очередь {len(rows)} редких, но копируемых (раньше сняты за неактивность слишком рано, "
                f"теперь испытание до 10 дней): {names}"]

    async def decide(self) -> list[str]:
        rows = await self.db.fetch("SELECT * FROM scout WHERE status='probation'")
        res = await self.results(TRIAL, [r["address"] for r in rows], "started_at")
        patient = await self.patient_set([r["address"] for r in rows])
        holds = await self.hold_of([r["address"] for r in rows], {r["address"]: r["stats"] for r in rows})
        need = float(cfg_get(self.cfg, "scout_min_hold_s"))
        now = datetime.now(timezone.utc)
        auto = int(await self.db.fetchval("SELECT count(*) FROM scout WHERE status='promoted'") or 0)
        cap = int(cfg_get(self.cfg, "scout_max_auto"))
        # открытые копии в S: 19.11 FfW5 пущен в smart по 11 закрытым копиям (73% в плюсе, +5.3%), а 6 открытых в
        # это время уже были в −45…−86% (раги, правило A без стопа держит до 30 мин) — после закрытия −7.9% на копию
        open_n = {x["w"]: int(x["n"]) for x in await self.db.fetch(
            """SELECT source_wallet w, count(*) n FROM paper_positions
               WHERE portfolio = $1 AND closed_at IS NULL AND source_wallet = ANY($2::text[]) GROUP BY 1""",
            TRIAL, [r["address"] for r in rows])}
        out: list[str] = []
        for r in rows:
            h = holds.get(r["address"])
            if h is not None and h < need:
                # скальпер на испытании (пустили до 20.11) — место держателю
                await self._close(r["address"], "rejected", scalper_text(h, need), REJECT_DAYS)
                self.stats["rejected"] += 1
                out.append(f"❌ отсеян {esc(short(r['address']))} — {esc(scalper_text(h, need))}")
                continue
            x = res.get(r["address"], {"n": 0, "wins": 0, "pnl": 0.0, "cost": 0.0, "best": 0.0})
            age = (now - r["started_at"]).total_seconds() / 86400 if r["started_at"] else 0.0
            v, why = verdict_a(int(x["n"]), int(x["wins"]), float(x["pnl"]), float(x["cost"]), age, self.cfg,
                               float(x["best"]), patient=r["address"] in patient, t=trial_t(x))
            addr = r["address"]
            if v == "promote" and open_n.get(addr):
                continue                     # в smart — только когда закрылись все его копии (не дольше 30 мин)
            if v == "promote" and auto < cap:
                await self._promote(addr)
                await self._close(addr, "promoted", why, 0)
                auto += 1
                self.stats["promoted"] += 1
                out.append(f"✅ в smart: <code>{addr}</code> — {esc(why)}")
            elif v == "promote":
                await self._close(addr, "passed", why + " — свой список разведки полон", 0)
                out.append(f"☑️ прошёл, но список полон ({cap}): <code>{addr}</code> — {esc(why)}")
            elif v in ("reject", "few", "idle"):
                await self._close(addr, "rejected" if v == "reject" else "few", why,
                                  REJECT_DAYS if v == "reject" else IDLE_DAYS if v == "idle" else FEW_TRADES_DAYS)
                self.stats["rejected"] += 1
                if v == "reject":
                    out.append(f"❌ отсеян {esc(short(addr))} — {esc(why)}")
        return out

    async def _promote(self, addr: str) -> None:
        # с 12.11 прошедший испытание — сразу в smart (с фильтрами smart и в smart-портфели), а не в копии:
        # pinned — ночной пересчёт по своей базе его не снимает, снимает только итог его копий (demote)
        await self.db.execute(
            """INSERT INTO traders(address, label, manual, active, qualified, pinned, updated_at)
               VALUES ($1, $2, false, true, true, true, now())
               ON CONFLICT (address) DO UPDATE SET active=true, pinned=true, updated_at=now(),
                   label = CASE WHEN traders.label LIKE '🔍%' THEN traders.label
                                ELSE '🔍 ' || COALESCE(NULLIF(traders.label, ''), $3) END""",
            addr, f"🔍 {short(addr)}", short(addr))
        await self.e._load_traders()

    async def demote(self) -> list[str]:
        rows = await self.db.fetch(
            """SELECT s.address, t.active, t.manual, t.pinned FROM scout s LEFT JOIN traders t ON t.address = s.address
               WHERE s.status = 'promoted'""")
        out: list[str] = []
        changed = False
        for r in rows:
            addr = r["address"]
            if not (r["active"] and (r["manual"] or r["pinned"])):
                await self._close(addr, "removed", "убран вручную", REJECT_DAYS)
                continue
            last = await self.db.fetch(
                """SELECT pnl_sol, pnl_pct FROM paper_positions p JOIN scout s ON s.address = p.source_wallet
                   WHERE p.portfolio='A' AND p.source_wallet=$1 AND p.closed_at IS NOT NULL
                     AND p.exit_reason <> 'reset' AND NOT p.suspect AND p.opened_at >= s.decided_at
                   ORDER BY p.closed_at DESC LIMIT 10""", addr)
            why = demote_reason([float(x["pnl_pct"] or 0) for x in last], sum(float(x["pnl_sol"] or 0) for x in last))
            if not why:
                # испытание целиком — с копиями, закрывшимися уже после решения: ушло в минус — пущен зря
                t = (await self.results(TRIAL, [addr], "started_at")).get(addr)
                if t and int(t["n"]) >= 6 and float(t["pnl"]) < 0 and float(t["cost"]) > 0:
                    why = (f"испытание целиком (с копиями, закрытыми после решения): {int(t['n'])} копий, в плюсе "
                           f"{int(t['wins'])}, в среднем {float(t['pnl']) / float(t['cost']):+.1%}")
            if why:
                await self.db.execute("UPDATE traders SET active=false, manual=false, pinned=false WHERE address=$1",
                                      addr)
                await self._close(addr, "demoted", why, REJECT_DAYS)
                self.stats["demoted"] += 1
                changed = True
                out.append(f"🗑 убран из smart {esc(short(addr))} — {esc(why)}")
        if changed:
            await self.e._load_traders()
        return out


async def scout_report(db: Any) -> str:
    """/scout — что разведка делает сейчас."""
    cnt = {r["status"]: r["n"] for r in await db.fetch("SELECT status, count(*) n FROM scout GROUP BY 1")}
    names = {"queued": "в очереди", "probation": "на испытании", "promoted": "в своём списке",
             "passed": "прошли (список полон)", "rejected": "отсеяны", "few": "мало сделок", "demoted": "убраны",
             "removed": "убраны вручную", LK.STATUS: "проверка по образцу A"}
    lines = ["🔍 <b>Разведка кошельков</b>",
             " · ".join(f"{names.get(k, k)} {v}" for k, v in sorted(cnt.items(), key=lambda kv: -kv[1])) or "пока пусто"]
    rows = await db.fetch(
        f"""SELECT s.address, s.source, s.started_at, count(p.id) FILTER (WHERE p.closed_at IS NOT NULL) n,
                   count(p.id) FILTER (WHERE p.pnl_sol > 0) wins, COALESCE(sum(p.pnl_sol), 0) pnl,
                   COALESCE(sum(p.cost_sol) FILTER (WHERE p.closed_at IS NOT NULL), 0) cost
            FROM scout s LEFT JOIN paper_positions p ON p.source_wallet = s.address AND p.portfolio = '{TRIAL}'
                 AND p.opened_at >= s.started_at AND NOT p.suspect AND p.exit_reason IS DISTINCT FROM 'reset'
            WHERE s.status = 'probation' GROUP BY 1, 2, 3 ORDER BY pnl DESC""")
    if rows:
        lines.append("\n<b>На испытании</b> (копия как у боевого бота — правило A, портфель S, по $10; в smart — "
                     "8+ копий, 65%+ в плюсе и +5% в среднем, или 10+ копий и +8% в среднем при любом винрейте; "
                     "за сутки меньше $10 — место следующему)")
        now = datetime.now(timezone.utc)
        for r in rows:
            res = f"{r['n']} копий, в плюсе {r['wins']}, {r['pnl'] / r['cost']:+.0%}" if r["cost"] else "копий ещё нет"
            if r["started_at"]:
                res += f" · за {(now - r['started_at']).total_seconds() / 3600:.0f} ч"
            lines.append(f"• {esc(short(r['address']))} ({esc(r['source'] or '')}) · {res}")
    done = await db.fetch(
        """SELECT address, status, decision FROM scout WHERE decided_at > now() - interval '2 days'
           AND status IN ('promoted', 'passed', 'demoted', 'rejected') ORDER BY decided_at DESC LIMIT 15""")
    if done:
        lines.append("\n<b>Решения за 2 дня</b>")
        icon = {"promoted": "✅", "passed": "☑️", "demoted": "🗑", "rejected": "❌"}
        for r in done:
            lines.append(f"{icon.get(r['status'], '•')} {esc(short(r['address']))} — {esc(r['decision'] or '')}")
    try:
        lines += await FAM.report_lines(db)
        lines += await DS.report_lines(db)
        lines += await LK.report_lines(db)
    except Exception:  # noqa: BLE001
        log.exception("разведка: сводка семьи и поиска по образцу")
    lines.append("<i>Файл по всем кошелькам — /wallets, всё за день и за 30 дней одним архивом — /export</i>")
    return "\n".join(lines)
