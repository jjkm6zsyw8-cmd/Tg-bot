"""Защита копитрейда (раздел «Копитрейд»): анти-PVP, дельта цены, журнал, пауза на конвейер девов.

* Анти-PVP: кошелёк K раз из последних 5 сделок продал быстрее N секунд после покупки → копирование его покупок
  на паузе (по умолчанию 2 часа), уведомление о паузе и о снятии. За таким скальпером мы — выходная ликвидность.
* Дельта цены: насколько дороже цели мы входим (MC нашего входа / MC сразу после его покупки − 1). Средняя по
  последним 20 копиям больше порога (15%) при 5+ замерах → кошелёк не копируем: между нами влезают боты.
* Журнал copy_log: каждая покупка/продажа своих кошельков — «скопировал» или «пропустил» с причиной (/copylog).
* Конвейер: дев из своего списка запустил следующую монету быстрее N минут после прошлой → пропуск.
* Вход дороже цели: наша цена входа выше MC сразу после его покупки больше чем на 15% → эту копию пропускаем
  (бумага не входит): мы покупаем после ботов, на вершине его же закупа.
"""
from __future__ import annotations

import logging
import time
from collections import deque
from typing import TYPE_CHECKING, Any

from ..alerts import format as fmt

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger(__name__)

DEFAULTS: dict[str, float] = {
    "copy_ratio": 0.10,            # позиция копии = 10% от суммы покупки цели (потолок — доля портфеля)
    "copy_min_buy_sol": 0.1,       # покупки цели мельче — не копируем
    "antipvp_hold_s": 20,          # «слишком быстро»: продал раньше чем через N секунд после покупки
    "antipvp_count": 3,            # сколько таких из последних 5 — пауза
    "antipvp_pause_min": 120,      # длительность паузы
    "copy_max_delta": 0.15,        # средняя дельта входа больше 15% — не копируем
    "copy_delta_min_n": 5,         # замеров для решения
    "copy_entry_max_delta": 0.15,  # эта копия: цена уже выше его покупки больше чем на 15% — не входим
    "copy_dev_min_gap_min": 3,     # дев из списка запускает чаще — конвейер, пропуск
}


def q(cfg: dict[str, Any], key: str) -> float:
    v = cfg.get(key)
    return float(v) if v is not None else DEFAULTS[key]


class CopyGuard:
    def __init__(self, engine: "Engine"):
        self.e = engine
        self.db = engine.db
        self.buys: dict[tuple[str, str], float] = {}          # (кошелёк, монета) → время его первой покупки
        self.holds: dict[str, deque[float]] = {}              # кошелёк → удержание последних 5 сделок, с
        self.paused: dict[str, float] = {}                    # кошелёк → до какого unix-времени пауза
        self.deltas: dict[str, deque[float]] = {}             # кошелёк → дельты входа последних 20 копий
        self.dev_last: dict[str, float] = {}                  # дев → время прошлого запуска

    @property
    def cfg(self) -> dict[str, Any]:
        return self.e.cfg

    async def load(self) -> None:
        saved = await self.db.kv_get("copy:paused", {}) or {}
        now = time.time()
        self.paused = {w: float(t) for w, t in saved.items() if float(t) > now}
        rows = await self.db.fetch(
            """SELECT wallet, delta FROM (SELECT wallet, delta, row_number() OVER (PARTITION BY wallet ORDER BY id DESC) n
               FROM copy_log WHERE delta IS NOT NULL) x WHERE n <= 20 ORDER BY wallet, n DESC""")
        self.deltas = {}
        for r in rows:
            self.deltas.setdefault(r["wallet"], deque(maxlen=20)).append(r["delta"])

    def label(self, wallet: str) -> str:
        t = self.e.traders.get(wallet) or self.e.my_devs.get(wallet) or {}
        return t.get("label") or f"{wallet[:4]}…{wallet[-4:]}"

    # ------------------------------------------------------------------ анти-PVP
    def on_buy(self, wallet: str, mint: str, ts: float) -> None:
        self.buys.setdefault((wallet, mint), ts)
        if len(self.buys) > 50_000:
            cut = time.time() - 86400
            self.buys = {k: v for k, v in self.buys.items() if v > cut}

    async def on_sell(self, wallet: str, mint: str, ts: float) -> float | None:
        """Удержание этой сделки (с) или None. Слишком быстрые продажи подряд — пауза копирования."""
        bought = self.buys.pop((wallet, mint), None)
        if bought is None:
            return None
        hold = max(0.0, ts - bought)
        hs = self.holds.setdefault(wallet, deque(maxlen=5))
        hs.append(hold)
        fast = sum(1 for h in hs if h < q(self.cfg, "antipvp_hold_s"))
        if fast >= q(self.cfg, "antipvp_count") and wallet not in self.paused:
            minutes = q(self.cfg, "antipvp_pause_min")
            self.paused[wallet] = time.time() + 60 * minutes
            await self._save_paused()
            await self._notify(f"⏸ <b>Анти-PVP: {fmt.esc(self.label(wallet))}</b> — {fast} из {len(hs)} последних сделок "
                               f"продал быстрее {q(self.cfg, 'antipvp_hold_s'):g} с после покупки. Не копирую "
                               f"{minutes:g} мин: за таким скальпером мы — выходная ликвидность.")
        return hold

    def pause_reason(self, wallet: str) -> str | None:
        until = self.paused.get(wallet)
        if until and until > time.time():
            return f"анти-PVP: продаёт быстрее {q(self.cfg, 'antipvp_hold_s'):g} с (пауза ещё {fmt.ago(until - time.time())})"
        return None

    async def tick(self) -> None:
        now = time.time()
        for w in [w for w, t in self.paused.items() if t <= now]:
            self.paused.pop(w, None)
            self.holds.pop(w, None)
            await self._notify(f"▶️ Анти-PVP: пауза для <b>{fmt.esc(self.label(w))}</b> снята — снова копирую.")
            await self._save_paused()

    async def _save_paused(self) -> None:
        await self.db.kv_set("copy:paused", {w: t for w, t in self.paused.items()} or None)

    # ------------------------------------------------------------------ дельта цены
    def delta_reason(self, wallet: str) -> str | None:
        ds = self.deltas.get(wallet)
        if not ds or len(ds) < q(self.cfg, "copy_delta_min_n"):
            return None
        avg = sum(ds) / len(ds)
        if avg > q(self.cfg, "copy_max_delta"):
            return f"дельта входа в среднем {avg:+.0%} по {len(ds)} копиям — между нами влезают боты"
        return None

    async def record_delta(self, wallet: str, mint: str, ref_mc: float, entry_mc: float) -> float | None:
        if not ref_mc or ref_mc <= 0 or not entry_mc:
            return None
        delta = entry_mc / ref_mc - 1
        self.deltas.setdefault(wallet, deque(maxlen=20)).append(delta)
        await self.db.execute(
            """UPDATE copy_log SET entry_mc=$3, delta=$4 WHERE id = (SELECT id FROM copy_log WHERE wallet=$1 AND mint=$2
               AND action='copied' ORDER BY id DESC LIMIT 1)""", wallet, mint, entry_mc, delta)
        return delta

    def entry_reason(self, delta: float | None) -> str | None:
        """Эта копия: цена входа уже выше его покупки больше порога — пропуск (0 или 100% — без проверки)."""
        limit = q(self.cfg, "copy_entry_max_delta")
        if delta is None or limit <= 0 or limit >= 1 or delta <= limit:
            return None
        return f"цена уже {delta:+.0%} от его покупки (порог +{limit:.0%})"

    async def mark_skipped(self, wallet: str, mint: str, reason: str) -> None:
        """Алерт ушёл, но бумага не вошла — в журнале копия становится «пропущено» с причиной."""
        await self.db.execute(
            """UPDATE copy_log SET action='skipped', reason=$3 WHERE id = (SELECT id FROM copy_log WHERE wallet=$1
               AND mint=$2 AND action='copied' ORDER BY id DESC LIMIT 1)""", wallet, mint, reason)

    # ------------------------------------------------------------------ конвейер девов
    def dev_gap_reason(self, dev: str, now: float | None = None) -> str | None:
        now = now or time.time()
        last = self.dev_last.get(dev)
        self.dev_last[dev] = now
        gap = q(self.cfg, "copy_dev_min_gap_min")
        if last is not None and gap > 0 and now - last < 60 * gap:
            return f"конвейер: прошлый запуск {fmt.ago(now - last)} назад (порог {gap:g} мин)"
        return None

    # ------------------------------------------------------------------ журнал
    async def log(self, wallet: str, mint: str, symbol: str | None, side: str, sol: float | None, mc: float | None,
                  action: str, reason: str | None = None, ref_mc: float | None = None) -> None:
        try:
            await self.db.execute(
                """INSERT INTO copy_log(wallet, mint, symbol, side, sol, mc, action, reason, ref_mc)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                wallet, mint, (symbol or "")[:50], side, sol, mc, action, reason, ref_mc)
        except Exception:  # noqa: BLE001 — журнал не должен ломать копирование
            log.exception("copy_log")

    async def _notify(self, text: str) -> None:
        await self.db.enqueue("text", {"topic": "copy", "text": text}, priority=4)
