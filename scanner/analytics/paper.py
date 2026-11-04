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
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from ..alerts import format as fmt
from .curve import buy_tokens_out, reserves_for_mc, sell_sol_out

if TYPE_CHECKING:
    from .engine import Engine
    from .state import TokenState

log = logging.getLogger(__name__)

START_USD = 30.0
POSITION_SHARE = 0.20
MAX_OPEN = 3
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
}
EXIT_NAMES = {"emergency": "экстренный выход (продажа кластера дева)", "trader_sell": "трейдер-источник продал",
              "stop": "стоп-лосс", "take_profit": "тейк-профит 50%", "target": "цель", "trailing": "трейлинг-стоп",
              "time_stop": "стоп по времени (нет +10% за 10 мин)", "max_hold": "максимальное удержание"}


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
    if scheme.get("trail") is not None and pos.tp_done and mc <= pos.max_mc * (1 - scheme["trail"]):
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

    @property
    def cfg(self) -> dict[str, Any]:
        return self.e.cfg

    def fee_bps(self) -> float:
        return float(self.e.fee_bps or self.e.settings.costs().get("curve_fee_bps", 125))

    def priority(self) -> float:
        return float(self.cfg.get("paper_priority_sol", 0.0005))

    def levels(self) -> set[str]:
        return set(self.cfg.get("paper_levels") or ["strong", "normal"])

    async def start(self) -> None:
        sol = self.e.price.value or 150
        for name, scheme in SCHEMES.items():
            await self.db.execute(
                """INSERT INTO paper_portfolios(name, scheme, start_usd, start_sol, balance_sol)
                   VALUES ($1, $2, $3, $4, $4) ON CONFLICT (name) DO NOTHING""",
                name, scheme, START_USD, START_USD / sol)
        for r in await self.db.fetch("SELECT * FROM paper_portfolios"):
            self.balance[r["name"]] = r["balance_sol"]
            self.paused[r["name"]] = r["paused"]
        for r in await self.db.fetch("SELECT * FROM paper_positions WHERE closed_at IS NULL"):
            self.positions[r["id"]] = Position(
                id=r["id"], portfolio=r["portfolio"], mint=r["mint"], opened_ts=r["opened_at"].timestamp(),
                entry_mc=r["entry_mc"], cost_sol=r["cost_sol"], tokens=r["tokens"], tokens_initial=r["tokens_initial"],
                target_mc=r["target_mc"], symbol=r["symbol"] or "", proceeds_sol=r["proceeds_sol"],
                max_mc=r["max_mc"] or r["entry_mc"], tp_done=r["tp_done"], level=r["level"] or "normal",
                alert_id=r["alert_id"])

    async def reset(self) -> None:
        async with self.lock:
            sol = self.e.price.value or 150
            await self.db.execute("UPDATE paper_positions SET closed_at=now(), exit_reason='reset' WHERE closed_at IS NULL")
            await self.db.execute("UPDATE paper_portfolios SET start_usd=$1, start_sol=$2, balance_sol=$2, paused=false, "
                                  "reset_at=now()", START_USD, START_USD / sol)
            self.positions.clear()
            for n in SCHEMES:
                self.balance[n] = START_USD / sol
                self.paused[n] = False
            await self.db.kv_set("paper:compared", None)

    # ------------------------------------------------------------------ вход
    async def on_alert(self, mint: str, level: str, alert_id: int | None, data: dict[str, Any],
                       trader: str | None) -> None:
        if level not in self.levels():
            self.skipped.setdefault("*", f"уровень «{level}» не выбран для бумажной торговли")
            return
        await asyncio.sleep(ENTRY_DELAY_S)          # реакция пользователя
        st = self.e.states.get(mint)
        mc = st.mc if st is not None else None
        if mc is None and getattr(self.e, "ext", None) is not None:
            pair = await self.e.ext.dex_pair(mint, timeout=2.0)       # цена через 5 с после алерта
            if pair and pair.get("market_cap_usd"):
                mc = pair["market_cap_usd"] / (self.e.price.value or 150)
        mc = mc or data.get("mc")
        if not mc or mc <= 0:
            return
        async with self.lock:
            for name in SCHEMES:
                await self._open(name, mint, level, alert_id, data, trader, mc)

    async def _open(self, name: str, mint: str, level: str, alert_id: int | None, data: dict[str, Any],
                    trader: str | None, mc: float) -> None:
        open_here = [p for p in self.positions.values() if p.portfolio == name]
        if any(p.mint == mint for p in open_here):
            self.skipped[name] = "уже есть позиция по этой монете"
            return
        if len(open_here) >= MAX_OPEN:
            self.skipped[name] = f"лимит {MAX_OPEN} открытых позиций"
            return
        size = self.balance[name] * POSITION_SHARE
        min_size = max(0.01, 10 * self.priority())
        if size < min_size:
            if not self.paused.get(name):
                self.paused[name] = True
                await self.db.execute("UPDATE paper_portfolios SET paused=true WHERE name=$1", name)
                await self._msg(f"⏸ Портфель {name}: баланс {self.balance[name]:.4f} SOL — меньше минимальной позиции "
                                f"({min_size / POSITION_SHARE:.3f} SOL). Торговля на паузе, /paper reset — сброс.")
            self.skipped[name] = "пауза: мал баланс"
            return
        fill = buy_fill(mc, size, self.fee_bps(), self.priority())
        if fill.tokens <= 0:
            return
        self.balance[name] -= size
        target = data.get("target_mc")
        now = datetime.now(timezone.utc)
        pid = await self.db.fetchval(
            """INSERT INTO paper_positions(portfolio, mint, alert_id, level, symbol, opened_at, entry_mc, cost_sol,
                   tokens, tokens_initial, max_mc, target_mc) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$7,$10) RETURNING id""",
            name, mint, alert_id, level, data.get("symbol"), now, mc, size, fill.tokens, target)
        await self.db.execute(
            "INSERT INTO paper_trades(position_id, portfolio, side, ts, mc, sol, tokens, fee_sol, reason) "
            "VALUES ($1,$2,'buy',$3,$4,$5,$6,$7,'алерт')", pid, name, now, mc, size, fill.tokens, fill.fee_sol)
        await self.db.execute("UPDATE paper_portfolios SET balance_sol=$2 WHERE name=$1", name, self.balance[name])
        self.positions[pid] = Position(id=pid, portfolio=name, mint=mint, opened_ts=time.time(), entry_mc=mc,
                                       cost_sol=size, tokens=fill.tokens, tokens_initial=fill.tokens, target_mc=target,
                                       trader=trader, symbol=data.get("symbol") or "", max_mc=mc, level=level,
                                       alert_id=alert_id)
        self.skipped.pop(name, None)
        sol_usd = self.e.price.value or 150
        await self._msg(f"📥 {name} вход ${fmt.esc(data.get('symbol') or '?')}: {size:.4f} SOL (${size * sol_usd:.2f}) при MC "
                        f"{fmt.mc_usd(mc, sol_usd)}, комиссии {fill.fee_sol:.4f} SOL")

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
            await self._check(p, st.mc, reason)

    async def tick_loop(self) -> None:
        last_report = time.time()
        while True:
            await asyncio.sleep(5)
            try:
                for p in list(self.positions.values()):
                    st = self.e.states.get(p.mint)
                    mc = st.mc if st is not None else None
                    if mc is None:
                        pair = await self.e.ext.dex_pair(p.mint, timeout=2.0)   # после миграции — DexScreener
                        if pair and pair.get("market_cap_usd"):
                            mc = pair["market_cap_usd"] / (self.e.price.value or 150)
                    if not mc:
                        # цены нет (монета без сделок и без пары на DexScreener): правила по времени всё равно
                        # должны срабатывать, иначе позиция зависает навсегда и занимает слот
                        mc = float(p.extra.get("last_mc") or p.entry_mc)
                    await self._check(p, mc, None)
                if time.time() - last_report >= 3600:
                    last_report = time.time()
                    await self.report_hourly()
                await self.maybe_compare()
            except Exception:  # noqa: BLE001
                log.exception("paper tick")

    async def _check(self, p: Position, mc: float, emergency: str | None) -> None:
        scheme = SCHEMES[p.portfolio]
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
        async with self.lock:
            await self._sell(p, reason, share, mc)

    async def current_mc(self, p: Position) -> float:
        """Текущий MC: свой поток сделок → DexScreener → последнее наблюдённое значение (не максимум)."""
        st = self.e.states.get(p.mint)
        if st is not None and st.mc > 0:
            return st.mc
        ext = getattr(self.e, "ext", None)
        if ext is not None:
            pair = await ext.dex_pair(p.mint, timeout=2.0)
            if pair and pair.get("market_cap_usd"):
                return pair["market_cap_usd"] / (self.e.price.value or 150)
        return float(p.extra.get("last_mc") or p.entry_mc)

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
            await self._msg(f"📤 {p.portfolio} ${fmt.esc(p.symbol or '?')}: {EXIT_NAMES[reason]} — продано "
                            f"{share:.0%} за {fill.sol:.4f} SOL при MC {fmt.mc_usd(mc, sol_usd)}")
            return
        pnl = p.proceeds_sol - p.cost_sol
        pct = pnl / p.cost_sol if p.cost_sol else 0.0
        await self.db.execute(
            """UPDATE paper_positions SET tokens=0, proceeds_sol=$2, closed_at=$3, exit_reason=$4, pnl_sol=$5,
                   pnl_pct=$6, max_mc=$7 WHERE id=$1""", p.id, p.proceeds_sol, now, reason, pnl, pct, p.max_mc)
        self.positions.pop(p.id, None)
        if self.paused.get(p.portfolio) and self.balance[p.portfolio] * POSITION_SHARE >= max(0.01, 10 * self.priority()):
            self.paused[p.portfolio] = False
            await self.db.execute("UPDATE paper_portfolios SET paused=false WHERE name=$1", p.portfolio)
        await self._msg(f"{'✅' if pnl > 0 else '❌'} {p.portfolio} выход ${fmt.esc(p.symbol or '?')}: {EXIT_NAMES[reason]}, "
                        f"MC {fmt.mc_usd(mc, sol_usd)}, итог {pnl:+.4f} SOL (${pnl * sol_usd:+.2f}, {pct:+.0%})")

    async def _msg(self, text: str) -> None:
        await self.db.enqueue("text", {"topic": "paper", "text": text}, priority=7)

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
            st = self.e.states.get(p.mint)
            mc = st.mc if st is not None else p.max_mc
            v = sell_fill(mc, p.tokens, self.fee_bps(), self.priority()).sol if p.tokens else 0.0
            open_value += v
            open_lines.append((p, (p.proceeds_sol + v) / p.cost_sol - 1 if p.cost_sol else 0.0))
        equity = self.balance.get(name, port["balance_sol"]) + open_value
        best = max(rows, key=lambda r: r["pnl_pct"] or 0, default=None)
        worst = min(rows, key=lambda r: r["pnl_pct"] or 0, default=None)
        return {"port": port, "trades": len(rows), "wins": len(wins), "pnl_sol": sum(r["pnl_sol"] or 0 for r in rows),
                "avg_win": sum(wins) / len(wins) if wins else 0.0, "avg_loss": sum(losses) / len(losses) if losses else 0.0,
                "equity": equity, "open": open_lines, "best": best, "worst": worst}

    async def render(self, hourly: bool = False) -> str:
        sol_usd = self.e.price.value or 150
        lines = ["📒 <b>Бумажная торговля</b>" + (" — отчёт за час" if hourly else "")]
        hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        for name, scheme in SCHEMES.items():
            s = await self.summary(name)
            h = await self.summary(name, hour_ago) if hourly else None
            start = s["port"]["start_sol"]
            total = s["equity"] - start
            lines.append(f"\n<b>{fmt.esc(scheme['name'])}</b>" + (" ⏸ пауза" if self.paused.get(name) else ""))
            lines.append(f"Баланс {s['equity']:.4f} SOL (${s['equity'] * sol_usd:.2f}) · всего {total:+.4f} SOL "
                         f"(${total * sol_usd:+.2f}, {total / start:+.1%})")
            if h is not None:
                if h["trades"]:
                    lines.append(f"За час: {h['pnl_sol']:+.4f} SOL (${h['pnl_sol'] * sol_usd:+.2f}), сделок {h['trades']}")
                else:
                    reason = self.skipped.get(name) or self.skipped.get("*") or "не было алертов нужного уровня"
                    lines.append(f"За час сделок нет: {reason}")
            if s["trades"]:
                lines.append(f"Сделок {s['trades']}, винрейт {s['wins'] / s['trades']:.0%}, средний плюс "
                             f"{s['avg_win']:+.4f}, средний минус {s['avg_loss']:+.4f} SOL")
                if s["best"] is not None:
                    lines.append(f"Лучшая ${fmt.esc(s['best']['symbol'] or '?')} {s['best']['pnl_pct']:+.0%}, худшая "
                                 f"${fmt.esc(s['worst']['symbol'] or '?')} {s['worst']['pnl_pct']:+.0%}")
            for p, net in s["open"]:
                lines.append(f"  открыта ${fmt.esc(p.symbol or '?')}: {net:+.0%}" + (" (тейк 50% взят)" if p.tp_done else ""))
        self.skipped.clear()
        return "\n".join(lines)

    async def report_hourly(self) -> None:
        await self.db.enqueue("text", {"topic": "paper", "text": await self.render(hourly=True)}, priority=8)

    async def history(self, n: int = 20) -> str:
        sol_usd = self.e.price.value or 150
        rows = await self.db.fetch("SELECT * FROM paper_positions WHERE closed_at IS NOT NULL AND exit_reason <> 'reset' "
                                   "ORDER BY closed_at DESC LIMIT $1", n)
        if not rows:
            return "Закрытых бумажных сделок пока нет."
        out = [f"📒 Последние {len(rows)} бумажных сделок"]
        for r in rows:
            out.append(f"{r['portfolio']} ${fmt.esc(r['symbol'] or '?')} {r['closed_at']:%d.%m %H:%M}: "
                       f"{r['pnl_sol']:+.4f} SOL (${r['pnl_sol'] * sol_usd:+.2f}, {r['pnl_pct']:+.0%}) — "
                       f"{EXIT_NAMES.get(r['exit_reason'], r['exit_reason'])}")
        return "\n".join(out)

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
