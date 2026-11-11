"""Разведка: бот сам ищет кошельки для копирования, проверяет их на испытательном сроке и чистит список.

Так же, как это делается руками (GMGN / Axiom → «Top traders» монеты → история кошелька → в список):

1. Кандидаты (раз в 15 минут):
   • свои данные: кошелёк в топ-10 по прибыли на 2+ разных монетах за 3 дня (coin_top_traders — по каждой монете,
     которую бот вёл от запуска; кластер дева, бандлы и снайперы туда не попадают);
   • GMGN (если задан GMGN_API_KEY): монеты в тренде за час → их лучшие трейдеры по реализованной прибыли.
   Сразу отсеиваются: биржи и пулы, подозрительные, снайперы, бандлеры, инсайдеры, девы; кто ещё держит больше
   половины купленного (не трейдер, а холдер); уже в своём списке; отсеянные недавно. С GMGN — ещё статистика кошелька
   за 7 дней: винрейт, прибыль, сколько монет, среднее удержание (бот-снайпер держит секунды), доля сливов хуже −50%,
   запускает ли сам монеты.
2. Испытательный срок (до 4 дней, до 20 кошельков одновременно): бот подписывается на кошелёк и копирует каждую его
   покупку в бумажный портфель H по $10 — с той же задержкой, рыночной ценой и выходами, что настоящие копии (как G).
   Это главный фильтр: красивый PnL на GMGN не значит, что его можно повторить с задержкой в 2 секунды.
3. Решение: 6+ закрытых копий, итог в плюсе и прибыльных ≥ 40% → в свой список (копируется как свои кошельки,
   метка «🔍»). Иначе — отсев на 14 дней. 4 копии подряд в минусе — отсев сразу. 4 дня и меньше 6 копий — «мало
   сделок», отсев на 7 дней.
4. Чистка: кошелёк разведки в своём списке — последние 10 его копий в G в сумме в минусе → убирается (14 дней).
   Кошельки, добавленные вручную, бот не трогает: в /wallets и в сводке они помечаются «шлак — убрать».

Всё, что разведка нашла и решила, лежит в таблице scout; файл — /wallets, сводка — /scout.
"""
from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ..alerts.format import esc
from ..external.base import Unavailable
from .copytrade import linked_to_dev
from .paper import SCOUT

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger(__name__)

EVERY_S = 900
DEFAULTS: dict[str, Any] = {
    "scout_enabled": True,
    "scout_slots": 20,                 # кошельков на испытании одновременно
    "scout_max_auto": 15,              # сколько кошельков разведки держать в своём списке (0 — не добавлять)
    "scout_min_copies": 6,             # копий на испытании для решения
    "scout_probation_days": 4,
    "scout_min_winrate": 0.40,         # доля прибыльных копий
    "scout_min_profit_usd": 300,       # GMGN: прибыль на монете, $
    "scout_gmgn_min_winrate": 0.45,    # GMGN 7 дней
    "scout_gmgn_min_tokens": 10,
    "scout_gmgn_min_hold_s": 30,
    "scout_gmgn_max_big_losses": 0.35,
}
BAD_TAGS = {"sniper", "bundler", "rat_trader", "dev", "dex_bot", "insider", "creator", "sandwich_bot"}
REJECT_DAYS = 14
FEW_TRADES_DAYS = 7


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
    if age_days >= float(cfg_get(cfg, "scout_probation_days")):
        return "few", res + f" за {age_days:.0f} дн. — мало сделок"
    return None, res


def short(addr: str) -> str:
    return f"{addr[:4]}…{addr[-4:]}"


class Scout:
    def __init__(self, engine: "Engine"):
        self.e = engine
        self.db = engine.db
        self.stats = {"found": 0, "queued": 0, "started": 0, "promoted": 0, "rejected": 0, "demoted": 0,
                      "gmgn_errors": 0, "runs": 0}
        self._token_cursor = 0

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
        await self.discover_gmgn()
        events += await self.decide()
        events += await self.demote()
        events += await self.fill_slots()
        await self.load()
        if events:
            await self.db.system("🔍 <b>Разведка</b>\n" + "\n".join(events[:25]))
        return events

    async def load(self) -> None:
        """Кошельки на испытании — движку (копии в H) и приёму (подписка на их сделки)."""
        self.e.scouts = {r["address"] for r in await self.db.fetch(
            "SELECT address FROM scout WHERE status='probation'")}

    # ------------------------------------------------------------------ кандидаты
    async def _known(self) -> set[str]:
        rows = await self.db.fetch(
            """SELECT address FROM watchlist WHERE kind IN ('trader', 'dev')
               UNION SELECT address FROM traders WHERE manual OR pinned
               UNION SELECT address FROM scout WHERE status IN ('probation', 'promoted', 'passed')
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
            if r["trader"] in known or (r["hold"] is not None and r["hold"] < 20):
                continue
            added += await self._queue(
                r["trader"], "свои данные", f"в топе по прибыли на {r['n']} монетах за 3 дня ({r['pnl']:+.2f} SOL)",
                score_of(None, int(r["n"]), float(r["pnl"] or 0) * sol_usd), None)
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
        if not hot:
            return 0
        i = self._token_cursor % len(hot)
        batch = (hot[i:] + hot[:i])[:tokens_per_run]
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
            "SELECT * FROM scout WHERE status='queued' ORDER BY score DESC NULLS LAST, found_at LIMIT $1", free * 3)
        started = []
        for r in rows:
            if len(started) >= free:
                break
            addr = r["address"]
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
                       COALESCE(sum(p.pnl_sol), 0) pnl, COALESCE(sum(p.cost_sol), 0) cost
                FROM paper_positions p JOIN scout s ON s.address = p.source_wallet
                WHERE p.portfolio = $1 AND p.source_wallet = ANY($2::text[]) AND p.closed_at IS NOT NULL
                  AND p.exit_reason <> 'reset' AND NOT p.suspect AND p.opened_at >= s.{since_col}
                GROUP BY 1""", portfolio, addrs)
        return {r["w"]: dict(r) for r in rows}

    async def decide(self) -> list[str]:
        rows = await self.db.fetch("SELECT * FROM scout WHERE status='probation'")
        res = await self.results(SCOUT, [r["address"] for r in rows], "started_at")
        now = datetime.now(timezone.utc)
        auto = int(await self.db.fetchval("SELECT count(*) FROM scout WHERE status='promoted'") or 0)
        cap = int(cfg_get(self.cfg, "scout_max_auto"))
        out: list[str] = []
        for r in rows:
            x = res.get(r["address"], {"n": 0, "wins": 0, "pnl": 0.0, "cost": 0.0})
            age = (now - r["started_at"]).total_seconds() / 86400 if r["started_at"] else 0.0
            v, why = verdict(int(x["n"]), int(x["wins"]), float(x["pnl"]), float(x["cost"]), age, self.cfg)
            addr = r["address"]
            if v == "promote" and auto < cap:
                await self._promote(addr)
                await self._close(addr, "promoted", why, 0)
                auto += 1
                self.stats["promoted"] += 1
                out.append(f"✅ в smart: <code>{addr}</code> — {esc(why)}")
            elif v == "promote":
                await self._close(addr, "passed", why + " — свой список разведки полон", 0)
                out.append(f"☑️ прошёл, но список полон ({cap}): <code>{addr}</code> — {esc(why)}")
            elif v in ("reject", "few"):
                await self._close(addr, "rejected" if v == "reject" else "few", why,
                                  REJECT_DAYS if v == "reject" else FEW_TRADES_DAYS)
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
                """SELECT pnl_sol FROM paper_positions p JOIN scout s ON s.address = p.source_wallet
                   WHERE p.portfolio='G' AND p.source_wallet=$1 AND p.closed_at IS NOT NULL
                     AND p.exit_reason <> 'reset' AND NOT p.suspect AND p.opened_at >= s.decided_at
                   ORDER BY p.closed_at DESC LIMIT 10""", addr)
            pnl = sum(float(x["pnl_sol"] or 0) for x in last)
            if len(last) >= 8 and pnl < 0:
                await self.db.execute("UPDATE traders SET active=false, manual=false, pinned=false WHERE address=$1",
                                      addr)
                await self._close(addr, "demoted", f"последние {len(last)} копий в G: {pnl:+.3f} SOL", REJECT_DAYS)
                self.stats["demoted"] += 1
                changed = True
                out.append(f"🗑 убран из списка {esc(short(addr))} — последние {len(last)} копий {pnl:+.3f} SOL")
        if changed:
            await self.e._load_traders()
        return out


async def scout_report(db: Any) -> str:
    """/scout — что разведка делает сейчас."""
    cnt = {r["status"]: r["n"] for r in await db.fetch("SELECT status, count(*) n FROM scout GROUP BY 1")}
    names = {"queued": "в очереди", "probation": "на испытании", "promoted": "в своём списке",
             "passed": "прошли (список полон)", "rejected": "отсеяны", "few": "мало сделок", "demoted": "убраны",
             "removed": "убраны вручную"}
    lines = ["🔍 <b>Разведка кошельков</b>",
             " · ".join(f"{names.get(k, k)} {v}" for k, v in sorted(cnt.items(), key=lambda kv: -kv[1])) or "пока пусто"]
    rows = await db.fetch(
        f"""SELECT s.address, s.source, s.started_at, count(p.id) FILTER (WHERE p.closed_at IS NOT NULL) n,
                   count(p.id) FILTER (WHERE p.pnl_sol > 0) wins, COALESCE(sum(p.pnl_sol), 0) pnl,
                   COALESCE(sum(p.cost_sol) FILTER (WHERE p.closed_at IS NOT NULL), 0) cost
            FROM scout s LEFT JOIN paper_positions p ON p.source_wallet = s.address AND p.portfolio = '{SCOUT}'
                 AND p.opened_at >= s.started_at AND NOT p.suspect AND p.exit_reason IS DISTINCT FROM 'reset'
            WHERE s.status = 'probation' GROUP BY 1, 2, 3 ORDER BY pnl DESC""")
    if rows:
        lines.append("\n<b>На испытании</b> (копии в H по $10)")
        for r in rows:
            res = f"{r['n']} копий, в плюсе {r['wins']}, {r['pnl'] / r['cost']:+.0%}" if r["cost"] else "копий ещё нет"
            lines.append(f"• {esc(short(r['address']))} ({esc(r['source'] or '')}) · {res}")
    done = await db.fetch(
        """SELECT address, status, decision FROM scout WHERE decided_at > now() - interval '2 days'
           AND status IN ('promoted', 'passed', 'demoted', 'rejected') ORDER BY decided_at DESC LIMIT 15""")
    if done:
        lines.append("\n<b>Решения за 2 дня</b>")
        icon = {"promoted": "✅", "passed": "☑️", "demoted": "🗑", "rejected": "❌"}
        for r in done:
            lines.append(f"{icon.get(r['status'], '•')} {esc(short(r['address']))} — {esc(r['decision'] or '')}")
    lines.append("<i>Файл по всем кошелькам — /wallets, всё за день и за 30 дней одним архивом — /export</i>")
    return "\n".join(lines)
