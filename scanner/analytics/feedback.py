"""Обратная связь и обучение (разд. 6 ТЗ).

* отметки «Очень успешно / Успешно / Провал / Скам»;
* поиск связанных кошельков с подтверждением пользователем;
* эталонные запуски;
* слежение за выводом SOL приоритетных кластеров (2–3 перехода, обрыв на бирже).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from ..alerts import format as fmt
from ..ingest.rpc import RpcError
from ..solana import short
from . import clusters as cl
from .filters import which_would_block
from .loader import cluster_profile, save_profile

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger(__name__)


class Feedback:
    def __init__(self, engine: "Engine"):
        self.e = engine
        self.db = engine.db

    async def handle(self, kind: str, p: dict[str, Any]) -> None:
        if kind == "mark":
            await self.on_mark(int(p["alert_id"]), p["mark"])
        elif kind == "watch":
            await self.e.watch_token(p["mint"])
        elif kind == "recompute_cluster":
            prof, _ = await cluster_profile(self.db, int(p["cluster_id"]), self.e.cfg)
            await save_profile(self.db, int(p["cluster_id"]), prof, self.e.cfg)
        elif kind == "paper":
            cmd = p.get("cmd", "")
            from .paper import PERIOD_KEYBOARD, SCHEMES
            if cmd == "trades":
                parts = await self.e.paper.trades_text(float(p.get("hours") or 24))
                for i, part in enumerate(parts):
                    last = i == len(parts) - 1
                    await self.db.enqueue("text", {"topic": "paper", "text": part,
                                                   "keyboard": PERIOD_KEYBOARD if last else None}, priority=4)
                return
            if cmd == "csv":
                data = await self.e.paper.trades_csv(p.get("hours"))
                n = max(0, data.count("\n") - 1)
                await self.db.enqueue("file", {"topic": "paper", "csv": data, "filename": "paper_trades.csv",
                                               "caption": f"📄 Все сделки с последнего сброса: {n}. "
                                                          "Открывается в Numbers / Excel."}, priority=4)
                return
            if cmd == "reset":
                await self.e.paper.reset()
                text = f"📒 Бумажная торговля сброшена: {len(SCHEMES)} портфелей по $30."
            elif cmd == "history":
                text = await self.e.paper.history(20)
            elif cmd == "period":
                h = p.get("hours")
                text = await self.e.paper.render_period(float(h) if h else None)
            else:
                text = await self.e.paper.render()
            await self.db.enqueue("text", {"topic": "paper", "text": text, "keyboard": PERIOD_KEYBOARD}, priority=4)
        elif kind == "reload":
            await self.e._load_traders()
            await self.e._load_references()

    async def on_mark(self, alert_id: int, mark: str) -> None:
        a = await self.db.fetchrow("SELECT * FROM alerts WHERE id=$1", alert_id)
        if not a:
            return
        cid = a["cluster_id"]
        if mark in ("great", "good"):
            await self.db.execute("UPDATE launches SET is_reference=true WHERE mint=$1", a["mint"])
            if cid:
                await self.db.execute("UPDATE clusters SET priority=true WHERE id=$1", cid)
                for w, _r, _c in await cl.cluster_wallets(self.db, cid):
                    await self.db.execute(
                        "INSERT INTO withdrawal_watch(wallet, cluster_id, hop) VALUES ($1,$2,0) "
                        "ON CONFLICT (wallet) DO NOTHING", w, cid)
                await self.db.enqueue("text", {"topic": "dev", "text":
                    f"⭐ Кластер #{cid} получил приоритет. Слежу за выводом SOL и ищу связанные кошельки.",
                    "keyboard": [[{"text": "Включить пониженный порог", "callback": f"lt:{cid}"}]]})
                await self.discover_links(cid, a["mint"])
            if a["trader"]:
                await self.db.execute("UPDATE traders SET priority=true, active=true, manual=true WHERE address=$1",
                                      a["trader"])
            await self.e._load_references()
        else:
            if cid:
                await self.db.execute(
                    "UPDATE clusters SET pattern_weight = GREATEST(0.3, pattern_weight * 0.8) WHERE id=$1", cid)
                await self.db.execute("UPDATE launches SET weight=0.5 WHERE mint=$1", a["mint"])
            metrics = (a["payload"] or {}).get("metrics") or {}
            blockers = which_would_block(metrics, self.e.settings.yaml.section("presets"),
                                         "smart" if a["mode"] == "smart" else "dev")
            txt = (f"📉 Отметка «{'Скам' if mark == 'scam' else 'Провал'}» · <code>{a['mint']}</code>\n"
                   "Вес похожего паттерна снижен.\n")
            txt += ("Отсеял бы фильтр:\n" + "\n".join(fmt.esc(b) for b in blockers)) if blockers else \
                "Ни один пресет фильтров не отсеял бы эту монету — нужен новый признак или порог."
            if mark == "scam" and cid:
                await self.db.execute(
                    "UPDATE clusters SET blacklisted=true, blacklist_reason='Скам по отметке' WHERE id=$1", cid)
                await self.db.execute(
                    """UPDATE wallets SET blacklisted=true WHERE address IN
                       (SELECT wallet FROM cluster_links WHERE cluster_id=$1 AND status IN ('auto','confirmed'))""",
                    cid)
                txt += f"\n⛔ Кластер #{cid} в чёрном списке."
            await self.db.enqueue("text", {"topic": "dev" if a["mode"] != "smart" else "smart", "text": txt})
        if cid:
            prof, _ = await cluster_profile(self.db, cid, self.e.cfg)
            await save_profile(self.db, cid, prof, self.e.cfg)

    # ------------------------------------------------------------------ поиск связей
    async def discover_links(self, cid: int, mint: str, limit: int = 10) -> int:
        tok = await self.db.fetchrow("SELECT * FROM tokens WHERE mint=$1", mint)
        if not tok:
            return 0
        linker = self.e.linker
        members = {w for w, _r, _c in await cl.cluster_wallets(self.db, cid)}
        rejected = {r["wallet"] for r in await self.db.fetch(
            "SELECT wallet FROM cluster_links WHERE cluster_id=$1 AND status='rejected'", cid)}
        cands: dict[str, tuple[str, str]] = {}

        def add(w: str, reason: str, conf: str) -> None:
            if w and w not in members and w not in rejected and w not in cands:
                cands[w] = (reason, conf)

        created = tok["created_at"]
        # 1) кто финансировал деплоер и бандлы
        first_slot = [r["trader"] for r in await self.db.fetch(
            "SELECT DISTINCT trader FROM trades WHERE mint=$1 AND is_buy AND slot=$2 LIMIT 10",
            mint, tok["created_slot"])] if tok["created_slot"] else []
        for w in [tok["deployer"], *first_slot]:
            for t in await linker.transfers_around(w, created - timedelta(hours=24), created, max_tx=8):
                if t.dst == w and t.src != w and not await linker.is_exchange(t.src):
                    add(t.src, f"финансировал {'деплоер' if w == tok['deployer'] else 'бандл-кошелёк'} "
                               f"({t.sol:.2f} SOL)", "high")
        # 2) куда ушли SOL после дампа (2–3 перехода)
        hops = int(self.e.settings.reliability().get("withdrawal_hops", 3))
        frontier = [tok["deployer"]]
        seen = set(frontier)
        for hop in range(1, hops + 1):
            nxt = []
            for w in frontier[:5]:
                for t in await linker.transfers_around(w, created, created + timedelta(hours=12), max_tx=10):
                    if t.src != w or t.sol < 0.3 or t.dst in seen:
                        continue
                    seen.add(t.dst)
                    if await linker.is_exchange(t.dst):
                        continue  # биржа: цепочка обрывается, клиентов биржи не склеиваем
                    add(t.dst, f"получил {t.sol:.2f} SOL после дампа ({hop} переход)", "high" if hop == 1 else "medium")
                    nxt.append(t.dst)
            frontier = nxt
        # 3) кто покупал в тех же бандлах в других запусках кластера
        rows = await self.db.fetch(
            """SELECT t.trader, count(DISTINCT t.mint) n FROM trades t JOIN tokens k ON k.mint=t.mint
               WHERE k.cluster_id=$1 AND t.is_buy AND t.slot = k.created_slot GROUP BY t.trader HAVING count(DISTINCT t.mint) >= 2
               LIMIT 20""", cid)
        for r in rows:
            add(r["trader"], f"покупал в слоте создания в {r['n']} запусках кластера", "high")
        # 4) повтор метаданных у других деплоеров
        for col in ("image", "twitter", "website"):
            if tok[col]:
                for r in await self.db.fetch(
                        f"SELECT DISTINCT deployer FROM tokens WHERE {col}=$1 AND deployer <> $2 LIMIT 5",
                        tok[col], tok["deployer"]):
                    add(r["deployer"], f"повтор метаданных ({col})", "medium")

        sent = 0
        for w, (reason, conf) in list(cands.items())[:limit]:
            lid = await cl.add_link(self.db, cid, w, reason, conf, status="proposed")
            if lid is None:
                continue
            await self.db.enqueue("text", {
                "topic": "dev",
                "text": (f"🔗 <b>Возможная связь с кластером #{cid}</b>\nКошелёк <code>{w}</code>\n"
                         f"Причина: {fmt.esc(reason)}\nУверенность: {fmt.CONF_NAMES[conf]}"),
                "keyboard": [[{"text": "Подтвердить", "callback": f"lk:{lid}:y", "style": "success"},
                              {"text": "Отклонить", "callback": f"lk:{lid}:n", "style": "danger"}]],
            }, priority=6)
            sent += 1
        return sent


class WithdrawalWatcher:
    """Исходящие переводы SOL приоритетных кластеров в реальном времени (опрос RPC)."""

    def __init__(self, engine: "Engine"):
        self.e = engine
        self.db = engine.db

    async def run(self) -> None:
        while True:
            rel = self.e.settings.reliability()
            await asyncio.sleep(float(rel.get("withdrawal_poll_s", 15)))
            try:
                await self.tick(int(rel.get("withdrawal_hops", 3)))
            except Exception:  # noqa: BLE001
                log.exception("withdrawal watcher")

    async def tick(self, max_hops: int) -> None:
        rows = await self.db.fetch(
            """SELECT w.* FROM withdrawal_watch w JOIN clusters c ON c.id = w.cluster_id
               WHERE w.stopped IS NULL AND c.priority AND NOT c.blacklisted ORDER BY w.created_at DESC LIMIT 300""")
        rpc = self.e.rpc
        for r in rows:
            try:
                sigs = await rpc.signatures_for(r["wallet"], limit=25, until=r["last_sig"])
            except RpcError:
                continue
            if not sigs:
                continue
            await self.db.execute("UPDATE withdrawal_watch SET last_sig=$2 WHERE wallet=$1", r["wallet"],
                                  sigs[0]["signature"])
            if r["last_sig"] is None:
                continue  # первая проверка — только запоминаем точку отсчёта
            for s in reversed(sigs):
                if s.get("err"):
                    continue
                try:
                    tx = await rpc.transaction(s["signature"])
                except RpcError:
                    continue
                if not tx:
                    continue
                from ..ingest.pump_parser import TxContext, sol_transfers
                for t in sol_transfers(tx, TxContext(s["signature"], s.get("slot"), s.get("blockTime"), None, "rpc")):
                    if t.src != r["wallet"] or t.sol < 0.1:
                        continue
                    exch = await self.e.linker.is_exchange(t.dst)
                    stopped = "exchange" if exch else ("max_hops" if r["hop"] + 1 > max_hops else None)
                    added = await self.db.fetchval(
                        """INSERT INTO withdrawal_watch(wallet, cluster_id, hop, parent, stopped)
                           VALUES ($1,$2,$3,$4,$5) ON CONFLICT (wallet) DO NOTHING RETURNING wallet""",
                        t.dst, r["cluster_id"], r["hop"] + 1, r["wallet"], stopped)
                    if added and exch:
                        await self.db.enqueue("text", {"topic": "system", "text":
                            f"↗️ Кластер #{r['cluster_id']} → биржа {t.sol:.2f} SOL ({short(t.dst)})"}, priority=7)
            await asyncio.sleep(0.05)

    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)
