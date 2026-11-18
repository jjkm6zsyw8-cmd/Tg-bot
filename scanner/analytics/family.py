"""Семья лучших кошельков: копилки, обменники и другие торговые кошельки того же владельца.

18.11, разбор по блокчейну:
• 7BNax (лучший smart: торговал с января 2026, около 1000 транзакций в сутки, PumpSwap) 17.11 в 03:56 UTC перевёл
  весь остаток — 421.5 SOL — на Ce8vr… и замолчал. Ce8vr — копилка: сама не торгует, с 12.07 получила от 7BNax
  411 переводов, 7376 SOL; в сентябре сама пополняла 7BNax (270 SOL).
• Из копилки крупные суммы уходят на EnvMK… — обменник (дальше SOL меняется на другие монеты). В обменник в ту же
  минуту (30.09 и 02.11) приходили деньги ещё из двух копилок — Askojg… и F6DxYF…: тот же владелец.
• F6DxYF… с 15.11 получает прибыль от Ar2Y6o1…ro1x — он торгует сейчас (1000 транзакций за 10 часов, PumpSwap,
  ~120 SOL). Это и есть «такой же кошелёк» — найден по деньгам, а не по сделкам.

Бот проходит ту же цепочку сам:
1. Кошелёк smart, который дал бумаге A прибыль за 14 дней (10+ сделок), молчит 2+ часа → его последние транзакции:
   перевод от 2 SOL на чужой адрес → копилка.
2. Копилки — раз в 15 минут: перевод с копилки от 50 SOL → обменник (следим); любой перевод с копилки от 0.5 SOL и
   прибыль в копилку не от владельца (за последние 3 дня) → кандидат «семья …».
3. Обменники: крупные поступления (от 50 SOL) из других адресов → ещё копилки того же владельца → пункт 2.
Кандидат идёт на испытание первым в очереди (испытание по цене блокчейна решает, копировать ли), сообщение — в тему
разведки. Переводы 0 SOL и адреса-двойники (те же первые и последние 4 знака — «отравление адреса»: 7BNaqa…LGH5 шлёт
копилке по 0 SOL после каждого перевода 7BNax, чтобы владелец скопировал не тот адрес) пропускаются; биржи — тоже.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from ..alerts.format import esc

log = logging.getLogger(__name__)

SOURCE = "семья"
PARENT_MIN_TRADES = 10
SILENT_S = 2 * 3600
PARENT_RECHECK_S = 24 * 3600
EVERY_S = 15 * 60
SWEEP_MIN_SOL = 2.0          # перевод владельца в копилку
CHILD_MIN_SOL = 0.5          # перевод с копилки / прибыль в копилку
CASHOUT_MIN_SOL = 50.0       # копилка → обменник, обменник ← другая копилка
RECENT_S = 3 * 86400         # при первом разборе копилки кандидаты — только из переводов за 3 дня
PARENT_TXS = 30
FIRST_TXS = {"stash": 300, "cashout": 40}
NEXT_TXS = 20
SCORE = 10_000.0             # первым в очереди на испытание (плюс сила владельца — см. _strength)
SERVICE_STASH = 4            # в «копилку» шлют прибыль 4+ разных кошелька — это общий адрес (сервис, биржа), не личный
SERVICE_CASHOUT = 6          # в «обменник» сливают 6+ разных копилок — тоже общий адрес


def short(addr: str | None) -> str:
    addr = addr or "?"
    return f"{addr[:4]}…{addr[-4:]}" if len(addr) > 10 else addr


def twin(a: str, b: str) -> bool:
    """Адрес-двойник: те же первые и последние 4 знака, но другой адрес (отравление адреса)."""
    return bool(a and b) and a != b and len(a) > 10 and a[:4] == b[:4] and a[-4:] == b[-4:]


def transfers(tx: dict[str, Any] | None, wallet: str) -> list[tuple[str, str, float]]:
    """Переводы SOL (системная программа) с участием wallet: (откуда, куда, SOL) — верхние и вложенные инструкции."""
    if not tx or (tx.get("meta") or {}).get("err") is not None:
        return []
    msg = (tx.get("transaction") or {}).get("message") or {}
    inner = [ix for g in ((tx.get("meta") or {}).get("innerInstructions") or []) for ix in g.get("instructions") or []]
    out = []
    for ix in list(msg.get("instructions") or []) + inner:
        p = ix.get("parsed")
        if ix.get("program") != "system" or not isinstance(p, dict) or p.get("type") not in ("transfer",
                                                                                            "transferWithSeed"):
            continue
        info = p.get("info") or {}
        src, dst = info.get("source") or "", info.get("destination") or ""
        if wallet in (src, dst):
            out.append((src, dst, int(info.get("lamports") or 0) / 1e9))
    return out


class Family:
    def __init__(self, engine: Any, scout: Any):
        self.e = engine
        self.db = engine.db
        self.scout = scout
        self._at = 0.0

    @property
    def rpc(self) -> Any:
        return getattr(self.e, "rpc", None)

    def exchanges(self) -> set[str]:
        settings = getattr(self.e, "settings", None)
        try:
            return set(settings.exchanges()) if settings is not None else set()
        except Exception:  # noqa: BLE001
            return set()

    async def run(self, force: bool = False) -> list[str]:
        if self.rpc is None or (not force and time.time() - self._at < EVERY_S):
            return []
        self._at = time.time()
        events = await self.check_parents()
        events += await self.check_stashes()
        events += await self.check_cashouts()
        events += await self.check_services()
        await self.reprioritize()
        return events

    async def check_services(self) -> list[str]:
        """«Копилка», в которую шлют прибыль много разных кошельков, — общий адрес (сервис, биржа): её «кошельки» —
        разные люди. 18.11: у GZVS «копилка» 94zb… получала от 5+ кошельков. Перестаём за ней следить, её кандидатов
        из очереди убираем (те, что уже на испытании, доиспытываются)."""
        out: list[str] = []
        for role, limit, kids in (("stash", SERVICE_STASH, "child"), ("cashout", SERVICE_CASHOUT, "stash")):
            for r in await self.db.fetch(
                    """SELECT f.address, f.root, count(c.*) n FROM wallet_family f
                       JOIN wallet_family c ON c.parent = f.address AND c.role = $2
                       WHERE f.role = $1 GROUP BY 1, 2 HAVING count(c.*) >= $3""", role, kids, limit):
                await self.db.execute("UPDATE wallet_family SET role='service', note=$2 WHERE address=$1", r["address"],
                                      f"общий адрес: {r['n']} разных кошельков — не личная копилка, не следим")
                dropped = await self.db.execute(
                    """DELETE FROM scout WHERE status = 'queued'
                         AND address IN (SELECT address FROM wallet_family WHERE parent = $1)""", r["address"])
                out.append(f"⚠️ {esc(short(r['address']))} — общий адрес ({r['n']} разных кошельков), а не копилка "
                           f"владельца {esc(short(r['root']))}: не слежу, из очереди убрано {dropped.split()[-1]}.")
        return out

    async def reprioritize(self) -> None:
        """Очередь: семья сильнейшего владельца — первой (у кандидатов, найденных раньше, — та же сила)."""
        for r in await self.db.fetch(
                """SELECT DISTINCT f.root FROM wallet_family f JOIN scout s ON s.address = f.address
                   WHERE f.role = 'child' AND s.status = 'queued' AND f.root IS NOT NULL"""):
            await self.db.execute(
                """UPDATE scout s SET score = $2 FROM wallet_family f
                   WHERE f.address = s.address AND f.role = 'child' AND f.root = $1 AND s.status = 'queued'""",
                r["root"], await self._strength(r["root"]))

    async def _rows(self, role: str | None = None) -> list[Any]:
        if role is None:
            return await self.db.fetch("SELECT * FROM wallet_family")
        return await self.db.fetch("SELECT * FROM wallet_family WHERE role=$1 ORDER BY found_at", role)

    async def _add(self, addr: str, role: str, parent: str, root: str, note: str) -> bool:
        res = await self.db.execute(
            """INSERT INTO wallet_family(address, role, parent, root, note) VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (address) DO NOTHING""", addr, role, parent, root, note)
        return res.endswith("1")

    def _skip(self, other: str, *near: str) -> bool:
        return not other or other in self.exchanges() or any(twin(other, x) for x in near if x)

    async def _strength(self, root: str | None) -> float:
        """Сила владельца: сумма процентов его копий на бумаге A за 14 дней (7BNax — сотни сделок по +28%). Семья
        сильнейшего — первой в очереди на испытание."""
        if not root:
            return SCORE
        v = await self.db.fetchval(
            """SELECT COALESCE(sum(pnl_pct), 0) FROM paper_positions WHERE portfolio = 'A' AND source_wallet = $1
                 AND opened_at > now() - interval '14 days' AND closed_at IS NOT NULL AND NOT suspect""", root)
        return SCORE + max(0.0, float(v or 0)) * 100

    async def _candidate(self, addr: str, stash: Any, reason: str) -> str:
        root = stash["root"] or stash["parent"]
        await self._add(addr, "child", stash["address"], root, reason)
        queued = await self.scout._queue(addr, f"{SOURCE} {short(root)}", reason, await self._strength(root), None)
        return (f"👪 {esc(reason)} → <code>{addr}</code>"
                + (" — первым на испытание" if queued else " — уже проверялся (в отсеве или в списке)"))

    async def _new_txs(self, row: Any) -> tuple[list[tuple[dict[str, Any], dict[str, Any] | None]], bool]:
        """Новые транзакции (старые → новые) и признак первого разбора."""
        first = row["last_sig"] is None
        sigs = await self.rpc.signatures_for(row["address"], limit=FIRST_TXS.get(row["role"], NEXT_TXS) if first
                                             else NEXT_TXS, until=None if first else row["last_sig"])
        if sigs:
            await self.db.execute("UPDATE wallet_family SET last_sig=$2, checked_at=now() WHERE address=$1",
                                  row["address"], sigs[0]["signature"])
        out = []
        for s in reversed(sigs):
            if s.get("err"):
                continue
            out.append((s, await self.rpc.transaction(s["signature"])))
        return out, first

    # ------------------------------------------------------------------ 1. копилки лучших
    async def parents(self) -> list[Any]:
        return await self.db.fetch(
            """SELECT source_wallet w, max(opened_at) last FROM paper_positions
               WHERE portfolio = 'A' AND source_wallet IS NOT NULL AND opened_at > now() - interval '14 days'
                 AND closed_at IS NOT NULL AND NOT suspect AND exit_reason IS DISTINCT FROM 'reset'
               GROUP BY 1 HAVING sum(pnl_sol) > 0 AND count(*) >= $1
               ORDER BY sum(pnl_pct) DESC LIMIT 10""", PARENT_MIN_TRADES)

    async def check_parents(self) -> list[str]:
        out: list[str] = []
        now = time.time()
        known = {r["address"] for r in await self._rows()}
        for p in await self.parents():
            w = p["w"]
            if p["last"] is None or now - p["last"].timestamp() < SILENT_S:
                continue                             # торгует — переводов не ищем (у активного 1000 транзакций в сутки)
            seen = await self.db.kv_get(f"family:parent:{w}")
            if seen and now - float(seen) < PARENT_RECHECK_S:
                continue
            await self.db.kv_set(f"family:parent:{w}", now)
            for s in await self.rpc.signatures_for(w, limit=PARENT_TXS):
                if s.get("err"):
                    continue
                for src, dst, sol in transfers(await self.rpc.transaction(s["signature"]), w):
                    if src != w or sol < SWEEP_MIN_SOL or dst in known or self._skip(dst, w):
                        continue
                    await self._add(dst, "stash", w, w, f"{short(w)} перевёл {sol:.1f} SOL")
                    known.add(dst)
                    out.append(f"🏦 Копилка лучшего кошелька {esc(short(w))}: <code>{dst}</code> — перевёл туда "
                               f"{sol:.1f} SOL. Слежу за ней и за кошельками того же владельца.")
        return out

    # ------------------------------------------------------------------ 2. копилки
    async def check_stashes(self) -> list[str]:
        out: list[str] = []
        family = {r["address"] for r in await self._rows()}
        now = time.time()
        for st in await self._rows("stash"):
            addr, parent = st["address"], st["parent"] or ""
            txs, first = await self._new_txs(st)
            for s, tx in txs:
                recent = now - float(s.get("blockTime") or 0) <= RECENT_S
                for src, dst, sol in transfers(tx, addr):
                    other = dst if src == addr else src
                    if sol < CHILD_MIN_SOL or other in (addr, parent) or self._skip(other, addr, parent, st["root"]):
                        continue
                    if src == addr and sol >= CASHOUT_MIN_SOL and other not in family:
                        # крупная сумма с копилки — обменник (или новый торговый кошелёк: в сентябре копилка 7BNax
                        # пополняла его самого по 50–100 SOL) — следим за ним и, если свежий перевод, проверяем
                        await self._add(other, "cashout", addr, st["root"] or parent,
                                        f"копилка {short(addr)} перевела {sol:.0f} SOL")
                        family.add(other)
                        out.append(f"🔁 Обменник владельца {esc(short(st['root'] or parent))}: <code>{other}</code> — "
                                   f"слежу, какие ещё копилки в него сливают.")
                        if first and not recent:
                            continue
                    elif other in family or (first and not recent):
                        continue
                    way = (f"копилка {short(addr)} перевела {sol:.2f} SOL" if src == addr else
                           f"переводит прибыль ({sol:.2f} SOL) в копилку {short(addr)}")
                    family.add(other)
                    out.append(await self._candidate(other, st, f"семья лучшего кошелька {short(st['root'] or parent)}:"
                                                                f" {way}"))
        return out

    # ------------------------------------------------------------------ 3. обменники
    async def check_cashouts(self) -> list[str]:
        out: list[str] = []
        family = {r["address"] for r in await self._rows()}
        for c in await self._rows("cashout"):
            addr = c["address"]
            txs, _first = await self._new_txs(c)
            for _s, tx in txs:
                for src, dst, sol in transfers(tx, addr):
                    if dst != addr or sol < CASHOUT_MIN_SOL or src in family or self._skip(src, addr, c["parent"]):
                        continue
                    await self._add(src, "stash", addr, c["root"] or c["parent"],
                                    f"сливает {sol:.0f} SOL в тот же обменник {short(addr)}")
                    family.add(src)
                    out.append(f"🏦 Ещё копилка владельца {esc(short(c['root'] or c['parent']))}: <code>{src}</code> — "
                               f"слила {sol:.0f} SOL в его обменник. Ищу её торговые кошельки.")
        return out


async def report_lines(db: Any) -> list[str]:
    rows = await db.fetch("SELECT * FROM wallet_family ORDER BY found_at DESC LIMIT 15")
    if not rows:
        return []
    icon = {"stash": "🏦 копилка", "cashout": "🔁 обменник", "child": "👪 кошелёк", "service": "⚠️ общий адрес"}
    lines = ["\n<b>👪 Семья лучших кошельков</b> (копилки, обменники и кошельки того же владельца)"]
    for r in rows:
        lines.append(f"• {icon.get(r['role'], r['role'])} {esc(short(r['address']))} · владелец "
                     f"{esc(short(r['root']))} · {esc(r['note'] or '')}")
    return lines
