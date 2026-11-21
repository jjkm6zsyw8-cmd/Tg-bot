"""Соседи лучшего кошелька по блокчейну: кто покупал те же монеты в те же секунды — до него, в том же блоке, после.

Зачем (18.11): весь недельный плюс бумаги A (+$716 из +$760) дал один кошелёк — 7BNax, и он 17.11 замолчал. Чтобы
найти «такого же», смотрим его последние монеты: кто раз за разом покупает их РАНЬШЕ него или в том же блоке —
торгует по тому же сигналу (или сам источник сигнала); кто покупает сразу ПОСЛЕ — копировщики (их копировать
поздно). Своя база сделок PumpSwap неполная (по монетам 7BNax в ней пропуски), поэтому сделки вокруг каждой его
покупки берутся из блокчейна: подписи монеты (getSignaturesForAddress) в окне −90…+40 с и сами транзакции.

Покупки самого кошелька — из своей базы (отслеживаемый кошелёк: время и слот каждой покупки), иначе — из бумаги.
Запросов: на монету — страницы подписей (по 1000; у оживлённых монет — десятки) и транзакции окна (обычно 10–60).
Вручную на сервере: python -m scanner cobuyers [кошелёк] [--coins 60] [--queue].
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

WSOL = "So11111111111111111111111111111111111111112"
BEFORE_S, AFTER_S = 90, 40
SLOTS_BEFORE, SLOTS_AFTER = 150, 100     # ~60 с до и ~40 с после его покупки
MAX_PAGES = 40
SOURCE = "рядом с лучшим (блокчейн)"


def short(addr: str | None) -> str:
    addr = addr or "?"
    return f"{addr[:4]}…{addr[-4:]}" if len(addr) > 10 else addr


def twin(a: str, b: str) -> bool:
    return bool(a and b) and a != b and len(a) > 10 and a[:4] == b[:4] and a[-4:] == b[-4:]


def swaps_of(tx: dict[str, Any] | None, mint: str) -> list[tuple[str, str, float]]:
    """Покупки и продажи монеты в транзакции: (кошелёк, buy|sell, SOL). Кошелёк — тот, кто подписал транзакцию и у
    кого изменился остаток монеты (пул и его хранилища не подписывают — их отсекает это условие). SOL — изменение
    баланса кошелька плюс его wSOL (PumpSwap платит из wSOL-счёта)."""
    if not tx or (tx.get("meta") or {}).get("err") is not None:
        return []
    meta = tx.get("meta") or {}
    keys = ((tx.get("transaction") or {}).get("message") or {}).get("accountKeys") or []
    names = [k.get("pubkey") if isinstance(k, dict) else str(k) for k in keys]
    signers = {k.get("pubkey") for k in keys if isinstance(k, dict) and k.get("signer")} or ({names[0]} if names else set())
    tok: dict[str, float] = defaultdict(float)
    wsol: dict[str, float] = defaultdict(float)
    for side, sign in (("preTokenBalances", -1), ("postTokenBalances", 1)):
        for b in meta.get(side) or []:
            owner = b.get("owner")
            if owner not in signers:
                continue
            amount = float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
            if b.get("mint") == mint:
                tok[owner] += sign * amount
            elif b.get("mint") == WSOL:
                wsol[owner] += sign * amount
    out = []
    pre, post = meta.get("preBalances") or [], meta.get("postBalances") or []
    for owner, d in tok.items():
        if abs(d) < 1e-9:
            continue
        i = names.index(owner) if owner in names else None
        sol = ((post[i] - pre[i]) / 1e9 if i is not None and i < len(pre) and i < len(post) else 0.0) + wsol[owner]
        if d > 0 and sol < -0.01:
            out.append((owner, "buy", -sol))
        elif d < 0 and sol > 0.01:
            out.append((owner, "sell", sol))
    return out


@dataclass
class Neighbour:
    wallet: str
    before: set[str] = field(default_factory=set)
    same: set[str] = field(default_factory=set)
    after: set[str] = field(default_factory=set)
    sol: list[float] = field(default_factory=list)

    @property
    def lead(self) -> int:
        """Монет, где он купил раньше или в том же блоке."""
        return len(self.before | self.same)

    @property
    def size(self) -> float:
        s = sorted(self.sol)
        return s[len(s) // 2] if s else 0.0


@dataclass
class Result:
    wallet: str
    coins: int = 0
    found: int = 0                  # монет, где его покупка нашлась в окне
    calls: int = 0
    near: dict[str, Neighbour] = field(default_factory=dict)


async def his_buys(db: Any, wallet: str, coins: int) -> list[tuple[str, float, int | None]]:
    """(монета, время первой покупки, слот) — новые первыми: из своей базы сделок, иначе из бумаги."""
    rows = await db.fetch(
        """SELECT mint, extract(epoch FROM min(ts)) ts, min(slot) slot FROM trades
           WHERE trader = $1 AND is_buy GROUP BY mint ORDER BY min(ts) DESC LIMIT $2""", wallet, coins)
    if rows:
        return [(r["mint"], float(r["ts"]), int(r["slot"]) if r["slot"] is not None else None) for r in rows]
    rows = await db.fetch(
        """SELECT mint, extract(epoch FROM min(opened_at)) ts FROM paper_positions
           WHERE source_wallet = $1 AND portfolio IN ('A', 'M', 'B', 'G') GROUP BY mint
           ORDER BY min(opened_at) DESC LIMIT $2""", wallet, coins)
    return [(r["mint"], float(r["ts"]) - 1.0, None) for r in rows]


async def best_wallet(db: Any) -> str | None:
    """Кошелёк, давший бумаге A больше всего прибыли за 14 дней."""
    return await db.fetchval(
        """SELECT source_wallet FROM paper_positions WHERE portfolio = 'A' AND source_wallet IS NOT NULL
             AND opened_at > now() - interval '14 days' AND closed_at IS NOT NULL AND NOT suspect
           GROUP BY 1 ORDER BY sum(pnl_pct) DESC LIMIT 1""")


async def neighbours(db: Any, rpc: Any, wallet: str, coins: int = 60, exchanges: set[str] | None = None,
                     progress: Any = None) -> Result:
    res = Result(wallet)
    skip = set(exchanges or ()) | {wallet}
    for mint, ts, slot in await his_buys(db, wallet, coins):
        res.coins += 1
        before, window = None, []
        for _ in range(MAX_PAGES):
            page = await rpc.signatures_for(mint, limit=1000, before=before)
            res.calls += 1
            if not page:
                break
            before = page[-1]["signature"]
            window += [s for s in page if not s.get("err") and ts - BEFORE_S <= float(s.get("blockTime") or 0)
                       <= ts + AFTER_S]
            if float(page[-1].get("blockTime") or 0) < ts - BEFORE_S or len(page) < 1000:
                break
        txs = await asyncio.gather(*(rpc.transaction(s["signature"]) for s in window), return_exceptions=True)
        res.calls += len(window)
        swaps = [(int(s.get("slot") or 0), w, side, sol) for s, tx in zip(window, txs) if isinstance(tx, dict)
                 for w, side, sol in swaps_of(tx, mint)]
        mine = [x[0] for x in swaps if x[1] == wallet and x[2] == "buy"]
        s0 = min(mine) if mine else slot
        if s0 is None:
            continue
        res.found += 1
        for sl, w, side, sol in swaps:
            if side != "buy" or w in skip or twin(w, wallet):
                continue
            d = sl - s0
            if not -SLOTS_BEFORE <= d <= SLOTS_AFTER:
                continue
            n = res.near.setdefault(w, Neighbour(w))
            (n.before if d < 0 else n.same if d == 0 else n.after).add(mint)
            n.sol.append(sol)
        if progress is not None:
            progress(res)
    return res


def report(res: Result, top: int = 25) -> str:
    lines = [f"Кошелёк {res.wallet}: монет {res.coins}, его покупка найдена в {res.found}, запросов к узлу {res.calls}",
             "", "Покупали те же монеты РАНЬШЕ него или в том же блоке (кандидаты — торгуют по тому же сигналу):",
             "кошелёк | монет: до него / тот же блок / после | обычно SOL"]
    leaders = sorted(res.near.values(), key=lambda n: (-n.lead, -len(n.after)))
    for n in leaders[:top]:
        if n.lead < 2:
            break
        lines.append(f"{n.wallet} | {len(n.before)} / {len(n.same)} / {len(n.after)} | {n.size:.2f}")
    lines += ["", "Покупали сразу ПОСЛЕ него (копировщики — за ними входить ещё позже):"]
    for n in sorted(res.near.values(), key=lambda n: -len(n.after))[:10]:
        if len(n.after) < 3:
            break
        lines.append(f"{n.wallet} | после на {len(n.after)} монетах, до/вместе на {n.lead}")
    return "\n".join(lines)


async def queue_leaders(scout: Any, res: Result, min_coins: int = 3, limit: int = 15) -> list[str]:
    """Кандидатов «раньше него или в том же блоке» (от min_coins монет) — на испытание (портфель S, правило бота)."""
    known = await scout._known()
    out = []
    for n in sorted(res.near.values(), key=lambda n: -n.lead):
        if n.lead < min_coins or len(out) >= limit:
            break
        if n.wallet in known:
            continue
        reason = (f"покупал монеты {short(res.wallet)} раньше него или в том же блоке: {n.lead} из {res.found} "
                  f"(до {len(n.before)}, вместе {len(n.same)}, после {len(n.after)}), обычно {n.size:.2f} SOL")
        if await scout._queue(n.wallet, f"{SOURCE} {short(res.wallet)}", reason, 9000.0 + n.lead * 10, None):
            out.append(f"{n.wallet} — {reason}")
    return out
