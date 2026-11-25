"""27.11 (владелец): отсев ферм при находке охотником. Ферма — кошелёк, который создавал монеты, получал комиссию
создателя pump.fun или профинансировал больше FUND_MAX кошельков (исходящие переводы SOL). Первый поиск на сервере:
3 из 4 найденных на solscan — Funding out 1–4 тыс. кошельков, баланса нет, у одного ещё Token Creator.

Проверка (`check`):
1. База: монеты, где он дев (tokens.deployer); кошельки, которые он профинансировал (wallets.funded_by — из разбора
   кластеров).
2. RPC: последние до MAX_SIGS подписей; из них до MAX_TX транзакций, которых нет среди его сделок в базе (сделки
   и так известны — остаются переводы, создания, сбор комиссий):
   - создание монеты pump.fun (TokenCreated, где он дев или создатель);
   - сбор комиссии создателя (в логах CollectCreatorFee / CollectCoinCreatorFee, он платит за транзакцию);
   - «чистые» переводы — транзакции только из system / compute budget / memo: SOL от него, разные получатели.
     Переводы внутри сделок (кривая, комиссии бота, чаевые Jito) не считаются.
Не хватило бюджета RPC или узел не ответил — «не проверен»: охотник берёт кошелёк в следующий раз, а не вслепую.
"""
from __future__ import annotations

from typing import Any, Callable

from ..ingest.pump_parser import ParseStats, parse_transaction
from ..ingest.rpc import RpcError
from ..models import TokenCreated

FUND_MAX = 20                          # профинансировал больше — ферма
MAX_SIGS = 1000                        # подписей за один запрос
MAX_TX = 150                           # транзакций на проверку (по кредиту Helius каждая)
SYSTEM = "11111111111111111111111111111111"
PLAIN = {SYSTEM, "ComputeBudget111111111111111111111111111111", "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",
         "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"}
FEE_LOGS = ("Instruction: CollectCreatorFee", "Instruction: CollectCoinCreatorFee")


def _key(k: Any) -> str:
    return k.get("pubkey", "") if isinstance(k, dict) else str(k)


def scan_tx(tx: dict[str, Any], addr: str, signature: str = "", slot: int | None = None) -> dict[str, Any]:
    """Одна транзакция (jsonParsed): кому он отправил SOL «чистым» переводом, создавал ли монету, забирал ли комиссию
    создателя."""
    out: dict[str, Any] = {"to": set(), "created": False, "fee": False}
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return out
    msg = (tx.get("transaction") or {}).get("message") or {}
    keys = [_key(k) for k in msg.get("accountKeys") or []]
    payer = keys[0] if keys else ""
    logs = meta.get("logMessages") or []
    if payer == addr and any(f in line for line in logs for f in FEE_LOGS):
        out["fee"] = True
    ixs = msg.get("instructions") or []
    if ixs and all(i.get("programId") in PLAIN for i in ixs):
        for i in ixs:
            p = i.get("parsed") or {}
            info = (p.get("info") or {}) if isinstance(p, dict) else {}
            if i.get("programId") == SYSTEM and isinstance(p, dict) and p.get("type") in ("transfer", "transferWithSeed") \
                    and info.get("source") == addr and info.get("destination") not in (None, addr):
                out["to"].add(info["destination"])
    try:
        for ev in parse_transaction(tx, signature, slot, "rpc", ParseStats(), {}):
            if isinstance(ev, TokenCreated) and addr in (ev.deployer, ev.creator):
                out["created"] = True
    except Exception:  # noqa: BLE001 — разбор чужой транзакции не должен ломать проверку
        pass
    return out


async def check(db: Any, rpc: Any, addr: str,
                over_budget: Callable[[], bool] | None = None) -> tuple[str | None, bool]:
    """(причина «ферма» или None, проверен ли кошелёк до конца)."""
    created = int(await db.fetchval("SELECT count(*) FROM tokens WHERE deployer = $1", addr) or 0)
    if created:
        return f"ферма: создал монет {created}", True
    funded = int(await db.fetchval("SELECT count(*) FROM wallets WHERE funded_by = $1", addr) or 0)
    if funded > FUND_MAX:
        return f"ферма: профинансировал {funded} кошельков", True
    if rpc is None:
        return None, False
    try:
        sigs = [s for s in await rpc.signatures_for(addr, limit=MAX_SIGS) if not s.get("err")]
    except RpcError:
        return None, False
    have = {r["signature"] for r in await db.fetch(
        "SELECT DISTINCT signature FROM trades WHERE trader = $1 AND signature = ANY($2::text[])",
        addr, [s["signature"] for s in sigs])} if sigs else set()
    to: set[str] = set()
    done = True
    for s in [s for s in sigs if s["signature"] not in have][:MAX_TX]:
        if over_budget is not None and over_budget():
            done = False
            break
        try:
            tx = await rpc.transaction(s["signature"])
        except RpcError:
            done = False
            continue
        if not tx:
            continue
        r = scan_tx(tx, addr, s["signature"], s.get("slot"))
        if r["created"]:
            return "ферма: создавал монеты pump.fun", True
        if r["fee"]:
            return "ферма: получал комиссию создателя", True
        to |= r["to"]
        if len(to) > FUND_MAX:
            return f"ферма: профинансировал {len(to)}+ кошельков", True
    return None, done
