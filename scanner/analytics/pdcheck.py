"""26.11 (сводное ТЗ, B5): сверка образца pumpdata.fun (Parquet за день: block_time, slot, wallet, mint, side,
sol_lamports, token_amount, venue…) с нашей базой сделок за тот же интервал.

  docker compose cp sample.parquet analytics:/tmp/s.parquet
  docker compose exec -T analytics sh -c "pip install -q pyarrow && python -m scanner pdcheck /tmp/s.parquet"

Что показывает: сколько сделок у них и у нас; какая доля наших сделок есть у них (по блоку, кошельку, монете,
стороне) — проверка, что их данные полные; какая доля их сделок по нашим отслеживаемым кошелькам есть у нас — сколько
мы теряем; по кривой и PumpSwap отдельно. Ничего не покупает и никуда не ходит — только читает файл и базу.
Нужен pyarrow: docker compose exec analytics pip install pyarrow (до пересборки образа).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def norm_side(v: Any) -> bool:
    s = str(v).lower()
    return s in ("buy", "b", "1", "true")


def norm_venue(v: Any) -> str:
    s = str(v or "").lower()
    return "pump-amm" if ("amm" in s or "swap" in s) else "pump"


def load_parquet(path: str) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq            # noqa: PLC0415 — нужен только для этой проверки
    t = pq.read_table(path)
    cols = set(t.column_names)
    need = {"slot", "wallet", "mint", "side"}
    if not need <= cols:
        raise SystemExit(f"в файле нет колонок {sorted(need - cols)} (есть: {sorted(cols)[:30]})")
    keep = [c for c in ("block_time", "ts", "slot", "wallet", "mint", "side", "venue", "sol_lamports", "token_amount",
                        "failed") if c in cols]
    return t.select(keep).to_pylist()


def compare(theirs: list[dict[str, Any]], ours: list[dict[str, Any]], tracked: set[str]) -> str:
    """theirs — строки образца; ours — наши сделки (slot, trader, mint, is_buy, pool) за тот же интервал."""
    theirs = [r for r in theirs if not r.get("failed")]
    tk = {(int(r["slot"]), r["wallet"], r["mint"], norm_side(r["side"])): norm_venue(r.get("venue")) for r in theirs}
    ok = {(int(r["slot"]), r["trader"], r["mint"], bool(r["is_buy"])): r["pool"] or "pump" for r in ours
          if r["slot"] is not None}
    lines = [f"Образец pumpdata: {len(tk)} сделок (без неудачных), у нас за тот же интервал: {len(ok)} (с блоком)"]
    for venue, name in (("pump", "кривая"), ("pump-amm", "PumpSwap")):
        o = {k for k, v in ok.items() if v == venue}
        t = {k for k, v in tk.items() if v == venue}
        found = len(o & set(tk)) / len(o) if o else None
        lines.append(f"  {name}: у них {len(t)}, у нас {len(o)}; наших у них нашлось "
                     + (f"{found:.1%}" if found is not None else "—"))
    mt = {k for k in tk if k[1] in tracked}
    if mt:
        have = len(mt & set(ok))
        lines.append(f"Отслеживаемые кошельки ({len(tracked)}): у них {len(mt)} сделок, у нас из них {have} "
                     f"({have / len(mt):.1%}) — столько их сделок мы видим")
    else:
        lines.append("Сделок наших отслеживаемых кошельков в образце нет")
    coins_t = {k[2] for k in tk}
    coins_o = {k[2] for k in ok}
    lines.append(f"Монет: у них {len(coins_t)}, у нас {len(coins_o)}, общих {len(coins_t & coins_o)}")
    return "\n".join(lines)


async def run(db: Any, path: str) -> str:
    theirs = load_parquet(path)
    if not theirs:
        return "Файл пустой."

    def ts_of(r: dict[str, Any]) -> float:
        v = r.get("block_time") or r.get("ts")
        return v.timestamp() if hasattr(v, "timestamp") else float(v)
    lo, hi = min(ts_of(r) for r in theirs), max(ts_of(r) for r in theirs)
    ours = [dict(r) for r in await db.fetch(
        """SELECT slot, trader, mint, is_buy, pool FROM trades WHERE ts BETWEEN to_timestamp($1) AND to_timestamp($2)""",
        lo, hi)]
    tracked = {r["address"] for r in await db.fetch(
        "SELECT address FROM traders WHERE active OR manual UNION SELECT address FROM hunter_wallets")}
    head = (f"Интервал образца: {datetime.fromtimestamp(lo, timezone.utc):%d.%m %H:%M} – "
            f"{datetime.fromtimestamp(hi, timezone.utc):%d.%m %H:%M} UTC")
    return head + "\n" + compare(theirs, ours, tracked)
