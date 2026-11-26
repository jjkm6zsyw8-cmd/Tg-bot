"""Бэктест «быстрого выхода» на настоящих движениях цены: если за N секунд после входа монета не выросла
на +G%, продаём по цене N-й секунды. Сравнивает с тем, что бумага сделала на самом деле.

Цена после входа — сделки по монете из своей базы (таблица trades: слежка подписывается на монету открытой
позиции). Позиции, закрытые раньше N секунд, и позиции без сделок в окне остаются как были.

Запуск на сервере (одна строка, без пересборки):
    cd /opt/tg-bot && docker compose exec -T analytics python - < scripts/exit_backtest.py
Переменные: BT_DAYS — за сколько дней (по умолчанию 3).
"""
from __future__ import annotations

import asyncio
import json
import os

import asyncpg

DAYS = float(os.environ.get("BT_DAYS", 3))
WINDOWS = (30, 60, 90, 120, 180, 300)
GAINS = (0.05, 0.10)
FEE = 0.0125          # комиссия кривой pump.fun с каждой стороны
PRIO = 0.0005         # приоритетная комиссия за транзакцию, SOL
GROUPS = [
    ("Копии одиночные", lambda m, lv: m == "copy" and lv == "normal"),
    ("Копии сильные", lambda m, lv: m == "copy" and lv == "strong"),
    ("Наблюдение (лотерея)", lambda m, lv: m == "dev" and lv == "watch"),
    ("Новые монеты", lambda m, lv: m == "new"),
    ("Мой дев", lambda m, lv: m == "mydev"),
]


def sell_now(cost: float, entry: float, mc: float) -> float:
    """Результат (SOL), если купили на cost по MC entry и продали по MC mc (комиссии и приоритет)."""
    tokens_value = (cost - PRIO) * (1 - FEE)
    return tokens_value * (mc / entry) * (1 - FEE) - PRIO - cost


async def main() -> None:
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    raw = await conn.fetchval("SELECT value FROM kv WHERE key='sol_usd'")
    sol_usd = float((json.loads(raw) if isinstance(raw, str) else raw or {}).get("price", 90)) if raw else 90.0
    rows = await conn.fetch(
        """SELECT p.id, p.mint, p.opened_at, p.closed_at, p.entry_mc, p.cost_sol, p.pnl_sol, p.pnl_pct, p.level,
                  COALESCE(a.mode, '?') mode
           FROM paper_positions p LEFT JOIN alerts a ON a.id = p.alert_id
           WHERE p.closed_at IS NOT NULL AND p.exit_reason <> 'reset' AND NOT p.suspect
             AND p.pnl_pct <= 20 AND p.cost_sol <= 2 AND p.entry_mc > 0
             AND p.opened_at > now() - make_interval(secs => $1)""", DAYS * 86400)
    paths: dict[tuple[str, object], list[tuple[float, float]]] = {}
    for r in rows:
        key = (r["mint"], r["opened_at"])
        if key in paths:
            continue
        ts = await conn.fetch(
            """SELECT ts, mc_sol FROM trades WHERE mint=$1 AND ts > $2 AND ts <= $2 + interval '300 seconds'
               AND mc_sol > 0 ORDER BY ts""", r["mint"], r["opened_at"])
        paths[key] = [((t["ts"] - r["opened_at"]).total_seconds(), t["mc_sol"]) for t in ts]
    await conn.close()

    usd = lambda sol: sol / 7 * sol_usd  # noqa: E731 — на один портфель из семи
    print(f"Бэктест быстрого выхода · {DAYS:g} дн. · $ на один портфель (SOL ${sol_usd:.0f})")
    print("строка: «нет +5% за N с» → итог (ракет ×3 срезано) · «нет +10%» → итог (ракет срезано)")
    for name, fn in GROUPS:
        g = [r for r in rows if fn(r["mode"], r["level"])]
        if not g:
            continue
        actual = sum(r["pnl_sol"] or 0 for r in g)
        nodata = sum(1 for r in g if not paths[(r["mint"], r["opened_at"])])
        print(f"\n{name}: сделок {len(g)} (без цены после входа {nodata}), сейчас {usd(actual):+.0f}$")
        for n in WINDOWS:
            parts = []
            for gain in GAINS:
                total, rockets_cut, cut = 0.0, 0, 0
                for r in g:
                    held = (r["closed_at"] - r["opened_at"]).total_seconds()
                    path = paths[(r["mint"], r["opened_at"])]
                    window = [mc for t, mc in path if t <= n]
                    if held <= n or not window or max(window) >= r["entry_mc"] * (1 + gain):
                        total += r["pnl_sol"] or 0
                        continue
                    cut += 1
                    rockets_cut += (r["pnl_pct"] or 0) >= 2.0
                    total += sell_now(r["cost_sol"], r["entry_mc"], window[-1])
                parts.append(f"+{gain:.0%}→ {usd(total):+.0f}$ ({rockets_cut})")
            print(f"  {n:>3} с: " + " · ".join(parts))


asyncio.run(main())
