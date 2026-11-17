"""Проверка реального бота на живой сети — без денег и без подписи.

Берёт монеты, которые торгуются прямо сейчас (одну на кривой pump.fun и одну в пуле PumpSwap), для каждой
читает состояние из блокчейна, считает цену и собирает ОДНУ транзакцию «купить и сразу продать» — теми же
функциями, которыми пользуется исполнитель. Затем отдаёт её сети на симуляцию: сеть исполняет её «понарошку»
и отвечает, прошла бы она. Ничего не подписывается и не отправляется.

Для симуляции на кошельке должно быть немного SOL (от ~0.01): без этого проверяются только чтение состояния
и расчёт цены. Запуск: кнопка «🧪 Проверка на сети» в /real или на сервере
`docker compose exec -T analytics python -m scanner real-check [адрес монеты ...]`.
"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from solders.pubkey import Pubkey

from . import pump, pumpswap, tx as rtx

if TYPE_CHECKING:
    from .executor import RealTrader

VENUES = {"pump": "кривая pump.fun", "pump-amm": "пул PumpSwap"}
MIN_SIM_LAMPORTS = 1_000_000
SIM_RESERVE = 8_000_000            # на залоги счетов и комиссию во время симуляции
CU_ROUND_TRIP = 400_000


def round_trip_ixs(trader: "RealTrader", st: dict[str, Any], mint: Pubkey, lamports: int, min_tokens: int) -> list[Any]:
    """Купить на lamports и тут же продать min_tokens — одной транзакцией, через тот же белый список."""
    user, tp = trader.wallet.pubkey, st["token_program"]
    ixs = rtx.budget_ixs(CU_ROUND_TRIP, 1_000)
    tip = int(float(trader.get("real_tip_sol")) * 1e9)
    if tip > 0:
        ixs.append(rtx.tip_ix(user, tip))
    if st["venue"] == "pump":
        ixs += [pump.create_ata_ix(user, user, mint, tp), pump.buy_ix(user, mint, tp, lamports, min_tokens),
                pump.sell_ix(user, mint, tp, min_tokens, 0)]
    else:
        ixs += pumpswap.wrap_sol_ixs(user, lamports)
        ixs += [pump.create_ata_ix(user, user, mint, tp), pumpswap.buy_ix(user, st["pool"], tp, lamports, min_tokens),
                pumpswap.sell_ix(user, st["pool"], tp, min_tokens, 0), pumpswap.unwrap_sol_ix(user)]
    rtx.check_instructions(ixs, user, lamports)
    return ixs


async def recent_mints(trader: "RealTrader", limit: int = 14) -> list[str]:
    """Монеты со свежими сделками (из памяти аналитики) и монеты последних бумажных сделок."""
    out: list[str] = []
    states = getattr(trader.e, "states", None) or {}
    fresh = []
    for mint, st in states.items():
        trades = getattr(st, "trades", None)
        if trades:
            fresh.append((getattr(trades[-1], "ts", 0) or 0, mint))
    out += [m for _ts, m in sorted(fresh, reverse=True)[:limit // 2]]
    try:
        rows = await trader.db.fetch("SELECT mint FROM paper_positions WHERE opened_at > now() - interval '24 hours' "
                                     "GROUP BY mint ORDER BY max(opened_at) DESC LIMIT $1", limit)
        out += [r["mint"] for r in rows]
    except Exception:  # noqa: BLE001
        pass
    seen: set[str] = set()
    return [m for m in out if not (m in seen or seen.add(m))][:limit]


async def run(trader: "RealTrader", mints: list[str] | None = None) -> str:
    lines = ["🧪 <b>Проверка реального бота на живой сети</b>", "Без денег: ничего не подписывается и не отправляется."]
    chain, wallet = trader.chain, trader.wallet
    if chain is None:
        return "\n".join(lines + ["❌ Нет доступа к сети (RPC не настроен)."])
    cands: list[Pubkey] = []
    for m in (mints or await recent_mints(trader)):
        try:
            cands.append(Pubkey.from_string(m))
        except Exception:  # noqa: BLE001
            continue
    if not cands:
        return "\n".join(lines + ["Нет монет со свежими сделками — повторите через пару минут."])
    t0 = time.monotonic()
    try:
        sts, extra = await trader._states(cands[:14], [wallet.pubkey] if wallet is not None else None)
    except Exception as exc:  # noqa: BLE001
        return "\n".join(lines + [f"❌ Сеть не ответила: {type(exc).__name__}: {str(exc)[:120]}"])
    rpc_ms = (time.monotonic() - t0) * 1000
    balance = extra[0].lamports if wallet is not None and extra and extra[0] is not None else 0
    lines.append(f"Ответ узла: {rpc_ms:.0f} мс · монет проверено: {len(sts)}")
    if wallet is None:
        lines.append("Кошелёк не задан — проверяю только чтение состояния и расчёт цены.")
    else:
        lines.append(f"Кошелёк <code>{wallet.pubkey}</code>: {balance / 1e9:.4f} SOL · ключ "
                     f"{'есть' if wallet.can_sign else 'не задан'}")
    picked: dict[str, tuple[Pubkey, dict[str, Any]]] = {}
    skipped: dict[str, int] = {}
    for m, st in zip(cands, sts):
        if st["venue"] and not st["why"]:
            picked.setdefault(st["venue"], (m, st))
        else:
            skipped[st["why"] or "?"] = skipped.get(st["why"] or "?", 0) + 1
    ok = True
    for venue, name in VENUES.items():
        if venue not in picked:
            lines.append(f"• {name}: подходящей монеты сейчас нет — не проверено")
            continue
        mint, st = picked[venue]
        stake, why = trader.stake_lamports(st["v_quote"])
        if why:
            lines.append(f"• {name}: {why}")
            continue
        tokens = trader._quote_buy(st, stake)
        head = (f"• {name} <code>{str(mint)[:6]}…</code>: MC {st['mc']:.1f} SOL, ставка {stake / 1e9:.4f} SOL → "
                + f"{tokens / 1e6:,.0f}".replace(",", " ") + " токенов")
        if wallet is None:
            lines.append(head + " · расчёт ✅")
            continue
        sim_stake = min(stake, balance - SIM_RESERVE)
        if sim_stake < MIN_SIM_LAMPORTS:
            lines.append(head + " · расчёт ✅ · симуляция не выполнена: на кошельке мало SOL (нужно от 0.01)")
            continue
        min_tokens = max(1, int(trader._quote_buy(st, sim_stake) * (1 - float(trader.get("real_buy_slippage")))))
        try:
            ixs = round_trip_ixs(trader, st, mint, sim_stake, min_tokens)
            sim = await chain.simulate(wallet.unsigned(rtx.compile_message(wallet.pubkey, ixs)))
        except rtx.Forbidden as exc:
            ok = False
            lines.append(head + f" · ❌ белый список отклонил транзакцию: {exc}")
            continue
        except Exception as exc:  # noqa: BLE001
            ok = False
            lines.append(head + f" · ❌ симуляция не выполнена: {type(exc).__name__}: {str(exc)[:100]}")
            continue
        if sim.err is None:
            lines.append(head + " · покупка и продажа ✅ (" + f"{sim.units:,}".replace(",", " ")
                         + " единиц вычислений)")
        else:
            ok = False
            lines.append(head + f" · ❌ {trader._err_text(sim.err, venue)}")
            tail = [x for x in sim.logs if "rror" in x or "failed" in x][-2:]
            lines += [f"   <code>{x[:160]}</code>" for x in tail]
    if skipped:
        lines.append("Пропущены как неторгуемые: " + "; ".join(f"{k} — {v}" for k, v in sorted(skipped.items())))
    lines.append("Итог: " + ("✅ сборка сделок и расчёт цены работают." if ok and picked else
                             "⚠️ проверка неполная." if ok else "❌ есть ошибки — бой включать нельзя."))
    return "\n".join(lines)
