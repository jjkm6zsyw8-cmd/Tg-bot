"""Реальные цифры (23.11, просьба владельца: «нахуя мне бумага и как бы оно в идеале торговало — дай РЕАЛЬНЫЕ цифры»).

Только реальный бот: тень (каждый вход — по настоящей цене в сети в момент входа, продажа — по настоящей цене в момент
выхода; транзакция не отправляется) и бой (настоящие деньги). Ни бумаги, ни расчётов по истории. По методам — сегодня /
вчера / 7 дней (местное время), в долларах по твоей ставке; ниже — кошельки «за ним».
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ..alerts.format import esc
from . import settings as S

Kb = list[list[dict[str, Any]]]
SHORT = {"smart": "smart", "mydev": "мой дев", "copy": "копии", "bigcopy": "крупные", "swing": "ракеты",
         "trend": "тренды", "newdev": "новый дев", "trendj": "тренды др.", "majorj": "крупные мон."}


def _usd(v: float) -> str:
    if abs(v) < 0.005:
        return "0"
    return f"{'+' if v > 0 else '−'}${abs(v):.0f}" if abs(v) >= 10 else f"{'+' if v > 0 else '−'}${abs(v):.2f}"


def _bounds(cfg: dict[str, Any]) -> dict[str, datetime]:
    try:
        tz = ZoneInfo(str(cfg.get("timezone") or "Asia/Tbilisi"))
    except Exception:  # noqa: BLE001
        tz = ZoneInfo("Asia/Tbilisi")
    d0 = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    return {"today": d0, "yday": d0 - timedelta(days=1), "week": datetime.now(timezone.utc) - timedelta(days=7)}


def shadow_kinds() -> list[str] | None:
    """25.11: в таблице тени — только оставшиеся методы (убранные не торгуют; сделки «за ним» — в «За кошельками»).
    None — все (ничего не убрано)."""
    return S.active_methods() if S.RETIRED else None


RETIRED_NOTE = "Убраны 25.11 (в минусе): smart, мой дев, копии, крупные копии, тренды pump.fun, новый дев."


async def mode_lines(db: Any, cfg: dict[str, Any], mode: str, sol_usd: float,
                     kinds: list[str] | None = None) -> list[str]:
    """Таблица одного режима (тень / бой): метод × сегодня / вчера / 7 дн. Пусто — сделок за 7 дней нет.
    kinds — только эти методы (None — все). Бой показывается всегда целиком: это деньги."""
    b = _bounds(cfg)
    rows = await db.fetch(
        """SELECT kind,
                  COALESCE(sum(pnl_sol) FILTER (WHERE closed_at >= $2), 0) d0,
                  COALESCE(sum(pnl_sol) FILTER (WHERE closed_at >= $3 AND closed_at < $2), 0) d1,
                  COALESCE(sum(pnl_sol), 0) wk,
                  count(*) FILTER (WHERE closed_at >= $2) n0, count(*) FILTER (WHERE closed_at >= $2 AND pnl_sol > 0) w0,
                  count(*) n, count(*) FILTER (WHERE pnl_sol > 0) w
           FROM real_positions WHERE mode = $1 AND closed_at IS NOT NULL AND closed_at >= $4
             AND ($5::text[] IS NULL OR (kind = ANY($5::text[]) AND NOT COALESCE((extra->>'followed')::boolean, false)))
           GROUP BY kind ORDER BY sum(pnl_sol)""", mode, b["today"], b["yday"], min(b["week"], b["yday"]), kinds)
    if not rows:
        return []
    head = f"{'метод':<9}{'сегодня':>8}{'вчера':>8}{'7 дн':>8}"
    table = [head]
    tot = [0.0, 0.0, 0.0]
    n0 = w0 = n = w = 0
    for r in rows:
        v = [float(r["d0"]) * sol_usd, float(r["d1"]) * sol_usd, float(r["wk"]) * sol_usd]
        tot = [a + x for a, x in zip(tot, v)]
        n0, w0, n, w = n0 + r["n0"], w0 + r["w0"], n + r["n"], w + r["w"]
        table.append(f"{SHORT.get(r['kind'], r['kind'])[:9]:<9}{_usd(v[0]):>8}{_usd(v[1]):>8}{_usd(v[2]):>8}")
    table.append(f"{'ВСЕГО':<9}{_usd(tot[0]):>8}{_usd(tot[1]):>8}{_usd(tot[2]):>8}")
    name = "👻 <b>Тень</b> — настоящие цены в сети, без денег" if mode == "shadow" else "💵 <b>Бой</b> — настоящие деньги"
    return [name, "<pre>" + "\n".join(table) + "</pre>",
            f"сделок сегодня {n0}" + (f", в плюсе {w0 / n0:.0%}" if n0 else "")
            + f" · за 7 дн {n}" + (f", в плюсе {w / n:.0%}" if n else "")]


async def follow_lines(db: Any, sol_usd: float, limit: int = 12) -> list[str]:
    """Кошельки «за ним»: итог реального бота за 7 дней по каждому (тень и бой вместе)."""
    follow = dict(await db.kv_get("real:follow") or {})
    if not follow:
        return []
    auto = set(await db.kv_get("real:follow_auto") or [])
    rows = await db.fetch(
        """SELECT trader, count(*) n, count(*) FILTER (WHERE pnl_sol > 0) w, COALESCE(sum(pnl_sol), 0) s
           FROM real_positions WHERE trader = ANY($1::text[]) AND closed_at IS NOT NULL
             AND closed_at > now() - interval '7 days' GROUP BY 1 ORDER BY sum(pnl_sol) DESC""", list(follow))
    labels = {r["address"]: r["label"] for r in await db.fetch(
        "SELECT address, label FROM watchlist WHERE kind = 'trader' AND address = ANY($1::text[])", list(follow))}
    out = ["<b>За кошельками</b> (7 дн, тень и бой; 👤 — выбрал ты, 🤖 — бот)"]
    seen = set()
    for r in rows[:limit]:
        a = r["trader"]
        seen.add(a)
        name = esc((labels.get(a) or f"{a[:4]}…{a[-4:]}")[:14])
        out.append(f"{'🤖' if a in auto else '👤'} {name}: {_usd(float(r['s']) * sol_usd)} · {r['n']} сд, в плюсе {r['w']}")
    quiet = [a for a in follow if a not in seen]
    if quiet:
        out.append(f"без сделок за 7 дн: {len(quiet)}")
    return out


async def newdev_lines(db: Any) -> list[str]:
    """«Новый дев» (быстрый вход на запуске): по какой цене бот реально успевает купить — от этого зависит, окупается
    ли метод (0.5–2 SOL — вход до ~35 SOL MC, 10+ SOL — до ~57)."""
    r = await db.fetchrow(
        """SELECT count(*) n, percentile_cont(0.5) WITHIN GROUP (ORDER BY p.entry_mc) mc,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY p.entry_slot - t.created_slot) blocks,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY (p.extra->>'detect_ms')::float) det,
                  count(*) FILTER (WHERE p.closed_at IS NOT NULL AND p.pnl_sol > 0) w,
                  count(*) FILTER (WHERE p.closed_at IS NOT NULL) closed
           FROM real_positions p LEFT JOIN tokens t ON t.mint = p.mint
           WHERE p.kind = 'newdev' AND p.opened_at > now() - interval '7 days'""")
    skips = await db.fetchval(
        """SELECT count(*) FROM real_skips WHERE kind = 'newdev' AND why LIKE 'цена уже убежала%'
           AND at > now() - interval '7 days'""")
    if not (r and r["n"]) and not skips:
        return []
    out = ["<b>Новый дев</b> (вход на запуске, 7 дн)"]
    if r and r["n"]:
        out.append(f"входов {r['n']} · MC входа медиана <b>{float(r['mc'] or 0):.0f} SOL</b> (окупается: до ~35 при "
                   f"покупке дева 0.5–2 SOL, до ~57 при 10+)"
                   + (f" · через {float(r['blocks']):.0f} блок. после запуска" if r["blocks"] is not None else "")
                   + (f" · узнали о запуске → решили за {float(r['det']):.0f} мс" if r["det"] is not None else "")
                   + f" · закрыто {r['closed']}, в плюсе {r['w']}")
    if skips:
        out.append(f"не купили — цена уже убежала: {skips}")
    # по цене входа: где плюс — сколько дошли до PumpSwap и итог закрытых (тень, $ по ставке)
    sol_usd = float(((await db.kv_get("sol_usd", {})) or {}).get("price") or 150)
    rows = await db.fetch(
        """SELECT CASE WHEN p.entry_mc < 35 THEN 1 WHEN p.entry_mc < 45 THEN 2 WHEN p.entry_mc < 57 THEN 3
                       WHEN p.entry_mc < 80 THEN 4 ELSE 5 END b, count(*) n,
                  count(*) FILTER (WHERE t.migrated_at IS NOT NULL) mig,
                  COALESCE(sum(p.pnl_sol) FILTER (WHERE p.closed_at IS NOT NULL), 0) s
           FROM real_positions p LEFT JOIN tokens t ON t.mint = p.mint
           WHERE p.kind = 'newdev' AND p.opened_at > now() - interval '7 days' GROUP BY 1 ORDER BY 1""")
    if rows:
        names = {1: "<35", 2: "35–45", 3: "45–57", 4: "57–80", 5: "80+"}
        table = [f"{'MC входа':<9}{'сд':>5}{'PumpSwap':>9}{'итог':>9}"]
        for r in rows:
            table.append(f"{names[r['b']]:<9}{r['n']:>5}{r['mig']:>9}{_usd(float(r['s']) * sol_usd):>9}")
        out.append("<pre>" + "\n".join(table) + "</pre>")
    return out


async def view(db: Any, cfg: dict[str, Any]) -> tuple[str, Kb]:
    sol_usd = float(((await db.kv_get("sol_usd", {})) or {}).get("price") or 150)
    lines = ["📊 <b>Реальные цифры</b> — только реальный бот, $ по твоей ставке", ""]
    shadow = await mode_lines(db, cfg, "shadow", sol_usd, shadow_kinds())
    live = await mode_lines(db, cfg, "live", sol_usd)
    lines += shadow or ["👻 Тень: сделок за 7 дней нет"]
    if live:
        lines += ["", *live]
    fol = await follow_lines(db, sol_usd)
    if fol:
        lines += ["", *fol]
    nd = await newdev_lines(db) if "newdev" not in S.RETIRED else []
    if nd:
        lines += ["", *nd]
    if S.RETIRED:
        lines += ["", RETIRED_NOTE]
    opened = await db.fetchval("SELECT count(*) FROM real_positions WHERE closed_at IS NULL")
    lines += ["", f"Открыто сейчас: {opened}"]
    tune = await db.kv_get("real:autotune_block") or {}
    if tune.get("kinds") or tune.get("sources"):
        lines.append(f"🤖 Автонастройка не пускает на деньги: методов {len(tune.get('kinds') or {})}, источников "
                     f"{len(tune.get('sources') or {})} (тень их мерит) — почему: /autotune")
    if S.get(cfg, "real_only_follow"):
        lines.append("Режим «🎯 На деньги — только выбранные мной кошельки»: методы меряются в тени, без денег.")
    kb: Kb = [[{"text": "🔄 Обновить", "callback": "menu:pnl"}, {"text": "👛 Мои кошельки", "callback": "rl:mine"}],
              [{"text": "💵 Реальный бот", "callback": "menu:real"}]]
    return "\n".join(lines), kb


async def daily_block(db: Any, cfg: dict[str, Any]) -> list[str]:
    """Блок «Реальные цифры» для разбора дня."""
    sol_usd = float(((await db.kv_get("sol_usd", {})) or {}).get("price") or 150)
    out = await mode_lines(db, cfg, "shadow", sol_usd, shadow_kinds())
    live = await mode_lines(db, cfg, "live", sol_usd)
    if live:
        out += live
    return ["", "📊 <b>Реальные цифры</b> (реальный бот, $ по ставке)", *(out or ["сделок реального бота за 7 дней нет"])]
