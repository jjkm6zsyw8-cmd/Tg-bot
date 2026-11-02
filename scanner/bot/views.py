"""Тексты ответов на команды: /token, /dev, /trader, /stats, /health, /watchlist."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from ..alerts.format import CONF_NAMES, age, esc, mc_usd, pct
from ..analytics.copytrade import TIER_NAMES, compute_trader, linked_to_dev, qualifies
from ..analytics.curve import Costs
from ..analytics.filters import check, compact_line
from ..analytics.loader import cluster_profile, load_state
from ..analytics.scoring import REASONS, Snapshot, evaluate_dev
from ..config import Settings
from ..db.pool import Database
from ..solana import short


async def sol_usd(db: Database) -> float:
    return float((await db.kv_get("sol_usd", {}) or {}).get("price", 150))


def costs_of(settings: Settings) -> Costs:
    return Costs.from_config(settings.costs())


async def token_view(db: Database, settings: Settings, mint: str, ext: Any = None) -> str:
    cfg = settings.effective()
    st = await load_state(db, mint, cfg)
    if not st:
        return "Монета не найдена в базе (бот видит монеты с момента запуска приёмника)."
    price = await sol_usd(db)
    lines = [f"🔎 <b>${esc(st.symbol or '?')}</b> {esc(st.name)}",
             f"MC {mc_usd(st.mc, price)} · пик {mc_usd(st.peak_mc, price)} · {age(time.time() - st.created_ts)} от запуска",
             f"Деплоер <code>{st.deployer}</code>" + (f" · владелец {short(st.creator)}" if st.creator and st.creator != st.deployer else "")]
    prof = None
    row = None
    if st.cluster_id:
        prof, row = await cluster_profile(db, st.cluster_id, cfg)
        lines.append(f"Кластер #{st.cluster_id}: {len(st.cluster_wallets())} кошельков, "
                     f"{prof.launches} запусков, повторяемость {pct(prof.repeatability, False)}"
                     + (" · ⛔ чёрный список" if row and row["blacklisted"] else ""))
        for w, l in list(st.cluster.items())[:8]:
            lines.append(f"  · {short(w)} — {esc(l.reason)} ({CONF_NAMES[l.confidence]})")
    funders = await db.fetch(
        """SELECT w.address, w.funded_by, w.funded_sol FROM wallets w
           WHERE w.address = ANY($1) AND w.funded_by IS NOT NULL LIMIT 10""", list(st.holders())[:200])
    if funders:
        lines.append("Финансирование холдеров:")
        for f in funders[:5]:
            lines.append(f"  · {short(f['address'])} ← {short(f['funded_by'])} ({f['funded_sol'] or 0:.2f} SOL)")
    metrics = st.metrics()
    res = check(metrics, settings.filters(), "smart", bool(row and row["blacklisted"]))
    lines.append(compact_line(metrics, res.violations))
    lines.append(f"Фильтры: {'✅ пройдены' if res.passed else '❌ ' + ', '.join(res.violations)}")
    if prof:
        snap = Snapshot(mc=st.mc, age_s=time.time() - st.created_ts,
                        cluster_sold=st.big_sell_ts is not None or any(
                            not t.is_buy and t.trader == st.deployer for t in st.trades),
                        cluster_share=st.share(st.cluster_wallets()), link_confidence="high",
                        blacklisted=bool(row and row["blacklisted"]))
        d = evaluate_dev(prof, snap, cfg, costs_of(settings))
        if d.potential:
            lines.append(f"Потенциал x{d.potential:.2f}, чистыми {pct(d.net)} · вход до MC "
                         f"{mc_usd(d.entry_max_mc, price)} · цель {mc_usd(d.target_mc, price)}")
        lines.append("Решение: " + ("алерт " + (d.level or "") if d.alert else REASONS.get(d.reason, d.reason)))
    if st.big_sell_ts:
        lines.append(f"⚠️ Крупная продажа кластера через {age(st.big_sell_ts - st.created_ts)}, MC дампа "
                     f"{mc_usd(st.dump_mc, price)}")
    lines += await external_token_block(ext, mint, metrics.get("top10"))
    lines.append(f"\n<code>{mint}</code>")
    return "\n".join(lines)


async def external_token_block(ext: Any, mint: str, onchain_top10: float | None) -> list[str]:
    """Данные внешних источников с указанием источника каждого значения."""
    if ext is None:
        return []
    import asyncio
    chk, pair = await asyncio.gather(
        ext.token_checks(mint, onchain={"top10": onchain_top10}, timeout=ext.command_timeout),
        ext.dex_pair(mint, timeout=ext.command_timeout))
    out = ["", "<b>Внешние источники</b>"]
    rc = chk.facts.get("rugcheck")
    if rc:
        auth = "отозваны" if not rc.get("mint_authority") and not rc.get("freeze_authority") else "⚠️ НЕ отозваны"
        out.append(f"RugCheck: mint/freeze {auth}, риск {rc.get('score_normalised') or 0:.0f}, "
                   f"топ-10 {pct(rc.get('top10_pct'), False)}, инсайдеров в топе {rc.get('insiders_in_top', 0)}"
                   + (f", опасно: {esc(', '.join(rc['danger_risks'][:3]))}" if rc.get("danger_risks") else ""))
    gm, gh = chk.facts.get("gmgn"), chk.facts.get("gmgn_holders")
    if gm:
        out.append(f"GMGN: rug {pct(gm.get('rug_ratio'), False)}, топ-10 {pct(gm.get('top10_rate'), False)}, "
                   f"снайперов {gm.get('sniper_count') or 0}")
    if gh:
        out.append(f"GMGN: смарт-мани среди холдеров {gh.get('smart', 0)}, KOL {gh.get('kol', 0)}")
    if pair:
        out.append(f"DexScreener: {esc(pair.get('dex'))}, цена ${pair.get('price_usd') or 0:.8f}, "
                   f"MC {mc_usd_plain(pair.get('market_cap_usd'))}, ликвидность {mc_usd_plain(pair.get('liquidity_usd'))}, "
                   f"5 мин: покупок {pair.get('buys_m5')} / продаж {pair.get('sells_m5')}")
    if "discrepancy" in chk.facts:
        d = chk.facts["discrepancy"].get("top10") or {}
        out.append(f"Расхождение топ-10: ончейн {pct(d.get('onchain'), False)} vs внешнее "
                   f"{pct(d.get('external'), False)} — используется ончейн")
    if chk.block:
        out.append("⛔ Внешние фильтры: " + esc("; ".join(chk.block)))
    if chk.missing:
        out.append("⏳ нет данных: " + ", ".join(chk.missing))
    return out if len(out) > 2 else []


def mc_usd_plain(v: float | None) -> str:
    from ..alerts.format import usd
    return usd(v)


async def resolve_cluster(db: Database, ref: str) -> int | None:
    if ref.isdigit():
        return int(ref)
    row = await db.fetchrow(
        "SELECT cluster_id FROM cluster_links WHERE wallet=$1 AND status IN ('auto','confirmed') LIMIT 1", ref)
    if row:
        return row["cluster_id"]
    row = await db.fetchrow("SELECT cluster_id FROM tokens WHERE mint=$1 OR deployer=$1 ORDER BY created_at DESC LIMIT 1",
                            ref)
    return row["cluster_id"] if row and row["cluster_id"] else None


async def dev_view(db: Database, settings: Settings, ref: str, ext: Any = None) -> str:
    cfg = settings.effective()
    cid = await resolve_cluster(db, ref)
    if not cid:
        return "Кластер не найден. Укажите адрес деплоера, монеты или номер кластера."
    prof, row = await cluster_profile(db, cid, cfg)
    price = await sol_usd(db)
    med = prof.medians
    lines = [
        f"👤 <b>Кластер дева #{cid}</b>" + (" · ⭐ приоритет" if row and row["priority"] else "")
        + (f" · ⛔ {esc(row['blacklist_reason'] or 'чёрный список')}" if row and row["blacklisted"] else ""),
        f"Запусков {prof.launches}, по сценарию «рост → дамп» {prof.pattern_launches}, похожих "
        f"{prof.similar_launches}/{prof.launches}, повторяемость {pct(prof.repeatability, False)}",
        f"Мигрировало {prof.migrated}, мгновенных рагов {prof.instant_rugs}",
        f"Медианы: старт {mc_usd(med.get('initial_mc'), price)}, пик {mc_usd(med.get('peak_mc'), price)}, "
        f"дамп {mc_usd(med.get('dump_mc'), price)} через {age(med['time_to_dump_s']) if med.get('time_to_dump_s') else '—'}",
        f"Продаёт {pct(med.get('sold_share'), False)} позиции, доля на старте {pct(med.get('cluster_share_start'), False)}, "
        f"ранних покупателей {med.get('early_buyers') or '—'}, достижимый профит {pct(med.get('achievable_profit'))}",
    ]
    if row and row["threshold_override"] is not None:
        lines.append(f"Пониженный порог алерта: {pct(row['threshold_override'], False)}")
    links = await db.fetch("SELECT wallet, reason, confidence, status FROM cluster_links WHERE cluster_id=$1 "
                           "AND status <> 'rejected' ORDER BY id LIMIT 12", cid)
    lines.append("Кошельки:")
    for l in links:
        mark = {"confirmed": "✅", "proposed": "❔"}.get(l["status"], "·")
        lines.append(f"  {mark} <code>{l['wallet']}</code> — {esc(l['reason'])} ({CONF_NAMES[l['confidence']]})")
    hist = await db.fetch(
        "SELECT l.*, t.symbol FROM launches l JOIN tokens t ON t.mint=l.mint WHERE l.cluster_id=$1 "
        "ORDER BY l.created_at DESC LIMIT 8", cid)
    if hist:
        lines.append("Последние запуски:")
        for h in hist:
            lines.append(f"  · ${esc(h['symbol'] or '?')} {h['created_at']:%d.%m %H:%M} · пик {mc_usd(h['peak_mc'], price)}"
                         f" · дамп {mc_usd(h['dump_mc'], price)}"
                         + (f" через {age(h['time_to_dump_s'])}" if h["time_to_dump_s"] else "")
                         + f" · профит {pct(h['achievable_profit'])}")
    if ext is not None and ext.gmgn.enabled:
        deployer = await db.fetchval(
            "SELECT wallet FROM cluster_links WHERE cluster_id=$1 AND reason='исходный деплоер' LIMIT 1", cid)
        if deployer:
            import asyncio
            info, created = await asyncio.gather(
                ext.wallet_info(deployer, timeout=ext.command_timeout),
                ext._call(ext.gmgn, ext.gmgn.created_tokens(deployer), ext.command_timeout))
            data = created[1]
            items = (data or {}).get("tokens") or (data or {}).get("list") if isinstance(data, dict) else data
            if isinstance(items, list):
                lines.append(f"GMGN: дев создал токенов {len(items)} (по данным GMGN, вкл. до запуска бота)")
            if info and (info.get("tags") or info.get("twitter")):
                lines.append("Метки деплоера: " + esc(", ".join(info.get("tags") or [])
                                                      + (f" @{info['twitter']}" if info.get("twitter") else ""))
                             + f" ({', '.join(sorted(set(info['sources'].values())))})")
    return "\n".join(lines)


async def trader_view(db: Database, settings: Settings, address: str, ext: Any = None) -> str:
    cfg = settings.effective()
    s7, s30, _ = await compute_trader(db, address, cfg, costs_of(settings))
    linked = await linked_to_dev(db, address)
    ok, fails = qualifies(s7, s30, linked, cfg)
    row = await db.fetchrow("SELECT * FROM traders WHERE address=$1", address)
    price = await sol_usd(db)
    alerts = await db.fetchrow(
        """SELECT count(*) n, avg(o.net_result) avg, count(*) FILTER (WHERE o.net_result > 0) wins
           FROM alerts a JOIN alert_outcomes o ON o.alert_id=a.id WHERE a.trader=$1""", address)
    tier = TIER_NAMES.get(row["tier"]) if row and row["tier"] else "—"
    lines = [
        f"📈 <b>Трейдер</b> {esc(row['label']) if row and row['label'] else ''} <code>{address}</code>",
        f"Уровень: {tier}" + (" · в списке" if row and row["active"] else " · не отслеживается"),
        f"PnL 7 д: {s7.pnl_sol:+.2f} SOL · 30 д: {s30.pnl_sol:+.2f} SOL",
        f"Копируемость (задержка {cfg.get('copy_delay_s', 5)} с): 7 д {pct(s7.copyability)} · 30 д {pct(s30.copyability)} на сделку",
        f"Сделок за 30 д: {s30.trades}, удачных {pct(s30.win_rate, False)}, медианное удержание {age(s30.median_hold_s)}",
        f"Средний MC входа {mc_usd(s30.avg_entry_mc, price)}, входов в первые 2 слота {pct(s30.snipe_share, False)}",
        "Фильтр отбора: " + ("✅ проходит" if ok else "❌ " + "; ".join(fails)),
    ]
    if alerts and alerts["n"]:
        lines.append(f"Алертов по нему: {alerts['n']}, удачных {alerts['wins']}, средний итог {pct(alerts['avg'])}")
    if ext is not None:
        import asyncio
        info, (_ok, be) = await asyncio.gather(
            ext.wallet_info(address, timeout=ext.command_timeout),
            ext._call(ext.birdeye, ext.birdeye.wallet_pnl(address), ext.command_timeout))
        if info:
            srcs = ", ".join(sorted(set(info["sources"].values())))
            if info.get("tags") or info.get("label"):
                lines.append(f"Метки ({srcs}): " + esc(", ".join([*(info.get("tags") or []), info.get("label") or ""]).strip(", ")))
            if info.get("twitter"):
                lines.append(f"Twitter: @{esc(info['twitter'])}")
            if info.get("pnl_30d") is not None:
                lines.append(f"GMGN 30 д: прибыль ${info['pnl_30d']:,.0f}, винрейт {pct(info.get('winrate_30d'), False)}")
            if info.get("is_exchange"):
                lines.append("⚠️ Помечен как биржа — в кластеризацию не входит")
        if be and be.get("total_usd") is not None:
            lines.append(f"Birdeye PnL: ${be['total_usd']:,.0f}")
    return "\n".join(lines)


async def stats_view(db: Database) -> str:
    lines = ["📊 <b>Итог серии</b> (вход с задержкой реакции, выход по цели/экстренному сигналу)"]
    for days in (7, 30):
        rows = await db.fetch(
            """SELECT a.mode, count(*) n, count(*) FILTER (WHERE o.net_result > 0) wins,
                      avg(o.net_result) FILTER (WHERE o.net_result > 0) aw,
                      avg(o.net_result) FILTER (WHERE o.net_result <= 0) al
               FROM alerts a JOIN alert_outcomes o ON o.alert_id = a.id
               WHERE a.created_at > now() - make_interval(days => $1) GROUP BY a.mode""", days)
        lines.append(f"\n<b>{days} дней</b>")
        if not rows:
            lines.append("  нет закрытых алертов")
        for r in rows:
            wr = r["wins"] / r["n"] if r["n"] else 0
            aw, al = r["aw"] or 0.0, -(r["al"] or 0.0)
            lines.append(f"  · {r['mode']}: {r['n']} алертов, удачных {wr:.0%}, +{aw:.0%}/−{al:.0%}, "
                         f"итог серии <b>{pct(wr * aw - (1 - wr) * al)}</b>")
    return "\n".join(lines)


async def health_view(db: Database) -> str:
    ing = await db.kv_get("health:ingest", {}) or {}
    ana = await db.kv_get("health:analytics", {}) or {}
    cov = await db.kv_get("coverage", {}) or {}
    backlog = await db.fetchval("SELECT count(*) FROM outbox WHERE sent_at IS NULL")
    now = datetime.now(timezone.utc)

    def ago(iso: str | None) -> str:
        if not iso:
            return "нет данных"
        return f"{(now - datetime.fromisoformat(iso)).total_seconds():.0f} с назад"

    lines = ["🩺 <b>Состояние</b>", f"Приёмник: обновлено {ago(ing.get('at'))}"]
    for s in ing.get("sources", []):
        state = "🟢" if s["connected"] and s["silence_s"] < 30 else "🔴"
        lines.append(f"  {state} {s['name']}: тишина {s['silence_s']:.0f} с, событий {s['events']}, "
                     f"переподключений {s['reconnects']}, ошибок парсинга {s['parse_errors']}")
    lines.append(f"  очередь слотов {ing.get('slot_queue', 0)}, метаданных {ing.get('meta_queue', 0)}")
    lines.append(f"Аналитика: обновлено {ago(ana.get('at'))}, монет в памяти {ana.get('tokens_in_memory', 0)}, "
                 f"сопровождений {ana.get('following', 0)}, трейдеров {ana.get('tracked_traders', 0)}")
    med, p95 = ana.get("latency_median_s"), ana.get("latency_p95_s")
    lines.append(f"Задержка до алерта: медиана {med:.1f} с, p95 {p95:.1f} с (цель ≤1.5 / ≤3 с)" if med else
                 "Задержка до алерта: ещё нет алертов")
    if ana.get("fee_bps"):
        lines.append(f"Комиссия кривой по реальным сделкам: {ana['fee_bps']:.0f} bps")
    lines.append(f"Курс SOL: ${ana.get('sol_usd', 0):.2f}")
    tr = await db.kv_get("health:trends", {}) or {}
    if tr:
        lines.append(f"Тренды: кандидатов {tr.get('candidates', 0)}, проверено {tr.get('checked', 0)}, "
                     f"алертов {tr.get('alerts', 0)}, на сопровождении {tr.get('tracked', 0)}, ошибок API {tr.get('errors', 0)}")
    if cov.get("parts"):
        lines.append("Покрытие за час: " + "; ".join(cov["parts"]))
    lines.append(f"Очередь сообщений: {backlog}")
    ext_a = await db.kv_get("health:external:analytics", {}) or {}
    ext_b = await db.kv_get("health:external:bot", {}) or {}
    for k, v in ext_b.items():   # запросы команд бота считаются вместе с запросами аналитики
        if k in ext_a:
            for f in ("requests", "cache_hits", "errors_hour", "rate_limited"):
                ext_a[k][f] = (ext_a[k].get(f) or 0) + (v.get(f) or 0)
    if ext_a:
        names = {"dexscreener": "DexScreener", "rugcheck": "RugCheck", "gmgn": "GMGN", "birdeye": "Birdeye",
                 "jupiter": "Jupiter", "solscan": "Solscan"}
        lines.append("\n<b>Внешние источники</b> (статус · остаток лимита · ошибок за час · запросов/из кеша)")
        for k, v in ext_a.items():
            icon = "🟢" if v.get("status") == "ok" else "⚪" if not v.get("enabled") else "🟡"
            lines.append(f"  {icon} {names.get(k, k)}: {esc(v.get('status'))} · {v.get('remaining')} · "
                         f"{v.get('errors_hour', 0)} · {v.get('requests', 0)}/{v.get('cache_hits', 0)}"
                         + (f" · 429: {v['rate_limited']}" if v.get("rate_limited") else ""))
    return "\n".join(lines)


async def watchlist_view(db: Database) -> tuple[str, list[list[dict[str, Any]]]]:
    rows = await db.fetch("SELECT * FROM watchlist ORDER BY kind, created_at DESC LIMIT 40")
    traders = await db.fetch("SELECT address, label, tier, manual FROM traders WHERE active ORDER BY copy_30d DESC NULLS LAST LIMIT 20")
    devs = await db.fetch("SELECT id, label FROM clusters WHERE priority ORDER BY id DESC LIMIT 20")
    names = {"token": "Монеты", "dev": "Девы", "trader": "Трейдеры"}
    lines, kb = ["👁 <b>Отслеживается</b>"], []
    for kind in ("token", "dev", "trader"):
        items = [r for r in rows if r["kind"] == kind]
        if items:
            lines.append(f"\n<b>{names[kind]}</b>")
            for r in items:
                lines.append(f"  · {esc(r['label'] or '')} <code>{r['address']}</code>")
                code = {"token": "k", "dev": "d", "trader": "t"}[kind]
                kb.append([{"text": f"✖ {short(r['address'])}", "callback": f"wl:{code}:{r['address']}"}])
    if devs:
        lines.append("\n<b>Приоритетные кластеры</b>: " + ", ".join(f"#{d['id']}" for d in devs))
    if traders:
        lines.append("\n<b>Трейдеры в списке</b>")
        for t in traders:
            lines.append(f"  · {esc(t['label'] or short(t['address']))} ({TIER_NAMES.get(t['tier'], 'ручной')})")
    return "\n".join(lines), kb[:30]
