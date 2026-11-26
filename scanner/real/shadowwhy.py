"""27.11 (владелец: «Тень с 25.11 — ни одной сделки; Smart, мой дев, крупные копии, смарт-ракеты включены, а по сути
отключены»): почему тень молчит — по шагам цепочки «данные → сигнал → кошелёк бота → вход».

1. Кошельки бота: какой режим выбран (выкл / тень / бой), кем и когда; остановлены ли новые входы; сколько методов
   включено; жив ли исполнитель (статус обновляется раз в ~минуту) и что он сам насчитал в пропусках.
   Выключенный кошелёк молчит без следа: сигналы до него доходят, но он их не берёт и пропуск не пишет.
2. Сигналы выше по цепочке: алерты (smart, дев, тренды) и копии своего списка за период — есть ли что брать.
3. Пропуски бота (real_skips) по причинам и позиции тени за период.
4. Поток сделок за последний час по источникам (PumpPortal, слежка Helius, pumpdata).
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from ..alerts.format import esc
from . import settings as S

STALE_S = 600


def _ago(ts: datetime | None) -> str:
    if ts is None:
        return "никогда"
    s = max(0.0, time.time() - ts.timestamp())
    return f"{s / 60:.0f} мин назад" if s < 5400 else f"{s / 3600:.1f} ч назад" if s < 172800 else f"{s / 86400:.1f} дн назад"


def _at(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        return v
    if isinstance(v, str) and v:
        try:
            return datetime.fromisoformat(v)
        except ValueError:
            return None
    return None


async def shadow_why_text(db: Any, cfg: dict[str, Any], hours: float = 24.0) -> str:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    out = [f"👻 <b>Почему тень молчит</b> — за {hours:g} ч"]
    # 1. кошельки бота
    wallets = await db.fetch("SELECT id, name, settings FROM real_wallets WHERE NOT archived ORDER BY id")
    verdicts: list[str] = []
    if not wallets:
        out.append("Кошельков бота нет — сигналы берёт общая полоса (настройки «общие»).")
    for w in wallets:
        own = dict(w["settings"] or {})
        wcfg = {**cfg, **own}
        want = S.get(wcfg, "real_mode")
        paused = str(own.get("real_paused") or cfg.get("real_paused") or "")
        on = [S.KIND_NAMES.get(k, k) for k in S.active_methods() if S.get(wcfg, S.METHODS[k][1])]
        st = await db.kv_get(f"real:status:{w['id']}") or {}
        st_at = _at(st.get("at"))
        line = (f"• <b>{esc(w['name'])}</b>: режим «{S.MODES.get(want, want)}»"
                + (f" (выбран {_at(own.get('real_mode_at')):%d.%m %H:%M} UTC, {esc(str(own.get('real_mode_by') or ''))})"
                   if _at(own.get("real_mode_at")) else "")
                + f"; методов включено {len(on)}, «мерить всё в тени» {'✅' if S.get(wcfg, 'real_shadow_all') else '⬜'}"
                + (f"; ⛔ входы остановлены: {esc(paused)}" if paused else "")
                + f"; исполнитель: статус {_ago(st_at)}"
                + (f", действует «{S.MODES.get(st.get('mode'), st.get('mode'))}»" if st.get("mode") else "")
                + (f", открыто {len(st.get('open') or [])}" if st else ""))
        out.append(line)
        if st.get("skips"):
            out.append("   пропуски с запуска: " + "; ".join(f"{esc(k)} — {v}" for k, v in list(st["skips"].items())[:6]))
        if want == "off":
            verdicts.append(f"{w['name']}: кошелёк выключен — ни тень, ни бой не берут сигналы, галочки методов не "
                            "действуют. Включи: «💵 Реальный бот» → «👛 Мои кошельки» → кошелёк → «👻 Тень».")
        elif paused:
            verdicts.append(f"{w['name']}: новые входы остановлены ({paused}) — «▶️ Продолжить» на экране кошелька "
                            "или в /real.")
        elif st_at is None or time.time() - st_at.timestamp() > STALE_S:
            verdicts.append(f"{w['name']}: исполнитель не отвечает (статус {_ago(st_at)}) — контейнер bot / analytics "
                            "не запущен или упал: docker compose ps.")
    # 2. сигналы выше по цепочке
    alerts = await db.fetch("SELECT mode, count(*) n FROM alerts WHERE created_at >= $1 GROUP BY 1 ORDER BY 2 DESC",
                            since)
    copies = await db.fetch("SELECT action, count(*) n FROM copy_log WHERE ts >= $1 AND side = 'buy' GROUP BY 1",
                            since)
    out.append("\n<b>Сигналы</b> (алерты по видам): " + (", ".join(f"{r['mode']} {r['n']}" for r in alerts) or "нет")
               + " · копии своего списка: " + (", ".join(f"{r['action']} {r['n']}" for r in copies) or "нет"))
    if not alerts and not copies:
        verdicts.append("Сигналов нет совсем — молчит не тень, а поток данных или анализ (см. «Поток сделок»).")
    # 3. пропуски и позиции
    skips = await db.fetch(
        """SELECT mode, kind, why, count(*) n FROM real_skips WHERE at >= $1 GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 10""",
        since)
    out.append("<b>Пропуски бота</b>: " + ("; ".join(
        f"{'бой' if r['mode'] == 'live' else 'тень'} · {S.KIND_NAMES.get(r['kind'], r['kind'])}: {esc(r['why'])} — "
        f"{r['n']}" for r in skips) or "нет ни одного (до бота сигналы не доходят или кошелёк выключен)"))
    pos = await db.fetchrow(
        """SELECT count(*) FILTER (WHERE opened_at >= $1) n, max(opened_at) last FROM real_positions
           WHERE mode = 'shadow'""", since)
    out.append(f"<b>Позиции тени</b>: за период {int(pos['n'] or 0)}, последняя открыта {_ago(pos['last'])}")
    # 4. поток сделок
    flow = await db.fetch(
        """SELECT coalesce(source, '?') src, count(*) n, max(ts) last FROM trades
           WHERE ts >= now() - interval '1 hour' GROUP BY 1 ORDER BY 2 DESC""")
    out.append("<b>Поток сделок за час</b>: " + (", ".join(
        f"{r['src']} {r['n']} (последняя {_ago(r['last'])})" for r in flow) or "пусто"))
    out.append("\n<b>Вывод</b>: " + ("\n".join(f"⚠️ {esc(v)}" for v in verdicts) if verdicts else
                                     "кошельки включены и живы — смотри причины пропусков выше."))
    return "\n".join(out)
