"""/excel и кнопка «📊 Excel» в /real: вся отчётность одним файлом Excel — по листу на раздел, без текста-стены.

Листы:
  Сводка                — кошельки реального бота (режим, баланс, итог), бот по методам, бумага за 7 дней ($10 на сделку)
  Бот по дням           — реальный бот: день × режим × метод
  Сделки бота           — каждая сделка реального бота: чей сигнал, вход, выход, итог
  Бот против бумаги     — каждая сделка бота рядом с той же сделкой бумаги (тот же сигнал, то же правило)
  Бумага без бота       — сделки бумаги A / Y, которых нет у бота, и почему он не вошёл
  Пропуски бота         — все пропуски сигналов с причиной (с 18.11)
  Бумага по дням        — день × портфель: сделок, в плюс, итог при $10 на сделку (честные входы, без ⚠)
  Сделки бумаги         — все сделки бумаги за 7 дней
  Smart-кошельки        — кто даёт сделки бумаге A: сделок, в плюс, итог при $10, статус в списке
  Разведка              — кандидаты: откуда, статус, итог испытания в H

Время — по Батуми (часовой пояс дневного сброса бумаги), доллары — по текущему курсу SOL, итог бумаги «при $10» —
процент сделки × $10 (как в разборах): так портфели с разным размером позиций сравниваются честно.
"""
from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from ..real.report import EXIT_NAMES, KINDS, MODE_SHORT, paper_pairs, skip_text, sol_price

PCT, USD, SOL, NUM, DT = "0.0%", '#,##0.00 "$"', "0.0000", "0", "dd.mm.yyyy hh:mm:ss"
HEAD = Font(bold=True, color="FFFFFF")
HEAD_FILL = PatternFill("solid", fgColor="305496")
TITLE = Font(bold=True, size=13)
GREEN, RED = Font(color="008000"), Font(color="C00000")
PAPER_DAYS = 7
# 19.11: бумага берёт все сигналы — H и S по ~450 сделок в сутки; файл рос, и Telegram не дожидался загрузки
# (Gateway Timeout). Построчно их сделки — за 2 дня, остальные портфели — за PAPER_DAYS
BULK, BULK_DAYS = ("H", "S"), 2
REAL_DAYS = 30


def _tz(cfg: dict[str, Any]) -> ZoneInfo:
    try:
        return ZoneInfo(str(cfg.get("paper_reset_tz") or "Asia/Tbilisi"))
    except Exception:  # noqa: BLE001
        return ZoneInfo("Asia/Tbilisi")


class Sheet:
    """Лист: таблицы с заголовком, форматом колонок, закреплённой шапкой и шириной по содержимому."""

    def __init__(self, wb: Workbook, title: str, tz: ZoneInfo, first: bool = False):
        self.ws = wb.active if first else wb.create_sheet()
        self.ws.title = title
        self.tz = tz
        self.row = 1
        self.widths: dict[int, int] = {}

    def title(self, text: str) -> None:
        self.ws.cell(self.row, 1, text).font = TITLE
        self.row += 1

    def note(self, text: str) -> None:
        self.ws.cell(self.row, 1, text).font = Font(italic=True, color="666666")
        self.row += 1

    def table(self, cols: list[tuple[str, str | None]], rows: list[list[Any]], freeze: bool = False,
              color_col: int | None = None) -> None:
        """cols — (заголовок, формат); color_col — номер колонки (с 0), по знаку которой строка зелёная/красная."""
        for j, (name, _fmt) in enumerate(cols, 1):
            c = self.ws.cell(self.row, j, name)
            c.font, c.fill = HEAD, HEAD_FILL
            c.alignment = Alignment(wrap_text=True, vertical="top")
            self._width(j, name, head=True)
        if freeze:
            self.ws.freeze_panes = self.ws.cell(self.row + 1, 1)
        self.row += 1
        for values in rows:
            for j, ((_name, fmt), v) in enumerate(zip(cols, values), 1):
                if isinstance(v, datetime):
                    v = v.astimezone(self.tz).replace(tzinfo=None)
                c = self.ws.cell(self.row, j, v)
                if fmt and v is not None and not isinstance(v, str):
                    c.number_format = fmt
                self._width(j, v)
            if color_col is not None and isinstance(values[color_col], (int, float)) and values[color_col]:
                font = GREEN if values[color_col] > 0 else RED
                for j in range(1, len(cols) + 1):
                    self.ws.cell(self.row, j).font = font
            self.row += 1
        if not rows:
            self.ws.cell(self.row, 1, "— пусто —").font = Font(italic=True, color="999999")
            self.row += 1
        self.row += 1

    def _width(self, j: int, v: Any, head: bool = False) -> None:
        if v is None:
            return
        n = 19 if isinstance(v, datetime) else min(len(str(v)), 24 if head else 60) + 2
        if n > self.widths.get(j, 8):
            self.widths[j] = n
            self.ws.column_dimensions[get_column_letter(j)].width = n


def _f(v: Any) -> float | None:
    return None if v is None else float(v)


def _usd(sol: Any, sol_usd: float) -> float | None:
    return None if sol is None else round(float(sol) * sol_usd, 2)


async def build_xlsx(db: Any, cfg: dict[str, Any]) -> tuple[bytes, str]:
    """(файл .xlsx, подпись)."""
    tz = _tz(cfg)
    sol_usd = await sol_price(db)
    wb = Workbook()
    now = datetime.now(timezone.utc)

    # ------------------------------------------------------------------ данные
    labels = {r["address"]: r["label"] for r in await db.fetch(
        "SELECT address, label FROM watchlist WHERE label IS NOT NULL "
        "UNION ALL SELECT address, label FROM traders WHERE label IS NOT NULL")}
    wallets = {r["id"]: r for r in await db.fetch("SELECT id, name, pubkey, settings, archived FROM real_wallets")}
    real = await db.fetch(
        "SELECT * FROM real_positions WHERE opened_at > now() - make_interval(days => $1) ORDER BY opened_at",
        REAL_DAYS)
    paper_rows = await db.fetch(
        """SELECT * FROM paper_positions WHERE opened_at > now() - make_interval(days => $1)
             AND exit_reason IS DISTINCT FROM 'reset' ORDER BY opened_at""", PAPER_DAYS)

    def wname(wid: Any) -> str:
        w = wallets.get(wid)
        return str(w["name"] or f"№{wid}") if w else ("общий" if wid is None else f"№{wid}")

    # ------------------------------------------------------------------ Сводка
    s = Sheet(wb, "Сводка", tz, first=True)
    s.title(f"Отчёт на {now.astimezone(tz):%d.%m.%Y %H:%M} (Батуми) · курс SOL ${sol_usd:.2f}")
    s.note("Каждый раздел — отдельный лист внизу. Итог бумаги «при $10» — процент сделки × $10.")
    s.title("Кошельки реального бота")
    rows = []
    for wid, w in sorted(wallets.items()):
        if w["archived"]:
            continue
        st = await db.kv_get(f"real:status:{wid}") or {}
        mine = [r for r in real if r["wallet_id"] == wid and r["closed_at"] is not None]
        live = [r for r in mine if r["mode"] == "live"]
        shadow = [r for r in mine if r["mode"] == "shadow"]
        bal = st.get("balance_sol")
        rows.append([w["name"], w["pubkey"], MODE_SHORT.get(st.get("mode") or (w["settings"] or {}).get("real_mode"),
                                                             st.get("mode") or ""),
                     bal, _usd(bal, sol_usd), len(live), sum(1 for r in live if (r["pnl_sol"] or 0) > 0),
                     _usd(sum(float(r["pnl_sol"] or 0) for r in live), sol_usd), len(shadow),
                     _usd(sum(float(r["pnl_sol"] or 0) for r in shadow), sol_usd)])
    s.table([("Кошелёк", None), ("Адрес", None), ("Режим", None), ("Баланс, SOL", SOL), ("Баланс, $", USD),
             ("Бой: сделок", NUM), ("Бой: в плюс", NUM), ("Бой: итог, $", USD), ("Тень: сделок", NUM),
             ("Тень: итог, $", USD)], rows)

    s.title(f"Реальный бот по методам (за {REAL_DAYS} дн.)")
    groups: dict[tuple[str, str], list[Any]] = {}
    for r in real:
        if r["closed_at"] is not None:
            groups.setdefault((r["mode"], r["kind"]), []).append(r)
    rows = []
    for (mode, kind), rs in sorted(groups.items()):
        n = len(rs)
        rows.append([MODE_SHORT.get(mode, mode), KINDS.get(kind, kind), n,
                     sum(1 for r in rs if (r["pnl_sol"] or 0) > 0) / n,
                     sum(float(r["pnl_pct"] or 0) for r in rs) / n,
                     _usd(sum(float(r["pnl_sol"] or 0) for r in rs), sol_usd)])
    s.table([("Режим", None), ("Метод", None), ("Сделок", NUM), ("В плюс", PCT), ("В среднем на сделку", PCT),
             ("Итог, $", USD)], rows, color_col=5)

    s.title(f"Бумага за {PAPER_DAYS} дн. — при $10 на сделку (честные входы, без ⚠)")
    by_port: dict[str, list[Any]] = {}
    for p in paper_rows:
        if p["closed_at"] is not None and not p["suspect"] and p["pnl_pct"] is not None:
            by_port.setdefault(p["portfolio"], []).append(p)
    rows = []
    for port, ps in sorted(by_port.items()):
        n = len(ps)
        rows.append([port, n, sum(1 for p in ps if p["pnl_pct"] > 0) / n, sum(float(p["pnl_pct"]) for p in ps) / n,
                     round(sum(float(p["pnl_pct"]) for p in ps) * 10, 2)])
    s.table([("Портфель", None), ("Сделок", NUM), ("В плюс", PCT), ("В среднем на сделку", PCT),
             ("Итог при $10, $", USD)], rows, color_col=4)

    # ------------------------------------------------------------------ Бот по дням
    s = Sheet(wb, "Бот по дням", tz)
    days: dict[tuple[str, str, str], list[Any]] = {}
    for r in real:
        if r["closed_at"] is not None:
            days.setdefault((r["closed_at"].astimezone(tz).strftime("%Y-%m-%d"), r["mode"], r["kind"]), []).append(r)
    rows = []
    for (day, mode, kind), rs in sorted(days.items()):
        rows.append([datetime.strptime(day, "%Y-%m-%d").strftime("%d.%m.%Y"), MODE_SHORT.get(mode, mode),
                     KINDS.get(kind, kind), len(rs), sum(1 for r in rs if (r["pnl_sol"] or 0) > 0),
                     _usd(sum(float(r["pnl_sol"] or 0) for r in rs), sol_usd)])
    s.table([("День", None), ("Режим", None), ("Метод", None), ("Сделок", NUM), ("В плюс", NUM), ("Итог, $", USD)],
            rows, freeze=True, color_col=5)

    # ------------------------------------------------------------------ Сделки бота
    s = Sheet(wb, "Сделки бота", tz)
    rows = []
    for r in real:
        ex = r["extra"] or {}
        rows.append([wname(r["wallet_id"]), MODE_SHORT.get(r["mode"], r["mode"]), KINDS.get(r["kind"], r["kind"]),
                     r["symbol"] or "", r["mint"], labels.get(r["trader"]) or "", r["trader"] or "", r["venue"] or "",
                     r["opened_at"], r["closed_at"], ex.get("blocks_after"), _f(r["trigger_mc"]), _f(r["entry_mc"]),
                     _f(r["exit_mc"]), _f(r["cost_sol"]), _f(r["proceeds_sol"]), _f(r["pnl_sol"]),
                     _usd(r["pnl_sol"], sol_usd), _f(r["pnl_pct"]),
                     EXIT_NAMES.get(r["exit_reason"], r["exit_reason"] or ("открыта" if r["closed_at"] is None else "")),
                     ex.get("note") or "", r["buy_sig"] or "", r["sell_sig"] or ""])
    s.table([("Кошелёк", None), ("Режим", None), ("Метод", None), ("Монета", None), ("Адрес монеты", None),
             ("Чей сигнал (метка)", None), ("Чей сигнал (адрес)", None), ("Площадка", None), ("Открыта", DT),
             ("Закрыта", DT), ("Блоков после сигнала", NUM), ("MC сигнала, SOL", "0.0"), ("MC входа, SOL", "0.0"),
             ("MC выхода, SOL", "0.0"), ("Потрачено, SOL", SOL), ("Получено, SOL", SOL), ("Итог, SOL", SOL),
             ("Итог, $", USD), ("Итог, %", PCT), ("Причина выхода", None), ("Примечание", None),
             ("Подпись покупки", None), ("Подпись продажи", None)], rows, freeze=True, color_col=17)

    # ------------------------------------------------------------------ Бот против бумаги
    s = Sheet(wb, "Бот против бумаги", tz)
    closed = [r for r in real if r["closed_at"] is not None]
    rows = []
    for r, port, q in await paper_pairs(db, closed, REAL_DAYS):
        prem = (float(r["entry_mc"]) / float(q["entry_mc"]) - 1) if q is not None and r["entry_mc"] and q["entry_mc"] \
            else None
        rows.append([r["closed_at"], wname(r["wallet_id"]), MODE_SHORT.get(r["mode"], r["mode"]),
                     KINDS.get(r["kind"], r["kind"]), r["symbol"] or "", _f(r["pnl_pct"]), port or "—",
                     _f(q["pnl_pct"]) if q is not None else None, prem, (r["extra"] or {}).get("blocks_after"),
                     "нет сделки у бумаги" if q is None else ""])
    s.table([("Закрыта", DT), ("Кошелёк", None), ("Режим", None), ("Метод", None), ("Монета", None), ("Бот, %", PCT),
             ("Портфель бумаги", None), ("Бумага, %", PCT), ("Вход бота дороже бумаги на", PCT),
             ("Блоков после сигнала (бумага: 2)", NUM), ("Примечание", None)], rows, freeze=True, color_col=5)

    # ------------------------------------------------------------------ Бумага без бота
    s = Sheet(wb, "Бумага без бота", tz)
    since = await db.fetchval("SELECT min(opened_at) FROM real_positions")
    rows = []
    if since is not None:
        for m in await db.fetch(
                """SELECT pp.portfolio, pp.symbol, pp.mint, pp.source_wallet, pp.opened_at, pp.pnl_pct,
                          (SELECT s.mode || '|' || s.kind || '|' || s.why
                           FROM real_skips s WHERE s.mint = pp.mint
                             AND s.at BETWEEN pp.opened_at - interval '2 minutes' AND pp.opened_at + interval '2 minutes'
                           ORDER BY (s.mode = 'live') DESC, s.at DESC LIMIT 1) why
                   FROM paper_positions pp WHERE pp.portfolio IN ('A', 'Y') AND pp.opened_at >= $1
                     AND pp.exit_reason IS DISTINCT FROM 'reset' AND NOT pp.suspect
                     AND NOT EXISTS (SELECT 1 FROM real_positions r WHERE r.alert_id = pp.alert_id AND r.mint = pp.mint)
                   ORDER BY pp.opened_at""", since):
            rows.append([m["opened_at"], m["portfolio"], m["symbol"] or "", m["mint"],
                         labels.get(m["source_wallet"]) or m["source_wallet"] or "", _f(m["pnl_pct"]),
                         skip_text(m["why"]) if m["why"] else
                         "причина не записана (сигнал до бота не дошёл или это было до 18.11)"])
    s.table([("Бумага вошла", DT), ("Портфель", None), ("Монета", None), ("Адрес монеты", None), ("Чей сигнал", None),
             ("Итог бумаги, %", PCT), ("Почему бот не вошёл", None)], rows, freeze=True, color_col=5)

    # ------------------------------------------------------------------ Пропуски бота
    s = Sheet(wb, "Пропуски бота", tz)
    skips = await db.fetch("SELECT * FROM real_skips WHERE at > now() - interval '14 days' ORDER BY at")
    counts: dict[tuple[str, str, str], int] = {}
    for k in skips:
        key = (MODE_SHORT.get(k["mode"], k["mode"]), KINDS.get(k["kind"], k["kind"]), k["why"])
        counts[key] = counts.get(key, 0) + 1
    s.title("Сколько раз и почему")
    s.table([("Режим", None), ("Метод", None), ("Причина", None), ("Раз", NUM)],
            [[*k, v] for k, v in sorted(counts.items(), key=lambda kv: -kv[1])])
    s.title("Каждый пропуск")
    s.table([("Время", DT), ("Кошелёк", None), ("Режим", None), ("Метод", None), ("Монета (адрес)", None),
             ("Причина", None)],
            [[k["at"], wname(k["wallet_id"]), MODE_SHORT.get(k["mode"], k["mode"]), KINDS.get(k["kind"], k["kind"]),
              k["mint"], k["why"]] for k in skips])

    # ------------------------------------------------------------------ Бумага по дням
    s = Sheet(wb, "Бумага по дням", tz)
    pdays: dict[tuple[str, str], list[float]] = {}
    for p in await db.fetch(
            """SELECT portfolio, opened_at, pnl_pct FROM paper_positions WHERE opened_at > now() - interval '30 days'
                 AND closed_at IS NOT NULL AND NOT suspect AND pnl_pct IS NOT NULL
                 AND exit_reason IS DISTINCT FROM 'reset'"""):
        pdays.setdefault((p["opened_at"].astimezone(tz).strftime("%Y-%m-%d"), p["portfolio"]), []).append(
            float(p["pnl_pct"]))
    rows = [[datetime.strptime(day, "%Y-%m-%d").strftime("%d.%m.%Y"), port, len(v), sum(1 for x in v if x > 0) / len(v),
             round(sum(v) * 10, 2)] for (day, port), v in sorted(pdays.items(), reverse=True)]
    s.table([("День", None), ("Портфель", None), ("Сделок", NUM), ("В плюс", PCT), ("Итог при $10, $", USD)], rows,
            freeze=True, color_col=4)

    # ------------------------------------------------------------------ Сделки бумаги
    s = Sheet(wb, "Сделки бумаги", tz)
    rows = []
    bulk_since = now - timedelta(days=BULK_DAYS)
    for p in paper_rows:
        if p["portfolio"] in BULK and p["opened_at"] < bulk_since:
            continue      # испытание и копии — сотни сделок в сутки: построчно 2 дня (по дням — лист «Бумага по дням»)
        pct = _f(p["pnl_pct"])
        rows.append([p["portfolio"], p["symbol"] or "", p["mint"], labels.get(p["source_wallet"]) or "",
                     p["source_wallet"] or "", p["opened_at"], p["closed_at"], _f(p["entry_mc"]), _f(p["max_mc"]),
                     pct, round(pct * 10, 2) if pct is not None else None, p["exit_reason"] or "открыта",
                     "⚠ не в итоге" if p["suspect"] else ""])
    s.table([("Портфель", None), ("Монета", None), ("Адрес монеты", None), ("Кошелёк (метка)", None),
             ("Кошелёк (адрес)", None), ("Открыта", DT), ("Закрыта", DT), ("MC входа, SOL", "0.0"),
             ("MC макс., SOL", "0.0"), ("Итог, %", PCT), ("Итог при $10, $", USD), ("Причина выхода", None),
             ("Проверка", None)], rows, freeze=True, color_col=9)

    # ------------------------------------------------------------------ Smart-кошельки
    s = Sheet(wb, "Smart-кошельки", tz)
    rows = []
    for r in await db.fetch(
            """SELECT p.source_wallet w, count(*) n, count(*) FILTER (WHERE p.pnl_pct > 0) wins,
                      sum(p.pnl_pct) s, max(p.opened_at) last, max(t.label) label, bool_or(t.pinned) pinned,
                      bool_or(t.manual) manual, bool_or(t.active) active, max(sc.status) scout
               FROM paper_positions p LEFT JOIN traders t ON t.address = p.source_wallet
                    LEFT JOIN scout sc ON sc.address = p.source_wallet
               WHERE p.portfolio = 'A' AND p.source_wallet IS NOT NULL AND p.closed_at IS NOT NULL AND NOT p.suspect
                 AND p.opened_at > now() - make_interval(days => $1) AND p.exit_reason IS DISTINCT FROM 'reset'
               GROUP BY 1 ORDER BY sum(p.pnl_pct) DESC""", PAPER_DAYS):
        status = ("в списке" if r["active"] else "снят") + (" · разведка" if r["scout"] else "") + \
            (" · вручную" if r["manual"] else "")
        rows.append([r["label"] or "", r["w"], int(r["n"]), int(r["wins"]) / int(r["n"]),
                     round(float(r["s"] or 0) * 10, 2), r["last"], status])
    s.title(f"Кто даёт сделки бумаге A за {PAPER_DAYS} дн. (итог при $10 на сделку)")
    s.table([("Метка", None), ("Адрес", None), ("Сделок", NUM), ("В плюс", PCT), ("Итог при $10, $", USD),
             ("Последняя сделка", DT), ("Статус", None)], rows, freeze=True, color_col=4)

    # ------------------------------------------------------------------ Разведка
    s = Sheet(wb, "Разведка", tz)
    rows = []
    for r in await db.fetch(
            """SELECT s.*, (SELECT count(*) FROM paper_positions p WHERE p.source_wallet = s.address
                              AND p.portfolio = 'S' AND p.closed_at IS NOT NULL AND NOT p.suspect
                              AND s.started_at IS NOT NULL AND p.opened_at >= s.started_at) n,
                           (SELECT count(*) FILTER (WHERE p.pnl_sol > 0) FROM paper_positions p
                            WHERE p.source_wallet = s.address AND p.portfolio = 'S' AND p.closed_at IS NOT NULL
                              AND NOT p.suspect AND s.started_at IS NOT NULL AND p.opened_at >= s.started_at) wins,
                           (SELECT sum(p.pnl_pct) FROM paper_positions p WHERE p.source_wallet = s.address
                              AND p.portfolio = 'S' AND p.closed_at IS NOT NULL AND NOT p.suspect
                              AND s.started_at IS NOT NULL AND p.opened_at >= s.started_at) s_pct
               FROM scout s ORDER BY s.found_at DESC"""):
        rows.append([r["address"], r["status"], r["source"] or "", r["reason"] or "", r["found_at"], r["started_at"],
                     int(r["n"] or 0), int(r["wins"] or 0), round(float(r["s_pct"] or 0) * 10, 2) if r["n"] else None,
                     r["decided_at"], r["decision"] or "", r["cooldown_until"]])
    s.table([("Адрес", None), ("Статус", None), ("Откуда", None), ("Почему попал", None), ("Найден", DT),
             ("Начало испытания", DT), ("Копий в S (как бот)", NUM), ("В плюсе", NUM), ("Итог копий при $10, $", USD),
             ("Решение", DT), ("Что решено", None), ("Вернуть не раньше", DT)], rows, freeze=True, color_col=8)

    buf = io.BytesIO()
    wb.save(buf)
    n_real = sum(1 for r in real if r["closed_at"] is not None)
    caption = (f"📊 Отчёт {now.astimezone(tz):%d.%m %H:%M}: реальный бот — {n_real} сделок за {REAL_DAYS} дн., "
               f"бумага — {len(paper_rows)} сделок за {PAPER_DAYS} дн., пропусков {len(skips)}. "
               "Листы внизу: сводка, бот, бот против бумаги, пропуски, бумага, smart-кошельки, разведка.")
    return buf.getvalue(), caption
