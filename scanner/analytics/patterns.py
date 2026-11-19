"""Паттерны монет: какие признаки в первые секунды жизни монеты отличают те, на которых бот заработал бы, от слитых.

Зачем (19.11): копии кошельков в честном замере теряют (тень — 183 копии, в среднем −6.7%), «мой дев» — тоже.
Второй подход, помимо кошельков, — решать не «за кем повторять», а «какую монету брать».

Для каждой монеты pump.fun за N дней (своя база: все сделки первых 30 минут на кривой) в момент решения T (30 и 60 с
после создания) считаются признаки — так, как их видит живой бот, тем же классом состояния монеты (TokenState):
покупка дева, снайперы (блок создания), связка (4+ сделки в одном блоке), доля кластера дева и самого дева, продавал
ли дев, покупатели, продажи, объём, рост с создания, откат от пика, топ-10 держателей, ссылки на соцсети, сколько
монет у дева за 7 дней, час. Только то, что известно к моменту T, — ничего из будущего.
Дальше — честная сделка бота: вход через LAG_S после T по худшей цене этих секунд, комиссии туда-обратно FEE, выход —
+25% чистыми, −20% (по цене сделки: проскок вниз считается) или через HOLD_S.
Правила (одно-два условия) ищутся на первых 2/3 монет по времени и проверяются на последней трети: иначе находится
случайность, которая завтра не повторится.

Сервер: docker compose run --rm bot patterns [--days 3] [--at 30,60] [--no-send] — итог печатается и приходит
в Telegram вместе с zip (CSV: признаки и итог сделки по каждой монете).
"""
from __future__ import annotations

import asyncio
import base64
import csv
import io
import logging
import statistics
import zipfile
from array import array
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

from ..solana import TOTAL_SUPPLY
from .state import TokenState, TradeRow

log = logging.getLogger(__name__)

FEE = 0.03            # комиссии туда-обратно при ставке $10: pump.fun с покупки и продажи + плата сети
TP, SL = 0.25, 0.20   # выход: +25% чистыми или −20%
HOLD_S = 900.0        # и не дольше 15 минут
LAG_S = 2.0           # бот покупает через ~4 блока после решения
WINDOW_S = 1800.0     # своя база хранит сделки первых 30 минут монеты
BATCH = 400           # монет на один запрос сделок
MIN_N = 200           # правило — только если на обучении под него попало 200+ монет
TOP_SINGLE = 40       # лучших одиночных условий, из которых собираются пары
TZ = ZoneInfo("Asia/Tbilisi")

FEATURES: list[tuple[str, str]] = [
    ("dev_buy", "покупка дева, SOL"),
    ("snipers", "снайперов (блок создания)"),
    ("sniper_share", "доля снайперов"),
    ("bundle_share", "доля связки (4+ сделки в блоке)"),
    ("cluster_share", "доля кластера дева"),
    ("dev_share", "доля дева"),
    ("dev_sold", "дев продавал (1 — да)"),
    ("buyers", "покупателей"),
    ("sells", "продаж"),
    ("buy_sol", "куплено, SOL"),
    ("sell_ratio", "продано к купленному (SOL)"),
    ("growth", "рост с создания, ×"),
    ("from_peak", "цена от пика, ×"),
    ("top10", "доля топ-10 держателей"),
    ("socials", "ссылок на соцсети"),
    ("dev_coins_7d", "монет у дева за 7 дней"),
    ("hour", "час создания (Батуми)"),
]
SHARES = {"sniper_share", "bundle_share", "cluster_share", "dev_share", "top10"}
NAMES = dict(FEATURES)


def snapshot(st: TokenState, tok: dict[str, Any]) -> dict[str, float]:
    """Признаки монеты сейчас — по сделкам, уже добавленным в состояние (то, что видит живой бот)."""
    dev = st.deployer
    creation = st.creation_mc() or st.initial_mc
    buyers = {t.trader for t in st.trades if t.is_buy and t.trader != dev}
    sells = [t for t in st.trades if not t.is_buy]
    buy_sol = sum(t.sol for t in st.trades if t.is_buy and not (t.trader == dev and t.ts - st.created_ts <= 1.5))
    snipers = st.sniper_wallets()
    holders = sorted((b for b in st.balances.values() if b > 0), reverse=True)
    created = datetime.fromtimestamp(st.created_ts, timezone.utc).astimezone(TZ)
    return {
        "dev_buy": st.dev_buy_sol(),
        "snipers": float(len(snipers)),
        "sniper_share": st.share(snipers),
        "bundle_share": st.share(st.bundle_wallets()),
        "cluster_share": st.share(st.cluster_wallets()),
        "dev_share": st.share({dev}),
        "dev_sold": 1.0 if any(t.trader == dev and not t.is_buy for t in st.trades) else 0.0,
        "buyers": float(len(buyers)),
        "sells": float(len(sells)),
        "buy_sol": buy_sol,
        "sell_ratio": sum(t.sol for t in sells) / buy_sol if buy_sol > 0 else 0.0,
        "growth": st.mc / creation if creation > 0 else 1.0,
        "from_peak": st.mc / st.peak_mc if st.peak_mc > 0 else 1.0,
        "top10": sum(holders[:10]) / TOTAL_SUPPLY,
        "socials": float(sum(1 for k in ("twitter", "telegram", "website") if tok.get(k))),
        "dev_coins_7d": float(tok.get("dev_prev") or 0),
        "hour": float(created.hour),
    }


def simulate(after: list[tuple[float, float]], t_dec: float, mc_now: float) -> tuple[float, str, float]:
    """Сделка бота с момента решения t_dec: вход через LAG_S по худшей цене этих секунд, выход +TP / −SL / через
    HOLD_S. after — (время, MC) сделок монеты после решения. Итог: (чистыми, причина выхода, пик от входа ×)."""
    entry, i = mc_now, 0
    while i < len(after) and after[i][0] <= t_dec + LAG_S:
        entry = max(entry, after[i][1])
        i += 1
    if entry <= 0:
        return -FEE, "нет цены", 1.0
    up, down = entry * (1 + TP) / (1 - FEE), entry * (1 - SL) / (1 - FEE)
    peak = last = entry
    for ts, mc in after[i:]:
        if ts - (t_dec + LAG_S) > HOLD_S:
            break
        last, peak = mc, max(peak, mc)
        if mc >= up:
            return TP, "тейк", peak / entry
        if mc <= down:
            return mc / entry * (1 - FEE) - 1, "стоп", peak / entry
    return last / entry * (1 - FEE) - 1, "время", peak / entry


def study_coin(tok: dict[str, Any], rows: Iterable[Any], at: list[float]) -> list[dict[str, Any]]:
    """Признаки в каждый момент решения и итог сделки бота с него. Пусто — монета не на кривой SOL или без сделок."""
    created = tok["created_at"].timestamp()
    trades = [t for t in (TradeRow.from_record(r) for r in rows) if t.mc > 0 and t.ts - created <= WINDOW_S]
    if not trades or not 20 <= trades[0].mc <= 150:
        return []          # пара не к SOL (USDC, акции): MC в другой валюте — не наша кривая
    st = TokenState(mint=tok["mint"], deployer=tok["deployer"], created_ts=created, created_slot=tok.get("created_slot"),
                    initial_mc=float(tok.get("initial_mc_sol") or 0), initial_buy_sol=float(tok.get("initial_buy_sol") or 0))
    out, j = [], 0
    for at_s in sorted(at):
        while j < len(trades) and trades[j].ts - created <= at_s:
            st.add_trade(trades[j])
            j += 1
        feats = snapshot(st, tok)
        net, why, peak = simulate([(t.ts, t.mc) for t in trades[j:]], created + at_s, st.mc)
        out.append({"at": at_s, **feats, "net": net, "exit": why, "peak": peak})
    return out


@dataclass
class Columns:
    """Монеты одного момента решения — по столбцам (сотни тысяч монет не помещаются словарями в память сервера)."""
    mints: list[str] = field(default_factory=list)
    created: array = field(default_factory=lambda: array("d"))
    feats: dict[str, array] = field(default_factory=lambda: {k: array("d") for k, _ in FEATURES})
    net: array = field(default_factory=lambda: array("d"))
    peak: array = field(default_factory=lambda: array("d"))
    exits: list[str] = field(default_factory=list)

    def add(self, tok: dict[str, Any], r: dict[str, Any]) -> None:
        self.mints.append(tok["mint"])
        self.created.append(tok["created_at"].timestamp())
        for k, _ in FEATURES:
            self.feats[k].append(float(r[k]))
        self.net.append(float(r["net"]))
        self.peak.append(float(r["peak"]))
        self.exits.append(str(r["exit"]))

    def __len__(self) -> int:
        return len(self.net)


async def load_coins(db: Any, since: datetime, until: datetime) -> list[dict[str, Any]]:
    """Монеты, созданные в [since, until), по времени; dev_prev — сколько монет дев запустил за 7 дней до этой."""
    rows = await db.fetch(
        """SELECT * FROM (
               SELECT mint, deployer, created_at, created_slot, initial_mc_sol, initial_buy_sol, twitter, telegram,
                      website, count(*) OVER (PARTITION BY deployer ORDER BY created_at
                                              RANGE BETWEEN interval '7 days' PRECEDING AND CURRENT ROW) - 1 AS dev_prev
               FROM tokens WHERE created_at >= $1::timestamptz - interval '7 days' AND created_at < $2) x
           WHERE created_at >= $1 ORDER BY created_at""", since, until)
    return [dict(r) for r in rows]


async def load_trades(db: Any, mints: list[str]) -> dict[str, list[Any]]:
    rows = await db.fetch(
        """SELECT mint, trader, is_buy, sol_amount, token_amount, mc_sol, slot, ts, signature, new_balance,
                  real_tokens, source, pool
           FROM trades WHERE mint = ANY($1::text[]) AND pool = 'pump' ORDER BY mint, ts, id""", mints)
    out: dict[str, list[Any]] = {}
    for r in rows:
        out.setdefault(r["mint"], []).append(r)
    return out


# ---------------------------------------------------------------------------------------------- разбор
def stats(nets: Iterable[float], exits: Iterable[str] | None = None) -> dict[str, float]:
    xs = list(nets)
    if not xs:
        return {"n": 0, "avg": 0.0, "med": 0.0, "win": 0.0, "tp": 0.0}
    tp = sum(1 for e in exits if e == "тейк") / len(xs) if exits is not None else 0.0
    return {"n": len(xs), "avg": sum(xs) / len(xs), "med": statistics.median(xs),
            "win": sum(1 for x in xs if x > 0) / len(xs), "tp": tp}


def quantiles(values: list[float], qs: Iterable[float]) -> list[float]:
    s = sorted(values)
    if not s:
        return []
    out = []
    for q in qs:
        v = s[min(len(s) - 1, int(q * len(s)))]
        if v not in out:
            out.append(v)
    return out


def fmt_val(key: str, v: float) -> str:
    if key in SHARES:
        return f"{v:.0%}"
    if v == int(v):
        return f"{int(v)}"
    return f"{v:.2f}" if abs(v) < 10 else f"{v:.0f}"


def cond_text(key: str, op: str, thr: float) -> str:
    return f"{NAMES[key]} {'≥' if op == '>=' else '<'} {fmt_val(key, thr)}"


def line(s: dict[str, float]) -> str:
    return f"{s['n']} монет · тейк {s['tp']:.0%} · в плюсе {s['win']:.0%} · в среднем {s['avg']:+.1%}"


def feature_table(c: Columns) -> list[str]:
    """По каждому признаку — 5 групп по значению: сколько монет, доля тейков, средний итог сделки бота."""
    lines = []
    for key, name in FEATURES:
        vals = list(c.feats[key])
        cuts = quantiles(vals, (0.2, 0.4, 0.6, 0.8))
        edges = [float("-inf"), *cuts, float("inf")]
        parts = []
        for lo, hi in zip(edges, edges[1:]):
            idx = [i for i, v in enumerate(vals) if lo <= v < hi]
            if not idx:
                continue
            s = stats((c.net[i] for i in idx), (c.exits[i] for i in idx))
            v_lo, v_hi = min(vals[i] for i in idx), max(vals[i] for i in idx)
            label = (fmt_val(key, v_lo) if v_lo == v_hi                       # одно значение (0 / 1, число ссылок)
                     else f"<{fmt_val(key, hi)}" if lo == float("-inf") else f"≥{fmt_val(key, lo)}"
                     if hi == float("inf") else f"{fmt_val(key, lo)}–{fmt_val(key, hi)}")
            parts.append(f"{label}: {s['n']} · {s['tp']:.0%} · {s['avg']:+.0%}")
        lines.append(f"• {name}: " + " | ".join(parts))
    return lines


def find_rules(c: Columns, top: int = 12, min_n: int = MIN_N) -> tuple[list[dict[str, Any]], int]:
    """Правила из одного-двух условий: ищутся на первых 2/3 монет (по времени), проверяются на последней трети."""
    n = len(c)
    cut = n * 2 // 3
    train, test = range(cut), range(cut, n)
    conds = []
    for key, _ in FEATURES:
        vals = c.feats[key]
        for thr in quantiles([vals[i] for i in train], [q / 10 for q in range(1, 10)]):
            for op in (">=", "<"):
                idx = {i for i in train if (vals[i] >= thr) == (op == ">=")}
                if min_n <= len(idx) <= 0.95 * cut:
                    conds.append({"conds": [(key, op, thr)], "idx": idx,
                                  "avg": sum(c.net[i] for i in idx) / len(idx)})
    conds.sort(key=lambda x: -x["avg"])
    best = conds[:TOP_SINGLE]
    rules = list(best)
    for a_i, a in enumerate(best):
        for b in best[a_i + 1:]:
            if a["conds"][0][0] == b["conds"][0][0]:
                continue
            idx = a["idx"] & b["idx"]
            if len(idx) >= min_n:
                rules.append({"conds": a["conds"] + b["conds"], "idx": idx,
                              "avg": sum(c.net[i] for i in idx) / len(idx)})
    rules.sort(key=lambda x: -x["avg"])
    out, seen = [], set()
    for r in rules:
        key = tuple(sorted(r["conds"]))
        if key in seen:
            continue
        seen.add(key)
        ok = [i for i in test if all((c.feats[k][i] >= thr) == (op == ">=") for k, op, thr in r["conds"])]
        out.append({"conds": r["conds"],
                    "train": stats((c.net[i] for i in r["idx"]), (c.exits[i] for i in r["idx"])),
                    "test": stats((c.net[i] for i in ok), (c.exits[i] for i in ok))})
        if len(out) >= top:
            break
    return out, cut


def report(cols: dict[float, Columns], days: float, min_n: int = MIN_N) -> str:
    any_c = next(iter(cols.values()), None)
    n = len(any_c) if any_c is not None else 0
    head = [f"🧪 <b>Паттерны монет</b> — {days:g} дн., монет на кривой pump.fun: {n}",
            (f"Сделка бота: вход через {LAG_S:g} с после момента решения по худшей цене, комиссии {FEE:.0%} "
             f"туда-обратно, выход +{TP:.0%} чистыми / −{SL:.0%} / через {HOLD_S / 60:g} мин.")]
    if n < 3 * min_n:
        return "\n".join(head + [f"Мало монет для выводов (нужно от {3 * min_n}) — своя база сделок за эти дни пуста?"])
    out = list(head)
    for at_s, c in sorted(cols.items()):
        base = stats(c.net, c.exits)
        doubled = sum(1 for p in c.peak if p >= 2) / len(c)
        out += ["", f"⏱ <b>Решение на {at_s:g}-й секунде</b>",
                f"Все монеты: {line(base)}, медиана {base['med']:+.0%}; выросли ×2 от входа — {doubled:.0%}",
                "По признакам (группа: монет · тейк · в среднем):", *feature_table(c)]
        rules, cut = find_rules(c, min_n=min_n)
        out.append(f"Лучшие правила — найдены на первых {cut} монетах, проверены на следующих {len(c) - cut}:")
        if not rules:
            out.append(f"нет правил с {min_n}+ монетами")
        for k, r in enumerate(rules, 1):
            t = r["test"]
            mark = "✅" if t["n"] >= 50 and t["avg"] > 0 else "❌" if t["n"] >= 50 else "❔"
            out.append(f"{k}. {mark} " + " и ".join(cond_text(*x) for x in r["conds"])
                       + f"\n   обучение: {line(r['train'])} → проверка: {line(t)}")
    out += ["", ("✅ — на проверке в плюсе (50+ монет), ❌ — в минусе, ❔ — мало монет. Правилу, которое в плюсе только "
                 "на обучении, верить нельзя.")]
    return "\n".join(out)


def to_csv_zip(cols: dict[float, Columns]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["монета", "создана", "решение, с", *[k for k, _ in FEATURES], "итог", "выход", "пик от входа, ×"])
    for at_s, c in sorted(cols.items()):
        for i in range(len(c)):
            w.writerow([c.mints[i], datetime.fromtimestamp(c.created[i], timezone.utc).astimezone(TZ).strftime(
                "%d.%m %H:%M:%S"), f"{at_s:g}", *[f"{c.feats[k][i]:.4g}" for k, _ in FEATURES],
                f"{c.net[i]:.4f}", c.exits[i], f"{c.peak[i]:.3f}"])
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("patterns.csv", buf.getvalue().encode("utf-8-sig"))
    return out.getvalue()


def split(text: str, limit: int = 3800) -> list[str]:
    parts, cur = [], ""
    for ln in text.split("\n"):
        if cur and len(cur) + len(ln) + 1 > limit:
            parts.append(cur)
            cur = ""
        cur = f"{cur}\n{ln}" if cur else ln
    if cur:
        parts.append(cur)
    return parts


async def run(db: Any, days: float = 3.0, at: Iterable[float] = (30.0, 60.0), send: bool = True,
              progress: Callable[[str], Any] | None = None, now: datetime | None = None, min_n: int = MIN_N) -> str:
    now = now or datetime.now(timezone.utc)
    until = now - timedelta(seconds=WINDOW_S + 300)          # у монет до этого времени окно в 30 минут закрыто
    since = until - timedelta(days=days)
    coins = await load_coins(db, since, until)
    at = sorted(float(x) for x in at)
    cols = {a: Columns() for a in at}
    for k in range(0, len(coins), BATCH):
        chunk = coins[k:k + BATCH]
        trades = await load_trades(db, [c["mint"] for c in chunk])
        for c in chunk:
            for r in study_coin(c, trades.get(c["mint"], []), at):
                cols[r["at"]].add(c, r)
        if progress is not None and (k // BATCH) % 25 == 0:
            progress(f"монет {min(k + BATCH, len(coins))} из {len(coins)}")
        await asyncio.sleep(0)
    text = report(cols, days, min_n)
    if send:
        for part in split(text):
            await db.system(part)
        if cols and len(next(iter(cols.values()))):
            data = to_csv_zip(cols)
            await db.enqueue("zip", {"topic": "system", "b64": base64.b64encode(data).decode(),
                                     "filename": f"patterns_{now:%Y%m%d_%H%M}.zip",
                                     "caption": "Паттерны монет: признаки и итог сделки бота по каждой монете (CSV)"},
                             priority=3)
    return text
