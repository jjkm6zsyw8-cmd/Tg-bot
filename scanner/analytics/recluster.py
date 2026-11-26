"""Отчёт о склейке кластеров и пересборка (блок A5).

    python -m scanner recluster --report     # только отчёт: через какие адреса склеились кластеры
    python -m scanner recluster              # отчёт + пересборка + пересчёт профилей

Пересборка (union-find по «личностям»):
* личность монеты = создатель (creator), если подписант — сервис запуска, иначе деплоер;
* ребро «общий источник финансирования» — только если финансист не сервис/биржа и финансировал
  не больше FUNDER_MAX_DEPLOYERS личностей;
* ребро «деплоер профинансировал другого деплоера» — прямой перевод SOL между личностями;
* подтверждённые пользователем связи сохраняются;
* повтор картинки, покупки в слоте создания и автосвязи через сервисы — не объединяют кластеры.
Кластер с 50+ запусками в день помечается подозрительным и не используется для алертов.
"""
from __future__ import annotations

import logging
from collections import Counter, defaultdict
from typing import Any

from ..db.pool import Database
from . import services as svc

log = logging.getLogger(__name__)
SUSPICIOUS_PER_DAY = 50


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}
        self.why: dict[tuple[str, str], tuple[str, str]] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


async def report(db: Database, per_day: int = SUSPICIOUS_PER_DAY) -> str:
    """Через какие адреса склеились кластеры с per_day+ запусками в сутки."""
    big = await db.fetch(
        """SELECT cluster_id, max(n) per_day, sum(n) total FROM (
               SELECT cluster_id, date_trunc('day', created_at) d, count(*) n FROM tokens
               WHERE cluster_id IS NOT NULL GROUP BY 1, 2) x
           GROUP BY 1 HAVING max(n) >= $1 ORDER BY 2 DESC LIMIT 15""", per_day)
    if not big:
        return "Кластеров с 50+ запусками в сутки нет."
    services = await svc.service_set(db)
    lines = [f"Кластеров с {per_day}+ запусками в сутки: {len(big)}"]
    for b in big:
        cid = b["cluster_id"]
        deps = await db.fetch("SELECT deployer, count(*) n, count(DISTINCT current_owner) owners FROM tokens "
                              "WHERE cluster_id=$1 GROUP BY 1 ORDER BY 2 DESC", cid)
        lines.append(f"\n#{cid}: до {b['per_day']} запусков/сутки, всего {b['total']}, деплоеров {len(deps)}")
        for top in deps[:3]:
            if top["n"] < svc.LAUNCH_SERVICE_PER_DAY:
                break
            lines.append(f"  один подписант {top['deployer']} создал {top['n']} монет"
                         + (f" за {top['owners']} разных создателей — сервис запуска" if top["owners"] > 1
                            else " — массовый запуск"))
        reasons = await db.fetch("SELECT reason, confidence, count(*) n FROM cluster_links WHERE cluster_id=$1 "
                                 "AND status <> 'rejected' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 6", cid)
        lines.append("  связи: " + "; ".join(f"{r['reason']} ({r['confidence']}) ×{r['n']}" for r in reasons))
        funders = await db.fetch(
            """SELECT w.funded_by f, count(DISTINCT w.address) n FROM wallets w
               JOIN tokens t ON t.deployer = w.address AND t.cluster_id = $1
               WHERE w.funded_by IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 5""", cid)
        for f in funders:
            if f["n"] >= 3:
                tag = " — СЕРВИС/БИРЖА" if f["f"] in services else ""
                lines.append(f"  финансист {f['f']}: {f['n']} деплоеров{tag}")
        imgs = await db.fetch("SELECT image, count(DISTINCT deployer) n FROM tokens WHERE cluster_id=$1 AND image "
                              "IS NOT NULL GROUP BY 1 HAVING count(DISTINCT deployer) >= 3 ORDER BY 2 DESC LIMIT 3",
                              cid)
        for i in imgs:
            lines.append(f"  одна картинка у {i['n']} деплоеров: {str(i['image'])[:80]}")
        hubs = await db.fetch(
            """SELECT l.wallet, l.reason, count(DISTINCT l2.cluster_id) n FROM cluster_links l
               JOIN cluster_links l2 ON l2.wallet = l.wallet
               WHERE l.cluster_id = $1 GROUP BY 1, 2 HAVING count(DISTINCT l2.cluster_id) >= 2
               ORDER BY 3 DESC LIMIT 5""", cid)
        for h in hubs:
            lines.append(f"  узел {h['wallet']} ({h['reason']}) — в {h['n']} кластерах")
    return "\n".join(lines)


async def rebuild(db: Database, days: int = 60) -> dict[str, Any]:
    found = await svc.detect(db)
    services = await svc.service_set(db)
    launch_services = await svc.launch_services(db)
    tokens = await db.fetch(
        "SELECT mint, deployer, current_owner, created_at, cluster_id FROM tokens "
        "WHERE created_at > now() - make_interval(days => $1)", days)
    uf = UnionFind()
    identity: dict[str, str] = {}
    for t in tokens:
        ident = t["current_owner"] if (t["deployer"] in launch_services and t["current_owner"]
                                       and t["current_owner"] != t["deployer"]) else t["deployer"]
        identity[t["mint"]] = ident
        uf.find(ident)
    idents = set(identity.values())
    link_reason: dict[str, tuple[str, str]] = {i: ("деплоер", "high") for i in idents}
    for t in tokens:
        if identity[t["mint"]] != t["deployer"]:
            link_reason[identity[t["mint"]]] = ("создатель (запуск через сервис)", "high")

    # общий финансист (не сервис, не больше FUNDER_MAX_DEPLOYERS личностей)
    funded = await db.fetch("SELECT address, funded_by FROM wallets WHERE address = ANY($1) AND funded_by IS NOT NULL",
                            list(idents))
    by_funder: dict[str, list[str]] = defaultdict(list)
    for r in funded:
        if r["funded_by"] not in services:
            by_funder[r["funded_by"]].append(r["address"])
    edges = Counter()
    for funder, group in by_funder.items():
        if funder in idents:
            for g in group:
                uf.union(funder, g)
                edges["деплоер профинансировал другого деплоера"] += 1
        elif len(group) <= svc.FUNDER_MAX_DEPLOYERS:
            for g in group[1:]:
                uf.union(group[0], g)
                edges["общий источник финансирования"] += 1
    # прямые переводы SOL между личностями
    for r in await db.fetch(
            """SELECT DISTINCT src, dst FROM sol_transfers WHERE src = ANY($1) AND dst = ANY($1) AND src <> dst
               AND sol >= 0.05""", list(idents)):
        uf.union(r["src"], r["dst"])
        edges["перевод SOL между деплоерами"] += 1
    # подтверждённые пользователем связи
    confirmed = await db.fetch("SELECT cluster_id, wallet FROM cluster_links WHERE status = 'confirmed'")
    by_cl: dict[int, list[str]] = defaultdict(list)
    for r in confirmed:
        if r["wallet"] in idents:
            by_cl[r["cluster_id"]].append(r["wallet"])
    for group in by_cl.values():
        for g in group[1:]:
            uf.union(group[0], g)
            edges["подтверждено пользователем"] += 1

    # компоненты → кластеры (сохраняем старый id, если он «принадлежит» компоненте)
    comp_tokens: dict[str, list[Any]] = defaultdict(list)
    for t in tokens:
        comp_tokens[uf.find(identity[t["mint"]])].append(t)
    used: set[int] = set()
    comp_cluster: dict[str, int] = {}
    old_size = Counter(t["cluster_id"] for t in tokens if t["cluster_id"])
    for root, toks in sorted(comp_tokens.items(), key=lambda x: -len(x[1])):
        votes = Counter(t["cluster_id"] for t in toks if t["cluster_id"])
        cid = None
        for cand, n in votes.most_common():
            if cand not in used and n * 2 >= old_size[cand]:
                cid = cand
                break
        if cid is None:
            cid = await db.fetchval("INSERT INTO clusters(label) VALUES ($1) RETURNING id", root[:6])
        used.add(cid)
        comp_cluster[root] = cid

    async with db.p.acquire() as conn:
        async with conn.transaction():
            await conn.execute("CREATE TEMP TABLE remap(mint text PRIMARY KEY, cluster_id bigint, ident text) "
                               "ON COMMIT DROP")
            await conn.copy_records_to_table("remap", records=[
                (t["mint"], comp_cluster[uf.find(identity[t["mint"]])], identity[t["mint"]]) for t in tokens])
            await conn.execute("UPDATE tokens t SET cluster_id = r.cluster_id, identity = r.ident FROM remap r "
                               "WHERE t.mint = r.mint")
            await conn.execute("UPDATE launches l SET cluster_id = r.cluster_id FROM remap r WHERE l.mint = r.mint")
            await conn.execute("DELETE FROM cluster_links WHERE status = 'auto'")
            rows = []
            for ident in idents:
                cid = comp_cluster[uf.find(ident)]
                reason, conf = link_reason[ident]
                rows.append((cid, ident, reason, conf))
            for funder, group in by_funder.items():
                if funder not in idents and len(group) <= svc.FUNDER_MAX_DEPLOYERS and len(group) >= 2:
                    rows.append((comp_cluster[uf.find(group[0])], funder,
                                 f"общий источник финансирования {len(group)} деплоеров", "high"))
            await conn.executemany(
                """INSERT INTO cluster_links(cluster_id, wallet, reason, confidence, status) VALUES ($1,$2,$3,$4,'auto')
                   ON CONFLICT (cluster_id, wallet) DO NOTHING""", rows)
            # автоматические чёрные списки пересчитает профиль; ручные и «Скам» сохраняются
            await conn.execute("UPDATE clusters SET blacklisted = false, blacklist_reason = NULL "
                               "WHERE blacklist_reason = 'мгновенные раги'")
    suspicious = await mark_suspicious(db)
    from .loader import cluster_profile, save_profile
    from ..config import Settings, YamlConfig
    cfg = Settings(YamlConfig("config/config.yaml"), await db.get_overrides()).effective()
    profiled = 0
    multi = {r["cluster_id"] for r in await db.fetch(
        "SELECT cluster_id FROM launches WHERE cluster_id IS NOT NULL GROUP BY 1 HAVING count(*) >= 2")}
    for cid in {c for c in comp_cluster.values()} & multi:
        prof, _ = await cluster_profile(db, cid, cfg)
        if prof.launches:
            await save_profile(db, cid, prof, cfg)
            profiled += 1
    return {"services": len(found), "tokens": len(tokens), "identities": len(idents),
            "clusters": len(set(comp_cluster.values())), "edges": dict(edges), "suspicious": suspicious,
            "profiled": profiled}


async def mark_suspicious(db: Database, per_day: int = SUSPICIOUS_PER_DAY) -> int:
    rows = await db.fetch(
        """SELECT cluster_id, max(n) per_day FROM (
               SELECT cluster_id, date_trunc('day', created_at) d, count(*) n FROM tokens
               WHERE cluster_id IS NOT NULL AND created_at > now() - interval '30 days' GROUP BY 1, 2) x
           GROUP BY 1 HAVING max(n) >= $1""", per_day)
    await db.execute("UPDATE clusters SET suspicious = false, suspicious_reason = NULL WHERE suspicious")
    for r in rows:
        await db.execute("UPDATE clusters SET suspicious = true, suspicious_reason = $2 WHERE id = $1",
                         r["cluster_id"], f"{r['per_day']} запусков в сутки")
    return len(rows)
