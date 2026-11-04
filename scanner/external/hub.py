"""Хаб внешних источников: единая точка для движка и команд бота.

* запросы только для кандидатов на алерт и по командам (/token, /dev, /trader);
* все источники опрашиваются параллельно, каждый с тайм-аутом (1 с для алертов);
* не ответил — алерт уходит без его данных, в алерте пометка «нет данных: …»;
* каждое значение пишется в external_facts с указанием источника;
* при расхождении с ончейном приоритет у ончейна (внешнее значение только сохраняется для сверки).
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ..config import Env
from ..db.pool import Database
from .adapters import Birdeye, DexScreener, Gmgn, Jupiter, RugCheck, Solscan
from .base import ExternalSource, Unavailable

log = logging.getLogger(__name__)

DEFAULT_CONFIG: dict[str, Any] = {
    "timeout_s": 1.0,
    "command_timeout_s": 4.0,
    "sources": {n: {"enabled": True} for n in ("dexscreener", "rugcheck", "gmgn", "birdeye", "jupiter", "solscan")},
    "filters": {
        "require_mint_renounced": True,
        "require_freeze_renounced": True,
        "block_rugged": True,
        "max_rug_ratio": 0.3,          # GMGN rug_ratio 0..1
        "block_rugcheck_danger": False,  # опасные риски RugCheck: выделять (False) или отсекать (True)
        "max_top10_external": 0.45,    # используется, только если своего ончейн-значения нет
    },
}

SOURCE_NAMES = {"dexscreener": "DexScreener", "rugcheck": "RugCheck", "gmgn": "GMGN", "birdeye": "Birdeye",
                "jupiter": "Jupiter", "solscan": "Solscan"}


def load_config(path: str | Path = "config/external.yaml") -> dict[str, Any]:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    p = Path(path)
    if p.exists():
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        for k, v in data.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                for kk, vv in v.items():
                    if isinstance(vv, dict) and isinstance(cfg[k].get(kk), dict):
                        cfg[k][kk].update(vv)
                    else:
                        cfg[k][kk] = vv
            else:
                cfg[k] = v
    return cfg


@dataclass
class TokenCheck:
    facts: dict[str, dict[str, Any]] = field(default_factory=dict)   # источник → значения
    missing: list[str] = field(default_factory=list)                  # включён, но не ответил
    block: list[str] = field(default_factory=list)                    # причины отсечь монету
    warnings: list[str] = field(default_factory=list)                 # нарушения без отсечения
    lines: list[str] = field(default_factory=list)                    # строки для алерта
    smart: int | None = None
    kol: int | None = None
    wallets: dict[str, dict[str, Any]] = field(default_factory=dict)

    def short(self) -> str:
        """Одна строка для алерта: 🛡 RugCheck ✓ · GMGN rug 5% · ⚠ … · нет: GMGN."""
        parts = []
        rc, gm = self.facts.get("rugcheck"), self.facts.get("gmgn")
        if rc:
            parts.append("RugCheck ⚠" if rc.get("danger_risks") else "RugCheck ✓")
        if gm and gm.get("rug_ratio") is not None:
            parts.append(f"GMGN rug {gm['rug_ratio']:.0%}")
        if self.kol:
            parts.append(f"KOL {self.kol}")
        for w in self.warnings[:1]:
            parts.append("⚠ " + w.split(": ", 1)[-1][:40])
        if self.missing:
            parts.append("нет: " + ", ".join(SOURCE_NAMES.get(m, m) for m in self.missing))
        return ("🛡 " + " · ".join(parts)) if parts else ""


class ExternalHub:
    def __init__(self, env: Env, db: Database, config_path: str | None = None):
        self.db = db
        self.cfg = load_config(config_path or env.external_config_path)
        src = self.cfg["sources"]

        def mk(cls: type[ExternalSource], key: str = "") -> Any:
            c = src.get(cls.name) or {}
            return cls(key=key, enabled=bool(c.get("enabled", True)), rate_per_s=c.get("rate_per_s"),
                       capacity=c.get("capacity"), timeout_s=float(self.cfg.get("timeout_s", 1.0)))

        self.dex: DexScreener = mk(DexScreener)
        self.rugcheck: RugCheck = mk(RugCheck)
        self.gmgn: Gmgn = mk(Gmgn, env.gmgn_api_key)
        self.birdeye: Birdeye = mk(Birdeye, env.birdeye_api_key)
        self.jupiter: Jupiter = mk(Jupiter, env.jupiter_api_key)
        if env.jupiter_api_key and not (src.get("jupiter") or {}).get("rate_per_s"):
            self.jupiter.bucket.rate = 1.0
        self.solscan: Solscan = mk(Solscan, env.solscan_api_key)
        self.sources: list[ExternalSource] = [self.dex, self.rugcheck, self.gmgn, self.birdeye, self.jupiter,
                                              self.solscan]

    @property
    def timeout(self) -> float:
        return float(self.cfg.get("timeout_s", 1.0))

    @property
    def command_timeout(self) -> float:
        return float(self.cfg.get("command_timeout_s", 4.0))

    def any_token_source(self) -> bool:
        return self.rugcheck.enabled or self.gmgn.enabled

    def disable_all(self, reason: str = "выключен") -> None:
        for s in self.sources:
            s.enabled = False
            s.disabled_reason = reason

    async def close(self) -> None:
        for s in self.sources:
            await s.close()

    # ------------------------------------------------------------------ вызовы с тайм-аутом
    async def _call(self, src: ExternalSource, coro: Any, timeout: float) -> tuple[bool, Any]:
        """(ответил, значение). Выключенный источник не считается «не ответившим»."""
        if not src.enabled:
            coro.close()
            return True, None
        try:
            return True, await asyncio.wait_for(coro, timeout)
        except Unavailable:
            return False, None
        except asyncio.TimeoutError:
            src.stats.error("тайм-аут")
            return False, None
        except Exception as exc:  # noqa: BLE001 — внешний источник не должен ронять бота
            src.stats.error(f"{type(exc).__name__}: {exc}")
            log.debug("%s: %s", src.name, exc)
            return False, None

    # ------------------------------------------------------------------ токен
    async def token_checks(self, mint: str, onchain: dict[str, Any] | None = None, wallets: list[str] | None = None,
                           timeout: float | None = None) -> TokenCheck:
        timeout = timeout or self.timeout
        onchain = onchain or {}
        calls = {
            "rugcheck": (self.rugcheck, self.rugcheck.report(mint)),
            "gmgn": (self.gmgn, self.gmgn.token_security(mint)),
            "gmgn_holders": (self.gmgn, self.gmgn.holder_tags(mint)),
        }
        names = list(calls)
        wl = (wallets or [])[:3]
        gathered = await asyncio.gather(*(self._call(src, coro, timeout) for src, coro in calls.values()),
                                        *(self.wallet_info(w, timeout) for w in wl))
        results, wallet_infos = gathered[:len(names)], gathered[len(names):]
        chk = TokenCheck()
        for name, (ok, value) in zip(names, results):
            src_name = calls[name][0].name
            if not ok and src_name not in chk.missing:
                chk.missing.append(src_name)
            if value:
                chk.facts[name] = value
        for w, info in zip(wl, wallet_infos):
            if info:
                chk.wallets[w] = info
        self._evaluate(chk, onchain)
        await self._save_facts(f"token:{mint}", chk.facts)
        return chk

    def _evaluate(self, chk: TokenCheck, onchain: dict[str, Any]) -> None:
        f = self.cfg["filters"]
        rc, gm, gh = chk.facts.get("rugcheck"), chk.facts.get("gmgn"), chk.facts.get("gmgn_holders")
        parts = []
        if rc:
            if f.get("require_mint_renounced", True) and rc.get("mint_authority"):
                chk.block.append("mint authority не отозвана (RugCheck)")
            if f.get("require_freeze_renounced", True) and rc.get("freeze_authority"):
                chk.block.append("freeze authority не отозвана (RugCheck)")
            if f.get("block_rugged", True) and rc.get("rugged"):
                chk.block.append("RugCheck: монета помечена как раг")
            if rc.get("danger_risks"):
                msg = "RugCheck: " + ", ".join(rc["danger_risks"][:3])
                (chk.block if f.get("block_rugcheck_danger") else chk.warnings).append(msg)
            score = rc.get("score_normalised")
            parts.append(f"RugCheck риск {score:.0f}" if score is not None else "RugCheck")
        if gm:
            if f.get("require_mint_renounced", True) and gm.get("mint_renounced") in (0, False, "0"):
                chk.block.append("mint authority не отозвана (GMGN)")
            if f.get("require_freeze_renounced", True) and gm.get("freeze_renounced") in (0, False, "0"):
                chk.block.append("freeze authority не отозвана (GMGN)")
            rr = gm.get("rug_ratio")
            if rr is not None and rr > float(f.get("max_rug_ratio", 0.3)):
                chk.block.append(f"GMGN rug_ratio {rr:.0%}")
            if rr is not None:
                parts.append(f"GMGN rug {rr:.0%}")
        # топ-10: приоритет у ончейна; внешнее — только если своего нет
        ext_top10 = (rc or {}).get("top10_pct") if rc else None
        if ext_top10 is None and gm:
            ext_top10 = gm.get("top10_rate")
        own = onchain.get("top10")
        if own is None and ext_top10 is not None:
            if ext_top10 > float(f.get("max_top10_external", 0.45)):
                chk.block.append(f"топ-10 {ext_top10:.0%} (внешние данные)")
            parts.append(f"топ-10 {ext_top10:.0%}")
        elif own is not None and ext_top10 is not None and abs(own - ext_top10) > 0.10:
            chk.facts.setdefault("discrepancy", {})["top10"] = {"onchain": own, "external": ext_top10}
        if gh:
            chk.smart, chk.kol = gh.get("smart"), gh.get("kol")
            parts.append(f"смарт-мани {chk.smart or 0} · KOL {chk.kol or 0} (GMGN)")
        if parts:
            chk.lines.append("Внешние: " + " · ".join(parts))
        for w, info in chk.wallets.items():
            tags = [t for t in info.get("tags") or [] if t]
            if tags or info.get("twitter"):
                chk.lines.append(f"Кошелёк {w[:4]}…{w[-4:]}: " + ", ".join(tags[:4])
                                 + (f" · @{info['twitter']}" if info.get("twitter") else ""))
        if chk.warnings:
            chk.lines.append("⚠ " + "; ".join(chk.warnings))
        if chk.missing:
            chk.lines.append("⏳ нет данных: " + ", ".join(SOURCE_NAMES.get(m, m) for m in chk.missing))

    # ------------------------------------------------------------------ кошельки
    async def wallet_info(self, wallet: str, timeout: float | None = None) -> dict[str, Any] | None:
        """Метки кошелька (кеш 24 ч): биржа, smart, KOL, теги, Twitter. Ончейн-эвристики не перекрывает."""
        timeout = timeout or self.timeout
        (ok_s, sol), (ok_g, gm) = await asyncio.gather(
            self._call(self.solscan, self.solscan.label(wallet), timeout),
            self._call(self.gmgn, self.gmgn.wallet_stats(wallet), timeout))
        if not sol and not gm:
            return None
        info: dict[str, Any] = {"tags": [], "sources": {}}
        if sol:
            info["label"] = sol.get("label")
            info["tags"] += sol.get("tags") or []
            info["is_exchange"] = bool(sol.get("is_exchange"))
            info["sources"]["label"] = "solscan"
        if gm:
            info["tags"] += gm.get("tags") or []
            info["twitter"] = gm.get("twitter")
            info["is_smart"] = gm.get("is_smart")
            info["is_kol"] = gm.get("is_kol")
            info["pnl_30d"] = gm.get("realized_profit")
            info["winrate_30d"] = gm.get("winrate")
            info["is_exchange"] = info.get("is_exchange") or gm.get("is_exchange")
            info["sources"]["tags"] = "gmgn"
        info["tags"] = list(dict.fromkeys(str(t) for t in info["tags"]))
        await self._save_facts(f"wallet:{wallet}", {k: v for k, v in (("solscan", sol), ("gmgn", gm)) if v})
        try:
            await self.db.execute(
                """INSERT INTO wallets(address, label, label_source, tags, tags_source, is_exchange, labels_at)
                   VALUES ($1, $2, $3, $4, $5, $6, now())
                   ON CONFLICT (address) DO UPDATE SET
                       label = COALESCE(EXCLUDED.label, wallets.label),
                       label_source = COALESCE(EXCLUDED.label_source, wallets.label_source),
                       tags = EXCLUDED.tags, tags_source = EXCLUDED.tags_source,
                       is_exchange = wallets.is_exchange OR EXCLUDED.is_exchange, labels_at = now()""",
                wallet, info.get("label"), info["sources"].get("label"), info["tags"],
                ",".join(sorted(set(info["sources"].values()))), bool(info.get("is_exchange")))
        except Exception:  # noqa: BLE001
            log.debug("wallet labels save failed", exc_info=True)
        return info

    async def is_exchange(self, wallet: str) -> bool | None:
        info = await self.wallet_info(wallet)
        return None if info is None else bool(info.get("is_exchange"))

    # ------------------------------------------------------------------ цены и пары
    async def dex_pair(self, mint: str, timeout: float | None = None) -> dict[str, Any] | None:
        ok, v = await self._call(self.dex, self.dex.pair(mint), timeout or self.timeout)
        if v:
            await self._save_facts(f"token:{mint}", {"dexscreener": v})
        return v

    async def sol_usd(self) -> float | None:
        _ok, v = await self._call(self.jupiter, self.jupiter.sol_usd(), 3.0)
        return v

    # ------------------------------------------------------------------ хранение и здоровье
    async def _save_facts(self, entity: str, facts: dict[str, Any]) -> None:
        rows = []
        for source, values in facts.items():
            if not isinstance(values, dict):
                continue
            src = {"gmgn_holders": "gmgn", "discrepancy": "onchain_vs_external"}.get(source, source)
            for k, v in values.items():
                key = f"{source}.{k}" if source in ("gmgn_holders", "discrepancy") else k
                rows.append((entity, key, src, json.loads(json.dumps(v, default=str))))
        if not rows:
            return
        try:
            await self.db.p.executemany(
                """INSERT INTO external_facts(entity, key, source, value, fetched_at) VALUES ($1, $2, $3, $4, now())
                   ON CONFLICT (entity, key, source) DO UPDATE SET value = EXCLUDED.value, fetched_at = now()""", rows)
        except Exception:  # noqa: BLE001
            log.debug("external facts save failed", exc_info=True)

    def health(self) -> dict[str, Any]:
        return {s.name: s.health() for s in self.sources}

    async def publish_health(self, proc: str) -> None:
        await self.db.kv_set(f"health:external:{proc}", self.health())
