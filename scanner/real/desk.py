"""Кошельки реального бота: у каждого кошелька — своя «полоса» исполнителя (RealTrader) со своим режимом
(выкл / тень / бой), ставкой, сигналами, позициями, стопами и журналом. Управление — /real → «👛 Кошельки».

Откуда кошельки:
  • создан кнопкой в Telegram — ключ хранится в базе только зашифрованным (vault.py, ключ шифра REAL_MASTER_KEY
    в .env на сервере);
  • ключ в .env (REAL_PRIVATE_KEY) — кошелёк «Основной (.env)», в базе его ключа нет.

Вывод денег — только на адреса из REAL_WITHDRAW_TO в .env сервера (белый список инструкций это проверяет): даже
тот, кто получит доступ к вашему Telegram, вывести деньги себе не сможет.

Пока кошельков нет, работает «общая полоса» без кошелька (режим из /real, обычно — тень): она считает сделки по
сети без денег, как раньше.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from solders.keypair import Keypair
from solders.pubkey import Pubkey

from ..alerts import format as fmt
from . import settings as S, tx as rtx
from .executor import TOPIC, RealTrader, make_chain
from .keys import Wallet, load_wallet
from .vault import Vault, load_vault

if TYPE_CHECKING:
    from ..analytics.engine import Engine

log = logging.getLogger(__name__)

DESK_KEY = "real:desk"
ENV_WITHDRAW = "REAL_WITHDRAW_TO"
MAX_WALLETS = 10
WITHDRAW_PRIORITY = 5_000            # лампортов приоритета на вывод
WITHDRAW_FEE = 5_000 + WITHDRAW_PRIORITY
MIN_WITHDRAW = 100_000               # меньше 0.0001 SOL не выводим
ARCHIVE_MAX_LAMPORTS = 1_000_000     # удалить из списка можно кошелёк, на котором меньше 0.001 SOL
ENV_NAME = "Основной (.env)"


def withdraw_targets(env: Any = None) -> list[Pubkey]:
    """Адреса для вывода из REAL_WITHDRAW_TO (через запятую или пробел), не больше пяти."""
    raw = ((os.environ if env is None else env).get(ENV_WITHDRAW) or "").strip()
    out: list[Pubkey] = []
    for part in re.split(r"[,;\s]+", raw):
        try:
            pk = Pubkey.from_string(part)
        except Exception:  # noqa: BLE001
            continue
        if pk not in out:
            out.append(pk)
    return out[:5]


class RealDesk:
    def __init__(self, engine: "Engine", chain: rtx.Chain | None = None, vault: Vault | None = None,
                 env_wallet: tuple[Wallet | None, str] | None = None, withdraw_to: list[Pubkey] | None = None):
        self.e = engine
        self.db = engine.db
        self.vault, self.vault_note = (vault, "задан") if vault is not None else load_vault()
        self.env_wallet, self.env_note = env_wallet if env_wallet is not None else load_wallet()
        self.withdraw_to = list(withdraw_to) if withdraw_to is not None else withdraw_targets()
        self.chain = chain if chain is not None else make_chain(engine)
        self.lanes: dict[int | None, RealTrader] = {}
        self._last_status = 0.0
        self._last_poll = 0.0
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ то, что зовёт движок
    @property
    def positions(self) -> dict[int, Any]:
        return {pid: p for lane in self.lanes.values() for pid, p in lane.positions.items()}

    def wallet_lanes(self) -> dict[int, RealTrader]:
        return {wid: lane for wid, lane in self.lanes.items() if wid is not None}

    async def start(self) -> None:
        await self._ensure_env_row()
        for r in await self.db.fetch("SELECT * FROM real_wallets WHERE NOT archived ORDER BY id"):
            await self._add_lane(r)
        await self._sync_common_lane()
        await self.publish_status()
        log.info("реальный бот: кошельков %d (%s), вывод на %d адр.", len(self.wallet_lanes()), self.vault_note,
                 len(self.withdraw_to))

    async def on_signal(self, mint: str, level: str, alert_id: int | None, data: dict[str, Any],
                        trader: str | None, targets: set[str]) -> None:
        lanes = list(self.lanes.values())
        res = await asyncio.gather(*(lane.on_signal(mint, level, alert_id, dict(data), trader, set(targets))
                                     for lane in lanes), return_exceptions=True)
        for lane, r in zip(lanes, res):
            if isinstance(r, Exception):
                log.error("реальный бот %s: сигнал %s: %r", lane.name or "общий", mint, r)

    async def on_trade(self, st: Any, eff: Any, trader: str, is_buy: bool, slot: int | None = None,
                       sol: float | None = None) -> None:
        for lane in list(self.lanes.values()):
            if lane.positions or lane.trials:
                try:
                    await lane.on_trade(st, eff, trader, is_buy, slot, sol)
                except Exception:  # noqa: BLE001
                    log.exception("реальный бот %s: сделка монеты", lane.name or "общий")

    async def loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            await self.tick()

    async def tick(self) -> None:
        for lane in list(self.lanes.values()):
            lane.external_poll = True
            try:
                await lane.tick()
            except Exception:  # noqa: BLE001 — сбой одного кошелька не задевает остальные
                log.exception("реальный бот %s: цикл", lane.name or "общий")
        await self.poll_prices()
        if time.time() - self._last_status >= 5:
            await self.publish_status()
        if time.time() >= getattr(self, "_tune_at", 0):
            await self.autotune()

    async def autotune(self) -> None:
        """Автонастройка по тени раз в 6 ч (первый раз — через 10 мин после старта): отчёт в тему «Реальный бот»."""
        from . import autotune as AT
        first = getattr(self, "_tune_at", None) is None
        self._tune_at = time.time() + (600 if first else AT.EVERY_S)
        if first:
            return
        lanes = list(self.lanes.values())
        if not lanes:
            return
        live = {k for lane in lanes for k in S.METHODS if lane.mode_for(k) == "live"}
        try:
            out = await AT.run(self.db, lanes[0].e.cfg, live)
            for lane in lanes:
                await lane.load_autotune()
            for text in out:
                await self.db.enqueue("text", {"topic": "real", "text": text}, priority=4)
        except Exception:  # noqa: BLE001 — автонастройка не должна мешать торговле
            log.exception("автонастройка")

    async def poll_prices(self) -> None:
        """Цена открытых позиций всех кошельков — из сети, одним запросом на 16 монет (а не запрос на кошелёк)."""
        lanes = [lane for lane in self.lanes.values() if lane.watched()]
        if not lanes or self.chain is None:
            return
        every = min(float(lane.get("real_poll_s")) for lane in lanes)
        if time.time() - self._last_poll < every:
            return
        self._last_poll = time.time()
        mints = list(dict.fromkeys(p.mint for lane in lanes for p in lane.watched()))
        for i in range(0, len(mints), 16):
            chunk = mints[i:i + 16]
            try:
                sts, _ = await lanes[0]._states([Pubkey.from_string(m) for m in chunk])
            except Exception as exc:  # noqa: BLE001
                log.warning("реальный бот: цена позиций: %s", str(exc)[:120])
                return
            by_mint = dict(zip(chunk, sts))
            for lane in lanes:
                for p in lane.watched():
                    if p.mint in by_mint:
                        try:
                            lane.on_state(p, by_mint[p.mint])
                        except Exception:  # noqa: BLE001
                            log.exception("реальный бот: решение по позиции %s", p.mint)

    async def command(self, p: dict[str, Any]) -> None:
        """Команды из Telegram (через очередь заданий)."""
        cmd = p.get("cmd")
        try:
            if cmd == "create_wallet":
                await self.create_wallet(str(p.get("name") or ""))
            elif cmd == "withdraw":
                await self.withdraw(int(p["wallet"]), str(p.get("what") or ""), int(p.get("to") or 0))
            elif cmd == "archive":
                await self.archive(int(p["wallet"]))
            elif cmd == "reload":
                await self.reload()
            elif cmd == "sell":
                for lane in self.lanes.values():
                    if int(p.get("id") or 0) in lane.positions:
                        await lane.command(p)
            elif cmd == "check":
                lane = self.lanes.get(p.get("wallet")) or next(
                    (x for x in self.wallet_lanes().values() if x.wallet is not None), None) or next(
                    iter(self.lanes.values()), None)
                if lane is not None:
                    await lane.command(p)
            else:
                wid = p.get("wallet")
                for lane in ([self.lanes[wid]] if wid in self.lanes else list(self.lanes.values())):
                    await lane.command(p)
        finally:
            await self.publish_status()

    # ------------------------------------------------------------------ кошельки
    async def _ensure_env_row(self) -> None:
        """Ключ (или адрес) из .env — кошелёк «Основной (.env)»: режим берётся из общих настроек на момент появления."""
        if self.env_wallet is None:
            return
        cfg = self.e.cfg
        settings = {"real_mode": S.get(cfg, "real_mode")}
        if cfg.get("real_armed_at"):
            settings["real_armed_at"] = cfg["real_armed_at"]
        await self.db.execute(
            "INSERT INTO real_wallets(name, pubkey, secret_enc, source, settings) VALUES ($1, $2, NULL, 'env', $3) "
            "ON CONFLICT (pubkey) DO NOTHING", ENV_NAME, str(self.env_wallet.pubkey), settings)

    def _wallet_of(self, row: Any) -> tuple[Wallet, str]:
        pk = Pubkey.from_string(row["pubkey"])
        if row["secret_enc"] is not None:
            if not self.vault.can_store:
                return Wallet(pk), "ключ не расшифрован: на сервере нет REAL_MASTER_KEY — только тень"
            try:
                return Wallet(pk, self.vault.decrypt(bytes(row["secret_enc"]), pk)), "ключ в базе (зашифрован)"
            except Exception as exc:  # noqa: BLE001 — сообщения об ошибке свои, без кусков ключа
                return Wallet(pk), f"ключ не расшифрован ({exc}) — только тень"
        if self.env_wallet is not None and self.env_wallet.pubkey == pk:
            return self.env_wallet, "ключ в .env" if self.env_wallet.can_sign else "только адрес (.env) — тень"
        return Wallet(pk), "ключа нет — только тень"

    async def _add_lane(self, row: Any) -> RealTrader:
        profile = {"id": int(row["id"]), "name": row["name"], "settings": dict(row["settings"] or {}),
                   "source": row["source"], "pubkey": row["pubkey"]}
        wallet, note = self._wallet_of(row)
        lane = RealTrader(self.e, self.chain, wallet, profile, wallet_note=note)
        self.lanes[profile["id"]] = lane
        await lane.start()
        return lane

    async def _sync_common_lane(self) -> None:
        """Общая полоса (без кошелька из списка): принимает новые сигналы, только пока кошельков нет; иначе
        лишь доводит до выхода свои старые позиции."""
        has_wallets = bool(self.wallet_lanes())
        orphan = await self.db.fetchval(
            "SELECT count(*) FROM real_positions WHERE closed_at IS NULL AND wallet_id IS NULL")
        lane = self.lanes.get(None)
        if lane is None and (not has_wallets or orphan):
            lane = RealTrader(self.e, self.chain, self.env_wallet, None, wallet_note=self.env_note)
            self.lanes[None] = lane
            await lane.start()
        if lane is not None:
            lane.accept_new = not has_wallets
            if has_wallets and not lane.positions:
                self.lanes.pop(None, None)
                await self.db.kv_set(lane.status_key(), None)

    async def reload(self) -> None:
        """Настройки кошельков изменили в Telegram — перечитать (без перезапуска)."""
        async with self._lock:
            rows = await self.db.fetch("SELECT * FROM real_wallets ORDER BY id")
            for r in rows:
                lane = self.lanes.get(int(r["id"]))
                if r["archived"]:
                    if lane is not None and not lane.positions:
                        self.lanes.pop(int(r["id"]), None)
                        await self.db.kv_set(lane.status_key(), None)
                    continue
                if lane is None:
                    await self._add_lane(r)
                else:
                    lane.profile["settings"] = dict(r["settings"] or {})
                    lane.profile["name"] = lane.name = r["name"]
            await self._sync_common_lane()
        for lane in list(self.lanes.values()):          # режим могли включить после пополнения — баланс свежий
            await lane.refresh_balance(force=True)

    async def create_wallet(self, name: str) -> int | None:
        if not self.vault.can_store:
            await self.say("👛 Создать кошелёк нельзя: на сервере нет ключа шифра. Одна команда на сервере:\n"
                           "<code>cd /opt/tg-bot &amp;&amp; echo &gt;&gt; .env &amp;&amp; docker compose run --rm "
                           "--no-deps -T analytics real-masterkey &gt;&gt; .env &amp;&amp; docker compose up -d "
                           "--force-recreate analytics</code>")
            return None
        async with self._lock:
            n = int(await self.db.fetchval("SELECT count(*) FROM real_wallets WHERE NOT archived") or 0)
            if n >= MAX_WALLETS:
                await self.say(f"👛 Кошельков уже {n} — больше {MAX_WALLETS} нельзя. Удалите ненужный.")
                return None
            name = re.sub(r"[<>&]", "", name or "").strip()[:32] or f"Кошелёк {n + 1}"
            kp = Keypair()
            row = await self.db.fetchrow(
                "INSERT INTO real_wallets(name, pubkey, secret_enc, source, settings) VALUES ($1, $2, $3, 'tg', $4) "
                "RETURNING *", name, str(kp.pubkey()), self.vault.encrypt(kp), {"real_mode": "shadow"})
            await self._add_lane(row)
            await self._sync_common_lane()
        addr = str(kp.pubkey())
        await self.say(f"👛 Создан кошелёк <b>{fmt.esc(name)}</b>. Режим — 👻 тень.\nАдрес для пополнения (SOL, сеть "
                       f"Solana):\n<code>{addr}</code>",
                       [[{"text": "📋 Скопировать адрес", "copy": addr}],
                        [{"text": "👛 Открыть кошелёк", "callback": f"rl:w:{row['id']}"}]])
        await self.publish_status()
        return int(row["id"])

    async def withdraw(self, wid: int, what: str, to: int) -> str:
        """Вывод SOL с кошелька на свой адрес из REAL_WITHDRAW_TO. what: all — всё (кошелёк → «выкл»), half — половина.
        Возвращает состояние транзакции (landed / failed / expired) или причину отказа."""
        lane = self.lanes.get(wid)
        if lane is None or lane.wallet is None or not lane.wallet.can_sign:
            await self.say("📤 Вывод невозможен: у этого кошелька нет ключа на сервере.")
            return "нет ключа"
        if not self.withdraw_to:
            await self.say("📤 Вывод выключен: на сервере не задан адрес для вывода REAL_WITHDRAW_TO.")
            return "нет адреса"
        if not 0 <= to < len(self.withdraw_to) or what not in ("all", "half"):
            return "неверная команда"
        if what == "all" and lane.positions:
            await lane.say_raw(f"📤 «Вывести всё» — только без открытых позиций (сейчас {len(lane.positions)}). "
                               "Нажмите «💰 Продать всё» в кошельке и повторите вывод.")
            return "есть позиции"
        dest = self.withdraw_to[to]
        balance = await self.chain.balance(lane.wallet.pubkey)
        amount = balance - WITHDRAW_FEE if what == "all" else balance // 2
        if amount < MIN_WITHDRAW:
            await lane.say_raw(f"📤 Выводить нечего: на кошельке {balance / 1e9:.6f} SOL.")
            return "нечего выводить"
        if what == "all":
            await lane.set_setting("real_mode_at", datetime.now(timezone.utc).isoformat())
            await lane.set_setting("real_mode_by", "вывод всех денег с кошелька")
            await lane.set_setting("real_mode", "off")        # пустой кошелёк не должен пытаться торговать
        user = lane.wallet.pubkey
        ixs = rtx.budget_ixs(1_000, WITHDRAW_PRIORITY) + [rtx.transfer_ix(user, dest, amount)]
        rtx.check_instructions(ixs, user, 0, withdraw_to=self.withdraw_to)
        blockhash, last_valid = await self.chain.blockhash()
        tx = lane.wallet.sign(rtx.compile_message(user, ixs, blockhash))
        sig = str(tx.signatures[0])
        oid = await lane._order(None, {"mode": "live", "mint": None}, "withdraw", "sent", signature=sig,
                                stake_sol=amount / 1e9, note=f"вывод на {dest}")
        await self.chain.send(tx)
        landed = await self.chain.confirm(tx, timeout_s=30.0, resend_s=2.0, last_valid_height=last_valid or None)
        await lane._order_done(oid, landed.state, slot=landed.slot,
                               error=None if landed.state == "landed" else str(landed.err or "не легла в блок"))
        await lane.refresh_balance(force=True)
        sol_usd = lane.sol_usd()
        if landed.state == "landed":
            await lane.say_raw(f"📤 Выведено <b>{amount / 1e9:.4f} SOL</b> (${amount / 1e9 * sol_usd:.2f}) на "
                               f"<code>{dest}</code>\n<a href=\"https://solscan.io/tx/{sig}\">транзакция</a>")
        else:
            await lane.say_raw(f"⚠️ Вывод не прошёл ({'отклонён сетью' if landed.state == 'failed' else 'не лёг в блок'}). "
                               "Деньги на месте, повторите.")
        await self.publish_status()
        return landed.state

    async def archive(self, wid: int) -> bool:
        """Убрать кошелёк из списка: только без позиций и почти пустой. Ключ остаётся в базе зашифрованным."""
        lane = self.lanes.get(wid)
        if lane is None:
            return False
        if lane.positions:
            await lane.say_raw("🗑 Удалить нельзя: есть открытые позиции.")
            return False
        balance = await self.chain.balance(lane.wallet.pubkey) if lane.wallet is not None else 0
        if balance > ARCHIVE_MAX_LAMPORTS:
            await lane.say_raw(f"🗑 Удалить нельзя: на кошельке {balance / 1e9:.4f} SOL. Сначала выведите деньги.")
            return False
        async with self._lock:
            await self.db.execute("UPDATE real_wallets SET archived=true WHERE id=$1", wid)
            self.lanes.pop(wid, None)
            await self.db.kv_set(lane.status_key(), None)
            await self._sync_common_lane()
        await self.say(f"🗑 Кошелёк <b>{fmt.esc(lane.name)}</b> убран из списка. Его ключ остаётся в базе "
                       "зашифрованным: если на адрес что-то придёт, деньги не пропадут.")
        await self.publish_status()
        return True

    # ------------------------------------------------------------------ сообщения и состояние
    async def say(self, text: str, keyboard: list[list[dict[str, Any]]] | None = None) -> None:
        try:
            await self.db.enqueue("text", {"topic": TOPIC, "text": text, "keyboard": keyboard}, priority=8)
        except Exception:  # noqa: BLE001
            log.exception("реальный бот: сообщение")

    async def publish_status(self) -> None:
        self._last_status = time.time()
        try:
            await self.db.kv_set(DESK_KEY, {
                "at": datetime.now(timezone.utc).isoformat(), "vault": self.vault.can_store,
                "vault_note": self.vault_note, "withdraw_to": [str(x) for x in self.withdraw_to],
                "live_allowed": S.live_allowed(), "common": None in self.lanes,
                "common_accepts": bool(self.lanes.get(None) and self.lanes[None].accept_new),
                "wallets": [{"id": wid, "name": lane.name, "pubkey": str(lane.wallet.pubkey) if lane.wallet else None,
                             "mode": lane.mode(), "can_sign": bool(lane.wallet and lane.wallet.can_sign),
                             "balance_sol": None if lane.balance is None else lane.balance / 1e9,
                             "open": len(lane.positions), "note": lane.wallet_note}
                            for wid, lane in self.wallet_lanes().items()]})
        except Exception:  # noqa: BLE001
            log.exception("реальный бот: состояние кошельков")
