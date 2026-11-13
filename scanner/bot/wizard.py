"""Пошаговые меню на кнопках, чтобы не набирать команды руками.

/import и /addwallet без аргументов спрашивают кнопками: откуда кошельки (Pump.fun, Axiom, GMGN) и как отобраны
(реализованный PnL — закрытые сделки, нереализованный — открытые позиции, нейтрально), потом ждут файл или адреса
одним сообщением. Состояние — у каждого пользователя своё, живёт 10 минут.
"""
from __future__ import annotations

import time
from typing import Any

from ..analytics.origin import ORIGINS, PICKS

WAIT_S = 600
CANCEL = [{"text": "✖ Отмена", "callback": "wz:x"}]
TITLES = {"import": "📥 <b>Импорт кошельков</b>", "add": "➕ <b>Добавить кошелёк</b>"}


def origin_kb() -> list[list[dict[str, Any]]]:
    return [[{"text": ORIGINS[o], "callback": f"wz:o:{o}"} for o in ("pumpfun", "axiom", "gmgn")],
            [{"text": "Другое", "callback": "wz:o:other"}, {"text": "Не помечать", "callback": "wz:o:-"}],
            CANCEL]


def pick_kb() -> list[list[dict[str, Any]]]:
    return [[{"text": "Реализованный PnL (закрытые сделки)", "callback": "wz:p:realized"}],
            [{"text": "Нереализованный PnL (открытые позиции)", "callback": "wz:p:unrealized"}],
            [{"text": "Нейтрально / не знаю", "callback": "wz:p:neutral"}],
            CANCEL]


class Wizard:
    def __init__(self) -> None:
        self.state: dict[int, dict[str, Any]] = {}

    def start(self, uid: int, kind: str) -> tuple[str, list[list[dict[str, Any]]]]:
        self.state[uid] = {"kind": kind, "origin": None, "pick": None, "step": "origin", "until": time.time() + WAIT_S}
        return f"{TITLES[kind]}\nШаг 1 из 3 — откуда кошел{'ьки' if kind == 'import' else 'ёк'}?", origin_kb()

    def get(self, uid: int) -> dict[str, Any] | None:
        st = self.state.get(uid)
        if st is not None and st["until"] < time.time():
            self.state.pop(uid, None)
            return None
        return st

    def waiting_input(self, uid: int) -> dict[str, Any] | None:
        """Ждём файл / адреса от этого пользователя — состояние (и сбрасываем его: ввод принят)."""
        st = self.get(uid)
        if st is None or st["step"] != "input":
            return None
        self.state.pop(uid, None)
        return st

    def on_button(self, uid: int, data: str) -> tuple[str, list[list[dict[str, Any]]] | None]:
        """Нажатие кнопки мастера → новый текст и кнопки сообщения."""
        st = self.get(uid)
        if data == "wz:x" or st is None:
            self.state.pop(uid, None)
            return ("✖ Отменено." if data == "wz:x" else "⌛ Время вышло — начните заново командой."), None
        kind, value = data[3:4], data[5:]
        if kind == "o" and st["step"] == "origin":
            st["origin"] = None if value == "-" else value
            st["step"] = "pick"
            return f"{TITLES[st['kind']]}\nИсточник: {self._origin(st)}\nШаг 2 из 3 — как отобраны?", pick_kb()
        if kind == "p" and st["step"] == "pick":
            st["pick"] = value if value in PICKS else None
            st["step"] = "input"
            st["until"] = time.time() + WAIT_S
            ask = ("файл экспорта (Axiom JSON/CSV/TXT) или адреса одним сообщением — адрес и подпись, "
                   "по одному в строке" if st["kind"] == "import" else
                   "адрес кошелька и метку одним сообщением, например: <code>адрес лега</code>")
            return (f"{TITLES[st['kind']]}\nИсточник: {self._origin(st)} · отбор: {PICKS.get(st['pick'] or '', '—')}"
                    f"\nШаг 3 из 3 — <b>ответом на это сообщение</b> пришлите {ask}. Жду 10 минут.", [CANCEL])
        return "Нажмите одну из кнопок выше.", None

    @staticmethod
    def _origin(st: dict[str, Any]) -> str:
        return ORIGINS.get(st.get("origin") or "", "не помечен")
