"""Импорт списка кошельков: файл из трекера Axiom (JSON/CSV/TXT) или просто адреса в сообщении.

Формат файла заранее неизвестен, поэтому разбор «всеядный»: в JSON ищем строки-адреса Solana и подписи
рядом с ними (name/label/alias/nickname/emoji), в тексте — адреса по строкам, остаток строки — подпись.
"""
from __future__ import annotations

import json
import re
from typing import Any

from ..solana import is_valid_address

B58 = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")
NAME_KEYS = ("name", "label", "alias", "nick", "title", "tag", "emoji")
ADDR_KEYS = ("address", "wallet", "walletaddress", "trackedwalletaddress", "pubkey", "publickey", "owner")
MAX_WALLETS = 300


def _addr(v: Any) -> str | None:
    if isinstance(v, str):
        v = v.strip()
        if 32 <= len(v) <= 44 and is_valid_address(v):
            return v
    return None


def _walk(node: Any, out: dict[str, str | None]) -> None:
    if isinstance(node, dict):
        addr: str | None = None
        names: list[str] = []
        for k, v in node.items():
            key = str(k).lower().replace("_", "").replace("-", "")
            a = _addr(v)
            if a and (addr is None or key in ADDR_KEYS):
                addr = a
            elif isinstance(v, str) and v.strip() and any(n in key for n in NAME_KEYS):
                names.append(v.strip())
            ka = _addr(k)
            if ka:   # {"<адрес>": "подпись"} или {"<адрес>": {...}}
                label = v.strip() if isinstance(v, str) else None
                if isinstance(v, dict):
                    label = next((str(x).strip() for kk, x in v.items()
                                  if isinstance(x, str) and any(n in str(kk).lower() for n in NAME_KEYS)), None)
                out.setdefault(ka, (label or None) and label[:64])
        if addr:
            label = " ".join(names)[:64] or None
            if out.get(addr) is None:
                out[addr] = label
        for v in node.values():
            if isinstance(v, (dict, list)):
                _walk(v, out)
    elif isinstance(node, list):
        for v in node:
            a = _addr(v)
            if a:
                out.setdefault(a, None)
            else:
                _walk(v, out)


def extract_wallets(text: str) -> list[tuple[str, str | None]]:
    """[(адрес, подпись или None)] без повторов, в порядке появления, не больше 300."""
    out: dict[str, str | None] = {}
    stripped = text.strip()
    if stripped[:1] in "[{":
        try:
            _walk(json.loads(stripped), out)
        except ValueError:
            pass
    if not out:
        for line in text.splitlines():
            found = [a for a in B58.findall(line) if is_valid_address(a)]
            if not found:
                continue
            rest = B58.sub(" ", line)
            label = re.sub(r"[,;\t|\"'{}\[\]]+", " ", rest).strip(" -:=").strip()
            label = re.sub(r"\s+", " ", label)[:64] or None
            for a in found:
                out.setdefault(a, label)
    return list(out.items())[:MAX_WALLETS]
