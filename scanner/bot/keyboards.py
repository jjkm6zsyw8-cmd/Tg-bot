"""JSON-описание кнопок → InlineKeyboardMarkup."""
from __future__ import annotations

from typing import Any

from aiogram.types import CopyTextButton, InlineKeyboardButton, InlineKeyboardMarkup


def build(spec: list[list[dict[str, Any]]] | None, styles: bool = True) -> InlineKeyboardMarkup | None:
    if not spec:
        return None
    rows = []
    for row in spec:
        out = []
        for b in row:
            kw: dict[str, Any] = {"text": b["text"]}
            if "url" in b:
                kw["url"] = b["url"]
            elif "copy" in b:
                kw["copy_text"] = CopyTextButton(text=b["copy"])   # адрес в буфер одним нажатием
            elif "callback" in b:
                kw["callback_data"] = b["callback"][:64]
            else:
                continue
            if styles and b.get("style"):
                kw["style"] = b["style"]                            # danger — красная кнопка выхода
            out.append(InlineKeyboardButton(**kw))
        if out:
            rows.append(out)
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None
