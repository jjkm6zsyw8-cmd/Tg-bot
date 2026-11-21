"""21.11: /paper длиннее 4000 символов резался внутри <i>…</i> — Telegram: «can't find end tag». Теперь по строкам,
теги на стыке закрываются и открываются заново."""
from __future__ import annotations

import re

from scanner.alerts.format import split_html
from scanner.analytics.patterns import split


def balanced(part: str) -> bool:
    stack = []
    for m in re.finditer(r"<(/?)(b|i|u|s|code|pre|a)(\s[^>]*)?>", part):
        if m.group(1):
            if not stack or stack[-1] != m.group(2):
                return False
            stack.pop()
        else:
            stack.append(m.group(2))
    return not stack


def test_long_italic_legend_is_split_with_balanced_tags():
    text = "📒 <b>Бумага</b>\n<pre>" + "\n".join(f"A {i:>5}" for i in range(400)) + "</pre>\n<i>" + "легенда " * 900 + "</i>"
    parts = split_html(text, 3900)
    assert len(parts) > 1 and all(len(p) <= 4096 for p in parts)
    assert all(balanced(p) for p in parts)
    plain = re.sub(r"</?(b|i|pre)>", "", "\n".join(parts))
    assert plain.replace("\n", "").replace(" ", "") == re.sub(r"</?(b|i|pre)>", "", text).replace("\n", "").replace(" ", "")
    assert split(text, 3800) and all(balanced(p) for p in split(text, 3800))


def test_short_text_untouched():
    assert split_html("<b>коротко</b>") == ["<b>коротко</b>"]
