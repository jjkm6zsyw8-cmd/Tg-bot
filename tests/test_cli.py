"""25.11: jupcheck и trendfeat были в run_tool, но не в списке разовых команд — сервер отвечал «неизвестная команда».
Каждая ветка run_tool должна быть в TOOLS."""
from __future__ import annotations

import inspect
import re

import scanner.__main__ as cli


def test_every_tool_branch_is_reachable():
    branches = set(re.findall(r'cmd == "([a-z-]+)"', inspect.getsource(cli.run_tool)))
    assert branches and branches <= set(cli.TOOLS), branches - set(cli.TOOLS)
    assert {"copycheck", "jupcheck", "trendfeat"} <= set(cli.TOOLS)
