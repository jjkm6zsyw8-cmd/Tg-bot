"""Конфигурация: секреты из переменных окружения, пороги и шаблоны — из YAML.

YAML перечитывается на лету (по mtime), поэтому пороги и шаблоны ссылок меняются
без пересборки. Пользовательские значения из /settings хранятся в таблице settings
и накладываются поверх раздела defaults (см. Settings.effective).
"""
from __future__ import annotations

import copy
import os
import time
from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict


class Env(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    bot_token: str = ""
    # Белый список Telegram user_id через запятую
    allowed_user_ids: str = ""
    # Супергруппа с топиками (id вида -100...). Пусто — алерты уходят в личку первому user_id.
    alert_chat_id: int | None = None

    database_url: str = "postgresql://scanner:scanner@localhost:5432/scanner"

    pumpportal_ws: str = "wss://pumpportal.fun/api/data"
    pumpportal_api_key: str = ""
    helius_api_key: str = ""
    helius_ws: str = ""  # по умолчанию wss://atlas-mainnet.helius-rpc.com/?api-key=...
    rpc_url: str = "https://api.mainnet-beta.solana.com"

    redirect_base: str = "https://example.com"
    web_host: str = "0.0.0.0"
    web_port: int = 8080

    config_path: str = "config/config.yaml"
    log_level: str = "INFO"

    @property
    def allowed_ids(self) -> set[int]:
        out: set[int] = set()
        for part in self.allowed_user_ids.replace(" ", "").split(","):
            if part.lstrip("-").isdigit():
                out.add(int(part))
        return out

    @property
    def helius_ws_url(self) -> str:
        if self.helius_ws:
            return self.helius_ws
        if self.helius_api_key:
            return f"wss://atlas-mainnet.helius-rpc.com/?api-key={self.helius_api_key}"
        return ""


class YamlConfig:
    """YAML-конфиг с перечитыванием по изменению файла."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._data: dict[str, Any] = {}
        self._mtime = 0.0
        self._checked = 0.0
        self.reload(force=True)

    def reload(self, force: bool = False) -> bool:
        try:
            mtime = self.path.stat().st_mtime
        except FileNotFoundError:
            return False
        if not force and mtime == self._mtime:
            return False
        with self.path.open(encoding="utf-8") as fh:
            self._data = yaml.safe_load(fh) or {}
        self._mtime = mtime
        return True

    @property
    def data(self) -> dict[str, Any]:
        now = time.monotonic()
        if now - self._checked > 10:
            self._checked = now
            self.reload()
        return self._data

    def section(self, name: str) -> dict[str, Any]:
        return copy.deepcopy(self.data.get(name) or {})


class Settings:
    """Эффективные настройки = defaults из YAML + переопределения пользователя."""

    def __init__(self, yaml_cfg: YamlConfig, overrides: dict[str, Any] | None = None):
        self.yaml = yaml_cfg
        self.overrides: dict[str, Any] = overrides or {}

    def effective(self) -> dict[str, Any]:
        base = self.yaml.section("defaults")
        for key, value in self.overrides.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                base[key] = {**base[key], **value}
            else:
                base[key] = value
        return base

    def get(self, key: str, default: Any = None) -> Any:
        return self.effective().get(key, default)

    def filters(self) -> dict[str, float]:
        eff = self.effective()
        presets = self.yaml.section("presets")
        preset = presets.get(eff.get("filter_preset", "normal")) or presets.get("normal") or {}
        custom = eff.get("filter_overrides") or {}
        return {**preset, **custom}

    def costs(self) -> dict[str, float]:
        return self.yaml.section("costs")

    def links(self) -> dict[str, Any]:
        return self.yaml.section("links")

    def reliability(self) -> dict[str, Any]:
        return self.yaml.section("reliability")

    def topics(self) -> dict[str, str]:
        return self.yaml.section("topics")

    def exchanges(self) -> set[str]:
        return set(self.yaml.data.get("exchanges") or [])


# Описание редактируемых через /settings параметров: ключ → (подпись, шаг, мин, макс, формат)
EDITABLE: dict[str, tuple[str, float, float, float, str]] = {
    "min_net_profit": ("Порог чистого профита", 0.05, 0.05, 3.0, "pct"),
    "min_repeatability": ("Повторяемость", 0.05, 0.1, 1.0, "pct"),
    "max_entry_mc_sol": ("Макс. MC входа, SOL (0 — нет)", 10, 0, 2000, "num"),
    "reaction_s": ("Время реакции, с", 1, 1, 30, "num"),
    "position_sol": ("Позиция, SOL", 0.1, 0.05, 50, "num"),
    "smart_min_buy_sol": ("Мин. покупка трейдера, SOL", 0.1, 0.05, 50, "num"),
    "max_cluster_share": ("Макс. доля кластера", 0.05, 0.05, 1.0, "pct"),
}

PRESETS = ("cautious", "normal", "aggressive")
PRESET_NAMES = {"cautious": "Осторожный", "normal": "Обычный", "aggressive": "Агрессивный"}


def load_env() -> Env:
    return Env()

