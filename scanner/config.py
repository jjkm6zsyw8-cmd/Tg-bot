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
from pydantic import field_validator
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
    helius_ws: str = ""  # Enhanced WebSockets (платный Helius), напр. wss://atlas-mainnet.helius-rpc.com/?api-key=...
    logs_ws: str = ""    # websocket для logsSubscribe; по умолчанию выводится из RPC_URL (https → wss)
    rpc_rps: float = 5   # лимит запросов к RPC в секунду (бесплатный Helius — 10)
    helius_stream: str = "backup"   # off | backup (только при обрыве PumpPortal) | primary
    logs_stream: str = "off"        # on — постоянный logsSubscribe (дорого: ~1,7 млн кредитов/сутки)
    watch_stream: str = "on"        # on — сделки только нужных кошельков и монет через Helius (дёшево)
    # запасной узел, если основной (Helius) отвечает 429 — например, кончились кредиты месяца; "" — без запаса
    rpc_fallback_url: str = "https://api.mainnet-beta.solana.com"

    # внешние источники (необязательно; без ключа источник выключен)
    gmgn_api_key: str = ""
    birdeye_api_key: str = ""
    solscan_api_key: str = ""
    jupiter_api_key: str = ""
    external_config_path: str = "config/external.yaml"
    rpc_url: str = "https://api.mainnet-beta.solana.com"

    redirect_base: str = "https://example.com"
    web_host: str = "0.0.0.0"
    web_port: int = 8080

    config_path: str = "config/config.yaml"
    log_level: str = "INFO"

    @field_validator("bot_token", mode="before")
    @classmethod
    def _clean_token(cls, v: Any) -> Any:
        """Пробелы, кавычки и префикс «bot» при вставке токена — частая ошибка."""
        if isinstance(v, str):
            v = v.strip().strip("'\"").strip()
            if v.lower().startswith("bot") and ":" in v:
                v = v[3:]
        return v

    @property
    def allowed_ids(self) -> set[int]:
        out: set[int] = set()
        for part in self.allowed_user_ids.replace(" ", "").split(","):
            if part.lstrip("-").isdigit():
                out.add(int(part))
        return out

    @property
    def helius_key(self) -> str:
        """HELIUS_API_KEY или ключ из RPC_URL вида ...helius-rpc.com/?api-key=KEY."""
        if self.helius_api_key:
            return self.helius_api_key
        if "helius-rpc.com" in self.rpc_url and "api-key=" in self.rpc_url:
            return self.rpc_url.split("api-key=", 1)[1].split("&", 1)[0]
        return ""

    @property
    def rpc_endpoint(self) -> str:
        """RPC только через Helius: публичный mainnet-beta заменяется, если есть ключ Helius."""
        public = (not self.rpc_url) or "api.mainnet-beta.solana.com" in self.rpc_url
        if public and self.helius_key:
            return f"https://mainnet.helius-rpc.com/?api-key={self.helius_key}"
        return self.rpc_url

    @property
    def helius_ws_url(self) -> str:
        """Helius transactionSubscribe (LaserStream WebSocket): тариф Developer+, трафик 20 кредитов/МБ."""
        if self.helius_stream == "off":
            return ""
        if self.helius_ws:
            return self.helius_ws
        return f"wss://mainnet.helius-rpc.com/?api-key={self.helius_key}" if self.helius_key else ""

    @property
    def watch_ws_url(self) -> str:
        """Стандартный websocket Helius для подписок на отдельные адреса (WATCH_STREAM=off — выключить)."""
        if self.watch_stream != "on":
            return ""
        if self.logs_ws:
            return self.logs_ws
        if self.helius_key:
            return f"wss://mainnet.helius-rpc.com/?api-key={self.helius_key}"
        if self.rpc_url.startswith("https://") and "api.mainnet-beta.solana.com" not in self.rpc_url:
            return "wss://" + self.rpc_url[len("https://"):]
        return ""

    @property
    def watch_fallback_ws(self) -> str:
        u = self.rpc_fallback_url
        return ("wss://" + u[len("https://"):]) if u.startswith("https://") else ""

    @property
    def logs_ws_url(self) -> str:
        """websocket RPC для logsSubscribe. Тарифицируется как стриминг — по умолчанию выключен."""
        if self.logs_stream != "on":
            return ""
        if self.logs_ws:
            return self.logs_ws
        if self.rpc_url.startswith("https://") and "api.mainnet-beta.solana.com" not in self.rpc_url:
            return "wss://" + self.rpc_url[len("https://"):]
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
        return {"new": "Новые монеты", "trend": "Тренды", "paper": "Бумажная торговля", "copy": "Копитрейд",
                **self.yaml.section("topics")}

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
    "new_min_buyers": ("Новые: мин. покупателей", 5, 5, 200, "num"),
    "new_min_volume_sol": ("Новые: мин. объём, SOL", 2, 1, 500, "num"),
    "new_mc_min_sol": ("Новые: мин. MC, SOL", 5, 28, 1000, "num"),
    "paper_priority_sol": ("Бумага: приоритетная комиссия, SOL", 0.0001, 0, 0.01, "num"),
    "copy_ratio": ("Копия: доля от суммы цели", 0.05, 0.01, 1.0, "pct"),
    "copy_min_buy_sol": ("Копия: мин. покупка цели, SOL", 0.05, 0.01, 10, "num"),
    "antipvp_hold_s": ("Анти-PVP: продал быстрее, с", 5, 5, 300, "num"),
    "copy_max_delta": ("Копия: макс. дельта входа", 0.05, 0.05, 1.0, "pct"),
    "copy_dev_min_gap_min": ("Мой дев: пауза между запусками, мин", 1, 0, 60, "num"),
}

PRESETS = ("cautious", "normal", "aggressive")
PRESET_NAMES = {"cautious": "Осторожный", "normal": "Обычный", "aggressive": "Агрессивный"}


def load_env() -> Env:
    return Env()

