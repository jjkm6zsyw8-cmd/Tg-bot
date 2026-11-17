"""Настройки реального бота — все в одном месте.

Каждая настройка: ключ → (подпись, значение по умолчанию, шаг, минимум, максимум, вид, пояснение).
Значения меняются кнопками в /real → «Настройки» и хранятся там же, где остальные настройки бота (таблица
settings); код менять не нужно. Чтобы добавить настройку — допишите строку сюда и используйте get(cfg, "ключ").

Жёсткие пределы, которые из Telegram превысить нельзя, задаются переменными окружения на сервере:
  REAL_LIVE=1              — без этого режим «бой» не включится вовсе
  REAL_MAX_STAKE_USD=30    — потолок ставки на сделку
  REAL_PRIVATE_KEY=...     — ключ кошелька (только здесь; в базе, логах и чате его нет)
  REAL_WALLET=адрес        — публичный адрес для режима «тень», если ключа ещё нет
"""
from __future__ import annotations

import os
from typing import Any

MODES = {"off": "выключен", "shadow": "тень (без денег)", "live": "бой (реальные деньги)"}
# Правила выхода — те же, что у бумажных портфелей (scanner/analytics/paper.py, SCHEMES): решение принимает одна и
# та же функция decide_exit. A — всё на +25%, без стопа, до 30 мин (как бумажные A и M); B — 70% на +25%, остаток
# трейлинг 15%; G — всё на +25%, стоп −8%, 5 мин на +10% (как бумажные G и Y «мой дев»).
SCHEMES = ("A", "B", "G")
SCHEME_KEYS = {"smart": "real_scheme", "mydev": "real_scheme_mydev"}
DEFAULT_SCHEMES = {"real_scheme": "A", "real_scheme_mydev": "G"}

# вид: usd | sol | pct | num | sec | bool | choice
SETTINGS: dict[str, tuple[str, Any, float, float, float, str, str]] = {
    "real_stake_usd": ("Ставка на сделку, $", 5.0, 5.0, 1.0, 1000.0, "usd",
                       "Фиксированная сумма на вход. Потолок — REAL_MAX_STAKE_USD на сервере."),
    "real_max_impact": ("Своя покупка двигает цену не больше чем на", 0.03, 0.01, 0.005, 0.20, "pct",
                        "Ставка урезается так, чтобы собственная покупка не сдвигала цену монеты сильнее."),
    "real_min_stake_usd": ("Мин. ставка, $", 2.0, 1.0, 1.0, 100.0, "usd",
                           "Если после урезания ставка выходит меньше — сделку пропускаем."),
    "real_max_open": ("Макс. открытых позиций", 3, 1, 1, 10, "num", "Больше — новые входы пропускаются."),
    "real_day_stop_usd": ("Дневной стоп, $", 10.0, 1.0, 1.0, 1000.0, "usd",
                          "Убыток за день дошёл до суммы — новых входов нет до полуночи (Батуми)."),
    "real_total_stop_usd": ("Общий стоп, $", 30.0, 5.0, 5.0, 5000.0, "usd",
                            "Убыток с включения боя дошёл до суммы — бой сам переключается в тень."),
    "real_buy_slippage": ("Лимит проскальзывания покупки", 0.10, 0.01, 0.01, 0.50, "pct",
                          "Цена к моменту исполнения ушла дальше — сеть отклонит покупку целиком."),
    "real_sell_slippage": ("Лимит проскальзывания продажи", 0.15, 0.05, 0.05, 0.90, "pct",
                           "Первая попытка продажи; при неудаче лимит расширяется."),
    "real_max_runup": ("Не входить, если цена выше цены трейдера на", 0.15, 0.05, 0.0, 1.0, "pct",
                       "К моменту нашего решения монета уже убежала от цены сигнала — пропуск."),
    "real_max_signal_age_s": ("Макс. возраст сигнала, с", 4.0, 1.0, 1.0, 30.0, "sec",
                              "Сигнал дошёл до исполнителя позже — пропуск."),
    "real_max_blocks": ("Не входить, если после сигнала прошло блоков больше", 10, 2, 2, 100, "num",
                        "Блок ≈ 0.4 с. Бумага считает вход через 2 блока; опоздали сильнее — это уже другая сделка."),
    "real_priority_sol": ("Приоритет на транзакцию, SOL", 0.0003, 0.0001, 0.00001, 0.01, "sol",
                          "Доплата сети за скорость включения в блок (за каждую транзакцию)."),
    "real_tip_sol": ("Чаевые за быструю доставку, SOL", 0.00001, 0.00001, 0.000005, 0.01, "sol",
                     "Helius Sender: от 0.000005 SOL — быстрый канал, от 0.001 SOL — все каналы сразу."),
    "real_max_buys_hour": ("Макс. покупок в час", 20, 5, 1, 200, "num", "Защита от серии входов при сбое."),
    "real_reentry_h": ("После убытка не возвращаться в монету, ч", 24, 6, 0, 168, "num", ""),
    "real_fail_pause": ("Пауза после неудачных транзакций подряд", 5, 1, 2, 50, "num",
                        "Столько неудач подряд — входы останавливаются до команды «Продолжить»."),
    "real_poll_s": ("Проверка цены открытых позиций, с", 5.0, 1.0, 1.0, 30.0, "sec",
                    "Запасной контроль цены боевых позиций через блокчейн, если поток сделок отстал."),
    "real_reserve_sol": ("Неприкосновенный остаток, SOL", 0.02, 0.01, 0.005, 1.0, "sol",
                         "На комиссии и залоги счетов; ниже остатка покупки не делаются."),
    "real_land_s": ("Тень: задержка «посадки» сделки, с", 0.8, 0.4, 0.0, 5.0, "sec",
                    "Через сколько секунд после решения тень берёт цену — как будто сделка легла в блок."),
}
TOGGLES: dict[str, tuple[str, bool, str]] = {
    "real_smart": ("Сигналы smart", True, ""),
    "real_mydev": ("Сигналы «мой дев»", True, ""),
    "real_live_mydev": ("«Мой дев» в бою (иначе только в тени)", False,
                        "Пока выключено, запуски девов исполняются только в тени, даже когда включён бой."),
    "real_mev_protect": ("Обходить валидаторов-«сэндвичеров»", True, "Параметр mev-protect у Helius Sender."),
    "real_close_ata": ("Закрывать пустые счета токенов (возврат залога)", True, "≈0.0015 SOL с каждой монеты."),
    "real_notify_shadow": ("Сообщения о сделках тени", False, ""),
}
DEFAULT_MODE = "shadow"


def get(cfg: dict[str, Any], key: str) -> Any:
    """Значение настройки с учётом пределов."""
    if key in SETTINGS:
        _label, default, _step, lo, hi, kind, _d = SETTINGS[key]
        try:
            v = float(cfg.get(key, default))
        except (TypeError, ValueError):
            v = float(default)
        v = min(hi, max(lo, v))
        if key == "real_stake_usd":
            v = min(v, max_stake_usd())
        return int(v) if kind == "num" else v
    if key in TOGGLES:
        return bool(cfg.get(key, TOGGLES[key][1]))
    if key == "real_mode":
        m = str(cfg.get(key) or DEFAULT_MODE)
        return m if m in MODES else "off"
    if key in DEFAULT_SCHEMES:
        s = str(cfg.get(key) or DEFAULT_SCHEMES[key])
        return s if s in SCHEMES else DEFAULT_SCHEMES[key]
    raise KeyError(key)


def step(cfg: dict[str, Any], key: str, sign: str) -> float:
    """Новое значение настройки после нажатия «−» или «+» (с учётом пределов)."""
    _label, _default, st, lo, hi, kind, _d = SETTINGS[key]
    v = float(get(cfg, key)) + (st if sign == "+" else -st)
    v = min(hi, max(lo, v))
    if key == "real_stake_usd":
        v = min(v, max_stake_usd())
    return int(round(v)) if kind == "num" else round(v, 6)


def max_stake_usd() -> float:
    try:
        return max(1.0, float(os.environ.get("REAL_MAX_STAKE_USD", 30)))
    except ValueError:
        return 30.0


def live_allowed() -> bool:
    """Бой разрешён на сервере (REAL_LIVE=1). Без этого кнопка «бой» ничего не включит."""
    return os.environ.get("REAL_LIVE", "").strip().lower() in ("1", "yes", "true", "да")


def fmt(key: str, v: Any) -> str:
    kind = SETTINGS[key][5]
    if kind == "usd":
        return f"${v:g}"
    if kind == "pct":
        return f"{round(v * 100, 2):g}%"
    if kind == "sol":
        return f"{v:.6f}".rstrip("0").rstrip(".") + " SOL"
    if kind == "sec":
        return f"{v:g} с"
    return f"{v:g}"
