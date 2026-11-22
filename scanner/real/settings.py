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
# та же функция decide_exit. Любое правило можно поставить любому методу:
#   A — всё на +25%, без стопа, до 30 мин (как бумажные A и M);
#   B — 70% на +25%, остаток трейлинг 15%, без стопа, до 30 мин;
#   G — всё на +25%, стоп −8%, нет +10% за 5 мин — выход, до 30 мин (как бумажные G, Y, H, K);
#   R — трейлинг 35% после +30%, стоп −50%, до суток (как бумажный R, «смарт-ракеты»).
SCHEMES = ("A", "B", "G", "R")
SCHEME_KEYS = {"smart": "real_scheme", "mydev": "real_scheme_mydev", "bigcopy": "real_scheme_bigcopy",
               "copy": "real_scheme_copy", "swing": "real_scheme_swing", "trend": "real_scheme_trend"}
DEFAULT_SCHEMES = {"real_scheme": "A", "real_scheme_mydev": "G", "real_scheme_bigcopy": "G", "real_scheme_copy": "G",
                   "real_scheme_swing": "R", "real_scheme_trend": "G"}
SCHEME_TEXT = {"A": "всё на +25%, без стопа, до 30 мин", "B": "70% на +25%, остаток трейлинг 15%, до 30 мин",
               "G": "всё на +25%, стоп −8%, нет +10% за 5 мин — выход", "R": "трейлинг 35% после +30%, стоп −50%, до суток",
               "W": "продать через его обычное время удержания − 2 с (раньше него), или когда он продал"}
# пробы правил выхода: A / B / G / R + W (21.11, идея владельца: выйти раньше трейдера — не попасть под его продажу).
# W — только проба (бот так не продаёт): у сигналов с трейдером, чьё обычное удержание известно (до 30 мин)
TRIAL_RULES = (*SCHEMES, "W")
# Вход smart: «all» — все smart-сигналы (как бумажные A / B / G), «m» — только те, что бумага пустила в боевой
# портфель M (автофильтр по истории сделок трейдеров и правила отбора M). M = вход «m» + правило A + до 3 позиций.
SMART_FILTERS = {"all": "все smart-сигналы", "m": "только отобранные, как у M"}

# вид: usd | sol | pct | num | sec | bool | choice
SETTINGS: dict[str, tuple[str, Any, float, float, float, str, str]] = {
    "real_stake_usd": ("Ставка на сделку, $", 5.0, 5.0, 1.0, 1000.0, "usd",
                       "Фиксированная сумма на вход. Потолок — REAL_MAX_STAKE_USD на сервере."),
    "real_max_impact": ("Своя покупка двигает цену не больше чем на", 0.03, 0.01, 0.005, 0.20, "pct",
                        "Ставка урезается так, чтобы собственная покупка не сдвигала цену монеты сильнее."),
    "real_min_stake_usd": ("Мин. ставка, $", 2.0, 1.0, 1.0, 100.0, "usd",
                           "Если после урезания ставка выходит меньше — сделку пропускаем."),
    "real_max_open": ("Макс. открытых позиций", 3, 1, 1, 10, "num", "Больше — новые входы пропускаются."),
    "real_day_stop_usd": ("Дневной стоп, $", 10.0, 5.0, 5.0, 1000.0, "usd",
                          "Убыток за день дошёл до суммы — новых входов нет до полуночи (Батуми)."),
    "real_total_stop_usd": ("Общий стоп, $", 30.0, 10.0, 10.0, 5000.0, "usd",
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
    "real_poll_s": ("Проверка цены открытых позиций, с", 4.0, 1.0, 1.0, 30.0, "sec",
                    "Цена открытых позиций (бой и тень) прямо из блокчейна — как бумага берёт её каждые 4 с."),
    "real_reserve_sol": ("Неприкосновенный остаток, SOL", 0.02, 0.01, 0.005, 1.0, "sol",
                         "На комиссии и залоги счетов; ниже остатка покупки не делаются."),
    "real_land_s": ("Тень: задержка «посадки» сделки, с", 0.8, 0.4, 0.0, 5.0, "sec",
                    "Через сколько секунд после решения тень берёт цену — как будто сделка легла в блок."),
}
TOGGLES: dict[str, tuple[str, bool, str]] = {
    # 22.11: «торговать только за теми, кого я выбрал» — методы, замер тени и авто-тень не торгуют
    "real_only_follow": ("🎯 Только выбранные мной кошельки", False,
                         "Бот (тень и бой) берёт только покупки кошельков, за которыми ты сам нажал «👻 Тень за ним» "
                         "или «💵 Бой за ним» в карточке. Методы (smart, мой дев, тренды…), замер тени и тень, "
                         "которую бот включил сам, — не торгуют."),
    "real_smart": ("Сигналы smart", True, ""),
    "real_mydev": ("Сигналы «мой дев»", True, ""),
    "real_live_mydev": ("«Мой дев» в бою (иначе только в тени)", False,
                        "Пока выключено, запуски девов исполняются только в тени, даже когда включён бой."),
    "real_copy": ("Копии своего списка и разведки (как бумага H)", False,
                  "Покупки кошельков из вашего списка и кандидатов разведки; выход как G."),
    "real_bigcopy": ("Крупные копии — трейдер зашёл от 5 SOL (как K)", False, "Выход как G."),
    "real_swing": ("Смарт-ракеты — покупки держателей (как R)", False,
                   "Трейлинг 35% после +30%, стоп −50%, держит до суток — занимает место позиции надолго."),
    "real_trend": ("Тренды — монеты, которые уже растут (как T)", False,
                   "Вход после отката, выход как G. Только монеты на кривой pump.fun и в пуле PumpSwap: на других "
                   "биржах реальный бот не торгует."),
    "real_mev_protect": ("Обходить валидаторов-«сэндвичеров»", True, "Параметр mev-protect у Helius Sender."),
    "real_close_ata": ("Закрывать пустые счета токенов (возврат залога)", True, "≈0.0015 SOL с каждой монеты."),
    "real_notify_shadow": ("Сообщения о сделках тени", False, ""),
    # 19.11: бумага до исправлений завышала PumpSwap и «мой дев» — честные цифры по методам даёт тень (симуляция
    # настоящей сделки). Чтобы она считала все сигналы, а не первые 3 до дневного стопа, — замер без ограничений
    "real_shadow_all": ("Тень считает все методы (замер)", True,
                        "Выключенные методы тоже идут в тень — для честной таблицы по каждому методу. На деньги "
                        "по-прежнему только включённые."),
    "real_shadow_free": ("Тень без дневного стопа и лимита позиций", True,
                         "Тень — только симуляция: считает все сигналы (до 30 позиций), а не первые до стопа."),
    # 20.11: у метода одно правило выхода, а сравнить хочется все — пробы считают A / B / G / R на том же входе
    "real_rule_trials": ("Пробовать все правила выхода (A, B, G, R)", True,
                         "На каждый вход бота — 4 пробы с тем же входом и разными правилами выхода. Только расчёт по "
                         "цене в сети: без транзакций и без денег. Итог — в «⚖️ Против бумаги»."),
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
    if key == "real_smart_filter":
        v = str(cfg.get(key) or "all")
        return v if v in SMART_FILTERS else "all"
    raise KeyError(key)


def stake_key(kind: str) -> str:
    return f"real_stake_usd_{kind}"


def method_stake(cfg: dict[str, Any], kind: str | None) -> float | None:
    """Своя ставка метода, $ (None — как у кошелька). Не выше потолка сервера."""
    if not kind:
        return None
    try:
        v = float(cfg.get(stake_key(kind)) or 0)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return min(max(1.0, v), max_stake_usd(), SETTINGS["real_stake_usd"][4])


def method_name(cfg: dict[str, Any], kind: str) -> str:
    """«Smart M», «Smart A», «Smart (как M) → G», «Мой дев G (только тень)», «Копии → A»…"""
    rule = get(cfg, SCHEME_KEYS[kind])
    if kind == "smart":
        if get(cfg, "real_smart_filter") == "m":
            return "Smart M" if rule == "A" else f"Smart (отбор M) → {rule}"
        return f"Smart {rule}"
    base = {"mydev": "Мой дев", "bigcopy": "Крупные копии", "copy": "Копии", "swing": "Смарт-ракеты",
            "trend": "Тренды"}[kind]
    default = DEFAULT_SCHEMES[SCHEME_KEYS[kind]]
    out = f"{base} {rule}" if kind == "mydev" else (base if rule == default else f"{base} → {rule}")
    if kind == "mydev" and not get(cfg, "real_live_mydev"):
        out += " (только тень)"
    return out


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


# Методы торговли: вид сигнала → (название, выключатель, бумажный портфель с тем же входом и выходом).
# У smart и «мой дев» правило выхода выбирается (A / B / G), у остальных — как у их бумажного портфеля.
METHODS: dict[str, tuple[str, str, str]] = {
    "smart": ("Smart", "real_smart", "A"),
    "mydev": ("Мой дев", "real_mydev", "Y"),
    "copy": ("Копии своего списка и разведки", "real_copy", "H"),
    "bigcopy": ("Крупные копии (от 5 SOL)", "real_bigcopy", "K"),
    "swing": ("Смарт-ракеты (держатели)", "real_swing", "R"),
    # 20.11: тренды — монеты, которые уже растут (сигнал DexScreener, вход после отката). Без трейдера: вход по
    # времени сигнала, а не по блоку покупки. Только pump.fun и PumpSwap — другие биржи бот не умеет
    "trend": ("Тренды (монеты, которые уже растут)", "real_trend", "T"),
}
KIND_NAMES = {"smart": "smart", "mydev": "мой дев", "copy": "копия", "bigcopy": "крупная копия",
              "swing": "смарт-ракета", "trend": "тренд"}


def methods_summary(cfg: dict[str, Any]) -> str:
    """Коротко, какими методами торгует кошелёк: «Smart A · Мой дев G (только тень) · Копии»; своя ставка — в скобках."""
    out = []
    for kind in ("smart", "mydev", "bigcopy", "copy", "swing", "trend"):
        if get(cfg, METHODS[kind][1]):
            stake = method_stake(cfg, kind)
            out.append(method_name(cfg, kind) + (f" ${stake:g}" if stake else ""))
    return " · ".join(out) if out else "все выключены"


def paper_portfolio(kind: str, rule: str, smart_filter: str = "all") -> str | None:
    """Бумажный портфель с тем же входом и тем же правилом выхода (для сравнения и для итогов на экранах)."""
    if kind == "smart":
        if smart_filter == "m":
            return "M" if rule == "A" else None
        return rule if rule in ("A", "B", "G") else None
    return {("mydev", "G"): "Y", ("copy", "G"): "H", ("bigcopy", "G"): "K", ("swing", "R"): "R",
            ("trend", "G"): "T"}.get((kind, rule))
