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
SCHEMES = ("A", "B", "G", "R", "L", "D", "U", "V", "P", "Q")
# 23.11: правила по разбору трендов на других биржах (20–22.11, путь цены по свечам): U — трейлинг 15% после +20%,
# стоп −20% (+41% на сделку); V — трейлинг 15% после +50%, без стопа (+42%); P — тейк +75%, стоп −20% (+24%);
# Q — просто держать 30 мин (+33%). Все — не дольше 30 мин (столько видно в данных)
EXTRA_RULES: dict[str, dict[str, Any]] = {
    "U": {"stop": 0.20, "tp_net": None, "tp_share": 0.0, "trail": 0.15, "trail_after_net": 0.20, "target": False,
          "time_stop_s": None, "time_stop_gain": None, "max_hold_s": 1800},
    "V": {"stop": None, "tp_net": None, "tp_share": 0.0, "trail": 0.15, "trail_after_net": 0.50, "target": False,
          "time_stop_s": None, "time_stop_gain": None, "max_hold_s": 1800},
    "P": {"stop": 0.20, "tp_net": 0.75, "tp_share": 1.0, "trail": None, "target": False, "time_stop_s": None,
          "time_stop_gain": None, "max_hold_s": 1800},
    "Q": {"stop": None, "tp_net": None, "tp_share": 0.0, "trail": None, "target": False, "time_stop_s": None,
          "time_stop_gain": None, "max_hold_s": 1800},
}
# 23.11: D — «по почерку дева» (как владелец торговал руками): продать раньше дева — на цели из его истории (чуть ниже
# нижней четверти его первых продаж), или сразу, когда дев начал продавать; стоп −30%, не дольше 5 мин. Только «мой дев»
DEV_SCHEME = {"stop": 0.30, "tp_net": None, "tp_share": 0.0, "trail": None, "target": True, "time_stop_s": None,
              "time_stop_gain": None, "max_hold_s": 300}
# 23.11: L — «лотерея до PumpSwap» (метод «новый дев»): без стопа и тейка, держать, пока монета не вырастет ×3, дальше
# трейлинг 40%; продажа дева её не выкидывает; не дольше 1 ч (за час — 91% переездов на PumpSwap из тех, что за 3 ч). Потеря ограничена «полом» кривой pump.fun: при входе
# на MC ~45–65 SOL монета, которую все бросили, возвращается к ~28 SOL — около −40…−57%
LOTTERY = {"stop": None, "tp_net": None, "tp_share": 0.0, "trail": 0.40, "trail_after_net": 2.0, "target": False,
           "time_stop_s": None, "time_stop_gain": None, "max_hold_s": 3600, "ignore_emergency": True,
           "ignore_trader_sell": True}
SCHEME_KEYS = {"smart": "real_scheme", "mydev": "real_scheme_mydev", "bigcopy": "real_scheme_bigcopy",
               "copy": "real_scheme_copy", "swing": "real_scheme_swing", "trend": "real_scheme_trend",
               "newdev": "real_scheme_newdev", "trendj": "real_scheme_trendj"}
DEFAULT_SCHEMES = {"real_scheme": "A", "real_scheme_mydev": "D", "real_scheme_bigcopy": "G", "real_scheme_copy": "G",
                   "real_scheme_swing": "R", "real_scheme_trend": "U", "real_scheme_newdev": "L",
                   "real_scheme_trendj": "U"}
SCHEME_TEXT = {"A": "всё на +25%, без стопа, до 30 мин", "B": "70% на +25%, остаток трейлинг 15%, до 30 мин",
               "G": "всё на +25%, стоп −8%, нет +10% за 5 мин — выход", "R": "трейлинг 35% после +30%, стоп −50%, до суток",
               "L": "лотерея до PumpSwap: держать до ×3, дальше трейлинг 40%, без стопа, до 1 ч",
               "D": "по почерку дева: продать раньше него — на его обычной цене продажи или сразу, как он начал "
                    "продавать; стоп −30%, до 5 мин",
               "U": "трейлинг 15% после +20%, стоп −20%, до 30 мин",
               "V": "трейлинг 15% после +50%, без стопа, до 30 мин",
               "P": "всё на +75%, стоп −20%, до 30 мин",
               "Q": "просто держать 30 мин",
               "W": "продать через его обычное время удержания − 2 с (раньше него), или когда он продал"}
# пробы правил выхода: A / B / G / R + W (21.11, идея владельца: выйти раньше трейдера — не попасть под его продажу).
# W — только проба (бот так не продаёт): у сигналов с трейдером, чьё обычное удержание известно (до 30 мин)
TRIAL_RULES = (*(r for r in SCHEMES if r != "D"), "W")      # D — только у «мой дев», меряется самой позицией
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
    # 23.11: защита прибыли дня и короткие сообщения о крупном плюсе (просьба владельца)
    "real_big_win_usd": ("Сообщить о сделке с плюсом от, $", 20.0, 5.0, 5.0, 1000.0, "usd", "«💰 +$27 · $SYM»."),
    "real_day_note_usd": ("Сообщать о плюсе за день каждые, $", 40.0, 10.0, 10.0, 10000.0, "usd", ""),
    "real_lock_after_usd": ("Беречь прибыль дня, начиная с, $", 20.0, 5.0, 5.0, 10000.0, "usd", ""),
    "real_lock_keep": ("Сколько прибыли дня беречь", 0.5, 0.1, 0.1, 0.9, "pct",
                       "Плюс дня упал ниже этой доли от лучшего — входов нет до полуночи."),
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
    "real_jup_slippage": ("Jupiter: лимит проскальзывания", 0.05, 0.01, 0.01, 0.30, "pct", ""),
    "real_trendj_live_min_age_min": ("Др. биржи в бой: возраст от, мин", 60, 15, 0, 1440, "num", ""),
    "real_trendj_live_min_lp_locked": ("Др. биржи в бой: LP закрыт от", 0.9, 0.1, 0.0, 1.0, "pct", ""),
    "real_land_s": ("Тень: задержка «посадки» сделки, с", 0.8, 0.4, 0.0, 5.0, "sec",
                    "Через сколько секунд после решения тень берёт цену — как будто сделка легла в блок."),
}
TOGGLES: dict[str, tuple[str, bool, str]] = {
    # 22.11: «торговать только за теми, кого я выбрал»; 24.11: методы при этом меряются в тени (без денег)
    "real_only_follow": ("🎯 На деньги — только выбранные мной кошельки", False,
                         "В бой идут только покупки кошельков, за которыми ты сам нажал «💵 Бой за ним». Методы "
                         "(smart, мой дев, тренды…) и тень, которую бот включил сам, — только в тени, без денег."),
    "real_smart": ("Сигналы smart", True, ""),
    "real_mydev": ("Сигналы «мой дев»", True, ""),
    "real_autofollow": ("Авто-тень за кошельками с плюсом", True, ""),
    "real_mydev_strong_only": ("«Мой дев»: только девы с почерком", True,
                               "3+ продажи и ×1.5 раньше его продажи в 60%+ запусков."),
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
    "real_newdev": ("Новый дев с крупной покупкой — лотерея до PumpSwap", False,
                    "Дев без монет за 7 дней купил на запуске 0.5–2 SOL или от 10 SOL — вход сразу (быстрый путь), "
                    "только если цена ещё низкая; держать до роста ×3 (дальше трейлинг 40%), без стопа, до 1 ч. "
                    "Пока проверяется в тени."),
    "real_trendj": ("Тренды на других биржах (Raydium, Meteora…)", False,
                    "Монеты трендов не с pump.fun: вход и выход по котировке Jupiter. В бой — только отдельной "
                    "кнопкой «💵 Торговать и в бою» на экране метода."),
    # 24.11: бой через Jupiter — только по кнопке владельца (экран метода, с подтверждением); автонастройка, «включить
    # все» и общий экран настроек его не трогают
    "real_live_trendj": ("«Тренды на других биржах» в бою (иначе только в тени)", False,
                         "Пока выключено, тренды других бирж — только тень, даже когда кошелёк в бою."),
    "real_jup_dry_run": ("Сухой прогон Jupiter в тени", True, "Собрать и проверить транзакцию, не отправляя."),
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
    "real_rule_trials": ("Пробовать все правила выхода (A…Q)", True,
                         "Тот же вход — все правила выхода; только расчёт, без денег."),
}
DEFAULT_MODE = "shadow"
# бой отдельного метода — только кнопкой на экране метода (с подтверждением у трендов других бирж)
LIVE_TOGGLES = {"real_live_mydev", "real_live_trendj"}


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
            "trend": "Тренды", "newdev": "Новый дев", "trendj": "Тренды (др. биржи)"}[kind]
    default = DEFAULT_SCHEMES[SCHEME_KEYS[kind]]
    out = f"{base} {rule}" if kind == "mydev" else (base if rule == default else f"{base} → {rule}")
    if kind == "mydev" and not get(cfg, "real_live_mydev"):
        out += " (только тень)"
    if kind == "trendj" and not get(cfg, "real_live_trendj"):
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
    # 23.11: из анализа 128 тыс. запусков — монеты нового дева (0 монет за 7 дней), купившего на запуске 10+ SOL,
    # уходят на PumpSwap в 8.5–9.8% случаев (все монеты — 1.6–2.3%); вход сразу на запуске, выход L. Только тень,
    # пока нет результата на реальных ценах; бой — только по выключателю владельца
    "newdev": ("Новый дев с крупной покупкой (лотерея до PumpSwap)", "real_newdev", "Z"),
    # 23.11: тренды на монетах НЕ с pump.fun (Raydium, Meteora и др.) — бумага T 20–22.11: 85 сделок, в плюсе каждый
    # день, +$91 (+10.7% на сделку), а на pump.fun −$26 на 76. Реальный бот там не торгует — тень по котировке
    # Jupiter (без транзакции), бой — только когда появится покупка через Jupiter
    "trendj": ("Тренды на других биржах (Raydium, Meteora…) — тень по Jupiter", "real_trendj", "TJ"),
}
KIND_NAMES = {"smart": "smart", "mydev": "мой дев", "copy": "копия", "bigcopy": "крупная копия",
              "swing": "смарт-ракета", "trend": "тренд", "newdev": "новый дев", "trendj": "тренд (др. биржи)"}


def live_methods(cfg: dict[str, Any]) -> list[str]:
    """24.11: какие методы пойдут на деньги, если кошелёк в бою (у «мой дев» и трендов других бирж — ещё и своя
    кнопка боя). «На деньги — только выбранные мной кошельки» — ни один метод."""
    if get(cfg, "real_only_follow"):
        return []
    out = []
    for k, (_name, on_key, _port) in METHODS.items():
        if not get(cfg, on_key) or (k == "mydev" and not get(cfg, "real_live_mydev")) \
                or (k == "trendj" and not get(cfg, "real_live_trendj")):
            continue
        out.append(KIND_NAMES.get(k, k))
    return out


def methods_summary(cfg: dict[str, Any]) -> str:
    """Коротко, какими методами торгует кошелёк: «Smart A · Мой дев G (только тень) · Копии»; своя ставка — в скобках."""
    out = []
    for kind in ("smart", "mydev", "bigcopy", "copy", "swing", "trend", "newdev", "trendj"):
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
