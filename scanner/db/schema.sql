-- Схема Pump.fun Dev Scanner (разд. 11 ТЗ). Идемпотентна: применяется при каждом старте.

CREATE TABLE IF NOT EXISTS tokens (
    mint            text PRIMARY KEY,
    id              bigserial UNIQUE,
    signature       text NOT NULL,
    deployer        text NOT NULL,           -- исходный деплоер, не меняется
    current_owner   text NOT NULL,           -- текущий владелец (передача владения с янв. 2026)
    name            text,
    symbol          text,
    uri             text,
    image           text,
    description     text,
    twitter         text,
    telegram        text,
    website         text,
    bonding_curve   text,
    created_slot    bigint,
    created_at      timestamptz NOT NULL,    -- время блока (или приёма, если блок неизвестен)
    received_at     timestamptz NOT NULL DEFAULT now(),
    initial_mc_sol  double precision,
    initial_buy_sol double precision,
    migrated_at     timestamptz,
    amm_pool        text,
    cluster_id      bigint,
    source          text
);
CREATE INDEX IF NOT EXISTS tokens_deployer_idx ON tokens (deployer);
CREATE INDEX IF NOT EXISTS tokens_created_idx ON tokens (created_at);
CREATE INDEX IF NOT EXISTS tokens_cluster_idx ON tokens (cluster_id);
CREATE INDEX IF NOT EXISTS tokens_image_idx ON tokens (image) WHERE image IS NOT NULL;

CREATE TABLE IF NOT EXISTS trades (
    id            bigserial PRIMARY KEY,
    signature     text NOT NULL,
    mint          text NOT NULL,
    trader        text NOT NULL,
    is_buy        boolean NOT NULL,
    sol_amount    double precision NOT NULL,
    token_amount  double precision NOT NULL,
    mc_sol        double precision NOT NULL,
    v_sol         double precision,
    v_tokens      double precision,
    real_tokens   double precision,
    new_balance   double precision,
    fee_sol       double precision,
    pool          text NOT NULL DEFAULT 'pump',
    slot          bigint,
    ts            timestamptz NOT NULL,
    received_at   timestamptz NOT NULL DEFAULT now(),
    source        text,
    UNIQUE (signature, mint, trader, is_buy)  -- дедуп по сигнатуре между источниками
);
CREATE INDEX IF NOT EXISTS trades_mint_ts_idx ON trades (mint, ts);
CREATE INDEX IF NOT EXISTS trades_trader_ts_idx ON trades (trader, ts);
CREATE INDEX IF NOT EXISTS trades_ts_idx ON trades (ts);

CREATE TABLE IF NOT EXISTS sol_transfers (
    id         bigserial PRIMARY KEY,
    signature  text NOT NULL,
    src        text NOT NULL,
    dst        text NOT NULL,
    sol        double precision NOT NULL,
    slot       bigint,
    ts         timestamptz NOT NULL,
    UNIQUE (signature, src, dst, sol)
);
CREATE INDEX IF NOT EXISTS sol_transfers_src_idx ON sol_transfers (src, ts);
CREATE INDEX IF NOT EXISTS sol_transfers_dst_idx ON sol_transfers (dst, ts);

CREATE TABLE IF NOT EXISTS wallets (
    address        text PRIMARY KEY,
    first_seen     timestamptz NOT NULL DEFAULT now(),
    created_est    timestamptz,               -- оценка времени создания (первая подпись)
    funded_by      text,
    funded_at      timestamptz,
    funded_sol     double precision,
    is_exchange    boolean NOT NULL DEFAULT false,
    label          text,
    kind           text,                      -- dev | trader
    manual         boolean NOT NULL DEFAULT false,
    blacklisted    boolean NOT NULL DEFAULT false,
    checked_at     timestamptz
);

CREATE TABLE IF NOT EXISTS clusters (
    id                 bigserial PRIMARY KEY,
    label              text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    priority           boolean NOT NULL DEFAULT false,
    threshold_override double precision,       -- пониженный порог после «Успешно»
    pattern_weight     double precision NOT NULL DEFAULT 1.0,
    blacklisted        boolean NOT NULL DEFAULT false,
    blacklist_reason   text,
    profile            jsonb,
    profile_updated_at timestamptz,
    qualified_at       timestamptz              -- первый раз прошёл пороги профиля
);

CREATE TABLE IF NOT EXISTS cluster_links (
    id          bigserial PRIMARY KEY,
    cluster_id  bigint NOT NULL REFERENCES clusters (id) ON DELETE CASCADE,
    wallet      text NOT NULL,
    reason      text NOT NULL,
    confidence  text NOT NULL CHECK (confidence IN ('high', 'medium', 'low')),
    status      text NOT NULL DEFAULT 'auto' CHECK (status IN ('auto', 'proposed', 'confirmed', 'rejected')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    decided_at  timestamptz,
    UNIQUE (cluster_id, wallet)
);
CREATE INDEX IF NOT EXISTS cluster_links_wallet_idx ON cluster_links (wallet);

CREATE TABLE IF NOT EXISTS launches (
    mint                text PRIMARY KEY REFERENCES tokens (mint) ON DELETE CASCADE,
    cluster_id          bigint,
    deployer            text NOT NULL,
    created_at          timestamptz NOT NULL,
    initial_mc          double precision,
    peak_mc             double precision,
    dump_mc             double precision,
    time_to_dump_s      double precision,
    sold_share          double precision,
    cluster_share_start double precision,
    early_buyers        integer,
    achievable_profit   double precision,
    entry_mc            double precision,
    mc_30s              double precision,
    mc_60s              double precision,
    mc_120s             double precision,
    dev_buy_sol         double precision,
    bundle_wallets      integer,
    bundle_share        double precision,
    migrated            boolean NOT NULL DEFAULT false,
    features            jsonb NOT NULL DEFAULT '{}'::jsonb,
    is_reference        boolean NOT NULL DEFAULT false,
    weight              double precision NOT NULL DEFAULT 1.0,
    finalized_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS launches_cluster_idx ON launches (cluster_id, created_at);
CREATE INDEX IF NOT EXISTS launches_created_idx ON launches (created_at);

CREATE TABLE IF NOT EXISTS traders (
    address       text PRIMARY KEY,
    label         text,
    tier          text,                         -- excellent | good
    manual        boolean NOT NULL DEFAULT false,
    active        boolean NOT NULL DEFAULT true, -- в списке отслеживания
    qualified     boolean NOT NULL DEFAULT false,
    qualified_at  timestamptz,
    pnl_7d        double precision,
    pnl_30d       double precision,
    copy_7d       double precision,
    copy_30d      double precision,
    win_rate      double precision,
    trades_30d    integer,
    median_hold_s double precision,
    avg_entry_mc  double precision,
    snipe_share   double precision,
    linked_to_dev boolean NOT NULL DEFAULT false,
    last_active   timestamptz,
    is_kol        boolean NOT NULL DEFAULT false,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
    id            bigserial PRIMARY KEY,
    mint          text NOT NULL,
    mode          text NOT NULL,                -- dev | smart | new_dev_wallet
    level         text NOT NULL,                -- strong | normal | watch
    signal        text,                         -- single | consensus | dev_match ...
    trader        text,
    cluster_id    bigint,
    mc_alert      double precision,
    entry_mc      double precision,
    target_mc     double precision,
    dump_mc       double precision,
    potential     double precision,
    net_profit    double precision,
    reasons       jsonb NOT NULL DEFAULT '[]'::jsonb,
    payload       jsonb NOT NULL DEFAULT '{}'::jsonb,
    incomplete    boolean NOT NULL DEFAULT false,
    latency_ms    integer,
    created_at    timestamptz NOT NULL DEFAULT now(),
    follow_until  timestamptz,
    closed_at     timestamptz,
    UNIQUE (mint, mode)                         -- ключ алерта: токен + режим
);
CREATE INDEX IF NOT EXISTS alerts_created_idx ON alerts (created_at);

-- Одна монета — одно сообщение (разд. 9)
CREATE TABLE IF NOT EXISTS token_messages (
    mint        text PRIMARY KEY,
    chat_id     bigint,
    thread_id   integer,
    message_id  bigint,
    level       text,
    modes       text[] NOT NULL DEFAULT '{}',
    text        text,
    keyboard    jsonb,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_outcomes (
    alert_id          bigint PRIMARY KEY REFERENCES alerts (id) ON DELETE CASCADE,
    max_mc            double precision,
    exit_mc           double precision,
    exit_reason       text,                    -- target | emergency | timeout
    net_result        double precision,         -- результат, если войти с задержкой реакции
    achievable_profit double precision,
    auto_mark         text,
    computed_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_marks (
    id         bigserial PRIMARY KEY,
    alert_id   bigint NOT NULL REFERENCES alerts (id) ON DELETE CASCADE,
    user_id    bigint NOT NULL,
    mark       text NOT NULL CHECK (mark IN ('great', 'good', 'fail', 'scam')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (alert_id, user_id)
);

CREATE TABLE IF NOT EXISTS settings (
    key        text PRIMARY KEY,
    value      jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS watchlist (
    kind       text NOT NULL CHECK (kind IN ('token', 'dev', 'trader')),
    address    text NOT NULL,
    label      text,
    created_at timestamptz NOT NULL DEFAULT now(),
    until      timestamptz,
    PRIMARY KEY (kind, address)
);

-- Слежение за выводом SOL приоритетных кластеров (разд. 6)
CREATE TABLE IF NOT EXISTS withdrawal_watch (
    wallet      text PRIMARY KEY,
    cluster_id  bigint NOT NULL,
    hop         integer NOT NULL DEFAULT 0,
    parent      text,
    last_sig    text,
    stopped     text,                            -- причина обрыва цепочки (exchange)
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Очередь сообщений аналитика → бот
CREATE TABLE IF NOT EXISTS outbox (
    id          bigserial PRIMARY KEY,
    kind        text NOT NULL,
    payload     jsonb NOT NULL,
    priority    integer NOT NULL DEFAULT 5,      -- 0 — наивысший
    created_at  timestamptz NOT NULL DEFAULT now(),
    sent_at     timestamptz,
    attempts    integer NOT NULL DEFAULT 0,
    error       text
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (priority, id) WHERE sent_at IS NULL;

-- Покрытие источников (сверка раз в час) и разрывы потока
CREATE TABLE IF NOT EXISTS source_stats (
    source   text NOT NULL,
    hour     timestamptz NOT NULL,
    creates  integer NOT NULL DEFAULT 0,
    trades   integer NOT NULL DEFAULT 0,
    PRIMARY KEY (source, hour)
);
CREATE TABLE IF NOT EXISTS source_creates (
    source   text NOT NULL,
    mint     text NOT NULL,
    seen_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, mint)
);
CREATE TABLE IF NOT EXISTS ingest_gaps (
    id         bigserial PRIMARY KEY,
    source     text NOT NULL,
    started_at timestamptz NOT NULL,
    ended_at   timestamptz
);

CREATE TABLE IF NOT EXISTS kv (
    key        text PRIMARY KEY,
    value      jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Задания бот → аналитика (обработка отметок, пересчёты)
CREATE TABLE IF NOT EXISTS jobs (
    id         bigserial PRIMARY KEY,
    kind       text NOT NULL,
    payload    jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    done_at    timestamptz,
    error      text
);
CREATE INDEX IF NOT EXISTS jobs_pending_idx ON jobs (id) WHERE done_at IS NULL;

ALTER TABLE traders ADD COLUMN IF NOT EXISTS priority boolean NOT NULL DEFAULT false;

-- Внешние источники: каждое значение с указанием источника (ончейн имеет приоритет)
CREATE TABLE IF NOT EXISTS external_facts (
    entity     text NOT NULL,            -- token:<mint> | wallet:<address>
    key        text NOT NULL,
    source     text NOT NULL,            -- rugcheck | gmgn | dexscreener | birdeye | jupiter | solscan
    value      jsonb,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity, key, source)
);
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS label_source text;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS tags jsonb;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS tags_source text;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS labels_at timestamptz;
ALTER TABLE traders ADD COLUMN IF NOT EXISTS tags jsonb;
ALTER TABLE traders ADD COLUMN IF NOT EXISTS tags_source text;

-- Блок A/B: роли кошельков, подозрительные кластеры, число сделок запуска
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS role text;          -- dev | trader | sniper | bundler | kol | exchange | service
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS role_reason text;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS role_source text;
CREATE INDEX IF NOT EXISTS wallets_role_idx ON wallets (role) WHERE role IS NOT NULL;
CREATE INDEX IF NOT EXISTS wallets_funded_by_idx ON wallets (funded_by) WHERE funded_by IS NOT NULL;
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS suspicious boolean NOT NULL DEFAULT false;
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS suspicious_reason text;
ALTER TABLE launches ADD COLUMN IF NOT EXISTS trades_count integer;
ALTER TABLE tokens ADD COLUMN IF NOT EXISTS last_mc_sol double precision;
ALTER TABLE tokens ADD COLUMN IF NOT EXISTS peak_mc_sol double precision;
ALTER TABLE tokens ADD COLUMN IF NOT EXISTS identity text;       -- кошелёк, по которому строится кластер

-- Блок E: бумажная торговля (без реальных денег и ключей)
CREATE TABLE IF NOT EXISTS paper_portfolios (
    name          text PRIMARY KEY,                 -- A | B | C
    scheme        jsonb NOT NULL,
    start_usd     double precision NOT NULL,
    start_sol     double precision NOT NULL,
    balance_sol   double precision NOT NULL,
    paused        boolean NOT NULL DEFAULT false,
    reset_at      timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS paper_positions (
    id            bigserial PRIMARY KEY,
    portfolio     text NOT NULL,
    mint          text NOT NULL,
    alert_id      bigint,
    level         text,
    symbol        text,
    opened_at     timestamptz NOT NULL,
    entry_mc      double precision,
    cost_sol      double precision NOT NULL,           -- потрачено с учётом комиссий
    tokens        double precision NOT NULL,           -- текущий остаток токенов
    tokens_initial double precision NOT NULL,
    proceeds_sol  double precision NOT NULL DEFAULT 0, -- получено от частичных продаж
    max_mc        double precision,
    target_mc     double precision,
    tp_done       boolean NOT NULL DEFAULT false,
    closed_at     timestamptz,
    exit_reason   text,
    pnl_sol       double precision,
    pnl_pct       double precision
);
CREATE INDEX IF NOT EXISTS paper_positions_open_idx ON paper_positions (portfolio) WHERE closed_at IS NULL;
CREATE TABLE IF NOT EXISTS paper_trades (
    id            bigserial PRIMARY KEY,
    position_id   bigint NOT NULL REFERENCES paper_positions (id) ON DELETE CASCADE,
    portfolio     text NOT NULL,
    side          text NOT NULL,                       -- buy | sell
    ts            timestamptz NOT NULL,
    mc            double precision,
    sol           double precision NOT NULL,           -- вход: потрачено; выход: получено (после комиссий)
    tokens        double precision NOT NULL,
    fee_sol       double precision NOT NULL,
    reason        text
);

-- Блок B: лучшие трейдеры по монетам и статистика кошельков
CREATE TABLE IF NOT EXISTS coin_top_traders (
    mint        text NOT NULL,
    rank        integer NOT NULL,
    trader      text NOT NULL,
    pnl_sol     double precision,
    entry_mc    double precision,
    exit_mc     double precision,
    hold_s      double precision,
    entry_age_s double precision,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (mint, rank)
);
CREATE INDEX IF NOT EXISTS coin_top_traders_trader_idx ON coin_top_traders (trader, created_at);
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS pnl_7d double precision;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS pnl_30d double precision;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS winrate double precision;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS trades_30d integer;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS avg_hold_s double precision;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS copyability double precision;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS stats_at timestamptz;

-- «Тренды»: теневой учёт отсеянных монет — что было бы через час (проверка, не режут ли фильтры хорошие монеты)
CREATE TABLE IF NOT EXISTS trend_shadow (
    id          bigserial PRIMARY KEY,
    mint        text NOT NULL,
    symbol      text,
    reason      text NOT NULL,
    detail      text,
    created_at  timestamptz NOT NULL,
    price       double precision,
    max_price   double precision,
    last_price  double precision
);
CREATE INDEX IF NOT EXISTS trend_shadow_created_idx ON trend_shadow (created_at);

-- Девы из своего списка (импорт из Axiom, /import): почерк по их истории — где и как быстро продают
CREATE TABLE IF NOT EXISTS my_devs (
    address      text PRIMARY KEY,
    launches     integer NOT NULL DEFAULT 0,      -- запусков в просмотренной истории
    sold         integer NOT NULL DEFAULT 0,      -- из них дев продавал (есть точка продажи)
    med_sell_mc  double precision,                -- MC (SOL) перед первой продажей дева: медиана
    p25_sell_mc  double precision,
    p75_sell_mc  double precision,
    med_sell_s   double precision,                -- через сколько секунд после запуска первая продажа
    hit_rate     double precision,                -- доля запусков, где продал на ×1.5 и выше от старта
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Журнал копитрейда: каждая сделка своих кошельков — скопировано или пропущено и почему (/copylog)
CREATE TABLE IF NOT EXISTS copy_log (
    id        bigserial PRIMARY KEY,
    ts        timestamptz NOT NULL DEFAULT now(),
    wallet    text NOT NULL,
    mint      text NOT NULL,
    symbol    text,
    side      text NOT NULL,             -- buy | sell | launch
    sol       double precision,
    mc        double precision,          -- MC (SOL) сразу после сделки цели
    action    text NOT NULL,             -- copied | skipped | info
    reason    text,
    ref_mc    double precision,          -- MC цели для дельты
    entry_mc  double precision,          -- MC нашего входа (бумага, +5 с)
    delta     double precision           -- entry_mc / ref_mc − 1
);
CREATE INDEX IF NOT EXISTS copy_log_wallet_idx ON copy_log (wallet, id);
CREATE INDEX IF NOT EXISTS copy_log_ts_idx ON copy_log (ts);
ALTER TABLE my_devs ADD COLUMN IF NOT EXISTS once boolean NOT NULL DEFAULT false;       -- зайти к деву один раз
ALTER TABLE my_devs ADD COLUMN IF NOT EXISTS once_done boolean NOT NULL DEFAULT false;
