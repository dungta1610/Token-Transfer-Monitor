CREATE TABLE IF NOT EXISTS tracked_tokens (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    contract_address TEXT NOT NULL,
    symbol TEXT,
    decimals INT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tracked_tokens_chain_contract UNIQUE (chain_id, contract_address)
);

CREATE INDEX IF NOT EXISTS idx_tracked_tokens_chain_id
    ON tracked_tokens (chain_id);

CREATE INDEX IF NOT EXISTS idx_tracked_tokens_contract_address
    ON tracked_tokens (contract_address);

CREATE TABLE IF NOT EXISTS wallet_watchlist (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    label TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_wallet_watchlist_address UNIQUE (address)
);

CREATE INDEX IF NOT EXISTS idx_wallet_watchlist_address
    ON wallet_watchlist (address);

CREATE INDEX IF NOT EXISTS idx_wallet_watchlist_is_active
    ON wallet_watchlist (is_active);

CREATE TABLE IF NOT EXISTS processed_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    chain_id BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    contract_address TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_processed_events_event_id UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_tx_hash
    ON processed_events (tx_hash);

CREATE INDEX IF NOT EXISTS idx_processed_events_chain_tx_log
    ON processed_events (chain_id, tx_hash, log_index);

CREATE TABLE IF NOT EXISTS transfer_activities (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    chain_id BIGINT NOT NULL,
    contract_address TEXT NOT NULL,
    token_symbol TEXT,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    amount_raw TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash TEXT,
    direction TEXT,
    matched_wallet TEXT,
    observed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_transfer_activities_event_id UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS idx_transfer_activities_tx_hash
    ON transfer_activities (tx_hash);

CREATE INDEX IF NOT EXISTS idx_transfer_activities_from_address
    ON transfer_activities (from_address);

CREATE INDEX IF NOT EXISTS idx_transfer_activities_to_address
    ON transfer_activities (to_address);

CREATE INDEX IF NOT EXISTS idx_transfer_activities_matched_wallet
    ON transfer_activities (matched_wallet);

CREATE INDEX IF NOT EXISTS idx_transfer_activities_block_number
    ON transfer_activities (block_number);