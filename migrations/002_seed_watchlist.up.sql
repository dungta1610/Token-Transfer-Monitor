INSERT INTO wallet_watchlist (
    address,
    label,
    is_active
) VALUES
(
    '0xcccccccccccccccccccccccccccccccccccccccc',
    'sample receiver wallet',
    true
)
ON CONFLICT (address) DO UPDATE SET
    label = EXCLUDED.label,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();

INSERT INTO tracked_tokens (
    chain_id,
    contract_address,
    symbol,
    decimals,
    is_active
) VALUES
(
    11155111,
    '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    'TST',
    18,
    true
)
ON CONFLICT (chain_id, contract_address) DO UPDATE SET
    symbol = EXCLUDED.symbol,
    decimals = EXCLUDED.decimals,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();