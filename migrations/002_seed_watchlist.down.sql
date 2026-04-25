DELETE FROM wallet_watchlist
WHERE address = '0xcccccccccccccccccccccccccccccccccccccccc';

DELETE FROM tracked_tokens
WHERE chain_id = 11155111
  AND contract_address = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';