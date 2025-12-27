#!/bin/bash

# Get category_id and exchange_id first
category_id=$(psql -p 5433 -d asset_prices -tA -c "
WITH ins AS (
    INSERT INTO category(name)
    VALUES ('Shares')
    ON CONFLICT (name) DO NOTHING
    RETURNING category_id
)
SELECT category_id FROM ins
UNION ALL
SELECT category_id FROM category WHERE name = 'Shares'
LIMIT 1;
")

exchange_id=$(psql -p 5433 -d asset_prices -tA -c "
WITH ins AS (
    INSERT INTO exchanges(name)
    VALUES ('Nasdaq')
    ON CONFLICT (name) DO NOTHING
    RETURNING id
)
SELECT id FROM ins
UNION ALL
SELECT id FROM exchanges WHERE name = 'Nasdaq'
LIMIT 1;
")

# Process CSV properly
tail -n +2 nq.csv | while IFS=',' read -r symbol name last_sale net_change pct_change _rest; do
    # Remove $ and % signs and convert to numbers
    last_sale_clean=$(echo "$last_sale" | tr -d '$,' )
    net_change_clean=$(echo "$net_change" | tr -d '$,' )
    pct_change_clean=$(echo "$pct_change" | tr -d '%$,' )

    # Use last_sale_clean as close, pct_change_clean as change
    echo "INSERT INTO ticker_list (ticker, category_id, change, close, exchange_id)
          VALUES ('$symbol', $category_id, $pct_change_clean, $last_sale_clean, $exchange_id)
          ON CONFLICT (ticker) DO NOTHING;"
done | psql -p 5433 -d asset_prices
