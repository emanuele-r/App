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


awk -F ',' 'NR>1 && NF==6 && $1 && $3 && $5 {print $1","$3","$5}' nq.csv |
while IFS=',' read -r ticker close change; do
    change_clean=$(echo "$change" | sed 's/[^0-9.-]//g')
    close_clean=$(echo "$close" | sed 's/[^0-9.-]//g')

    echo "
    INSERT INTO ticker_list (ticker, category_id, change, close, exchange_id)
    VALUES ('$ticker', $category_id, $change_clean, $close_clean, $exchange_id)
    ON CONFLICT (ticker) DO NOTHING;
    "
done | psql -p 5433 -d asset_prices
