category_id=$(psql -p 5433 -d asset_prices -tA -c "
WITH ins AS (
    INSERT INTO category(name)
    VALUES ('Crypto')
    ON CONFLICT (name) DO NOTHING
    RETURNING category_id   
)
SELECT category_id FROM ins
UNION ALL
SELECT category_id FROM category WHERE name = 'Crypto'
LIMIT 1;
")

exchange_id=$(psql -p 5433 -d asset_prices -tA -c "
WITH ins AS (
    INSERT INTO exchanges(name)
    VALUES ('Yahoo')
    ON CONFLICT (name) DO NOTHING
    RETURNING id
)
SELECT id FROM ins
UNION ALL
SELECT id FROM exchanges WHERE name = 'Yahoo'
LIMIT 1;
")


awk -F ',' 'NR>1 && NF==6 && $3 && $4 && $6 {print $3","$4","$6}' cryptos.csv |
while IFS=',' read -r ticker close change;do 
    echo "insert into ticker_list (ticker, category_id ,change, close, exchange_id) 
    values ('$ticker-USD', '$category_id',  '$change' , '$close', '$exchange_id')
    ON CONFLICT (ticker) DO NOTHING;"
done | psql -p 5433  -d asset_prices
