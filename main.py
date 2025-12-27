import pandas as pd
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
import time
import yfinance as yf
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import psycopg
import httpx
import state
import asyncio


def create_db():
    with psycopg.connect(
        dbname="asset_prices",
        user="postgres",
        password="strongpassword",
        host="127.0.0.1",
        port=5433,
    ) as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS exchanges (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS countries (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS sectors (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS industries (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS category (
                    category_id BIGSERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS ticker_list (
                    id BIGSERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL UNIQUE,
                    category_id BIGINT NOT NULL,
                    change DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    exchange_id BIGINT,
                    CONSTRAINT fk_exchange
                        FOREIGN KEY (exchange_id)
                        REFERENCES exchanges(id)
                        ON DELETE SET NULL
                        ON UPDATE CASCADE,
                    CONSTRAINT fk_category
                        FOREIGN KEY (category_id)
                        REFERENCES category(category_id)
                );
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS favourites (
                    ticker_id BIGINT PRIMARY KEY,
                    category TEXT NOT NULL,
                    CONSTRAINT fk_fav_ticker
                        FOREIGN KEY (ticker_id)
                        REFERENCES ticker_list(id)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE
                );
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS asset_data (
                    ticker_id BIGINT PRIMARY KEY,
                    name TEXT,
                    sector_id BIGINT NOT NULL,
                    industry_id BIGINT NOT NULL,
                    country_id BIGINT NOT NULL,
                    CONSTRAINT fk_asset_ticker
                        FOREIGN KEY (ticker_id)
                        REFERENCES ticker_list(id)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE,
                    CONSTRAINT fk_sector
                        FOREIGN KEY (sector_id)
                        REFERENCES sectors(id),
                    CONSTRAINT fk_industry
                        FOREIGN KEY (industry_id)
                        REFERENCES industries(id),
                    CONSTRAINT fk_country
                        FOREIGN KEY (country_id)
                        REFERENCES countries(id)
                );
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS asset_prices (
                    ticker_id BIGINT NOT NULL,
                    date DATE NOT NULL,
                    timeframe TEXT NOT NULL,
                    open DOUBLE PRECISION NOT NULL,
                    high DOUBLE PRECISION NOT NULL,
                    low DOUBLE PRECISION NOT NULL,
                    close DOUBLE PRECISION NOT NULL,
                    change DOUBLE PRECISION,
                    period TEXT,
                    PRIMARY KEY (ticker_id, date, timeframe),
                    CONSTRAINT fk_price_ticker
                        FOREIGN KEY (ticker_id)
                        REFERENCES ticker_list(id)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE
                );
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ticker_exchange
                ON ticker_list(exchange_id);
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_asset_ticker_date
                ON asset_prices(ticker_id, date DESC);
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ticker_category
                ON ticker_list(category_id);
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_symbols_ticker
                ON ticker_list(ticker, category_id, change, close);
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_favourites_tid
                ON favourites(ticker_id);
            """
            )

        conn.commit()


create_db()


async def readCategory():
    async with state.pg_pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT * FROM category")
            data = await cursor.fetchall()
            return data


async def readTickerList(category: str = None):
    async with state.pg_pool.connection() as conn:
        async with conn.cursor() as cursor:
            if category:
                await cursor.execute(
                    """
                    SELECT ticker, category_id, change, close
                    FROM ticker_list
                    WHERE category_id = (SELECT category_id FROM category WHERE name = %s)
                    """,
                    (category,),
                )
            else:
                await cursor.execute(
                    """
                    SELECT ticker, category_id, change, close
                    FROM ticker_list
                    LIMIT 500
                    """
                )
            data = await cursor.fetchall()
    return data


async def getNews(query: str, lang: str = "en") -> list[dict]:
    load_dotenv("/app/news_api.env")
    api_key = os.getenv("NEWS_API_KEY")
    url = f"https://newsdata.io/api/1/latest?apikey={api_key}&q={query}&language={lang}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        data = response.json()

    results = data.get("results", [])
    return [
        {
            "title": news.get("title"),
            "description": news.get("description"),
            "url": news.get("link"),
        }
        for news in results
    ]


async def deleteDataFromFavourites(ticker: str):
    ticker = ticker.upper()
    async with state.pg_pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT id FROM ticker_list WHERE ticker = %s", (ticker,)
            )
            result = await cursor.fetchone()
            if result:
                ticker_id = result[0]
                await cursor.execute(
                    "DELETE FROM favourites WHERE ticker_id = %s", (ticker_id,)
                )
                await conn.commit()


async def insertDataIntoFavourites(ticker: str):
    ticker = ticker.upper()
    async with state.pg_pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO favourites (ticker_id, category)
                SELECT id, category_id
                FROM ticker_list
                WHERE ticker = %s
                ON CONFLICT (ticker_id) DO UPDATE
                SET category = EXCLUDED.category
                RETURNING ticker_id
                """,
                (ticker,),
            )
            inserted = await cursor.fetchone()
            await conn.commit()
            return inserted


async def readFavorites():
    async with state.pg_pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT t.ticker, t.change, t.close, t.category_id
                FROM ticker_list t
                JOIN favourites f ON t.id = f.ticker_id
                """
            )
            data = await cursor.fetchall()
    return data


def RiskMetrics(ticker: str, risk_free_rate: float = 0.044) -> dict:
    tkr = yf.Ticker(ticker)

    price = tkr.fast_info["last_price"]
    income_stmt = tkr.income_stmt
    eps = income_stmt.iloc[income_stmt.index.get_loc("Basic EPS"), 0]

    earnings_yield = eps / price
    yield_spread = earnings_yield - risk_free_rate
    pe_ratio = 1 / earnings_yield

    cf = tkr.cashflow
    fcf = cf.loc["Free Cash Flow"].iloc[0]
    market_cap = tkr.fast_info["market_cap"]
    fcf_yield = fcf / market_cap

    hist = tkr.history(period="6mo")
    returns = hist["Close"].pct_change().dropna()
    vol_60d = returns.std() * (252**0.5)
    risk_adj_earnings_yield = earnings_yield / vol_60d

    total_debt = tkr.balance_sheet.loc["Total Debt"].iloc[0]
    ebitda = tkr.financials.loc["EBITDA"].iloc[0]
    leverage = total_debt / ebitda

    vix = yf.Ticker("^VIX").fast_info["last_price"]
    price_of_risk = earnings_yield / (vix / 100)

    if eps <= 0 or price <= 0 or vix <= 0 or ebitda <= 0 or market_cap <= 0:
        raise ValueError("Invalid inputs for valuation or risk metrics")

    return {
        "ticker": ticker,
        "earnings_yield": earnings_yield,
        "fcf_yield": fcf_yield,
        "pe_ratio": pe_ratio,
        "vol_60d": vol_60d,
        "risk_adj_earnings_yield": risk_adj_earnings_yield,
        "price_of_risk": price_of_risk,
        "leverage": leverage,
        "vix": vix,
        "yield_spread": yield_spread,
    }


def get_data(
    ticker: str,
    start_date: str = None,
    end_date: str = None,
    period: str = None,
    timeframe: str = "1d",
) -> pd.DataFrame:
    if start_date and end_date:
        data = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            threads=True,
            period=period,
            interval=timeframe,
            multi_level_index=False,
        )[["Open", "High", "Low", "Close"]]
    elif timeframe:
        if timeframe.endswith("m"):
            data = yf.download(
                ticker,
                period="1d",
                interval=timeframe,
                threads=True,
                multi_level_index=False,
            )[["Open", "High", "Low", "Close"]]
        elif timeframe.endswith("h"):
            data = yf.download(
                ticker,
                period="1mo",
                interval=timeframe,
                threads=True,
                multi_level_index=False,
            )[["Open", "High", "Low", "Close"]]
        else:
            data = yf.download(
                ticker,
                period="max",
                interval=timeframe,
                threads=True,
                multi_level_index=False,
            )[["Open", "High", "Low", "Close"]]

    data.reset_index(inplace=True)
    data.columns = data.columns.str.lower()
    if "datetime" in data.columns:
        data.rename(columns={"datetime": "date"}, inplace=True)

    data["ticker"] = ticker
    data["timeframe"] = timeframe
    data["period"] = period
    data["change"] = data["close"].pct_change()

    return data


async def read_db_v2(
    ticker: str,
    start_date: str = None,
    end_date: str = None,
    period: str = None,
    timeframe: str = "1d",
) -> pd.DataFrame:
    ticker = ticker.upper()

    today = (
        datetime.now().strftime("%Y-%m-%d")
        if timeframe == "1d"
        else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    try:
        async with state.pg_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT id FROM ticker_list WHERE ticker = %s", (ticker,)
                )
                ticker_id_row = await cursor.fetchone()
                if not ticker_id_row:
                    raise ValueError(f"Ticker {ticker} not found in DB")
                ticker_id = ticker_id_row[0]

                await cursor.execute(
                    "SELECT MAX(date) FROM asset_prices WHERE ticker_id = %s AND timeframe = %s",
                    (ticker_id, timeframe),
                )
                isUpToDate_row = await cursor.fetchone()
                isUpToDate = isUpToDate_row[0] if isUpToDate_row else None

                needs_update = isUpToDate != today

                fetch_start_date = (
                    (pd.to_datetime(isUpToDate) + pd.Timedelta(days=1)).strftime(
                        "%Y-%m-%d"
                    )
                    if isUpToDate is not None
                    else "2008-01-01"
                )

                upData = await asyncio.to_thread(
                    get_data,
                    ticker=ticker,
                    start_date=fetch_start_date,
                    end_date=today,
                    timeframe=timeframe,
                )

                if not upData.empty:
                    records = [
                        (
                            ticker_id,
                            str(row.date),
                            row.open,
                            row.high,
                            row.low,
                            row.close,
                            row.change,
                            row.period,
                            timeframe,
                        )
                        for row in upData.itertuples(index=False)
                    ]

                    await cursor.executemany(
                        """
                            INSERT INTO asset_prices 
                            (ticker_id, date, open, high, low, close, change, period, timeframe)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ticker_id, date, timeframe) DO UPDATE
                            SET open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                change = EXCLUDED.change,
                                period = EXCLUDED.period
                            """,
                        records,
                    )

                if start_date and end_date:
                    await cursor.execute(
                        """
                        SELECT * FROM asset_prices 
                        WHERE date BETWEEN %s AND %s AND ticker_id = %s AND timeframe = %s ORDER BY date ASC
                        """,
                        (start_date, end_date, ticker_id, timeframe),
                    )
                else:
                    await cursor.execute(
                        "SELECT * FROM asset_prices WHERE ticker_id = %s AND timeframe = %s  ORDER BY date ASC",
                        (ticker_id, timeframe),
                    )

                rows = await cursor.fetchall()
                updated_data = pd.DataFrame(
                    rows, columns=[col[0] for col in cursor.description]
                )

    except Exception as e:
        raise ValueError(f"Error reading database : {e}")

    return updated_data


async def helperFunctionPattern(
    ticker: str, start_date: str = None, end_date: str = None, timeframe: str = "1d"
):
    async with state.pg_pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "select id from ticker_list where ticker = %s ", (ticker,)
            )
            ticker_id = await cursor.fetchone()[0]
            if start_date and end_date:
                await cursor.execute(
                    "select close, date from asset_prices where ticker_id = %s  and timeframe = %s and date between %s and %s order by date",
                    (ticker_id, timeframe, start_date, end_date),
                )
            else:
                await cursor.execute(
                    "select close, date from asset_prices where ticker_id = %s and timeframe = %s order by date",
                    (ticker_id, timeframe),
                )

            data = await cursor.fetchall()
        return data


async def helperFunctionOhlcPattern(
    ticker: str, start_date: str = None, end_date: str = None, timeframe: str = "1d"
):
    async with state.pg_pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "select id from ticker_list where ticker = %s ", (ticker,)
            )
            ticker_id = await cursor.fetchone()[0]
            if start_date and end_date:
                await cursor.execute(
                    "select close, date from asset_prices where ticker_id = %s  and timeframe = %s and date between %s and %s order by date",
                    (ticker_id, timeframe, start_date, end_date),
                )
            else:
                await cursor.execute(
                    "select close, date from asset_prices where ticker_id = %s and timeframe = %s order by date",
                    (ticker_id, timeframe),
                )

            data = await cursor.fetchall()
        return data


async def Helper(ticker: str, start_date: str, end_date: str, timeframe: str = "1d"):
    async with state.pg_pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "select id from ticker_list where ticker = %s", (ticker,)
            )
            ticker_id = await cursor.fetchone()[0]

            await cursor.execute(
                """
                select open,high,low,close from asset_prices where ticker_id = %s and timeframe = %s and date between %s  and %s order by date
                """,
                (ticker_id, timeframe, start_date, end_date),
            )

            data = await cursor.fetchall()

        return data


async def calculate_query_return(ticker: str, start_date: str, end_date: str) -> float:
    try:
        async with state.pg_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "select id from ticker_list where ticker = %s", (ticker,)
                )
                ticker_id = await cursor.fetchone()[0]
                await cursor.execute(
                    """
                    select close from asset_prices where ticker_id = %s and date between %s and %s
                    """,
                    (ticker_id, start_date, end_date),
                )
                closes = await cursor.fetchall()
            query_return = (closes[-1][0] / closes[0][0]) - 1

            if len(closes) < 2:
                raise ValueError("Error : query length is less than 2")

    except Exception as e:
        raise ValueError(f"Error reading database: {e}")

    return query_return


def array_with_shift(
    array, array2, dates, shift_range: int = 0, k: int = 3, metric="l1", wrap=True
):
    """
    Vectorized motif search: find top-k matches of `array` inside `array2`.

    Parameters
    ----------
    array : array-like
        The query array (length m).
    array2 : array-like
        The reference array (length n).
    shift_range : int, optional
        Maximum shift allowed between query and reference arrays.
    k : int, optional
        Best results to return.
    metric : {"l1", "l2"}
        Distance metric.
    wrap : bool
        Whether to allow wrapping (circular search).
    """
    array = np.asarray(array, dtype=float)
    array2 = np.asarray(array2, dtype=float)

    if len(array) == 0:
        raise ValueError("Query array is empty")
    if len(array2) == 0:
        raise ValueError("Reference array is empty")
    if len(array2) < len(array):
        raise ValueError(
            f"Reference array (len={len(array2)}) is shorter than query array (len={len(array)})"
        )
    if len(dates) != len(array2):
        raise ValueError(
            f"Dates array (len={len(dates)}) must match reference array (len={len(array2)})"
        )

    m = len(array)
    n = len(array2)

    array_mean = np.mean(array)
    array_std = np.std(array)
    if array_std > 0:
        array_normalized = (array - array_mean) / array_std
    else:
        array_normalized = array - array_mean

    if wrap:
        extended_prices = np.concatenate([array2, array2[: m - 1]])
        extended_dates = np.concatenate([dates, dates[: m - 1]])
        source_array = extended_prices
    else:
        extended_prices = array2
        extended_dates = dates
        source_array = array2

    subsequences = sliding_window_view(extended_prices, m)

    subseq_means = np.mean(subsequences, axis=1, keepdims=True)
    subseq_stds = np.std(subsequences, axis=1, keepdims=True)

    subseq_stds = np.where(subseq_stds > 0, subseq_stds, 1.0)
    subsequences_normalized = (subsequences - subseq_means) / subseq_stds

    if metric == "l1":
        dists = np.sum(np.abs(subsequences_normalized - array_normalized), axis=1)
    elif metric == "l2":
        dists = np.sqrt(
            np.sum((subsequences_normalized - array_normalized) ** 2, axis=1)
        )
    else:
        raise ValueError("metric must be 'l1' or 'l2'")

    k_actual = min(k, len(dists))

    if k_actual > 0:
        best_idx = np.argpartition(dists, k_actual - 1)[:k_actual]
        best_idx = best_idx[np.argsort(dists[best_idx])]
    else:
        best_idx = np.array([], dtype=int)

    best_distances = dists[best_idx].tolist()
    best_starts = best_idx.tolist()
    best_indices = [list(range(start, start + m)) for start in best_starts]
    best_subarrays = [source_array[start : start + m].tolist() for start in best_starts]
    best_dates = [extended_dates[start : start + m].tolist() for start in best_starts]

    return best_indices, best_dates, best_subarrays, best_distances, array, array2


async def pattern_forward_return(
    ticker: str,
    best_dates: list[list[str]],
    rolling_window_low: int = 7,
    rolling_window_high: int = 30,
) -> list[dict]:
    ticker = ticker.upper()
    forward_return = []
    seven_day_returns = []
    monthly_returns = []
    pattern_returns = []

    async with state.pg_pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT id FROM ticker_list WHERE ticker = %s", (ticker,)
            )
            ticker_id_row = await cursor.fetchone()
            if not ticker_id_row:
                raise ValueError(f"Ticker {ticker} not found")
            ticker_id = ticker_id_row[0]

            start_date = min(p[0] for p in best_dates)
            end_date = max(p[-1] for p in best_dates)
            max_window_end = (
                datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                + timedelta(days=rolling_window_high)
            ).strftime("%Y-%m-%d %H:%M:%S")

            await cursor.execute(
                """
                SELECT date, close
                FROM asset_prices
                WHERE ticker_id = %s AND date >= %s AND date <= %s
                ORDER BY date
                """,
                (ticker_id, start_date, max_window_end),
            )
            rows = await cursor.fetchall()

    if not rows:
        return []

    df = pd.DataFrame(rows, columns=["date", "close"])
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    for idx, pat in enumerate(best_dates):
        pat_start = pd.to_datetime(pat[0])
        pat_end = pd.to_datetime(pat[-1])

        pattern_data = df.loc[pat_start:pat_end]
        if pattern_data.empty:
            continue

        end_week_dt = pat_end + pd.Timedelta(days=rolling_window_low)
        weekly_data = df.loc[pat_end:end_week_dt]
        if len(weekly_data) < 2:
            continue

        end_month_dt = pat_end + pd.Timedelta(days=rolling_window_high)
        monthly_data = df.loc[pat_end:end_month_dt]
        if len(monthly_data) < 2:
            continue

        avgReturnForPattern = (
            pattern_data["close"].iloc[-1] / pattern_data["close"].iloc[0] - 1
        ) * 100
        sevenDayReturnAfterPattern = (
            weekly_data["close"].iloc[-1] / weekly_data["close"].iloc[0] - 1
        ) * 100
        monthlyReturnAfterPattern = (
            monthly_data["close"].iloc[-1] / monthly_data["close"].iloc[0] - 1
        ) * 100

        pattern_returns.append(avgReturnForPattern)
        seven_day_returns.append(sevenDayReturnAfterPattern)
        monthly_returns.append(monthlyReturnAfterPattern)

        forward_return.append(
            {
                "patternIdx": idx,
                "sevenDayReturnAfterPattern": sevenDayReturnAfterPattern,
                "monthlyReturnAfterPattern": monthlyReturnAfterPattern,
            }
        )

    summary = [
        {
            "Pattern": forward_return,
            "summary": {
                "Average 7 day return": (
                    sum(seven_day_returns) / len(seven_day_returns)
                    if seven_day_returns
                    else 0
                ),
                "Average 30 day return": (
                    sum(monthly_returns) / len(monthly_returns)
                    if monthly_returns
                    else 0
                ),
                "Pattern Average return": (
                    sum(pattern_returns) / len(pattern_returns)
                    if pattern_returns
                    else 0
                ),
            },
        }
    ]

    return summary


def dynamic_time_warping(
    array,
    array2,
    dates=None,
    shift_range: int = 0,
    k: int = 3,
    metric="l1",
    wrap=True,
    length_tolerance: int = 0,
):
    """
    Vectorized motif search: find top-k matches of `array` inside `array2`,
    allowing for variable-length motifs.

    Parameters
    ----------
    array : array-like
        The query array (length m).
    array2 : array-like
        The reference array.
    dates : array-like, optional
        Dates aligned with array2.
    shift_range : int, optional
        (Unused for now) Maximum shift allowed between query and reference arrays.
    k : int, optional
        Best results to return.
    metric : {"l1", "l2"}
        Distance metric.
    wrap : bool
        Whether to allow wrapping (circular search).
    length_tolerance : int, optional
        Allowed variation in subsequence length around len(array).
        e.g., 2 means [m-2 ... m+2] window sizes will be checked.
    """

    array = np.asarray(array, dtype=float)
    array2 = np.asarray(array2, dtype=float)
    if dates is not None:
        dates = pd.to_datetime(dates)  # ensure datetime

    if len(array2) < len(array):
        return None, None, None, None, array, array2

    m = len(array)
    n = len(array2)

    start_time = time.time()

    best_matches = []

    for window_size in range(max(2, m - length_tolerance), m + length_tolerance + 1):
        if window_size > n:
            continue

        if wrap:
            extended = np.concatenate([array2, array2[: window_size - 1]])
            source_array = extended
            subsequences = sliding_window_view(extended, window_size)
            if dates is not None:
                extended_dates = np.concatenate([dates, dates[: window_size - 1]])
        else:
            source_array = array2
            subsequences = sliding_window_view(array2, window_size)
            if dates is not None:
                extended_dates = dates

        dates_to_slice = extended_dates if dates is not None else None

        if window_size != m:
            query_rescaled = np.interp(
                np.linspace(0, m - 1, window_size), np.arange(m), array
            )
        else:
            query_rescaled = array

        if metric == "l1":
            dists = np.sum(np.abs(subsequences - query_rescaled), axis=1)
        elif metric == "l2":
            dists = np.sum((subsequences - query_rescaled) ** 2, axis=1)
        else:
            raise ValueError("metric must be 'l1' or 'l2'")

        for start, dist in enumerate(dists):

            prices = source_array[start : start + window_size]

            current_dates = (
                dates_to_slice[start : start + window_size]
                if dates_to_slice is not None
                else None
            )

            best_matches.append((start, dist, window_size, prices, current_dates))

    best_matches = sorted(best_matches, key=lambda x: x[1])[:k]

    best_indices = [list(range(start, start + w)) for start, _, w, _, _ in best_matches]
    best_distances = [dist for _, dist, _, _, _ in best_matches]
    best_subarrays = [sub for _, _, _, sub, _ in best_matches]
    best_dates = [d for _, _, _, _, d in best_matches]

    elapsed_time = time.time() - start_time
    print(f"Elapsed time (variable-length search): {elapsed_time:.6f} seconds")

    return best_indices, best_dates, best_subarrays, best_distances, array, array2
