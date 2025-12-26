import pandas as pd
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
import time
import yfinance as yf
import sqlite3
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv
import psycopg


def create_db():
    with psycopg.connect(
        dbname="asset_prices",
        user="postgres",
        password="strongpassword",
        host="46.224.51.58",
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


def get_pg_connection():
    return psycopg.connect(
        dbname="asset_prices",
        user="postgres",
        password="strongpassword",
        host="46.224.51.58",
        port=5433,
    )


def readCategory():
    with get_pg_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM category")
            data = cursor.fetchall()
            return data


def readTickerList(category: str = None):
    with get_pg_connection() as conn:
        with conn.cursor() as cursor:
            if category:
                cursor.execute(
                    """
                    SELECT ticker, category_id, change, close
                    FROM ticker_list
                    WHERE category_id = (SELECT category_id FROM category WHERE name = %s)
                    """,
                    (category,),
                )
            else:
                cursor.execute(
                    """
                    SELECT ticker, category_id, change, close
                    FROM ticker_list
                    
                    """
                )
            data = cursor.fetchall()
    return data


def getNews(query: str, lang: str = "en") -> dict:
    load_dotenv("/mpattern/news_api.env")
    api_key = os.getenv("NEWS_API_KEY")
    url = f"https://newsdata.io/api/1/latest?apikey={api_key}&q={query}&language={lang}"
    response = requests.get(url).json()["results"]

    news = [
        {
            "title": news["title"],
            "description": news["description"],
            "url": news["link"],
        }
        for news in response
    ]

    return news


def deleteDataFromFavourites(ticker: str):
    ticker = ticker.upper()
    with get_pg_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM ticker_list WHERE ticker = %s", (ticker,))
            result = cursor.fetchone()
            if result:
                ticker_id = result[0]
                cursor.execute(
                    "DELETE FROM favourites WHERE ticker_id = %s", (ticker_id,)
                )
                conn.commit()


def insertDataIntoFavourites(ticker: str):
    ticker = ticker.upper()
    with get_pg_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
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
            inserted = cursor.fetchone()
            conn.commit()
            return inserted


def readFavorites():
    with get_pg_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT t.ticker, t.change, t.close, t.category_id
                FROM ticker_list t
                JOIN favourites f ON t.id = f.ticker_id
                """
            )
            data = cursor.fetchall()
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
    vol_60d = returns.std() * (252 ** 0.5)
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


def read_db_v2(
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
        with get_pg_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT id FROM ticker_list WHERE ticker = %s", (ticker,))
            ticker_id_row = cursor.fetchone()
            ticker_id = ticker_id_row[0]

            cursor.execute(
                """select max(date) from asset_prices where ticker_id = %s
                and timeframe = %s""",
                (ticker_id, timeframe),
            )
            isUpToDate = cursor.fetchone()[0]
            needs_update = isUpToDate != today

            if needs_update:

                if timeframe != "1d":
                    upData = get_data(ticker=ticker, timeframe=timeframe)

                else:
                    upData = get_data(
                        ticker=ticker,
                        start_date="2008-01-01",
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

                    cursor.executemany(
                        """INSERT INTO asset_prices 
                        (ticker_id, date, open, high, low, close, change,  period, timeframe)
                        VALUES (%s,%s, %s, %s,%s, %s, %s, %s,%s )""",
                        records,
                    )

            if start_date and end_date:
                cursor.execute(
                    "SELECT * FROM asset_prices "
                    "WHERE date BETWEEN %s AND %s  AND ticker_id = %s AND timeframe = %s",
                    (start_date, end_date, ticker_id, timeframe),
                )
            else:
                cursor.execute(
                    "SELECT * FROM asset_prices WHERE ticker_id= %s AND timeframe = %s",
                    (ticker_id, timeframe),
                )

            rows = cursor.fetchall()
            updated_data = pd.DataFrame(
                rows, columns=[col[0] for col in cursor.description]
            )

    except Exception as e:
        raise ValueError(f"Error reading database : {e}")

    return updated_data


def helperFunctionPattern(
    ticker: str, start_date: str = None, end_date: str = None, timeframe: str = "1d"
):
    with get_pg_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("select id from ticker_list where ticker = %s ", (ticker,))
        ticker_id = cursor.fetchone()[0]
        if start_date and end_date:
            cursor.execute(
                "select close, date from asset_prices where ticker_id = %s  and timeframe = %s and date between %s and %s order by date",
                (ticker_id, timeframe, start_date, end_date),
            )
        else:
            cursor.execute(
                "select close, date from asset_prices where ticker_id = %s and timeframe = %s order by date",
                (ticker_id, timeframe),
            )

        data = cursor.fetchall()
        return data


def helperFunctionOhlcPattern(
    ticker: str, start_date: str = None, end_date: str = None, timeframe: str = "1d"
):
    with get_pg_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("select id from ticker_list where ticker = %s", (ticker,))
        ticker_id = cursor.fetchone()[0]
        if start_date and end_date:
            cursor.execute(
                "select date,open,high,low,close from asset_prices where ticker_id = %s and timeframe = %s and date between %s and %s order by date",
                (ticker_id, timeframe, start_date, end_date),
            )
        else:
            cursor.execute(
                "select date,open,high,low,close from asset_prices where ticker_id = %s and timeframe = %s order by date",
                (ticker_id, timeframe),
            )

        data = cursor.fetchall()
        return data


def Helper(ticker: str, start_date: str, end_date: str, timeframe: str = "1d"):
    with get_pg_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("select id from ticker_list where ticker = %s", (ticker,))
        ticker_id = cursor.fetchone()[0]

        cursor.execute(
            """
            select open,high,low,close from asset_prices where ticker_id = %s and timeframe = %s and date between $s  and %s order by date
            """,
            (ticker_id, timeframe, start_date, end_date),
        )

        data = cursor.fetchall()

        return data


def calculate_query_return(ticker: str, start_date: str, end_date: str) -> float:
    try:
        with get_pg_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("select id from ticker_list where ticker = %s", (ticker,))
            ticker_id = cursor.fetchone()[0]
            cursor.execute(
                """
                select close from asset_prices where ticker_id = %s and date between %s and %s
                """,
                (ticker_id, start_date, end_date),
            )
            closes = cursor.fetchall()
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


def pattern_forward_return(
    ticker: str,
    best_dates: list[list[str]],
    rolling_window_low: int = 7,
    rolling_window_high: int = 30,
) -> float:
    ticker = ticker.upper()

    with get_pg_connection() as conn:
        cursor_monthly = conn.cursor()
        cursor_week = conn.cursor()
        cursor = conn.cursor()
        forward_return = []
        avgReturn = []
        summary = []
        seven_day_returns = []
        monthly_returns = []
        pattern_returns = []

        for idx, pat in enumerate(best_dates):
            date_str = pat[-1].strip()
            end_date_dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            end_week_dt = end_date_dt + timedelta(days=rolling_window_low)
            end_month_dt = end_date_dt + timedelta(days=rolling_window_high)
            cursor.execute("select id from ticker_list where ticker = ?", (ticker,))
            ticker_id = cursor.fetchone()[0]
            cursor.execute(
                "SELECT close, date from asset_prices WHERE  date >= ? AND date < ? AND ticker_id = ?",
                (best_dates[idx][0], best_dates[idx][-1], ticker_id),
            )
            cursor_week.execute(
                """SELECT close, date from asset_prices WHERE date BETWEEN ? AND ? AND ticker_id = ?""",
                (end_date_dt, end_week_dt, ticker_id),
            )
            cursor_monthly.execute(
                """SELECT close from asset_prices WHERE date BETWEEN ? AND ? AND ticker_id= ?""",
                (end_date_dt, end_month_dt, ticker_id),
            )
            weekly_data = cursor_week.fetchall()
            monthly_data = cursor_monthly.fetchall()
            data = cursor.fetchall()
            sevenDayReturnAfterPattern = (
                weekly_data[-1][0] / weekly_data[0][0] - 1
            ) * 100
            monthlyReturnAfterPattern = (
                monthly_data[-1][0] / monthly_data[0][0] - 1
            ) * 100
            avgReturnForPattern = (data[-1][0] / data[0][0] - 1) * 100

            seven_day_returns.append(sevenDayReturnAfterPattern)
            monthly_returns.append(monthlyReturnAfterPattern)
            pattern_returns.append(avgReturnForPattern)

            forward_return.append(
                {
                    "patternIdx": idx,
                    "sevenDayReturnAfterPattern": sevenDayReturnAfterPattern,
                    "monthlyReturnAfterPattern": monthlyReturnAfterPattern,
                }
            )

        avgReturn.append(
            {
                "avgReturn": (
                    sum(pattern_returns) / len(pattern_returns)
                    if pattern_returns
                    else 0
                ),
                "avgSevenDayReturn": (
                    sum(seven_day_returns) / len(seven_day_returns)
                    if seven_day_returns
                    else 0
                ),
                "avgMonthlyReturn": (
                    sum(monthly_returns) / len(monthly_returns)
                    if monthly_returns
                    else 0
                ),
            }
        )

        summary.append(
            {
                "Pattern": forward_return,
                "summary": {
                    "Average 7 day return": avgReturn[0]["avgSevenDayReturn"],
                    "Average 30 day return": avgReturn[0]["avgMonthlyReturn"],
                    "Pattern Average return": avgReturn[0]["avgReturn"],
                },
            }
        )

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
