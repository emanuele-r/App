from fastapi import FastAPI, HTTPException, Query
from typing import List
from main import *
from pydantic import BaseModel
from typing import Union
from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import redis.asyncio as redis
import orjson


app = FastAPI(
    title="Time Series Motif API",
    description="API for time series analysis and motif discovery",
    version="1.0.0",
    docs_url="/docs",
    default_response_class=ORJSONResponse,
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, compresslevel=5)


class SubsequenceMatch(BaseModel):
    dates: List[str]
    closes: List[float]
    similarity: Union[float, List[float]]
    query_return: Union[float, List[float]]
    description: str


class SubsequenceResponse(BaseModel):
    matches: List[SubsequenceMatch]


class HistoricalPrice(BaseModel):
    date: str
    close: float


class HistoricalPricesResponse(BaseModel):
    prices: List[HistoricalPrice]


redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)


@app.get("/")
async def read_root():
    return {"Hello World"}


@app.get("/health")
async def health_check():
    return {"status": "ok, connected"}


@app.get("/get_favorites")
async def get_favorites_ticker():
    try:

        cache_key = "favourites"

        cached = await redis_client.get(cache_key)

        if cached:
            return orjson.loads(cached)

        data = await readFavorites()

        categoryTypes = [
            {
                "category": category,
                "ticker": ticker,
                "change": change,
                "close": close,
            }
            for ticker, change, close, category in data
        ]
        tickers = [
            {
                "category": category,
                "symbol": ticker,
                "change": change,
                "close": close,
            }
            for ticker, change, close, category in data
        ]

        favourites = {"categoryTypes": categoryTypes, "tickers": tickers}

        await redis_client.set(cache_key, orjson.dumps(favourites).decode(), ex=60)

        return favourites
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/update_favorites")
async def addToFavourites(ticker: str = Query(..., description="Ticker symbol")):
    try:
        await insertDataIntoFavourites(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": "Updated successfully"}


@app.post("/delete_favorites")
async def deleteFromFavourites(ticker: str = Query(..., description="Ticker symbol")):
    try:
        await deleteDataFromFavourites(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": "Removed successfully"}


@app.post("/get_risk_metrics")
def get_risk_metrics(ticker: str = Query(..., description="Ticker symbol")):
    try:
        data = RiskMetrics(ticker)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_ticker_list")
async def get_tickers(category_id: int = Query(default=None, description="Category")):

    try:

        cached_key = "ticker_list"

        cached = await redis_client.get(cached_key)

        if cached:
            return orjson.loads(cached)

        data = await readTickerList(category_id)

        tickers = [
            {
                "category": category_id,
                "symbol": ticker,
                "change": change,
                "price": close,
            }
            for ticker, category_id, change, close in data
        ]
        prices = {"tickers": tickers}

        await redis_client.set(cached_key, orjson.dumps(prices), ex=3600)

        return prices

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_category_list")
async def fetch_category_list():
    try:
        cache_key = "category"
        
        cached = await redis_client.get(cache_key)

        if cached:
            return orjson.loads(cached)
        
        data = await readCategory()
        category = [
            {"category_id": category_id, "category_name": name}
            for category_id, name in data
        ]
        
        await redis_client.set(cache_key, orjson.dumps(category), ex=3600)
        
        return category
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/historical_prices")
async def read_data(
    ticker: str = Query(..., description="Ticker symbol"),
    start_date: str = Query(default=None, description="Start date interval (Optional)"),
    end_date: str = Query(default=None, description="End date interval(Optional)"),
    timeframe: str = Query(default="1d", description="Timeframe (Optional)"),
):
    """
    Example usage : POST /historical_prices?ticker=AAPL
    """

    ticker = ticker.upper()
    try:

        cached_key = f"hist:{ticker}:{timeframe}"

        cached = await redis_client.get(cached_key)
        if cached:
            return orjson.loads(cached)

        data = await read_db_v2(
            ticker=ticker, start_date=start_date, end_date=end_date, timeframe=timeframe
        )

        chartData = []
        for row in data.index:
            data_row = data.loc[row]
            chartData.append(
                {
                    "timeframe": data_row["timeframe"],
                    "date": str(data_row["date"]),
                    "close": float(data_row["close"]),
                }
            )

        await redis_client.set(cached_key, orjson.dumps(chartData), ex=86400)

        return chartData

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get_ohlc")
async def get_ohlc_endpoint(
    ticker: str = Query(..., description="Ticker symbol"),
    start_date: str = Query(default=None, description="Start date interval (Optional)"),
    end_date: str = Query(default=None, description="End date interval(Optional)"),
    timeframe: str = Query(default="1d", description="Timeframe (Optional)"),
):
    """
    Get OHLC (Open, High, Low, Close) data for a ticker.

    Example: POST /get_ohlc?ticker=btc-usd&start_date=2024-01-01&end_date=2024-12-31&timeframe=1d
    """
    ticker = ticker.upper()

    try:
        data = read_db_v2(
            ticker=ticker, start_date=start_date, end_date=end_date, timeframe=timeframe
        )
        if data.empty:
            raise HTTPException(status_code=404, detail=f"No data found for {ticker}")

        ohlc_data = []
        for row in data.index:
            data_row = data.loc[row]
            ohlc_data.append(
                {
                    "date": str(data_row["date"]),
                    "open": float(data_row["open"]),
                    "high": float(data_row["high"]),
                    "low": float(data_row["low"]),
                    "close": float(data_row["close"]),
                    "timeframe": str(data_row["timeframe"]),
                }
            )

        return ohlc_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get_mutiple_patterns")
async def get_patterns(
    ticker: str = Query(..., description="Ticker symbol"),
    start_date: str = Query(...),
    end_date: str = Query(...),
    k: int = Query(3, description="Number of pattern to return"),
    metric: str = Query("l2", description="Distance metric: 'l1' or 'l2'"),
    wrap: bool = Query(True, description="Allow wrapping (circular search)"),
    timeframe: str = Query(
        "1d", description="Timeframe for the reference data ('1d', '1h', etc.)"
    ),
):
    if start_date >= end_date:
        raise HTTPException(
            status_code=400, detail="Start date must be less than end date"
        )

    ticker = ticker.upper()
    try:

        query_data = await helperFunctionPattern(ticker, start_date, end_date, timeframe)
        reference_data = await helperFunctionPattern(ticker, timeframe)

        query = [row[0] for row in query_data]
        array2 = [row[0] for row in reference_data]
        dates = [row[1] for row in reference_data]

        if not query_data:
            raise HTTPException(
                status_code=404, detail="No data found for the given date range"
            )

        if not reference_data:
            raise HTTPException(
                status_code=404, detail="No data found for the given date range"
            )

        query_return = await calculate_query_return(ticker, start_date, end_date)

        best_indices, best_dates, best_subarrays, best_distances, query, array2 = (
            array_with_shift(query, array2, dates, k=k, metric=metric, wrap=wrap)
        )

        summary = await pattern_forward_return(ticker, best_dates)

        matches = []
        for i, (dates_, values, dist) in enumerate(
            zip(best_dates, best_subarrays, best_distances)
        ):
            match = SubsequenceMatch(
                dates=[str(d) for d in dates_],
                closes=[float(v) for v in values],
                similarity=float(dist),
                query_return=float(query_return),
                description=f"{ticker} pattern match {i + 1}",
            )
            matches.append(match)

        return matches, summary

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pattern search failed: {str(e)}")


@app.post("/get_multiple_patterns_ohcl")
async def get_patterns(
    ticker: str = Query(..., description="Ticker symbol"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    k: int = Query(3, description="Number of patterns to return"),
    metric: str = Query("l2", description="Distance metric: 'l1' or 'l2'"),
    wrap: bool = Query(True, description="Allow wrapping (circular search)"),
    timeframe: str = Query(
        "1d", description="Timeframe for the reference data ('1d', '1h', etc.)"
    ),
):

    if start_date >= end_date:
        raise HTTPException(
            status_code=400, detail="Start date must be less than end date"
        )

    ticker = ticker.upper()

    try:
        query_data = await helperFunctionOhlcPattern(ticker, start_date, end_date, timeframe)
        reference_data = await helperFunctionOhlcPattern(ticker, timeframe)

        query = [row[4] for row in query_data]
        array2 = [row[4] for row in reference_data]
        dates = [row[0] for row in reference_data]

        if not query_data:
            raise HTTPException(
                status_code=404, detail="No data found for the given date range"
            )

        if not reference_data:
            raise HTTPException(
                status_code=404, detail="No data found for the given date range"
            )

        query_return = await calculate_query_return(ticker, start_date, end_date)

        best_indices, best_dates, best_subarrays, best_distances, query, array2 = (
            array_with_shift(query, array2, dates, k=k, metric=metric, wrap=wrap)
        )

        matches = []
        for idx, (indices, dates_, values, dist) in enumerate(
            zip(best_indices, best_dates, best_subarrays, best_distances)
        ):
            data = Helper(ticker, dates_[0], dates_[-1], timeframe)
            match = {
                "pattern_id": idx + 1,
                "dates": [d for d in dates_],
                "opens": [row[0] for row in data],
                "highs": [row[1] for row in data],
                "lows": [row[2] for row in data],
                "closes": [row[3] for row in data],
                "similarity": float(dist),
                "query_return": float(query_return),
                "description": f"{ticker} pattern match {idx+1}",
            }

            matches.append(match)

        return {
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "timeframe": timeframe,
            "query_return": float(query_return),
            "patterns": matches,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pattern search failed: {str(e)}")


@app.post("/get_news")
async def fetch_news(
    query: str = Query(..., description="Query for news search"),
    lang: str = Query(default="en", description="Language for news search"),
):
    try:
        news = await getNews(query, lang)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"News search failed: {str(e)}")
    return news
