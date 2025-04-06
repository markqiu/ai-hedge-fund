import pandas as pd
import requests

from data.cache import get_cache
from data.models import (
    CompanyNews,
    FinancialMetrics,
    Price,
    LineItem,
    LineItemResponse,
    InsiderTrade,
)

# Global cache instance
_cache = get_cache()
base_url = 'http://47.83.223.200:8000'


# base_url = 'http://172.30.64.1:8000'
def get_prices(ticker: str, start_date: str, end_date: str) -> list[Price]:
    """Fetch price data from cache or API."""
    # Check cache first
    if cached_data := _cache.get_prices(ticker):
        # Filter cached data by date range and convert to Price objects
        filtered_data = [Price(**price) for price in cached_data if start_date <= price["time"] <= end_date]
        if filtered_data:
            return filtered_data

    # If not in cache or no data in range, fetch from API

    url = base_url + "/api/v1/equity/search"
    params = {
        "provider": "xiaoyuan",
    }
    response = requests.get(url, params=params).json().get('results', [])
    base_df = pd.DataFrame(response)
    base_df['code'] = base_df['symbol'].apply(lambda x: x.split('.')[0])

    ticker = [i for i in ticker.split(',')]
    ticker = [i.split('.')[0] for i in ticker]
    df = base_df[base_df['code'].isin(ticker)]
    ticker = df['symbol'].tolist()
    ticker = ','.join(ticker)
    stock_url = base_url + "/api/v1/equity/price/historical"
    params = {
        "provider": "xiaoyuan",
        "symbol": ticker,
        "start_date": start_date,
        "end_date": end_date,
    }
    df = requests.get(stock_url, params=params).json().get('results', [])
    df = pd.DataFrame(df)
    df.rename(columns={'date': 'time'}, inplace=True)
    prices = [Price(**row) for row in df.to_dict(orient='records')]
    if not prices:
        return []

    # Cache the results as dicts
    _cache.set_prices(ticker, [p.model_dump() for p in prices])
    return prices


def get_financial_metrics(
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
) -> list[FinancialMetrics]:
    """Fetch financial metrics from cache or API."""

    # If not in cache or insufficient data, fetch from API
    url = base_url + "/api/v1/equity/search"
    params = {
        "provider": "xiaoyuan",
    }
    response = requests.get(url, params=params).json().get('results', [])
    base_df = pd.DataFrame(response)
    base_df['code'] = base_df['symbol'].apply(lambda x: x.split('.')[0])

    ticker = [i for i in ticker.split(',')]
    ticker = [i.split('.')[0] for i in ticker]
    df = base_df[base_df['code'].isin(ticker)]
    code = df['symbol'].tolist()
    code = ','.join(code)

    # Check cache first
    if cached_data := _cache.get_financial_metrics(code):
        # Filter cached data by date and limit
        filtered_data = [FinancialMetrics(**metric) for metric in cached_data if metric["report_period"] <= end_date]
        filtered_data.sort(key=lambda x: x.report_period, reverse=True)
        if filtered_data:
            return filtered_data[:limit]
    stock_url = base_url + "/api/v1/equity/fundamental/ratios"
    stock_url2 = base_url + "/api/v1/equity/fundamental/metrics"
    stock_url3 = base_url + "/api/v1/equity/fundamental/cash_growth"
    stock_url4 = base_url + "/api/v1/equity/fundamental/income_growth"
    stock_url5 = base_url + "/api/v1/equity/fundamental/balance"
    params = {
        "provider": "fmp",
        "symbol": code,
        'limit': limit,
        'period': 'quarter' if period == 'ttm' else 'annual',
        'with_ttm': True if period == 'ttm' else False,
    }
    if period == 'ttm':
        period = 'ytd'
    params2 = {
        "provider": "xiaoyuan",
        "symbol": code,
        'limit': limit,
        'period': period,
    }
    ticker = requests.get(stock_url, params=params).json().get('results', [])
    ticker2 = requests.get(stock_url2, params=params).json().get('results', [])
    ticker3 = requests.get(stock_url3, params=params).json().get('results', [])
    ticker4 = requests.get(stock_url4, params=params).json().get('results', [])
    ticker5 = requests.get(stock_url4, params=params2).json().get('results', [])
    ticker6 = requests.get(stock_url2, params=params2).json().get('results', [])
    ticker7 = requests.get(stock_url5, params=params).json().get('results', [])
    df = pd.DataFrame(ticker)
    df['symbol'] = code
    df2 = pd.DataFrame(ticker2)
    df3 = pd.DataFrame(ticker3)
    df4 = pd.DataFrame(ticker4)
    df5 = pd.DataFrame(ticker5)
    df6 = pd.DataFrame(ticker6)
    df7 = pd.DataFrame(ticker7)
    # 计算book_value_growth = 总资产- 总负债 同比增长率
    df7['book_value_growth'] = (df7['total_assets'] - df7['total_liabilities']) / (
                df7['total_assets'] - df7['total_liabilities']).shift(1) - 1
    df = pd.concat([df, df2, df3, df4, df5, df6, df7], axis=1)
    # 删除report_period为nan的行
    df = df.dropna(subset=['period_ending'])
    df['currency'] = 'CNY'
    df.rename(columns={
        'symbol': 'ticker',
        'period_ending': 'report_period',
        "fiscal_period": "period",
        'currency': 'currency',
        'pe_ratio': 'price_to_earnings_ratio',
        'price_to_book': 'price_to_book_ratio',
        "price_to_sales": "price_to_sales_ratio",
        "ev_to_ebitda": "enterprise_value_to_ebitda_ratio",
        "gross_profit_margin": "gross_margin",
        "operating_profit_margin": "operating_margin",
        "net_profit_margin": "net_margin",
        # 'growth_change_in_working_capital':'working_capital_turnover',
        "growth_operating_cash_flow": "operating_cash_flow_ratio",
        "growth_basic_earings_per_share": 'earnings_per_share_growth',
        # "growth_free_cash_flow": 'free_cash_flow_growth',
        "growth_operating_income": "operating_income_growth",
        "growth_ebitda": "ebitda_growth",
        "growth_revenue": "revenue_growth",
        "eps_ttm": 'earnings_per_share',
        'price_earnings_to_growth_ratio': 'peg_ratio',
        'enterprise_value_multiple': 'enterprise_value_to_revenue_ratio',

    }, inplace=True)

    # df列名去重
    df = df.loc[:, ~df.columns.duplicated()]
    df['earnings_growth'] = 0
    df['fiscal_period'] = df['fiscal_year'].astype(str) + '-' + df['period']
    financial_metrics = [FinancialMetrics(**row) for row in df.to_dict(orient='records')]
    if not financial_metrics:
        return []
    # Cache the results as dicts
    _cache.set_financial_metrics(code, [m.model_dump() for m in financial_metrics])
    return financial_metrics


def search_line_items(
        ticker: str,
        line_items: list[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
) -> list[LineItem]:
    """Fetch line items from API."""
    # If not in cache or insufficient data, fetch from API
    url = base_url + "/api/v1/equity/search"
    params = {
        "provider": "xiaoyuan",
    }
    response = requests.get(url, params=params).json().get('results', [])
    base_df = pd.DataFrame(response)
    base_df['code'] = base_df['symbol'].apply(lambda x: x.split('.')[0])

    ticker = [i for i in ticker.split(',')]
    ticker = [i.split('.')[0] for i in ticker]
    df = base_df[base_df['code'].isin(ticker)]
    ticker = df['symbol'].tolist()
    ticker = ','.join(ticker)

    stock_url = base_url + "/api/v1/equity/fundamental/balance"
    params3 = {
        "provider": "fmp",
        "symbol": ticker,
        'limit': limit,
        'period': 'quarter' if period == 'ttm' else 'annual',
        'with_ttm': True if period == 'ttm' else False,

    }
    if period == 'ttm':
        period = 'ytd'
    params = {
        "provider": "xiaoyuan",
        "symbol": ticker,
        'period': period
    }
    df = requests.get(stock_url, params=params).json().get('results', [])
    df = pd.DataFrame(df)

    stock_url2 = base_url + "/api/v1/equity/fundamental/income"
    stock_url3 = base_url + "/api/v1/equity/fundamental/metrics"
    stock_url4 = base_url + "/api/v1/equity/fundamental/ratios"
    stock_url5 = base_url + "/api/v1/equity/fundamental/cash"
    stock_url6 = base_url + "/api/v1/equity/fundamental/balance"

    params2 = {
        "provider": "xiaoyuan",
        "symbol": ticker,
        'period': period
    }

    ticker2 = requests.get(stock_url2, params=params2).json().get('results', [])
    ticker3 = requests.get(stock_url3, params=params3).json().get('results', [])
    ticker4 = requests.get(stock_url4, params=params3).json().get('results', [])
    ticker5 = requests.get(stock_url5, params=params3).json().get('results', [])
    ticker6 = requests.get(stock_url6, params=params3).json().get('results', [])
    ticker7 = requests.get(stock_url3, params=params2).json().get('results', [])
    df2 = pd.DataFrame(ticker2)
    df3 = pd.DataFrame(ticker3)
    df4 = pd.DataFrame(ticker4)
    df5 = pd.DataFrame(ticker5)
    df6 = pd.DataFrame(ticker6)
    df7 = pd.DataFrame(ticker7)

    df = pd.concat([df, df2, df3, df4, df5, df6, df7], axis=1)
    df = df.loc[:, ~df.columns.duplicated()]
    df.rename(columns={'timestamp': 'time',
                       'symbol': 'ticker',
                       'period_ending': 'report_period',
                       'fiscal_period': 'period',
                       'basic_earnings_per_share': 'earnings_per_share',
                       'pe_ratio': 'price_to_earnings_ratio',
                       'price_to_book': 'price_to_book_ratio',
                       'total_current_assets': 'current_assets',
                       'total_current_liabilities': 'current_liabilities',
                       "dividend_payout_ratio": "dividends_and_other_cash_distributions",
                       "total_operating_income": "revenue",
                       "operating_profit_margin": "operating_margin",
                       'research_and_development_expense': 'research_and_development',
                       "gross_profit_margin": "gross_margin",
                       'total_shareholders_equity': 'shareholders_equity',
                       }, inplace=True)
    df['outstanding_shares'] = 0  # 流通股
    df['free_cash_flow'] = 0
    df['currency'] = 'CNY'
    df['cash_and_equivalents'] = 0
    df.dropna(subset=['report_period'], inplace=True)
    # 将 DataFrame 转换为 LineItem 对象列表
    line_items = [LineItem(**row) for row in df.to_dict(orient='records')]
    # 创建 LineItemResponse 实例
    line_item_response = LineItemResponse(search_results=line_items)

    if not line_item_response:
        return []

    # Cache the results
    return line_item_response.search_results[:limit]


def get_insider_trades(
        ticker: str,
        end_date: str,
        start_date: str | None = None,
        limit: int = 1000,
) -> list[InsiderTrade]:
    """Fetch insider trades from cache or API."""
    # Check cache first

    url = base_url + "/api/v1/equity/search"
    params = {
        "provider": "xiaoyuan",
    }
    response = requests.get(url, params=params).json().get('results', [])
    base_df = pd.DataFrame(response)
    base_df['code'] = base_df['symbol'].apply(lambda x: x.split('.')[0])

    ticker = [i for i in ticker.split(',')]
    ticker = [i.split('.')[0] for i in ticker]
    df = base_df[base_df['code'].isin(ticker)]
    ticker = df['symbol'].tolist()
    ticker = ','.join(ticker)

    if cached_data := _cache.get_insider_trades(ticker):
        # Filter cached data by date range
        filtered_data = [InsiderTrade(**trade) for trade in cached_data
                         if
                         (start_date is None or (trade.get("transaction_date") or trade["filing_date"]) >= start_date)
                         and (trade.get("transaction_date") or trade["filing_date"]) <= end_date]
        filtered_data.sort(key=lambda x: x.transaction_date or x.filing_date, reverse=True)
        if filtered_data:
            return filtered_data

    stock_url = 'http://172.19.224.1:8000' + "/api/v1/equity/ownership/major_holders"
    params = {"symbol": ticker,
              "start_date": None,
              "end_date": end_date,
              'limit': limit
              }
    df = requests.get(stock_url, params=params).json().get('results', [])
    df = pd.DataFrame(df)
    # 将 DataFrame 转换为 InsiderTrade 对象列表
    insider_trades = [InsiderTrade(**row) for row in df.to_dict(orient='records')]
    # Cache the results
    _cache.set_insider_trades(ticker, [trade.model_dump() for trade in insider_trades])
    return insider_trades


def get_company_news(
        ticker: str,
        end_date: str,
        start_date: str | None = None,
        limit: int = 1000,
) -> list[CompanyNews]:
    """Fetch company news from cache or API."""

    # If not in cache or insufficient data, fetch from API
    url = base_url + "/api/v1/equity/search"
    params = {
        "provider": "xiaoyuan",
    }
    response = requests.get(url, params=params).json().get('results', [])
    base_df = pd.DataFrame(response)
    base_df['code'] = base_df['symbol'].apply(lambda x: x.split('.')[0])

    ticker = [i for i in ticker.split(',')]
    ticker = [i.split('.')[0] for i in ticker]
    df = base_df[base_df['code'].isin(ticker)]
    ticker = df['symbol'].tolist()
    ticker = ','.join(ticker)

    # Check cache first
    if cached_data := _cache.get_company_news(ticker):
        # Filter cached data by date range
        filtered_data = [CompanyNews(**news) for news in cached_data
                         if (start_date is None or news["date"] >= start_date)
                         and news["date"] <= end_date]
        filtered_data.sort(key=lambda x: x.date, reverse=True)
        if filtered_data:
            return filtered_data

    stock_url = base_url + "/api/v1/news/company"
    params = {"symbol": ticker,
              "start_date": start_date,
              "end_date": end_date,
              }
    df = requests.get(stock_url, params=params).json().get('results', [])
    df = pd.DataFrame(df)
    df['sentiment'] = 'neutral'

    # 将 DataFrame 转换为 CompanyNews 对象列表
    all_news = [CompanyNews(**row) for row in df.to_dict(orient='records')]
    # Cache the results
    _cache.set_company_news(ticker, [news.model_dump() for news in all_news])
    return all_news


def get_market_cap(
        ticker: str,
        end_date: str,
) -> float | None:
    """Fetch market cap from the API."""
    financial_metrics = get_financial_metrics(ticker, end_date)
    market_cap = financial_metrics[0].market_cap
    if not market_cap:
        return None

    return market_cap


def prices_to_df(prices: list[Price]) -> pd.DataFrame:
    """Convert prices to a DataFrame."""
    df = pd.DataFrame([p.model_dump() for p in prices])
    df["Date"] = pd.to_datetime(df["time"])
    df.set_index("Date", inplace=True)
    numeric_cols = ["open", "close", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_index(inplace=True)
    return df


# Update the get_price_data function to use the new functions
def get_price_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    prices = get_prices(ticker, start_date, end_date)
    return prices_to_df(prices)
