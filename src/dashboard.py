import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from get_etf_holdings import get_json_and_replace_tickers

def setup_datetime_parameters(
        testing: bool = False,
        testing_date: str = '2021-02-26'
) -> tuple:
    if testing:
        # hard-coded for testing on weekends:
        if isinstance(testing_date, str):
            today_dt = datetime.strptime(testing_date, '%Y-%m-%d')
        elif isinstance(testing_date, datetime):
            today_dt = testing_date
    else:
        today_dt = datetime.today()

    today = today_dt.strftime('%Y-%m-%d')
    tomorrow = (today_dt + timedelta(1)).strftime('%Y-%m-%d')
    yesterday = (today_dt - timedelta(1)).strftime('%Y-%m-%d')
    one_yr_ago = (today_dt - timedelta(365)).strftime('%Y-%m-%d')
    one_yr_ago_plus1 = (today_dt - timedelta(364)).strftime('%Y-%m-%d')

    strings_to_datetime = {
        'today': today,
        'yesterday': yesterday,
        'one_yr_ago': one_yr_ago,
        'tomorrow': tomorrow,
        'one_yr_ago_plus1': one_yr_ago_plus1
    }

    datetime_to_strings = {
        today: 'todays_price',
        yesterday: 'yesterdays_price',
        one_yr_ago: 'lastyears_price',
        tomorrow: 'tomorrow',
        one_yr_ago_plus1: 'one_yr_ago_plus1'
    }

    return strings_to_datetime, datetime_to_strings


def preprocess_portfolio_dataframe(
        df: pd.DataFrame,
        csv_schema: dict
) -> pd.DataFrame:
    schema_fields = list(csv_schema.keys())
    required_schema_fields = [x for x in schema_fields if csv_schema[x]['required'] == True]
    optional_schema_fields = [x for x in schema_fields if csv_schema[x]['required'] == False]
    required_schema_fields_available_in_df = [x for x in required_schema_fields if x in df.columns]
    assert len(required_schema_fields_available_in_df) == len(required_schema_fields), \
        f"Some required fields are not available in the portfolio table: \n" \
        f"{[x for x in required_schema_fields if x not in df.columns]}"
    for optional_field in optional_schema_fields:
        if optional_field not in df.columns:
            print(
                f" - The optional field {optional_field} is not available in the portfolio table. Field description: {csv_schema[optional_field]['description']}")

    df.columns = map(str.lower, df.columns)

    for field in csv_schema.keys():
        if csv_schema[field]['type'] == "string":
            if field not in df.columns:
                df[field] = "na"
            df[field] = df[field].astype("string").str.lower()
        elif csv_schema[field]['type'] == "datetime":
            if field not in df.columns:
                df[field] = pd.Timestamp("1900-01-01")
            df[field] = pd.to_datetime(df[field], format=csv_schema[field]['format'])
        elif csv_schema[field]['type'] == "float":
            if field not in df.columns:
                df[field] = 0.0
            df[field] = df[field].astype("float16")
        elif csv_schema[field]['type'] == "integer":
            if field not in df.columns:
                df[field] = 0
            df[field] = df[field].astype("int16")

    df['ticker'] = df['ticker'].str.upper()

    return df


def get_ticker_prices(
        df: pd.DataFrame,
        testing: bool = False,
        testing_date: str = '2021-02-26'
) -> tuple:

    if 'asset_type' in df.columns:
        tickers = list(df[df['asset_type'] != 'cash']['ticker'].unique())
    else:
        tickers = list(df['ticker'].unique())

    strings_to_datetime, datetime_to_strings = setup_datetime_parameters(testing, testing_date)

    yesterday = strings_to_datetime['yesterday']
    tomorrow = strings_to_datetime['tomorrow']
    one_yr_ago = strings_to_datetime['one_yr_ago']
    one_yr_ago_plus1 = strings_to_datetime['one_yr_ago_plus1']

    prices_since_yday = yf.download(tickers, start=yesterday, end=tomorrow)
    prices_last_year = yf.download(tickers, start=one_yr_ago, end=one_yr_ago_plus1)

    adj_close_since_yday = prices_since_yday['Adj Close'].reset_index()
    adj_close_one_yr_ago = prices_last_year['Adj Close'].reset_index()

    adj_close_since_yday['Date'] = adj_close_since_yday['Date'].astype("string").str[:10].map(datetime_to_strings)
    adj_close_one_yr_ago['Date'] = adj_close_one_yr_ago['Date'].astype("string").str[:10].map(datetime_to_strings)

    adj_close_since_yday['CASH'] = 1.0
    adj_close_one_yr_ago['CASH'] = 1.0

    adj_close_since_yday = adj_close_since_yday.set_index('Date').T.reset_index().rename(columns={'index': 'ticker'})
    adj_close_one_yr_ago = adj_close_one_yr_ago.set_index('Date').T.reset_index().rename(columns={'index': 'ticker'})

    return adj_close_since_yday, adj_close_one_yr_ago


def get_portfolio_prices(
        df: pd.DataFrame,
        csv_schema: dict,
        testing: bool = False,
        testing_date: str = '2021-02-26'
) -> pd.DataFrame:
    preprocessed_portfolio = preprocess_portfolio_dataframe(df, csv_schema)
    yesterdays_prices, lastyears_prices = get_ticker_prices(preprocessed_portfolio, testing, testing_date)

    df = preprocessed_portfolio.merge(lastyears_prices, on='ticker', how='left') \
        .merge(yesterdays_prices, on='ticker', how='left')

    return df


def calculate_kpis_asset_level(
        portfolio_updated: pd.DataFrame,
        tax_rate: 'float' = 0.28,
        testing: bool = False,
        testing_date: str = '2021-02-26'
) -> pd.DataFrame:
    strings_to_datetime, datetime_to_strings = setup_datetime_parameters(testing, testing_date)

    portfolio_updated['today_dt'] = pd.to_datetime(strings_to_datetime['today'])
    portfolio_updated['years_since_entry'] = ((portfolio_updated['today_dt'] - portfolio_updated['entry_date']).astype(
        'timedelta64[D]')) / 365
    portfolio_updated['entry_value'] = portfolio_updated['holdings'] * portfolio_updated['entry_price']
    portfolio_updated['current_value'] = portfolio_updated['holdings'] * portfolio_updated['todays_price']
    portfolio_updated['exit_cost_total'] = portfolio_updated['exit_cost_fixed_fee'] + portfolio_updated[
        'exit_cost_pct'] * portfolio_updated['current_value']
    portfolio_updated['net_gain_ex_dividend_pre_tax'] = portfolio_updated['current_value'] - portfolio_updated[
        'entry_value'] - portfolio_updated['entry_cost'] - portfolio_updated['exit_cost_total']
    portfolio_updated['tax_on_gain'] = portfolio_updated['net_gain_ex_dividend_pre_tax'] * tax_rate
    portfolio_updated['annual_costs_paid'] = portfolio_updated['annual_cost'] * portfolio_updated['years_since_entry']
    portfolio_updated['net_gain_ex_dividend'] = portfolio_updated['net_gain_ex_dividend_pre_tax'] - portfolio_updated[
        'tax_on_gain'] - portfolio_updated['annual_costs_paid']
    portfolio_updated['net_gain'] = portfolio_updated['net_gain_ex_dividend'] + portfolio_updated[
        'dividends_received'] - portfolio_updated['dividends_costs']

    portfolio_updated['1_day_roa'] = portfolio_updated['todays_price'] / portfolio_updated['yesterdays_price'] - 1
    portfolio_updated['per_annum_roa_ex_dividends'] = ((portfolio_updated['net_gain_ex_dividend']
                                                        + portfolio_updated['entry_value'])
                                                       / portfolio_updated['entry_value']) \
                                                      ** (1 / portfolio_updated['years_since_entry']) - 1
    portfolio_updated['per_annum_roa'] = ((portfolio_updated['net_gain']
                                           + portfolio_updated['entry_value'])
                                          / portfolio_updated['entry_value']) \
                                         ** (1 / portfolio_updated['years_since_entry']) - 1

    return portfolio_updated


def calculate_kpis_portfolio_level(
        portfolio_with_kpis: pd.DataFrame
) -> dict:
    portfolio_kpis = {}
    portfolio_kpis['Starting capital'] = portfolio_with_kpis['entry_value'].sum()
    portfolio_kpis['Costs paid so far'] = portfolio_with_kpis['annual_costs_paid'].sum() \
                                          + portfolio_with_kpis['entry_cost'].sum()

    portfolio_kpis['Capital after liquidating pre-tax'] = portfolio_with_kpis['current_value'].sum() \
                                                          - portfolio_with_kpis['exit_cost_total'].sum()
    portfolio_kpis['Capital after liquidating post-tax'] = portfolio_with_kpis['current_value'].sum() \
                                                           - portfolio_with_kpis['exit_cost_total'].sum() \
                                                           - portfolio_with_kpis['tax_on_gain'].sum()
    portfolio_kpis['ROC per annum post-tax'] = portfolio_with_kpis['per_annum_roa'].dot(
        portfolio_with_kpis['current_value']) / portfolio_with_kpis['current_value'].sum()

    return portfolio_kpis


def get_indirect_positions(
        portfolio_with_kpis: pd.DataFrame,
        tickers_to_replace: dict,
        testing: bool = False,
        testing_date: str = '2021-02-26'
) -> pd.DataFrame:

    columns_to_get_from_portfolio = ['ticker', 'asset_type', 'current_value']
    portfolio_indirect_positions = portfolio_with_kpis[columns_to_get_from_portfolio].copy(deep=False)
    portfolio_indirect_positions = portfolio_indirect_positions.groupby(['asset_type', 'ticker']).sum().reset_index()
    portfolio_total_value = portfolio_indirect_positions['current_value'].sum()
    portfolio_indirect_positions['pct_of_portfolio'] = portfolio_indirect_positions[
                                                           'current_value'] / portfolio_total_value
    etfs_in_portfolio = portfolio_indirect_positions[portfolio_indirect_positions['asset_type'] == 'etf'][
        'ticker'].unique().tolist()

    holdings_df = pd.DataFrame([])
    for etf in etfs_in_portfolio:
        holdings_yahoo_url = f'https://finance.yahoo.com/quote/{etf}/holdings?p={etf}'
        etf_holdings = get_json_and_replace_tickers(tickers_to_replace, holdings_yahoo_url).rename(
            columns={
                'symbol': 'ticker',
                'holdingName': 'holding_name',
                'holdingPercent': 'holding_percent'
            }
        )
        etf_holdings['etf'] = etf
        holdings_df = pd.concat([holdings_df, etf_holdings]).reset_index(drop=True)

    yesterdays_prices, lastyears_prices = get_ticker_prices(holdings_df, testing=testing, testing_date=testing_date)
    holdings_df = holdings_df.merge(lastyears_prices, on='ticker', how='left').merge(yesterdays_prices, on='ticker',
                                                                                     how='left')
    holdings_df['holding_daily_return'] = holdings_df['todays_price'] / holdings_df['yesterdays_price'] - 1
    holdings_df['holding_annual_return'] = holdings_df['todays_price'] / holdings_df['lastyears_price'] - 1
    holdings_df = holdings_df.rename(columns={'ticker': 'holding_ticker', 'etf': 'ticker'})
    holdings_df.drop(['lastyears_price', 'yesterdays_price', 'todays_price'], axis=1, inplace=True)
    portfolio_indirect_positions = portfolio_indirect_positions.merge(holdings_df, on='ticker', how='left')

    portfolio_indirect_positions.loc[portfolio_indirect_positions['asset_type'] == 'cash', 'holding_ticker'] = 'CASH'
    portfolio_indirect_positions.loc[portfolio_indirect_positions['asset_type'] == 'cash', 'holding_name'] = 'Cash'
    portfolio_indirect_positions.loc[portfolio_indirect_positions['asset_type'] == 'cash', 'holding_percent'] = 1.0
    portfolio_indirect_positions.loc[portfolio_indirect_positions['asset_type'] == 'cash', 'holding_daily_return'] = 0.0
    portfolio_indirect_positions.loc[
        portfolio_indirect_positions['asset_type'] == 'cash', 'holding_annual_return'] = 0.0

    portfolio_indirect_positions['pct_of_portfolio'] = portfolio_indirect_positions['pct_of_portfolio'] * \
                                                       portfolio_indirect_positions['holding_percent']
    portfolio_indirect_positions['current_value'] = portfolio_indirect_positions['current_value'] * \
                                                    portfolio_indirect_positions['holding_percent']

    cols_to_groupby = [
        'holding_ticker',
        'holding_name'
    ]

    cols_to_sum = [
        'current_value',
        'pct_of_portfolio'
    ]

    cols_to_first = [
        'holding_daily_return',
        'holding_annual_return'
    ]

    sums = portfolio_indirect_positions[cols_to_groupby + cols_to_sum].groupby(cols_to_groupby).sum().reset_index()
    firsts = portfolio_indirect_positions[cols_to_groupby + cols_to_first].groupby(
        cols_to_groupby).first().reset_index()
    result = sums.merge(firsts, on=cols_to_groupby, how='left').sort_values(by='pct_of_portfolio',
                                                                            ascending=False).rename(
        columns={
            'holding_ticker': 'Ticker',
            'holding_name': 'Name',
            'current_value': 'Value',
            'pct_of_portfolio': 'Pct',
            'holding_daily_return': '∆ daily',
            'holding_annual_return': '∆ annual',
        }
    )

    return result

