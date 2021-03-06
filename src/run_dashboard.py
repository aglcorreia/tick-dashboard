import argparse
import os
import yaml
import pandas as pd
from datetime import datetime
from dashboard import get_portfolio_prices, get_indirect_positions, calculate_kpis_portfolio_level, \
    calculate_kpis_asset_level
from styles import style_df, style_indirect_holdings_df
from oauth2 import get_oauth_token_and_update_config
from send_email import create_email_message, send_email_message_oauth
from get_portfolio_df import get_google_sheet_df
from utils import set_heroku_config_var


def create_html_tables(
        df: pd.DataFrame,
        csv_schema: dict,
        testing: bool,
        date_to_use: str,
        tax_rate: float,
        tickers_to_replace: dict
):
    # get prices
    portfolio_with_prices = get_portfolio_prices(df, csv_schema, testing=testing, testing_date=date_to_use)

    # calculate kpis for the portfolio at asset level
    portfolio_with_kpis = calculate_kpis_asset_level(portfolio_with_prices, tax_rate, testing=testing,
                                                     testing_date=date_to_use)

    # calculate kpis for the portfolio globally
    portfolio_global_kpis = calculate_kpis_portfolio_level(portfolio_with_kpis)
    portfolio_global_kpis_df = pd.DataFrame.from_dict(portfolio_global_kpis, orient='index').rename(
        columns={0: date_to_use}
    )

    # calculate kpis for the indirect positions
    portfolio_indirect_positions = get_indirect_positions(portfolio_with_kpis,
                                                          tickers_to_replace,
                                                          testing=testing,
                                                          testing_date=date_to_use)

    # styling and saving as html
    amount_cols = [
        'Starting capital',
        'Costs paid so far',
        'Capital after liquidating pre-tax',
        'Capital after liquidating post-tax'
    ]
    pct_cols = [
        'ROC per annum post-tax'
    ]
    portfolio_global_kpis_df_html = style_df(portfolio_global_kpis_df, amount_cols=amount_cols, pct_cols=pct_cols,
                                             row_wise_style=True).render()
    print('Saving global portfolio results...')
    with open("html_outputs/portfolio_global_kpis.html", "w") as file:
        file.write(portfolio_global_kpis_df_html)

    amount_cols = [
        'entry_price',
        'entry_cost',
        'annual_cost',
        'exit_cost_fixed_fee',
        'dividends_received',
        'dividends_costs',
        'lastyears_price',
        'yesterdays_price',
        'todays_price',
        'entry_value',
        'current_value',
        'exit_cost_total',
        'net_gain_ex_dividend_pre_tax',
        'tax_on_gain',
        'annual_costs_paid',
        'net_gain_ex_dividend',
        'net_gain'
    ]
    pct_cols = [
        'exit_cost_pct',
        '1_day_roa',
        'per_annum_roa_ex_dividends',
        'per_annum_roa'
    ]
    date_cols = [
        'entry_date',
        'today_dt'
    ]
    float_cols = [
        'holdings',
        'years_since_entry'
    ]
    portfolio_with_kpis_html = style_df(portfolio_with_kpis.T, amount_cols=amount_cols, pct_cols=pct_cols,
                                        date_cols=date_cols, float_cols=float_cols, row_wise_style=True).render()
    print('Saving asset level results...')
    with open("html_outputs/portfolio_with_kpis.html", "w") as file:
        file.write(portfolio_with_kpis_html)

    amount_cols = [
        'Value'
    ]
    pct_cols = [
        'Pct',
        '∆ daily',
        '∆ annual'
    ]
    bar_cols = [
        'Value',
        '∆ daily',
        '∆ annual'
    ]
    str_cols = [
        'Ticker',
        'Name'
    ]
    portfolio_indirect_positions_html = style_indirect_holdings_df(portfolio_indirect_positions,
                                                                   amount_cols=amount_cols,
                                                                   pct_cols=pct_cols,
                                                                   bar_cols=bar_cols,
                                                                   str_cols=str_cols
                                                                   ).render()
    print('Saving indirect positions results...')
    with open("html_outputs/portfolio_indirect_positions.html", "w") as file:
        file.write(portfolio_indirect_positions_html)
    print('Done.')


def main():
    parser = argparse.ArgumentParser(description="Produce simple portfolio KPIs for a given portfolio")
    parser.add_argument("config", type=str, help="The path to a config yaml required to run the program")
    parser.add_argument("--testdate", type=lambda s: datetime.strptime(s, '%Y-%m-%d'), help="Add a dummy date to test "
                                                                                            "the program")
    parser.add_argument("--local", action='store_true', help="Perform local test. You'll be prompted to input env vars.")
    parser.add_argument("--dummy", action='store_true',
                        help="Perform test on dummy portfolio file.")
    args = parser.parse_args()

    # Initial setup based on the configuration file
    with open(args.config) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    csv_schema = config['portfolio_file']['schema_fields']
    tax_rate = config['parameters']['tax_rate']
    tickers_to_replace = config['tickers_to_replace']

    # check if it is a local test to setup the necessary env vars, otherwise assumes vars will be set already
    # note: in local mode google refresh token is assumed to be empty
    if args.local:
        with open('./configs/vars.yaml') as local_f:
            local_config = yaml.load(local_f, Loader=yaml.FullLoader)
            sender_email = local_config['SENDER_EMAIL']
            receiver_email = local_config['RECEIVER_EMAIL']
            google_client_id = local_config['GOOGLE_CLIENT_ID']
            google_client_secret = local_config['GOOGLE_CLIENT_SECRET']
            google_refresh_token = local_config['GOOGLE_REFRESH_TOKEN']
            if args.dummy:
                google_sheet_id = local_config['GOOGLE_SHEET_DUMMY_ID']
            else:
                google_sheet_id = local_config['GOOGLE_SHEET_ID']
    else:
        try:
            sender_email = os.environ['SENDER_EMAIL']
            receiver_email = os.environ['RECEIVER_EMAIL']
            google_client_id = os.environ['GOOGLE_CLIENT_ID']
            google_client_secret = os.environ['GOOGLE_CLIENT_SECRET']
            google_refresh_token = os.environ['GOOGLE_REFRESH_TOKEN']
            google_sheet_id = os.environ['GOOGLE_SHEET_ID']
        except KeyError as err:
            print(f'Environment variable not available: {err}')
            exit()

    # get access token or refresh token
    refresh_token, access_token, auth_string = get_oauth_token_and_update_config(
        sender_email,
        google_client_id,
        google_client_secret,
        google_refresh_token
    )

    # set refresh token environment variables
    if args.local:
        local_config['GOOGLE_REFRESH_TOKEN'] = refresh_token
        with open('./configs/vars.yaml', 'w') as change_local_config:
            yaml.dump(local_config, change_local_config)
    else:
        set_heroku_config_var('GOOGLE_REFRESH_TOKEN', refresh_token)

    # read portfolio dataframe from google sheets
    df_pfolio = get_google_sheet_df(access_token, google_sheet_id)

    if args.testdate is not None:
        testing = True
        date_to_use = args.testdate
    else:
        testing = False
        date_to_use = datetime.today().strftime('%Y-%m-%d')

    # create the html table outputs
    create_html_tables(df_pfolio, csv_schema, testing, date_to_use, tax_rate, tickers_to_replace)

    # send email
    message = create_email_message(sender_email, receiver_email, date_to_use)
    send_email_message_oauth(message, sender_email, receiver_email, google_client_id, auth_string)


if __name__ == "__main__":
    main()
