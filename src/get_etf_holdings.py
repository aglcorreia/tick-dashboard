import requests as _requests
import json as _json
import re as _re
import pandas as pd


def get_json(url, proxy=None):
    html = _requests.get(url=url, proxies=proxy).text

    if "QuoteSummaryStore" not in html:
        html = _requests.get(url=url, proxies=proxy).text
        if "QuoteSummaryStore" not in html:
            return {}

    json_str = html.split('root.App.main =')[1].split(
        '(this)')[0].split(';\n}')[0].strip()
    data = _json.loads(json_str)[
        'context']['dispatcher']['stores']['QuoteSummaryStore']

    # return data
    new_data = _json.dumps(data).replace('{}', 'null')
    new_data = _re.sub(
        r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', new_data)

    return _json.loads(new_data)


def get_json_and_replace_tickers(tickers_to_replace, url, proxy=None):
    etf_holdings_df = pd.DataFrame(get_json(url, proxy)['topHoldings']['holdings'])
    etf_holdings_df['symbol'] = etf_holdings_df['symbol'].replace(tickers_to_replace)

    return etf_holdings_df
