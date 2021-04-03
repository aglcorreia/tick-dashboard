from oauth2 import get_oauth_token_and_update_config, refresh_authorization, get_authorization
import pandas as pd
import numpy as np
import requests


def get_google_sheet_df(
        access_token: str,
        google_sheet_id: str,
        sheet_name: str = 'portfolio',
        _range: str = 'A:Z'
) -> pd.DataFrame:
    """from: https://stackoverflow.com/questions/52365907/how-to-access-google-sheets-data-using-python-requests-module"""

    url = f'https://sheets.googleapis.com/v4/spreadsheets/{google_sheet_id}/values/{sheet_name}!{_range}'
    headers = {'authorization': f'Bearer {access_token}',
               'Content-Type': 'application/vnd.api+json'}

    r = requests.get(url, headers=headers)
    values = r.json()['values']
    df = pd.DataFrame(values[1:])
    df.columns = values[0]
    df = df.apply(lambda x: x.str.strip()).replace('', np.nan)
    return df
