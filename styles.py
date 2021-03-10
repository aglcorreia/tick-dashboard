import pandas as pd
from pandas.io.formats.style import Styler

def style_df(
        df: pd.DataFrame,
        amount_cols: list,
        pct_cols: list,
        date_cols: list = [],
        float_cols: list = [],
        str_cols: list = [],
        row_wise_style: bool = False,
        max_col_width: str = '230px'
)-> Styler:

    format_dict = {}

    amount_format = "{:,.1f}€"
    for col in amount_cols:
        format_dict[col] = amount_format

    pct_format = "{:.2%}"
    for col in pct_cols:
        format_dict[col] = pct_format

    date_format = "{:%Y-%m-%d}"
    for col in date_cols:
        format_dict[col] = date_format

    float_format = "{:,.1f}"
    for col in float_cols:
        format_dict[col] = float_format

    for col in str_cols:
        df[col] = df[col].str[:23]

    t_style = [
        dict(selector="th", props=[
            ("text-align", "center"),
            ("text-weight", "bold"),
            ("font-size", "14"),
            ("font-family", "monospace"),
            ("width", max_col_width)
        ]),
        dict(selector="index", props=[
            ("text-align", "center"),
            ("text-weight", "bold"),
            ("font-size", "14"),
            ("font-family", "monospace"),
        ]),
        dict(selector="td", props=[
            ("text-align", "center"),
            ("text-weight", "bold"),
            ("font-size", "12"),
            ("font-family", "monospace"),
            ("width", max_col_width),
        ]),
    ]

    if not row_wise_style:
        styled_df = df.style.format(format_dict, na_rep="-").set_table_styles(t_style).hide_index()
    else:
        styled_df = df.style.format(amount_format, subset=pd.IndexSlice[amount_cols,:], na_rep="-") \
            .format(pct_format, subset=pd.IndexSlice[pct_cols,:], na_rep="-") \
            .format(date_format, subset=pd.IndexSlice[date_cols,:], na_rep="-") \
            .format(float_format, subset=pd.IndexSlice[float_cols,:], na_rep="-")\
            .set_table_styles(t_style)

    return styled_df

def style_indirect_holdings_df(
        df: pd.DataFrame,
        amount_cols: list,
        pct_cols: list,
        date_cols: list = [],
        float_cols: list = [],
        str_cols: list = [],
        bar_cols: list = ['current_value_in_portfolio_approx', 'pct_of_portfolio'],
        bar_neg_color: str = 'lightcoral',
        bar_pos_color: str = 'lightgreen'
)-> Styler:

    df = style_df(df, amount_cols, pct_cols, date_cols, float_cols, str_cols)

    return df.bar(subset=bar_cols, align='mid', color=[bar_neg_color, bar_pos_color])