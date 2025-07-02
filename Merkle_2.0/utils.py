from datetime import date

import pandas as pd

def upc_collapse(series: pd.Series) -> str:
    """
    Custom pd.Series.agg() function for unique_upc(). Returns list values
    partitioned by upc.
    """
    return series.to_list()


def alt_modulus(series: pd.Series) -> bool:
    """
    Custom pd.Series.agg() function for alt_uom_mod().
    Returns TRUE if conversion numerators don't divide evenly by each other
    after being partitioned by material_number.
    """
    series_list = sorted(series.to_list(), reverse=True)

    if len(series_list) == 1:
        return False

    for i in range(len(series_list) - 1):
        if series_list[i] % series_list[i + 1] != 0:
            return True

    return False


def format_df(df: pd.DataFrame, issue_category: str, issue_code: str, error_message: str) -> pd.DataFrame:
    """
    Formats submitted dataframe for issue repository table in Snowflake.
    df argument requires material_number and alt_uom columns.
        :param df: Target dataframe to format.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :return: Formatted pd.DataFrame that can be submitted directly to issue repository table.
    """

    df['date_discovered'] = date.today()
    df['date_resolved'] = None
    df['issue_code'] = issue_code
    df['issue_category'] = issue_category
    df['error_message'] = error_message

    return df.filter(items=['material_number',
                            'alt_uom',
                            'date_discovered',
                            'date_resolved',
                            'issue_category',
                            'issue_code',
                            'error_message'])
