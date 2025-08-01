from datetime import date
from math import ceil

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


def passed_check_digit(upc: str) -> bool:
    """
    Compares existing check digit (last digit in UPC) with the calculated check
    digit. Link to formula: https://www.gs1.org/services/how-calculate-check-digit-manually
        :param upc: upc number to  be checked. Must be string so leading 0's aren't truncated.
        :return: Boolean signifying if current check digit matches the calculated check digit.
    """

    cd_coefficient_index: tuple = (3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3)

    current_check_digit: int = int(upc[-1])
    base_upc: str = upc[:-1]

    if len(base_upc) > 17:
        return False

    check_digit_sum: int = 0
    for i in range(1, len(base_upc) + 1):
        check_digit_sum += int(base_upc[-i]) * int(cd_coefficient_index[-i])

    nearest_ten: int = ceil(check_digit_sum/10) * 10
    calculated_check_digit: int = int(nearest_ten - check_digit_sum)

    return current_check_digit == calculated_check_digit


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

    df = (df
          .filter(items=['material_number',
                         'alt_uom',
                         'issue_category',
                         'issue_code',
                         'error_message',
                         'date_discovered',
                         'date_resolved'])
          .rename(columns={'alt_uom': 'uom'}))

    df.columns = [column.upper() for column in df.columns]

    return df


def update_issues(new_df: pd.DataFrame, old_df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds which issues are new/current and which are resolved and need a resolved date.
        :param new_df: pd.DataFrame containing the latest issues found.
        :param old_df: pd.DataFrame containing issues already in the repo table that need updating.
    """

    df = pd.merge(left=new_df,
                  right=old_df,
                  how='outer',
                  on=['MATERIAL_NUMBER', 'UOM', 'ISSUE_CODE'],
                  indicator=True,
                  suffixes=(None, '_OLD'))

    new_df = (df[df['_merge'] == 'left_only']
              .drop(columns=['ERROR_MESSAGE_OLD',
                             'DATE_DISCOVERED_OLD',
                             'DATE_RESOLVED_OLD',
                             'ISSUE_CATEGORY_OLD',
                             '_merge']))

    old_and_current_df = (df[df['_merge'] != 'left_only']
                          .filter(items=['MATERIAL_NUMBER',
                                         'UOM',
                                         'ISSUE_CATEGORY_OLD',
                                         'ISSUE_CODE',
                                         'ERROR_MESSAGE_OLD',
                                         'DATE_DISCOVERED_OLD',
                                         'DATE_RESOLVED_OLD',
                                         '_merge'])

                          .rename(columns={'ERROR_MESSAGE_OLD': 'ERROR_MESSAGE',
                                           'ISSUE_CATEGORY_OLD': 'ISSUE_CATEGORY',
                                           'DATE_DISCOVERED_OLD': 'DATE_DISCOVERED',
                                           'DATE_RESOLVED_OLD': 'DATE_RESOLVED'}))

    old_df = old_and_current_df[old_and_current_df['_merge'] == 'right_only'].drop(columns=['_merge'])
    old_df['DATE_RESOLVED'] = date.today()

    current_df = old_and_current_df[old_and_current_df['_merge'] == 'both'].drop(columns=['_merge'])
    current_df['DATE_RESOLVED'] = None

    return pd.concat([new_df, old_df, current_df])
