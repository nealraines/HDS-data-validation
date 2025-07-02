from datetime import date
import os

import pandas as pd
import pyodbc as odbc

from exempt_pcat import exempt_pcat
from queries import duplicate_upc
from utils import upc_collapse, alt_modulus, format_df


def package_dimensions(df: pd.DataFrame,
                       issue_category: str = 'SUPPLY_CHAIN',
                       issue_code: str = 'INVALID_DIMENSIONS',
                       error_message: str = 'Dimensions are missing or contain all default values (1)') -> pd.DataFrame:
    """
    For alt_uoms with a numerator > 1
    Length, width and height should not all equal 1 (dummy values).
    Length, width or height should not contain nulls
    Length, width or height should not equal 0
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    df_list = []

    df = df[(df['base_uom'] != df['alt_uom']) & (df['conversion_numerator'] > 1)]

    # Length, width and height should not all equal 1 (dummy values).
    is_len_one = df['length'] == 1
    is_wid_one = df['width'] == 1
    is_ht_one = df['height'] == 1

    dummy_dim_df = df[is_len_one & is_wid_one & is_ht_one]
    if not dummy_dim_df.empty:
        df_list.append(dummy_dim_df)

    # Length, width or height should not contain nulls
    is_len_null = df['length'].isna()
    is_wid_null = df['width'].isna()
    is_ht_null = df['height'].isna()

    null_dim_df = df[is_len_null | is_wid_null | is_ht_null]
    if not null_dim_df.empty:
        df_list.append(null_dim_df)

    # Length, width or height should not equal 0
    is_len_zero = df['length'] == 0
    is_wid_zero = df['width'] == 0
    is_ht_zero = df['height'] == 0

    zero_dim_df = df[is_len_zero | is_wid_zero | is_ht_zero]
    if not zero_dim_df.empty:
        df_list.append(zero_dim_df)

    if not df_list:
        return pd.DataFrame()

    master_df = (pd.concat(df_list)
                .drop_duplicates()
                .reset_index(drop=True))

    return format_df(master_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def is_blank_or_zero(df: pd.DataFrame,
                     column_label: str,
                     issue_code: str,
                     error_message: str,
                     issue_category: str = 'SUPPLY_CHAIN') -> pd.DataFrame:
    """
    Target column should not be blank or zero.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param column_label: Target column to evaluate.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    is_blank = df[column_label].isna()
    is_zero = df[column_label] == 0

    df = df[is_blank | is_zero]

    return format_df(df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def is_alt_uom_volume_zero(df: pd.DataFrame,
                           issue_category: str = 'SUPPLY_CHAIN',
                           issue_code: str = 'MISSING_VOLUME',
                           error_message: str = 'Volume should not be blank for AUOM with Numerator > 1.') -> pd.DataFrame:
    """
    Should not be blank for AUOM with Numerator > 1.
    May appear to be 0 due to small LWH values in inches being converted to cubic feet.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    alt_uom_df = df[(df['base_uom'] != df['alt_uom']) & (df['conversion_numerator'] > 1)]

    alt_uom_df = alt_uom_df[(alt_uom_df['volume'].isna()) | (alt_uom_df['volume'] == 0)]

    return format_df(alt_uom_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def smaller_alt_volume(df: pd.DataFrame,
                       issue_category: str = 'SUPPLY_CHAIN',
                       issue_code: str = 'INVALID_VOLUME',
                       error_message: str = 'Greater than or equal to volume of base unit.') -> pd.DataFrame:
    """
    If present, Volume of AUOM level with lesser Qty (Denominator > 1)
    should not be Equal to or Greater than Volume of Base UOM
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    base_df = df[df['base_uom'] == df['alt_uom']][['material_number', 'volume']].rename(columns={'volume': 'b_volume'})

    alt_volume_df = pd.merge(left=df, right=base_df, how='inner', on='material_number')
    alt_volume_df = alt_volume_df[alt_volume_df['base_uom'] != alt_volume_df['alt_uom']]
    alt_volume_df = alt_volume_df[alt_volume_df['conversion_denominator'] > 1]

    alt_volume_df = alt_volume_df[alt_volume_df['volume'] >= alt_volume_df['b_volume']].drop(columns=['b_volume'])

    return format_df(alt_volume_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def larger_alt_volume(df: pd.DataFrame,
                      issue_category: str = 'SUPPLY_CHAIN',
                      issue_code: str = 'INVALID_VOLUME',
                      error_message: str = 'Less than or equal to volume of lower AUOM level.') -> pd.DataFrame:
    """
    Volume of AUOM level with greater Qty (Numerator > 1) should not be
    Equal to or Less than Volume of lower AUOM level. Iterate for all UOM comparisons.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    alt_uom_df = df[(df['base_uom'] != df['alt_uom']) & (df['conversion_numerator'] > 1)].reset_index(drop=True)
    alt_uom_df['volume_test'] = alt_uom_df.groupby('material_number')['volume'].rolling(2).min().reset_index(drop=True)
    alt_uom_df = alt_uom_df.dropna(subset='volume_test')

    alt_uom_df = alt_uom_df[alt_uom_df['volume'] == alt_uom_df['volume_test']].drop(columns=['volume_test'])

    return format_df(alt_uom_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def is_alt_uom_weight_zero(df: pd.DataFrame,
                           issue_category: str = 'SUPPLY_CHAIN',
                           issue_code: str = 'MISSING_WEIGHT',
                           error_message: str = 'Weight should not be blank or zero for AUOM with Numerator > 1.') -> pd.DataFrame:
    """
    Weight should not be blank or zero for AUOM with Numerator > 1.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    alt_uom_df = df[(df['base_uom'] != df['alt_uom']) & (df['conversion_numerator'] > 1)]

    alt_uom_df = alt_uom_df[(alt_uom_df['gross_weight'].isna()) | (alt_uom_df['gross_weight'] == 0)]

    return format_df(alt_uom_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def missing_alternate_uom(df: pd.DataFrame,
                          issue_category: str = 'SUPPLY_CHAIN',
                          issue_code: str = 'MISSING_AUOM',
                          error_message: str = 'Every SKU needs an alternative unit of measure that is not a 1:1 equivalent.') -> pd.DataFrame:
    """
    Every SKU needs an alternative unit of measure that is not a
    1:1 equivalent. Three exceptions disqualify certain SKUs from this rule:
    1. Base unit weight 26 lbs or greater
    2. Base unit is already a case.
    3. SKU belongs in a manually exempt product category.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """

    base_df = df[df['base_uom'] == df['alt_uom']][['material_number', 'gross_weight']].rename(columns={'gross_weight': 'b_gross_weight'})

    df = df.merge(base_df, on='material_number', how='inner')
    df = df[df['conversion_numerator'] >= df['conversion_denominator']] # Removes AUOMs that are smaller than base UOM

    weight_exception = df['b_gross_weight'] >= 26
    base_case_exception = df['base_uom'] == 'CS'
    pcat_exception = df['product_category'].isin(exempt_pcat)

    blacklist = df[weight_exception | base_case_exception | pcat_exception]
    blacklist = blacklist['material_number'].drop_duplicates()

    whitelist = df[['material_number', 'conversion_numerator', 'conversion_denominator']]
    whitelist = whitelist.groupby(by='material_number', as_index=False).sum()
    whitelist = whitelist[whitelist['conversion_numerator'] == whitelist['conversion_denominator']]
    whitelist = whitelist[~whitelist['material_number'].isin(blacklist)]['material_number']

    no_alt_uom_df = df[df['base_uom'] == df['alt_uom']].drop(columns=['b_gross_weight'])

    no_alt_uom_df = no_alt_uom_df[no_alt_uom_df['material_number'].isin(whitelist)]

    return format_df(no_alt_uom_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def invalid_numerator(df: pd.DataFrame,
                      issue_category: str = 'SUPPLY_CHAIN',
                      issue_code: str = 'INVALID_NUMERATOR',
                      error_message: str = 'Numerator should not be 1 if AUOM has greater Volume or Weight than Base UOM.') -> pd.DataFrame:
    """
    Numerator should not be 1 if AUOM has greater Volume or Weight than Base UOM.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """

    base_df = df[df['base_uom'] == df['alt_uom']][['material_number', 'gross_weight', 'volume']]
    base_df = base_df.rename(columns={'volume': 'b_volume', 'gross_weight': 'b_gross_weight'})

    inv_df = df.merge(base_df, on='material_number', how='inner')
    inv_df = inv_df[inv_df['base_uom'] != inv_df['alt_uom']]

    volume_gt = inv_df['volume'] > inv_df['b_volume']
    g_weight_gt = inv_df['gross_weight'] > inv_df['b_gross_weight']
    num_one = inv_df['conversion_numerator'] == 1

    inv_df = inv_df[((volume_gt) | (g_weight_gt)) & (num_one)]
    inv_df = inv_df.drop(columns=['b_gross_weight', 'b_volume'])

    return format_df(inv_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def duplicate_alt_uoms(df: pd.DataFrame,
                       issue_category: str = 'SUPPLY_CHAIN',
                       issue_code: str = 'DUPLICATE_AUOMS',
                       error_message: str = 'Numerator & Denominator for two AUOM levels should not be equal') -> pd.DataFrame:
    """
    Numerator & Denominator for two AUOM levels should not be equal
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    alt_uom_df = df[df['base_uom'] != df['alt_uom']]

    dup_alt_uoms = (alt_uom_df
                    .value_counts(subset=['material_number',
                                          'conversion_numerator',
                                          'conversion_denominator'])
                    .reset_index())
    dup_alt_uoms = dup_alt_uoms[dup_alt_uoms['count'] > 1]
    dup_alt_uoms = dup_alt_uoms.drop(columns=['count'])

    alt_uom_df = alt_uom_df.merge(dup_alt_uoms, how='inner', on=['material_number',
                                                                 'conversion_numerator',
                                                                 'conversion_denominator'])

    return format_df(alt_uom_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def alt_uom_mod(df: pd.DataFrame,
                issue_category: str = 'SUPPLY_CHAIN',
                issue_code: str = 'NON_DIVISIBLE_CONVERSION',
                error_message: str = 'AUOM conversion numerators should be evenly divisible by each other.') -> pd.DataFrame:
    """
    Alternative UOM conversion numerators should be evenly divisible by each other.
    e.g GOOD: 1/1 > 5/1 > 25/1
    e.g BAD: 1/1 > 5/1 > 12/1
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    no_mod_df = df[df['conversion_numerator'] > df['conversion_denominator']]
    no_mod_df = no_mod_df[no_mod_df['base_uom'] != no_mod_df['alt_uom']]

    no_mod_df = no_mod_df[['material_number', 'conversion_numerator']].groupby(by='material_number', as_index=False).agg(alt_modulus)
    no_mod_df = no_mod_df[no_mod_df['conversion_numerator'] == True]['material_number']

    df = df[df['material_number'].isin(no_mod_df)]

    return format_df(df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def inv_conv_by_upc(df: pd.DataFrame,
                    issue_category: str = 'SUPPLY_CHAIN',
                    issue_code: str = 'INVALID_CONVERSION_BY_UPC',
                    error_message: str = 'Num & Denom should not both be 1 if AUOM has different UPC/GTIN value from Base UOM') -> pd.DataFrame:
    """
    Num & Denom should not both be 1 if AUOM has different UPC/GTIN value from Base UOM
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    base_uom = df[df['base_uom'] == df['alt_uom']].rename(columns={'upc': 'base_upc'})

    bad_con_df = df[(df['conversion_numerator'] == 1) & (df['conversion_denominator'] == 1) & (~df['upc'].isna())]

    uom_count = bad_con_df['material_number'].value_counts().reset_index()
    uom_count = uom_count[uom_count['count'] > 1]['material_number']

    bad_con_df = bad_con_df[bad_con_df['material_number'].isin(uom_count)]
    bad_con_df = bad_con_df.merge(base_uom[['material_number', 'base_upc']], how='inner', on='material_number')
    bad_con_df = bad_con_df[bad_con_df['upc'] != bad_con_df['base_upc']]
    bad_con_df = bad_con_df[bad_con_df['base_uom'] != bad_con_df['alt_uom']].drop(columns=['base_upc'])

    return format_df(bad_con_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def redundant_conversion(df: pd.DataFrame,
                         issue_category: str = 'SUPPLY_CHAIN',
                         issue_code: str = 'INVALID_CONVERSION',
                         error_message: str = 'Numerator & Denominator should not be equal and greater than 1.') -> pd.DataFrame:
    """
    Numerator & Denominator for a given AUOM level should not be equal to each other and greater than 1 at the same time
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    df_alt_uom = df[df['base_uom'] != df['alt_uom']]
    df_alt_uom = df_alt_uom[df_alt_uom['conversion_numerator'] == df_alt_uom['conversion_denominator']]
    df_alt_uom = df_alt_uom[df_alt_uom['conversion_numerator'] > 1]

    return format_df(df_alt_uom, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def pallet_case_fault_tolerance(df: pd.DataFrame,
                                issue_category: str = 'SUPPLY_CHAIN',
                                issue_code: str = 'PALLET_VOLUME',
                                error_message: str = 'Volume of PAL level should not be Greater than 120% of Expected/Calculated Volume') -> pd.DataFrame:
    """
    If both CS and PAL levels exist, Volume of PAL level should not be Greater than 120% of
    Expected/Calculated Volume (Volume of CS level multiplied by Number of Cases on a Pallet)
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """

    df = df[(df['alt_uom'] == 'CS') | (df['alt_uom'] == 'PAL')]

    df_count = df.value_counts(subset=['material_number']).reset_index()
    df_count = df_count[df_count['count'] == 2]['material_number']

    df = df[df['material_number'].isin(df_count)]

    pallet_df = df[df['alt_uom'] == 'PAL']
    case_df = df[df['alt_uom'] == 'CS'][['material_number', 'volume', 'conversion_numerator']].rename(columns={'volume': 'case_volume',
                                                                                                               'conversion_numerator': 'case_num'})

    pallet_df = pallet_df.merge(case_df, on='material_number', how='inner')
    pallet_df = pallet_df[pallet_df['conversion_numerator'] > 1]
    pallet_df = pallet_df[pallet_df['case_num'] > 1]
    pallet_df = pallet_df[pallet_df['conversion_numerator'] > pallet_df['case_num']]
    pallet_df['number_of_cases'] = pallet_df['conversion_numerator'] / pallet_df['case_num']
    pallet_df['calculated_volume'] = pallet_df['number_of_cases'] * pallet_df['case_volume']
    pallet_df['volume_diff'] = (pallet_df['volume'] - pallet_df['calculated_volume']) / pallet_df['calculated_volume']

    pallet_df = pallet_df[pallet_df['volume_diff'] > 1.2].drop(columns=['case_volume',
                                                                        'case_num',
                                                                        'number_of_cases',
                                                                        'calculated_volume',
                                                                        'volume_diff'])

    return format_df(pallet_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def smaller_gross_weight_failure(df: pd.DataFrame,
                                 issue_category: str = 'SUPPLY_CHAIN',
                                 issue_code: str = 'WEIGHT_TOLERANCE',
                                 error_message: str = 'Gross weight is outside expected tolerance of calculated gross weight.') -> None:
    """
    If present, Weight of AUOM level with lesser Qty (Denominator > 1) should not be Greater than
    calculated Weight (Weight of Base UOM level divided by the Denominator for Conversion).
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    base_uom_df = df[df['base_uom'] == df['alt_uom']][['material_number', 'gross_weight']].rename(columns={'gross_weight': 'b_gross_weight'})

    alt_uom_df = df[(df['base_uom'] != df['alt_uom']) & (df['conversion_denominator'] > 1)]
    alt_uom_df = alt_uom_df.merge(base_uom_df, on='material_number', how='inner')
    alt_uom_df['calculated_weight'] = alt_uom_df['b_gross_weight'] / alt_uom_df['conversion_denominator']
    alt_uom_df = alt_uom_df[alt_uom_df['gross_weight'] > alt_uom_df['calculated_weight']].drop(columns=['b_gross_weight', 'calculated_weight'])

    return format_df(alt_uom_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def larger_gross_weight_failure(df: pd.DataFrame,
                                issue_category: str = 'SUPPLY_CHAIN',
                                issue_code: str = 'WEIGHT_TOLERANCE',
                                error_message: str = 'Gross weight is outside expected tolerance of calculated gross weight.',
                                upper_tolerance: float=0.25,
                                lower_tolerance: float=-0.05) -> pd.DataFrame:
    """
    Weight of higher AUOM level should not be Less than calculated Weight (Weight of lower UOM level times the Numerator for Conversion ratio).
    Iterate for all UOM comparisons. +25% Variation is acceptable.
    **There should also be a minimal negative variation allowed, to account for measurment precision.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :param upper_tolerance: How much greater the gross_weight is allowed to be in comparison to the calculated weight.
        :param lower_tolerance: How much smaller the gross_weight is allowed to be in comparison to the calculated weight.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    base_uom_df = df[df['base_uom'] == df['alt_uom']][['material_number', 'gross_weight']].rename(columns={'gross_weight': 'b_gross_weight'})

    alt_uom_df = df[(df['base_uom'] != df['alt_uom']) & (df['conversion_numerator'] > 1)]
    alt_uom_df = alt_uom_df.merge(base_uom_df, on='material_number', how='inner')
    alt_uom_df['calculated_weight'] = alt_uom_df['b_gross_weight'] * alt_uom_df['conversion_numerator']
    alt_uom_df['percent_diff'] = (alt_uom_df['gross_weight'] - alt_uom_df['calculated_weight']) / alt_uom_df['calculated_weight']

    alt_uom_df = alt_uom_df[(alt_uom_df['percent_diff'] > upper_tolerance) | (alt_uom_df['percent_diff'] < lower_tolerance)]
    alt_uom_df = alt_uom_df.drop(columns=['b_gross_weight', 'calculated_weight', 'percent_diff'])

    return format_df(alt_uom_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def invalid_gtin(df: pd.DataFrame,
                 issue_category: str = 'SUPPLY_CHAIN',
                 issue_code: str = 'INVALID_UPC',
                 error_message: str = 'UPC failed check digit validation.') -> pd.DataFrame:
    """
    UPC does not match an approved format. Pallets and AUOMs that are 1:1 are excluded from this requirement.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    gtin_df = df[~df['upc'].isna()]
    gtin_df = gtin_df[~gtin_df['upc'].str.match(r'^\d{8}$|^\d{12}$|^\d{13}$|^\d{14}$')]
    gtin_df = gtin_df[gtin_df['alt_uom'] != 'PAL']
    gtin_df = gtin_df[~((gtin_df['base_uom'] != gtin_df['alt_uom']) & (gtin_df['conversion_numerator'] == gtin_df['conversion_denominator']))]

    return format_df(gtin_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def upc_required(df: pd.DataFrame,
                 issue_category: str = 'SUPPLY_CHAIN',
                 issue_code: str = 'NO_UPC',
                 error_message: str = 'Valid UPC/GTIN is required for all valid package levels.') -> pd.DataFrame:
    """
    Valid UPC/GTIN is required for all valid PKG levels (those which are not 1:1) except PAL/PALLET
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """
    no_upc_df = df[df['upc'].isna()]
    no_upc_df = no_upc_df[no_upc_df['alt_uom'] != 'PAL']
    no_upc_df = no_upc_df[~((no_upc_df['base_uom'] != no_upc_df['alt_uom']) & (no_upc_df['conversion_numerator'] == no_upc_df['conversion_denominator']))]

    return format_df(no_upc_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)


def unique_upc(df: pd.DataFrame,
               issue_category: str = 'SUPPLY_CHAIN',
               issue_code: str = 'DUPLICATE_UPC') -> None:
    """
    UPC/GTIN values must be Valid and must be unique for each AUOM entry within the record and across all other records
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |
    """

    with odbc.connect(os.environ.get('Snowflake_Connection_String')) as con:
        duplicate_upc_df = pd.read_sql_query(sql=duplicate_upc, con=con, dtype='string')

    duplicate_upc_df = duplicate_upc_df.groupby('upc').agg(upc_collapse).reset_index()
    duplicate_upc_df['key'] = duplicate_upc_df['error_message']
    duplicate_upc_df = duplicate_upc_df.explode('key').reset_index(drop=True)
    duplicate_upc_df[['material_number', 'alt_uom']] = duplicate_upc_df['key'].str.split(' - ', expand=True)

    for i in range(len(duplicate_upc_df)):
        error_list = duplicate_upc_df.loc[i, 'error_message']
        upc_key = duplicate_upc_df.loc[i, 'upc']
        error_dict = str({upc_key: error_list})

        duplicate_upc_df.loc[i, 'error_message'] = f'Duplicate UPC {error_dict}'

    df = df.merge(duplicate_upc_df, on=['material_number', 'alt_uom'], how='inner')

    return format_df(df, issue_category=issue_category, issue_code=issue_code, error_message=df['error_message'])
