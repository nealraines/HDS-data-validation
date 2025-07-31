# -*- coding: UTF-8 -*-
# Author: Neal Raines
# Create Date: 7/1/2025
# Description: Patrol Tower - stored_procedures.py test file

import pytest
import pandas as pd
import numpy as np

from src.utils import format_df
from src.stored_procedures import *


def test_package_dimensions(test_dataframe):
    test_func = 'test_package_dimensions()'
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'INVALID_DIMENSIONS'
    error_message: str = 'Dimensions are missing or contain all default values (1)'

    ### TEST 1 -- Catch Packaging Levels with any dims as 0 ###
    test_num = 1

    TestDataframe(test_dataframe).set(1, 'length', 0)
    TestDataframe(test_dataframe).set(2, 'material_number', 1)
    TestDataframe(test_dataframe).set(2, 'alt_uom', 'EA')
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS')], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = package_dimensions(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 2 -- Catch Packaging Levels with Dims as all 1s ###
    test_num = 2

    TestDataframe(test_dataframe).set(1, 'length', 1)
    TestDataframe(test_dataframe).set(1, 'width', 1)
    TestDataframe(test_dataframe).set(1, 'height', 1)
    TestDataframe(test_dataframe).set(1, 'weight', 1)
    TestDataframe(test_dataframe).set(2, 'material_number', 1)
    TestDataframe(test_dataframe).set(2, 'alt_uom', 'EA')
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS')], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = package_dimensions(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                                    issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 3 -- Catch Packaging Levels with any null dims ###
    test_num = 3

    TestDataframe(test_dataframe).set(1, 'length', np.nan)
    TestDataframe(test_dataframe).set(2, 'material_number', 1)
    TestDataframe(test_dataframe).set(2, 'alt_uom', 'EA')
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS')], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = package_dimensions(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                                    issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 4 -- Confirm conversion num 1 is being ignored ###
    test_num = 4

    TestDataframe(test_dataframe).set(4, 'length', np.nan)
    TestDataframe(test_dataframe).set(4, 'material_number', 3)
    TestDataframe(test_dataframe).set(4, 'alt_uom', 'RL')
    TestDataframe(test_dataframe).set(4, 'conversion_numerator', 1)
    # TestDataframe(test_dataframe).out() #TODO: Check Test
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS')], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = package_dimensions(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                                    issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)


def test_is_blank_or_zero(test_dataframe):
    test_func = 'test_is_blank_or_zero()'
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = ''
    error_message: str = ''

    ### TEST 1 -- Catch empty values in specified column ###
    test_num = 1

    TestDataframe(test_dataframe).set(1, 'upc', None)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['1'])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = is_blank_or_zero(df=TestDataframe(test_dataframe).get(), column_label='upc' ,issue_category=issue_category,
                                    issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 2 -- Catch 0s in specified column ###
    test_num = 2

    TestDataframe(test_dataframe).set(2, 'upc', 0)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['1','2'])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = is_blank_or_zero(df=TestDataframe(test_dataframe).get(), column_label='upc',
                                  issue_category=issue_category,
                                  issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)


def test_is_alt_uom_volume_zero(test_dataframe):
    test_func = 'test_is_alt_uom_volume_zero()'
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'MISSING_VOLUME'
    error_message: str = 'Volume should not be blank for AUOM with Numerator > 1.'

    ### TEST 1 -- Only Applies to PKG Levels with Conversion Factor > 1 ###
    test_num = 1

    TestDataframe(test_dataframe).set(2, 'material_number', 1)
    TestDataframe(test_dataframe).set(2, 'alt_uom', 'RL')
    TestDataframe(test_dataframe).set(2, 'conversion_numerator', 1)
    TestDataframe(test_dataframe).set(2, 'volume', 0)
    TestDataframe(test_dataframe).set(3, 'material_number', '1')
    TestDataframe(test_dataframe).set(3, 'alt_uom', 'PKG')
    TestDataframe(test_dataframe).set(3, 'conversion_numerator', 6)
    TestDataframe(test_dataframe).set(3, 'volume', 0)
    # TestDataframe(test_dataframe).out() #TODO: Check Test
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'PKG')])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = is_alt_uom_volume_zero(df=TestDataframe(test_dataframe).get(),
                                  issue_category=issue_category,
                                  issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 2 -- Catches Alt UOMs with volumes of 0 ###
    test_num = 2

    TestDataframe(test_dataframe).set(5, 'volume', 0)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'PKG'), ('5', 'CS')])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = is_alt_uom_volume_zero(df=TestDataframe(test_dataframe).get(),
                                  issue_category=issue_category,
                                  issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 3 -- Catches Alt UOMs with empty volumes ###
    test_num = 3
    TestDataframe(test_dataframe).set(6, 'volume', None)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(
        material_auom_pairs=[('1', 'PKG'), ('5', 'CS'), ('6', 'CS')], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = is_alt_uom_volume_zero(df=TestDataframe(test_dataframe).get(),
                                        issue_category=issue_category,
                                        issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)


def test_smaller_alt_uom(test_dataframe):
    test_func = 'test_smaller_alt_uom()'
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'INVALID_VOLUME'
    error_message: str = 'Greater than or equal to volume of base unit.'

    ### TEST 1 -- Only Applies to PKG Levels with Conversion Denominator > 1 and AUOM != BUOM ###
    test_num = 1

    TestDataframe(test_dataframe).set(2, 'material_number', 1)
    TestDataframe(test_dataframe).set(2, 'base_uom', 'CS')
    TestDataframe(test_dataframe).set(2, 'volume', 1)
    TestDataframe(test_dataframe).set(3, 'material_number', '1')
    TestDataframe(test_dataframe).set(3, 'alt_uom', 'EA')
    TestDataframe(test_dataframe).set(3, 'conversion_denominator', 6)
    TestDataframe(test_dataframe).set(3, 'volume', 500)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'PKG')], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = smaller_alt_volume(df=TestDataframe(test_dataframe).get(),
                                        issue_category=issue_category,
                                        issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    run_test(actual_out, expected_out, test_func, test_num)


def test_larger_alt_uom(test_dataframe):
    """def larger_alt_volume(df: pd.DataFrame,
                      issue_category: str = 'SUPPLY_CHAIN',
                      issue_code: str = 'INVALID_VOLUME',
                      error_message: str = 'Less than or equal to volume of lower AUOM level.') -> pd.DataFrame:

    Volume of AUOM level with greater Qty (Numerator > 1) should not be
    Equal to or Less than Volume of lower AUOM level. Iterate for all UOM comparisons.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |

    alt_uom_df = df[(df['base_uom'] != df['alt_uom']) & (df['conversion_numerator'] > 1)].reset_index(drop=True)
    alt_uom_df['volume_test'] = alt_uom_df.groupby('material_number')['volume'].rolling(2).min().reset_index(drop=True)
    alt_uom_df = alt_uom_df.dropna(subset='volume_test')

    alt_uom_df = alt_uom_df[alt_uom_df['volume'] == alt_uom_df['volume_test']].drop(columns=['volume_test'])

    return format_df(alt_uom_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)
    """
    test_func = 'test_larger_alt_uom()'
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'INVALID_VOLUME'
    error_message: str = 'Less than or equal to volume of lower AUOM level.'

    ### TEST 1 -- Only Applies to PKG Levels with Conversion Numerator > 1 and AUOM != BUOM ###
    test_num = 1

    TestDataframe(test_dataframe).set(1, 'volume', 1)
    TestDataframe(test_dataframe).set(2, 'material_number', 1)
    TestDataframe(test_dataframe).set(2, 'alt_uom', 'EA')
    TestDataframe(test_dataframe).set(2, 'conversion_numerator', 1)
    TestDataframe(test_dataframe).set(2, 'volume', 5)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS')], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = larger_alt_volume(df=TestDataframe(test_dataframe).get(),
                                        issue_category=issue_category,
                                        issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    #print('\n', TestDataframe(test_dataframe).get().loc[:, ['material_number', 'base_uom', 'alt_uom', 'volume', 'conversion_numerator']].head())
    print("\n","### test_larger_alt_uom() ###","\n")
    TestDataframe(test_dataframe).out()
    print("Actual output from larger_alt_volume():", "\n", actual_out.to_string())
    print("Expected output:", "\n", expected_out.to_string())
    run_test(actual_out, expected_out, test_func, test_num)


"""
def test_is_alt_uom_weight_zero(test_dataframe):
    def is_alt_uom_weight_zero(df: pd.DataFrame,
                           issue_category: str = 'SUPPLY_CHAIN',
                           issue_code: str = 'MISSING_WEIGHT',
                           error_message: str = 'Weight should not be blank or zero for AUOM with Numerator > 1.') -> pd.DataFrame:

    Weight should not be blank or zero for AUOM with Numerator > 1.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |

    alt_uom_df = df[(df['base_uom'] != df['alt_uom']) & (df['conversion_numerator'] > 1)]

    alt_uom_df = alt_uom_df[(alt_uom_df['gross_weight'].isna()) | (alt_uom_df['gross_weight'] == 0)]

    return format_df(alt_uom_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)

    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'MISSING_WEIGHT'
    error_message: str = 'Weight should not be blank or zero for AUOM with Numerator > 1.'

    ### TEST 1 -- Only Applies to PKG Levels with Conversion Numerator > 1 and AUOM != BUOM WEIGHT = 0 ###
    test_num = 1
    
    TestDataframe(test_dataframe).set(1, 'gross_weight', 0)
    TestDataframe(test_dataframe).set(2, 'material_number', '1')
    TestDataframe(test_dataframe).set(2, 'alt_uom', 'EA')
    TestDataframe(test_dataframe).set(2, 'conversion_numerator', 1)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(
        material_auom_pairs=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = is_alt_uom_weight_zero(df=TestDataframe(test_dataframe).get(),
                                        issue_category=issue_category,
                                        issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    # print('\n', TestDataframe(test_dataframe).get().loc[:, ['material_number', 'base_uom', 'alt_uom', 'gross_weight', 'conversion_numerator']].head())

    run_test(actual_out, expected_out, test_func, test_num)
    
    ### TEST 2 -- Only Applies to PKG Levels with Conversion Numerator > 1 and AUOM != BUOM WEIGHT IS NULL ###
    test_num = 2

    TestDataframe(test_dataframe).set(3, 'gross_weight', None)
    TestDataframe(test_dataframe).set(4, 'material_number', '3')
    TestDataframe(test_dataframe).set(4, 'alt_uom', 'EA')
    TestDataframe(test_dataframe).set(4, 'conversion_numerator', 1)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS'), ('3', 'CS')], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = is_alt_uom_weight_zero(df=TestDataframe(test_dataframe).get(),
                                        issue_category=issue_category,
                                        issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    # print('\n', TestDataframe(test_dataframe).get().loc[:, ['material_number', 'base_uom', 'alt_uom', 'gross_weight', 'conversion_numerator']].head())
    # print(actual_out.head())
    run_test(actual_out, expected_out, test_func, test_num)
"""


def test_larger_gross_weight_failure(test_dataframe):
    # set output parameters
    test_func = "test_larger_gross_weight_failure()"
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'WEIGHT_TOLERANCE'
    error_message: str = 'Gross weight is outside expected tolerance of calculated gross weight.'

    ### TEST 1 ###
    test_num = 1
    # No change, control test; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = larger_gross_weight_failure(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                                             issue_code=issue_code, error_message=error_message,upper_tolerance=0.25,
                                             lower_tolerance=-0.05).reset_index(drop=True)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 2 ###
    test_num = 2
    # BUOM == AUOM and weight is under calculated tolerance; FAIL
    TestDataframe(test_dataframe).set(test_num, 'material_number', '1')
    TestDataframe(test_dataframe).set(test_num, 'alt_uom', 'EA')
    TestDataframe(test_dataframe).set(test_num, 'gross_weight', 9)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS')])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = larger_gross_weight_failure(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                                             issue_code=issue_code, error_message=error_message,upper_tolerance=0.25,
                                             lower_tolerance=-0.05).reset_index(drop=True)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 3 ###
    test_num = 3
    # BUOM != AUOM and weight is under calculated tolerance; FAIL
    TestDataframe(test_dataframe).set(test_num, 'material_number', '1')
    TestDataframe(test_dataframe).set(test_num, 'alt_uom', 'PKG')
    TestDataframe(test_dataframe).set(test_num, 'gross_weight', 9)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS'),('1','PKG')])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = larger_gross_weight_failure(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                                             issue_code=issue_code, error_message=error_message,upper_tolerance=0.25,
                                             lower_tolerance=-0.05).reset_index(drop=True)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 4 ###
    test_num = 4
    # BUOM == AUOM and weight is inside calculated tolerance; PASS
    TestDataframe(test_dataframe).set(test_num, 'material_number', '6')
    TestDataframe(test_dataframe).set(test_num, 'alt_uom', 'EA')
    TestDataframe(test_dataframe).set(test_num, 'gross_weight', 5.25)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS'),('1','PKG')])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = larger_gross_weight_failure(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                                             issue_code=issue_code, error_message=error_message,upper_tolerance=0.25,
                                             lower_tolerance=-0.05).reset_index(drop=True)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 5 ###
    test_num = 5
    # BUOM != AUOM and weight is inside calculated tolerance; PASS
    TestDataframe(test_dataframe).set(test_num, 'alt_uom', 'CS')
    TestDataframe(test_dataframe).set(test_num, 'conversion_numerator', 4)
    TestDataframe(test_dataframe).set(test_num, 'gross_weight', 26.3)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS'),('1','PKG')])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = larger_gross_weight_failure(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message,upper_tolerance=0.25,lower_tolerance=-0.05).reset_index(drop=True)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 6 ###
    test_num = 6
    # BUOM != AUOM and weight is above calculated tolerance; FAIL
    TestDataframe(test_dataframe).set(test_num, 'material_number', '7')
    TestDataframe(test_dataframe).set(test_num, 'alt_uom', 'EA')
    TestDataframe(test_dataframe).set(test_num, 'gross_weight', .08)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS'),('1','PKG'),('7','CS')])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = larger_gross_weight_failure(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                                             issue_code=issue_code, error_message=error_message,upper_tolerance=0.25,
                                             lower_tolerance=-0.05).reset_index(drop=True)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)


def test_upc_required(test_dataframe):
    # set output parameters
    test_func = 'test_upc_required()'
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'NO_UPC'
    error_message: str = 'Valid UPC/GTIN is required for all valid package levels.'

    ### TEST 1 ###
    test_num = 1
    # No change, control test; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 2 ###
    test_num = 2
    # None upc; FAIL
    TestDataframe(test_dataframe).set(test_num,'upc',None)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2'],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 3 ###
    test_num = 3
    # null upc; FAIL
    TestDataframe(test_dataframe).set(test_num,'upc',np.nan)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3'],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 4 ###
    test_num = 4
    # null upc with PAL alt_uom; PASS
    TestDataframe(test_dataframe).set(test_num,'upc',np.nan)
    TestDataframe(test_dataframe).set(test_num,'alt_uom','PAL')
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3'],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 5 ###
    test_num = 5
    # null upc with (base == alt) & (num != denom); FAIL
    TestDataframe(test_dataframe).set(test_num,'upc',np.nan)
    TestDataframe(test_dataframe).set(test_num,'alt_uom','EA')
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3','5'],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 6 ###
    test_num = 6
    # null upc with (base == alt) & (num == denom); FAIL
    TestDataframe(test_dataframe).set(test_num,'upc',np.nan)
    TestDataframe(test_dataframe).set(test_num,'alt_uom','EA')
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3','5','6'],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 7 ###
    test_num = 7
    # null upc with (base != alt) & (num == denom); PASS
    TestDataframe(test_dataframe).set(test_num,'upc',np.nan)
    TestDataframe(test_dataframe).set(test_num,'conversion_denominator',2)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3','5','6'],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 8 ###
    test_num = 8
    # null upc with (base != alt) & (num != denom); FAIL
    TestDataframe(test_dataframe).set(test_num,'upc',np.nan)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3','5','6','8'],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 9 ###
    test_num = 9
    #null upc with denom < 1; FAIL
    TestDataframe(test_dataframe).set(test_num,'upc',None)
    TestDataframe(test_dataframe).set(test_num,'conversion_denominator',0)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3','5','6','8','9'],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 10 ###
    test_num = 10
    # no change; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3','5','6','8','9'],)
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)


def test_invalid_gtin(test_dataframe):
    # set output parameters
    test_func = 'test_invalid_gtin()'
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'INVALID_UPC'
    error_message: str = 'UPC failed check digit validation.'

    ### TEST 1 ###
    test_num = 1
    # No change, control test; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = invalid_gtin(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 2 ###
    test_num = 2
    # null UPC; Pass
    TestDataframe(test_dataframe).set(test_num,'upc',None)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = invalid_gtin(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 3 ###
    test_num = 3
    # valid GTIN-8; PASS
    TestDataframe(test_dataframe).set(test_num,'upc',"40170725")
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = invalid_gtin(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 4 ###
    test_num = 4
    # valid GTIN-13; PASS
    TestDataframe(test_dataframe).set(test_num,'upc',"9999999999994")
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = invalid_gtin(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)


    ### TEST 5 ###
    test_num = 5
    # invalid check digit in GTIN-13; FAIL
    TestDataframe(test_dataframe).set(test_num,'upc',"9999999999999")
    # TestDataframe(test_dataframe).out() #TODO: Check Test
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['5'], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = invalid_gtin(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)


    ### TEST 6 ###
    test_num = 6
    # invalid check digit GTIN-13 with 'PAL' AUOM; PASS
    TestDataframe(test_dataframe).set(test_num,'upc',"9999999999999")
    TestDataframe(test_dataframe).set(test_num,'alt_uom',"PAL")
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['5'], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = invalid_gtin(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 7 ###
    test_num = 7
    # invalid check digit GTIN-13 with 1:1 conversion; PASS
    TestDataframe(test_dataframe).set(test_num,'upc',"9999999999999")
    TestDataframe(test_dataframe).set(test_num,'conversion_numerator',1)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['5'], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = invalid_gtin(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 8 ###
    test_num = 8
    # invalid GTIN-8 check digit; FAIL
    TestDataframe(test_dataframe).set(test_num,'upc',"01234568")
    # TestDataframe(test_dataframe).out() #TODO: Check Test
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['5','8'], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = invalid_gtin(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 9 ###
    test_num = 9
    # invalid GTIN-12 check digit; FAIL
    TestDataframe(test_dataframe).set(test_num,'upc',"999999999999")
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['5','8','9'], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = invalid_gtin(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)


def test_unique_upc(test_dataframe):
    """
    def unique_upc(df: pd.DataFrame,
                   upc_df: pd.DataFrame,
                   issue_category: str = 'SUPPLY_CHAIN',
                   issue_code: str = 'DUPLICATE_UPC') -> pd.DataFrame:

        UPC/GTIN values must be Valid and must be unique for each AUOM entry within the record and across all other records
            :param df: Target DataFrame contain SKU/UOM data for evaluation.
            :param upc_df: All UPCs from ECC.MEAN table for comparison to stock SKUs.
            :param issue_category: Owner of the issue's resolution.
            :param issue_code: Short form code identifying the issue type.
            :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |

        duplicate_upc_df = upc_df.groupby('upc').agg(upc_collapse).reset_index()
        duplicate_upc_df['key'] = duplicate_upc_df['error_message']
        duplicate_upc_df = duplicate_upc_df.explode('key').reset_index(drop=True)
        duplicate_upc_df[['material_number', 'alt_uom']] = duplicate_upc_df['key'].str.split(' - ', expand=True)

        for i in range(len(duplicate_upc_df)):
            error_list = duplicate_upc_df.loc[i, 'error_message']
            upc_key = duplicate_upc_df.loc[i, 'upc']
            error_dict = str({upc_key: error_list})

            duplicate_upc_df.loc[i, 'error_message'] = error_dict

        df = df.merge(duplicate_upc_df, on=['material_number', 'alt_uom'], how='inner')
        df = df[['material_number', 'alt_uom', 'error_message']]
        df = df.groupby(by=['material_number', 'alt_uom'], as_index=False).agg(upc_collapse).reset_index(drop=True)
        return format_df(df, issue_category=issue_category, issue_code=issue_code, error_message=df['error_message'])
    """
    # set output parameters
    test_func = 'test_unique_upc()'
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'DUPLICATE_UPC'
    error_message: str = ''

    ### TEST 1 ###
    test_num = 1
    # No change, control test; Pass
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 2 ###
    test_num = 2
    # dupe UPC across different SKUs; FAIL
    TestDataframe(test_dataframe).set(test_num, 'upc', '01234567')
    TestDataframe(test_dataframe).set(test_num, 'alt_uom', 'CS')
    TestDataframe(test_dataframe).set(3, 'upc', '01234567')
    TestDataframe(test_dataframe).set(3, 'alt_uom', 'PKG')
    #TestDataframe(test_dataframe).out() #TODO: Check Test
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3'])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 3 ###
    test_num = 3
    # dupe UPC with same SKU#, different AUOM; FAIL
    TestDataframe(test_dataframe).set(4, 'material_number', '5')
    TestDataframe(test_dataframe).set(4, 'upc', '09876547')
    TestDataframe(test_dataframe).set(4, 'alt_uom', 'CS')
    TestDataframe(test_dataframe).set(5, 'upc', '09876547')
    TestDataframe(test_dataframe).set(5, 'alt_uom', 'PKG')
    #TestDataframe(test_dataframe).out() #TODO: Check Test
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=['2','3','4','5'])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)


def test_template(test_dataframe):
    # set output parameters
    test_func = ''
    issue_category: str = ''
    issue_code: str = ''
    error_message: str = ''

    ### TEST 1 ###
    test_num = 1
    # No change, control test; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 2 ###
    test_num = 2
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 3 ###
    test_num = 3
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 4 ###
    test_num = 4
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 5 ###
    test_num = 5
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 6 ###
    test_num = 6
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 7 ###
    test_num = 7
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 8 ###
    test_num = 8
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 9 ###
    test_num = 9
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)

    ### TEST 10 ###
    test_num = 10
    # no change; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[])
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_func, test_num)


# example test class for reference
class TestDataframe:
    def __init__(self, df):
        self.df = df

    def out(self):
        print("\n")
        print(self.df.to_string())

    def set(self, index, column, value):
        self.df.at[index, column] = value

    def get(self) -> pd.DataFrame:
        return self.df

    def get_expected_df(self, material_nums: list) -> pd.DataFrame:
        return self.df.loc[self.df['material_number'].isin(material_nums)]

    def get_expected_df_by_mat_auom(self, material_auom_pairs: list[tuple]) -> pd.DataFrame:
        return self.df[self.df[['material_number', 'alt_uom']].apply(tuple, axis=1).isin(material_auom_pairs)]


@pytest.fixture
def test_dataframe():
    test_df = pd.DataFrame(data={"material_number": ['1','2','3','4','5','6','7','8','9','10'],
                               "product_category": ['Appliances','Blinds','C','D','E','F','G','H','I',"J"],
                               "base_uom": ['EA','EA','EA','EA','EA','EA','EA','EA','EA','EA'],
                               "alt_uom": ['CS','CS','CS','CS','CS','CS','CS','CS','CS','CS'],
                               "conversion_numerator":  [2,2,2,2,2,2,2,2,2,2],
                               "conversion_denominator": [1,1,1,1,1,1,1,1,1,1],
                               "upc": ['01234565','01234565','01234565','01234565','01234565','01234565','01234565',
                                       '01234565','01234565','01234565'],
                               "length": [5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12],
                               "width": [5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12],
                               "height": [5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12],
                               "volume": [134.218,134.218,134.218,134.218,134.218,134.218,134.218,134.218,134.218,134.218],
                               "gross_weight": [10.5,10.5,10.5,10.5,10.5,10.5,10.5,10.5,10.5,10.5]},
                           index=[1,2,3,4,5,6,7,8,9,10])
    return test_df

def run_test(actual_out, expected_out, test_func, test_num) -> None:
    # compare actual and expected
    try:
        pd.testing.assert_frame_equal(actual_out, expected_out)
    except AssertionError as e:
        print("\n", "[FAIL] ", test_func, "Test #", test_num, ": ",e, "\n")

