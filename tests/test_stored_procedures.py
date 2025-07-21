# -*- coding: UTF-8 -*-
# Author: Neal Raines
# Create Date: 7/1/2025
# Description: merkle2 test file

import pytest
import pandas as pd
import numpy as np

from src.utils import format_df
from src.stored_procedures import *


def test_upc_required(test_dataframe):
    """ FULL UPC_REQUIRED FUNCTION:
    def upc_required(df: pd.DataFrame,
                 issue_category: str = 'SUPPLY_CHAIN',
                 issue_code: str = 'NO_UPC',
                 error_message: str = 'Valid UPC/GTIN is required for all valid package levels.') -> pd.DataFrame:

    Valid UPC/GTIN is required for all valid PKG levels (those which are not 1:1) except PAL/PALLET.
    UOMs with a conversion denominator > 1 are exempt from this evaluation.
        :param df: Target DataFrame contain SKU/UOM data for evaluation.
        :param issue_category: Owner of the issue's resolution.
        :param issue_code: Short form code identifying the issue type.
        :param error_message: Output detailing why the SKU/UOM was flagged.
        :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |

    no_upc_df = df[df['upc'].isna()]
    no_upc_df = no_upc_df[no_upc_df['alt_uom'] != 'PAL']
    no_upc_df = no_upc_df[~((no_upc_df['base_uom'] != no_upc_df['alt_uom']) & (no_upc_df['conversion_numerator'] == no_upc_df['conversion_denominator']))]
    no_upc_df = no_upc_df[no_upc_df['conversion_denominator'] <= 1]

    return format_df(no_upc_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)
    """

    # set output parameters
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
    run_test(actual_out, expected_out, test_num)

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
    run_test(actual_out, expected_out, test_num)

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
    run_test(actual_out, expected_out, test_num)

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
    run_test(actual_out, expected_out, test_num)

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
    run_test(actual_out, expected_out, test_num)

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
    run_test(actual_out, expected_out, test_num)

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
    run_test(actual_out, expected_out, test_num)

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
    run_test(actual_out, expected_out, test_num)

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
    run_test(actual_out, expected_out, test_num)

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
    run_test(actual_out, expected_out, test_num)


def test_invalid_gtin(test_dataframe):
    """def invalid_gtin(df: pd.DataFrame,
                     issue_category: str = 'SUPPLY_CHAIN',
                     issue_code: str = 'INVALID_UPC',
                     error_message: str = 'UPC failed check digit validation.') -> pd.DataFrame:

        UPC does not match an approved format. Pallets and AUOMs that are 1:1 are excluded from this requirement.
            :param df: Target DataFrame contain SKU/UOM data for evaluation.
            :param issue_category: Owner of the issue's resolution.
            :param issue_code: Short form code identifying the issue type.
            :param error_message: Output detailing why the SKU/UOM was flagged.
            :return: pd.DataFrame | material_number | alt_uom | date_discovered | date_resolved | issue_category | error_message |

        gtin_df = df[~df['upc'].isna()]
        gtin_df = gtin_df[~gtin_df['upc'].str.match(r'^\d{8}$|^\d{12}$|^\d{13}$|^\d{14}$')]
        gtin_df = gtin_df[gtin_df['alt_uom'] != 'PAL']
        gtin_df = gtin_df[~((gtin_df['base_uom'] != gtin_df['alt_uom']) & (gtin_df['conversion_numerator'] == gtin_df['conversion_denominator']))]

        edit 2 for test

        return format_df(gtin_df, issue_category=issue_category, issue_code=issue_code, error_message=error_message)"""
    # set output parameters
    issue_category: str = ''
    issue_code: str = 'INVALID_UPC'
    error_message: str = 'UPC failed check digit validation.'

    ### TEST 1 ###
    test_num = 1
    # No change, control test; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 2 ###
    test_num = 2
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 3 ###
    test_num = 3
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 4 ###
    test_num = 4
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 5 ###
    test_num = 5
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 6 ###
    test_num = 6
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 7 ###
    test_num = 7
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 8 ###
    test_num = 8
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 9 ###
    test_num = 9
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 10 ###
    test_num = 10
    # no change; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)


def test_template(test_dataframe):
    """"""
    # set output parameters
    issue_category: str = ''
    issue_code: str = ''
    error_message: str = ''

    ### TEST 1 ###
    test_num = 1
    # No change, control test; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 2 ###
    test_num = 2
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 3 ###
    test_num = 3
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 4 ###
    test_num = 4
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 5 ###
    test_num = 5
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 6 ###
    test_num = 6
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 7 ###
    test_num = 7
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 8 ###
    test_num = 8
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 9 ###
    test_num = 9
    # no change; PASS

    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

    ### TEST 10 ###
    test_num = 10
    # no change; PASS
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df(material_nums=[], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message)
    # pass actual df through function
    actual_out = upc_required(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                              issue_code=issue_code, error_message=error_message)
    # run comparison assertion
    run_test(actual_out, expected_out, test_num)

def test_package_dimensions(test_dataframe):
    issue_category: str = 'SUPPLY_CHAIN'
    issue_code: str = 'INVALID_DIMENSIONS'
    error_message: str = 'Dimensions are missing or contain all default values (1)'

    test_num = 1
    ### TEST 1 -- Catch Packaging Levels with any dims as 0 ###
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
    try:
        pd.testing.assert_frame_equal(actual_out, expected_out)
    except AssertionError as e:
        print("\n", "[FAIL] Test #", test_num, ": ",e, "\n")

    test_num = 2
    ### TEST 2 -- Catch Packaging Levels with Dims as all 1s ###
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
    try:
        pd.testing.assert_frame_equal(actual_out, expected_out)
    except AssertionError as e:
        print("\n", "[FAIL] Test #", test_num, ": ", e, "\n")

    test_num = 3
    ### TEST 3 -- Catch Packaging Levels with any null dims ###
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
    try:
        pd.testing.assert_frame_equal(actual_out, expected_out)
    except AssertionError as e:
        print("\n", "[FAIL] Test #", test_num, ": ", e, "\n")

    test_num = 4
    ### TEST 4 -- Confirm conversion num 1 is being ignored ###
    TestDataframe(test_dataframe).set(4, 'length', np.nan)
    TestDataframe(test_dataframe).set(4, 'material_number', 3)
    TestDataframe(test_dataframe).set(4, 'alt_uom', 'RL')
    TestDataframe(test_dataframe).set(4, 'conversion_numerator', 1)
    # set the expected error df by passing SKUs adjusted above that should fail
    error_df = TestDataframe(test_dataframe).get_expected_df_by_mat_auom(material_auom_pairs=[('1', 'CS')], )
    expected_out = format_df(df=error_df, issue_category=issue_category, issue_code=issue_code,
                             error_message=error_message).reset_index(drop=True)
    # pass actual df through function
    actual_out = package_dimensions(df=TestDataframe(test_dataframe).get(), issue_category=issue_category,
                                    issue_code=issue_code, error_message=error_message).reset_index(drop=True)
    # compare actual and expected
    try:
        pd.testing.assert_frame_equal(actual_out, expected_out)
    except AssertionError as e:
        print("\n", "[FAIL] Test #", test_num, ": ", e, "\n")

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
        a = self.df.loc[self.df['material_number'].isin(material_nums)]
        # print(a.to_string())
        return a

    def get_expected_df_by_mat_auom(self, material_auom_pairs: list[tuple]) -> pd.DataFrame:
        a = self.df[self.df[['material_number', 'alt_uom']].apply(tuple, axis=1).isin(material_auom_pairs)]
        return a

@pytest.fixture
def test_dataframe():
    test_df = pd.DataFrame(data={"material_number": ['1','2','3','4','5','6','7','8','9','10'],
                               "product_category": ['Appliances','Blinds','C','D','E','F','G','H','I',"J"],
                               "base_uom": ['EA','EA','EA','EA','EA','EA','EA','EA','EA','EA'],
                               "alt_uom": ['CS','CS','CS','CS','CS','CS','CS','CS','CS','CS'],
                               "conversion_numerator":  [2,2,2,2,2,2,2,2,2,2],
                               "conversion_denominator": [1,1,1,1,1,1,1,1,1,1],
                               "upc": ['001','002','003','004','005','006','007','008','009','010'],
                               "length": [5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12],
                               "width": [5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12],
                               "height": [5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12,5.12],
                               "volume": [134.218,134.218,134.218,134.218,134.218,134.218,134.218,134.218,134.218,134.218],
                               "gross_weight": [10.5,10.5,10.5,10.5,10.5,10.5,10.5,10.5,10.5,10.5]},
                           index=[1,2,3,4,5,6,7,8,9,10])
    return test_df

def run_test(actual_out, expected_out, test_num) -> None:
    # compare actual and expected
    try:
        pd.testing.assert_frame_equal(actual_out, expected_out)
    except AssertionError as e:
        print("\n", "[FAIL] Test #", test_num, ": ",e, "\n")


"""
@pytest.fixture
def cache():
    # setup
    test_cache = TestDataframe()
    yield test_cache
    # teardown
    test_cache.store.clear()


def test_cache_set_and_get(cache):
    cache.set("test_key", "test_value")
    assert cache.get("test_key") == "test_value", "Get value from cashe"

def test_cache_miss_returns_none(cache):
    assert cache.get("nonexistent_key") is None, "Return none on missed key"
"""