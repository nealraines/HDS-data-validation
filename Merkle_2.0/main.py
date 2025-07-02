import os

import pandas as pd
import pyodbc as odbc

from queries import material_data
from stored_procedures import *

def main():
    with odbc.connect(os.environ.get('Snowflake_Connection_String')) as con:
        material_df = pd.read_sql_query(sql=material_data, con=con, dtype={'material_number': 'string',
                                                                           'product_category': 'string',
                                                                           'base_uom': 'string',
                                                                           'alt_uom': 'string',
                                                                           'conversion_numerator': 'int64',
                                                                           'conversion_denominator': 'int64',
                                                                           'upc': 'string',
                                                                           'length': 'float64',
                                                                           'width': 'float64',
                                                                           'height': 'float64',
                                                                           'volume': 'float64',
                                                                           'gross_weight': 'float64'})

    response_array: list = []

    response_array.append(package_dimensions(material_df))
    response_array.append(is_blank_or_zero(material_df, 'conversion_numerator', 'BLANK_NUM', 'Numerator cannot be blank or zero.'))
    response_array.append(is_blank_or_zero(material_df, 'conversion_denominator', 'BLANK_DENOM', 'Denominator cannot be blank or zero.'))
    response_array.append(is_alt_uom_volume_zero(material_df))
    response_array.append(smaller_alt_volume(material_df))
    response_array.append(larger_alt_volume(material_df))
    response_array.append(is_alt_uom_weight_zero(material_df))
    response_array.append(missing_alternate_uom(material_df))
    response_array.append(invalid_numerator(material_df))
    response_array.append(duplicate_alt_uoms(material_df))
    response_array.append(alt_uom_mod(material_df))
    response_array.append(inv_conv_by_upc(material_df))
    response_array.append(redundant_conversion(material_df))
    response_array.append(pallet_case_fault_tolerance(material_df))
    response_array.append(smaller_gross_weight_failure(material_df))
    response_array.append(larger_gross_weight_failure(material_df))
    response_array.append(invalid_gtin(material_df))
    response_array.append(upc_required(material_df))
    response_array.append(package_dimensions(material_df))
    response_array.append(unique_upc(material_df))

    error_df = pd.concat(response_array)
    error_df.to_csv('error_output.csv', index=False)


if __name__ == '__main__':
    main()
