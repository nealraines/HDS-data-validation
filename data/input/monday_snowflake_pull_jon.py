# -*- coding: UTF-8 -*-
# Author: Jon Wheeler
# Modified By: Neal Raines
# Date: 6/9/2025
# Description: Pulls data from Monday.com into Snowflake

from datetime import date
import pandas as pd
import os
import requests
import json
import snowflake.connector
import snowflake
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime


def run_json_query(query2: str):
    """ Makes post request with given headers and query to given api address

    Args:
        query2 (str): JSON structured string of query

    Returns:
        (requests.Response Object): response object from post request
        """
    # API key:
    api_key = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjQ3MzgxNDg4NywiYWFpIjoxMSwidWlkIjozNTUxMzQ1MywiaWFkIjoiMjAyNS0wMi0xN1QyMDo0NDoyOC4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6OTM1Mzk1MywicmduIjoidXNlMSJ9.GREM3F1cLzck1rhK1zwNo0a91pwKiYw7OAOhhPrRKfA"

    # API url:
    api_url = "https://api.monday.com/v2"

    # Request headers:
    headers = {"Authorization": api_key, "API-Version": "2025-04"}

    query_data = {'query': query2}  # format query into dict
    return requests.post(url=api_url, json=query_data, headers=headers)  # make request


def extract_items(json_data):
    """ Extracts all items out of Json_data dictionary

    Args:
        json_data (dict): JSON object based on API request

    Returns:
        all_items (list): list of all items from json_data dictionary
    """
    items = json_data['data']['boards'][0]['items_page']['items']
    cursor = json_data['data']['boards'][0]['items_page']['cursor']

    all_items = items

    while cursor:
        query = f'''{{
        boards(ids: 8283305838) {{
            items_page(cursor: "{cursor}") {{
                cursor
                items {{
                    group {{
                        id
                        title
                    }}
                    id
                    name
                    column_values {{
                        id
                        text
                        type
                    }}
                }}
            }}
        }}
    }}'''

        json_data = run_json_query(query).json()
        items = json_data['data']['boards'][0]['items_page']['items']
        cursor = json_data['data']['boards'][0]['items_page']['cursor']
        all_items.extend(items)
        print(cursor)

    return all_items


def create_df(all_items):
    """ Creates pandas dataframe containing data from all items in all_items

    Args:
        all_items (list): list of all items from json_data dictionary:

    Returns:
        extracted_data (pandas dataframe): pandas dataframe containing data from all items in all_items
    """
    extracted_data = []

    # Define Columns Here
    for row in all_items:
        item = {'GROUP': row['group']['title'], 'VENDOR_NAME': row['name']}
        for col in row['column_values']:
            item[col['id']] = col['text']
        extracted_data.append(item)

    return extracted_data


def modify_df(df):
    """ Modify dataframe by renaming columns, dropping subitems, and adjusting report date format

    Args:
        df (pandas dataframe): dataframe containing extracted JSON items

    Returns:
        df (pandas dataframe): modified dataframe
    """
    column_renames = {'GROUP':'WORK_GROUP',
    'VENDOR_NAME':'VENDOR_NAME',
    'person':'ASSIGNED_TO',
    'text_Mjj6KQah':'VENDOR_ID',
    'text_mkmvk6g3':'THIRD_PARTY',
    'label_Mjj6pRZM':'WEEK_INITIATED',
    'date_Mjj6uUuy':'DATE_INITIATED',
    'merchant__s__mkmcsk0p':'MERCHANTS',
    'numeric_mkr4b8k5':'TOTAL_GSAR_SKUS_TO_COLLECT',
    'color_mkr4zfk':'LB_STATUS',
    'date_Mjj6hGWn':'SCHEDULED_WEBINAR_DATE',
    'formula_mkmaaaes':'WEBINAR_DUE_DATE',
    'status_mkm76fpp':'OVERALL_STATUS',
    'label_Mjj6o6Dr':'WEBINAR_STATUS',
    'date_mkm7xf43':'ATTENDED_WEBINAR_DATE',
    'status_mkm7ey2t':'SUPPORT_CALL_NEEDED',
    'date_mkn3wpx5':'SUPPORT_CALL_DATE',
    'formula_Mjj6xxZq':'WORKFLOW_DUE_DATE',
    'status_mkmpn099':'WORKFLOW_STATUS',
    'status_mkmp2rsg':'COMMUNICATION_STATUS',
    'date_mkqrxf0e':'DATE_OF_LAST_CONTACT',
    'status_mkmr45b5':'DATA_UPDATED',
    'progress_mkmrbg9g':'PROGRESS',
    'text_mkm73rje':'DATA_CONTACTS'}

    df = df.drop(labels='subitems_Mjj6XJ8d', axis=1)

    df.rename(columns=column_renames, inplace=True)
    df['REPORT_DATE'] = datetime.today().strftime('%Y-%m-%d')

    return df


def write_to_snowflake(df):
    """ Connect to snowflake database and write df

    Args:
        df (pandas dataframe): pandas dataframe containing extracted JSON items
    """
    con = snowflake.connector.connect(
        user="neal.raines@hdsupply.com",  # You can get it by executing in UI: desc user <username>;
        account="data.us-central1.gcp",  # Add all of the account-name between https:// and snowflakecomputing.com in URL
        authenticator="externalbrowser",
        warehouse='DATA_GOVERNANCE_WH1',
        database='DM_DATA_GOVERNANCE',
        schema='EWM_ANALYTICS'
    )

    cur = con.cursor()

    # truncate_table_query = 'Truncate Table DM_DATA_GOVERNANCE.EWM_ANALYTICS.MONDAY_SC_CERTIFICATION_TRACKER'
    # cur.execute(truncate_table_query)
    # print("Old Data Dropped")

    # write df to Snowflake table
    success, nchunks, nrows, _ = write_pandas(con, df, 'MONDAY_SC_CERTIFICATION_TRACKER')

    print(f"Success: {success}, Number of chunks: {nchunks}, Number of rows: {nrows}")

    cur.close()
    con.close()


def main():
    # Desired query
    query2 = '''{
        boards(ids: 8283305838) {
            items_page {
                cursor
                items {
                    group {
                        id
                        title
                }
                    id
                    name
                    column_values {
                        id
                        text
                        type
                    }
                }
            }
        }
    }'''

    # Capture post request query response
    json_data = run_json_query(query2).json()

    # View the post request response
    # print(json_data)

    # Extract all Data
    extracted_data = extract_items(json_data)

    # Create DataFrame using post request response
    df = pd.DataFrame(create_df(extracted_data))

    # Modify DataFrame
    mod_df = modify_df(df)

    # Write Dataframe to excel
    # mod_df.to_excel('MondayData.xlsx')

    # Write Dataframe to Snowflake
    write_to_snowflake(mod_df)


if __name__ == "__main__":
    main()
