from datetime import date
import pandas as pd
import os
import requests
import json
import snowflake.connector
import snowflake
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime

# Enter our API key we will use for access:
apiKey = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjQ3MzgxNDg4NywiYWFpIjoxMSwidWlkIjozNTUxMzQ1MywiaWFkIjoiMjAyNS0wMi0xN1QyMDo0NDoyOC4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6OTM1Mzk1MywicmduIjoidXNlMSJ9.GREM3F1cLzck1rhK1zwNo0a91pwKiYw7OAOhhPrRKfA"

#Enter the API url:
apiUrl = "https://api.monday.com/v2"

# Enter the headers:
headers = {"Authorization" : apiKey, "API-Version" : "2025-04"}


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

data = {'query' : query2}
# Run the desired query:
r = requests.post(url=apiUrl, json=data, headers=headers) # make request

# Set the result dictionary to a variable:
json_data = r.json()

# View the response
# pprint(json_data)


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
    data = {'query' : query}

    r = requests.post(url=apiUrl, json=data, headers=headers)
    json_data = r.json()
    items = json_data['data']['boards'][0]['items_page']['items']
    cursor = json_data['data']['boards'][0]['items_page']['cursor']
    all_items.extend(items)
    print(cursor)

extracted_data = []

#Define Columns Here
for row in all_items:
    item = {'GROUP': row['group']['title'], 'VENDOR_NAME': row['name']}
    for col in row['column_values']:
        item[col['id']] = col['text']
    extracted_data.append(item)

# Create DataFrame
df = pd.DataFrame(extracted_data)

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

df = df.drop('subitems_Mjj6XJ8d', axis=1)

df.rename(columns=column_renames, inplace=True)
df['REPORT_DATE'] = datetime.today().strftime('%Y-%m-%d')

#df.to_excel('MondayData.xlsx')

con = snowflake.connector.connect(
    user="jonathan.wheeler@hdsupply.com",  # You can get it by executing in UI: desc user <username>;
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

success, nchunks, nrows, _ = write_pandas(con, df, 'MONDAY_SC_CERTIFICATION_TRACKER')

print(f"Success: {success}, Number of chunks: {nchunks}, Number of rows: {nrows}")

cur.close()
con.close()


