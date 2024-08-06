from google.cloud import bigquery
import requests
import uuid
import pandas_gbq
from tqdm import tqdm
import pandas as pd

def is_website_available(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=3)
        return response.status_code
    except requests.RequestException:
        return None

def check_websites_in_bigquery(input_project_id, input_dataset_id, input_table_id, output_project_id, output_dataset_id, output_table_id):
    input_client = bigquery.Client(project=input_project_id)
    
    query = f"SELECT * FROM `{input_project_id}.{input_dataset_id}.{input_table_id}`"
    df = input_client.query(query).to_dataframe()

    tqdm.pandas(desc="Checking website availability")
    df['status_code'] = df['website'].progress_apply(lambda url: is_website_available(url) if pd.notnull(url) else None)
    df['_ingestion_time'] = pd.to_datetime('now')
    df['_ingestion_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

    pandas_gbq.to_gbq(
        df,
        destination_table=f'{output_dataset_id}.{output_table_id}',
        project_id=output_project_id,
        if_exists='replace'
    )

# Example usage
project_id = 'new-ventures-427409'
in_dataset_id = '010_dm'
out_dataset_id = '011_dm'
in_table_id= 'specter_org'
out_table_id= 'specter_org'

# check_websites_in_bigquery(project_id, in_dataset_id, in_table_id, project_id, out_dataset_id, out_table_id)


project_id = 'new-ventures-427409'
in_dataset_id = '010_dm'
out_dataset_id = '011_dm'
in_table_id= 'crunchbase'
out_table_id= 'crunchbase'

check_websites_in_bigquery(project_id, in_dataset_id, in_table_id, project_id, out_dataset_id, out_table_id)
