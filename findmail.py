from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import time
import requests
import uuid
import queue
import threading


def worker(q, results):
    while not q.empty():
        full_name, domain = q.get()
        email_result = find_email(full_name, domain)
        results.append({
            'founder_full_name': full_name,
            'domain': domain,
            'email_result': email_result.get('status', None)
        })
        # To avoid hitting rate limits
        time.sleep(1)
        q.task_done()

def find_email(name, domain):
    url = "https://app.findymail.com/api/search/name"
    headers = {
        "Authorization": "Bearer R4jGWOewN5JG9rqoS8LB4E3N0yvrmzqy9iWzely893b2fb53",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    data = {
        "name": name,
        "domain": domain,
        "webhook_url": "https://45b5-2804-7c78-56-ec00-d41-2bad-597-5bf6.ngrok-free.app"
    }

    response = requests.post(url, headers=headers, json=data)
    return response.json()

def process_bigquery_and_find_email(input_project_id, input_dataset_id, input_table_id, output_project_id, output_dataset_id, output_table_id):
    client = bigquery.Client(project=input_project_id)
    
    query = f"SELECT founder_full_name, domain FROM `{input_project_id}.{input_dataset_id}.{input_table_id}` where findmymail_ingestion_id is null"
    df = client.query(query).to_dataframe()

    q = queue.Queue()
    results = []

    for index, row in df.iterrows():
        q.put((row['founder_full_name'], row['domain']))

    num_threads = 5  # Adjust the number of threads as needed
    threads = []

    for _ in range(num_threads):
        t = threading.Thread(target=worker, args=(q, results))
        t.start()
        threads.append(t)

    q.join()

    for t in threads:
        t.join()

    results_df = pd.DataFrame(results)
    results_df['_ingestion_time'] = pd.to_datetime('now')
    results_df['_ingestion_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

    pandas_gbq.to_gbq(
        results_df,
        destination_table=f'{output_dataset_id}.{output_table_id}',
        project_id=output_project_id,
        if_exists='append'
    )

# Example usage
input_project_id = 'new-ventures-427409'
input_dataset_id = '010_dm'
input_table_id = 'founders'
output_project_id = 'new-ventures-427409'
output_dataset_id = '000_src'
output_table_id = 'raw_findymail_run'

process_bigquery_and_find_email(input_project_id, input_dataset_id, input_table_id, output_project_id, output_dataset_id, output_table_id)