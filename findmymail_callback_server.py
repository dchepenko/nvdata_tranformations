import asyncio
from aiohttp import web
import json
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial

async def handle_post(request):
    try:
        post_data = await request.json()
    except json.JSONDecodeError as e:
        return web.json_response({'error': 'Invalid JSON'}, status=400)

    contact_info = post_data.get('payload', {}).get('contact', {})
    email, domain, full_name = extract_contact_info(contact_info)
    await save_to_bigquery(email, domain, full_name)
    return web.json_response({'status': 'success'})

def extract_contact_info(contact_info):
    email = contact_info.get('email', 'unknown')
    domain = contact_info.get('domain', 'unknown')
    full_name = contact_info.get('name', 'unknown')
    return email, domain, full_name

async def save_to_bigquery(email, domain, full_name):
    project_id = 'new-ventures-427409'
    dataset_id = '000_src'
    table_id = 'raw_findymail_callback'

    data = {
        'email': email,
        'domain': domain,
        'full_name': full_name,
        '_ingestion_time': pd.to_datetime('now'),
        '_ingestion_id': str(uuid.uuid4())
    }

    df = pd.DataFrame([data])

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        to_csv_partial = partial(df.to_csv, 'output_email_findymail.csv', index=False, mode='a', header=False)
        await loop.run_in_executor(pool, to_csv_partial)


async def init_app():
    app = web.Application()
    app.router.add_post('/', handle_post)
    return app

if __name__ == '__main__':
    web.run_app(init_app(), port=5000)