from google.cloud import bigquery

def get_table_schema(client, project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = client.get_table(table_ref)
    return  table.schema
    return set(field.name for field in table.schema)

def compare_schemas(schema1, schema2):
    missing_in_schema1 = schema2 - schema1
    missing_in_schema2 = schema1 - schema2
    return missing_in_schema1, missing_in_schema2

def main():
    # Initialize BigQuery client
    client = bigquery.Client()

    # Define project, dataset, and tables
    project_id = 'new-ventures-427409'
    dataset_id = 'temp_bc'
    table1_id = 'ns_review'
    table2_id = 'raw_specter_tmp'

    # Get schemas
    schema1 = get_table_schema(client, project_id, dataset_id, table1_id)
    # schema2 = get_table_schema(client, project_id, dataset_id, table2_id)

    print(schema1)
    
    # # Compare schemas
    # missing_in_schema1, missing_in_schema2 = compare_schemas(schema1, schema2)

    # # Print results
    # print(f"Columns missing in {table1_id}: {missing_in_schema1}")
    # print(f"Columns missing in {table2_id}: {missing_in_schema2}")

if __name__ == "__main__":
    main()
