from google.cloud import bigquery
import os
from datetime import datetime

def load_parquet_to_bq(project_id, dataset_id, table_id, parquet_file):
    client = bigquery.Client(project=project_id)  # connect to BigQuery
    table_ref = f"{project_id}.{dataset_id}.{table_id}"  # full table name

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,  # Parquet file
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # overwrite if exists
    )

    # open file and load to BigQuery
    with open(parquet_file, "rb") as file_obj:
        load_job = client.load_table_from_file(
            file_obj,
            destination=table_ref,
            job_config=job_config
        )

    load_job.result()  # wait for job to finish
    print(f"Loaded {load_job.output_rows} rows into {table_ref}")

if __name__ == "__main__":
    try:
        start_time = datetime.now()
        print(f"Loading has begun at {start_time}")

        PROJECT_ID = "london-housing-480514"
        DATASET_ID = "london_blackcab"
        TABLE_ID = "housing_data"
        PARQUET_FILE = "Data/housing_in_london_monthly_variables.parquet"

        os.makedirs("Data", exist_ok=True)  # ensure folder exists
        load_parquet_to_bq(PROJECT_ID, DATASET_ID, TABLE_ID, PARQUET_FILE)

        finish_time = datetime.now()
        elapsed_time = finish_time - start_time
        print(f"Loading has finished at {finish_time}")
        print(f"Elapsed time: {elapsed_time}")

    except Exception as e:
        print(f"There was an error: {e}")


