from google.cloud import bigquery
import os
from datetime import datetime

def load_parquet_to_bq(project_id, dataset_id, table_id, parquet_file):
    client = bigquery.Client(project=project_id)  # fixed typo

    # Create dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        print(f"Dataset {dataset_id} not found. Creating dataset...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"  # set your location
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

    table_ref = f"{project_id}.{dataset_id}.{table_id}"  # full table name

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
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
        DATASET_ID = "housing_london"  # update to match your actual dataset
        TABLE_ID = "housing_data"
        PARQUET_FILE = "/workspaces/London_blackcab/Data/housing_in_london_monthly_variables.parquet"

        os.makedirs("Data", exist_ok=True)  # ensure folder exists
        load_parquet_to_bq(PROJECT_ID, DATASET_ID, TABLE_ID, PARQUET_FILE)

        finish_time = datetime.now()
        elapsed_time = finish_time - start_time
        print(f"Loading has finished at {finish_time}")
        print(f"Elapsed time: {elapsed_time}")

    except Exception as e:
        print(f"There was an error: {e}")

<<<<<<< HEAD

=======
>>>>>>> origin/aladon090-dbt

