import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def return_csv(file, CHUNK):
    """Read CSV in chunks."""
    df_iter = pd.read_csv(file, chunksize=CHUNK)
    return df_iter

def return_parquet(df_iter, parquet_file):
    """Write CSV chunks to a Parquet file."""
    parquet_writer = None

    for i, chunk in enumerate(df_iter):
        print(f"Processing chunk {i}")

        if i == 0:
            # Guess schema from first chunk
            parquet_schema = pa.Table.from_pandas(chunk).schema
            # Open Parquet writer
            parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')

        # Convert chunk to PyArrow Table and write
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)

    # Close writer
    if parquet_writer:
        parquet_writer.close()

if __name__ == '__main__':
    try:
        FILE = "Data/housing_in_london_monthly_variables.csv"
        CHUNK = 10000
        PARQUET_FILE = "/workspaces/London_blackcab/Data/housing_in_london_monthly_variables.csv"

        # Make sure output folder exists
        os.makedirs(os.path.dirname(PARQUET_FILE), exist_ok=True)

        df_iter = return_csv(file=FILE, CHUNK=CHUNK)
        return_parquet(df_iter, PARQUET_FILE)

        print("CSV successfully converted to Parquet!")

    except Exception as e:
        print(f"Current error: {e}")
