import os

parquet_file = "/workspaces/London_blackcab/Data/housing_in_london_monthly_variables.parquet"

if os.path.exists(parquet_file):
    print(f"File exists: {parquet_file}")
else:
    print(f"File NOT found: {parquet_file}")
