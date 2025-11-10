from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project='flight-analytics-portfolio2001')

# Update this path to your actual CSV file
csv_path = r"C:\Users\SHAIYAN ALAM\Downloads\T_ONTIME_REPORTING_20251021_155601\T_ONTIME_REPORTING.csv"

print("Reading CSV...")
# Read CSV in chunks (BTS files are big)
df = pd.read_csv(csv_path, low_memory=False)

print(f"Loaded {len(df):,} flights")
print(f"Columns: {list(df.columns)}")
print(f"\nSample data:")
# Sample data  (Preview)
print(df.head())

# Load to BigQuery
table_id = 'flight-analytics-portfolio2001.raw.flights_raw'

print(f"\nLoading to BigQuery...")
job = client.load_table_from_dataframe(
    df, 
    table_id,
    job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrites table
        autodetect=True
    )
)
job.result()  # Wait for completion

print(f" Loaded {len(df):,} rows to BigQuery!")
print(f"View it: https://console.cloud.google.com/bigquery?project=flight-analytics-portfolio2001")