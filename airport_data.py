from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project='flight-analytics-portfolio2001')

# Load  airports file
csv_path = r"C:\Users\SHAIYAN ALAM\Downloads\archive\airports.csv"
airports = pd.read_csv(csv_path)

print(f"Columns: {list(airports.columns)}")
print(f"\nSample data:")
print(airports.head())

# Load to BigQuery
table_id = 'flight-analytics-portfolio2001.raw.airports_raw'
job = client.load_table_from_dataframe(airports, table_id)
job.result()

print(f"\n Loaded {len(airports)} airports to BigQuery!")