from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project='flight-analytics-portfolio2001')

# Create sample flight data
df = pd.DataFrame({
    'flight_date': ['2024-01-15'],
    'carrier': ['AA'],
    'flight_num': ['100'],
    'origin': ['JFK'],
    'dest': ['LAX'],
    'dep_delay': [15],
    'arr_delay': [10]
})

# Load to BigQuery
table_id = 'flight-analytics-portfolio2001.raw.flights_raw'
job = client.load_table_from_dataframe(df, table_id)
job.result()

print("Data loaded! Query it:")
print(f"https://console.cloud.google.com/bigquery?project=flight-analytics-portfolio2001&ws=!1m5!1m4!4m3!1sflight-analytics-portfolio2001!2sraw!3sflights_raw")

# Test query
query = "SELECT * FROM `flight-analytics-portfolio2001.raw.flights_raw`"
results = client.query(query).to_dataframe()
print(results)