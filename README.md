# US Domestic Flight Analytics Data Warehouse

Dashboard analyzing 547K+ US domestic flights. Built with Bureau of Transportation Statistics (BTS) data 

**[View Dashboard here →](https://lookerstudio.google.com/reporting/1ebe72c4-1eae-4f09-be64-ee63fa1c0ab1)**

##  Overview

This project implements a dimensional data warehouse analyzing flight performance, delays, and operational metrics across US airlines and airports. The pipeline ingests raw BTS data, transforms it through a star schema, and delivers insights via interactive dashboards on Looker studio.

**Key Metrics Tracked:**
- On-Time Performance (OTP): 82.3% average
- 547,271 flights analyzed (January 2025)
- 340+ airports and 19 carriers
- 5 delay cause categories tracked

## Architecture
```
BTS Raw Data (CSV)
    ↓
Python ETL Scripts
    ↓
BigQuery (Bronze Layer - raw.flights_raw)
    ↓
dbt Transformations
    ↓
Star Schema (Gold Layer)
    ├── fact_flights
    ├── dim_date
    ├── dim_carrier
    ├── dim_airport
    └── flight_analysis (denormalized view)
    ↓
Looker Studio Dashboard
```

### Data Flow

1. **Extract**: Python scripts download BTS On-Time Performance data
2. **Load**: Raw CSV data loaded to BigQuery `raw` dataset
3. **Transform**: dbt models create staging → dimensional → analytical layers
4. **Visualize**: Looker Studio dashboards

## Project Structure
```
flight-analytics-data-warehouse/
├── flight_analytics_dbt/          # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_flights.sql    # Clean raw data
│   │   └── marts/
│   │       ├── dim_date.sql       # Date dimension
│   │       ├── dim_carrier.sql    # Airline dimension
│   │       ├── dim_airport.sql    # Airport dimension
│   │       ├── fact_flights.sql   # Core fact table
│   │       ├── flight_analysis.sql # Denormalized analytics view
│   │       └── delay_breakdown.sql # Delay attribution pivot
│   └── dbt_project.yml
├── load_flight_data.py            # BTS data loader
├── airport_data.py                # Airport reference data loader
└── README.md
```

## Data Model

### Star Schema Design

**Fact Table: `fact_flights`**
- Grain: One row per flight segment
- Metrics: Delay minutes, air time, on-time status
- Foreign Keys: Links to all dimension tables

**Dimension Tables:**
- `dim_date`: Calendar attributes, weekends, holidays
- `dim_carrier`: Airline names and codes
- `dim_airport`: Airport details, locations, city/state
- `dim_aircraft`: Aircraft types (planned)

**Analytical Views:**
- `flight_analysis`: Denormalized view joining all dimensions
- `delay_breakdown`: Pivoted delay causes by carrier

### Key Fields

**Delay Categories (BTS Standard):**
- `carrier_delay_minutes`: Airline operational issues
- `weather_delay_minutes`: Extreme weather conditions
- `nas_delay_minutes`: Air traffic control/airport operations
- `security_delay_minutes`: Security breaches
- `late_aircraft_delay_minutes`: Cascading delays from previous flights

## Getting Started

### Prerequisites

- Python 3.10+
- Google Cloud Platform account
- dbt-bigquery

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/YOUR-USERNAME/flight-analytics-data-warehouse.git
cd flight-analytics-data-warehouse
```

2. **Install dependencies**
```bash
pip install google-cloud-bigquery pandas dbt-bigquery
```

3. **Set up GCP**
```bash
# Authenticate
gcloud auth application-default login

# Set project
gcloud config set project YOUR-PROJECT-ID
```

4. **Download BTS data**
- Visit: https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ
- Select desired month and fields
- Download CSV

5. **Load data to BigQuery**
```bash
# Update csv_path in load_flight_data.py
python load_flight_data.py
python airport_data.py
```

6. **Run dbt transformations**
```bash
cd flight_analytics_dbt
dbt run
dbt test
```

## Analytics & Insights

### Dashboard Views

**1.  Overview**
- Total flights, on-time %, cancellation rate
- Daily flight volume trends
- Top 20 routes by performance

**2. Delay Analysis**
- Stacked breakdown of delay causes by carrier
- Root cause attribution (carrier vs external factors)
- Delay category distribution (on-time, delayed, severely delayed)

**3. Carrier Performance**
- Head-to-head airline comparisons
- Reliability metrics by carrier
- Route-level competitive analysis

**4. Airport Performance**
- Departure delay patterns by airport
- Geographic heatmap of performance 



## Data Sources

- **Primary**: Bureau of Transportation Statistics (BTS) On-Time Performance
  - https://www.transtats.bts.gov/
  - Updated monthly, ~30 days after month-end
  - 547,271 January 2025 records

- **Reference Data**: 
  - Airport codes/locations: Kaggle US Airports dataset
  - Carrier names: Manual mapping of IATA codes




