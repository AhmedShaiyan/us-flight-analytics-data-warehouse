# Flight Analytics

**[Check it out here](https://flights-agent-763890546537.us-central1.run.app)**

A conversational agent over a BigQuery data warehouse of 547K US domestic flights (January 2025). Type a question, get an answer backed by real data. The site also includes an embedded Looker Studio dashboard for visual exploration.

## How it works

The underlying data is BTS On-Time Performance data, loaded into BigQuery and modelled with dbt into a star schema. When you ask a question, the agent pulls relevant schema context from a ChromaDB vector store and uses Claude Haiku to generate a BigQuery SQL query. The query goes through a dry-run cost check before execution, and the results come back as a summary with an auto-selected chart.

## Stack

- BigQuery + dbt (data warehouse)
- LangChain + Claude Haiku
- ChromaDB + sentence-transformers (schema retrieval)
- Streamlit (frontend)
- Google Cloud Run (deployment)

## Running locally

**Data warehouse setup** (one-time)

```bash
gcloud auth application-default login

python load_flight_data.py
python airport_data.py

cd flight_analytics_dbt
dbt run && dbt test
```

**Agent**

```bash
cd flights_agent
pip install -r requirements.txt
```

Create a `.env` file:

```
ANTHROPIC_API_KEY=...
GOOGLE_CLOUD_PROJECT=...
BQ_DATASET=marts
```

```bash
python setup_vectorstore.py   # run once, or after schema changes
streamlit run app.py
```

## Deploying to Cloud Run

Build and push (run from repo root so the Dockerfile can access both `flights_agent/` and `flight_analytics_dbt/`):

```bash
gcloud builds submit . --tag REGION-docker.pkg.dev/PROJECT/REPO/IMAGE

gcloud run deploy flights-agent \
  --image REGION-docker.pkg.dev/PROJECT/REPO/IMAGE \
  --service-account SA@PROJECT.iam.gserviceaccount.com \
  --set-secrets ANTHROPIC_API_KEY=SECRET_NAME:latest \
  --set-env-vars GOOGLE_CLOUD_PROJECT=PROJECT,BQ_DATASET=marts,LLM_MODE=haiku \
  --allow-unauthenticated \
  --max-instances=2
```

The vector store is baked into the image at build time, so there is no setup delay on cold starts.

## Data

Bureau of Transportation Statistics On-Time Performance data.
547,271 flights, 19 carriers, 340+ airports, January 2025.
https://www.transtats.bts.gov/
