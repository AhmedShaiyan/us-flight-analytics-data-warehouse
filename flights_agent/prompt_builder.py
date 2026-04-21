from __future__ import annotations

import os

import pandas as pd
from langchain_core.prompts import ChatPromptTemplate



_FEW_SHOT_EXAMPLES = """
--- Example 1 ---
Question: Which airline had the most delays in January?
SQL:
SELECT carrier_name,
       COUNT(*) AS delayed_flights
FROM `{project_id}.{dataset}.flight_analysis`
WHERE arr_delay_minutes > 15
  AND is_cancelled = 0
  AND month_name = 'January'
GROUP BY carrier_name
ORDER BY delayed_flights DESC
LIMIT 100

--- Example 2 ---
Question: What is the average departure delay by day of the week?
SQL:
SELECT day_name,
       ROUND(AVG(dep_delay_minutes), 2) AS avg_dep_delay,
       COUNT(*) AS total_flights
FROM `{project_id}.{dataset}.flight_analysis`
WHERE is_cancelled = 0
GROUP BY day_name
ORDER BY avg_dep_delay DESC
LIMIT 100

--- Example 3 ---
Question: Which routes have the highest cancellation rate?
SQL:
SELECT route_code,
       route_name,
       ROUND(100.0 * SUM(is_cancelled) / COUNT(*), 2) AS cancel_pct,
       COUNT(*) AS total_flights
FROM `{project_id}.{dataset}.flight_analysis`
GROUP BY route_code, route_name
HAVING total_flights >= 50
ORDER BY cancel_pct DESC
LIMIT 100

--- Example 4 ---
Question: How many flights arrived and departed at ORD each day in January?
SQL:
WITH departures AS (
  SELECT flight_date,
         COUNT(*) AS departing_flights
  FROM `{project_id}.{dataset}.flight_analysis`
  WHERE origin_code = 'ORD'
    AND month_name = 'January'
  GROUP BY flight_date
),
arrivals AS (
  SELECT flight_date,
         COUNT(*) AS arriving_flights
  FROM `{project_id}.{dataset}.flight_analysis`
  WHERE dest_code = 'ORD'
    AND month_name = 'January'
  GROUP BY flight_date
)
SELECT d.flight_date,
       d.departing_flights,
       a.arriving_flights
FROM departures d
JOIN arrivals a USING (flight_date)
ORDER BY flight_date
LIMIT 100
"""


_SQL_SYSTEM_TEMPLATE = """\
You are an expert BigQuery SQL analyst for a US domestic flights data warehouse.
Your job is to translate the user's natural language question into a single,
valid BigQuery SQL query.

GCP Project  : {project_id}
Dataset      : {dataset}

## Available tables
{context}

## Rules — follow every rule exactly
1. Return ONLY the raw SQL query — no markdown fences, no explanations, no comments.
2. Always use fully-qualified table names in the format `project_id.dataset.table`.
3. Always include LIMIT 100 at the end of the query.
4. Never use DROP, DELETE, INSERT, UPDATE, CREATE, ALTER, or TRUNCATE.
5. If joining tables, prefer `flight_analysis` as it is already fully denormalised.
6. Use ROUND() for floating-point metrics to 2 decimal places.
7. Use standard BigQuery SQL syntax (not standard SQL, not Postgres).

## Few-shot examples
{examples}
"""

_SQL_HUMAN_TEMPLATE = "Question: {question}"


def build_sql_prompt(
    context_docs: list[str],
    question: str,
    project_id: str = "",
    dataset: str = "",
) -> ChatPromptTemplate:
    if not project_id:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "your-gcp-project")
    if not dataset:
        dataset = os.getenv("BQ_DATASET", "marts")

    context_text = "\n\n".join(context_docs) if context_docs else "No context retrieved."
    examples = _FEW_SHOT_EXAMPLES.format(project_id=project_id, dataset=dataset)

    system_content = _SQL_SYSTEM_TEMPLATE.format(
        project_id=project_id,
        dataset=dataset,
        context=context_text,
        examples=examples,
    )

    return ChatPromptTemplate.from_messages([
        ("system", system_content),
        ("human", question),
    ])



_ANSWER_SYSTEM_TEMPLATE = """\
You are a helpful data analyst explaining BigQuery query results about US
domestic flight performance to a non-technical business user.

Write 2–3 concise sentences summarising the key finding from the data.
Be specific: include numbers, percentages, and airline or airport names
where relevant.  Do not mention SQL, tables, or technical terms.
"""

_ANSWER_HUMAN_TEMPLATE = """\
The user asked: {question}

The query returned the following data (first {row_count} rows shown):
{data_preview}

Summarise the key insight in 2-3 sentences.
"""


def build_answer_prompt(
    question: str,
    df: pd.DataFrame,
) -> ChatPromptTemplate:
    preview_rows = min(20, len(df))
    data_preview = df.head(preview_rows).to_string(index=False)
    row_count = len(df)

    human_content = _ANSWER_HUMAN_TEMPLATE.format(
        question=question,
        row_count=row_count,
        data_preview=data_preview,
    )

    return ChatPromptTemplate.from_messages([
        ("system", _ANSWER_SYSTEM_TEMPLATE),
        ("human", human_content),
    ])
