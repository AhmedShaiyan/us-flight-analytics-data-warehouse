"""
bigquery_runner.py
------------------
Executes validated SQL against BigQuery and returns a pandas DataFrame.

Authentication : Application Default Credentials (gcloud auth application-default login)
Row cap        : 1000 rows maximum; excess rows are silently truncated and a
                 warning attribute is attached to the returned DataFrame.
Byte budget    : A free dry-run is performed before execution.  If the query
                 would scan more than BQ_MAX_GB gigabytes (default 1 GB) the
                 query is blocked and an error is returned instead.
Error handling : All BigQuery exceptions are caught and returned as plain
                 strings beginning with "ERROR: " — never exposed raw to the UI.
"""

from __future__ import annotations

import os
from typing import Union

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

_ROW_CAP = 1000
_WARNING_ATTR = "row_cap_warning"
_BYTES_PER_GB = 1024 ** 3


def _get_byte_budget() -> float:
    """Return the max bytes allowed per query (from BQ_MAX_GB env var, default 1 GB)."""
    try:
        return float(os.getenv("BQ_MAX_GB", "1")) * _BYTES_PER_GB
    except ValueError:
        return 1 * _BYTES_PER_GB


def _dry_run(client: bigquery.Client, sql: str) -> Union[int, str]:
    """
    Free BigQuery dry-run to estimate bytes that would be processed.
    Returns byte count on success, or an error string.
    """
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_job = client.query(sql, job_config=job_config)
        return dry_job.total_bytes_processed
    except GoogleAPIError as exc:
        return f"ERROR: Dry-run failed — {_safe_bq_error(exc)}"
    except Exception as exc:
        return f"ERROR: Dry-run failed ({type(exc).__name__})."


def run_query(sql: str) -> Union[pd.DataFrame, str]:
    """
    Execute *sql* on BigQuery and return a DataFrame, or an error string.

    The DataFrame will have a custom attribute `row_cap_warning` (str | None)
    set to a message if the result was truncated at 1000 rows.

    Args:
        sql: A validated SELECT statement.

    Returns:
        pd.DataFrame on success, or a string starting with "ERROR: " on failure.
    """
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        return "ERROR: GOOGLE_CLOUD_PROJECT environment variable is not set."

    try:
        client = bigquery.Client(project=project_id)
    except Exception as exc:
        return (
            f"ERROR: Could not initialise BigQuery client. "
            f"Check your Application Default Credentials. ({type(exc).__name__})"
        )

    # ------------------------------------------------------------------
    # Byte budget check — free dry-run before executing
    # ------------------------------------------------------------------
    byte_budget = _get_byte_budget()
    dry_result = _dry_run(client, sql)
    if isinstance(dry_result, str):
        return dry_result  # dry-run itself errored

    estimated_bytes: int = dry_result
    estimated_gb = estimated_bytes / _BYTES_PER_GB
    if estimated_bytes > byte_budget:
        budget_gb = byte_budget / _BYTES_PER_GB
        return (
            f"ERROR: Query blocked — it would scan {estimated_gb:.2f} GB, "
            f"which exceeds the {budget_gb:.1f} GB budget (BQ_MAX_GB). "
            f"Try a more selective query, or raise BQ_MAX_GB in your .env."
        )

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------
    try:
        query_job = client.query(sql)
        rows = query_job.result()
        df = rows.to_dataframe()
    except GoogleAPIError as exc:
        message = _safe_bq_error(exc)
        return f"ERROR: BigQuery returned an error — {message}"
    except Exception as exc:
        return f"ERROR: Unexpected error while running query ({type(exc).__name__})."

    warning: str | None = None
    if len(df) > _ROW_CAP:
        warning = (
            f"Result truncated to {_ROW_CAP} rows "
            f"(query returned {len(df)} rows total)."
        )
        df = df.head(_ROW_CAP).copy()

    df.attrs[_WARNING_ATTR] = warning
    return df


def _safe_bq_error(exc: GoogleAPIError) -> str:
    """Return a terse, user-friendly message from a GoogleAPIError."""
    msg = str(exc)
    if "HttpError" in msg or len(msg) > 300:
        return "query execution failed (check column names, table names, and SQL syntax)"
    return msg.split("\n")[0][:200]
