"""
bigquery_runner.py
------------------
Executes validated SQL against BigQuery and returns a pandas DataFrame.

Authentication : Application Default Credentials (gcloud auth application-default login)
Row cap        : 1000 rows maximum; excess rows are silently truncated and a
                 warning attribute is attached to the returned DataFrame.
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
        return f"ERROR: Could not initialise BigQuery client. Check your Application Default Credentials. ({type(exc).__name__})"

    try:
        query_job = client.query(sql)
        rows = query_job.result()
        df = rows.to_dataframe()
    except GoogleAPIError as exc:
        # Extract a user-safe message without raw BigQuery internals
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

    # Attach the warning as a DataFrame attribute (accessible via df.attrs)
    df.attrs[_WARNING_ATTR] = warning

    return df


def _safe_bq_error(exc: GoogleAPIError) -> str:
    """Return a terse, user-friendly message from a GoogleAPIError."""
    msg = str(exc)
    # Strip verbose HTTP response bodies if present
    if "HttpError" in msg or len(msg) > 300:
        return "query execution failed (check column names, table names, and SQL syntax)"
    # Keep first sentence only
    first_sentence = msg.split("\n")[0][:200]
    return first_sentence
