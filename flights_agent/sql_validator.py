"""
sql_validator.py
----------------
Validates LLM-generated SQL before it is sent to BigQuery.

All checks return the SQL unchanged on success or an "ERROR: …" string on
failure.  No exceptions are raised so callers can handle failures gracefully.
"""

from __future__ import annotations

import re

# DDL / DML keywords that must never appear in the query
_FORBIDDEN_KEYWORDS = {
    "DROP", "DELETE", "INSERT", "UPDATE", "CREATE",
    "ALTER", "TRUNCATE", "MERGE", "REPLACE",
}


def validate(sql: str) -> str:
    """
    Run all safety checks on *sql*.

    Returns:
        The original (stripped) SQL string if all checks pass.
        A string starting with "ERROR: " describing the first failed check.
    """
    if not sql or not sql.strip():
        return "ERROR: The model returned an empty SQL query."

    cleaned = sql.strip()

    # 1. Must start with SELECT
    first_token = re.split(r"\s+", cleaned, maxsplit=1)[0].upper()
    if first_token != "SELECT":
        return (
            "ERROR: Generated query does not start with SELECT. "
            "Only read-only SELECT statements are permitted."
        )

    # 2. No forbidden DML / DDL keywords
    # Tokenise to avoid false positives inside string literals or identifiers
    tokens = {t.upper() for t in re.findall(r"\b[A-Z_]+\b", cleaned, re.IGNORECASE)}
    bad = tokens & _FORBIDDEN_KEYWORDS
    if bad:
        return (
            f"ERROR: Query contains forbidden keyword(s): {', '.join(sorted(bad))}. "
            "Only SELECT statements are permitted."
        )

    # 3. No multiple statements — semicolons allowed only at the very end
    # Strip a trailing semicolon first, then check for any remaining ones
    without_trailing = cleaned.rstrip(";").rstrip()
    if ";" in without_trailing:
        return (
            "ERROR: Query contains multiple statements (semicolons mid-query). "
            "Only a single SELECT statement is permitted."
        )

    # 4. Must contain a LIMIT clause
    if not re.search(r"\bLIMIT\b", cleaned, re.IGNORECASE):
        return (
            "ERROR: Query is missing a LIMIT clause. "
            "All queries must include LIMIT to prevent runaway scans."
        )

    return cleaned
