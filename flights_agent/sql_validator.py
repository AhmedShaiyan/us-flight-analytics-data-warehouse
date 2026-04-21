from __future__ import annotations

import re

_FORBIDDEN_KEYWORDS = {
    "DROP", "DELETE", "INSERT", "UPDATE", "CREATE",
    "ALTER", "TRUNCATE", "MERGE", "REPLACE",
}


def validate(sql: str) -> str:
    """Run safety checks on sql. Returns the cleaned SQL or an ERROR: string."""
    if not sql or not sql.strip():
        return "ERROR: The model returned an empty SQL query."

    cleaned = sql.strip()

    first_token = re.split(r"\s+", cleaned, maxsplit=1)[0].upper()
    if first_token not in {"SELECT", "WITH"}:
        return (
            "ERROR: Generated query does not start with SELECT or WITH. "
            "Only read-only SELECT statements are permitted."
        )

    # Tokenise to avoid false positives on forbidden keywords inside string literals
    tokens = {t.upper() for t in re.findall(r"\b[A-Z_]+\b", cleaned, re.IGNORECASE)}
    bad = tokens & _FORBIDDEN_KEYWORDS
    if bad:
        return (
            f"ERROR: Query contains forbidden keyword(s): {', '.join(sorted(bad))}. "
            "Only SELECT statements are permitted."
        )

    # Allow trailing semicolon, but not multiple statements
    without_trailing = cleaned.rstrip(";").rstrip()
    if ";" in without_trailing:
        return (
            "ERROR: Query contains multiple statements (semicolons mid-query). "
            "Only a single SELECT statement is permitted."
        )

    if not re.search(r"\bLIMIT\b", cleaned, re.IGNORECASE):
        return (
            "ERROR: Query is missing a LIMIT clause. "
            "All queries must include LIMIT to prevent runaway scans."
        )

    return cleaned
