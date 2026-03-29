"""
schema_loader.py
----------------
Reads dbt .sql model files from models/marts/ and the accompanying schema.yml
files, then produces a list of structured TableSchema documents ready to be
embedded into ChromaDB.

Column descriptions come from schema.yml where available; for any column not
covered there, a description is generated automatically from the column name
using the flights domain context dictionary below.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
    name: str
    description: str
    data_type: str = "STRING"


@dataclass
class TableSchema:
    table_name: str
    description: str
    columns: list[ColumnInfo] = field(default_factory=list)

    def to_document(self, project_id: str, dataset: str) -> dict:
        """Return a dict suitable for embedding as a ChromaDB document."""
        col_lines = "\n".join(
            f"  - {c.name} ({c.data_type}): {c.description}"
            for c in self.columns
        )
        text = (
            f"Table: {project_id}.{dataset}.{self.table_name}\n"
            f"Description: {self.description}\n"
            f"Columns:\n{col_lines}"
        )
        return {
            "id": f"table_{self.table_name}",
            "text": text,
            "metadata": {
                "type": "table_schema",
                "table_name": self.table_name,
                "full_table_name": f"{project_id}.{dataset}.{self.table_name}",
            },
        }


# ---------------------------------------------------------------------------
# Domain-aware description generator for unmapped columns
# ---------------------------------------------------------------------------

_COLUMN_DESCRIPTIONS: dict[str, str] = {
    # fact_flights / stg_flights
    "flight_date": "Calendar date of the scheduled departure (DATE).",
    "flight_timestamp": "Full departure date-time as a TIMESTAMP.",
    "carrier_code": "IATA two-character operating carrier code, e.g. 'AA' for American Airlines.",
    "flight_number": "Carrier-assigned numeric flight number.",
    "origin_airport": "Three-letter IATA departure airport code, e.g. 'JFK'.",
    "dest_airport": "Three-letter IATA arrival airport code, e.g. 'LAX'.",
    "origin_city": "City and state string for the departure airport, e.g. 'Dallas, TX'.",
    "dest_city": "City and state string for the arrival airport.",
    "scheduled_dep_time": "Scheduled departure time as an integer in HHMM format (e.g. 1430 = 14:30).",
    "actual_dep_time": "Actual departure time in HHMM integer format. NULL for cancelled flights.",
    "scheduled_arr_time": "Scheduled arrival time in HHMM integer format.",
    "actual_arr_time": "Actual arrival time in HHMM integer format. NULL for cancelled flights.",
    "dep_delay_minutes": "Departure delay in minutes. Negative values indicate early departure.",
    "arr_delay_minutes": "Arrival delay in minutes. Negative = early arrival. On-time threshold is <= 15 minutes.",
    "air_time_minutes": "Wheels-off to wheels-on flight duration in minutes.",
    "actual_elapsed_time_minutes": "Total gate-to-gate block time in minutes.",
    "is_cancelled": "1 if the flight was cancelled before departure, 0 otherwise.",
    "is_diverted": "1 if the flight landed at an unplanned alternate airport, 0 otherwise.",
    "cancellation_code": "BTS cancellation reason: A=Carrier, B=Weather, C=NAS, D=Security. NULL for operated flights.",
    "carrier_delay_minutes": "Arrival delay minutes attributed to the airline (crew, maintenance, etc.).",
    "weather_delay_minutes": "Arrival delay minutes caused by adverse weather conditions.",
    "nas_delay_minutes": "Arrival delay minutes attributed to the National Aviation System (ATC, traffic).",
    "security_delay_minutes": "Arrival delay minutes caused by security-related incidents.",
    "late_aircraft_delay_minutes": "Arrival delay minutes caused by the inbound aircraft arriving late on a prior leg.",
    "is_ontime": "1 when arr_delay_minutes <= 15 and flight operated, 0 otherwise.",
    # dim_carrier
    "carrier_name": "Full commercial airline name, e.g. 'Delta Air Lines'.",
    # dim_date
    "date_day": "Calendar date (DATE). Primary key of the date dimension.",
    "year": "Four-digit calendar year.",
    "quarter": "Calendar quarter (1-4).",
    "month": "Calendar month number (1-12).",
    "day_of_month": "Day of the month (1-31).",
    "day_of_week": "Day of week as integer: 1=Sunday, 7=Saturday (BigQuery convention).",
    "day_name": "Full weekday name, e.g. 'Monday'.",
    "month_name": "Full month name, e.g. 'January'.",
    "is_weekend": "1 if Saturday or Sunday, 0 for weekdays.",
    # dim_airport
    "airport_code": "Three-letter IATA airport code. Primary key of the airport dimension.",
    "airport_name": "Full airport name, e.g. 'Los Angeles Intl'.",
    "city": "City where the airport is located.",
    "state": "Two-letter US state abbreviation.",
    "country": "Country code. All rows are 'USA'.",
    "latitude": "Decimal latitude of the airport location.",
    "longitude": "Decimal longitude of the airport location.",
    # flight_analysis
    "origin_code": "Three-letter IATA code for the departure airport.",
    "origin_airport_name": "Full name of the departure airport.",
    "origin_state": "US state of the departure airport.",
    "origin_lat": "Decimal latitude of the departure airport.",
    "origin_lon": "Decimal longitude of the departure airport.",
    "dest_code": "Three-letter IATA code for the arrival airport.",
    "dest_airport_name": "Full name of the arrival airport.",
    "dest_state": "US state of the arrival airport.",
    "dest_lat": "Decimal latitude of the arrival airport.",
    "dest_lon": "Decimal longitude of the arrival airport.",
    "scheduled_dep_time_formatted": "Scheduled departure time as 'HH:MM' string, e.g. '14:30'.",
    "actual_dep_time_formatted": "Actual departure time as 'HH:MM' string. NULL for cancelled flights.",
    "scheduled_arr_time_formatted": "Scheduled arrival time as 'HH:MM' string.",
    "actual_arr_time_formatted": "Actual arrival time as 'HH:MM' string. NULL for cancelled flights.",
    "delay_category": "Human-readable bucket: Cancelled / Diverted / Early / On-Time / Delayed / Severely Delayed.",
    "route_name": "Readable route label: 'Origin City -> Destination City'.",
    "route_code": "Compact route identifier in 'ORIGIN-DEST' format, e.g. 'JFK-LAX'.",
    # delay_breakdown
    "delay_type": "Delay category: Carrier Delay, Weather Delay, NAS Delay, Security Delay, or Late Aircraft Delay.",
    "delay_minutes": "Total accumulated delay minutes for this carrier and delay type.",
}

# Inferred data types per column suffix patterns
_TYPE_HINTS: list[tuple[str, str]] = [
    ("_date", "DATE"),
    ("_timestamp", "TIMESTAMP"),
    ("_minutes", "FLOAT64"),
    ("_time", "INT64"),
    ("is_", "INT64"),
    ("_lat", "FLOAT64"),
    ("_lon", "FLOAT64"),
    ("latitude", "FLOAT64"),
    ("longitude", "FLOAT64"),
    ("year", "INT64"),
    ("quarter", "INT64"),
    ("month", "INT64"),
    ("day_of_month", "INT64"),
    ("day_of_week", "INT64"),
    ("flight_number", "INT64"),
]


def _infer_type(col_name: str) -> str:
    for suffix, dtype in _TYPE_HINTS:
        if col_name.endswith(suffix) or col_name.startswith(suffix):
            return dtype
    return "STRING"


def _describe_column(col_name: str) -> str:
    if col_name in _COLUMN_DESCRIPTIONS:
        return _COLUMN_DESCRIPTIONS[col_name]
    # Fallback: humanise snake_case
    return col_name.replace("_", " ").capitalize() + "."


# ---------------------------------------------------------------------------
# SQL parsing helpers
# ---------------------------------------------------------------------------

def _strip_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


def _extract_columns_from_sql(sql: str) -> list[str]:
    """Return a list of column aliases from the outermost SELECT statement."""
    clean = _strip_comments(sql)

    # Remove Jinja blocks
    clean = re.sub(r"\{\{.*?\}\}", "", clean, flags=re.DOTALL)
    clean = re.sub(r"\{%-?.*?-%?\}", "", clean, flags=re.DOTALL)

    # Find the SELECT … FROM span
    select_match = re.search(r"\bSELECT\b(.*?)\bFROM\b", clean, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return []

    select_body = select_match.group(1)

    columns: list[str] = []
    for raw_line in select_body.split("\n"):
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        # Look for AS alias
        alias_match = re.search(r"\bAS\s+(\w+)\s*$", line, re.IGNORECASE)
        if alias_match:
            columns.append(alias_match.group(1).lower())
            continue
        # Last bare identifier on the line
        ident_match = re.search(r"(\w+)\s*$", line)
        if ident_match:
            token = ident_match.group(1).upper()
            if token not in {
                "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "HAVING",
                "DISTINCT", "ALL", "ON", "AND", "OR", "NOT", "NULL",
            }:
                columns.append(ident_match.group(1).lower())

    return [c for c in columns if c]


# ---------------------------------------------------------------------------
# YAML description loader
# ---------------------------------------------------------------------------

def _load_yaml_descriptions(yaml_path: Path) -> dict[str, dict[str, str]]:
    """
    Parse a dbt schema.yml and return:
        { table_name: { column_name: description, "__table__": table_description } }
    """
    if not yaml_path.exists():
        return {}

    with yaml_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    result: dict[str, dict[str, str]] = {}
    for model in data.get("models", []):
        name = model.get("name", "")
        entry: dict[str, str] = {}
        if "description" in model:
            entry["__table__"] = model["description"].strip()
        for col in model.get("columns", []):
            col_name = col.get("name", "")
            col_desc = col.get("description", "").strip()
            if col_name and col_desc:
                entry[col_name] = col_desc
        result[name] = entry

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_mart_schemas(
    models_dir: Optional[Path] = None,
    project_id: str = "",
    dataset: str = "",
) -> list[TableSchema]:
    """
    Parse all .sql files under models/marts/ and return a list of TableSchema
    objects enriched with column descriptions from schema.yml.

    Args:
        models_dir: Path to the dbt models/ directory.  Defaults to
                    ../../flight_analytics_dbt/models relative to this file.
        project_id:  GCP project ID (used only for document text generation).
        dataset:     BigQuery dataset name.
    """
    if models_dir is None:
        models_dir = Path(__file__).parent.parent / "flight_analytics_dbt" / "models"

    marts_dir = models_dir / "marts"

    # Load YAML descriptions
    yaml_descs = _load_yaml_descriptions(marts_dir / "schema.yml")

    schemas: list[TableSchema] = []

    for sql_file in sorted(marts_dir.glob("*.sql")):
        table_name = sql_file.stem
        sql_content = sql_file.read_text(encoding="utf-8")

        # Table-level description from YAML or auto-generated
        table_entry = yaml_descs.get(table_name, {})
        table_description = table_entry.get(
            "__table__",
            f"Mart table containing {table_name.replace('_', ' ')} data.",
        )
        # Collapse multiline YAML descriptions to a single line for embedding
        table_description = " ".join(table_description.split())

        # Columns from SQL
        col_names = _extract_columns_from_sql(sql_content)

        columns: list[ColumnInfo] = []
        for col_name in col_names:
            desc = table_entry.get(col_name) or _describe_column(col_name)
            desc = " ".join(desc.split())
            columns.append(ColumnInfo(
                name=col_name,
                description=desc,
                data_type=_infer_type(col_name),
            ))

        schemas.append(TableSchema(
            table_name=table_name,
            description=table_description,
            columns=columns,
        ))

    return schemas


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    project = os.getenv("GOOGLE_CLOUD_PROJECT", "<project>")
    dataset = os.getenv("BQ_DATASET", "marts")

    for schema in load_mart_schemas(project_id=project, dataset=dataset):
        print(f"\n=== {schema.table_name} ({len(schema.columns)} columns) ===")
        print(schema.description[:120])
        for col in schema.columns[:5]:
            print(f"  {col.name}: {col.description[:80]}")
        if len(schema.columns) > 5:
            print(f"  ... and {len(schema.columns) - 5} more columns")
