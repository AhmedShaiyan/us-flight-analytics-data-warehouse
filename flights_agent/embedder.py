from __future__ import annotations

from schema_loader import TableSchema


def _example_qa_pairs(project_id: str, dataset: str) -> list[dict]:
    p = f"`{project_id}.{dataset}"
    return [
        {
            "id": "example_01_airline_delays",
            "text": (
                "Question: Which airline had the most arrival delays?\n"
                "SQL:\n"
                f"SELECT carrier_name, COUNT(*) AS delayed_flights\n"
                f"FROM {p}.flight_analysis`\n"
                f"WHERE arr_delay_minutes > 15 AND is_cancelled = 0\n"
                f"GROUP BY carrier_name\n"
                f"ORDER BY delayed_flights DESC\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "delay_ranking"},
        },
        {
            "id": "example_02_ontime_rate",
            "text": (
                "Question: What is the on-time arrival rate by carrier?\n"
                "SQL:\n"
                f"SELECT carrier_name,\n"
                f"       ROUND(100.0 * SUM(is_ontime) / COUNT(*), 2) AS ontime_pct,\n"
                f"       COUNT(*) AS total_flights\n"
                f"FROM {p}.flight_analysis`\n"
                f"WHERE is_cancelled = 0\n"
                f"GROUP BY carrier_name\n"
                f"ORDER BY ontime_pct DESC\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "ontime_performance"},
        },
        {
            "id": "example_03_busiest_routes",
            "text": (
                "Question: What are the busiest flight routes by number of flights?\n"
                "SQL:\n"
                f"SELECT route_code, route_name, COUNT(*) AS flight_count\n"
                f"FROM {p}.flight_analysis`\n"
                f"GROUP BY route_code, route_name\n"
                f"ORDER BY flight_count DESC\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "route_analysis"},
        },
        {
            "id": "example_04_delay_by_dayofweek",
            "text": (
                "Question: Which day of the week has the highest average arrival delay?\n"
                "SQL:\n"
                f"SELECT day_name,\n"
                f"       ROUND(AVG(arr_delay_minutes), 2) AS avg_arr_delay\n"
                f"FROM {p}.flight_analysis`\n"
                f"WHERE is_cancelled = 0\n"
                f"GROUP BY day_name\n"
                f"ORDER BY avg_arr_delay DESC\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "temporal_analysis"},
        },
        {
            "id": "example_05_cancellation_rate",
            "text": (
                "Question: Which carrier has the highest cancellation rate?\n"
                "SQL:\n"
                f"SELECT carrier_name,\n"
                f"       ROUND(100.0 * SUM(is_cancelled) / COUNT(*), 2) AS cancel_pct,\n"
                f"       SUM(is_cancelled) AS cancelled_flights,\n"
                f"       COUNT(*) AS total_flights\n"
                f"FROM {p}.flight_analysis`\n"
                f"GROUP BY carrier_name\n"
                f"ORDER BY cancel_pct DESC\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "cancellation_analysis"},
        },
        {
            "id": "example_06_delay_attribution",
            "text": (
                "Question: What causes the most delay minutes across all airlines?\n"
                "SQL:\n"
                f"SELECT delay_type,\n"
                f"       ROUND(SUM(delay_minutes) / 60.0, 1) AS total_delay_hours\n"
                f"FROM {p}.delay_breakdown`\n"
                f"GROUP BY delay_type\n"
                f"ORDER BY total_delay_hours DESC\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "delay_attribution"},
        },
        {
            "id": "example_07_airport_ranking",
            "text": (
                "Question: Which origin airports have the worst average departure delay?\n"
                "SQL:\n"
                f"SELECT origin_code, origin_airport_name, origin_city,\n"
                f"       ROUND(AVG(dep_delay_minutes), 2) AS avg_dep_delay,\n"
                f"       COUNT(*) AS total_flights\n"
                f"FROM {p}.flight_analysis`\n"
                f"WHERE is_cancelled = 0\n"
                f"GROUP BY origin_code, origin_airport_name, origin_city\n"
                f"HAVING total_flights >= 100\n"
                f"ORDER BY avg_dep_delay DESC\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "airport_ranking"},
        },
        {
            "id": "example_08_monthly_trend",
            "text": (
                "Question: How do delays vary month by month?\n"
                "SQL:\n"
                f"SELECT month_name,\n"
                f"       ROUND(AVG(arr_delay_minutes), 2) AS avg_arr_delay,\n"
                f"       COUNT(*) AS total_flights\n"
                f"FROM {p}.flight_analysis`\n"
                f"WHERE is_cancelled = 0\n"
                f"GROUP BY month_name\n"
                f"ORDER BY MIN(flight_date)\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "monthly_trend"},
        },
        {
            "id": "example_09_carrier_delay_breakdown",
            "text": (
                "Question: Break down the total delay minutes by carrier and delay type.\n"
                "SQL:\n"
                f"SELECT carrier_name, delay_type,\n"
                f"       SUM(delay_minutes) AS total_delay_minutes\n"
                f"FROM {p}.delay_breakdown`\n"
                f"GROUP BY carrier_name, delay_type\n"
                f"ORDER BY carrier_name, total_delay_minutes DESC\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "carrier_delay_breakdown"},
        },
        {
            "id": "example_10_weather_delays",
            "text": (
                "Question: Which routes suffer the most weather delay minutes?\n"
                "SQL:\n"
                f"SELECT route_code, route_name,\n"
                f"       SUM(weather_delay_minutes) AS total_weather_delay,\n"
                f"       COUNT(*) AS flight_count\n"
                f"FROM {p}.flight_analysis`\n"
                f"WHERE weather_delay_minutes > 0\n"
                f"GROUP BY route_code, route_name\n"
                f"ORDER BY total_weather_delay DESC\n"
                f"LIMIT 100"
            ),
            "metadata": {"type": "example_qa", "topic": "weather_delay"},
        },
    ]


def build_documents(
    schemas: list[TableSchema],
    project_id: str,
    dataset: str,
) -> list[dict]:
    """Convert TableSchema objects into ChromaDB document dicts and append example Q&A pairs."""
    docs: list[dict] = []
    for schema in schemas:
        docs.append(schema.to_document(project_id=project_id, dataset=dataset))
    docs.extend(_example_qa_pairs(project_id=project_id, dataset=dataset))
    return docs
