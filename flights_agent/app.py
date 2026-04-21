from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

load_dotenv()

import gcp_secrets as _secrets
_secrets.load_secrets()

import re

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# Page config — must be first Streamlit call
st.set_page_config(
    page_title="Flights Data Assistant",
    page_icon="✈",
    layout="wide",
)

import agent
import vectorstore

# Session state initialisation
if "query_count" not in st.session_state:
    st.session_state.query_count = 0

# Set model, defaults to haiku
llm_mode = os.getenv("LLM_MODE", "haiku").lower()

# Chart selection logic
_DATE_HINT_RE = re.compile(r'\b(?:date|time|month|year|day|week|period|quarter)\b')

def _is_date_col(col: pd.Series) -> bool:
    """True if the column is a datetime type or its name suggests a time axis."""
    if pd.api.types.is_datetime64_any_dtype(col):
        return True
    if _DATE_HINT_RE.search(col.name.lower()):
        try:
            pd.to_datetime(col)
            return True
        except Exception:
            return False
    return False


def _numeric_cols(df: pd.DataFrame) -> list[str]:
    return df.select_dtypes(include="number").columns.tolist()


def _categorical_cols(df: pd.DataFrame) -> list[str]:
    return df.select_dtypes(exclude="number").columns.tolist()


def _requested_chart_type(question: str) -> str | None:
    """Return 'pie', 'line', or 'bar' if the question explicitly requests one."""
    import re
    q = question.lower()
    if re.search(r"\bpie\b", q):
        return "pie"
    if re.search(r"\bline\b", q) or re.search(r"\bover time\b", q) or re.search(r"\btrend\b", q):
        return "line"
    if re.search(r"\bbar\b", q):
        return "bar"
    return None


def render_chart(df: pd.DataFrame, question: str = "") -> None:
    """
    Pick and render the most appropriate chart, then show the raw table in
    a collapsed expander. Falls back to table-only when no chart fits.

    Priority:
      1. Explicit request in question ("pie chart", "bar chart", "line graph")
      2. Date/time column present → line chart
      3. ≤ 15 rows, 1 categorical + 1 numeric  → pie chart
      4. Categorical + numeric                  → bar chart
      5. Anything else                          → table only
    """
    import plotly.express as px

    num_cols = _numeric_cols(df)
    cat_cols = _categorical_cols(df)
    date_col = next((c for c in df.columns if _is_date_col(df[c])), None)

    requested = _requested_chart_type(question)
    chart_rendered = False

    def _pie():
        fig = px.pie(
            df,
            names=cat_cols[0],
            values=num_cols[0],
            color_discrete_sequence=px.colors.sequential.Oranges_r,
        )
        fig.update_layout(
            paper_bgcolor="#1C1C1C", plot_bgcolor="#1C1C1C",
            font_color="#FFFFFF", margin=dict(t=30, b=0, l=0, r=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    def _bar():
        fig = px.bar(
            df, x=cat_cols[0], y=num_cols[0],
            color_discrete_sequence=["#F5A623"],
        )
        fig.update_layout(
            paper_bgcolor="#1C1C1C", plot_bgcolor="#1C1C1C",
            font_color="#FFFFFF",
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
            margin=dict(t=30, b=0, l=0, r=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    def _line():
        nonlocal date_col
        if date_col:
            plot_df = df.copy()
            plot_df[date_col] = pd.to_datetime(plot_df[date_col], infer_datetime_format=True)
            plot_df = plot_df.sort_values(date_col).set_index(date_col)
            st.line_chart(plot_df[num_cols])
        elif cat_cols and num_cols:
            st.line_chart(df.set_index(cat_cols[0])[num_cols])

    # --- Explicit request overrides auto-detection ---
    if requested == "pie" and cat_cols and num_cols:
        _pie(); chart_rendered = True
    elif requested == "line" and num_cols:
        _line(); chart_rendered = True
    elif requested == "bar" and cat_cols and num_cols:
        _bar(); chart_rendered = True

    # --- Auto-detection ---
    elif date_col and num_cols:
        _line(); chart_rendered = True
    elif cat_cols and num_cols and len(df) <= 15 and len(num_cols) == 1:
        _pie(); chart_rendered = True
    elif cat_cols and num_cols:
        _bar(); chart_rendered = True

    if chart_rendered:
        with st.expander("View table", expanded=False):
            st.dataframe(df, use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)


# Main content
st.title("Flights Data Assistant")


_LOOKER_EMBED = (
    "https://datastudio.google.com/embed/reporting/"
    "1ebe72c4-1eae-4f09-be64-ee63fa1c0ab1/page/aX0cF"
)
components.iframe(_LOOKER_EMBED, height=700, scrolling=True)

st.divider()
st.subheader("Ask questions about US domestic flight performance")

# Wrap in a form so Enter key submits
with st.form(key="question_form", border=False):
    question = st.text_input(
        label="Ask a question…",
        placeholder="e.g. Which airline had the most delays in January?",
        key="question_input",
    )
    run_button = st.form_submit_button("Ask", type="primary")

def _friendly_error(error: str) -> tuple[str, str]:
    """
    Map a technical ERROR: string to (headline, suggestion) for the user.
    Returns plain strings — no markdown.
    """
    e = error.lower()
    if "does not start with select or with" in e:
        return (
            "I couldn't turn that into a data question.",
            "Try asking something more specific, e.g. 'Which airline had the most delays in January?' "
            "or 'Show me daily flights at JFK in January'.",
        )
    if "dry-run failed" in e or "query execution failed" in e:
        return (
            "The query referenced a column or table that doesn't exist.",
            "Try rephrasing with clearer terms — for example, use airport codes like 'ORD' or 'JFK', "
            "airline names like 'Delta' or 'Southwest', and month names like 'January'.",
        )
    if "exceeds the" in e and "budget" in e:
        return (
            "That query would scan too much data.",
            "Try narrowing it down — add a month filter or a specific airline to reduce the data scanned.",
        )
    if "missing a limit" in e:
        return (
            "The generated query was missing a safety limit.",
            "Try asking again — this is usually a one-off issue.",
        )
    if "returned no rows" in e or "no rows" in e:
        return (
            "No data matched your question.",
            "Check that the filters are correct — for example, month names should be written in full "
            "('January' not 'Jan') and airport codes should be uppercase ('ORD' not 'ord').",
        )
    return (
        "Something went wrong.",
        "Try rephrasing your question or making it more specific.",
    )


if run_button and question.strip():
    with st.spinner(""):
        result = agent.run_query(question.strip(), llm_mode=llm_mode)
    st.session_state.query_count += 1

    error = result.get("error")
    df = result.get("dataframe")
    sql = result.get("sql")
    answer = result.get("answer")
    row_cap_warning = result.get("row_cap_warning")

    if error and df is None:
        headline, suggestion = _friendly_error(error)
        st.error(f"**{headline}**\n\n{suggestion}")
        if sql:
            with st.expander("View generated SQL", expanded=False):
                st.code(sql, language="sql")
    else:
        if answer:
            st.info(answer)

        if sql:
            with st.expander("View SQL", expanded=False):
                st.code(sql, language="sql")

        if df is not None and not df.empty:
            if row_cap_warning:
                st.warning(row_cap_warning)
            st.caption(f"Showing {len(df):,} row{'s' if len(df) != 1 else ''}")
            render_chart(df, question=question)
        elif df is not None and df.empty:
            st.info("No data found. Try adjusting your filters — check month names, airport codes, or airline names.")

        if error and df is not None:
            headline, suggestion = _friendly_error(error)
            st.warning(f"{headline} {suggestion}")

elif run_button and not question.strip():
    st.warning("Please enter a question before clicking Ask.")
