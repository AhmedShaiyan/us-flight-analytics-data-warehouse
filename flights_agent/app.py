"""
app.py
------
Streamlit frontend for the Flights Data Assistant.

Run with:
    cd flights_agent
    streamlit run app.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure the flights_agent directory is on sys.path so sibling modules resolve
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

load_dotenv()

import streamlit as st

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Flights Data Assistant",
    page_icon="✈",
    layout="wide",
)

import agent
import vectorstore

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------
if "query_count" not in st.session_state:
    st.session_state.query_count = 0
if "query_log" not in st.session_state:
    st.session_state.query_log = []  # list of {"question": str, "model": str}

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Settings")

    llm_choice = st.radio(
        "Model",
        options=["Haiku (fast)", "Sonnet (accurate)"],
        index=0,
        help="Haiku is cheaper and faster. Sonnet produces more accurate SQL.",
    )
    llm_mode = "haiku" if llm_choice.startswith("Haiku") else "sonnet"
    os.environ["LLM_MODE"] = llm_mode

    st.divider()
    st.subheader("Session usage")
    count = st.session_state.query_count
    haiku_count = sum(1 for q in st.session_state.query_log if q["model"] == "haiku")
    sonnet_count = sum(1 for q in st.session_state.query_log if q["model"] == "sonnet")
    st.metric("Queries this session", count)
    st.caption(f"Haiku: {haiku_count}   Sonnet: {sonnet_count}")
    if count >= 20:
        st.warning("20+ queries this session — consider restarting if costs are a concern.")
    if count > 0 and st.button("Reset counter", use_container_width=True):
        st.session_state.query_count = 0
        st.session_state.query_log = []
        st.rerun()

    st.divider()
    st.subheader("Vector store")
    try:
        doc_count = vectorstore.collection_count()
        if doc_count > 0:
            st.success(f"{doc_count} documents indexed")
        else:
            st.warning(
                "Vector store is empty.  Run `setup_vectorstore.py` first."
            )
    except Exception as e:
        st.error(f"ChromaDB unavailable: {e}")

    st.divider()
    st.caption(
        "Data: US domestic flights (BTS)\n\n"
        "Powered by Claude + BigQuery"
    )

# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------
st.title("Flights Data Assistant")
st.subheader("Ask questions about US domestic flight performance")

question = st.text_input(
    label="Ask a question…",
    placeholder="e.g. Which airline had the most delays in January?",
    key="question_input",
)

run_button = st.button("Ask", type="primary")

if run_button and question.strip():
    with st.spinner("Generating SQL and querying BigQuery…"):
        result = agent.run_query(question.strip(), llm_mode=llm_mode)
    # Count every attempt (successful or not) so you see real API call volume
    st.session_state.query_count += 1
    st.session_state.query_log.append({"question": question.strip(), "model": llm_mode})

    error = result.get("error")
    df = result.get("dataframe")
    sql = result.get("sql")
    answer = result.get("answer")
    row_cap_warning = result.get("row_cap_warning")

    if error and df is None:
        # Hard failure — show error and nothing else
        st.error(f"Something went wrong. Please rephrase your question or try again.\n\n_{error}_")
    else:
        # Answer summary (most prominent)
        if answer:
            st.info(answer)

        # Generated SQL (collapsible)
        if sql:
            with st.expander("View SQL", expanded=False):
                st.code(sql, language="sql")

        # Results table
        if df is not None and not df.empty:
            if row_cap_warning:
                st.warning(row_cap_warning)
            st.caption(f"Showing {len(df):,} row{'s' if len(df) != 1 else ''}")
            st.dataframe(df, use_container_width=True)
        elif df is not None and df.empty:
            st.info("The query returned no rows.")

        # Non-fatal error (e.g. answer synthesis failed but data is available)
        if error and df is not None:
            st.warning(f"Note: {error}")

elif run_button and not question.strip():
    st.warning("Please enter a question before clicking Ask.")
