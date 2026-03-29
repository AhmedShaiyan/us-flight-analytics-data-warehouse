"""
agent.py
--------
LangChain LCEL orchestration for the NL-to-SQL agent.

Two LCEL sub-chains:
  sql_chain    : prompt | sql_llm    | StrOutputParser()   (max_tokens=512)
  answer_chain : prompt | answer_llm | StrOutputParser()   (max_tokens=256)

Public function:
  run_query(question) -> dict  with keys: sql, dataframe, answer, error
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import StrOutputParser

import bigquery_runner
import prompt_builder
import sql_validator
import vectorstore

load_dotenv()

# ---------------------------------------------------------------------------
# LLM factory
# ---------------------------------------------------------------------------

_HAIKU_MODEL = "claude-haiku-4-5-20251001"
_SONNET_MODEL = "claude-sonnet-4-6"


def _build_sql_llm(mode: Optional[str] = None) -> ChatAnthropic:
    """LLM for SQL generation. 512 tokens is ample for any SQL query."""
    if mode is None:
        mode = os.getenv("LLM_MODE", "haiku").lower()
    model_id = _SONNET_MODEL if mode == "sonnet" else _HAIKU_MODEL
    return ChatAnthropic(model=model_id, temperature=0, max_tokens=512)


def _build_answer_llm(mode: Optional[str] = None) -> ChatAnthropic:
    """LLM for answer synthesis. 256 tokens covers 2-3 sentences comfortably."""
    if mode is None:
        mode = os.getenv("LLM_MODE", "haiku").lower()
    model_id = _SONNET_MODEL if mode == "sonnet" else _HAIKU_MODEL
    return ChatAnthropic(model=model_id, temperature=0, max_tokens=256)


# ---------------------------------------------------------------------------
# Core orchestration
# ---------------------------------------------------------------------------

def run_query(
    question: str,
    llm_mode: Optional[str] = None,
) -> dict:
    """
    Run the full NL-to-SQL pipeline for *question*.

    Pipeline steps:
        1. Retrieve top-4 schema/example docs from ChromaDB
        2. Build SQL generation prompt (LCEL: prompt | sql_llm | parser)
        3. Validate generated SQL
        4. Dry-run byte budget check + execute on BigQuery
        5. Build answer synthesis prompt (LCEL: prompt | answer_llm | parser)
        6. Return structured result dict

    Args:
        question:  Natural language question from the user.
        llm_mode:  "haiku" or "sonnet".  Falls back to LLM_MODE env var.

    Returns:
        dict with keys:
            sql             (str | None)          – validated SQL that was executed
            dataframe       (pd.DataFrame | None) – query results
            answer          (str | None)          – plain English summary
            error           (str | None)          – error message if any step failed
            row_cap_warning (str | None)          – set if results were truncated
    """
    result: dict = {
        "sql": None,
        "dataframe": None,
        "answer": None,
        "error": None,
        "row_cap_warning": None,
    }

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    dataset = os.getenv("BQ_DATASET", "marts")

    # ------------------------------------------------------------------
    # Step 1 — Retrieve relevant schema context from ChromaDB
    # ------------------------------------------------------------------
    try:
        context_docs = vectorstore.retrieve(question, n_results=4)
    except Exception as exc:
        result["error"] = (
            f"Could not retrieve context from vector store. "
            f"Have you run setup_vectorstore.py? ({type(exc).__name__})"
        )
        return result

    # ------------------------------------------------------------------
    # Step 2 — Generate SQL (LCEL chain: prompt | sql_llm | parser)
    # ------------------------------------------------------------------
    sql_llm = _build_sql_llm(llm_mode)
    answer_llm = _build_answer_llm(llm_mode)
    parser = StrOutputParser()

    try:
        sql_prompt = prompt_builder.build_sql_prompt(
            context_docs=context_docs,
            question=question,
            project_id=project_id,
            dataset=dataset,
        )
        sql_chain = sql_prompt | sql_llm | parser
        raw_sql: str = sql_chain.invoke({})
    except Exception as exc:
        result["error"] = f"SQL generation failed ({type(exc).__name__}): {exc}"
        return result

    # ------------------------------------------------------------------
    # Step 3 — Validate SQL
    # ------------------------------------------------------------------
    validated_sql = sql_validator.validate(raw_sql)
    if validated_sql.startswith("ERROR:"):
        result["sql"] = raw_sql
        result["error"] = validated_sql
        return result

    result["sql"] = validated_sql

    # ------------------------------------------------------------------
    # Step 4 — Byte budget check + execute on BigQuery
    # ------------------------------------------------------------------
    bq_result = bigquery_runner.run_query(validated_sql)
    if isinstance(bq_result, str):
        result["error"] = bq_result
        return result

    df: pd.DataFrame = bq_result
    result["dataframe"] = df
    result["row_cap_warning"] = df.attrs.get("row_cap_warning")

    # ------------------------------------------------------------------
    # Step 5 — Synthesise plain English answer (LCEL: prompt | answer_llm | parser)
    # ------------------------------------------------------------------
    try:
        answer_prompt = prompt_builder.build_answer_prompt(
            question=question,
            df=df,
        )
        answer_chain = answer_prompt | answer_llm | parser
        result["answer"] = answer_chain.invoke({})
    except Exception as exc:
        result["answer"] = None
        result["error"] = f"Answer synthesis failed ({type(exc).__name__}): {exc}"

    return result
