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

# LLM factory
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

# Core orchestration

def run_query(
    question: str,
    llm_mode: Optional[str] = None,
) -> dict:
    """Run the full NL-to-SQL pipeline. Returns dict with keys: sql, dataframe, answer, error, row_cap_warning."""
    result: dict = {
        "sql": None,
        "dataframe": None,
        "answer": None,
        "error": None,
        "row_cap_warning": None,
    }

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    dataset = os.getenv("BQ_DATASET", "marts")


  
    try:
        context_docs = vectorstore.retrieve(question, n_results=4)
    except Exception as exc:
        result["error"] = (
            f"Could not retrieve context from vector store. "
            f"Have you run setup_vectorstore.py? ({type(exc).__name__})"
        )
        return result


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
        print(f"[agent] ERROR: {result['error']}", flush=True)
        return result


    validated_sql = sql_validator.validate(raw_sql)
    if validated_sql.startswith("ERROR:"):
        result["sql"] = raw_sql
        result["error"] = validated_sql
        return result

    result["sql"] = validated_sql


    bq_result = bigquery_runner.run_query(validated_sql)
    if isinstance(bq_result, str):
        result["error"] = bq_result
        return result

    df: pd.DataFrame = bq_result
    result["dataframe"] = df
    result["row_cap_warning"] = df.attrs.get("row_cap_warning")


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
