# agents/dimensional_modeling_agent.py
from __future__ import annotations
from typing import Dict, List, Any, Tuple
import re

from backend.agents.agent_state import AgentState
from langchain_core.prompts import PromptTemplate
from backend.llm_provider import llm

# ---------------- Prompt (SQL only) ----------------
data_modeling_prompt_sql = PromptTemplate.from_template("""
You are an expert data modeler specializing in Kimball's dimensional modeling.

Task: Generate a single cohesive set of SQL DDL statements (ONLY RAW SQL, no markdown, no comments)
for an optimal dimensional model (star or snowflake) based on the provided schema.

Source schema (consolidated view of available data):
---
{schema}
---

Rules:
1) Model design: derive dimension tables (descriptive entities) and fact tables (events/processes).
2) Column inclusion: include ALL columns from the source schema; place in appropriate dim/fact tables.
3) Naming: tables lower_snake_case; prefix dimensions with dim_, facts with fact_; columns lower_snake_case.
4) Keys: every dim/fact has BIGINT surrogate key named <table_name>_sk as PRIMARY KEY.
   Include natural keys where appropriate (NOT NULL UNIQUE if reliable).
5) Relationships: add FOREIGN KEY constraints referencing surrogate keys.
6) Types: suggest reasonable types (e.g., BIGINT for IDs, VARCHAR/TEXT for strings, TIMESTAMP/DATE).
7) No duplicates: each table defined once; consolidate columns.
8) Temporal: add dim_date if timestamps/dates suggest it.
9) Multiple sources: add data_source and granularity where applicable.

Output: ONLY the raw SQL DDL (no explanations).
""")

# ---------------- Helpers ----------------
def sanitize_name(name: str) -> str:
    """snake_case + safe identifier"""
    name = re.sub(r'[^0-9a-zA-Z]+', '_', name).strip('_').lower()
    if re.match(r'^\d', name):
        name = f"t_{name}"
    return name

def clean_llm_code_response(raw_text: str) -> str:
    """Strip ```sql fences if present."""
    blocks = re.findall(r"```(?:sql)?\s*(.*?)```", raw_text, re.DOTALL)
    return ("\n".join(blocks).strip() if blocks else raw_text.strip())

def build_bronze_schema_text(dfs: Dict[str, Any]) -> str:
    parts: List[str] = []
    for name, df in dfs.items():
        t = sanitize_name(name)
        try:
            cols = [sanitize_name(c) for c in df.columns]
        except Exception:
            cols = []
        parts.append(f"Table: {t}\nColumns: {', '.join(cols)}")
    return "\n\n".join(parts)

def build_silver_schema_text(mapping_rows: List[Dict[str, Any]]) -> str:
    # Group silver columns by silver table, ignore Unknowns
    grouped: Dict[str, set] = {}
    for r in mapping_rows or []:
        stbl = str(r.get("silver_table", "")).strip()
        scol = str(r.get("silver_column", "")).strip()
        if not stbl or stbl.lower() == "unknown" or not scol or scol.lower() == "unknown":
            continue
        t = sanitize_name(stbl)
        c = sanitize_name(scol.replace(".", "_").replace("[", "_").replace("]", "_"))
        grouped.setdefault(t, set()).add(c)

    parts: List[str] = []
    for t, cols in grouped.items():
        parts.append(f"Table: {t}\nColumns: {', '.join(sorted(cols))}")
    return "\n\n".join(parts)

# ---------------- Agent Factory ----------------
def get_dimensional_modeling_agent(schema_view: str = "bronze"):
    """
    schema_view: "bronze" -> uses state.dfs
                 "silver" -> uses state.mapping_rows / state.fhir_mapping_rows
    Returns: (node_fn, description)
    """
    view = (schema_view or "bronze").strip().lower()

    def dimensional_modeling_agent_node(state: AgentState) -> AgentState:
        # normalize messages to dicts
        messages = [
            m if isinstance(m, dict) else {
                "role": getattr(m, "role", "assistant"),
                "content": getattr(m, "content", ""),
                "name": getattr(m, "name", None),
            }
            for m in (state.messages or [])
        ]

        # Build schema text based on chosen view
        if view == "silver":
            rows = getattr(state, "mapping_rows", []) or getattr(state, "fhir_mapping_rows", [])
            if not rows:
                return state.copy(update={
                    "messages": messages + [{
                        "role": "assistant",
                        "name": "Dimensional_Modeling_SQL",
                        "content": "No silver mappings found. Run the RAG Mapper first, then try again."
                    }]
                })
            schema_text = build_silver_schema_text(rows)
        else:
            if not state.dfs:
                return state.copy(update={
                    "messages": messages + [{
                        "role": "assistant",
                        "name": "Dimensional_Modeling_SQL",
                        "content": "No DataFrames in memory (bronze). Upload data first."
                    }]
                })
            schema_text = build_bronze_schema_text(state.dfs)

        # Generate SQL via LLM
        prompt = data_modeling_prompt_sql.format(schema=schema_text)
        raw = llm.invoke(prompt).content
        ddl = clean_llm_code_response(raw)

        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "name": "Dimensional_Modeling_SQL",
                "content": ddl  # SQL only
            }],
            "modeling_sql": ddl, # NEW for ER diagram                 
            "modeling_schema_view": view, 
        })
        

    desc = f"Generate Kimball-style dimensional model SQL from {view} schema."
    return dimensional_modeling_agent_node, desc
