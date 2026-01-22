# system_assessment_agent.py

from typing import List, Dict, Any
from backend.agents.agent_state import AgentState
from backend.llm_provider import llm

# Default question used when user didn't ask anything explicit
default_question = (
    "Assess the gaps in my current data system, explain how to convert it to a Medallion "
    "architecture, and show how Databricks can optimise these pipelines, using only the "
    "objects present in the context snapshot."
)


def _safe_get_last_user_question(messages: List[Dict[str, Any]]) -> str:
    """Get the last user message content, if any."""
    if not messages:
        return ""
    for m in reversed(messages):
        if m.get("role") == "user" and m.get("content"):
            return str(m["content"]).strip()
    return ""


def _summarize_dfs(df_heads):
    """
    Build short summaries from df_heads:
    returns (df_names, df_summary_lines)
    """
    if not df_heads:
        return [], []

    df_names = list(df_heads.keys())
    summary_lines = []
    for name, head in df_heads.items():
        try:
            cols = list(head.columns)
        except Exception:
            cols = []
        sample_cols = ", ".join(cols[:8])
        summary_lines.append(f"- {name}: columns → {sample_cols}")
    return df_names, summary_lines


def system_assessment_node(state: AgentState) -> AgentState:
    messages = state.messages or []
    question = _safe_get_last_user_question(messages)

    dfs = state.dfs or {}
    df_heads = state.df_heads or {}
    df_dtypes = state.df_dtypes or {}
    mapping_rows = getattr(state, "mapping_rows", []) or []
    gold_mapping_rows = getattr(state, "gold_mapping_rows", []) or []
    kpis = (getattr(state, "kpis", "") or "").strip()
    dq_rules = (getattr(state, "dq_rules", "") or "").strip()
    masking_sql = (getattr(state, "masking_sql", "") or "").strip()
    sensitive_metadata = getattr(state, "sensitive_metadata", {}) or {}
    pii_columns = getattr(state, "pii_columns", []) or []
    phi_columns = getattr(state, "phi_columns", []) or []
    pyspark_code = (getattr(state, "pyspark_code", "") or "").strip()
    modeling_sql = (getattr(state, "modeling_sql", "") or "").strip()
    catalog = getattr(state, "catalog", "workspace")
    schema = getattr(state, "schema", "default")

    # ---- Build a concrete snapshot of the current system ----
    if df_heads:
        df_names, df_summaries = _summarize_dfs(df_heads)
    else:
        df_names = list(dfs.keys())
        df_summaries = []

    df_summaries_text = "\n".join(df_summaries) if df_summaries else "No dataframe heads available."

    # Bronze/Silver/Gold hints from mappings
    bronze_tables = sorted(
        {row.get("bronze_table") for row in mapping_rows if row.get("bronze_table")}
    ) or df_names
    silver_tables = sorted(
        {row.get("silver_table") for row in mapping_rows if row.get("silver_table")}
    )
    gold_tables = sorted(
        {
            row.get("gold_table") or row.get("target_table")
            for row in gold_mapping_rows
            if row.get("gold_table") or row.get("target_table")
        }
    )

    # Sensitive metadata summary
    smeta_lines = []
    for layer in ("bronze", "silver", "gold"):
        layer_meta = (sensitive_metadata or {}).get(layer) or {}
        if not layer_meta:
            continue
        smeta_lines.append(f"- {layer} layer:")
        for tbl, cols in layer_meta.items():
            tagged = ", ".join(f"{c} ({tag})" for c, tag in cols.items())
            smeta_lines.append(f"  • {tbl}: {tagged}")

    smeta_summary = (
        "\n".join(smeta_lines) if smeta_lines else "No structured sensitive metadata recorded yet."
    )

    has_kpis = bool(kpis)
    has_masking = bool(masking_sql)
    has_dq = bool(dq_rules)
    has_pyspark = bool(pyspark_code)
    has_modeling_sql = bool(modeling_sql)

    # Shorten pyspark snippet so prompt doesn’t explode
    pyspark_snippet = ""
    if has_pyspark:
        lines = pyspark_code.splitlines()
        pyspark_snippet = "\n".join(lines[:120])  # first ~120 lines are enough for assessment

    # ---- Build a strict, grounded prompt ----
    context_block = f"""
CATALOG / SCHEMA
- Active catalog: {catalog}
- Active schema: {schema}

RAW / SOURCE DATAFRAMES
- DataFrames detected: {", ".join(df_names) if df_names else "None"}
{df_summaries_text}

MAPPINGS / LAYERS
- Bronze tables inferred (from mappings or df names): {", ".join(bronze_tables) if bronze_tables else "None"}
- Silver tables inferred: {", ".join(silver_tables) if silver_tables else "None"}
- Gold tables inferred: {", ".join(gold_tables) if gold_tables else "None"}
- Total Bronze→Silver mappings: {len(mapping_rows)}
- Total Silver→Gold mappings: {len(gold_mapping_rows)}

SENSITIVE DATA
- PII columns (flattened): {", ".join(pii_columns) if pii_columns else "None"}
- PHI columns (flattened): {", ".join(phi_columns) if phi_columns else "None"}
- Sensitive metadata by layer:
{smeta_summary}

GOVERNANCE & QUALITY
- Has masking SQL? {"YES" if has_masking else "NO"}
- Has DQ rules / expectations? {"YES" if has_dq else "NO"}
- Has business KPIs? {"YES" if has_kpis else "NO"}

MODELING & PIPELINES
- Has PySpark medallion pipeline code? {"YES" if has_pyspark else "NO"}
- Has dimensional modeling SQL (DDL)? {"YES" if has_modeling_sql else "NO"}
"""

    final_question = question or default_question

    prompt = f"""
You are a Databricks solution architect.

Your task:
The user can ask things like:
- "What are the gaps in my current system?"
- "What steps do I need to take to convert to a Medallion architecture?"
- "How can Databricks optimise my pipelines?"

You must answer using ONLY the concrete context below (tables, columns, mappings, sensitive metadata, KPIs, PySpark code, etc.).
Do NOT answer generically. Ground everything in the actual names you see.

---------------- CONTEXT SNAPSHOT OF USER SYSTEM ----------------
{context_block}
-----------------------------------------------------------------

PySpark pipeline snippet (if available):
```python
{pyspark_snippet or "# No PySpark code has been generated yet."}
User question (latest):
"{final_question}"

---------------- INSTRUCTIONS (VERY IMPORTANT) ----------------

Your answer MUST have exactly 3 markdown sections with these headings:

Gaps in the current system

Steps to convert to a Medallion architecture

How Databricks can optimise these pipelines

GENERAL RULES (STRICT):

Always reference real objects from the context (e.g., actual table/dataframe names like patient_test,
bronze_* / silver_* / gold_* names, column names you see in the dataframe summaries).

If something (e.g., Silver tables, DQ rules, Gold layer) is missing in the context, explicitly say
it is missing instead of inventing it.

Avoid vague sentences like:
"Define Bronze tables to capture source data",
"Define Silver tables with data quality expectations",
"Define Gold tables to capture relevant metrics"
UNLESS you immediately tie them to specific tables/columns from the context.
Example: "Define Bronze tables such as bronze_patient_raw from dataframe patient_test to capture source data."

SECTION 1: Gaps in the current system

Mention gaps only when suggested by the context, for example:

No Silver or Gold tables inferred.

No DQ rules (dq_rules empty).

No masking SQL even though pii_columns or phi_columns exist.

No KPIs even if fact-like tables exist.

No PySpark/DDL even though mappings exist.

Be explicit: "Currently there are no Silver tables defined (silver_tables is empty)" etc.

SECTION 2: Steps to convert to a Medallion architecture

Use actual names from the context where possible:

For Bronze: list which existing dataframes/tables should be treated as Bronze (e.g., patient_test or bronze_patient_raw).

For Silver: reference mappings from mapping_rows and show how to clean/standardise Bronze into Silver tables.

For Gold: if gold_mapping_rows exist, explain how to turn them into gold_* tables or dimensional models.

Provide 2–4 concrete actionable steps, with bullets such as:

"Create Bronze table workspace.default.bronze_patient_raw from dataframe patient_test."

"Define Silver table workspace.default.silver_patient with DQ checks on column patient_id (not null, unique)."

If you propose new tables, clearly label them as "proposed_*" and explain how they relate to existing ones.

SECTION 3: How Databricks can optimise these pipelines

Explain how Databricks features apply SPECIFICALLY to the objects in this system:

Delta Lake / DLT / Lakeflow for managing Bronze→Silver→Gold for these exact tables.

Unity Catalog for securing pii_columns/phi_columns and the tables that contain them.

Masking SQL + sensitive_metadata for protecting sensitive columns in the actual tables mentioned.

If KPIs exist, mention how they can be materialised into Gold tables and served to BI tools.

Refer explicitly to the catalog/schema shown, and to any existing masking_sql / dq_rules / modeling_sql.

Keep the answer concise but concrete and system-specific.
Now produce your answer.
"""

    llm_response = llm.invoke(prompt)
    answer_content = getattr(llm_response, "content", str(llm_response))

    return state.copy(update={
        "messages": messages + [{
            "role": "assistant",
            "content": answer_content,
            "name": "SystemAssessment",
        }]
    })


def get_system_assessment_agent():
    description = (
    "Assesses the current data system (based on AgentState dfs, mappings, sensitive metadata, "
    "PySpark code, KPIs, masking SQL) and explains gaps, Medallion conversion steps, and "
    "Databricks optimisation opportunities in a grounded, system-specific way."
    )
    return system_assessment_node, description