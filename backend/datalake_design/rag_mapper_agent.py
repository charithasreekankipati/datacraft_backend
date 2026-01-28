# agents/rag_mapper_agent.py
from __future__ import annotations
from typing import Dict, List, Tuple, Optional
from pydantic import BaseModel
import json
import logging
import os
import time
  #for debugging

import pandas as pd
from databricks_langchain.vectorstores import DatabricksVectorSearch

from backend.agents.agent_state import AgentState
from backend.llm_provider import llm

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from dotenv import load_dotenv
load_dotenv()  



# ---- Helpers ---------------------------------------------------------------

def _safe_json(text: str) -> dict:
    try:
        return json.loads(text)
    except Exception:
        return {}

from pydantic import BaseModel, Field

class ColumnSummary(BaseModel):
    description: str = Field(..., description="Semantic description of the column")
    semantic_type: str = Field(default="text")
    role: str = Field(default="attribute")
    business_entities: List[str] = Field(default_factory=list)
    examples: List[str] = Field(default_factory=list)

class DFColSummary(BaseModel):
    df_summary: str = Field(..., description="High-level summary of the table's purpose")
    dataset_entities: List[str] = Field(default_factory=list)
    column_summaries: Dict[str, ColumnSummary] = Field(
        ..., 
        description="A dictionary mapping EVERY column name to its specific metadata"
    )


# def _summarize_dataframe_and_columns(df_name: str, df: pd.DataFrame) -> Tuple[str, Dict[str, str]]:
#     """One LLM call per DF to produce df_summary + column_summaries."""
#     head_rows = df.head(3).to_dict(orient="records")
#     dtypes = {c: str(t) for c, t in df.dtypes.items()}

#     prompt = f"""
# You are mapping source data columns to a target standard. Summarize the DataFrame and each column.

# DataFrame: {df_name}
# dtypes: {json.dumps(dtypes, ensure_ascii=False)}
# head(3): {json.dumps(head_rows, ensure_ascii=False)}

# Return ONLY valid JSON:
# {{
#   "df_summary": "one-line overview",
#   "column_summaries": {{"colA": "one line", "colB": "one line"}}
# }}
# """
#     resp = llm.invoke(prompt)
#     content = (getattr(resp, "content", None) or str(resp)).strip()
#     data = _safe_json(content)

#     df_summary = data.get("df_summary") or f'{df_name} with {len(df)} rows and {df.shape[1]} columns.'
#     col_summaries = data.get("column_summaries") or {}

#     # ensure coverage
#     for c in df.columns:
#         if c not in col_summaries:
#             samples = ", ".join(map(str, df[c].dropna().head(3).tolist()))
#             col_summaries[c] = f'Column "{c}" (dtype={dtypes.get(c)}), e.g., {samples}'
#     return df_summary, col_summaries
def _summarize_dataframe_and_columns(df_name: str, df: pd.DataFrame) -> Tuple[str, Dict[str, str]]:
    head_rows = df.head(3).to_dict(orient="records")
    dtypes = {c: str(t) for c, t in df.dtypes.items()}

    system = (
        "You are a data modeling assistant. Given a small sample and dtypes, "
        "infer the table’s business meaning and for each column give a compact, domain-aware summary."
        "Return ONLY a JSON object matching the provided schema."
        "Prefer concrete business terminology (e.g., clinic, location, provider, patient)."
        "Infer semantic_type (id/code/datetime/enum/amount/quantity/text/bool), role (primary_key/foreign_key/attribute), "
        "and possible business_entities (FHIR resources like Location/Organization/Patient/etc.). "
        "Include up to 3 examples per column."
    )
    user = {
        "table": df_name,
        "dtypes": dtypes,
        "head": head_rows
    }

    try:
        # resp: DFColSummary = llm.with_structured_output(DFColSummary,method="function_calling").invoke(
        #     [{"role": "system", "content": system},
        #      {"role": "user", "content": json.dumps(user, ensure_ascii=False)}]
        # )
        # Inside _summarize_dataframe_and_columns
        structured_llm = llm.with_structured_output(DFColSummary, method="function_calling")
        resp: DFColSummary = structured_llm.invoke(
            [{"role": "system", "content": system},
             {"role": "user", "content": json.dumps(user, ensure_ascii=False)}]
        )
        logger.info(f"DEBUG: Raw LLM Object: {resp}")

        df_summary_rich = resp.df_summary
        col_summaries_str = {}
        for col, meta in resp.column_summaries.items():
            tag_entities = f" entities={','.join(meta.business_entities)}" if meta.business_entities else ""
            tag_sem = f" sem={meta.semantic_type}" if meta.semantic_type else ""
            tag_role = f" role={meta.role}" if meta.role else ""
            ex = (meta.examples or [])[:3]
            eg = f" e.g., {', '.join(map(str, ex))}" if ex else ""
            col_summaries_str[col] = f"{meta.description}{tag_entities}{tag_sem}{tag_role}{eg}"
        
        # st.info(df_summary_rich)
        # st.info(col_summaries_str)
        return df_summary_rich, col_summaries_str

    # except Exception as e:
    #     logger.error(f"Summary Error: {e}")
    #     # Fallback: conservative, but we still build reasonable strings
    #     df_summary_fallback = f'{df_name} with {len(df)} rows and {df.shape[1]} columns.'
    #     col_summaries_str = {}
    #     for c in df.columns:
    #         samples = ", ".join(map(str, df[c].dropna().astype(str).head(3).tolist()))
    #         col_summaries_str[c] = f'Column "{c}" (dtype={dtypes.get(c)}), e.g., {samples}'
    #     return df_summary_fallback, col_summaries_str
    except Exception as e:
        logger.error(f"Summary Error: {e}")
        # FALLBACK: If this hits, RAG will likely return 'Unknown' because the context is too weak
        df_summary_fallback = f"Table {df_name} containing health data."
        col_summaries_str = {c: f"Data column {c}" for c in df.columns}
        return df_summary_fallback, col_summaries_str


# Add this helper near the top (under other helpers)
def _search_with_scores(vs, query: str, k: int) -> List[Dict]:
    """Try to get (doc, score); fall back to docs only."""
    try:
        results = vs.similarity_search_with_score(query, k=k)
        print(f"DEBUG: Found {len(results)} matches for {query[:30]}")
        print(f"DEBUG: Vector Search found {len(results)} results for query: {query[:50]}...")
        out = []
        for d, score in results:
            m = d.metadata or {}
            # st.write("DEBUG d.metadata:",d.metadata)
            out.append({
                "resource_name": m.get("resource_name"),
                "fhir_path": m.get("fhir_path"),
                "score": float(score),
                "text": (d.page_content or "")[:300],
            })
        return out
    except Exception:
        print(f"DEBUG: Vector Search Error: {str(e)}")
        docs = vs.similarity_search(query, k=k)
        out = []
        for d in docs:
            m = d.metadata or {}
            out.append({
                "resource_name": m.get("resource_name"),
                "fhir_path": m.get("fhir_path"),
                "score": None,
                "text": (d.page_content or "")[:300],
            })
        return out

# --- New: batch re-run & DF summary update ----------------------------------

def rerun_rag_for_columns(
    state: AgentState,
    *,
    df_name: str,
    col_names: list[str],
    standard: str = "fhir",
    resource_index: str | None = None,
    column_index: str | None = None,
    k_res: int = 5,
    k_col: int = 8,
) -> AgentState:
    """Re-run RAG only for selected columns of a single DF."""
    std = (standard or "fhir").strip().lower()
    label = std.upper()

    res_index = resource_index or f"datacraft.default.cdm_{std}_resource"
    col_index = column_index or f"datacraft.default.cdm_{std}_column"
    resource_vs = DatabricksVectorSearch(index_name=res_index)
    column_vs   = DatabricksVectorSearch(index_name=col_index)

    # Start from current state (user may have edited summaries in the UI)
    df_summary = (state.llm_summaries or {}).get(df_name, "")
    col_summ_all = (state.col_summaries or {}).get(df_name, {}) or {}

    # Copies we’ll mutate
    new_ev = dict(state.rag_evidence or {})
    new_ev.setdefault(df_name, {})
    new_rows = list(state.mapping_rows or [])

    src_df = state.dfs.get(df_name)

    for col in col_names:
        col_summary = col_summ_all.get(col, "")
        samples = []
        if src_df is not None and col in src_df.columns:
            samples = src_df[col].dropna().astype(str).head(3).tolist()

        ctx_json, res_snips, col_snips = _pack_docs_for_llm(
            label, resource_vs, column_vs,
            df_name, df_summary,
            col, col_summary, samples,
            k_res, k_col
        )
        choice = _llm_choose_mapping(ctx_json)

        # update evidence
        new_ev[df_name][col] = {"resource_docs": res_snips, "column_docs": col_snips}

        # upsert mapping_rows
        updated = False
        for r in new_rows:
            if r.get("bronze_table") == df_name and r.get("bronze_columns") == col:
                r["silver_table"] = choice["silver_table"]
                r["silver_column"] = choice["silver_column"]
                updated = True
                break
        if not updated:
            new_rows.append({
                "bronze_table": df_name,
                "bronze_columns": col,
                "silver_table": choice["silver_table"],
                "silver_column": choice["silver_column"],
            })

    messages = state.messages + [
        {"role": "assistant", "name": f"{label}Mapper", "content": f"{label} selective re-run for {df_name}: {len(col_names)} column(s)."}
    ]

    return state.copy(update={
        "rag_evidence": new_ev,
        "mapping_rows": new_rows,
        "fhir_mapping_rows": new_rows,  # keep alias in sync
        "messages": messages,
    })


def update_df_summary(state: AgentState, *, df_name: str, new_summary: str) -> AgentState:
    """Save DF-level llm_summary only (no RAG run)."""
    llm_summ = dict(state.llm_summaries or {})
    llm_summ[df_name] = new_summary
    return state.copy(update={"llm_summaries": llm_summ})

def apply_bulk_manual_mappings(
    state: AgentState,
    *,
    df_name: str,
    assignments: list[dict],  # [{"col_name":"allergy_date","silver_table":"Allergy","silver_column":"allergy_date"}, ...]
) -> AgentState:
    rows = list(state.mapping_rows or [])
    index = {(r.get("bronze_table"), r.get("bronze_columns")): i for i, r in enumerate(rows)}

    for a in assignments:
        col = a.get("col_name")
        stbl = a.get("silver_table") or "Unknown"
        scol = a.get("silver_column") or "Unknown"
        key = (df_name, col)

        if key in index:
            i = index[key]
            rows[i]["silver_table"] = stbl
            rows[i]["silver_column"] = scol
        else:
            rows.append({
                "bronze_table": df_name,
                "bronze_columns": col,
                "silver_table": stbl,
                "silver_column": scol,
            })

    # IMPORTANT: DO NOT sync to fhir_mapping_rows here
    msg = {"role": "assistant", "name": "ManualMapper", "content": f"Manual saves to mapping_rows for {df_name}: {len(assignments)} columns."}
    return state.copy(update={"mapping_rows": rows, "messages": (state.messages or []) + [msg]})


def _pack_docs_for_llm(
    standard_label: str,
    resource_vs: DatabricksVectorSearch,
    column_vs: DatabricksVectorSearch,
    df_name: str,
    df_summary: str,          # already LLM-enriched
    col_name: str,
    col_summary: str,         # already LLM-enriched
    samples: List[str],
    k_res: int,
    k_col: int,
) -> Tuple[str, List[Dict], List[Dict]]:

    # Build a single, rich query string used for both resource & column lookups
    q = f"Table: {df_name}. Summary: {df_summary}. Column: {col_name}. Meaning: {col_summary}."
    if samples:
        q += f" Examples: {', '.join(samples[:3])}."

    # Resource candidates (which FHIR resource/table?)
    res_snips = _search_with_scores(resource_vs, q, k_res)

    # Column candidates (which field/path?)
    col_snips = _search_with_scores(column_vs, q, k_col)

    context = {
        "standard": standard_label,
        "dataframe": df_name,
        "df_summary": df_summary,
        "source_column": col_name,
        "column_summary": col_summary,
        "column_examples": samples[:3] if samples else [],
        "resource_docs": res_snips,
        "column_docs": col_snips,
    }
    return json.dumps(context, ensure_ascii=False), res_snips, col_snips
#fix pran:20250922 for JSON output
from pydantic import BaseModel

class MappingChoice(BaseModel):
    silver_table: str
    silver_column: str


def _llm_choose_mapping(context_json: str) -> Dict:
    """
    Use Groq structured output to ensure clean JSON mapping.
    """
    prompt = f"""
    You are mapping a source column to a FHIR standard resource and column.

    Rules:
    - silver_table must be chosen from resource_docs.resource_name
    - silver_column must be chosen from column_docs.fhir_path
    - Do not copy source table/column names.
    - If no clear match, return "Unknown".

    Context JSON:
    {context_json}
    """

    try:
        resp: MappingChoice = llm.with_structured_output(MappingChoice).invoke(prompt)
        # st.write("DEBUG: LLM structured response:", resp)
        return {"silver_table": resp.silver_table, "silver_column": resp.silver_column}
    except Exception as e:
        # st.write("DEBUG: Exception in _llm_choose_mapping:", str(e))
        return {"silver_table": "Unknown", "silver_column": "Unknown"}

    # content = (getattr(resp, "content", None) or str(resp)).strip()
    # data = _safe_json(content)
    # silver_table = data.get("silver_table") or "Unknown"
    # silver_column = data.get("silver_column") or "Unknown"
    # return {"silver_table": silver_table, "silver_column": silver_column}


# ---- Factory: build a domain-agnostic node --------------------------------


def get_rag_mapper_agent(
    standard: str = "fhir",
    *,
    resource_index: Optional[str] = None,
    column_index: Optional[str] = None,
    k_res: int = 5,
    k_col: int = 8,
):
    std = (standard or "fhir").strip().lower()
    label = std.upper()
    res_index = resource_index or f"datacraft.default.cdm_{std}_resource"
    col_index = column_index or f"datacraft.default.cdm_{std}_column"

    def rag_mapper_agent_node(state: AgentState) -> AgentState:
        # --- 1. STRICT AUTHENTICATION PATCH ---
        # We must set these inside the node to ensure the SDK picks them up
        
        try:
            # Initialize indices inside the node scope
            resource_vs = DatabricksVectorSearch(index_name=res_index)
            column_vs   = DatabricksVectorSearch(index_name=col_index)
            
            # Simple connectivity ping (optional but helpful for logs)
            logger.info(f"[{label}Mapper] Connected to Vector Search indices.")
        except Exception as e:
            logger.error(f"Vector Search Init Error: {e}")
            # We return the state with a message so the UI knows why it failed
            return state.copy(update={
                "messages": state.messages + [{"role": "assistant", "content": f"Auth Failure: {str(e)}"}]
            })

        # --- 2. DATA RECONSTRUCTION ---
        # If state.dfs is empty, we MUST rebuild it from df_heads right here
        working_dfs = state.dfs if state.dfs else {}
        if not working_dfs and state.df_heads:
            logger.info(f"[{label}Mapper] Reconstructing DFs from df_heads inside Agent Node.")
            for table_name, content in state.df_heads.items():
                if isinstance(content, dict) and "data" in content:
                    working_dfs[table_name] = pd.DataFrame(content["data"])
                elif isinstance(content, list):
                    working_dfs[table_name] = pd.DataFrame(content)

        if not working_dfs:
            logger.warning(f"[{label}Mapper] No data found in state.dfs or state.df_heads.")
            return state.copy(update={
                "messages": state.messages + [{"role": "assistant", "content": "Error: No data available to map."}]
            })

        # --- 3. MAPPING LOGIC ---
        llm_summaries, col_summaries = {}, {}
        mapping_rows, rag_evidence = [], {}

        for df_name, df in working_dfs.items():
            logger.info(f"DEBUG: Processing {df_name} with columns: {df.columns.tolist()}")
            df_sum, col_sum = _summarize_dataframe_and_columns(df_name, df)
            logger.info(f"DEBUG: LLM returned {len(col_sum)} columns for mapping")

            time.sleep(2.0)

            llm_summaries[df_name] = df_sum
            col_summaries[df_name] = col_sum
            rag_evidence[df_name] = {}

            for col_name, col_sum_text in col_sum.items():

                logger.info(f"DEBUG: Attempting RAG for column: {col_name}")
                samples = df[col_name].dropna().astype(str).head(3).tolist() if col_name in df.columns else []

                # Perform RAG + LLM call
                try:
                    ctx_json, res_snips, col_snips = _pack_docs_for_llm(
                        label, resource_vs, column_vs,
                        df_name, df_sum, col_name, col_sum_text, samples,
                        k_res, k_col
                    )
                    choice = _llm_choose_mapping(ctx_json)
                    
                    mapping_rows.append({
                        "bronze_table": df_name.lower(),
                        "bronze_columns": col_name,
                        "silver_table": str(choice.get("silver_table", "Unknown")).lower().replace(" ", "_"),
                        "silver_column": str(choice.get("silver_column", "Unknown")).lower().replace(" ", "_"),
                    })
                    
                    rag_evidence[df_name][col_name] = {
                        "resource_docs": res_snips,
                        "column_docs": col_snips
                    }
                except Exception as col_err:
                    logger.error(f"Failed mapping for {df_name}.{col_name}: {col_err}")
                    # ADD THIS FALLBACK:
                    mapping_rows.append({
                        "bronze_table": df_name,
                        "bronze_columns": col_name,
                        "silver_table": "mapping_failed",
                        "silver_column": "check_logs"
                    })

        return state.copy(update={
            "dfs": working_dfs, # Store reconstructed dfs back into state
            "llm_summaries": llm_summaries,
            "col_summaries": col_summaries,
            "mapping_rows": mapping_rows,
            "fhir_mapping_rows": mapping_rows,
            "rag_evidence": rag_evidence,
            "messages": state.messages + [{"role": "assistant", "content": f"Successfully mapped {len(mapping_rows)} columns."}]
        })

    return rag_mapper_agent_node, f"{label} Mapper Agent"





# llm dock pack for custom schema -------



# --- Custom patch for FAISS-based schema retrieval (replaces FHIR retrievers) ---
def _pack_docs_for_llm_custom(label, resource_vs, column_vs,
                              df_name, df_summary,
                              col_name, col_summary, samples,
                              k_res, k_col):
    """
    Specialized _pack_docs_for_llm for FAISS custom schema.
    Retrieves similar target columns/tables from the uploaded schema.
    """

    # Build source query
    q = f"{df_name}.{col_name}: {col_summary}"
    if samples:
        q += f" | samples: {', '.join(samples[:3])}"

    # Use FAISS similarity search directly (not retriever)
    res_hits = resource_vs.similarity_search_with_score(q, k=k_res)
    col_hits = column_vs.similarity_search_with_score(q, k=k_col)

    def _to_snip(docs_with_score):
        snips = []
        for d, score in docs_with_score:
            m = d.metadata or {}
            snips.append({
                "target_table": m.get("target_table"),
                "target_column": m.get("target_column"),
                "description": m.get("description"),
                "score": float(score),
                "text": (d.page_content or "")[:300],
            })
        return snips

    res_snips = _to_snip(res_hits)
    col_snips = _to_snip(col_hits)

    # Prepare structured context for the LLM
    ctx = {
        "source_table": df_name,
        "source_column": col_name,
        "source_summary": col_summary,
        "samples": samples,
        "target_candidates": col_snips,
    }

    return json.dumps(ctx, ensure_ascii=False, indent=2), res_snips, col_snips





# llm choose mapping function --------




def _llm_choose_mapping_custom(context_json: str) -> Dict:
    """
    Use Groq structured output to map source columns to uploaded custom schema.
    """
    prompt = f"""
    You are mapping a source column from a data lake schema to a target column
    in a user-uploaded target schema.

    Rules:
    - silver_table must be chosen from target_candidates.target_table
    - silver_column must be chosen from target_candidates.target_column
    - Prefer the target_column whose description semantically matches the source column context.
    - If no good match is found, return "Unknown" for both.
    - Do NOT create new names; only use from target_candidates.
    
    Context JSON:
    {context_json}
    """

    try:
        resp: MappingChoice = llm.with_structured_output(MappingChoice).invoke(prompt)
        return {
            "silver_table": resp.silver_table or "Unknown",
            "silver_column": resp.silver_column or "Unknown",
        }
    except Exception as e:
        print(" LLM mapping error (custom):", e)
        return {"silver_table": "Unknown", "silver_column": "Unknown"}







# Rag Mapper for custom schema -------





def get_custom_schema_rag_mapper_agent(
    uploaded_schema_path: str,
    k_res: int = 10,
    k_col: int = 15,
):
    """
    Custom schema mapper — mirrors get_rag_mapper_agent but uses a user-uploaded schema
    to build a local FAISS vector store (no Databricks Vector Search dependency).
    The store is passed into existing helpers that expect a VectorStore.
    """

    import json
    import pandas as pd
    from typing import List
    from langchain_core.documents import Document
    from langchain_community.vectorstores import FAISS
    from langchain_community.embeddings import HuggingFaceEmbeddings
    from langchain_community.document_loaders import TextLoader

    std = "custom"
    label = "CUSTOM"

    if not uploaded_schema_path:
        raise ValueError("uploaded_schema_path is required for custom schema upload.")

    # --- Read uploaded schema and build Documents with metadata ---
    docs: List[Document] = []

    def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
        # lower + strip for robustness
        df = df.copy()
        df.columns = [c.strip().lower() for c in df.columns]
        return df

    if uploaded_schema_path.endswith(".csv"):
        df_schema = _norm_cols(pd.read_csv(uploaded_schema_path))
        # Accept common header variants
        possible_tbl = [c for c in ("table_name", "target_table", "table") if c in df_schema.columns]
        possible_col = [c for c in ("column_name", "target_column", "column") if c in df_schema.columns]
        possible_desc = [c for c in ("description", "desc", "details") if c in df_schema.columns]

        if not possible_tbl or not possible_col:
            raise ValueError(
                "CSV must have at least table and column headers "
                "(try: table_name/target_table/table and column_name/target_column/column)."
            )
        tbl_col = possible_tbl[0]
        col_col = possible_col[0]
        desc_col = possible_desc[0] if possible_desc else None

        for _, r in df_schema.iterrows():
            t = str(r.get(tbl_col, "") or "").strip()
            c = str(r.get(col_col, "") or "").strip()
            d = str(r.get(desc_col, "") or "").strip() if desc_col else ""
            text = f"{t}.{c}: {d}" if d else f"{t}.{c}"
            docs.append(
                Document(
                    page_content=text,
                    metadata={"target_table": t, "target_column": c, "description": d},
                )
            )

    elif uploaded_schema_path.endswith(".txt"):
        loader = TextLoader(uploaded_schema_path)
        raw_docs = loader.load()
        # Put entire lines as target docs (no structured metadata available)
        for d in raw_docs:
            docs.append(Document(page_content=d.page_content, metadata={}))
    else:
        raise ValueError("Unsupported schema type. Use CSV or TXT.")

    if not docs:
        raise ValueError("No rows found in uploaded schema file to index.")

    # --- Build FAISS store (keep store + optional retrievers) ---
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    faiss_store = FAISS.from_documents(docs, embeddings)

    # If you later need retrievers for other flows, keep them; but pass the STORE to helpers
    # that expect VectorStore (similarity_search_with_score).
    resource_vs = faiss_store          # NOTE: pass store, not .as_retriever(...)
    column_vs   = faiss_store

    print(f"Built FAISS indexes for custom schema: {uploaded_schema_path} "
          f"({len(docs)} entries)")

    def rag_mapper_agent_node(state: AgentState) -> AgentState:
        if not state.dfs:
            return state.copy(update={
                "messages": state.messages + [
                    {"role": "assistant", "name": f"{label}Mapper", "content": "No DataFrames available in state."}
                ]
            })

        llm_summaries, col_summaries = {}, {}
        mapping_rows = []
        per_df_matches, rag_evidence = {}, {}

        # 1) summarize each DF + columns
        for df_name, df in state.dfs.items():
            df_sum, col_sum = _summarize_dataframe_and_columns(df_name, df)
            llm_summaries[df_name] = df_sum
            col_summaries[df_name] = col_sum

        # 2) per-column RAG → LLM decision
        for df_name, cols in col_summaries.items():
            per_df_matches[df_name] = {}
            rag_evidence[df_name] = {}
            src_df = state.dfs[df_name]

            for col_name, col_sum in cols.items():
                time.sleep(2.0)
                samples = (
                    src_df[col_name].dropna().astype(str).head(3).tolist()
                    if col_name in src_df.columns else []
                )

                # IMPORTANT: pass the VectorStore(s), not retrievers
                ctx_json, res_snips, col_snips = _pack_docs_for_llm_custom(
                    label, resource_vs, column_vs,
                    df_name, llm_summaries[df_name],
                    col_name, col_sum, samples,
                    k_res, k_col
                )

                rag_evidence[df_name][col_name] = {
                    "resource_docs": res_snips,
                    "column_docs":  col_snips,
                }

                choice = _llm_choose_mapping_custom(ctx_json)
                per_df_matches[df_name][col_name] = choice

                mapping_rows.append({
                    "bronze_table": df_name.lower(),
                    "bronze_columns": col_name,
                    "silver_table": choice["silver_table"].lower()
                        .replace(" ", "_").replace("-", "_"),
                    "silver_column": choice["silver_column"].lower()
                        .replace(" ", "_").replace("-", "_"),
                })

        rows_json = json.dumps(mapping_rows, ensure_ascii=False)

        messages = state.messages + [
            {"role": "assistant", "name": f"{label}Mapper",
             "content": f"{label} mapping complete (Custom RAG → LLM)."},
            {"role": "assistant", "name": "RAGMapperRows", "content": rows_json},
            {"role": "assistant", "name": f"{label}MapperRows", "content": rows_json},
        ]

        return state.copy(update={
            "llm_summaries": llm_summaries,
            "col_summaries": col_summaries,
            "custom_column_matches": per_df_matches,
            "mapping_rows": mapping_rows,
            "rag_evidence": rag_evidence,
            "messages": messages,
        })

    desc = (
        f"{label} RAGMapper: Built FAISS vectorstore from uploaded schema "
        f"({uploaded_schema_path}) and mapped using LLM. "
        "Outputs [bronze_table, bronze_columns, silver_table, silver_column]."
    )

    return rag_mapper_agent_node, desc
