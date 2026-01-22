# # app.py
# import os
# from code_generation.run_masking_sql import execute_masking_sql
# from datetime import datetime
# from datetime import datetime
# from databricks.sdk.runtime import dbutils
# # from agent import get_multi_agent  
# from agents.agent_state import AgentState
# from chat_wrapper import LangGraphChatAgent
# from mlflow.types.agent import ChatAgentMessage
# import uuid
# from utils.cleanup import cleanup_volume
# from setup_and_upload.db_utils import create_catalog, create_schema, create_volume_in_schema
# from code_generation.masking_agent import masking_agent
# import glob
# from setup_and_upload.pandas_tools import *
# from utils.table_metadata import fetch_databricks_metadata
# from setup_and_upload.classify_sensitive import classify_column,sanitize_llm_output
# from setup_and_upload.mask import mask_value,apply_masking
# from datalake_design.gold_mapper_agent import gold_mapper_agent_node
# # NEW: Import schema analyzer 
# from business_kpis.analyze_schema_agent import run_schema_analysis_agent
# from business_kpis.analyze_schema_agent import run_kpi_generation_agent
# # from agents.resource_mapper_agent import run_resource_mapper_agent
# # from agents.code_improver_agent import code_improver_agent_node
# from code_generation.bricks_medallion_agent import run_bricks_medallion_agent
# from code_generation.examples_snippets import *
# from databricks import sql


# import .components.v1 as components
# import re
# # import mlflow




# st.title("Intelligent Data Ingestion")
# # st.logo("img/db-logo.jpg") # This sets the app logo in the header/sidebar, not inline.

# # Create columns with vertical_alignment set to "center"
# col_text, col_logo = st.columns([0.2, 1])  # Logo column narrower

# with col_text:
#     st.markdown("**Powered by**")

# with col_logo:
#     st.image("img/Databricks-Emblem.png", width=120)  # smaller width

# # st.markdown(
# #     """
# #     <div style="display: flex; align-items: center; gap: 6px;">
# #         <span style="font-weight: bold; font-size: 12px;">Powered by</span>
# #         <img src="img/Databricks-Emblem.png" width="120">
# #     </div>
# #     """,
# #     unsafe_allow_html=True
# # )

# st.info("Welcome to the multi-agent data assistant.")

# # -- ensure AgentState exists when needed --
# def ensure_agent_context():
#     """
#     Ensure agent context exists with all required fields initialized.
#     Preserves existing state and merges with session state.
#     """
#     prev_ctx = st.session_state.get("agent_context")
    
#     # Define all default values
#     default_state = {
#         "messages": [],
#         "ui_chat_history": [],
#         "next_node": "supervisor",
#         "iteration_count": 0,
#         "dfs": {},
#         "df_heads": {},
#         "df_dtypes": {},
#         "dbfs_path": "",
#         "file_types": {},
#         "xml_root_tags": {},
#         "modeling_sql": "",
#         "modeling_schema_view": "",
#         "pyspark_code": "",
#         "canvas_code": "",
#         "code_history": [],
#         "llm_summaries": {},
#         "col_summaries": {},
#         "rag_evidence": {},
#         "mapping_rows": [],
#         "gold_mapping_rows": [],
#         "kpis": None,
#         "pii_columns": [],
#         "phi_columns": [],
#         "dq_rules": "",
#         "sensitive_metadata": {},
#         "masking_sql": "",
#         "masking_sql_lines": [],
#         "masking_version": 0,
#         "mask_execution_status": "NOT_STARTED",
#         "mask_execution_log": [],
#         "catalog": st.session_state.get("catalog_name", "workspace"),
#         "schema": st.session_state.get("schema_name", "default"),
#         "target_catalog": st.session_state.get("catalog_name", "workspace"),
#         "target_schema": st.session_state.get("schema_name", "default"),
#         "pii_access_mode": st.session_state.get("pii_access_mode", "group"),
#         "pii_access_value": st.session_state.get("pii_access_value", "pii_access"),
#         "phi_access_mode": st.session_state.get("phi_access_mode", "group"),
#         "phi_access_value": st.session_state.get("phi_access_value", "phi_access"),
#         "access_mode": st.session_state.get("access_mode", "group"),
#         "access_value": st.session_state.get("access_value", "pii_access"), 


#     }
    
#     # If previous context exists, preserve all its fields
#     if prev_ctx:
#         for key in default_state:
#             default_state[key] = getattr(prev_ctx, key, default_state[key])
    
#     # Update with latest session state values
#     default_state.update({
#         "ui_chat_history": st.session_state.get("chat_messages", []),
#         "dfs": st.session_state.get("dfs", default_state["dfs"]),
#         "df_heads": st.session_state.get("df_heads", default_state["df_heads"]),
#         "df_dtypes": st.session_state.get("df_dtypes", default_state["df_dtypes"]),
#         "dbfs_path": st.session_state.get("user_dbfs_path") or st.session_state.get("dbfs_path", ""),
#         "file_types": st.session_state.get("file_types", default_state["file_types"]),
#         "xml_root_tags": st.session_state.get("xml_root_tags", {}),
#         "pii_columns": st.session_state.get("pii_columns", default_state["pii_columns"]),
#         "phi_columns": st.session_state.get("phi_columns", default_state["phi_columns"]),
#         "sensitive_metadata": st.session_state.get("sensitive_metadata", default_state["sensitive_metadata"]),
#         "pii_access_mode": st.session_state.get("pii_access_mode", default_state["pii_access_mode"]),
#         "pii_access_value": st.session_state.get("pii_access_value", default_state["pii_access_value"]),
#         "phi_access_mode": st.session_state.get("phi_access_mode", default_state["phi_access_mode"]),
#         "phi_access_value": st.session_state.get("phi_access_value", default_state["phi_access_value"]),
#         "access_mode": st.session_state.get("access_mode", default_state["access_mode"]),
#         "access_value": st.session_state.get("access_value", default_state["access_value"]),

#     })
#     # ---------------------------
#     # Persist dataset summary (for conversational memory)
#     # ---------------------------
#     dfs = default_state.get("dfs") or {}
#     if dfs:
#         default_state["dataset_summary"] = {
#             "tables": list(dfs.keys()),
#             "columns": {
#                 name: list(df.columns)
#                 for name, df in dfs.items()
#                 if hasattr(df, "columns")
#             }
#         }

    
#     st.session_state["agent_context"] = AgentState(**default_state)

# # --- Left Pane: RAG scores + live AgentState --------------------------------
# import pandas as pd


# with st.sidebar:
#     # st.markdown("### 📚 Document RAG (Groq)")

#     # Initialize RAG once
#     if "rag_helper" not in st.session_state:
#         st.session_state["rag_helper"] = None

#     # uploaded_doc = st.file_uploader("Upload a PDF or TXT document for RAG", type=["pdf", "txt"])

#     # if uploaded_doc is not None:
#     #     tmp_path = os.path.join("/tmp", uploaded_doc.name)
#     #     with open(tmp_path, "wb") as f:
#     #         f.write(uploaded_doc.getbuffer())

#     #     st.session_state["rag_helper"] = RAGHelper(tmp_path)
#     #     st.success(f"Document loaded: {uploaded_doc.name}")

#     # if st.session_state.get("rag_helper"):
#     #     query = st.text_input("Ask a question (prefix optional with 'rag:')")
#     #     if st.button("Run RAG Query"):
#     #         with st.spinner("Querying document via Groq..."):
#     #             try:
#     #                 answer = st.session_state["rag_helper"].query(query)
#     #                 st.markdown(f"**Answer:** {answer}")
#     #                 # Optional: Save RAG evidence into AgentState for reuse
#     #                 ctx = st.session_state.get("agent_context")
#     #                 if ctx:
#     #                     ctx.rag_evidence["custom_doc"] = {
#     #                         "query": query,
#     #                         "answer": answer
#     #                     }
#     #                     st.session_state["agent_context"] = ctx
#     #             except Exception as e:
#     #                 st.error(f"RAG query failed: {e}")



#     # st.markdown("### 🔎 RAG Match Scores & Edits")

#     # ctx = st.session_state.get("agent_context")
#     # if not ctx:
#     #     st.info("Upload data and run the mapper.")
#     # else:
#     #     ctxd = ctx.dict() if hasattr(ctx, "dict") else dict(ctx)
#     #     evidence = ctxd.get("rag_evidence", {}) or {}
#     #     llm_summ = ctxd.get("llm_summaries", {}) or {}
#     #     col_summ = ctxd.get("col_summaries", {}) or {}

#     #     if not evidence:
#     #         st.info("Run **RAG Mapper** to see scores.")
#     #     else:
#     #         df_names = sorted(evidence.keys())
#     #         sel_df = st.selectbox("DataFrame", df_names, key="sb_df")

#     #         # --- DF-level summary editor ---
#     #         st.markdown("**DataFrame summary (llm_summaries)**")
#     #         df_summary_val = st.text_area(
#     #             f"{sel_df} summary",
#     #             value=llm_summ.get(sel_df, ""),
#     #             key=f"dfsum_{sel_df}",
#     #             height=90
#     #         )
#     #         if st.button("Save Save DF summary", key="btn_save_df_sum"):
#     #             from agents.rag_mapper_agent import update_df_summary
#     #             st.session_state["agent_context"] = update_df_summary(
#     #                 st.session_state["agent_context"], df_name=sel_df, new_summary=df_summary_val
#     #             )
#     #             st.success("Saved DF summary.")

#     #         # --- Multi-select columns & per-column editors ---
#     #         col_names = sorted(evidence[sel_df].keys())
#     #         picked_cols = st.multiselect("Columns to review / re-run", col_names, default=col_names[:1], key="sb_cols")

#     #         # Per-column text areas to edit col_summaries
#     #         edited = {}
#     #         for c in picked_cols:
#     #             st.caption(f"**{c}** — column summary")
#     #             edited[c] = st.text_area(
#     #                 f"Summary for {c}",
#     #                 value=(col_summ.get(sel_df, {}) or {}).get(c, ""),
#     #                 key=f"cols_{sel_df}_{c}",
#     #                 height=110
#     #             )

#     #             # Show current evidence tables (optional but handy)
#     #             import pandas as pd
#     #             pack = evidence[sel_df][c]
#     #             res_hits_df = pd.DataFrame(pack.get("resource_docs", []))
#     #             col_hits_df = pd.DataFrame(pack.get("column_docs", []))
#     #             # if not res_hits_df.empty:
#     #             #     # st.dataframe(res_hits
#     #             #     # _df[["resource_name","fhir_path","score","text"]], use_container_width=True)
#     #             # if not col_hits_df.empty:
#     #             #     # st.dataframe(col_hits_df[["resource_name","fhir_path","score","text"]], use_container_width=True)

#     #         # --- Save summaries (no re-run) ---
#     #         if st.button("Save Save column summaries (no re-run)", key="btn_save_cols_only"):
#     #             # persist into AgentState
#     #             ctx.col_summaries.setdefault(sel_df, {})
#     #             for c, txt in edited.items():
#     #                 ctx.col_summaries[sel_df][c] = txt
#     #             # DF summary too (in case user edited both)
#     #             ctx.llm_summaries[sel_df] = df_summary_val
#     #             st.session_state["agent_context"] = ctx
#     #             st.success("Saved summaries.")

#     #         # --- Save & re-run RAG for selected columns only ---
#     #         if st.button("Re-run Save & Re-run for selected columns", key="btn_rerun_cols"):
#     #             from agents.rag_mapper_agent import rerun_rag_for_columns, update_df_summary
#     #             # Save edits first
#     #             ctx = st.session_state["agent_context"]
#     #             ctx.col_summaries.setdefault(sel_df, {})
#     #             for c, txt in edited.items():
#     #                 ctx.col_summaries[sel_df][c] = txt
#     #             ctx.llm_summaries[sel_df] = df_summary_val
#     #             st.session_state["agent_context"] = ctx

#     #             # Re-run only for picked columns
#     #             new_state = rerun_rag_for_columns(
#     #                 st.session_state["agent_context"],
#     #                 df_name=sel_df,
#     #                 col_names=picked_cols,
#     #                 standard=st.session_state.get("selected_standard", "fhir"),
#     #             )
#     #             st.session_state["agent_context"] = new_state
#     #             st.success(f"Re-ran RAG for {len(picked_cols)} column(s) in {sel_df}.")

#     #             # Optional: refresh center mapping grid if present
#     #             ctxd2 = new_state.dict() if hasattr(new_state, "dict") else dict(new_state)
#     #             mappings = ctxd2.get("mapping_rows") or ctxd2.get("fhir_mapping_rows") or []
#     #             if mappings:
#     #                 import pandas as pd
#     #                 st.session_state["silver_mappings_df"] = pd.DataFrame(mappings)

#     # ... keep your existing RAG UI above ...

#     # st.markdown("---")
#     # st.markdown("### Manual Manual mapping (after RAG)")
    
#     # ctx = st.session_state.get("agent_context")
#     # if ctx and getattr(ctx, "dfs", None):
#     #     ctxd = ctx.dict() if hasattr(ctx, "dict") else dict(ctx)

#     #     # Choose DF (reuse sel_df if you already have one above; else compute fresh)
#     #     df_names = sorted((ctxd.get("dfs") or {}).keys())
#     #     sel_df_manual = st.selectbox("DataFrame (manual)", df_names, key="manual_df")

#     #     # Build a small editor: list all source columns; let user tick which to save
#     #     src_cols = []
#     #     try:
#     #         dfs_dict = ctxd.get("dfs", {}) or {}
#     #         if sel_df_manual in dfs_dict and dfs_dict[sel_df_manual] is not None:
#     #             src_cols = list(dfs_dict[sel_df_manual].columns)
#     #     except Exception:
#     #         # if dfs holds heads not full DFs, fall back to rag_evidence keys
#     #         ev = (ctxd.get("rag_evidence", {}) or {}).get(sel_df_manual, {}) or {}
#     #         src_cols = sorted(ev.keys())

#     #     import pandas as pd
#     #     existing = pd.DataFrame(ctxd.get("mapping_rows") or [])
#     #     existing = existing[(existing["bronze_table"] == sel_df_manual)] if not existing.empty else pd.DataFrame()

#     #     # Pre-fill a grid: one row per source col; allow editing of silver fields and selecting rows to save
#     #     rows = []
#     #     for c in src_cols:
#     #         # read current saved mapping if any
#     #         if not existing.empty:
#     #             r = existing[existing["bronze_columns"] == c]
#     #             stbl = r["silver_table"].iloc[0] if not r.empty else ""
#     #             scol = r["silver_column"].iloc[0] if not r.empty else ""
#     #         else:
#     #             stbl = ""
#     #             scol = ""
#     #         rows.append({"select": False, "bronze_columns": c, "silver_table": stbl, "silver_column": scol})

#     #     grid_df = pd.DataFrame(rows)
#     #     edited_df = st.data_editor(
#     #         grid_df,
#     #         hide_index=True,
#     #         use_container_width=True,
#     #         column_config={
#     #             "select": st.column_config.CheckboxColumn("Save?"),
#     #             "bronze_columns": st.column_config.TextColumn("Source column", disabled=True),
#     #             "silver_table": st.column_config.TextColumn("Target table"),
#     #             "silver_column": st.column_config.TextColumn("Target column"),
#     #         },
#     #         key=f"manual_editor_{sel_df_manual}",
#     #     )

#     #     if st.button("Save selected to mapping_rows", key="btn_manual_save"):
#     #         from agents.rag_mapper_agent import apply_bulk_manual_mappings

#     #         # Collect only checked rows
#     #         picks = []
#     #         for _, r in edited_df.iterrows():
#     #             if bool(r["select"]):
#     #                 # require at least a target column; table can be blank or a custom name
#     #                 picks.append({
#     #                     "col_name": r["bronze_columns"],
#     #                     "silver_table": (r.get("silver_table") or "").strip() or "custom",
#     #                     "silver_column": (r.get("silver_column") or "").strip() or r["bronze_columns"],
#     #                 })

#     #         if not picks:
#     #             st.warning("No rows selected.")
#     #         else:
#     #             st.session_state["agent_context"] = apply_bulk_manual_mappings(
#     #                 st.session_state["agent_context"],
#     #                 df_name=sel_df_manual,
#     #                 assignments=picks,
#     #             )
#     #             st.success(f"Saved {len(picks)} mapping(s) to mapping_rows (no RAG, no FHIR).")

#     #             # Optional: refresh the center grid if you show it
#     #             ctx2 = st.session_state["agent_context"]
#     #             ctxd2 = ctx2.dict() if hasattr(ctx2, "dict") else dict(ctx2)
#     #             mappings = ctxd2.get("mapping_rows") or []
#     #             if mappings:
#     #                 st.session_state["silver_mappings_df"] = pd.DataFrame(mappings)


#     # st.markdown("---")
#     # st.markdown("### 🧠 Agent State (live)")

#     # if ctx:
#     #     # Small summary
#     #     st.json({
#     #         "dfs": list((ctxd.get("dfs") or {}).keys()),
#     #         "mapping_rows": len(ctxd.get("mapping_rows") or []),
#     #         "messages": len(ctxd.get("messages") or []),
#     #         "next_node": ctxd.get("next_node"),
#     #         "iteration_count": ctxd.get("iteration_count"),
#     #         "llm_summaries": ctxd.get("llm_summaries"),
#     #         "col_summaries": ctxd.get("col_summaries"),
#     #         "rag_evidence": ctxd.get("rag_evidence")
#     #     })
#         # with st.expander("Full AgentState JSON"):
#         #     st.json(ctxd)

#     # if mlflow.active_run():
#     #     run = mlflow.active_run()
#     #     st.markdown("**Active MLflow Run**")
#     #     st.write(f"Run ID: {run.info.run_id}")
#     #     st.write(f"Experiment ID: {run.info.experiment_id}")
#     # else:
#     #     st.info("No active MLflow run.")
    
#     st.title("🔍 Databricks Table Metadata Viewer")
#     st.markdown("Enter your Databricks details to view tables and their columns (with data types).")

#     catalog = st.text_input("Catalog Name", value="workspace")
#     schema = st.text_input("Schema Name", value="default")
#     pat_token = st.text_input("Personal Access Token (PAT)", type="password")

#     st.markdown("---")

#     # --- Main Button ---
#     if st.button("Show Tables and Schema"):
#         if not catalog or not schema or not pat_token:
#             st.error("Please fill all fields before fetching metadata.")
#         else:
#             with st.spinner("Fetching metadata..."):
#                 try:
#                     df = fetch_databricks_metadata(
                    
#                         token=pat_token,
#                         catalog=catalog,
#                         schema=schema
#                     )
#                     st.dataframe(df, use_container_width=True)
#                     st.success(f"Found {len(df)} tables under {catalog}.{schema}")

#                 except RuntimeError as e:
#                     #st.error("Incorrect Databricks details. Please verify and try again.")
#                     st.error(str(e))  
#     st.markdown("---")

# if "log_messages" not in st.session_state:
#     st.session_state.log_messages = []

# from datetime import datetime

# def log_message(msg: str):
#     if "log_messages" not in st.session_state or not isinstance(st.session_state.log_messages, dict):
#         st.session_state.log_messages = {}

#     timestamp = datetime.now().strftime("%H:%M:%S")

#     # Avoid duplicates by message content
#     if msg not in st.session_state.log_messages.values():
#         st.session_state.log_messages[timestamp] = msg


# def show_log():
#     if "log_messages" in st.session_state:
#         with st.expander("📜 Log Messages"):
#             for ts, msg in st.session_state.log_messages.items():
#                 st.markdown(f"`[{ts}]` {msg}")


# menu_choice = st.sidebar.radio("Navigate", ["Home", "Cleanup"])

# if menu_choice == "Home":
#     # st.title("🏠 Datacraft Agent Home")
#     st.write("Welcome to the multi-agent data assistant.")

# elif menu_choice == "Cleanup":
#     st.title("\U0001F9F9 Cleanup")
#     if st.button("Run Cleanup"):
#         cleanup_volume()
#         st.success("Cleanup completed.")

# # Step 1: File Upload
# with st.container():
#     st.subheader("Step 1: File Upload")

    
#     col1, col2, col3 = st.columns(3)

#     with col1:
#         catalog_name = st.text_input("Enter Catalog Name:", "", key="catalog_input")  # Catalog input

#     with col2:
#         schema_name = st.text_input("Enter Schema Name:", "", key="schema_input")    # Schema input

#     with col3:
#         volume_name = st.text_input("Enter Volume Name:", "", key="volume_input")     # Volume input

#     token = st.text_input("Enter your Databricks Personal Access Token:", type="password")
#     # persist for reuse elsewhere
#     if token:
#         st.session_state["user_pat_token"] = token
#     # keep any existing saved token if user didn't enter in this run
#     st.session_state.setdefault("user_pat_token", st.session_state.get("user_pat_token", ""))


#     # Step 2: Create Catalog, Schema, and Volume
#     if st.button("Create Catalog, Schema, and Volume"):
#         if catalog_name and schema_name and volume_name and token:
#             create_catalog(catalog_name, token)
#             create_schema(catalog_name, schema_name, token)
#             create_volume_in_schema(catalog_name, schema_name, volume_name, token)

#             st.success(f"Catalog '{catalog_name}', Schema '{schema_name}', and Volume '{volume_name}' created successfully.")
#         else:
#             st.error("Please provide all fields (Catalog, Schema, Volume, and Token).")

#     # Handle file uploads
    
#     uploaded_files = st.file_uploader(
#         "Upload CSV, Excel, Parquet,XML or JSON files",
#         type=["csv", "xlsx", "parquet", "json", "xml"],
#         accept_multiple_files=True
#     )
    

#     if uploaded_files:
#         # Validate catalog/schema/volume are provided
#         if not (catalog_name and schema_name and volume_name):
#             st.error("Please enter Catalog, Schema, and Volume names, then click 'Create Catalog, Schema, and Volume' before uploading files.")
#             st.stop()
        
#         current_dir = os.path.dirname(os.path.abspath(__file__))
#         UPLOAD_DIR = os.path.join(current_dir, "uploaded_files")
#         os.makedirs(UPLOAD_DIR, exist_ok=True)

#         all_dbfs_paths = []
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         folder = f"upload_{timestamp}"

#         # Default DBFS path for the uploaded files
#         dbfs_base_path = f"/Volumes/datacraft/default/data_uploads/{folder}"

#         # Use user-specified DBFS base path
#         user_dbfs_base_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{folder}"

        
#         dbutils.fs.mkdirs(f"dbfs:{dbfs_base_path}")
#         dbutils.fs.mkdirs(f"dbfs:{user_dbfs_base_path}")

#         for f in glob.glob("/tmp/*"):
#             try:
#                 os.remove(f)
#             except Exception as e:
#                 print(f"Error removing {f}: {e}")
        
#         file_types = {}

#         for uploaded_file in uploaded_files:
#             filename = uploaded_file.name
#             ext = os.path.splitext(filename)[1].lower()  
#             file_types[filename] = ext.lstrip('.').upper()
#             local_path = os.path.join(UPLOAD_DIR, filename)

#             if os.path.exists(local_path):
#                 os.remove(local_path)

#             with open(local_path, "wb") as f:
#                 f.write(uploaded_file.getbuffer())

#             if ext == ".xlsx":
#                 try:
#                     # Convert Excel to CSV locally
#                     df = pd.read_excel(local_path, engine="openpyxl")
#                     csv_name = filename.replace(".xlsx", ".csv")
#                     csv_local_path = os.path.join(UPLOAD_DIR, csv_name)
#                     df.to_csv(csv_local_path, index=False)
#                     log_message(f"Converted {filename} → {csv_name}")
                    
#                     # Use converted CSV path and name for upload
#                     upload_filename = csv_name
#                     upload_local_path = csv_local_path
#                     file_types[csv_name] = "CSV"
#                 except Exception as e:
#                     st.error(f"Failed to convert {filename} to CSV: {e}")
#                     continue
#             else:
#                 # For non-xlsx files, upload as is
#                 upload_filename = filename
#                 upload_local_path = local_path

#             log_message(f"File ready for upload: {upload_local_path}")

#             dbfs_path_default = f"{dbfs_base_path}/{upload_filename}"  # Use converted CSV name here
#             dbfs_path_user = f"{user_dbfs_base_path}/{upload_filename}"

#             # Upload the converted CSV file or original file
#             dbutils.fs.cp(f"file:{upload_local_path}", dbfs_path_default)
#             dbutils.fs.cp(f"file:{upload_local_path}", dbfs_path_user)

#             all_dbfs_paths.append((dbfs_path_default, dbfs_path_user))


   
#         st.session_state["dbfs_path"] = dbfs_base_path
#         st.session_state["user_dbfs_path"] = user_dbfs_base_path
#         st.session_state["file_types"] = file_types
#         log_message(f"All files copied to DBFS: {dbfs_base_path} and {user_dbfs_base_path}")

#     # Step 2.1: Load to DataFrames
#     # Use user-specified path if available, otherwise fall back to default
#     active_dbfs_path = st.session_state.get("user_dbfs_path") or st.session_state.get("dbfs_path")
    
#     if active_dbfs_path:
#         with st.spinner("Reading uploaded files into DataFrames..."):
#             dfs, xml_root_tags = read_all_files_from_dbfs(active_dbfs_path)
#             st.session_state["dfs"] = dfs
#             st.session_state["df_heads"] = get_dataframes_head(dfs, n=5)
#             st.session_state["df_dtypes"] = get_dataframes_dtypes(dfs)
#             st.session_state["xml_root_tags"] = xml_root_tags

#         log_message(f"{len(dfs)} file(s) loaded into memory.")
      


#         # Display preview of the loaded files
#         for name, df_head in st.session_state["df_heads"].items():
#             st.markdown(f"### `{name}` — Preview")

#             # Sensitivity options
#             options = ["NON_SENSITIVE", "PII", "PCI", "PHI"]
#             state_key = f"tags_{name}"

#             # Initializing the classification

#             if state_key not in st.session_state:
#                 tag_row={}
#                 for col in df_head.columns:
#                     tag_row[col] = sanitize_llm_output(classify_column(col))
#                 st.session_state[state_key] = pd.DataFrame([tag_row])
            
#             # Loading from session state 

#             tag_df = st.session_state[state_key]

            
#             # ---- TABLE 1: Editable Sensitivity Tags ----
#             st.subheader("Sensitivity Classification (Editable)")
                    
#             edited_tags = st.data_editor(
#                 tag_df,
#                 hide_index=False,
#                 column_config={
#                     col: st.column_config.SelectboxColumn(options=options)
#                     for col in tag_df.columns
#                 },
#                 key=f"tag_editor_{name}"
#             )

#             # Save updated table to session_state
#             st.session_state[state_key] = edited_tags
#             st.session_state["bronze_sensitivity_map"] = edited_tags.iloc[0].to_dict()
#             st.session_state["bronze_version"] = st.session_state.get("bronze_version", 0) + 1
#             ensure_agent_context()
#             ctx = st.session_state["agent_context"]
#             smeta = getattr(ctx, "sensitive_metadata", {}) or {}
#             smeta.setdefault("bronze", {})
#             smeta["bronze"][name] = dict(st.session_state.get("bronze_sensitivity_map", {}) or {})

            
#             pii = [c for t in smeta["bronze"] for c,tag in smeta["bronze"][t].items() if tag == "PII"]
#             phi = [c for t in smeta["bronze"] for c,tag in smeta["bronze"][t].items() if tag == "PHI"]

#             ctx.sensitive_metadata = smeta
#             ctx.pii_columns = sorted(set(pii))
#             ctx.phi_columns = sorted(set(phi))
#             st.session_state["agent_context"] = ctx
#             # ---- TABLE 2: Data Preview ----
#             st.subheader("Data Preview (Non-Editable)")

#             mask_data = st.toggle("Enable Masking (Hide Sensitive Data)", key=f"mask_toggle_{name}")

#             # Extract the latest sensitivity map
#             bronze_map = st.session_state["bronze_sensitivity_map"]

#             if mask_data:
#                 masked_df = apply_masking(df_head, bronze_map)
#                 st.dataframe(masked_df)
#             else: 
#                 st.dataframe(df_head)

#             # def highlight_first_row(row):
#             #     if row.name == 0:
#             #         return ['background-color: #ffffff; color: #000000; font-weight: bold;'] * len(row)
#             #     return [''] * len(row)


#             # st.dataframe(preview_df.style.apply(highlight_first_row, axis=1))

            
        

# with st.container():
#     st.subheader("Step 2:Data Lake Design:")
#     domain_choice = st.selectbox(
#     "Select industry specific mapping standard",
#     ["FHIR", "ACORD", "X12","Upload Custom Schema"],
#     index=0
#     )
#     st.session_state["selected_standard"] = domain_choice.lower()

#     # Handle custom schema upload
  
#     uploaded_schema_path = st.session_state.get("uploaded_schema_path", None)
#     if st.session_state["selected_standard"] == "upload custom schema":
#         with st.expander("Upload Custom Schema", expanded=True):
#             uploaded_file = st.file_uploader(
#                 "Upload your custom schema file (CSV, TSV, Excel, or Parquet)",
#                 type=["csv", "tsv", "xlsx", "parquet", "txt"],
#                 accept_multiple_files=False
#             )

#         if uploaded_file is not None:
#             st.success(f" {uploaded_file.name} uploaded successfully!")
#             tmp_path = f"/tmp/{uploaded_file.name}"
#             with open(tmp_path, "wb") as f:
#                 f.write(uploaded_file.getbuffer())
#             uploaded_schema_path = tmp_path
#             st.session_state["uploaded_schema_path"] = tmp_path
#         else:
#             st.info("Please upload a schema file to continue.")

#     # Step 2.3: Build the correct RAG mapper node
#     from datalake_design.rag_mapper_agent import get_rag_mapper_agent
#     from datalake_design.rag_mapper_agent import get_custom_schema_rag_mapper_agent 

#     if st.session_state["selected_standard"] == "upload custom schema":
#         if uploaded_schema_path:
#             node, desc = get_custom_schema_rag_mapper_agent(
#                 uploaded_schema_path=uploaded_schema_path
#             )
#             st.session_state["rag_mapper_node"] = node
#             st.success("Custom RAG Mapper initialized successfully.")
#         else:
#             st.warning("Please upload a custom schema before continuing.")
#     else:
#         node, _ = get_rag_mapper_agent(standard=st.session_state["selected_standard"])
#         st.session_state["rag_mapper_node"] = node

#     import pandas as pd
#     with st.expander("Generate Bronze to Silver Mappings:", expanded=True):
        
#         def generate_silver_mappings():
#             ensure_agent_context()
#             if not st.session_state["agent_context"].dfs:
#                 st.session_state["silver_mapping_warning"] = "Please upload and load data first."
#                 return

#             mapper_node = st.session_state["rag_mapper_node"]
#             st.session_state["agent_context"] = mapper_node(st.session_state["agent_context"])
#             ctx = st.session_state["agent_context"]
#             ctx_dict = ctx.dict() if hasattr(ctx, "dict") else dict(ctx)

#             # Decide which mapping to use based on the selected standard
#             if st.session_state["selected_standard"] == "upload custom schema":
#                 mappings = ctx_dict.get("mapping_rows", [])
#             else:
#                 mappings = ctx_dict.get("mapping_rows") or ctx_dict.get("fhir_mapping_rows") or []
#             if mappings:
#                 df = pd.DataFrame(mappings)

#                 # Add classification column based on the relevant column name
#                 # (adjust the column key as per your mapping schema — e.g., 'target_column' or 'column_name')
#                 target_col_key = "bronze_columns" if "bronze_columns" in df.columns else "column_name"

#                 bronze_map = st.session_state["bronze_sensitivity_map"]
#                 df["classification"] = df[target_col_key].map(bronze_map).fillna("NON_SENSITIVE")


#                 st.session_state["silver_mappings_df"] = df

#                 # --- propagate silver classifications into agent_context (paste after 
#                 ensure_agent_context()
#                 ctx = st.session_state["agent_context"]
#                 smeta = getattr(ctx, "sensitive_metadata", {}) or {}
#                 smeta.setdefault("silver", {})

#                 # df expected columns: 'bronze_columns','silver_table','silver_column','classification'
#                 for _, row in df.iterrows():
#                     stbl = row.get("silver_table") or "<unknown_silver>"
#                     scol = row.get("silver_column") or row.get("bronze_columns")
#                     cls = row.get("classification") or "NON_SENSITIVE"

#                     smeta["silver"].setdefault(stbl, {})
#                     prev = smeta["silver"][stbl].get(scol)  
#                     sev = {"NON_SENSITIVE":0,"PII":1,"PHI":2}
#                     chosen = prev if prev and sev.get(prev,0) >= sev.get(cls,0) else cls
#                     smeta["silver"][stbl][scol] = chosen

                
#                 pii, phi = [], []
#                 for layer in ("bronze","silver"):
#                     for t, cols in (smeta.get(layer) or {}).items():
#                         for col, tag in cols.items():
#                             if tag == "PII": pii.append(col)
#                             if tag == "PHI": phi.append(col)

#                 ctx.sensitive_metadata = smeta
#                 ctx.pii_columns = sorted(set(pii))
#                 ctx.phi_columns = sorted(set(phi))
#                 st.session_state["agent_context"] = ctx
#                 # --- end silver propagation ---

#                 #  UPDATE SILVER VERSION
#                 st.session_state["silver_version"] = st.session_state.get("silver_version", 0) + 1

#                 st.session_state["silver_mapping_success"] = "Mapping complete."
#                 st.session_state["silver_mapping_warning"] = None
#             else:
#                 st.session_state["silver_mappings_df"] = None
#                 st.session_state["silver_mapping_warning"] = "No mappings found."


        
#         # Button triggers the callback once; results live in session_state
#         st.button("Generate Silver Mappings", on_click=generate_silver_mappings, key="btn_generate_silver")

#         # Always render from state
#         if st.session_state.get("silver_mapping_warning"):
#             st.warning(st.session_state["silver_mapping_warning"])
#         elif st.session_state.get("silver_mapping_success"):
#             st.success(st.session_state["silver_mapping_success"])
#             if st.session_state.get("silver_mappings_df") is not None:
#                 st.dataframe(st.session_state["silver_mappings_df"], use_container_width=True)
#             else:
#                 st.info("No mappings found.")

# # Step3.2: render dimensionl model : end

# # Step 2.5: Gold Layer Mapping
# with st.container():
#     st.subheader("Step 2.5: Gold Layer Mapping")
#     st.markdown("### Generate Silver → Gold Mappings")
#     with st.expander("Map Silver columns to analytical Gold tables", expanded=True):
#         from datalake_design.gold_mapper_agent import gold_mapper_agent_node
#         import pandas as pd
        
#         def generate_gold_mappings():
#             ensure_agent_context()
#             ctx = st.session_state["agent_context"]
            
#             if not getattr(ctx, "mapping_rows", []):
#                 st.session_state["gold_mapping_warning"] = "Please generate Silver mappings first (Step 2)."
#                 return
            
#             with st.spinner("Generating Gold layer mappings..."):
#                 ctx = gold_mapper_agent_node(ctx)
#                 st.session_state["agent_context"] = ctx
                
#                 gold_mappings = getattr(ctx, "gold_mapping_rows", [])
#                 if gold_mappings:
#                     df = pd.DataFrame(gold_mappings)

#                     # Add classification column
#                     target_col_key = "silver_column" if "silver_column" in df.columns else "column_name"
#                     silver_df = st.session_state["silver_mappings_df"]
#                     silver_map = dict(zip(silver_df["silver_column"], silver_df["classification"]))

#                     df["classification"] = df[target_col_key].map(silver_map).fillna("NON_SENSITIVE")

#                     st.session_state["gold_mappings_df"] = df

#                     # --- propagate gold classifications into agent_context ---
#                     ensure_agent_context()
#                     ctx = st.session_state["agent_context"]
#                     smeta = getattr(ctx, "sensitive_metadata", {}) or {}
#                     smeta.setdefault("gold", {})

#                     target_table_key = "target_table" if "target_table" in df.columns else ("gold_table" if "gold_table" in df.columns else "table")
#                     target_col_key2 = "target_column" if "target_column" in df.columns else ("gold_column" if "gold_column" in df.columns else "column_name")
#                     source_key = "silver_column" if "silver_column" in df.columns else "silver_column"

#                     for _, row in df.iterrows():
#                         gtable = row.get(target_table_key) or "gold_unknown"
#                         gcol = row.get(target_col_key2) or row.get(source_key)
#                         cls = row.get("classification") or "NON_SENSITIVE"
#                         smeta["gold"].setdefault(gtable, {})
#                         prev = smeta["gold"][gtable].get(gcol)
#                         sev = {"NON_SENSITIVE":0,"PII":1,"PHI":2}
#                         chosen = prev if prev and sev.get(prev,0) >= sev.get(cls,0) else cls
#                         smeta["gold"][gtable][gcol] = chosen

#                     # flatten all bronze+silver+gold tags for the DQ agent
#                     pii, phi = [], []
#                     for layer in ("bronze","silver","gold"):
#                         for t, cols in (smeta.get(layer) or {}).items():
#                             for c, tag in cols.items():
#                                 if tag == "PII": pii.append(c)
#                                 if tag == "PHI": phi.append(c)

#                     ctx.sensitive_metadata = smeta
#                     ctx.pii_columns = sorted(set(pii))
#                     ctx.phi_columns = sorted(set(phi))
#                     st.session_state["agent_context"] = ctx
#                     # --- end gold propagation ---

#                     st.session_state["gold_mapping_warning"] = None
#                     st.session_state["gold_mapping_success"] = f"Generated {len(gold_mappings)} Gold mappings."
#                     st.session_state["gold_version"] = st.session_state.get("gold_version", 0) + 1

#                 else:
#                     st.session_state["gold_mappings_df"] = None
#                     st.session_state["gold_mapping_warning"] = "No Gold mappings found."

         
#         # Button triggers the callback once; results live in session_state
#         st.button("Generate Gold Mappings", on_click=generate_gold_mappings, key="btn_generate_gold")
        
#         # Always render from state (persists across interactions)
#         if st.session_state.get("gold_mapping_warning"):
#             st.warning(st.session_state["gold_mapping_warning"])
#         elif st.session_state.get("gold_mapping_success"):
#             st.success(st.session_state["gold_mapping_success"])
#             if st.session_state.get("gold_mappings_df") is not None:
#                 st.dataframe(st.session_state["gold_mappings_df"], use_container_width=True)
#             else:
#                 st.info("No Gold mappings found.")

# # New Button Section: Analyze Schema
# with st.container():
#     st.subheader("Step 3: Business KPIs")
#     st.markdown("### Business KPI Suggestions")
#     with st.expander("Business Insights & KPI Suggestions", expanded=True):
#         col1, col2 = st.columns([1, 4])
#         with col1:
#             pass
#         with col2:
#             st.markdown(
#                 "Start by detecting Business Domain<br>Then choose an area of interest within the domain to generate KPIs",
#                 unsafe_allow_html=True,
#             )

#         if st.button("Analyze Schema"):
#             if "dfs" in st.session_state:
#                 with st.spinner("Analyzing schema with agent..."):
#                     try:
#                         insights = run_schema_analysis_agent(st.session_state["df_heads"])
#                         st.session_state["schema_insights"] = insights

#                         # Extract domain and areas from response
#                         domain_line = [line for line in insights.splitlines() if line.startswith("Domain:")]
#                         area_line = [line for line in insights.splitlines() if line.startswith("Areas:")]

#                         if domain_line:
#                             st.session_state["business_domain"] = domain_line[0].replace("Domain:", "").strip()

#                         if area_line:
#                             areas_str = area_line[0].replace("Areas:", "").strip()
#                             st.session_state["area_options"] = [area.strip() for area in areas_str.split(",")]

#                         st.success("Schema analysis complete.")
#                     except Exception as e:
#                         st.error(f"Agent failed to analyze schema: {e}")
#             else:
#                 st.warning("Please upload and load data files first.")

#         # Display domain and dropdown if schema was analyzed
#         if "business_domain" in st.session_state and "area_options" in st.session_state:
#             st.markdown(f"**Detected Domain:** {st.session_state['business_domain']}")
#             selected_area = st.selectbox("Select Area for KPI Suggestions", st.session_state["area_options"])

#             if st.button("Generate KPIs for Selected Area"):
#                 with st.spinner(f"Generating KPIs for area: {selected_area}"):
#                     try:
#                         kpi_output = run_kpi_generation_agent(
#                             st.session_state["df_heads"],
#                             st.session_state["business_domain"],
#                             selected_area
#                         )
#                         st.success("Top 10 KPIs (DAX Format):")
#                         st.code(kpi_output)
#                         ctx = st.session_state["agent_context"]
#                         ctx.kpis = kpi_output
#                         st.session_state["agent_context"] = ctx
                        
#                     except Exception as e:
#                         st.error(f"Failed to generate KPIs: {e}")




# with st.container():
#     st.subheader("Step 4: Generate Medallion PySpark Code")
#     st.markdown("### End-to-End PySpark Pipeline (Bronze → Silver → Gold)")

#     # Show preview of DQ expectations before generating code
#     with st.expander("Preview Data Quality Expectations", expanded=False):
#         ensure_agent_context()
#         ctx = st.session_state.get("agent_context")
#         if ctx and getattr(ctx, "mapping_rows", []):
#             from code_generation.dq_expectations import generate_expectations_for_mapping, format_expectations_for_prompt
            
#             expectations_dict = generate_expectations_for_mapping(
#                 mapping_rows=getattr(ctx, "mapping_rows", []),
#                 df_dtypes=getattr(ctx, "df_dtypes", {}),
#                 pii_columns=getattr(ctx, "pii_columns", []),
#                 phi_columns=getattr(ctx, "phi_columns", [])
#             )
            
#             if expectations_dict:
#                 fail_badge = """
#                 <span style="
#                     background-color: #ff4b4b;
#                     color: white;
#                     padding: 2px 8px;
#                     border-radius: 6px;
#                     font-size: 0.85em;
#                     font-weight: 600;
#                 ">FAIL</span>
#                 """

#                 drop_badge = """
#                 <span style="
#                     background-color: #ffa500;
#                     color: white;
#                     padding: 2px 8px;
#                     border-radius: 6px;
#                     font-size: 0.85em;
#                     font-weight: 600;
#                 ">DROP</span>
#                 """

#                 log_badge = """
#                 <span style="
#                     background-color: #28a745;
#                     color: white;
#                     padding: 2px 8px;
#                     border-radius: 6px;
#                     font-size: 0.85em;
#                     font-weight: 600;
#                 ">LOG</span>
#                 """
#                 st.markdown("**The following DLT expectations will be added to your Silver tables:**", unsafe_allow_html=True)
#                 st.markdown(
#                     f"{fail_badge} = Pipeline fails | {drop_badge} = Drop bad rows | {log_badge} = Log violations",
#                     unsafe_allow_html=True
#                 )
#                 for table, exps in expectations_dict.items():
#                     st.markdown(f"**{table}:**")
#                     for mode, rule_name, condition in exps: 
#                         # Color code by severity
#                         if mode == "expect_or_fail":
#                             badge_color = "#ff4b4b"  
#                             badge_text = "FAIL"
#                         elif mode == "expect_or_drop":
#                             badge_color = "#ffa500"  
#                             badge_text = "DROP"
#                         else:
#                             badge_color = "#28a745"  
#                             badge_text = "LOG"

#                         badge_html = f"""
#                         <span style="
#                             background-color: {badge_color};
#                             color: white;
#                             padding: 2px 8px;
#                             border-radius: 6px;
#                             font-size: 0.85em;
#                             font-weight: 600;
#                             margin-right: 6px;
#                         ">{badge_text}</span>
#                         """
                        
#                         st.markdown(f"{badge_html} `@dlt.{mode}(\"{rule_name}\", \"{condition}\")`",unsafe_allow_html=True)
#             else:
#                 st.info("No expectations generated. Generate Silver mappings first.")
#         else:
#             st.info("Generate Silver mappings first to preview expectations.")

#     if st.button("Generate Lakeflow PySpark Code"):
#         ensure_agent_context()
#         ctx = st.session_state["agent_context"]

#         # STEP 1: Deduplicate column names first (critical fix for duplicate columns)
#         from code_generation.mapping_deduplicator import deduplicate_silver_mappings, get_deduplication_report
        
#         dedup_result = deduplicate_silver_mappings(getattr(ctx, "mapping_rows", []))
        
#         # Show deduplication report
#         if dedup_result["total_resolved"] > 0:
#             st.warning(f"Resolved {dedup_result['total_resolved']} duplicate column conflicts")
#             with st.expander("View Deduplication Details"):
#                 st.markdown(get_deduplication_report(dedup_result))
#                 for warning in dedup_result["warnings"]:
#                     st.info(warning)
        
#         # Update mappings with deduplicated columns
#         ctx.mapping_rows = dedup_result["resolved_mappings"]
        
#         # STEP 2: Process and group mappings for table generation
#         from code_generation.mapping_processor import process_mappings_for_code_generation
#         processed = process_mappings_for_code_generation(
#             mapping_rows=ctx.mapping_rows,
#             gold_mapping_rows=getattr(ctx, "gold_mapping_rows", []),
#             strategy="group"
#         )
        
#         # Update context with clean mappings
#         ctx.mapping_rows = processed["resolved_mappings"]
#         ctx.gold_mapping_rows = processed["gold_validated"]
        
#         # Warn about duplicate tables found
#         if processed["duplicates_found"]:
#             st.warning(f"Duplicate table names resolved: {list(processed['duplicates_found'].keys())}")
        
#         st.session_state["agent_context"] = ctx

#         ctx.extra_examples = "Strictly use these examples for Databricks syntax for Lakeflow DLT jobs:\n" + DLT_EXAMPLES

#         if hasattr(ctx, "kpis"):
#             print(f"Passing KPIs to codegen agent:\n{ctx.kpis}")

#         with st.spinner("Generating PySpark code with Bricks Coder Agent..."):
#             ctx = run_bricks_medallion_agent(ctx)
#             st.session_state["agent_context"] = ctx

#         last_code = getattr(ctx, "pyspark_code", "")
#         if last_code:
#             st.code(last_code, language="python")
#             ctx.canvas_code = (ctx.canvas_code or "") + "\n\n" + last_code
#         else:
#             st.info("No PySpark code returned.")



# def generate_masking_sql():
#     ensure_agent_context()
#     ctx = st.session_state["agent_context"]

    
#     ctx.catalog = ctx.catalog or st.session_state.get("catalog_name") or "workspace"
#     ctx.schema = ctx.schema or st.session_state.get("schema_name") or "default"

#     ui_mode = st.session_state.get("access_mode")
#     ui_value = st.session_state.get("access_value")
#     if ui_mode:
#         ctx.access_mode = ui_mode
#     if ui_value:
#         ctx.access_value = ui_value

#     # maintain backwards-compatibility fields if you still want them
#     # (not strictly necessary, but keeps older code paths working)
#     if ui_mode == "group":
#         ctx.pii_access_mode = "group"
#         ctx.pii_access_value = ui_value or ctx.pii_access_value
#         ctx.phi_access_mode = "group"
#         ctx.phi_access_value = ui_value or ctx.phi_access_value
#     else:
#         ctx.pii_access_mode = "single"
#         ctx.pii_access_value = ui_value or ctx.pii_access_value
#         ctx.phi_access_mode = "single"
#         ctx.phi_access_value = ui_value or ctx.phi_access_value

#     # persist changes to session_state
#     st.session_state["agent_context"] = ctx

#     # call masking agent
#     from code_generation.masking_agent import masking_agent as run_masking_agent
#     new_ctx = run_masking_agent(ctx)
#     # persist
#     st.session_state["agent_context"] = new_ctx
#     st.success("Masking SQL generated and saved to AgentState.")


# # UI: access control simplified to single principal (single user OR group)
# with st.expander("🔒 Generate & Manage Masking SQL", expanded=True):
#     st.markdown("#### Access control for masking (PII / PHI)")
#     col_a = st.columns(1)[0]  # keep layout but only need one column

#     with col_a:
#         st.markdown("**Access control principal**")
#         access_mode = st.radio("Allow by", ["Group", "Single user"], index=0, key="ui_access_mode")
#         if access_mode == "Group":
#             access_value = st.text_input("Group name", value=st.session_state.get("access_value", "pii_access"), key="ui_access_group")
#         else:
#             access_value = st.text_input("Allowed user id (current_user match)", value=st.session_state.get("access_value", "user@example.com"), key="ui_access_user")

#     # Save UI choices into session_state keys used by ensure_agent_context / masking_agent
#     st.session_state["access_mode"] = ("group" if access_mode == "Group" else "single")
#     st.session_state["access_value"] = access_value.strip()


#     if st.button("Generate Masking SQL"):
#         generate_masking_sql()

#     ctx = st.session_state.get("agent_context")
#     masking_sql = getattr(ctx, "masking_sql", "") or ""
#     # Constants (hardcode as you requested)
#     EXEC_HOST_DEFAULT = "dbc-83de24b5-b7ed.cloud.databricks.com"
#     EXEC_HTTP_PATH_DEFAULT = "/sql/1.0/warehouses/0069c79611dc9f7e"

#     # ... inside your masking expander where `masking_sql` is shown ...
#     if masking_sql:
#         st.markdown("**Generated Masking SQL (editable)**")
#         edited_mask_sql = st.text_area("Masking SQL", value=masking_sql, height=300, key="mask_sql_editor")

#         if st.button("Save Edited Masking SQL"):
#             ctx.masking_sql = edited_mask_sql
#             stmts = [s.strip()+';' for s in re.split(r';\s*', edited_mask_sql) if s.strip()]
#             ctx.masking_sql_lines = stmts
#             ctx.masking_version = (getattr(ctx, "masking_version", 0) or 0) + 1
#             st.session_state["agent_context"] = ctx
#             st.success("Masking SQL saved to AgentState.")

#         # show download
#         st.download_button("Download Masking SQL", edited_mask_sql or masking_sql, file_name="masking_sql.sql")

#         # Connection UI (hardcoded defaults, editable)
#         st.markdown("**Databricks SQL Connection (host & http_path are prefilled)**")
#         c1, c2, c3 = st.columns([4,4,6])
#         with c1:
#             exec_host = st.text_input("Server Hostname", value=EXEC_HOST_DEFAULT, key="exec_host")
#         with c2:
#             exec_http_path = st.text_input("HTTP Path (warehouse)", value=EXEC_HTTP_PATH_DEFAULT, key="exec_http_path")
#         with c3:
#             # prefer the PAT saved earlier, else let user paste
#             token_prefill = st.session_state.get("user_pat_token", "")
#             exec_token = st.text_input("Access Token (will reuse Step 1 token if present)", type="password", value=token_prefill, key="exec_token")

#         # Run button
#         if st.button("Run Masking SQL"):
#             sql_text_to_run = (edited_mask_sql or masking_sql).strip()
#             if not sql_text_to_run:
#                 st.error("No masking SQL available to run.")
#             else:
#                 host_val = exec_host.strip() or EXEC_HOST_DEFAULT
#                 http_path_val = exec_http_path.strip() or EXEC_HTTP_PATH_DEFAULT
#                 token_val = exec_token.strip() or st.session_state.get("user_pat_token")

#                 if not token_val:
#                     st.error("No access token available. Paste your token here or enter in Step 1.")
#                 else:
#                     # Save token for later convenience
#                     st.session_state["user_pat_token"] = token_val

#                     # update agent state -> RUNNING
#                     ensure_agent_context()
#                     ctx = st.session_state["agent_context"]
#                     ctx.mask_execution_status = "RUNNING"
#                     st.session_state["agent_context"] = ctx

#                     with st.spinner("Executing masking SQL..."):
#                         result = execute_masking_sql(
#     masking_sql=ctx.masking_sql,
#     masking_sql_lines=ctx.masking_sql_lines,
#     host=exec_host,
#     http_path=exec_http_path,
#     access_token=exec_token
# )


#                     # Record structured log entry
#                     ts = datetime.utcnow().isoformat() + "Z"
#                     log_entry = {
#                         "ts": ts,
#                         "status": result.get("status", "error"),
#                         "summary": result.get("logs", [])[:3] if result.get("logs") else [],
#                     }

#                     # append to agent_context.mask_execution_log (persisted)
#                     ensure_agent_context()
#                     ctx = st.session_state["agent_context"]
#                     if not getattr(ctx, "mask_execution_log", None):
#                         ctx.mask_execution_log = []
#                     ctx.mask_execution_log.append(log_entry)
#                     ctx.mask_execution_status = "SUCCESS" if result.get("status") == "ok" else "FAILED"
#                     st.session_state["agent_context"] = ctx

#                     # Display logs & verification
#                     # Display logs & verification (avoid nested expanders)
#                     if result.get("logs"):
#                         st.markdown("**Execution Logs**")
#                         # show as a single block for readability
#                         st.code("\n".join(str(l) for l in result["logs"]))

#                     if result.get("verify"):
#                         st.markdown("**Verification Output**")
#                         st.code("\n".join(str(l) for l in result["verify"]))


#                     if result.get("status") == "ok":
#                         st.success("Masking SQL executed successfully.")
#                     else:
#                         st.error("Masking SQL execution failed. Check logs for details.")







# # with st.container():
# #     st.subheader("Step 5: Improve/Extend PySpark Code")

# #     ensure_agent_context()
# #     ctx = st.session_state["agent_context"]

# #     task = st.text_input(
# #         "Enter improvement task (e.g., 'add data governance policies for sensitive columns')",
# #         key="improvement_task"
# #     )

# #     if st.button("Run Code Improver"):
# #         if not getattr(ctx, "pyspark_code", ""):
# #             st.warning("Generate PySpark code first (Step 6).")
# #         else:
# #             with st.spinner("Improving PySpark code..."):
# #                 before = ctx.pyspark_code
# #                 ctx = code_improver_agent_node(ctx, task)
# #                 st.session_state["agent_context"] = ctx
# #                 after = ctx.pyspark_code

# #                 # compute_diff now lives in code_improver_agent.py
# #                 from agents.code_improver_agent import compute_diff
# #                 diff = compute_diff(before, after)

# #                 history_entry = {
# #                     "task": task,
# #                     "before": before,
# #                     "after": after,
# #                     "diff": diff
# #                 }
# #                 if not hasattr(ctx, "code_history"):
# #                     ctx.code_history = []
# #                 ctx.code_history.append(history_entry)

# #             improved_code = getattr(ctx, "pyspark_code", "")
# #             if improved_code:
# #                 st.code(improved_code, language="python")
# #                 ctx.canvas_code = (ctx.canvas_code or "") + "\n\n" + improved_code

# #     # Show change history
# #     with st.expander("Code Change History", expanded=False):
# #         if ctx and getattr(ctx, "code_history", []):
# #             for i, h in enumerate(ctx.code_history, 1):
# #                 st.markdown(f"**{i}. Task:** {h['task']}")
# #                 if h['diff']:
# #                     st.code(h['diff'], language="diff")
# #                 else:
# #                     st.info("No diff available.")
# #         else:
# #             st.info("No changes recorded yet.")

# #     # Show evolving canvas
# #     st.subheader("Final Evolving Script")
# #     if ctx and getattr(ctx, "canvas_code", ""):
# #         st.code(ctx.canvas_code, language="python")
# #         st.download_button("Download Final Script", ctx.canvas_code, "final_pipeline.py")



# with st.container():
#     st.subheader("Step 5: Data Modeling")
#     with st.expander("Generate Dimensional Model", expanded=True):
#         from data_modeling.dimensional_modeling_agent import get_dimensional_modeling_agent

#         st.markdown("### Dimensional Modeling (SQL only)")

#         schema_choice = st.selectbox(
#             "Choose schema to model",
#             ["Bronze (source)", "Silver (RAG mappings)"],
#             index=0,
#             key="dm_schema_choice"
#         )
#         if st.button("Generate Dimensional Model (SQL)", key="dm_generate_sql"):
#             ensure_agent_context()
#             if not st.session_state["agent_context"].dfs and not (
#                 getattr(st.session_state["agent_context"], "mapping_rows", []) or
#                 getattr(st.session_state["agent_context"], "fhir_mapping_rows", [])
#             ):
#                 st.warning("Please upload data and/or run the RAG Mapper first.")
#             else:
#                 view = "silver" if schema_choice.startswith("Silver") else "bronze"
#                 node, _ = get_dimensional_modeling_agent(schema_view=view)
#                 st.session_state["agent_context"] = node(st.session_state["agent_context"])

#                 msgs = st.session_state["agent_context"].messages
#                 last_sql = next(
#                     (m for m in reversed(msgs)
#                     if m.get("role") == "assistant" and m.get("name") == "Dimensional_Modeling_SQL"),
#                     None
#                 )
#                 if last_sql and last_sql.get("content"):
#                     st.code(last_sql["content"], language="sql")
#                 else:
#                     st.info("No SQL returned.")


# # Step3.2: render dimensionl model : start 

#         import re
#         from graphviz import Digraph

#         # after you call the modeling node and display the SQL:
#         ctx = st.session_state.get("agent_context")
#         ddl = getattr(ctx, "modeling_sql", "") if ctx else ""


#         if ddl:
#             # Parse DDL
#             tables = {}
#             foreign_keys = []

#             for match in re.finditer(r'CREATE TABLE (\w+)\s*\((.*?)\);', ddl, re.DOTALL | re.IGNORECASE):
#                 table_name = match.group(1)
#                 table_body = match.group(2)

#                 columns = []
#                 lines = [line.strip().rstrip(',') for line in table_body.splitlines() if line.strip()]

#                 for line in lines:
#                     fk_match = re.match(
#                         r'FOREIGN KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)\s*\((\w+)\)',
#                         line, re.IGNORECASE
#                     )
#                     col_match = re.match(r'(\w+)\s+[A-Z]+(?:\s+[A-Z]+)*', line, re.IGNORECASE)

#                     if fk_match:
#                         fk_col, ref_table, ref_col = fk_match.groups()
#                         foreign_keys.append((table_name, fk_col, ref_table, ref_col))
#                     elif col_match and "FOREIGN KEY" not in line.upper():
#                         col_name = col_match.group(1)
#                         columns.append(col_name)

#                 tables[table_name] = columns

#             # Build Graphviz ERD
#             erd = Digraph("dimensional_model", format="png")
#             erd.attr(rankdir="LR", compound="true")

#             for table_name, cols in tables.items():
#                 with erd.subgraph(name=f"cluster_{table_name}") as cluster:
#                     cluster.attr(label=f"{table_name}", style="filled", color="lightgray")
#                     for col in cols:
#                         cluster.node(f"{table_name}_{col}", label=col, shape="box")

#             for src_table, src_col, ref_table, ref_col in foreign_keys:
#                 erd.edge(f"{src_table}_{src_col}", f"{ref_table}_{ref_col}", label=f"{src_col} → {ref_table}.{ref_col}")

#             st.graphviz_chart(erd.source)
#         else:
#             st.info("No modeling SQL found yet. Generate the dimensional model first.")



# #-----------------------Data Governance Button---------------------------------------------------

# # from agents.data_governance_agent import dg_detection_agent
# # from agents.agent_state import AgentState


# # st.subheader("Step 5 Data Governance")

# # if st.button("Run Data Governance Detection"):
# #     if "agent_context" not in st.session_state or not getattr(st.session_state["agent_context"], "mapping_rows", None):
# #         st.warning("Please upload data and generate bronze mapping first.")
# #     else:
# #         agent_context: AgentState = st.session_state["agent_context"]

# #         # Call DG agent with full AgentState, not just mapping_rows
# #         updated_state = dg_detection_agent(agent_context)

# #         # Update session state
# #         st.session_state["agent_context"] = updated_state

# #         # Show results
# #         st.success("Data Governance check completed.")
# #         st.write("**Detected PII Columns:**", updated_state.pii_columns or "None")
# #         st.write("**Detected PHI Columns:**", updated_state.phi_columns or "None")

# #         st.subheader("Full Sensitive Metadata")
# #         if isinstance(updated_state.sensitive_metadata, dict):
# #             st.json(updated_state.sensitive_metadata)
# #         else:
# #             st.error("Sensitive metadata is not valid JSON:")
# #             st.write(updated_state.sensitive_metadata)


# # with st.container():
# #     st.subheader("🧠 Live Agent State Status ")

# #     ensure_agent_context()
# #     ctx = st.session_state.get("agent_context")

# #     if not ctx:
# #         st.warning("AgentState not initialized yet.")
# #     else:
# #         ctx_dict = ctx.dict(exclude_none=True)

# #         def field_status(val):
# #             if val is None:
# #                 return "❌ MISSING"
# #             if isinstance(val, (list, dict, str)) and len(val) == 0:
# #                 return "⚠️ EMPTY"
# #             return "✅ PRESENT"

# #         rows = []

# #         for field, value in ctx_dict.items():
# #             status = field_status(value)

            
# #             if isinstance(value, dict):
# #                 preview = f"{len(value)} keys"
# #             elif isinstance(value, list):
# #                 preview = f"{len(value)} items"
# #             else:
# #                 preview = str(value)

# #             rows.append({
# #                 "Field": field,
# #                 "Status": status,
# #             })

# #         import pandas as pd
# #         df_status = pd.DataFrame(rows)

# #         st.dataframe(
# #             df_status.sort_values("Field"),
# #             use_container_width=True
# #         )

# # ---------------------------
# # Chat with Datacraft Agent
# # ---------------------------
# st.markdown("## Chat with Datacraft Agent")

# # --- Intro / message history init ---
# if "chat_messages" not in st.session_state:
#     intro_messages = [
#         ChatAgentMessage(
#             id=str(uuid.uuid4()),
#             role="system",
#             content=(
#                 "You are a multi-agent data assistant developed by Datacraft. "
#                 "You assist users with exploring and analyzing structured data uploaded from files. "
#                 "Use tools, collaborate with agents, and guide the user toward insights."
#             )
#         ),
#         ChatAgentMessage(
#             id=str(uuid.uuid4()),
#             role="user",
#             content="I have uploaded my dataset."
#         ),
#         ChatAgentMessage(
#             id=str(uuid.uuid4()),
#             role="assistant",
#             content="File successfully received. Ready to assist you with your data."
#         )
#     ]
#     st.session_state.chat_messages = [msg.model_dump() for msg in intro_messages]


# # --- Helper: initialize agent/context when data is available ---
# def init_chat_agent_if_needed():
#     """Create chat_agent using the full AgentState built by ensure_agent_context."""

#     # Only initialize if we actually have data
#     if "dfs" not in st.session_state and "df_heads" not in st.session_state:
#         return

#     # Always rebuild from the latest AgentState (so it sees new pyspark_code, mappings, etc.)
#     ensure_agent_context()
#     ctx = st.session_state["agent_context"]

#     # Keep messages in sync with the chat history
#     if "chat_messages" in st.session_state:
#         ctx.messages = st.session_state.chat_messages[:]
#         st.session_state["agent_context"] = ctx

#     try:
#         st.session_state.chat_agent = LangGraphChatAgent(
#             agent_state=ctx
#         )

#     except Exception as e:
#         st.session_state["chat_agent_init_error"] = str(e)
#         if "chat_agent" in st.session_state:
#             del st.session_state["chat_agent"]


# # Attempt initialization now (will be a no-op if data not present)
# init_chat_agent_if_needed()

# # --- Render existing chat history (skip the very first file-load message if present) ---
# for msg in st.session_state.chat_messages[1:]:
#     with st.chat_message(msg["role"]):
#         if msg.get("name"):
#             st.markdown(f"**{msg['name']}**: {msg['content']}")
#         else:
#             st.markdown(msg["content"])

# # --- Accept new question input ---
# if user_input := st.chat_input("Ask your agent..."):
#     st.session_state.chat_messages.append({
#         "id": str(uuid.uuid4()),
#         "role": "user",
#         "content": user_input
#     })

#     with st.chat_message("user"):
#         st.markdown(user_input)

#     # Convert current history to ChatAgentMessage objects
#     chat_messages = [
#         ChatAgentMessage(
#             id=msg.get("id", str(uuid.uuid4())),
#             role=msg["role"],
#             content=msg["content"],
#             name=msg.get("name"),
#         )
#         for msg in st.session_state.chat_messages
#     ]

#     # Ensure agent is initialized before calling predict
#     init_chat_agent_if_needed()


#     ctx = st.session_state.get("agent_context")
#     if ctx:
#         ctx.messages = [m.model_dump() for m in chat_messages]
#         ctx.ui_chat_history = st.session_state.chat_messages[:]
#         st.session_state["agent_context"] = ctx

#     with st.chat_message("assistant"):
#         with st.spinner("Thinking..."):
#             # If chat_agent failed to initialize, show a helpful message instead of crashing
#             if "chat_agent" not in st.session_state:
#                 init_err = st.session_state.get("chat_agent_init_error")
#                 if init_err:
#                     st.error("Agent initialization failed. See logs for details.")
#                     st.markdown(f"**Debug:** {init_err}")
#                 else:
#                     st.info("Agent isn't ready yet. Please upload dataset or refresh the page.")
#                 # Save a short assistant response so UI history remains consistent
#                 fallback_msg = ChatAgentMessage(
#                     id=str(uuid.uuid4()),
#                     role="assistant",
#                     content="Agent not initialized. Please ensure you uploaded a dataset and try again."
#                 )
#                 st.session_state.chat_messages.append(fallback_msg.model_dump())
#                 st.markdown(fallback_msg.content)
#             else:
#                 # Agent is present: call predict (non-streaming)
#                 try:
#                     ctx = st.session_state.get("agent_context")
#                     if ctx:
#                         ctx.messages = [m.model_dump() for m in chat_messages]
#                         ctx.ui_chat_history = st.session_state.chat_messages[:] 
#                         st.session_state["agent_context"] = ctx

#                     resp = st.session_state.chat_agent.predict(
#                         messages=chat_messages,
#                         context=st.session_state.get("agent_context")
#                     )
#                     st.session_state["agent_context"] = st.session_state.chat_agent.agent_state

#                 except Exception as e:
#                     st.error("Agent predict() raised an exception. Check logs.")
#                     st.markdown(f"**Debug:** {e}")
#                     # Save error message into history
#                     error_msg = ChatAgentMessage(
#                         id=str(uuid.uuid4()),
#                         role="assistant",
#                         content=f"Agent error: {e}"
#                     )
#                     st.session_state.chat_messages.append(error_msg.model_dump())
#                     st.markdown(error_msg.content)
#                 else:
#                     # Filter/display final assistant message (prefer 'final_answer' name)
#                     final_messages_to_display = [
#                         msg for msg in resp.messages
#                         if getattr(msg, 'name', None) == "final_answer" or msg.role == "assistant"
#                     ]

#                     if final_messages_to_display:
#                         final_msg = final_messages_to_display[-1]
#                         content = final_msg.content
#                         if content.lower().startswith("final_answer:"):
#                             content = content[len("final_answer:"):].strip()
#                         st.markdown(content)
#                         st.session_state.chat_messages.append(final_msg.model_dump())
#                     else:
#                         # No assistant message produced
#                         empty_msg = ChatAgentMessage(
#                             id=str(uuid.uuid4()),
#                             role="assistant",
#                             content="No response from agent."
#                         )
#                         st.session_state.chat_messages.append(empty_msg.model_dump())
#                         st.markdown(empty_msg.content)
# # --- end of chat block ---
