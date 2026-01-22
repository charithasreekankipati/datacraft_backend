# qna_agent.py
from backend.agents.agent_state import AgentState
from backend.llm_provider import llm
import re


def detect_meta_intent(question: str) -> str:
    q = re.sub(r"[^\w\s]", "", question.lower())

    if any(p in q for p in [
        "first thing i asked",
        "what did i ask first",
        "what was my first question",
        "summarize our chat",
        "what have we discussed"
    ]):
        return "CHAT_HISTORY"

    if any(p in q for p in [
        "what did i upload",
        "what dataset is loaded",
        "what data is loaded",
        "earlier what did i upload"
    ]):
        return "DATA_MEMORY"

    return "NORMAL"


def simple_data_qna_node(state: AgentState) -> AgentState:
    ui_msgs = getattr(state, "ui_chat_history", []) or []
    user_questions = [
        m["content"] for m in ui_msgs if m.get("role") == "user"
    ]
    messages = state.messages
    
    last_user_msg = next(
        (m for m in reversed(messages) if m.get("role") == "user" and m.get("content")),
        None
    )

    chat_history_lines = []
    for m in messages[-20:]:  
        role = m.get("role", "")
        content = (m.get("content") or "").strip()
        name = m.get("name")

        if not content:
            continue

        if role == "user":
            prefix = "USER"
        elif role == "assistant":
            # e.g. supervisor, QnA, Coder, etc.
            prefix = f"ASSISTANT({name})" if name else "ASSISTANT"
        else:
            prefix = role.upper()  # system, tool, etc.

        chat_history_lines.append(f"{prefix}: {content}")

    chat_history = "\n".join(chat_history_lines) if chat_history_lines else "No prior messages."



    if not last_user_msg:
        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "content": "I don't see a user question yet. Please ask me something about your data or pipelines.",
                "name": "QnA"
            }]
        })

    question = (last_user_msg["content"] or "").strip()
    if not question:
        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "content": "I got an empty question. Please type your question again.",
                "name": "QnA"
            }]
        })

    q_lower = question.lower()

    intent = detect_meta_intent(question)

    # --- CHAT HISTORY QUESTIONS (NO LLM) ---
    if intent == "CHAT_HISTORY":
        if not user_questions:
             answer = "You haven't asked any questions yet."
        else:
             # Fix logic for "first question" vs "previous question"
             if "first" in question.lower():
                 answer = f"The first question you asked was: '{user_questions[0]}'"
             elif "previous" in question.lower() or "last" in question.lower():
                 # The last question is the current one, so the previous one is index -2
                 if len(user_questions) >= 2:
                     answer = f"The previous question you asked was: '{user_questions[-2]}'"
                 else:
                     answer = "There is no previous question; this is your first one."
             else:
                 # Default summary
                 answer = "Here are the questions you've asked so far:\n" + "\n".join([f"- {q}" for q in user_questions])
        
        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "content": answer,
                "name": "QnA"
            }]
        })





# --- DATASET MEMORY QUESTIONS (NO LLM) ---
    if intent == "DATA_MEMORY":
        dfs = state.dfs or {}

        if not dfs:
            answer = "No dataset is currently loaded."
        else:
            lines = []
            for name, df in dfs.items():
                cols = ", ".join(df.columns)
                lines.append(f"{name}: {cols}")
            answer = "You uploaded the following dataset(s):\n" + "\n".join(lines)

        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "content": answer,
                "name": "QnA"
            }]
        })



    # ---- 2. Pull rich context from AgentState ----
    dfs = state.dfs or {}
    available_dfs = list(dfs.keys())

    # schemas per DataFrame
    available_schemas = {
        name: list(df.columns) for name, df in dfs.items()
    }

    mapping_rows = getattr(state, "mapping_rows", []) or []
    gold_mapping_rows = getattr(state, "gold_mapping_rows", []) or []
    kpis = (getattr(state, "kpis", "") or "").strip()
    masking_sql = (getattr(state, "masking_sql", "") or "").strip()
    sensitive_metadata = getattr(state, "sensitive_metadata", {}) or {}
    pii_columns = getattr(state, "pii_columns", []) or []
    phi_columns = getattr(state, "phi_columns", []) or []
    dq_rules = (getattr(state, "dq_rules", "") or "").strip()
    pyspark_code = (getattr(state, "pyspark_code", "") or "").strip()
    modeling_sql = (getattr(state, "modeling_sql", "") or "").strip()
    catalog = getattr(state, "catalog", "workspace")
    schema = getattr(state, "schema", "default")

    # ---- 3. Build human-readable context sections ----
    sections = []

    # 3.1 High-level summary
    summary_lines = []
    if available_dfs:
        summary_lines.append(f"- DataFrames: {', '.join(available_dfs)}")
    else:
        summary_lines.append("- No DataFrames loaded yet.")

    if mapping_rows:
        summary_lines.append(f"- Bronze → Silver mappings: {len(mapping_rows)} rows.")
    if gold_mapping_rows:
        summary_lines.append(f"- Silver → Gold mappings: {len(gold_mapping_rows)} rows.")

    if sensitive_metadata:
        summary_lines.append("- Sensitive metadata (PII/PHI) is available.")
    if pii_columns:
        summary_lines.append(f"- PII columns: {', '.join(pii_columns)}")
    if phi_columns:
        summary_lines.append(f"- PHI columns: {', '.join(phi_columns)}")

    if kpis:
        summary_lines.append("- Business KPIs have been generated.")
    if masking_sql:
        summary_lines.append("- Masking SQL has been generated.")
    if dq_rules:
        summary_lines.append("- Data quality rules / expectations are available.")
    if pyspark_code:
        summary_lines.append("- PySpark medallion pipeline code has been generated.")
    if modeling_sql:
        summary_lines.append("- Dimensional modeling SQL (DDL) has been generated.")

    summary_lines.append(f"- Active catalog/schema: {catalog}.{schema}")
    sections.append("=== SUMMARY ===\n" + "\n".join(summary_lines))

    # 3.2 Schemas
    sections.append("=== DATAFRAME SCHEMAS ===")
    if available_schemas:
        for df_name, cols in available_schemas.items():
            sections.append(f"- {df_name}: {', '.join(cols)}")
    else:
        sections.append("No DataFrames loaded.")

    # 3.3 Mappings
    if mapping_rows:
        sections.append(f"=== BRONZE → SILVER MAPPINGS (count={len(mapping_rows)}) ===")
        # Only show a small preview, not the whole thing
        preview = mapping_rows[:10]
        sections.append(str(preview))
    if gold_mapping_rows:
        sections.append(f"=== SILVER → GOLD MAPPINGS (count={len(gold_mapping_rows)}) ===")
        preview_gold = gold_mapping_rows[:10]
        sections.append(str(preview_gold))

    # 3.4 Sensitive metadata
    if sensitive_metadata:
        sections.append("=== SENSITIVE METADATA (PII/PHI) ===")
        sections.append(str(sensitive_metadata))

    # 3.5 KPIs
    if kpis:
        sections.append("=== BUSINESS KPIs (RAW TEXT, e.g. DAX/SQL) ===")
        sections.append(kpis)

    # 3.6 Masking SQL
    if masking_sql:
        sections.append("=== MASKING SQL ===")
        sections.append(masking_sql)

    # 3.7 PySpark code
    if pyspark_code:
        sections.append("=== PYSPARK MEDALLION PIPELINE CODE ===")
        sections.append(pyspark_code)

    # 3.8 Dimensional modeling SQL
    if modeling_sql:
        sections.append("=== DIMENSIONAL MODELING SQL (DDL) ===")
        sections.append(modeling_sql)

        # 3.X Chat history section (for meta questions)
    sections.append("=== CHAT HISTORY (LATEST MESSAGES) ===")
    sections.append(chat_history)

    state_context = "\n\n".join(sections)

    # ---- 4. Handle greetings / generic capability questions gracefully ----
    if any(g in q_lower for g in ["hi", "hello", "hey"]) and len(question.split()) <= 5:
        capability_lines = ["Hi! I’m your Datacraft assistant. I can help you with:"]

        if available_dfs:
            capability_lines.append(f"- Exploring tables: {', '.join(available_dfs)}")
        if mapping_rows:
            capability_lines.append("- Explaining Bronze → Silver mappings.")
        if gold_mapping_rows:
            capability_lines.append("- Explaining Silver → Gold mappings.")
        if kpis:
            capability_lines.append("- Explaining the generated business KPIs.")
        if pyspark_code:
            capability_lines.append("- Explaining or refining the PySpark medallion pipeline.")
        if masking_sql:
            capability_lines.append("- Explaining the generated masking SQL / access control.")
        if modeling_sql:
            capability_lines.append("- Explaining the dimensional model (fact/dimension tables).")

        capability_lines.append("Try asking things like:")
        capability_lines.append("- 'Explain all the KPIs that were generated'")
        capability_lines.append("- 'Summarize the PySpark pipeline'")
        capability_lines.append("- 'What sensitive columns did you detect?'")

        answer_text = "\n".join(capability_lines)
        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "content": answer_text,
                "name": "QnA"
            }]
        })

    if (
        "what kind of questions" in q_lower
        or "what questions can you answer" in q_lower
        or "how can you help" in q_lower
    ):
        capability_lines = ["I can answer questions about:"]

        if available_dfs:
            capability_lines.append(f"- The loaded DataFrames and their columns: {', '.join(available_dfs)}")
        if mapping_rows:
            capability_lines.append("- How Bronze columns map to Silver tables/columns.")
        if gold_mapping_rows:
            capability_lines.append("- How Silver columns map to Gold analytical tables.")
        if sensitive_metadata or pii_columns or phi_columns:
            capability_lines.append("- Which columns are PII/PHI and how they appear across layers.")
        if masking_sql:
            capability_lines.append("- The generated masking SQL functions and policies.")
        if pyspark_code:
            capability_lines.append("- The generated PySpark medallion (Bronze/Silver/Gold) pipeline code.")
        if modeling_sql:
            capability_lines.append("- The dimensional model tables and relationships (SQL DDL).")
        if kpis:
            capability_lines.append("- The business KPIs that were suggested for your data.")
        if dq_rules:
            capability_lines.append("- The data quality expectations (rules) to enforce on your tables.")

        capability_lines.append("If something wasn't generated yet, I'll tell you that instead of guessing.")
        answer_text = "\n".join(capability_lines)

        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "content": answer_text,
                "name": "QnA"
            }]
        })

    # ---- 5. Generic QnA path: one prompt that uses ALL state context ----
    prompt = f"""
You are the QnA agent in a multi-agent Datacraft system.

You answer questions ONLY using the current AgentState, provided below.
Do NOT make up tables, KPIs, code, or fields that are not present in the context.

================= AGENT STATE CONTEXT =================
{state_context}
======================================================

User question:
{question}

Answering rules:
- Use ONLY the information that appears in the context above.
- If the user asks to "explain" something (KPIs, PySpark pipeline, masking SQL, mappings, dimensional model),
  read the relevant section and explain it in clear, simple language.
- If the user asks about the conversation itself (e.g. "what have I asked so far",
  "what did we discuss", "summarize our chat"), use the CHAT HISTORY section.
- If something is clearly NOT available (e.g., no KPIs or no PySpark code yet),
  explicitly say that it has not been generated yet instead of guessing.
- Be concise but specific.

"""

    llm_response = llm.invoke(prompt)
    answer_content = getattr(llm_response, "content", str(llm_response))

    return state.copy(update={
        "messages": messages + [{
            "role": "assistant",
            "content": answer_content,
            "name": "QnA"
        }]
    })


def get_qna_agent():
    description = (
        "This agent answers questions using the full AgentState: "
        "DataFrames, Bronze→Silver→Gold mappings, sensitive metadata (PII/PHI), "
        "masking SQL, KPIs, PySpark pipeline code, and dimensional modeling SQL."
    )
    return simple_data_qna_node, description
