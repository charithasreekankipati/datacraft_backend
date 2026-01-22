from backend.agents.agent_state import AgentState
from backend.llm_provider import llm
from typing import Dict, List
import json


def gold_mapper_agent_node(state: AgentState) -> AgentState:
    """
    Generate Silver → Gold mappings using KPIs and existing Silver mappings.
    Creates aggregated/analytical Gold table columns from normalized Silver tables.
    Handles duplicate column detection and resolution.
    """
    
    messages = state.messages or []
    silver_mappings = getattr(state, "mapping_rows", [])
    kpis = getattr(state, "kpis", "")
    
    if not silver_mappings:
        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "content": "No Silver mappings found. Generate Silver mappings first.",
                "name": "GoldMapper"
            }]
        })
    
    # CRITICAL: Detect duplicate silver_column mappings
    from collections import defaultdict
    silver_col_usage = defaultdict(list)
    for mapping in silver_mappings:
        key = (mapping.get("silver_table"), mapping.get("silver_column"))
        silver_col_usage[key].append(mapping.get("bronze_columns"))
    
    # Find duplicates and create unique column names
    resolved_mappings = []
    for mapping in silver_mappings:
        silver_table = mapping.get("silver_table")
        silver_column = mapping.get("silver_column")
        bronze_column = mapping.get("bronze_columns")
        
        key = (silver_table, silver_column)
        
        # If this silver column is used multiple times, make it unique
        if len(silver_col_usage[key]) > 1:
            # Add bronze column context to make unique
            unique_column = f"{silver_column}_{bronze_column}"
            resolved_mappings.append({
                **mapping,
                "silver_column": unique_column,
                "original_column": silver_column,
                "is_deduplicated": True
            })
        else:
            resolved_mappings.append({
                **mapping,
                "original_column": silver_column,
                "is_deduplicated": False
            })
    
    # Update state with resolved mappings
    state.mapping_rows = resolved_mappings
    
    # Build context for LLM
    prompt = f"""
You are a data modeling expert. Given Silver layer tables and business KPIs, 
create Gold layer mappings that define analytical/aggregated columns.

Silver Layer Mappings (normalized source tables):
{json.dumps(silver_mappings, indent=2)}

Business KPIs:
{kpis or "No KPIs defined yet."}

Task:
Create Gold table column mappings that group related Silver columns into analytical tables.
Think about:
- Fact tables (transactional data with metrics)
- Dimension tables (reference/lookup data)
- Aggregations needed for KPIs

Return ONLY a JSON array in this format:
[
  {{
    "silver_table": "Patient",
    "silver_column": "patient_id",
    "gold_table": "fact_patient_encounters",
    "gold_column": "patient_key",
    "transformation": "direct",
    "description": "Patient identifier for fact table"
  }},
  {{
    "silver_table": "Appointment",
    "silver_column": "duration_minutes",
    "gold_table": "fact_patient_encounters",
    "gold_column": "encounter_duration",
    "transformation": "cast_to_int",
    "description": "Appointment duration for analytics"
  }}
]

Rules:
- Create 2-5 Gold tables maximum (fact and dimension tables)
- Include transformations: direct, cast, aggregation, derived
- Group logically related columns
- Return ONLY valid JSON array
"""
    
    try:
        answer = llm.invoke(prompt)
        answer_text = getattr(answer, "content", None) or str(answer)
        
        # Extract JSON from response
        import re
        json_match = re.search(r'\[.*\]', answer_text, re.DOTALL)
        if json_match:
            gold_mappings = json.loads(json_match.group(0))
        else:
            gold_mappings = []
        
        return state.copy(update={
            "gold_mapping_rows": gold_mappings,
            "messages": messages + [{
                "role": "assistant",
                "content": f"Generated {len(gold_mappings)} Gold layer mappings.",
                "name": "GoldMapper"
            }]
        })
    
    except Exception as e:
        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "content": f"Error generating Gold mappings: {str(e)}",
                "name": "GoldMapper"
            }]
        })


def get_gold_mapper_agent():
    description = (
        "Maps Silver normalized columns to Gold analytical/aggregated columns "
        "based on business KPIs and dimensional modeling principles."
    )
    return gold_mapper_agent_node, description


# Alias for direct usage
run_gold_mapper_agent = gold_mapper_agent_node