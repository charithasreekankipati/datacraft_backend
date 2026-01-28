import os
import shutil
from fastapi import APIRouter, HTTPException, Form, UploadFile, File
from backend.datalake_design.gold_mapper_agent import gold_mapper_agent_node
from typing import Optional
import pandas as pd 
import logging
from backend.datalake_design.schemas.schemas import MappingResponse
from backend.agents.agent_state import AgentState
from backend.datalake_design.rag_mapper_agent import get_rag_mapper_agent, get_custom_schema_rag_mapper_agent
from backend.state_store import agent_states

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/generate-silver", response_model=MappingResponse)
async def run_rag_mapping(
    standard: str = Form(...),
    catalog: str = Form(...),
    schema_name: str = Form(...),
    dbfs_path: str = Form(...),
    custom_schema: Optional[UploadFile] = File(None)
):
    try:
        # 1. State Retrieval
        state_id = f"{catalog}_{schema_name}"
        current_state = agent_states.get(state_id)
        
        if not current_state:
            raise HTTPException(
                status_code=404, 
                detail=f"State not found. Please run 'Read Files' first."
            )

        # 2. Data Reconstruction (Crucial for Agent processing)
        # We ensure current_state.dfs is populated before passing to the agent
        if not current_state.dfs and current_state.df_heads:
            reconstructed_dfs = {}
            for table_name, content in current_state.df_heads.items():
                if isinstance(content, dict) and "data" in content:
                    reconstructed_dfs[table_name] = pd.DataFrame(content["data"])
                elif isinstance(content, list):
                    reconstructed_dfs[table_name] = pd.DataFrame(content)
            current_state.dfs = reconstructed_dfs

        if not current_state.dfs:
             return MappingResponse(success=False, mapping_rows=[], message="No data available to map.")

        # 3. Custom Schema Handling
        if standard.lower() == "custom":
            if not custom_schema:
                raise HTTPException(status_code=400, detail="Custom schema file is required for 'custom' standard.")
            
            # Save file temporarily
            temp_dir = "temp_schemas"
            os.makedirs(temp_dir, exist_ok=True)
            custom_path = os.path.join(temp_dir, custom_schema.filename)
            
            with open(custom_path, "wb") as buffer:
                shutil.copyfileobj(custom_schema.file, buffer)
            
            # Get the Specialized Custom Agent Node
            node, _ = get_custom_schema_rag_mapper_agent(uploaded_schema_path=custom_path)
        else:
            # Standard FHIR/ACORD RAG Agent
            node, _ = get_rag_mapper_agent(standard=standard.lower())
        

        # 4. Execute Node
        # Capture the result of the node execution correctly
        updated_state = node(current_state) 
        
        # Get the nested bronze metadata: {"table_name": {"col_name": "PII/PHI"}}
        bronze_metadata = getattr(updated_state, "sensitive_metadata", {}).get("bronze", {})
        
        enriched_mappings = []
        for row in (updated_state.mapping_rows or []):
            table_name = row.get("bronze_table")
            bronze_col = row.get("bronze_columns")
            
            # 1. Get the sensitivity map for THIS specific table
            table_sensitivity = bronze_metadata.get(table_name, {})
            
            # 2. Get the specific label for THIS column
            # This will now correctly find "PII" or "PHI"
            label = table_sensitivity.get(bronze_col, "NON_SENSITIVE")
            
            row["classification"] = label
                
            enriched_mappings.append(row)

        # Update the state with the enriched rows and save
        updated_state.mapping_rows = enriched_mappings
        agent_states[state_id] = updated_state
        
        return MappingResponse(
            success=True,
            mapping_rows=enriched_mappings,
            message=f"{standard.upper()} Mapping generated with classifications"
        )

    except Exception as e:
        logger.error(f"Mapping error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/run-gold-mapping")
async def run_gold_mapping(catalog: str, schema: str):
    state_id = f"{catalog}_{schema}"
    if state_id not in agent_states:
        raise HTTPException(status_code=404, detail="State not found. Run Silver mapping first.")
    
    current_state = agent_states[state_id]
    
    try:
        # Run the Gold Mapper node
        new_state = gold_mapper_agent_node(current_state)
        agent_states[state_id] = new_state
        
        return {
            "success": True,
            "gold_mapping_rows": new_state.gold_mapping_rows,
            "message": "Gold mapping generated."
        }
    except Exception as e:
        logger.error(f"Gold Mapping Failed: {str(e)}", exc_info=True) 
        raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")