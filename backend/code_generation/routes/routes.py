from fastapi import APIRouter, HTTPException, Query
import logging
from datetime import datetime
from typing import List

from backend.code_generation.schemas.schemas import (
    DQExpectationResponse, GenerateCodeRequest, 
    CodeGenerationResponse, ExecutionResponse
)
from backend.state_store import agent_states
from backend.code_generation.bricks_medallion_agent import bricks_medallion_agent_node
from backend.code_generation.masking_agent import get_masking_agent
from backend.code_generation.dq_expectations import generate_expectations_for_mapping
from backend.code_generation.run_masking_sql import execute_masking_sql as execute_sql_against_databricks

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/dq-expectations", response_model=List[DQExpectationResponse])
async def get_dq_expectations(catalog: str, schema: str):
    state_id = f"{catalog}_{schema}"
    state = agent_states.get(state_id)
    if not state:
        raise HTTPException(status_code=404, detail="State not found")
    
    # Generate expectations based on mapping rows
    mapping_rows = getattr(state, "mapping_rows", [])
    expectations_dict = generate_expectations_for_mapping(mapping_rows)
    
    response = []
    for table, rules in expectations_dict.items():
        for mode, rule_name, condition in rules:
            # Map internal modes (expect_or_drop) to UI labels (DROP)
            enforcement = "LOG"
            if "fail" in mode: enforcement = "FAIL"
            elif "drop" in mode: enforcement = "DROP"
            
            response.append(DQExpectationResponse(
                table=table,
                rule_name=rule_name,
                condition=condition,
                enforcement=enforcement
            ))
    return response

@router.post("/generate-medallion", response_model=CodeGenerationResponse)
async def generate_medallion(catalog: str = Query(...), schema: str = Query(...)):
    state_id = f"{catalog}_{schema}"
    state = agent_states.get(state_id)
    if not state:
        raise HTTPException(status_code=404, detail="State not found")

    # The Medallion Agent node processes the state and returns an updated state
    updated_state = bricks_medallion_agent_node(state)
    agent_states[state_id] = updated_state
    
    return CodeGenerationResponse(
        success=True,
        pyspark_code=updated_state.pyspark_code,
        message="Medallion PySpark code generated successfully."
    )

@router.post("/generate-masking", response_model=CodeGenerationResponse)
async def generate_masking(req: GenerateCodeRequest):
    state_id = f"{req.catalog}_{req.schema_name}"
    state = agent_states.get(state_id)
    if not state:
        raise HTTPException(status_code=404, detail="State not found")

    # Update state with masking preferences from UI
    state.pii_access_mode = req.pii_access_mode
    state.pii_access_value = req.pii_access_value
    state.phi_access_mode = req.phi_access_mode
    state.phi_access_value = req.phi_access_value

    masking_agent_fn, _ = get_masking_agent()
    updated_state = masking_agent_fn(state)
    agent_states[state_id] = updated_state

    return CodeGenerationResponse(
        success=True,
        masking_sql=getattr(updated_state, "masking_sql", ""),
        message="Masking SQL generated."
    )

@router.post("/execute-masking", response_model=ExecutionResponse)
async def execute_masking(catalog: str, schema: str):
    state_id = f"{catalog}_{schema}"
    state = agent_states.get(state_id)
    sql = getattr(state, "masking_sql", None)
    
    if not sql:
        raise HTTPException(status_code=400, detail="No masking SQL found to execute.")

    # Call the execution helper (run_masking_sql.py)
    # Note: In a real app, connection details come from env/secrets
    result = execute_sql_against_databricks(sql)
    
    logs = [
        ExecutionLogEntry(
            timestamp=datetime.now().strftime("%H:%M:%S"),
            level="info" if result["status"] == "ok" else "error",
            message=log_msg
        ) for log_msg in result.get("logs", [])
    ]

    return ExecutionResponse(
        success=result["status"] == "ok",
        status="SUCCESS" if result["status"] == "ok" else "FAILED",
        logs=logs
    )