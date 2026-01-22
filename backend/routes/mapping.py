from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import tempfile
import os

# Import your existing agents
from backend.datalake_design.rag_mapper_agent import (
    get_rag_mapper_agent,
    get_custom_schema_rag_mapper_agent,
    rerun_rag_for_columns,
    apply_bulk_manual_mappings
)
from backend.datalake_design.gold_mapper_agent import get_gold_mapper_agent
from backend.agents.agent_state import AgentState

router = APIRouter()

class GenerateMappingRequest(BaseModel):
    standard: str
    session_id: Optional[str] = None

class RerunRAGRequest(BaseModel):
    df_name: str
    col_names: List[str]
    standard: str
    session_id: str

class UpdateMappingsRequest(BaseModel):
    mappings: List[Dict[str, Any]]
    session_id: str

# Import session store from databricks route
from backend.routes.databricks import session_store

@router.post("/generate-silver")
async def generate_silver_mappings(
    standard: str = Form(...),
    file: Optional[UploadFile] = File(None),
    session_id: Optional[str] = Form(None)
):
    """Generate Bronze to Silver mappings using RAG"""
    try:
        # Get session data
        if not session_id or session_id not in session_store:
            return {
                "success": False,
                "error": "No data uploaded. Please upload a file first."
            }
        
        session_data = session_store[session_id]
        
        # Create AgentState from session data
        state = AgentState(
            dfs=session_data["dfs"],
            df_heads=session_data["df_heads"],
            df_dtypes=session_data["df_dtypes"],
            dbfs_path=session_data["dbfs_path"],
            xml_root_tags=session_data.get("xml_root_tags", {})
        )
        
        # Handle custom schema upload
        if standard == "custom" and file:
            # Save uploaded schema file temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
                content = await file.read()
                tmp.write(content)
                tmp_path = tmp.name
            
            # Get custom schema mapper agent
            mapper_node, _ = get_custom_schema_rag_mapper_agent(tmp_path)
            
            # Clean up temp file
            os.unlink(tmp_path)
        else:
            # Get standard mapper agent (FHIR, ACORD, etc.)
            mapper_node, _ = get_rag_mapper_agent(standard=standard)
        
        # Run the mapper
        updated_state = mapper_node(state)
        
        # Store updated state
        session_store[session_id]["state"] = updated_state
        
        return {
            "success": True,
            "data": {
                "mapping_rows": updated_state.mapping_rows,
                "llm_summaries": updated_state.llm_summaries,
                "col_summaries": updated_state.col_summaries,
                "rag_evidence": updated_state.rag_evidence
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/generate-gold")
async def generate_gold_mappings(request: dict):
    """Generate Silver to Gold mappings"""
    try:
        session_id = request.get("session_id")
        
        if not session_id or session_id not in session_store:
            return {
                "success": False,
                "error": "No session found. Please generate silver mappings first."
            }
        
        session_data = session_store[session_id]
        state = session_data.get("state")
        
        if not state or not state.mapping_rows:
            return {
                "success": False,
                "error": "No silver mappings found. Please generate silver mappings first."
            }
        
        # Get gold mapper agent
        gold_mapper_node, _ = get_gold_mapper_agent()
        
        # Run the mapper
        updated_state = gold_mapper_node(state)
        
        # Store updated state
        session_store[session_id]["state"] = updated_state
        
        return {
            "success": True,
            "data": {
                "gold_mapping_rows": updated_state.gold_mapping_rows
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/rerun-rag")
async def rerun_rag_columns(request: RerunRAGRequest):
    """Re-run RAG for specific columns"""
    try:
        if request.session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session_data = session_store[request.session_id]
        state = session_data.get("state")
        
        if not state:
            return {
                "success": False,
                "error": "No state found in session"
            }
        
        # Re-run RAG for selected columns
        updated_state = rerun_rag_for_columns(
            state,
            df_name=request.df_name,
            col_names=request.col_names,
            standard=request.standard
        )
        
        # Store updated state
        session_store[request.session_id]["state"] = updated_state
        
        return {
            "success": True,
            "data": {
                "mapping_rows": updated_state.mapping_rows,
                "rag_evidence": updated_state.rag_evidence
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/update")
async def update_mappings(request: UpdateMappingsRequest):
    """Update mappings manually"""
    try:
        if request.session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session_data = session_store[request.session_id]
        state = session_data.get("state")
        
        if not state:
            return {
                "success": False,
                "error": "No state found in session"
            }
        
        # Update mappings
        state.mapping_rows = request.mappings
        
        # Store updated state
        session_store[request.session_id]["state"] = state
        
        return {
            "success": True,
            "message": "Mappings updated successfully"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }