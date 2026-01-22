from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

# Import your existing KPI agents
from backend.business_kpis.analyze_schema_agent import (
    run_schema_analysis_agent,
    run_kpi_generation_agent
)

router = APIRouter()

# Import session store
from backend.routes.databricks import session_store

class GenerateKPIRequest(BaseModel):
    domain: str
    area: str
    session_id: str

@router.post("/analyze-schema")
async def analyze_schema(request: dict):
    """Analyze schema and detect business domain"""
    try:
        session_id = request.get("session_id")
        
        if not session_id or session_id not in session_store:
            return {
                "success": False,
                "error": "No data found. Please upload a file first."
            }
        
        session_data = session_store[session_id]
        dfs = session_data.get("dfs", {})
        
        if not dfs:
            return {
                "success": False,
                "error": "No dataframes available"
            }
        
        # Run schema analysis
        result = run_schema_analysis_agent(dfs)
        
        # Store in session
        if "state" not in session_store[session_id]:
            from agents.agent_state import AgentState
            session_store[session_id]["state"] = AgentState(
                dfs=dfs,
                df_heads=session_data.get("df_heads", {}),
                df_dtypes=session_data.get("df_dtypes", {})
            )
        
        return {
            "success": True,
            "data": {
                "content": result
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/generate")
async def generate_kpis(request: GenerateKPIRequest):
    """Generate KPIs for selected domain and area"""
    try:
        if request.session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session_data = session_store[request.session_id]
        dfs = session_data.get("dfs", {})
        
        if not dfs:
            return {
                "success": False,
                "error": "No dataframes available"
            }
        
        # Generate KPIs
        kpis = run_kpi_generation_agent(dfs, request.domain, request.area)
        
        # Update state
        state = session_data.get("state")
        if state:
            state.domain = request.domain
            state.area = request.area
            state.kpis = kpis
            session_store[request.session_id]["state"] = state
        
        return {
            "success": True,
            "data": {
                "kpis": kpis
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
