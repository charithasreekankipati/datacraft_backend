from fastapi import APIRouter, HTTPException
import logging
import re
from backend.business_kpis.schemas.schemas import (
    KPIAnalyzeResponse, 
    KPIGenerateRequest, 
    KPIGenerateResponse
)
from backend.business_kpis.analyze_schema_agent import (
    analyze_uploaded_dataframes_and_suggest_kpis,
    suggest_kpis_for_area
)
from backend.state_store import agent_states

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/analyze-schema", response_model=KPIAnalyzeResponse)
async def analyze_schema(catalog: str, schema_name: str):
    try:
        state_id = f"{catalog}_{schema_name}"
        state = agent_states.get(state_id)
        
        if not state or not state.dfs:
            raise HTTPException(status_code=404, detail="No data found in state.")

        analysis_result = analyze_uploaded_dataframes_and_suggest_kpis(state.dfs)
        
        # --- ROBUST PARSING START ---
        domain = "Unknown"
        areas = []
        
        # Look for "Domain: <Value>" anywhere in the text, ignoring bolding and case
        domain_match = re.search(r"Domain:\s*(.*)", analysis_result, re.IGNORECASE)
        if domain_match:
            # Remove any potential markdown asterisks and strip whitespace
            domain = domain_match.group(1).replace("*", "").strip()
            
        # Look for "Areas: <Value1>, <Value2>..."
        areas_match = re.search(r"Areas:\s*(.*)", analysis_result, re.IGNORECASE)
        if areas_match:
            areas_str = areas_match.group(1).replace("*", "").strip()
            areas = [a.strip() for a in areas_str.split(",")]
        # --- ROBUST PARSING END ---

        # Update state for persistence
        state.dataset_summary["domain"] = domain
        state.dataset_summary["suggested_areas"] = areas
        
        logger.info(f"Successfully parsed domain: {domain}")
        
        return KPIAnalyzeResponse(
            success=True,
            domain=domain,
            suggested_areas=areas,
            message="Schema analysis complete."
        )
    except Exception as e:
        logger.error(f"Analysis error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/generate", response_model=KPIGenerateResponse)
async def generate_kpis(req: KPIGenerateRequest):
    try:
        state_id = f"{req.catalog}_{req.schema_name}"
        state = agent_states.get(state_id)
        
        if not state or not state.dfs:
            raise HTTPException(status_code=404, detail="State or data not found.")

        kpis_text = suggest_kpis_for_area(state.dfs, req.domain, req.area)
        state.dataset_summary["kpis"] = kpis_text
        
        return KPIGenerateResponse(
            success=True,
            kpis=kpis_text,
            message=f"KPIs generated for {req.area}."
        )
    except Exception as e:
        logger.error(f"Generation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))