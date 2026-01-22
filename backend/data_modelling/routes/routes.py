from fastapi import APIRouter, HTTPException, Query
import logging
from backend.data_modelling.schemas.schemas import ModelingRequest, ModelingResponse
from backend.data_modelling.dimensional_modeling_agent import get_dimensional_modeling_agent
# Import your central state store
from backend.state_store import agent_states 

router = APIRouter(prefix="/api/modeling", tags=["Modeling"])
logger = logging.getLogger(__name__)

@router.post("/generate", response_model=ModelingResponse)
async def generate_model(
    req: ModelingRequest, 
):
    # Construct state_id exactly like your code_generation routes
    state_id = f"{req.catalog}_{req.schema_name}"
    state = agent_states.get(state_id)
    
    if not state:
        raise HTTPException(
            status_code=404, 
            detail=f"State for {state_id} not found. Please run previous steps first."
        )

    try:
        # 1. Initialize the agent node
        modeling_node, _ = get_dimensional_modeling_agent()
        
        # 2. Update the view preference in the retrieved state
        state.modeling_schema_view = req.schema_view
        
        # 3. Execute the agent node using the PERSISTED state
        updated_state = modeling_node(state)
        
        # 4. Save the updated state back to the store
        agent_states[state_id] = updated_state
        
        # 5. Extract generated SQL
        generated_sql = getattr(updated_state, "modeling_sql", None)
        
        if not generated_sql:
            return ModelingResponse(
                success=False, 
                message="Agent failed to generate SQL. Check logs."
            )

        return ModelingResponse(
            success=True,
            modeling_sql=generated_sql
        )

    except Exception as e:
        logger.error(f"Modeling Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))