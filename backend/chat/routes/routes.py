import logging
from fastapi import APIRouter, HTTPException
from typing import List

# Internal imports - ensure these paths match your folder structure
from backend.chat.schemas.schemas import ChatRequest, ChatResponse
from backend.chat_wrapper import LangGraphChatAgent
from backend.state_store import agent_states
from mlflow.types.agent import ChatAgentMessage

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/message", response_model=ChatResponse)
async def chat_with_agent(req: ChatRequest):
    try:
        # 1. Identify the state (e.g., catalog_schema) to retrieve context (DFs, mappings)
        state_id = f"{req.catalog}_{req.schema_name}"
        current_state = agent_states.get(state_id)

        if not current_state:
            logger.warning(f"Chat context not found for {state_id}. Agent may lack data context.")
        
        # 2. Initialize the Multi-Agent Wrapper
        # This uses the agent.py logic you uploaded
        chat_agent = LangGraphChatAgent(agent_state=current_state)

        # 3. Convert history to MLFlow format
        history = [
            ChatAgentMessage(role=m.role, content=m.content, name=m.name)
            for m in req.conversation_history
        ]
        
        # Newest user message
        user_msg = ChatAgentMessage(role="user", content=req.message)

        # 4. Predict (Supervisor -> Workers -> Final Answer)
        response = chat_agent.predict(messages=history + [user_msg])

        # 5. Extract the final assistant message
        if not response.messages:
            raise ValueError("No messages returned from agent")
            
        final_msg = response.messages[-1]

        return ChatResponse(
            response=final_msg.content,
            agent=final_msg.name or "Assistant"
        )

    except Exception as e:
        logger.error(f"Chat Error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/system-assessment")
async def get_system_assessment(catalog: str, schema_name: str):
    """
    Directly triggers the System Assessment agent for the current data state.
    """
    state_id = f"{catalog}_{schema_name}"
    state = agent_states.get(state_id)
    
    if not state:
        raise HTTPException(status_code=404, detail="No data context found. Please upload files first.")
    
    try:
        # Note: Importing inside the function to avoid circular imports
        from backend.agents.system_assessment_agent import system_assessment_agent_node
        
        result_state = system_assessment_agent_node(state)
        
        # Get the last message which contains the assessment text
        assessment_content = result_state.messages[-1]["content"]
        
        return {"response": assessment_content}
    except Exception as e:
        logger.error(f"Assessment Error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to generate system assessment.")