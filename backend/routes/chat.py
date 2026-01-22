from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Dict, Any
from typing import Optional


# Import your chat agents
from backend.agents.qna_agent import get_qna_agent
from backend.agents.supervisor_agent import get_supervisor_agent
from backend.agents.system_assessment_agent import get_system_assessment_agent

router = APIRouter()

# Import session store
from backend.routes.databricks import session_store

class ChatMessage(BaseModel):
    role: str
    content: str
    name: Optional[str] = None

class ChatRequest(BaseModel):
    message: str
    conversation_history: List[ChatMessage]
    session_id: str

@router.post("/message")
async def send_chat_message(request: ChatRequest):
    """Send message to chat agent"""
    try:
        if request.session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session_data = session_store[request.session_id]
        state = session_data.get("state")
        
        if not state:
            from agents.agent_state import AgentState
            state = AgentState(
                dfs=session_data.get("dfs", {}),
                df_heads=session_data.get("df_heads", {}),
                df_dtypes=session_data.get("df_dtypes", {})
            )
        
        # Add user message to state
        user_message = {
            "role": "user",
            "content": request.message
        }
        state.messages.append(user_message)
        state.ui_chat_history.append(user_message)
        
        # Get QnA agent (you can add supervisor logic here)
        qna_node, _ = get_qna_agent()
        
        # Run agent
        updated_state = qna_node(state)
        
        # Get last assistant message
        assistant_message = updated_state.messages[-1]
        
        # Store updated state
        session_store[request.session_id]["state"] = updated_state
        
        return {
            "success": True,
            "data": {
                "response": assistant_message.get("content", ""),
                "agent": assistant_message.get("name", "Assistant")
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.get("/system-assessment")
async def get_system_assessment(session_id: str):
    """Get system assessment"""
    try:
        if session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session_data = session_store[session_id]
        state = session_data.get("state")
        
        if not state:
            return {
                "success": False,
                "error": "No state found"
            }
        # Get system assessment agent
        system_assessment_node, _ = get_system_assessment_agent()
        # Run the agent
        updated_state = system_assessment_node(state)
        # Store updated state
        session_store[session_id]["state"] = updated_state
        return {
            "success": True,
            "data": {
                "assessment": updated_state.system_assessment
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
