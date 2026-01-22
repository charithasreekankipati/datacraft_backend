from pydantic import BaseModel
from typing import List, Optional, Any

class ChatMessageSchema(BaseModel):
    role: str
    content: str
    name: Optional[str] = None
    timestamp: Optional[str] = None

class ChatRequest(BaseModel):
    message: str
    conversation_history: List[ChatMessageSchema]
    # These are needed to identify which data state the user is chatting about
    catalog: Optional[str] = None
    schema_name: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    agent: str  # The name of the specific agent that answered (QnA, SystemAssessment, etc.)