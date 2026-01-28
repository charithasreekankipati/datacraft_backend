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
    catalog: Optional[str] = None
    schema_name: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    agent: str  