from pydantic import BaseModel
from typing import Optional, List

class ModelingRequest(BaseModel):
    catalog: str
    schema_name: str
    schema_view: str  

class ModelingResponse(BaseModel):
    success: bool
    modeling_sql: Optional[str] = None
    message: Optional[str] = None