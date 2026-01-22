from pydantic import BaseModel
from typing import List, Optional

class MetadataRequest(BaseModel):
    catalog: str
    schema: str  
    token: str

class TableMetadata(BaseModel):
    Table: str
    Columns: str

class MetadataResponse(BaseModel):
    success: bool
    data: Optional[List[TableMetadata]] = None
    error: Optional[str] = None