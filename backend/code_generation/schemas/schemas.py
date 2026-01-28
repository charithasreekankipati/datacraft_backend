from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class DQExpectationResponse(BaseModel):
    table: str
    rule_name: str
    condition: str
    enforcement: str  

class GenerateCodeRequest(BaseModel):
    pii_access_mode: str = "group"
    pii_access_value: str = "pii_access"
    phi_access_mode: str = "group"
    phi_access_value: str = "phi_access"
    catalog: str
    schema_name: str

class CodeGenerationResponse(BaseModel):
    success: bool
    pyspark_code: Optional[str] = None
    masking_sql: Optional[str] = None
    message: str

class ExecutionLogEntry(BaseModel):
    timestamp: str
    level: str  
    message: str

class ExecutionResponse(BaseModel):
    success: bool
    status: str  # SUCCESS, FAILED
    logs: List[ExecutionLogEntry]