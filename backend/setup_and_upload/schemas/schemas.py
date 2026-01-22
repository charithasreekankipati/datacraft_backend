from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from backend.agents.agent_state import AgentState # Import your existing class

class CreateCatalogRequest(BaseModel):
    catalog: str
    token: str

class CreateSchemaRequest(BaseModel):
    catalog: str
    schema_name: str
    token: str

class CreateVolumeRequest(BaseModel):
    catalog: str
    schema_name: str
    volume: str
    token: str

class MetadataRequest(BaseModel):
    catalog: str
    schema: str
    token: str

class ReadFilesRequest(BaseModel):
    dbfs_path: str
    catalog: str
    schema_name: str
    token: str

class APIResponse(BaseModel):
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class DatabricksRequest(BaseModel):
    catalog_name: str
    schema_name: str
    volume_name: str
    token: str