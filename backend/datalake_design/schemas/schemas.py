from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class MappingRequest(BaseModel):
    catalog: str
    schema_name: str
    dbfs_path: Optional[str] = None
    standard: str = "FHIR"  # Default standard
    custom_schema_path: Optional[str] = None

class MappingRow(BaseModel):
    bronze_table: str
    bronze_columns: str
    silver_table: str
    silver_column: str

class GoldMappingRow(BaseModel):
    gold_table: str
    gold_column: str
    silver_table: str
    silver_column: str
    transformation: str

class MappingResponse(BaseModel):
    success: bool
    mapping_rows: List[Dict[str, Any]]
    message: str