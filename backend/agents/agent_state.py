#agent_state.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List

# LangGraph state model
class AgentState(BaseModel):
    messages: list = Field(default_factory=list)
    ui_chat_history: List[Dict[str, Any]] = Field(default_factory=list)
    next_node: str = "supervisor"
    iteration_count: int = 0
    dfs: dict = Field(default_factory=dict)
    df_heads: dict = Field(default_factory=dict)
    df_dtypes: dict = Field(default_factory=dict)
    dbfs_path: str = ""
    file_types: Dict[str, str] = Field(default_factory=dict)
    xml_root_tags: Optional[Dict[str, str]] = Field(default_factory=dict)

    # --- RAG mapper outputs (persist these so  can read them) ---
    mapping_rows: List[Dict[str, Any]] = Field(default_factory=list)
    fhir_mapping_rows: List[Dict[str, Any]] = Field(default_factory=list)
    gold_mapping_rows: List[Dict[str, Any]] = Field(default_factory=list)

    llm_summaries: Dict[str, str] = Field(default_factory=dict)
    col_summaries: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    rag_evidence: Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]] = Field(default_factory=dict)

    modeling_sql: str = ""
    modeling_schema_view: str = ""

    domain: Optional[str] = None
    area: Optional[str] = None
    suggested_areas: Optional[List[str]] = None
    kpis: Optional[str] = None

    pii_columns: Optional[List[str]] = Field(default_factory=list)
    phi_columns: Optional[List[str]] = Field(default_factory=list)
    sensitive_metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

    pyspark_code: str = ""
    dq_rules: str = ""
    canvas_code: str = ""
    code_history: list = Field(default_factory=list)
    dataset_summary: Optional[Dict[str, Any]] = Field(default_factory=dict)

    # Masking agent outputs
    masking_sql: Optional[str] = None
    masking_sql_lines: Optional[List[str]] = Field(default_factory=list)
    masking_version: int = 0

    # Execution state + structured log entries
    mask_execution_status: str = "NOT_STARTED"   # "NOT_STARTED" | "RUNNING" | "SUCCESS" | "FAILED"
    mask_execution_log: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Run-time / target configuration
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    pii_access_group: Optional[str] = "pii_access"
    phi_access_group: Optional[str] = "phi_access"
    target_catalog: Optional[str] = None
    target_schema: Optional[str] = None

    pii_access_mode: Optional[str] = "group"
    pii_access_value: Optional[str] = "pii_access"
    pii_access_user: Optional[str] = None
    phi_access_mode: Optional[str] = "group"
    phi_access_value: Optional[str] = "phi_access"
    phi_access_user: Optional[str] = None

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True
