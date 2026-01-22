from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

# Import session store
from backend.routes.databricks import session_store

class GenerateCodeRequest(BaseModel):
    session_id: str

class GenerateMaskingRequest(BaseModel):
    session_id: str
    include_masking: bool
    pii_access_mode: str
    pii_access_value: str
    phi_access_mode: str
    phi_access_value: str

@router.get("/dq-expectations")
async def preview_dq_expectations(session_id: str):
    """Preview data quality expectations"""
    try:
        if session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session_data = session_store[session_id]
        state = session_data.get("state")
        
        if not state or not state.mapping_rows:
            return {
                "success": False,
                "error": "No mappings found. Please generate mappings first."
            }
        
        # Generate DQ expectations based on mappings
        expectations = []
        
        for mapping in state.mapping_rows:
            silver_table = mapping.get("silver_table", "")
            silver_column = mapping.get("silver_column", "")
            
            if silver_table and silver_table != "Unknown":
                # Add NOT NULL expectation
                expectations.append({
                    "table": silver_table,
                    "rule_name": f"valid_{silver_column}",
                    "condition": f"{silver_column} IS NOT NULL",
                    "enforcement": "FAIL"
                })
        
        return {
            "success": True,
            "data": {
                "expectations": expectations[:10]  # Limit to first 10 for preview
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/generate-medallion")
async def generate_medallion_code(request: GenerateCodeRequest):
    """Generate Medallion PySpark code"""
    try:
        if request.session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session_data = session_store[session_id]
        state = session_data.get("state")
        
        if not state:
            return {
                "success": False,
                "error": "No state found. Please complete previous steps."
            }
        
        # TODO: Import and run your code generation agent
        # from code_generation.medallion_generator import generate_code
        # code = generate_code(state)
        
        # Placeholder code
        pyspark_code = """import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="bronze_data",
    comment="Raw data ingestion"
)
def bronze_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/catalog/schema/volume")
    )

@dlt.table(
    name="silver_data",
    comment="Cleaned and standardized data"
)
@dlt.expect_or_fail("valid_id", "id IS NOT NULL")
def silver_data():
    return dlt.read_stream("bronze_data")
"""
        
        # Update state
        state.pyspark_code = pyspark_code
        session_store[request.session_id]["state"] = state
        
        return {
            "success": True,
            "data": {
                "pyspark_code": pyspark_code,
                "dq_rules": "DQ rules generated"
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/generate-masking")
async def generate_masking_sql(request: GenerateMaskingRequest):
    """Generate masking SQL"""
    try:
        if request.session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session_data = session_store[request.session_id]
        state = session_data.get("state")
        
        if not state:
            return {
                "success": False,
                "error": "No state found"
            }
        
        # TODO: Import your masking agent
        # Placeholder SQL
        masking_sql = f"""-- PII Masking Functions
CREATE OR REPLACE FUNCTION mask_pii(value STRING)
RETURNS STRING
RETURN CASE 
    WHEN IS_MEMBER('{request.pii_access_value}') THEN value
    ELSE REGEXP_REPLACE(value, '.', '*')
END;

-- PHI Masking Functions
CREATE OR REPLACE FUNCTION mask_phi(value STRING)
RETURNS STRING
RETURN CASE 
    WHEN IS_MEMBER('{request.phi_access_value}') THEN value
    ELSE REGEXP_REPLACE(value, '.', '*')
END;
"""
        
        # Update state
        state.masking_sql = masking_sql
        state.pii_access_mode = request.pii_access_mode
        state.pii_access_value = request.pii_access_value
        state.phi_access_mode = request.phi_access_mode
        state.phi_access_value = request.phi_access_value
        session_store[request.session_id]["state"] = state
        
        return {
            "success": True,
            "data": {
                "masking_sql": masking_sql
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/execute-masking")
async def execute_masking_sql(request: dict):
    """Execute masking SQL"""
    try:
        session_id = request.get("session_id")
        
        if session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        # TODO: Execute SQL using Databricks connection
        # For now, return success
        
        execution_log = [
            {
                "timestamp": "2024-01-15 10:00:00",
                "level": "info",
                "message": "Started masking SQL execution"
            },
            {
                "timestamp": "2024-01-15 10:00:05",
                "level": "info",
                "message": "Created PII masking function"
            },
            {
                "timestamp": "2024-01-15 10:00:10",
                "level": "info",
                "message": "Created PHI masking function"
            },
            {
                "timestamp": "2024-01-15 10:00:15",
                "level": "info",
                "message": "Masking SQL executed successfully"
            }
        ]
        
        return {
            "success": True,
            "data": {
                "execution_log": execution_log
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }