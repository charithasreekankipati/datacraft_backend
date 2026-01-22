from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
from datetime import datetime

# Import your existing code
from backend.setup_and_upload.db_utils import (
    create_catalog,
    create_schema,
    create_volume_in_schema
)
from backend.setup_and_upload.pandas_tools import (
    read_all_files,
    get_dataframes_head,
    get_dataframes_dtypes
)
from backend.setup_and_upload.classify_sensitive import classify_column

router = APIRouter()

# Pydantic models for request validation
class CatalogRequest(BaseModel):
    catalog_name: str
    schema_name: str
    volume_name: str
    token: str

class ReadFilesRequest(BaseModel):
    dbfs_path: str

class MetadataRequest(BaseModel):
    catalog: str
    schema: str
    token: str

# Store session data (use Redis/Database in production)
session_store = {}

@router.post("/create-catalog")
async def create_catalog_endpoint(request: CatalogRequest):
    """Create Databricks catalog"""
    try:
        create_catalog(request.catalog_name, request.token)
        
        return {
            "success": True,
            "message": f"Catalog '{request.catalog_name}' created successfully"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/create-schema")
async def create_schema_endpoint(request: CatalogRequest):
    """Create Databricks schema"""
    try:
        create_schema(request.catalog_name, request.schema_name, request.token)
        
        return {
            "success": True,
            "message": f"Schema '{request.schema_name}' created successfully"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/create-volume")
async def create_volume_endpoint(request: CatalogRequest):
    """Create Databricks volume"""
    try:
        create_volume_in_schema(
            request.catalog_name,
            request.schema_name,
            request.volume_name,
            request.token
        )
        
        return {
            "success": True,
            "message": f"Volume '{request.volume_name}' created successfully"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/upload-file")
async def upload_file(
    file: UploadFile = File(...),
    catalog: str = Form(...),
    schema: str = Form(...),
    volume: str = Form(...),
    token: str = Form(...)
):
    """Upload file to Databricks volume"""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.runtime import dbutils
        
        # Create volume path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        upload_folder = f"upload_{timestamp}"
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{upload_folder}"
        
        # Upload file to DBFS
        file_content = await file.read()
        file_path = f"{volume_path}/{file.filename}"
        
        # Use dbutils to write file
        dbutils.fs.mkdirs(volume_path)
        dbutils.fs.put(file_path, file_content.decode('utf-8') if file.filename.endswith('.csv') else file_content, overwrite=True)
        
        return {
            "success": True,
            "message": "File uploaded successfully",
            "data": {
                "dbfs_path": volume_path,
                "file_name": file.filename
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/read-files")
async def read_files(request: ReadFilesRequest):
    """Read files from DBFS and return DataFrame info"""
    try:
        # Use your existing pandas_tools code
        dfs, xml_root_tags = read_all_files(request.dbfs_path)
        
        if not dfs:
            return {
                "success": False,
                "error": "No files found or unable to read files"
            }
        
        # Get DataFrame heads and dtypes
        df_heads = {}
        df_dtypes = get_dataframes_dtypes(dfs)
        
        for name, df in dfs.items():
            # Add sensitivity classification to columns
            classified_columns = {}
            for col in df.columns:
                classification = classify_column(col)
                classified_columns[col] = classification
            
            df_heads[name] = {
                "columns": df.columns.tolist(),
                "data": df.head(5).to_dict(orient='records'),
                "classifications": classified_columns
            }
        
        # Store in session for later use
        session_id = request.dbfs_path.split('/')[-1]
        session_store[session_id] = {
            "dfs": dfs,
            "df_heads": df_heads,
            "df_dtypes": df_dtypes,
            "xml_root_tags": xml_root_tags,
            "dbfs_path": request.dbfs_path
        }
        
        return {
            "success": True,
            "data": {
                "dfs": {name: df.to_dict() for name, df in dfs.items()},
                "df_heads": df_heads,
                "df_dtypes": df_dtypes,
                "xml_root_tags": xml_root_tags
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/metadata")
async def get_table_metadata(request: MetadataRequest):
    """Get Databricks table metadata"""
    try:
        from setup_and_upload.db_utils import get_databricks_connection
        
        connection = get_databricks_connection(request.token)
        cursor = connection.cursor()
        
        # Get all tables in schema
        cursor.execute(f"SHOW TABLES IN {request.catalog}.{request.schema}")
        tables = cursor.fetchall()
        
        result_tables = []
        for table_row in tables:
            table_name = table_row[1]  # Table name is usually in second column
            
            # Get columns for each table
            cursor.execute(f"DESCRIBE TABLE {request.catalog}.{request.schema}.{table_name}")
            columns = cursor.fetchall()
            
            result_tables.append({
                "name": table_name,
                "columns": [
                    {"name": col[0], "type": col[1]}
                    for col in columns
                ]
            })
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "data": {"tables": result_tables}
        }
    except Exception as e:
        return {
            "success": False,
            "error": "Incorrect Databricks details. Please verify and try again."
        }