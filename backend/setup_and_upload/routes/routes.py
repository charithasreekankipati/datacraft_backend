import os
import shutil
import pandas as pd
from datetime import datetime
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Body
from backend.setup_and_upload.db_utils import create_catalog, create_schema, create_volume_in_schema
from backend.setup_and_upload.pandas_tools import read_all_files, get_data_heads_and_dtypes
from backend.agents.agent_state import AgentState
from backend.state_store import agent_states
from backend.setup_and_upload.schemas.schemas import (
    CreateCatalogRequest,
    CreateSchemaRequest,
    CreateVolumeRequest,
    MetadataRequest,
    ReadFilesRequest
)


router = APIRouter(prefix="/api/databricks", tags=["setup_and_upload"])

@router.post("/create-catalog")
async def api_create_catalog(req: CreateCatalogRequest):
    res = create_catalog(req.catalog, req.token)
    if not res["success"]: raise HTTPException(status_code=500, detail=res["error"])
    return {"status": "success", "catalog": req.catalog}
@router.post("/create-schema")
async def api_create_schema(req: CreateSchemaRequest):
    res = create_schema(req.catalog, req.schema_name, req.token)
    return {"success": res["success"], "error": res.get("error")}

@router.post("/create-volume")
async def api_create_volume(req: CreateVolumeRequest):
    res = create_volume_in_schema(req.catalog, req.schema_name, req.volume, req.token)
    return {"success": res["success"], "error": res.get("error")}

@router.post("/upload-file")
async def api_upload_file(
    file: UploadFile = File(...),
    catalog: str = Form(...),
    schema: str = Form(...),
    volume: str = Form(...),
    token: str = Form(...)
):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"upload_{timestamp}"
    
    temp_dir = "temp_uploads"
    os.makedirs(temp_dir, exist_ok=True)
    local_path = os.path.join(temp_dir, file.filename)
    
    with open(local_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        final_filename = file.filename
        if file.filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(local_path)
            final_filename = file.filename.rsplit('.', 1)[0] + ".csv"
            local_path = os.path.join(temp_dir, final_filename)
            df.to_csv(local_path, index=False)

        dbfs_folder = f"/Volumes/{catalog}/{schema}/{volume}/{folder_name}"
        dbfs_dest = f"{dbfs_folder}/{final_filename}"
        
        from databricks.sdk.runtime import dbutils
        dbutils.fs.mkdirs(dbfs_folder)
        dbutils.fs.cp(f"file:{os.path.abspath(local_path)}", dbfs_dest)
        
        return {
            "success": True,
            "dbfs_path": dbfs_folder,  # Lift this out of the 'data' nesting
            "data": {
                "dbfs_path": dbfs_folder,
                "filename": final_filename
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if os.path.exists(local_path): os.remove(local_path)

@router.post("/read-files")
async def api_read_files(req: ReadFilesRequest):
    try:
        # 1. Read files and get basic metadata
        dfs, xml_root_tags = read_all_files(req.dbfs_path)
        heads, dtypes = get_data_heads_and_dtypes(dfs)
        
        from backend.setup_and_upload.classify_sensitive import classify_column, sanitize_llm_output
        
        formatted_heads = {}
        bronze_metadata = {}
        all_pii = []
        all_phi = []

        for name, df in heads.items():
            tag_row = {}
            for col in df.columns:
                label = sanitize_llm_output(classify_column(col))
                tag_row[col] = label
                if label == "PII": all_pii.append(col)
                if label == "PHI": all_phi.append(col)
            
            bronze_metadata[name] = tag_row
            formatted_heads[name] = {
                "columns": df.columns.tolist(),
                "data": df.to_dict(orient='records'),
                "sensitivity": tag_row
            }

        # --- THE FIX: STORE THE STATE ---
        # 1. Create the unique ID (Must match the one in generate-silver)
        state_id = f"{req.catalog}_{req.schema_name}"
        
        # 2. Create the AgentState instance
        # We store the actual DataFrames (dfs) so 'generate-silver' doesn't have to reconstruct them
        new_state = AgentState(
            dbfs_path=req.dbfs_path,
            dfs=dfs,  
            df_heads=formatted_heads,
            df_dtypes=dtypes,
            xml_root_tags=xml_root_tags,
            pii_columns=sorted(list(set(all_pii))),
            phi_columns=sorted(list(set(all_phi))),
            sensitive_metadata={"bronze": bronze_metadata}
        )

        # 3. Save to the global store
        agent_states[state_id] = new_state
        print(f"DEBUG: State initialized and saved for ID: {state_id}")

        return {
            "success": True,
            "data": {
                "success": True,
                "data": {
                    "dbfs_path": req.dbfs_path,
                    "df_heads": formatted_heads,
                    "df_dtypes": dtypes,
                    "pii_columns": new_state.pii_columns,
                    "phi_columns": new_state.phi_columns
                }
            }
        }
    except Exception as e:
        print(f"ERROR in read-files: {str(e)}")
        return {"success": False, "error": str(e)}