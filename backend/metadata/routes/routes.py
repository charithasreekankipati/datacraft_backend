from fastapi import APIRouter, HTTPException
from backend.metadata.schemas.schemas import MetadataRequest, MetadataResponse
from backend.metadata.table_metadata import fetch_databricks_metadata
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/metadata", response_model=MetadataResponse)
async def get_databricks_metadata(req: MetadataRequest):
    try:
        # Call the utility function from your table_metadata.py
        df = fetch_databricks_metadata(
            token=req.token,
            catalog=req.catalog,
            schema=req.schema
        )
        
        # Convert DataFrame to list of dictionaries for the response
        metadata_list = df.to_dict(orient="records")
        
        return MetadataResponse(
            success=True,
            data=metadata_list
        )
    except Exception as e:
        logger.error(f"Metadata fetch failed: {str(e)}")
        return MetadataResponse(
            success=False, 
            error=str(e)
        )