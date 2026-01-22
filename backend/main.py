from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import os
from dotenv import load_dotenv



from backend.setup_and_upload.routes.routes import router as setup_router
from backend.datalake_design.routes.routes import router as design_router
from backend.business_kpis.routes.routes import router as kpi_router
from backend.code_generation.routes.routes import router as codegen_router
from backend.data_modelling.routes.routes import router as modeling_router
from backend.chat.routes.routes import router as chat_router
from backend.metadata.routes.routes import router as metadata_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 DataCraft Backend starting...")
    yield

app = FastAPI(title="DataCraft API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(setup_router)
app.include_router(
    design_router, 
    prefix="/api/mapping", 
    tags=["Step 2: Data Lake Design"]
)
app.include_router(kpi_router, prefix="/api/kpi", tags=["KPI"])
app.include_router(codegen_router, prefix="/api/codegen", tags=["Code Generation"])
app.include_router(modeling_router)
app.include_router(chat_router, prefix="/api/chat", tags=["Chat"])
app.include_router(metadata_router, prefix="/api/databricks", tags=["Metadata"])

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)