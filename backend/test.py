from fastapi import FastAPI, Query
from databricks import sql
import os
from databricks_langchain import DatabricksVectorSearch

app = FastAPI()

def get_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

@app.get("/nyctaxi")
def get_nyctaxi_trips(limit: int = 5):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM samples.nyctaxi.trips LIMIT ?", [limit])
    rows = cursor.fetchall()
    result = [row.asDict() for row in rows]
    cursor.close()
    conn.close()
    return {"data": result}

@app.get("/vector_search")
def vector_search(
    query: str = Query(..., description="Search query"),
    k: int = Query(5, description="Number of results"),
    index_name: str = Query("datacraft.default.cdm_fhir_resource", description="Vector index name")
):
    # Initialize inside the endpoint!
    vector_store = DatabricksVectorSearch(index_name=index_name)
    results = vector_store.similarity_search_with_score(query, k=k)
    output = [
        {
            "score": float(score),
            "metadata": doc.metadata,
            "text": doc.page_content
        }
        for doc, score in results
    ]
    return {"results": output}