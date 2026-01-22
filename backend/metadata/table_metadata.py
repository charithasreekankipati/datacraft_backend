# databricks_metadata.py
import requests
import pandas as pd

# Databricks config
host = "https://dbc-83de24b5-b7ed.cloud.databricks.com"



def fetch_databricks_metadata( token, catalog="workspace", schema="default"):
    """
    Fetch all tables and their columns (with schema/type) from Databricks Unity Catalog.
    Returns a pandas DataFrame or raises an exception if invalid credentials/schema.
    """
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{host}/api/2.1/unity-catalog/tables?catalog_name={catalog}&schema_name={schema}"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        tables = data.get("tables", [])
        if not tables:
            raise ValueError("No tables found for the given catalog and schema.")

        metadata = []
        for t in tables:
            table_name = t.get("name")
            columns = t.get("columns", [])
            column_details = ", ".join([
                f"{c['name']} ({c.get('type_text', 'unknown')})"
                for c in columns
            ])
            metadata.append({"Table": table_name, "Columns": column_details})

        return pd.DataFrame(metadata)

    except Exception as e:
        # Raise exception so  can handle it and show a one-line error
        raise RuntimeError(f"Failed to fetch metadata: {str(e)}")