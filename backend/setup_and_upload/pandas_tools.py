import os
import io
import pandas as pd
import xml.etree.ElementTree as ET
from collections import Counter
from databricks.sdk import WorkspaceClient

SUPPORTED_EXTENSIONS = [".csv", ".xlsx", ".parquet", ".json", ".xml"]

def get_db_client(token=None):
    """Initializes the Databricks Workspace Client."""
    # It will automatically find your host from env vars or config
    # You can pass the token explicitly from your FastAPI route
    return WorkspaceClient(token=token)

def read_all_files(folder_path, token=None):
    """
    Modified to read from Databricks Volumes via SDK if running locally,
    or via OS if running on Databricks.
    """
    dfs = {}
    xml_root_tags = {}

    # Check if we are running inside Databricks or Locally
    is_databricks = os.path.exists("/databricks")
    
    if is_databricks:
        # Standard local logic for Databricks Runtime
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)]
        for file_path in files:
            process_file(file_path, dfs, xml_root_tags)
    else:
        # SDK logic for Local Machine
        w = get_db_client(token)
        # folder_path = "/Volumes/catalog/schema/volume/upload_..."
        list_content = w.files.list_directory_contents(folder_path)
        
        for file_info in list_content:
            base_name = file_info.name
            name_no_ext, ext = os.path.splitext(base_name)
            ext = ext.lower()
            
            if ext not in SUPPORTED_EXTENSIONS:
                continue

            # Download file content into memory
            response = w.files.download(file_info.path)
            file_bytes = io.BytesIO(response.contents.read())
            
            try:
                if ext == ".csv":
                    df = pd.read_csv(file_bytes)
                elif ext == ".xlsx":
                    df = pd.read_excel(file_bytes)
                elif ext == ".parquet":
                    df = pd.read_parquet(file_bytes)
                elif ext == ".json":
                    df = pd.read_json(file_bytes)
                elif ext == ".xml":
                    # For XML, we need a string or file-like object
                    df, root_tag = read_xml(file_bytes)
                    xml_root_tags[name_no_ext.lower()] = root_tag
                
                dfs[name_no_ext.lower()] = df
            except Exception as e:
                print(f"Error processing {base_name}: {e}")

    return dfs, xml_root_tags

def read_xml(file_source):
    """
    Modified to handle both file paths and ByteIO objects.
    """
    tree = ET.parse(file_source)
    root = tree.getroot()
    root_tag = root.tag

    child_tags = [child.tag for child in root]
    row_tag = Counter(child_tags).most_common(1)[0][0] if child_tags else root_tag

    # Reset pointer if it's BytesIO
    if hasattr(file_source, 'seek'):
        file_source.seek(0)

    try:
        df = pd.read_xml(file_source, xpath=f".//{row_tag}")
    except Exception:
        # Fallback to your recursive parse
        if hasattr(file_source, 'seek'): file_source.seek(0)
        rows = []
        def recursive_parse(element, parent_path=""):
            row = {}
            for child in element:
                path = f"{parent_path}/{child.tag}" if parent_path else child.tag
                if len(child):
                    row.update(recursive_parse(child, parent_path=path))
                else:
                    row[path] = child.text
            return row

        for elem in root.findall(f".//{row_tag}"):
            rows.append(recursive_parse(elem))
        df = pd.DataFrame(rows)

    return df, row_tag

# ... get_dataframes_head and other helper functions remain the same ...


def get_data_heads_and_dtypes(dfs):
    """
    Returns the first 5 rows and the data types for each dataframe.
    Includes the fix for numpy.int64 serialization errors.
    """
    heads = {}
    dtypes = {}
    for name, df in dfs.items():
        # The FIX: Convert all data to standard Python objects 
        # so FastAPI can send them as JSON without 'numpy.int64' errors
        heads[name] = df.head(5).astype(object).where(pd.notnull(df.head(5)), None)
        
        # Store data types as strings
        dtypes[name] = df.dtypes.apply(lambda x: str(x)).to_dict()
    
    return heads, dtypes