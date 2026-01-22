# agents/bricks_medallion_agent.py

from backend.agents.agent_state import AgentState
from backend.llm_provider import llm
from backend.code_generation.examples_snippets import *
from backend.code_generation.metadata_utils import *
from backend.code_generation.dq_expectations import generate_expectations_for_mapping, format_expectations_for_prompt
from backend.code_generation.mapping_processor import process_mappings_for_code_generation


def bricks_medallion_agent_node(state: AgentState) -> AgentState:
    """
    Generate end-to-end PySpark code for Databricks Medallion architecture
    (Bronze → Silver → Gold) using mapping_rows, gold_mapping_rows, and KPIs.
    """

    messages = state.messages or []
    available_dfs = list(state.dfs.keys())
    user_dbfs_path = state.dbfs_path

    mapping_rows = getattr(state, "mapping_rows", [])
    gold_mapping_rows = getattr(state, "gold_mapping_rows", [])
    kpis = getattr(state, "kpis", "No KPIs generated yet.")
    df_dtypes = getattr(state, "df_dtypes", {})
    pii_columns = getattr(state, "pii_columns", [])
    phi_columns = getattr(state, "phi_columns", [])
    file_types = getattr(state, "file_types", {})

    # Normalize file types to mapping (filename -> format)
    file_type_summary = "\n".join(
        f"- {file_name}: {file_ext}" for file_name, file_ext in file_types.items()
    ) if file_types else "No file type information available"

    #xml_tags
    xml_root_tags = getattr(state, "xml_root_tags", {})
    xml_tag_summary = "\n".join(  
            f"- {name}: {tag}" for name, tag in xml_root_tags.items()
    )
    
    # Create a lookup for file formats by table name
    file_format_lookup = {}
    for file_name, file_ext in file_types.items():
        # Extract base name without extension
        base_name = file_name.rsplit('.', 1)[0].lower()
        file_format_lookup[base_name] = file_ext

    # Process mappings to resolve duplicates and group columns
    processed = process_mappings_for_code_generation(
        mapping_rows=mapping_rows,
        gold_mapping_rows=gold_mapping_rows,
        strategy="group"  # Combine columns into single tables
    )
    
    # Use processed mappings
    cleaned_mapping_rows = processed["resolved_mappings"]
    grouped_silver = processed["silver_grouped"]
    silver_tables = processed["silver_tables"]
    join_keys = processed["join_keys"]
    formatted_mappings = processed["formatted_prompt"]
    split_detected = processed.get("split_detected", {})
    duplicate_columns = processed.get("duplicate_columns", {})
    
    # Warn about duplicates found
    if processed["duplicates_found"]:
        print(f"WARNING: Duplicate table names resolved: {processed['duplicates_found']}")
    
    # CRITICAL: Display consolidation info if single Bronze split was detected
    if split_detected.get("is_split"):
        print(f"\n🚨 CRITICAL FIX APPLIED:")
        print(f"   - Detected multiple Silver tables from single Bronze source")
        print(f"   - Bronze: {split_detected['bronze_source']}")
        print(f"   - Original Silver tables: {', '.join(split_detected['silver_tables'])}")
        print(f"   - Consolidated into: {split_detected['consolidated_name']}")
        print(f"   - This prevents join failures in Gold layer!\n")
    
    # CRITICAL: Display duplicate column fix info
    if duplicate_columns:
        print(f"\n DUPLICATE COLUMNS FIX APPLIED:")
        for table, dups in duplicate_columns.items():
            print(f"   - Table '{table}' had {len(dups)} duplicate column(s)")
            for dup in dups:
                print(f"     * '{dup['silver_column']}' from: {', '.join(dup['bronze_columns'])}")
        print(f"   - All columns now have unique names with bronze suffixes\n")
        
    # Generate DLT expectations automatically from CLEANED mappings
    expectations_dict = generate_expectations_for_mapping(
        mapping_rows=cleaned_mapping_rows,
        df_dtypes=df_dtypes,
        pii_columns=pii_columns,
        phi_columns=phi_columns
    )
    expectations_text = format_expectations_for_prompt(expectations_dict)
    
#     prompt = f"""
# You are a Databricks coding assistant. Generate end-to-end PySpark code using Databrics Lakeflow Declarative Pipeline syntax(@dlt) for Medallion architecture
# (Bronze → Silver → Gold) using the following context.

# Available DataFrames (Bronze layer): {available_dfs}

# Mapping for Silver layer transformations:
# {mapping_rows}

# Business KPIs for Gold layer (aggregations):
# {kpis}

# Rules:
# - Only return valid PySpark code in Databricks syntax
# - Use Delta tables for all reads/writes
# - Include Bronze ingestion, Silver transformations, and Gold aggregations
# - Do not explain
# """

    # Include gold mappings in prompt if available
    gold_context = ""
    if gold_mapping_rows:
        gold_context = f"\n\nGold layer mappings (Silver → Gold transformations):\n{gold_mapping_rows}"

    # Build enhanced context with grouped mappings
    mapping_context = f"""
GROUPED SILVER TABLES (combine these columns into single tables):
{formatted_mappings}

JOIN KEYS for Gold Layer:
{join_keys}

Original Mapping Rows (for reference):
{cleaned_mapping_rows}
"""

    prompt = f"""
    You are a Databricks coding assistant. Write COMPLETE PySpark code using Lakeflow Declarative Pipeline (@dlt) syntax to implement the full Medallion architecture (Bronze → Silver → Gold).

    Context:
    - Available Bronze DataFrames: {available_dfs}
    - Silver layer mappings (GROUPED - combine columns into single tables):
{mapping_context}{gold_context}
    - Gold layer business KPIs (aggregations): {kpis}
    - DATA QUALITY EXPECTATIONS (MUST INCLUDE):
    {expectations_text}
    - User-specified DBFS path: {user_dbfs_path}

    CRITICAL ARCHITECTURE PATTERN - CREATE CONNECTED PIPELINE
    
    **CRITICAL: The GROUPED structure above tells you to CONSOLIDATE columns into SINGLE tables.**
    
    DO NOT create separate Silver tables for each FHIR resource shown in mappings!
    Instead, create ONE primary Silver table per Bronze source with ALL columns.
    
    Example - If mappings show:
      - bronze_appointments → Appointment (columns: id, date, status)
      - bronze_appointments → Patient (columns: patient_id, age, gender)
      - bronze_appointments → Encounter (columns: clinic_id, diagnosis)
    
    WRONG - Creates disconnected graph:
      silver_appointment (only appointment columns)
      silver_patient (only patient columns)
      silver_encounter (only encounter columns)
    
    CORRECT - Creates connected pipeline:
      silver_appointments (ALL columns: id, date, status, patient_id, age, gender, clinic_id, diagnosis)
    
    **Pipeline Graph Must Show:**
    bronze_{{source}} → silver_{{source}} → gold_{{metrics}}
    
    NOT: bronze_{{source}} → [multiple disconnected silver tables] → [floating gold tables]

    CRITICAL RULES - MUST FOLLOW EXACTLY
    
    1. **TABLE NAMING**: ALL table names MUST be lowercase with underscores
       - Bronze: bronze_{{filename}} (e.g., bronze_providers)
       - Silver: silver_{{entity}}_{{context}} (e.g., silver_practitioner_profile)
       - Gold: gold_{{type}}_{{entity}} (e.g., gold_dim_provider)
       - NEVER use mixed case like "silver_Encounter" - use "silver_encounter"
    
    2. **COLUMN NAMING - ABSOLUTELY CRITICAL - NO DOTS ALLOWED**: 
       WRONG: F.col("status").alias("Flag.status")        # Has dot - WILL FAIL!
       WRONG: F.col("status").alias("appointment.status")  # Has dot - WILL FAIL!
       WRONG: F.col("start").alias("Appointment.start")    # Has dot - WILL FAIL!
       
       CORRECT: F.col("status").alias("flag_status")           # Underscores only
       CORRECT: F.col("status").alias("appointment_status")    # Underscores only
       CORRECT: F.col("start").alias("appointment_start")      # Underscores only
       
       RULES:
       - Replace ALL dots (.) with underscores (_) in EVERY .alias() call
       - Example: "practitioner.identifier" → "practitioner_identifier"
       - Example: "Flag.status" → "flag_status"
       - Example: "Appointment.start" → "appointment_start"
       - Example: "observation.value.x" → "observation_value_x"
       - If the mapping already has underscores (e.g., "identifier_clinic_id"), keep it as-is
       - NEVER EVER create columns with dots - PySpark treats dots as nested struct paths
       - Scan EVERY .alias() in your generated code and remove ALL dots
    
    3. **DUPLICATE COLUMN PREVENTION - CRITICAL**:
       The mappings have been PRE-PROCESSED to add unique suffixes (e.g., "_clinic_id").
       However, the LLM MUST STILL validate and ensure NO duplicates exist!
       
       **THIS IS THE #1 CAUSE OF PIPELINE FAILURES - PAY CLOSE ATTENTION!**
       
       WRONG - Multiple columns with same alias name:
       ```python
       .select(
           F.col("workspace_id").alias("id"),        # Duplicate!
           F.col("cluster_id").alias("id"),          # Duplicate!
           F.col("start_time").alias("time"),        # Duplicate!
           F.col("end_time").alias("time")           # Duplicate!
       )
       ```
       
       CORRECT Option 1 - Use source column names:
       ```python
       .select(
           F.col("workspace_id").alias("workspace_id"),  # Unique
           F.col("cluster_id").alias("cluster_id"),      # Unique
           F.col("start_time").alias("start_time"),      # Unique
           F.col("end_time").alias("end_time")           # Unique
       )
       ```
       
       CORRECT Option 2 - Add descriptive suffixes:
       ```python
       .select(
           F.col("workspace_id").alias("id_workspace"),  # Unique
           F.col("cluster_id").alias("id_cluster"),      # Unique
           F.col("start_time").alias("time_start"),      # Unique
           F.col("end_time").alias("time_end")           # Unique
       )
       ```
       
       CORRECT Option 3 - Keep deduplicated names from mappings:
       ```python
       .select(
           F.col("status").alias("status_flag_status"),              # From mapping
           F.col("appointment_status").alias("status_appt_status"),  # From mapping
           F.col("start").alias("start_period_start"),               # From mapping
           F.col("appointment_start").alias("start_appt_start")      # From mapping
       )
       ```
       
       **MANDATORY ALGORITHM FOR EVERY .select() STATEMENT:**
       ```
       Step 1: List all .alias("target_name") values
       Step 2: Check for duplicates in the list
       Step 3: If duplicates exist:
               - Add source column context (workspace_id → "workspace_id")
               - OR add descriptive suffix (workspace_id → "id_workspace")
               - OR use mapping's deduplicated name
       Step 4: Re-check - ensure ALL aliases are now unique
       ```
       
       RULES:
       - NEVER create two .alias() calls with the same target column name
       - If multiple bronze columns map to similar silver names, ADD CONTEXT
       - Common duplicate names: id, name, time, status, type, date, value
       - When mapping says "id_workspace_id", USE IT EXACTLY (keeps the suffix)
       - When in doubt: Use the full source column name as the alias
    
    4. **ROW_ID USAGE - CRITICAL JOIN RULE**:
       row_id is ONLY needed when performing JOINS between tables.
       
       WRONG - Selecting row_id when NOT joining:
       ```python
       def gold_dim_appointment():
           # NO joins here, so row_id not needed!
           return dlt.read("silver_appointment").select(
               F.col("row_id"),  # Unnecessary!
               F.col("appointment_id"),
               F.col("appointment_date")
           )
       ```
       
       CORRECT - NO row_id when no joins:
       ```python
       def gold_dim_appointment():
           # Simple select, no joins - don't include row_id
           return dlt.read("silver_appointment").select(
               F.col("appointment_id"),
               F.col("appointment_date"),
               F.col("appointment_status")
           )
       ```
       
       CORRECT - Include row_id ONLY when joining:
       ```python
       def gold_fact_appointment_metrics():
           # Joining two tables - MUST include row_id
           appointment_df = dlt.read("silver_appointment").select(
               F.col("row_id"),  # Needed for join
               F.col("appointment_id"),
               F.col("appointment_date")
           )
           
           patient_df = dlt.read("silver_patient").select(
               F.col("row_id"),  # Needed for join
               F.col("patient_id"),
               F.col("patient_age")
           )
           
           return appointment_df.join(patient_df, on="row_id", how="left")
       ```
       
       RULES:
       - If NO .join() in function: DON'T select row_id
       - If .join() exists: MUST select row_id from ALL tables being joined
       - row_id is for internal joins only, not for end-user analytics
    
    5. **UNIQUE TABLE NAMES**: If multiple columns map to the same resource:
       - The mappings have already been deduplicated at the TABLE level
       - Column names like "identifier_clinic_id" and "identifier_npi_number" are intentional
       - Keep these unique column names as provided - DO NOT merge them back
       - Each deduplicated column represents different data from different sources
    
    5. **BRONZE LAYER (STREAMING INGESTION)**: 
    BRONZE FORMAT MUST MATCH FILE EXTENSION EXACTLY. No format guessing. 
    If file is `.xml` → use `.option("cloudFiles.format","xml")` + correct `rowTag`. 
    Do NOT generate CSV ingestion unless extension is `.csv`. This is mandatory.
       - Use lowercase table names (e.g., bronze_providers, bronze_patients)
       - MUST use spark.readStream for continuous file ingestion (NOT dlt.read)
       - Use cloudFiles for auto schema inference and evolution
       - File types available: {file_type_summary}
       - For CSV: cloudFiles.format="csv", header=true, inferSchema=true
       - For JSON: cloudFiles.format="json", inferSchema=true, .option("multiline", "true")
       - For Parquet: cloudFiles.format="parquet" --> (very strict)
       - For XML : use rowtag {xml_tag_summary}
             Example: 
             ```python
             @dlt.table(
                 name="bronze_databricks",
                 comment="Raw streaming data from Databricks",
                 table_properties={{"quality": "bronze"}}
             )
             def bronze_databricks():
                 df = (
                     spark.readStream.format("cloudFiles")
                     .option("cloudFiles.format", "xml")
                     .option("header", "true")
                     .option("rowTag", "record")  # replace with the detected rowTag from {xml_tag_summary} when available
                     .option("inferSchema", "true")
                     .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                     .option("cloudFiles.schemaLocation", f"{user_dbfs_path}/_schemas/")
                     .load(f"{user_dbfs_path}")
                 )
                 # Some XMLs may surface a pseudo column like '<?xml...'; drop those
                 valid_columns = [col for col in df.columns if not col.startswith("<?xml")]
                 return df.select(*valid_columns)
             ```
       - Path: {user_dbfs_path}
       - Schema location: {user_dbfs_path}/_schemas/
       - Example:
         ```python
         @dlt.table(name="bronze_providers", comment="Raw streaming data", table_properties={{"quality": "bronze"}})
         def bronze_providers():
             return (
                 spark.readStream.format("cloudFiles")  # STREAMING
                 .option("cloudFiles.format", "csv")
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                 .option("cloudFiles.schemaLocation", "{user_dbfs_path}/_schemas/")
                 .load("{user_dbfs_path}")
             )
         ```
    
    6. **SILVER LAYER (MATERIALIZED VIEWS) - CRITICAL ARCHITECTURE RULES**:
       
       **SINGLE CSV = SINGLE SILVER TABLE** (DO NOT SPLIT!)
       
       If mappings show ONE source CSV (e.g., appointments.csv, providers.csv), you MUST create 
       ONE primary Silver table with ALL columns from that CSV. DO NOT split into multiple Silver tables.
       
       **CRITICAL RULE FOR SINGLE CSV FILES:**
       - If ALL Silver tables read from the SAME Bronze source (e.g., bronze_providers)
       - DO NOT create multiple Silver tables - consolidate into ONE
       - Creating multiple Silver tables from one Bronze source breaks joins
       - row_id from monotonically_increasing_id() is ONLY valid within the SAME table
       - NEVER join tables on row_id if they come from SEPARATE .select() operations
       - row_id values are independent per Silver table and DO NOT correlate
       - Natural business keys (like provider_id) only work if the column exists in BOTH tables
       - Best practice: ONE Bronze source = ONE Silver table with ALL columns
       
       WRONG PATTERN - Splitting single CSV into multiple tables:
       ```python
       # This creates disconnected graph!
       def silver_appointment():
           return dlt.read("bronze_appointments").select(
               F.col("appointment_id"),
               F.col("appointment_date")
           )
       
       def silver_patient():  # Reading same Bronze again!
           return dlt.read("bronze_appointments").select(
               F.col("patient_id"),
               F.col("patient_age")
           )
       
       # CANNOT JOIN - row_ids are independent!
       def gold_table():
           appt = dlt.read("silver_appointment").select(F.col("row_id"), ...)
           patient = dlt.read("silver_patient").select(F.col("row_id"), ...)
           return appt.join(patient, on="row_id")  # FAILS - row_ids don't match!
       ```
       
       CORRECT PATTERN - ONE Silver table per Bronze source:
       ```python
       # This creates connected pipeline: Bronze → Silver → Gold
       def silver_appointments():
           return dlt.read("bronze_appointments").select(
               F.col("appointment_id"),
               F.col("appointment_date"),
               F.col("patient_id"),
               F.col("patient_age"),
               F.col("provider_id"),
               F.col("clinic_id")
               # ALL columns from the CSV in ONE table
           ).withColumn("row_id", F.monotonically_increasing_id())
       ```
       
       **🚨 CRITICAL: SILVER LAYER DUPLICATE COLUMN PREVENTION **
       
       The mappings may contain MULTIPLE Bronze columns that map to the SAME Silver column name.
       You MUST ensure each .alias() in your .select() is UNIQUE!
       
       WRONG - Multiple bronze columns aliased to same silver name:
       ```python
       def silver_databricks():
           return dlt.read("bronze_databricks").select(
               F.col("workspace_id").alias("id"),         # Creates 'id'
               F.col("cluster_id").alias("id"),           # DUPLICATE 'id'!
               F.col("start_time").alias("time"),         # Creates 'time'
               F.col("end_time").alias("time")            # DUPLICATE 'time'!
           )
       ```
       
       CORRECT - Make each alias unique:
       ```python
       def silver_databricks():
           return dlt.read("bronze_databricks").select(
               F.col("workspace_id").alias("workspace_id"),     # Unique
               F.col("cluster_id").alias("cluster_id"),         # Unique
               F.col("start_time").alias("start_time"),         # Unique
               F.col("end_time").alias("end_time")              # Unique
           )
       ```
       
       OR use the deduplicated column names from mappings:
       ```python
       def silver_databricks():
           return dlt.read("bronze_databricks").select(
               F.col("workspace_id").alias("id_workspace_id"),  # Unique (keeps suffix)
               F.col("cluster_id").alias("id_cluster_id"),      # Unique (keeps suffix)
               F.col("start_time").alias("time_start_time"),    # Unique (keeps suffix)
               F.col("end_time").alias("time_end_time")         # Unique (keeps suffix)
           )
       ```
       
       **MANDATORY VALIDATION STEPS FOR SILVER TABLES:**
       1. Look at ALL .alias() calls in your .select() statement
       2. Check if ANY two aliases have the same target name
       3. If YES: Add a suffix or use the source column name to make it unique
       4. NEVER assume the mapping deduplicator fixed everything - YOU must ensure uniqueness
       5. When in doubt, use the original bronze column name as the alias
       
       **EXAMPLES OF COMMON DUPLICATES TO AVOID:**
       - `id` (from workspace_id, cluster_id, job_id, etc.)
       - `time` (from start_time, end_time, create_time, etc.)
       - `name` (from user_name, cluster_name, workspace_name, etc.)
       - `status` (from job_status, cluster_status, run_status, etc.)
       - `type` (from resource_type, cluster_type, instance_type, etc.)
       
       RULES FOR SILVER LAYER ARCHITECTURE:
       - ONE Bronze source (CSV) = ONE primary Silver table
       - Include ALL columns from mappings in that single Silver table
       - Only create multiple Silver tables if you have MULTIPLE Bronze sources
       - Use lowercase table names matching bronze references
       - MUST use dlt.read() to read from Bronze (NOT spark.readStream)
       - Silver tables are MATERIALIZED VIEWS, not streaming
       - Replace dots with underscores in ALL column aliases
       - Keep deduplicated column names (those with bronze column suffix) as-is
       - ALWAYS add row_id: .withColumn("row_id", F.monotonically_increasing_id())
       
       **CRITICAL: If mappings show multiple Silver tables from ONE Bronze source:**
       - The agent MUST consolidate them into a SINGLE Silver table
       - DO NOT generate separate tables like silver_location, silver_practitioner from bronze_providers
       - Generate ONE table: silver_providers with ALL mapped columns
       - This prevents join errors in Gold layer
       
       Example when you have MULTIPLE source files:
         ```python
         # providers.csv → Bronze → Silver
         @dlt.table(name="silver_providers")
         def silver_providers():
             return dlt.read("bronze_providers").select(
                 F.col("provider_id"),
                 F.col("provider_name"),
                 F.col("specialty")
             ).withColumn("row_id", F.monotonically_increasing_id())
         
         # appointments.csv → Bronze → Silver  
         @dlt.table(name="silver_appointments")
         def silver_appointments():
             return dlt.read("bronze_appointments").select(
                 F.col("appointment_id"),
                 F.col("provider_id"),
                 F.col("patient_id")
             ).withColumn("row_id", F.monotonically_increasing_id())
         ```
    
    7. **GOLD LAYER (MATERIALIZED VIEWS - AGGREGATED) - CONNECTED PIPELINE**:
       - Use lowercase table names (e.g., gold_dim_provider, gold_fact_appointments)
       - MUST use dlt.read() to read from Silver OR Bronze (NOT spark.readStream)
       - Gold tables are MATERIALIZED VIEWS for analytics
       - BE COLUMN-AWARE: Only reference columns that exist in the table you're reading
       
       **CREATE CONNECTED GRAPH** - Gold must read from Silver to show dependencies!
       
       **CRITICAL: row_id JOIN RULES**
       - row_id from monotonically_increasing_id() is UNIQUE PER TABLE
       - NEVER join Silver tables on row_id if they were created from separate .select() calls
       - row_id values DO NOT correlate between different Silver tables
       - Use NATURAL BUSINESS KEYS for joins (e.g., provider_id, patient_id, appointment_id)
       - ONLY use row_id for self-joins or when you're absolutely certain rows align 1-to-1
       
       FOR DIMENSIONAL TABLES (dim_*):
       - Read from Silver tables (NOT Bronze) to show Bronze→Silver→Gold flow
       - Simple dimension with NO JOINS: Just select columns from single Silver table
       - Dimension with JOINS: Join multiple Silver tables using NATURAL KEYS (provider_id, clinic_id, etc.)
       - Validate all referenced columns exist in source tables
       
       WRONG - Joining on provider_id that doesn't exist in right table:
       ```python
       def gold_dim_provider():
           # WRONG: provider_id not selected in organization_df
           provider_df = dlt.read("silver_practitioner").select(
               F.col("provider_id"),  # Has provider_id
               F.col("provider_name")
           )
           
           organization_df = dlt.read("silver_organization").select(
               F.col("is_accepting"),  # NO provider_id!
               F.col("license")
           )
           
           # FAILS - provider_id not in organization_df!
           return provider_df.join(organization_df, on="provider_id", how="left")
       ```
       
       CORRECT - Ensure join key exists in BOTH tables:
       ```python
       def gold_dim_provider():
           # MUST select provider_id in BOTH dataframes
           practitioner_df = dlt.read("silver_practitioner").select(
               F.col("provider_id"),  # Join key
               F.col("provider_name"),
               F.col("license_number")
           )
           
           role_df = dlt.read("silver_practitionerrole").select(
               F.col("provider_id"),  # MUST include same join key
               F.col("specialty"),
               F.col("languages_spoken")
           )
           
           # Now join works - provider_id exists in BOTH
           return practitioner_df.join(role_df, on="provider_id", how="left")
       ```
       
       BEST PRACTICE - Don't split if from same source:
       ```python
       # If all data comes from bronze_providers, create ONE Silver table
       def silver_providers():
           return dlt.read("bronze_providers").select(
               F.col("provider_id"),
               F.col("provider_name"),
               F.col("specialty"),
               F.col("languages_spoken"),
               F.col("license_number")
               # ALL columns together
           ).withColumn("row_id", F.monotonically_increasing_id())
       
       # Then Gold just reads from that one table - no joins needed!
       def gold_dim_provider():
           return dlt.read("silver_providers").select(
               F.col("provider_id"),
               F.col("provider_name"),
               F.col("specialty"),
               F.col("languages_spoken")
           )
       ```
       
       FOR FACT TABLES (fact_*):
       - PREFER reading from Silver (shows dependency chain)
       - Can join multiple Silver tables for comprehensive fact tables
       - Only read from Bronze if you need columns not in any Silver table
       - If joining: MUST use NATURAL KEYS (provider_id, clinic_id, etc.), NOT row_id
       - Perform aggregations and metrics calculations
       
       FOR FACT TABLES (fact_*):
       - PREFER reading from Silver (shows dependency chain)
       - Can join multiple Silver tables for comprehensive fact tables
       - Only read from Bronze if you need columns not in any Silver table
       - If joining: MUST include row_id in all SELECT statements
       - Perform aggregations and metrics calculations
       
       BEST PRACTICE - Connected graph pattern:
         ```python
         # Bronze → Silver → Gold (shows full dependency chain)
         @dlt.table(name="gold_dim_appointments")
         def gold_dim_appointments():
             # Read from Silver, not Bronze
             return dlt.read("silver_appointments").select(
                 F.col("appointment_id"),
                 F.col("appointment_date"),
                 F.col("appointment_status"),
                 F.col("provider_id"),
                 F.col("patient_id")
             )
         
         @dlt.table(name="gold_fact_appointment_summary")
         def gold_fact_appointment_summary():
             # Aggregate from Silver
             silver_df = dlt.read("silver_appointments")
             
             return silver_df.groupBy("appointment_status").agg(
                 F.count("*").alias("appointment_count"),
                 F.avg("duration_minutes").alias("avg_duration")
             )
         ```
       
       AVOID - Reading Bronze in Gold (breaks visual flow):
         ```python
         # This works but shows disconnected graph
         def gold_dim_appointments():
             # Skips Silver, goes straight to Bronze
             return dlt.read("bronze_appointments").select(...)
         ```
       
       Example - Simple dimension (NO joins, NO row_id needed):
         ```python
         @dlt.table(name="gold_dim_provider", comment="Provider dimension", table_properties={{"quality": "gold"}})
         def gold_dim_provider():
             return dlt.read("silver_practitioner").select(
                 F.col("practitioner_name").alias("provider_name"),
                 F.col("practitioner_identifier_license_number").alias("license_number"),
                 F.col("practitioner_specialty").alias("specialty")
             )
         ```
       
       Example - Dimension table WITH JOINS (MUST include row_id):
         ```python
         @dlt.table(name="gold_dim_provider", comment="Provider dimension", table_properties={{"quality": "gold"}})
         def gold_dim_provider():
             # CORRECT: Include row_id when joining
             provider_df = dlt.read("silver_practitioner").select(
                 F.col("row_id"),  # MUST include row_id for join
                 F.col("practitioner_name").alias("provider_name"),
                 F.col("practitioner_identifier_license_number").alias("license_number")
             )
             
             role_df = dlt.read("silver_practitionerrole").select(
                 F.col("row_id"),  # MUST include row_id for join
                 F.col("practitionerrole_specialty").alias("specialty")
             )
             
             # Now joins work because both have row_id
             return provider_df.join(role_df, on="row_id", how="left")
         ```
       
       **CRITICAL: JOIN COLUMN DISAMBIGUATION**
       When joining, if both DataFrames have columns with similar names, you MUST alias them differently:
       
       WRONG - Join creates duplicate column names:
         ```python
         providers_df = dlt.read("silver_providers").select(
             F.col("encounter_identifier").alias("provider_id"),     # Creates provider_id
             F.col("practitioner_name")
         )
         
         role_df = dlt.read("silver_providers").select(
             F.col("practitioner_identifier_provider_id").alias("provider_id"),  # DUPLICATE!
             F.col("specialty")
         )
         
         # This creates TWO provider_id columns - DUPLICATE_COLUMN_NAMES error!
         return providers_df.join(role_df, on="row_id")
         ```
       
       CORRECT - Disambiguate column names:
         ```python
         providers_df = dlt.read("silver_providers").select(
             F.col("encounter_identifier").alias("encounter_id"),    # Unique name
             F.col("practitioner_name")
         )
         
         role_df = dlt.read("silver_providers").select(
             F.col("practitioner_identifier_provider_id").alias("provider_id"),  # Unique name
             F.col("specialty")
         )
         
         # No duplicate columns
         return providers_df.join(role_df, on="row_id")
         ```
       
       Example - Fact table WITHOUT JOINS (read from Bronze, NO row_id needed):
         ```python
         @dlt.table(name="gold_fact_appointments", comment="Appointment metrics", table_properties={{"quality": "gold"}})
         def gold_fact_appointments():
             return dlt.read("bronze_appointments").select(
                 F.col("appointment_id"),
                 F.col("provider_id"),
                 F.col("patient_id"),
                 F.col("appointment_date"),
                 F.col("duration_minutes"),
                 F.col("status")
             )
         ```
       
       Example - KPI table from Bronze (recommended for comprehensive metrics):
         ```python
         @dlt.table(name="gold_kpi", comment="Business KPIs", table_properties={{"quality": "gold"}})
         def gold_kpi():
             # Read from Bronze - all columns available
             bronze_df = dlt.read("bronze_appointments")
             total = bronze_df.count()
             
             if total == 0:
                 return spark.createDataFrame([{{"total": 0}}], schema)
             
             # All bronze columns accessible here
             no_shows = bronze_df.filter("no_show_flag = TRUE").count()
             avg_duration = bronze_df.agg(F.avg("duration_minutes")).collect()[0][0] or 0.0
             
             return spark.createDataFrame([{{
                 "total_appointments": total,
                 "no_show_count": no_shows,
                 "average_duration": avg_duration
             }}], schema)
         ```
       
       CRITICAL: Always handle division by zero in KPI calculations:
         ```python
         # WRONG - will crash if denominator is 0
         male_ratio = male_count / female_count
         
         # CORRECT - check before dividing
         male_ratio = male_count / female_count if female_count > 0 else 0.0
         
         # WRONG - unsafe division
         acquisition_rate = new_providers / total_providers
         
         # CORRECT - validate both numerator and denominator
         acquisition_rate = new_providers / total_providers if total_providers > 0 else 0.0
         
         # For more complex calculations, use when() clauses:
         df = df.withColumn("conversion_rate", 
             F.when(F.col("total_visits") > 0, 
                    F.col("conversions") / F.col("total_visits"))
             .otherwise(0.0))
         ```
    
       - Output Structure Validation

            Every Gold table function must return exactly one Spark DataFrame where:

            Each column is a scalar type (int, float, string, boolean) or a simple array/struct.

            Do NOT include nested DataFrames, raw Row objects, or Spark objects inside your return DataFrame.

            You may include small Python lists or JSON strings to represent arrays or summaries.

            Examples:

            # Invalid: Trying to store a DataFrame inside another
            "top_rated_providers": bronze_df.orderBy("rating").limit(10)

            # Invalid: Python list without explicit schema - Spark cannot infer type
            "top_rated_providers": [
                row["practitioner_name"] for row in top_rated_df.collect()
            ]

            # Valid Option 1: Convert list to comma-separated string
            top_rated_list = [
                row["provider_id"]
                for row in bronze_df.orderBy(F.col("rating").desc()).limit(10).select("provider_id").collect()
            ]
            "top_rated_provider_ids": ",".join(map(str, top_rated_list))  # String type
            
            # Valid Option 2: Use explicit schema with ArrayType
            from pyspark.sql.types import ArrayType, StringType
            schema = StructType([
                StructField("total", IntegerType()),
                StructField("top_providers", ArrayType(StringType()))  # Explicit array type
            ])
            top_list = [r["name"] for r in df.limit(10).collect()]
            return spark.createDataFrame([{{"total": 100, "top_providers": top_list}}], schema)

            2. Type-Safe Metric Aggregations

            All computed metrics and aggregations must return Python-native types.

            Always extract scalar values using .collect()[0][0]

            Cast results explicitly to float, int, or str

            Example:

            # Correct
            avg_exp = silver_df.agg(F.avg("years_experience")).collect()[0][0] or 0.0
            "average_years_experience": float(avg_exp)

            # Incorrect
            "average_years_experience": silver_df.agg(F.avg("years_experience"))  # returns DataFrame

           3. Array/List Handling (CRITICAL)

            When returning lists or arrays in KPI tables, ALWAYS use one of these patterns:

            Pattern 1 - Convert to String (Recommended):
            ```python
            top_ids = [row["id"] for row in df.limit(10).collect()]
            return spark.createDataFrame([{{
                "top_provider_ids": ",".join(map(str, top_ids))  # String: "123,456,789"
            }}])
            ```

            Pattern 2 - Explicit Schema with ArrayType:
            ```python
            from pyspark.sql.types import ArrayType, StringType
            schema = StructType([
                StructField("total", IntegerType()),
                StructField("top_names", ArrayType(StringType()))
            ])
            names_list = [r["name"] for r in df.limit(10).collect()]
            return spark.createDataFrame([{{
                "total": 100,
                "top_names": names_list
            }}], schema)
            ```

            DO NOT use list comprehension without explicit schema - Spark cannot infer complex types:
            ```python
            # WRONG - Will cause "CANNOT_DETERMINE_TYPE" error
            return spark.createDataFrame([{{
                "top_providers": [row["name"] for row in df.collect()]  # No schema
            }}])
            ```

           4. Dependency Integrity

            Gold tables must show lineage in the pipeline graph:

            Preferred: Gold → Silver

            Allowed (with justification): Gold → Bronze only if required columns do not exist in Silver

            Never: Skip Silver without an explanatory comment

            Example:

            # justified: 'license_expiry' column only exists in Bronze
            bronze_df = dlt.read("bronze_providers")
       
       - Or use natural keys if they exist: .join(other_df, on="provider_id", how="left")
       - Only create gold tables for silver tables that actually exist in mappings
       - NEVER reference columns from Table A while reading Table B
    
    8. **DATA QUALITY EXPECTATIONS**:
       - Place @dlt.expect decorators BEFORE @dlt.table
       - Use lowercase table names in expectations
       - Use underscores instead of dots in column names
       - Include expectations from: {expectations_text}
       - Example:
         ```python
         @dlt.expect("not_null_identifier", "practitioner_identifier_clinic_id IS NOT NULL")
         @dlt.table(name="silver_practitioner")
         ```
    
    9. **DQ AUDIT LOG**:
       - Use lowercase table names: dlt.read("silver_practitioner") not "silver_Practitioner"
       - Use underscored column names in conditions
       - Wrap in try/except for each table
       - Return empty DataFrame with schema if no violations
       - Example:
         ```python
         dq_rules = {{
             "silver_practitioner": [{{"rule_name": "not_null_id", "condition": "practitioner_identifier_clinic_id IS NULL"}}]
         }}
         ```
    
    10. **LOAD LOG TABLE**:
       - Use schema for createDataFrame to avoid errors
       - Handle empty data gracefully
    
    11. **PROCESSING STATUS TABLE (CRITICAL FIX)**:
       - DLT does NOT have dlt.list_tables() method
       - Manually define list of tables to check:
         ```python
         tables_to_check = [
             "bronze_providers",
             "silver_location",
             "silver_practitioner",
             # ... add all your tables
         ]
         for table in tables_to_check:
             try:
                 dlt.read(table)
                 processed_tables.append(table)
             except Exception as e:
                 status = "FAILED"
         ```
       - Convert processed_tables list to string before returning
       - Define proper schema with StringType for processed_tables field
    
    12. **GOLD KPI TABLE (CRITICAL FIX - COLUMN AVAILABILITY & GRAPH CONNECTIVITY)**:
       KPIs MUST only reference columns that exist in the tables being queried
       For connected graph, PREFER reading from Silver aggregates, not directly from Bronze
       
       BEST PRACTICE - Use Silver for connected pipeline:
       ```python
       def gold_kpi():
           # Read from Silver - shows Bronze→Silver→Gold flow
           silver_df = dlt.read("silver_appointments")
           total = silver_df.count()
           
           if total == 0:
               return spark.createDataFrame([{{"total": 0}}], schema)
           
           # Use Silver columns (must exist in Silver table)
           no_shows = silver_df.filter("flag_status = TRUE").count()
           avg_duration = silver_df.agg(F.avg("appointment_minutesduration")).collect()[0][0] or 0.0
           show_rate = 1.0 - (no_shows / total) if total > 0 else 0.0
           
           return spark.createDataFrame([{{
               "total_appointments": total,
               "no_show_count": no_shows,
               "patient_show_rate": show_rate,
               "average_duration": avg_duration
           }}], schema)
       ```
       
       FALLBACK - Use Bronze ONLY if Silver lacks needed columns:
       ```python
       def gold_kpi():
           # Only use Bronze if Silver doesn't have required columns
           # This may show disconnected in graph
           bronze_df = dlt.read("bronze_appointments")
           # ... calculations using original CSV column names
       ```
       
       WRONG APPROACH - Assumes all columns in one table:
       ```python
       # BAD: References columns from multiple tables without joins
       appointment_df = dlt.read("silver_appointment")
       virtual_rate = appointment_df.filter("encounter_virtualservice = TRUE").count()  # Column doesn't exist!
       ```
       
       CORRECT APPROACH - Only use columns from the actual table:
       ```python
       # GOOD: Only use columns that exist in silver_appointment
       appointment_df = dlt.read("silver_appointment")
       total = appointment_df.count()
       no_shows = appointment_df.filter("flag_status = TRUE").count()
       avg_duration = appointment_df.agg(F.avg("duration_value")).collect()[0][0] or 0.0
       ```
       
       RULES FOR GOLD KPI GENERATION:
       1. Read from BRONZE table for comprehensive metrics (all columns available)
       2. OR read from individual Silver tables but ONLY use their columns
       3. If KPI needs columns from multiple Silver tables, use JOIN with row_id
       4. ALWAYS validate column exists before filtering/aggregating
       5. Check for zero count before division
       6. Handle null aggregations: .collect()[0][0] or 0.0
       7. Use explicit schema with DoubleType for percentages
       8. NEVER include DataFrame objects in KPI results - only scalar values
       9. NEVER use .limit() or .orderBy() without .count() - these return DataFrames
       10. ALWAYS use .count(), .sum(), .avg(), .min(), .max() for aggregations
       
       WRONG - DataFrame in results:
       ```python
       # BAD: Returns DataFrame, not a number
       top_providers = bronze_df.orderBy("rating", ascending=False).limit(10)
       
       return spark.createDataFrame([{{
           "top_providers": top_providers  # Can't serialize DataFrame!
       }}])
       ```
       
       CORRECT - Scalar values only:
       ```python
       # GOOD: Returns count (integer)
       top_providers_count = bronze_df.filter("rating > 4").count()
       
       return spark.createDataFrame([{{
           "top_providers_count": top_providers_count  # Integer value
       }}], schema)
       ```
       
       PREFERRED PATTERN - Use Bronze for KPIs:
       ```python
       def gold_kpi():
           bronze_df = dlt.read("bronze_appointments")  # Has ALL columns
           total = bronze_df.count()
           
           if total == 0:
               return spark.createDataFrame([{{"metric": 0.0}}], schema)
           
           # All aggregations from bronze - no column missing issues
           no_shows = bronze_df.filter("no_show_flag = TRUE").count()
           avg_duration = bronze_df.agg(F.avg("duration_minutes")).collect()[0][0] or 0.0
           show_rate = 1 - (no_shows / total)
           
           return spark.createDataFrame([{{
               "total_appointments": total,
               "no_show_count": no_shows,
               "patient_show_rate": show_rate,
               "average_duration": avg_duration
           }}], schema)
       ```
    
    13. **METADATA TABLES (STRICT REQUIREMENTS)**:
        {DQ_AUDIT_LOG_REQUIREMENTS}
        {LOAD_LOG_REQUIREMENTS}
        {PROCESSING_STATUS_REQUIREMENTS}
    
    14. **CODE STRUCTURE**:
       - Import at top: import dlt
       - Import: from pyspark.sql import functions as F
       - Import: from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
       - Import: import uuid, datetime
       - Use @dlt syntax examples: {DLT_EXAMPLES}
       - NO explanations, NO markdown, ONLY runnable Python code
       - Replace ALL dots in column names with underscores
       - Keep deduplicated column suffixes (e.g., _clinic_id, _npi_number)
    
    15. **VALIDATION BEFORE GOLD**:
        - Only reference silver tables that exist in mapping_rows
        - Check table exists before joining
        - Handle empty tables gracefully
        - Use the exact deduplicated column names from silver layer
    
    16. **GOLD LAYER DUPLICATE COLUMN PREVENTION - CRITICAL**:
        Gold tables often transform Silver column names for business users.
        MUST ensure no two .alias() calls create the same column name!
        
        WRONG - Two columns aliased to same name (causes DUPLICATE_COLUMN_NAMES error):
        ```python
        def gold_dim_providers():
            return dlt.read("silver_providers").select(
                F.col("encounter_identifier").alias("provider_id"),          # Duplicate!
                F.col("practitioner_identifier_provider_id").alias("provider_id")  # Duplicate!
            )
        ```
        
        CORRECT - Unique column names:
        ```python
        def gold_dim_providers():
            return dlt.read("silver_providers").select(
                F.col("encounter_identifier").alias("clinic_provider_id"),   # Unique
                F.col("practitioner_identifier_provider_id").alias("npi_provider_id")  # Unique
            )
        ```
        
        Or pick ONE column that best represents the business concept:
        ```python
        def gold_dim_providers():
            return dlt.read("silver_providers").select(
                # Use the most authoritative provider identifier
                F.col("practitioner_identifier_provider_id").alias("provider_id"),  # Single source
                F.col("encounter_identifier").alias("clinic_id"),  # Different purpose
                F.col("practitioner_name").alias("provider_name")
            )
        ```

    17. **FINAL VALIDATION - SCAN YOUR GENERATED CODE**:
        Before returning the code, mentally check EVERY .alias() call:
        - Does it contain a dot? → Replace with underscore
        - Does another .alias() use the same target name? → Make it unique
        - All column names use only underscores and alphanumeric characters
        - No two columns in the same .select() have identical alias names
        
        Example validation checklist for silver_appointment:
        ```python
        # Check 1: No dots in any alias
        F.col("status").alias("flag_status")          # No dots
        F.col("start").alias("period_start")          # No dots
        
        # Check 2: No duplicate alias names
        All alias names in .select() are unique       # Confirmed
        
        # Check 3: Gold layer - no duplicate business column names
        F.col("encounter_id").alias("appointment_id")  # Unique
        F.col("provider_id").alias("provider_id")      # Unique (not duplicated)
        ```
    
    17. **DATA TYPE HANDLING & PROPER FILTERING - CRITICAL FOR CORRECT DATA**:
        CSV files store ALL data as STRINGS! Must handle type conversions properly.
        
        **BOOLEAN FLAGS - CRITICAL FIXES:**
        
        CSV boolean values can be:
        - Strings: "True"/"False", "true"/"false", "TRUE"/"FALSE"
        - Strings: "Yes"/"No", "Y"/"N", "yes"/"no"
        - Numbers: "1"/"0", 1/0
        - Actual booleans: true/false (after inferSchema)
        
        WRONG - Assumes SQL boolean TRUE:
        ```python
        df.filter("no_show_flag = TRUE")  # Fails if flag is string "True" or "1"
        df.filter("is_virtual = TRUE")    # Fails if not actual boolean
        ```
        
        WRONG - Comparing string to Python boolean:
        ```python
        F.when(F.col("no_show_flag") == True, 1)  # Fails for string "True"
        ```
        
        CORRECT - Handle multiple boolean formats:
        ```python
        # Method 1: Convert to proper boolean first (RECOMMENDED)
        .withColumn("no_show_flag", 
            F.when(F.col("no_show_flag").isin("True", "true", "TRUE", "1", "Yes", "yes", "Y"), True)
             .when(F.col("no_show_flag").isin("False", "false", "FALSE", "0", "No", "no", "N"), False)
             .otherwise(None))
        
        # Then filter with actual boolean
        df.filter(F.col("no_show_flag") == True)
        
        # Method 2: Filter string values directly
        df.filter(F.col("no_show_flag").isin("True", "true", "TRUE", "1", "Yes", "Y"))
        
        # Method 3: Use upper() for case-insensitive
        df.filter(F.upper(F.col("no_show_flag")).isin("TRUE", "YES", "Y", "1"))
        ```
        
        CORRECT - Aggregations with boolean conversion:
        ```python
        # Count true values
        no_shows = df.filter(
            F.upper(F.col("no_show_flag")).isin("TRUE", "YES", "Y", "1")
        ).count()
        
        # Use in aggregations
        F.sum(
            F.when(F.upper(F.col("no_show_flag")).isin("TRUE", "YES", "Y", "1"), 1)
             .otherwise(0)
        ).alias("no_show_count")
        ```
        
        **NUMERIC TYPE HANDLING:**
        
        WRONG - Assumes numeric, may be string:
        ```python
        F.avg("copay_amount")  # Fails if stored as string "$123.45"
        F.sum("duration_minutes")  # Fails if has commas "1,234"
        ```
        
        CORRECT - Cast to proper numeric type:
        ```python
        # Cast to numeric in Silver layer
        .withColumn("copay_amount", F.col("copay_amount").cast("double"))
        .withColumn("duration_minutes", F.col("duration_minutes").cast("integer"))
        .withColumn("patient_age", F.col("patient_age").cast("integer"))
        
        # Then aggregate safely
        F.avg(F.col("copay_amount").cast("double"))
        F.sum(F.col("duration_minutes").cast("integer"))
        ```
        
        **NULL/EMPTY VALUE HANDLING:**
        
        WRONG - No null checks:
        ```python
        avg_duration = df.agg(F.avg("duration_minutes")).collect()[0][0] or 0.0
        # Still crashes if column is all nulls
        ```
        
        CORRECT - Filter nulls first:
        ```python
        # Filter out nulls and empty strings before aggregation
        avg_duration = df.filter(
            F.col("duration_minutes").isNotNull() & 
            (F.col("duration_minutes") != "") &
            (F.col("duration_minutes").cast("integer") > 0)
        ).agg(F.avg(F.col("duration_minutes").cast("integer"))).collect()[0][0]
        
        # Always provide fallback
        avg_duration = avg_duration if avg_duration is not None else 0.0
        ```
        
        **DATE/TIME HANDLING:**
        
        WRONG - String comparison:
        ```python
        df.filter("appointment_date > '2024-01-01'")  # May fail with different formats
        ```
        
        CORRECT - Cast to date/timestamp:
        ```python
        # Cast in Silver layer
        .withColumn("appointment_date", F.to_date(F.col("appointment_date")))
        .withColumn("appointment_time", F.to_timestamp(F.col("appointment_time")))
        
        # Then compare properly
        df.filter(F.col("appointment_date") > F.lit("2024-01-01").cast("date"))
        ```
        
        **COMPREHENSIVE SILVER LAYER PATTERN:**
        ```python
        def silver_appointments():
            return (
                dlt.read("bronze_appointments")
                .select(
                    # IDs - keep as string
                    F.col("appointment_id").alias("appointment_id"),
                    F.col("patient_id").alias("patient_id"),
                    
                    # Dates - cast to date
                    F.to_date(F.col("appointment_date")).alias("appointment_date"),
                    
                    # Times - cast to timestamp
                    F.to_timestamp(F.col("appointment_time")).alias("appointment_time"),
                    
                    # Numbers - cast to numeric
                    F.col("duration_minutes").cast("integer").alias("duration_minutes"),
                    F.col("copay_amount").cast("double").alias("copay_amount"),
                    F.col("patient_age").cast("integer").alias("patient_age"),
                    
                    # Booleans - convert from string
                    F.when(F.upper(F.col("no_show_flag")).isin("TRUE", "YES", "Y", "1"), True)
                     .otherwise(False).alias("no_show_flag"),
                    
                    F.when(F.upper(F.col("is_virtual")).isin("TRUE", "YES", "Y", "1"), True)
                     .otherwise(False).alias("is_virtual"),
                    
                    # Text - trim whitespace
                    F.trim(F.col("appointment_status")).alias("appointment_status"),
                    F.trim(F.col("provider_name")).alias("provider_name")
                )
                .withColumn("row_id", F.monotonically_increasing_id())
            )
        ```
        
        **RULES FOR DATA QUALITY:**
        1. ALWAYS cast numeric columns to integer/double in Silver
        2. ALWAYS convert boolean strings to actual booleans in Silver
        3. ALWAYS cast date/time strings to date/timestamp in Silver
        4. ALWAYS trim() text fields to remove whitespace
        5. ALWAYS filter nulls before aggregations
        6. ALWAYS provide fallback values (or 0.0, "" etc.)
        7. Use F.coalesce(col, default_value) for null handling
        8. Use upper() or lower() for case-insensitive string comparisons

    18. **FINAL CODE REVIEW CHECKLIST**:
        Before returning code, verify:
        - All boolean filters use .isin() or proper casting
        - All numeric columns cast to integer/double
        - All date columns cast to date/timestamp
        - All aggregations have null handling
        - All text comparisons use upper()/lower()
        - No dots in column names
        - No duplicate column names
        - Bronze streaming, Silver/Gold batch reads

    Return ONLY the complete runnable PySpark code following ALL rules above.
    DO NOT include any explanations, markdown formatting, or comments about the rules.
    ONLY return valid Python code that can be executed directly.
    """


    answer = llm.invoke(prompt)
    answer_text = getattr(answer, "content", None) or str(answer)

    return state.copy(update={
        "messages": messages + [{
            "role": "assistant",
            "content": answer_text,
            "name": "bricks_coder"
        }],
        "pyspark_code": answer_text
    })


def get_bricks_medallion_agent():
    description = (
        "Generates Databricks PySpark code for end-to-end Medallion data lake "
        "(Bronze → Silver → Gold) using mappings and KPIs."
    )
    return bricks_medallion_agent_node, description


# Optional alias for easier UI usage
run_bricks_medallion_agent = bricks_medallion_agent_node