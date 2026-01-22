# agents/metadata_utils.py

DQ_AUDIT_LOG_REQUIREMENTS = """
- Implement a Delta Live Tables (DLT) table named `dq_audit_log` that records **all Data Quality (DQ) rule evaluations** for each Silver table.
- The function must return a Spark DataFrame constructed from a **Python list of dictionaries (`audit_log`)** using an **explicitly defined schema** via `StructType`.
- The schema must exactly match:
    dq_rule_name STRING,
    dq_rule_condition STRING,
    dq_rule_type STRING,
    total_records INT,
    violation_count INT,
    pass_count INT,
    evaluation_time STRING,
    aud_load_id STRING,
    error_message STRING
- The `evaluation_time` must be stored as a **Python datetime string**, generated with `datetime.now().isoformat()` — NOT using `F.current_timestamp()` — to avoid Spark expression issues.
- For every Silver table in `dq_rules`, evaluate each defined rule and log **one record per rule** regardless of whether violations are found.
- Each rule evaluation record must include:
    - `dq_rule_name`: Name of the rule (e.g., "not_null_patient.identifier")
    - `dq_rule_condition`: The SQL filter condition used to check the rule (e.g., "identifier IS NULL")
    - `dq_rule_type`: "NOT NULL" if "not_null" is in the rule name, otherwise "VALID"
    - `total_records`: Total number of records in the table (`df.count()`)
    - `violation_count`: Number of rows that fail the rule (`df.filter(condition).count()`)
    - `pass_count`: `total_records - violation_count`
    - `evaluation_time`: Current timestamp as an ISO string
    - `aud_load_id`: A new UUID (`str(uuid.uuid4())`)
    - `error_message`: `None` if successful, or the exception message if failed
- Use **`try/except`** around both table reads and rule evaluations to ensure that any single failure does not interrupt other evaluations.
- In the `except` block:
    - If an error occurs before variables like `rule_name` or `condition` are defined, safely handle them using `'rule_name' in locals()` and `'condition' in locals()` checks.
    - Record a log entry with:
        - `dq_rule_type` = "ERROR"
        - All numeric fields (`total_records`, `violation_count`, `pass_count`) = `None`
        - `error_message` = `str(e)`
        - `evaluation_time` = `datetime.now().isoformat()`
- Always **define a `schema` variable** (using `StructType` and `StructField`) and pass it explicitly to `spark.createDataFrame(audit_log, schema)` to ensure the correct data types and consistent structure.
- The audit log must always return a DataFrame even if no violations or errors occur.
- The rule configuration should use **flat column references** (e.g., `"identifier IS NULL"`) instead of nested struct syntax like `"Patient.identifier IS NULL"`.
- Example rule configuration:
    ```python
    dq_rules = {
        "silver_patient": [
            {"rule_name": "not_null_patient.identifier", "condition": "identifier IS NULL"},
            {"rule_name": "positive_patient.identifier", "condition": "identifier <= 0"}
        ]
    }
    ```
- Example output record:
    | dq_rule_name               | dq_rule_type | violation_count | pass_count | total_records | evaluation_time           |
    |----------------------------|---------------|-----------------|-------------|----------------|---------------------------|
    | not_null_patient.identifier| NOT NULL      | 0               | 5000        | 5000           | 2025-10-29T12:34:56.123Z |
    | positive_patient.identifier| VALID         | 12              | 4988        | 5000           | 2025-10-29T12:34:56.456Z |
- The dq_audit_log function MUST always define and use an explicit schema:
    schema = StructType([
        StructField("dq_rule_name", StringType(), True),
        StructField("dq_rule_condition", StringType(), True),
        StructField("dq_rule_type", StringType(), True),
        StructField("total_records", IntegerType(), True),
        StructField("violation_count", IntegerType(), True),
        StructField("pass_count", IntegerType(), True),
        StructField("evaluation_time", StringType(), True),
        StructField("aud_load_id", StringType(), True),
        StructField("error_message", StringType(), True),
    ])

- The return statement MUST ALWAYS be:
    return spark.createDataFrame(audit_log, schema)

- NEVER allow the agent to use spark.createDataFrame(audit_log) without the schema.

"""




LOAD_LOG_REQUIREMENTS = """
- Implement a Delta Live Tables (DLT) table named `load_log` that captures **file-level metadata** for each Bronze table ingestion.
- The function must return a Spark DataFrame constructed from a **Python list of dictionaries (`load_log`)** using an **explicitly defined schema** via `StructType`.
- The schema must include and match these exact fields:
    file_name STRING,
    raw_record_count INT,
    inserted_count INT,
    load_start_time STRING,
    end_time STRING,
    aud_load_id STRING
- Use `datetime.now().isoformat()` (Python timestamp string) for both `load_start_time` and `end_time` — **do not** use `F.current_timestamp()`.
- Read data dynamically from the target Bronze Delta Live Table using:
    ```python
    bronze_df = dlt.read("bronze_<table_name>")
    ```
- Derive the `file_name` dynamically using Spark’s internal `_jdf.inputFiles()` method:
    ```python
    input_files = bronze_df._jdf.inputFiles()
    file_name = input_files[0] if input_files else "unknown_file"
    ```
- Compute:
    - `raw_record_count`: total rows in the Bronze table (`bronze_df.count()`)
    - `inserted_count`: same as `raw_record_count` unless additional logic is added later
    - `aud_load_id`: dynamically generated unique identifier using `str(uuid.uuid4())`
- Build a single audit entry in a Python list named `load_log` with these fields populated.
- Define a schema variable before creating the DataFrame using:
    ```python
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    ```
- Always use:
    ```python
    spark.createDataFrame(load_log, load_log_schema)
    ```
  where `load_log_schema` is the explicitly defined schema variable.
- Implement **error handling**:  
    - Wrap the logic in a `try/except` block.
    - On exception, append an entry with `"file_name": "error"`, `"error_message": str(e)`, and set record counts to `None`.
    - If no files or data found, return an empty DataFrame created using the defined schema.
- Example implementation snippet:
    ```python
    load_log_schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("raw_record_count", IntegerType(), True),
        StructField("inserted_count", IntegerType(), True),
        StructField("load_start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("aud_load_id", StringType(), True)
    ])

    @dlt.table(
        name="load_log",
        comment="Load log table",
        table_properties={"quality": "log"}
    )
    def load_log():
        load_log = []
        try:
            bronze_df = dlt.read("bronze_patient_test")
            input_files = bronze_df._jdf.inputFiles()
            file_name = input_files[0] if input_files else "unknown_file"
            now_str = datetime.now().isoformat()
            load_log.append({
                "file_name": file_name,
                "raw_record_count": bronze_df.count(),
                "inserted_count": bronze_df.count(),
                "load_start_time": now_str,
                "end_time": now_str,
                "aud_load_id": str(uuid.uuid4())
            })
        except Exception as e:
            load_log.append({
                "file_name": "error",
                "raw_record_count": None,
                "inserted_count": None,
                "load_start_time": datetime.now().isoformat(),
                "end_time": datetime.now().isoformat(),
                "aud_load_id": str(uuid.uuid4()),
                "error_message": str(e)
            })
        return spark.createDataFrame(load_log, load_log_schema)
    ```
- Expected behavior:
    - Always creates the `load_log` table.
    - Logs one record per load, even if the Bronze source is empty.
    - Returns a DataFrame with correct schema and timestamps.
"""


PROCESSING_STATUS_REQUIREMENTS = """
- Implement a `processing_status` table that logs one record per pipeline run.
- Include fields: status, start_time, end_time, processed_tables, error_message, aud_load_id.
- Do NOT include pipeline_name or pipeline_id (cannot be dynamically retrieved in DLT code).
- Values must be dynamic, not hardcoded.
- Compute:
    - start_time BEFORE any table processing begins.
    - Attempt to read all tables created by this same pipeline (Bronze, Silver, Gold, Audit, and Log tables).
        - The list of tables should come from the tables defined within the same generated code.
        - Do NOT use dlt.list_tables() (it can fail in DLT context).
        - The agent must automatically collect and reference all @dlt.table names defined in this pipeline.
    - If a table read fails, continue processing the remaining tables.
    - Collect error messages for any failed table reads.
    - processed_tables should list only tables successfully read.
    - status = "SUCCESS" if all dlt.read() calls succeed, otherwise "FAILED".
    - error_message = concatenated messages from any failed table reads, empty string if no errors.
    - end_time AFTER all processing completes or exceptions occur.
- Always generate a new aud_load_id with str(uuid.uuid4()).
- Wrap table reads in try/except to handle exceptions gracefully; never stop the pipeline on a single table failure.
- Never hardcode timestamps, status, or table names in the code. Table names must come from the generated pipeline context.
- Return a PySpark DataFrame with a single row containing all fields.
- The resulting code should look like:
    ```python
    @dlt.table(name="processing_status", comment="Processing status table", table_properties={"quality": "status"})
    def processing_status():
        tables = [
            # Auto-populated list of all tables defined in this pipeline
            "bronze_patients",
            "silver_patient",
            "gold_dim_patient",
            "gold_fact_patient_metrics",
            "dq_audit_log",
            "load_log"
        ]
        processed_tables = []
        error_message = ""
        start_time = datetime.datetime.now().isoformat()
        try:
            for table in tables:
                try:
                    dlt.read(table)
                    processed_tables.append(table)
                except Exception as e:
                    error_message += f"Error reading table {table}: {str(e)}\\n"
        except Exception as e:
            error_message += f"Error accessing tables: {str(e)}\\n"
        end_time = datetime.datetime.now().isoformat()
        status = "SUCCESS" if not error_message else "FAILED"
        return spark.createDataFrame([{
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "processed_tables": processed_tables,
            "error_message": error_message,
            "aud_load_id": str(uuid.uuid4())
        }])
    ```
- IMPORTANT: The list of tables must be automatically derived from the @dlt.table definitions produced in the same code generation step, not manually hardcoded.
"""



