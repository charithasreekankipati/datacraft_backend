DG_EXAMPLES = """
-- Example: Masking Policy
CREATE MASKING POLICY mask_ssn
  AS (val STRING) RETURNS STRING ->
    CASE
      WHEN is_account_group_member('compliance_team') THEN val
      ELSE '***-**-****'
    END;

ALTER TABLE sensitive_db.customers
ALTER COLUMN ssn
SET MASKING POLICY mask_ssn;

-- Example: Row Filter Policy
CREATE ROW FILTER POLICY only_finance
  AS (user STRING) RETURNS BOOLEAN ->
    is_account_group_member('finance_team');

ALTER TABLE sensitive_db.transactions
SET ROW FILTER POLICY only_finance ON (current_user());
"""

DLT_EXAMPLES = """
--- Example: @dlt.table with Data Quality Expectations
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Bronze Layer
@dlt.table(
    name="bronze_providers",
    comment="Raw providers data from source",
    table_properties={"quality": "bronze"}
)
def bronze_providers():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "<user uploaded file type>")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/kpi2/upload_20251027_045602/_schemas/")
            .load("/Volumes/workspace/default/kpi2/upload_20251027_045602")
    )

# Silver Layer with DQ Expectations
@dlt.expect("not_null_identifier", "identifier IS NOT NULL")
@dlt.table(
    name="silver_location",
    comment="Cleansed location data with quality checks",
    table_properties={"quality": "silver"}
)
def silver_location():
    return (
        dlt.read_stream("bronze_providers")
        .select(
            F.col("clinic_id").alias("identifier")
        )
    )

@dlt.expect("not_null_practitioner_identifier", "Practitioner_identifier IS NOT NULL")
@dlt.expect("not_null_practitioner_name", "Practitioner_name IS NOT NULL")
@dlt.expect_or_drop("not_null_practitioner_qualification_id", "Practitioner_qualification_id IS NOT NULL")
@dlt.table(
    name="silver_practitioner",
    comment="Cleansed practitioner data with quality checks",
    table_properties={"quality": "silver"}
)
def silver_practitioner():
    return (
        dlt.read("bronze_providers")
        .select(
            F.col("provider_id").alias("Practitioner_identifier"),
            F.col("provider_name").alias("Practitioner_name"),
            F.col("license_number").alias("Practitioner_license_number"),
            F.col("npi_number").alias("Practitioner_npi_number"),
            F.col("years_experience").alias("Practitioner_qualification_id"),
            F.col("gender").alias("Practitioner_gender"),
            F.col("is_accepting_new_patients").alias("Patient_active"),
            F.col("license_expiry").alias("Practitioner_qualification_period_end")
        )
    )

@dlt.table(
    name="silver_practitioner_role",
    comment="Cleansed practitioner role data with quality checks",
    table_properties={"quality": "silver"}
)
def silver_practitioner_role():
    return (
        dlt.read("bronze_providers")
        .select(
            F.col("specialty_id").alias("PractitionerRole_specialty"),
            F.col("languages_spoken").alias("PractitionerRole_communication"),
            F.col("rating").alias("PractitionerRole_text")
        )
    )

@dlt.table(
    name="silver_contact_point",
    comment="Cleansed contact point data with quality checks",
    table_properties={"quality": "silver"}
)
def silver_contact_point():
    return (
        dlt.read("bronze_providers")
        .select(
            F.col("phone_number").alias("ContactPoint_value")
        )
    )

@dlt.table(
    name="silver_schedule",
    comment="Cleansed schedule data with quality checks",
    table_properties={"quality": "silver"}
)
def silver_schedule():
    return (
        dlt.read("bronze_providers")
        .select(
            F.col("schedule_link").alias("url")
        )
    )

# Gold Layer with Aggregations
@dlt.table(
    name="gold_dim_location",
    comment="Location dimension table for analytics",
    table_properties={"quality": "gold"}
)
def gold_dim_location():
    return (
        dlt.read("silver_location")
        .select(
            F.col("identifier").alias("location_id")
        )
    )

@dlt.table(
    name="gold_dim_practitioner",
    comment="Practitioner dimension table for analytics",
    table_properties={"quality": "gold"}
)
def gold_dim_practitioner():
    practitioner_df = (
        dlt.read("silver_practitioner")
        .select(
            F.col("Practitioner_identifier").alias("practitioner_id"),
            F.col("Practitioner_name").alias("practitioner_name"),
            F.col("Practitioner_qualification_id").alias("years_experience"),
            F.col("Practitioner_gender").alias("gender")
        )
    )
    
    role_df = (
        dlt.read("silver_practitioner_role")
        .select(
            F.col("Practitioner_identifier"),
            F.col("PractitionerRole_specialty").alias("specialty"),
            F.col("PractitionerRole_communication").alias("languages_spoken")
        )
    )
    
    return (
        practitioner_df
        .join(role_df, on="Practitioner_identifier", how="left")
        .select(
            F.col("practitioner_id"),
            F.col("practitioner_name"),
            F.col("years_experience"),
            F.col("gender"),
            F.col("specialty"),
            F.col("languages_spoken")
        )
    )

@dlt.table(
    name="gold_fact_practitioner_metrics",
    comment="Practitioner metrics fact table for analytics",
    table_properties={"quality": "gold"}
)
def gold_fact_practitioner_metrics():
    metrics_df = (
        dlt.read("silver_practitioner")
        .select(
            F.col("Practitioner_identifier").alias("practitioner_id"),
            F.col("Practitioner_name").alias("practitioner_name"),
            F.col("Practitioner_qualification_id").alias("years_experience"),
            F.col("Practitioner_gender").alias("gender"),
            F.col("Patient_active").alias("is_accepting_new_patients")
        )
    )
    
    role_df = (
        dlt.read("silver_practitioner_role")
        .select(
            F.col("Practitioner_identifier"),
            F.col("PractitionerRole_text").alias("rating")
        )
    )
    
    return (
        metrics_df
        .join(role_df, on="Practitioner_identifier", how="left")
        .select(
            F.col("practitioner_id"),
            F.col("practitioner_name"),
            F.col("years_experience"),
            F.col("gender"),
            F.col("is_accepting_new_patients"),
            F.col("rating")
        )
    )

# Data Quality Audit Log
@dlt.table(
    name="dq_audit_log",
    comment="Data quality audit log",
    table_properties={"quality": "audit"}
)
def dq_audit_log():
    dq_rules = {
        "silver_location": [{"rule_name": "not_null_identifier", "condition": "identifier IS NULL"}],
        "silver_practitioner": [
            {"rule_name": "not_null_practitioner_identifier", "condition": "Practitioner_identifier IS NULL"},
            {"rule_name": "not_null_practitioner_name", "condition": "Practitioner_name IS NULL"},
            {"rule_name": "not_null_practitioner_qualification_id", "condition": "Practitioner_qualification_id IS NULL"}
        ],
        "silver_practitioner_role": [],
        "silver_contact_point": [],
        "silver_schedule": []
    }
    audit_log = []
    for table, rules in dq_rules.items():
        if not rules:
            continue
        try:
            df = dlt.read(table)
            for rule_dict in rules:
                rule_name = rule_dict["rule_name"]
                condition = rule_dict["condition"]
                violations = df.filter(condition).count()
                audit_log.append({
                    "dq_rule_name": rule_name,
                    "dq_rule_condition": condition,
                    "dq_rule_type": "NOT NULL" if "not_null" in rule_name else "VALID",
                    "violation_count": violations,
                    "evaluation_time": F.current_timestamp(),
                    "aud_load_id": F.uuid()
                })
        except Exception as e:
            print(f"Error reading table {table}: {str(e)}")
            continue
    
    if audit_log:
        return spark.createDataFrame(audit_log)
    else:
        # Return empty dataframe with expected schema if no data
        return spark.createDataFrame([], "dq_rule_name STRING, dq_rule_condition STRING, dq_rule_type STRING, violation_count INT, evaluation_time TIMESTAMP, aud_load_id STRING")

# Load Log Table
@dlt.table(
    name="load_log",
    comment="Load log table",
    table_properties={"quality": "log"}
)
def load_log():
    load_log = []
    bronze_table = dlt.read("bronze_providers")
    load_log.append({
        "file_name": bronze_table._jdf.inputFiles()[0],
        "raw_record_count": bronze_table.count(),
        "inserted_count": bronze_table.count(),
        "load_start_time": F.current_timestamp(),
        "end_time": F.current_timestamp(),
        "aud_load_id": F.uuid()
    })
    return spark.createDataFrame(load_log)

# Processing Status Table
@dlt.table(
    name="processing_status",
    comment="Processing status table",
    table_properties={"quality": "status"}
)
def processing_status():
    pipeline_name = "Medallion Architecture"
    status = "SUCCESS"
    start_time = F.current_timestamp()
    end_time = F.current_timestamp()
    processed_tables = ["bronze_providers", "silver_location", "silver_practitioner", "silver_practitioner_role", "silver_contact_point", "silver_schedule", "gold_dim_location", "gold_dim_practitioner", "gold_fact_practitioner_metrics"]
    error_message = ""
    aud_load_id = F.uuid()
    return spark.createDataFrame([{
        "pipeline_name": pipeline_name,
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "processed_tables": processed_tables,
        "error_message": error_message,
        "aud_load_id": aud_load_id
    }])

# Gold Layer Business KPIs
@dlt.table(
    name="gold_kpi",
    comment="Business KPIs for analytics",
    table_properties={"quality": "kpi"}
)
def gold_kpi():
    total_providers = dlt.read("bronze_providers").count()
    active_providers = dlt.read("bronze_providers").filter("is_accepting_new_patients = TRUE").count()
    female_providers = dlt.read("bronze_providers").filter("gender = 'Female'").count()
    male_providers = dlt.read("bronze_providers").filter("gender = 'Male'").count()
    average_years_experience = dlt.read("bronze_providers").agg(F.avg("years_experience")).collect()[0][0]
    average_rating = dlt.read("bronze_providers").agg(F.avg("rating")).collect()[0][0]
    providers_by_specialty = dlt.read("bronze_providers").select("specialty_id").distinct().count()
    top_rated_providers = dlt.read("bronze_providers").orderBy("rating", ascending=False).limit(10)
    licensed_providers = dlt.read("bronze_providers").filter("license_expiry > CURRENT_DATE").count()
    providers_accepting_new_patients = dlt.read("bronze_providers").filter("is_accepting_new_patients = TRUE").count()
    return spark.createDataFrame([{
        "total_providers": total_providers,
        "active_providers": active_providers,
        "female_providers": female_providers,
        "male_providers": male_providers,
        "average_years_experience": average_years_experience,
        "average_rating": average_rating,
        "providers_by_specialty": providers_by_specialty,
        "top_rated_providers": top_rated_providers,
        "licensed_providers": licensed_providers,
        "providers_accepting_new_patients": providers_accepting_new_patients
    }])
"""