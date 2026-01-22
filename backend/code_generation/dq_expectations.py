# agents/dq_expectations.py
"""
Data Quality Expectations for DLT Pipelines
Generates @dlt.expect(), @dlt.expect_or_drop(), and @dlt.expect_or_fail() decorators
Intelligently selects mode based on column criticality and data severity
"""

from typing import Dict, List, Any, Tuple
import pandas as pd


# Expectation modes based on severity
class ExpectationMode:
    EXPECT = "expect"               # Log violations, allow data through
    EXPECT_OR_DROP = "expect_or_drop"  # Drop violating rows
    EXPECT_OR_FAIL = "expect_or_fail"  # Fail pipeline on violations


def determine_expectation_mode(
    col_name: str,
    rule_type: str,
    is_pii: bool = False,
    is_phi: bool = False,
    is_primary_key: bool = False
) -> str:
    """
    Intelligently determine which expectation mode to use based on:
    - Column criticality (PK, PII, PHI)
    - Rule type (nullability, format, range)
    - Business impact of violations
    
    Returns: 'expect', 'expect_or_drop', or 'expect_or_fail'
    """
    
    # CRITICAL: Primary keys and foreign keys must not have violations
    if is_primary_key:
        return ExpectationMode.EXPECT_OR_FAIL
    
    # HIGH SEVERITY: PII/PHI nulls should drop rows (privacy compliance)
    if (is_pii or is_phi) and rule_type == "not_null":
        return ExpectationMode.EXPECT_OR_DROP
    
    # HIGH SEVERITY: ID columns must be valid (drop bad IDs)
    if "id" in col_name.lower() and rule_type in ["not_null", "positive"]:
        return ExpectationMode.EXPECT_OR_DROP
    
    # HIGH SEVERITY: Date integrity (drop invalid dates)
    if "date" in col_name.lower() or "time" in col_name.lower():
        if rule_type in ["not_null", "valid_date"]:
            return ExpectationMode.EXPECT_OR_DROP
    
    # MEDIUM SEVERITY: Format validations (email, phone) - drop malformed
    if rule_type in ["email_format", "phone_format", "regex_pattern"]:
        return ExpectationMode.EXPECT_OR_DROP
    
    # MEDIUM SEVERITY: Key amounts/prices - drop negative values
    if rule_type in ["positive_amount", "valid_range"]:
        if any(kw in col_name.lower() for kw in ["amount", "price", "cost", "balance"]):
            return ExpectationMode.EXPECT_OR_DROP
    
    # LOW SEVERITY: Non-critical validations - just log
    # (descriptions, comments, optional fields)
    if rule_type in ["not_empty_string", "min_length"]:
        return ExpectationMode.EXPECT
    
    # DEFAULT: Log violations for analysis
    return ExpectationMode.EXPECT


def generate_expectations_for_column(
    col_name: str, 
    dtype: str, 
    silver_table: str = None,
    pii_columns: List[str] = None,
    phi_columns: List[str] = None,
    primary_keys: List[str] = None
) -> List[Tuple[str, str, str]]:
    """
    Generate DLT expectations for a single column based on its data type.
    
    Returns list of tuples: (expectation_mode, rule_name, condition)
    Example: [
        ('expect_or_drop', 'not_null_patient_id', 'patient_id IS NOT NULL'),
        ('expect', 'valid_age', 'age >= 0')
    ]
    """
    expectations = []
    pii_columns = pii_columns or []
    phi_columns = phi_columns or []
    primary_keys = primary_keys or []
    
    # Clean column name for expectation name
    safe_col = col_name.replace(" ", "_").replace("-", "_").lower()
    
    is_pii = col_name in pii_columns
    is_phi = col_name in phi_columns
    is_pk = col_name in primary_keys or col_name.lower().endswith("_id") and "primary" in col_name.lower()
    
    # 1. NOT NULL checks for important columns
    if any(keyword in col_name.lower() for keyword in ["id", "key", "date", "time", "name"]):
        mode = determine_expectation_mode(col_name, "not_null", is_pii, is_phi, is_pk)
        expectations.append((
            mode,
            f"not_null_{safe_col}",
            f"{col_name} IS NOT NULL"
        ))
    
    # 2. Type-specific validations
    if "int" in dtype.lower() or "long" in dtype.lower():
        # IDs must be positive
        if "id" in col_name.lower():
            mode = determine_expectation_mode(col_name, "positive", is_pii, is_phi, is_pk)
            expectations.append((
                mode,
                f"positive_{safe_col}",
                f"{col_name} > 0"
            ))
        
        # Counts, quantities, ages must be >= 0
        elif any(kw in col_name.lower() for kw in ["count", "quantity", "age", "duration"]):
            mode = determine_expectation_mode(col_name, "valid_range", is_pii, is_phi, False)
            expectations.append((
                mode,
                f"valid_{safe_col}",
                f"{col_name} >= 0"
            ))
    
    elif "float" in dtype.lower() or "double" in dtype.lower() or "decimal" in dtype.lower():
        # Amounts, prices must be positive (drop negative values)
        if any(kw in col_name.lower() for kw in ["amount", "price", "cost", "fee", "rate", "balance"]):
            mode = determine_expectation_mode(col_name, "positive_amount", is_pii, is_phi, False)
            expectations.append((
                mode,
                f"positive_{safe_col}",
                f"{col_name} > 0"
            ))
    
    elif "string" in dtype.lower():
        # String columns should not be empty (log only for non-critical)
        is_critical = any(kw in col_name.lower() for kw in ["name", "email", "phone", "address"])
        rule_type = "not_empty_string"
        mode = determine_expectation_mode(col_name, rule_type, is_pii, is_phi, False)
        
        expectations.append((
            mode,
            f"not_empty_{safe_col}",
            f"length(trim({col_name})) > 0"
        ))
        
        # Email validation (drop malformed emails)
        if "email" in col_name.lower():
            mode = determine_expectation_mode(col_name, "email_format", is_pii, is_phi, False)
            expectations.append((
                mode,
                f"valid_email_{safe_col}",
                f"{col_name} RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$'"
            ))
        
        # Phone validation (drop malformed phones)
        if "phone" in col_name.lower():
            mode = determine_expectation_mode(col_name, "phone_format", is_pii, is_phi, False)
            expectations.append((
                mode,
                f"valid_phone_{safe_col}",
                f"{col_name} RLIKE '^[0-9]{{10,15}}$'"
            ))
    
    elif "date" in dtype.lower() or "timestamp" in dtype.lower():
        # Dates should be valid and not in future (drop invalid dates)
        mode = determine_expectation_mode(col_name, "valid_date", is_pii, is_phi, False)
        expectations.append((
            mode,
            f"valid_date_{safe_col}",
            f"{col_name} IS NOT NULL AND {col_name} <= current_date()"
        ))
    
    # 3. PII/PHI specific checks (strict - drop violations)
    if is_pii or is_phi:
        mode = determine_expectation_mode(col_name, "not_null", True, True, False)
        expectations.append((
            mode,
            f"sensitive_not_null_{safe_col}",
            f"{col_name} IS NOT NULL"
        ))
    
    return expectations


def generate_table_level_expectations(
    table_name: str,
    columns: List[str],
    primary_keys: List[str] = None
) -> List[Tuple[str, str, str]]:
    """
    Generate table-level expectations (uniqueness, referential integrity).
    Returns list of tuples: (mode, rule_name, condition)
    """
    expectations = []
    primary_keys = primary_keys or []
    
    # Primary key uniqueness - FAIL if violated
    if primary_keys:
        for pk in primary_keys:
            expectations.append((
                ExpectationMode.EXPECT_OR_FAIL,
                f"unique_{pk}",
                f"COUNT(*) = COUNT(DISTINCT {pk})"
            ))
    
    return expectations


def generate_expectations_for_mapping(
    mapping_rows: List[Dict[str, Any]],
    df_dtypes: Dict[str, Dict[str, str]] = None,
    pii_columns: List[str] = None,
    phi_columns: List[str] = None,
    primary_keys: Dict[str, List[str]] = None  # table_name -> [pk_cols]
) -> Dict[str, List[Tuple[str, str, str]]]:
    """
    Generate all expectations grouped by Silver table.
    
    Returns:
    {
        "silver_patient": [
            ('expect_or_drop', 'not_null_patient_id', 'patient_id IS NOT NULL'),
            ('expect', 'valid_age', 'age >= 0')
        ],
        "silver_appointment": [...]
    }
    """
    df_dtypes = df_dtypes or {}
    pii_columns = pii_columns or []
    phi_columns = phi_columns or []
    primary_keys = primary_keys or {}
    table_expectations = {}
    
    for mapping in mapping_rows:
        silver_table = mapping.get("silver_table", "unknown")
        bronze_table = mapping.get("bronze_table")
        bronze_col = mapping.get("bronze_columns")
        silver_col = mapping.get("silver_column", bronze_col)
        
        # Get dtype from bronze table
        dtype = "string"  # default
        if bronze_table and bronze_table in df_dtypes:
            dtype = str(df_dtypes[bronze_table].get(bronze_col, "string"))
        
        # Get primary keys for this table
        table_pks = primary_keys.get(silver_table, [])
        
        # Generate expectations
        col_expectations = generate_expectations_for_column(
            silver_col, 
            dtype,
            silver_table=silver_table,
            pii_columns=pii_columns,
            phi_columns=phi_columns,
            primary_keys=table_pks
        )
        
        # Group by table
        if silver_table not in table_expectations:
            table_expectations[silver_table] = []
        table_expectations[silver_table].extend(col_expectations)
    
    # Remove duplicates per table (keep first occurrence)
    for table in table_expectations:
        seen = set()
        unique_exps = []
        for exp in table_expectations[table]:
            # Use rule_name as uniqueness key
            if exp[1] not in seen:
                seen.add(exp[1])
                unique_exps.append(exp)
        table_expectations[table] = unique_exps
    
    return table_expectations


def format_expectations_for_prompt(expectations_dict: Dict[str, List[Tuple[str, str, str]]]) -> str:
    """
    Format expectations dictionary into a string for LLM prompt.
    
    Returns formatted string with mode-specific decorators:
    '''
    Silver Table: silver_patient
    Expectations:
    - @dlt.expect_or_drop("not_null_patient_id", "patient_id IS NOT NULL")
    - @dlt.expect("valid_age", "age >= 0")
    
    Silver Table: silver_appointment
    Expectations:
    - @dlt.expect_or_drop("not_null_appointment_id", "appointment_id IS NOT NULL")
    - @dlt.expect_or_drop("valid_duration", "duration_minutes >= 0")
    '''
    """
    if not expectations_dict:
        return "No expectations defined."
    
    lines = []
    for table, expectations in expectations_dict.items():
        lines.append(f"\nSilver Table: {table}")
        lines.append("Expectations:")
        for mode, rule_name, condition in expectations:
            lines.append(f'  @dlt.{mode}("{rule_name}", "{condition}")')
    
    return "\n".join(lines)


# Severity Level Documentation
SEVERITY_LEVELS = {
    "CRITICAL": {
        "mode": ExpectationMode.EXPECT_OR_FAIL,
        "use_for": ["Primary keys", "Foreign key integrity", "System-critical fields"],
        "example": "Primary key cannot have duplicates - fail immediately"
    },
    "HIGH": {
        "mode": ExpectationMode.EXPECT_OR_DROP,
        "use_for": ["IDs", "PII/PHI nulls", "Invalid dates", "Malformed emails/phones", "Negative amounts"],
        "example": "Drop rows with invalid patient_id to maintain referential integrity"
    },
    "LOW": {
        "mode": ExpectationMode.EXPECT,
        "use_for": ["Optional fields", "Descriptions", "Comments", "Non-critical validations"],
        "example": "Log empty description fields but allow through for analysis"
    }
}