# agents/mapping_processor.py
"""
Pre-process mappings to ensure clean, conflict-free table generation
"""

from typing import Dict, List, Any
from collections import defaultdict


def normalize_table_name(name: str) -> str:
    """Convert table name to lowercase with underscores"""
    return name.lower().replace(" ", "_").replace("-", "_")


def group_columns_by_silver_table(mapping_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group mappings by silver table to avoid creating duplicate tables.
    
    Returns:
    {
        "silver_practitioner": [
            {"bronze_table": "providers", "bronze_columns": "provider_id", "silver_column": "identifier"},
            {"bronze_table": "providers", "bronze_columns": "npi_number", "silver_column": "npi"}
        ],
        ...
    }
    """
    grouped = defaultdict(list)
    
    for mapping in mapping_rows:
        silver_table = normalize_table_name(mapping.get("silver_table", "unknown"))
        bronze_table = mapping.get("bronze_table", "")
        bronze_col = mapping.get("bronze_columns", "")
        silver_col = mapping.get("silver_column", bronze_col)
        
        grouped[silver_table].append({
            "bronze_table": bronze_table,
            "bronze_columns": bronze_col,
            "silver_column": silver_col
        })
    
    return dict(grouped)


def detect_duplicate_table_names(mapping_rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    Find silver tables that appear multiple times with different bronze sources.
    
    Returns dict of problematic tables:
    {
        "silver_practitioner": ["provider_id", "npi_number", "clinic_id"]  # All map to same table
    }
    """
    table_sources = defaultdict(list)
    bronze_sources = defaultdict(set)  # Track unique bronze tables per silver table
    
    for mapping in mapping_rows:
        silver_table = normalize_table_name(mapping.get("silver_table", "unknown"))
        bronze_table = mapping.get("bronze_table", "")
        bronze_col = mapping.get("bronze_columns", "")
        
        table_sources[silver_table].append(bronze_col)
        bronze_sources[silver_table].add(bronze_table)
    
    # Warn if multiple Silver tables come from SAME Bronze source
    problematic = {}
    for silver_table, bronze_set in bronze_sources.items():
        if len(bronze_set) == 1:  # All mappings from same Bronze source
            col_count = len(table_sources[silver_table])
            if col_count > 1:
                # Multiple Silver tables from same Bronze = should be ONE table
                problematic[silver_table] = table_sources[silver_table]
    
    return problematic


def detect_single_bronze_split(mapping_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    CRITICAL: Detect if multiple Silver tables are being created from a SINGLE Bronze source.
    This is the ROOT CAUSE of join failures in Gold layer.
    
    Returns:
    {
        "is_split": True/False,
        "bronze_source": "bronze_providers",
        "silver_tables": ["silver_location", "silver_practitioner", "silver_organization"],
        "should_consolidate": True/False,
        "consolidated_name": "silver_providers"
    }
    """
    # Group by bronze table
    bronze_to_silver = defaultdict(set)
    
    for mapping in mapping_rows:
        bronze_table = mapping.get("bronze_table", "")
        silver_table = normalize_table_name(mapping.get("silver_table", "unknown"))
        bronze_to_silver[bronze_table].add(silver_table)
    
    # Check for single bronze source with multiple silver tables
    for bronze_table, silver_set in bronze_to_silver.items():
        if len(silver_set) > 1:
            # CRITICAL ERROR: Multiple Silver tables from ONE Bronze source
            return {
                "is_split": True,
                "bronze_source": bronze_table,
                "silver_tables": list(silver_set),
                "should_consolidate": True,
                "consolidated_name": f"silver_{bronze_table.replace('bronze_', '')}",
                "error_message": f"ERROR: Detected {len(silver_set)} Silver tables from single Bronze source '{bronze_table}'. This will cause join failures in Gold layer!"
            }
    
    return {
        "is_split": False,
        "should_consolidate": False
    }


def resolve_duplicate_tables(
    mapping_rows: List[Dict[str, Any]], 
    strategy: str = "group"
) -> List[Dict[str, Any]]:
    """
    Resolve duplicate table mappings using specified strategy.
    
    Strategies:
    - "group": Combine all columns for same silver table into one (default)
    - "suffix": Add bronze column suffix to make unique tables
    
    Returns: Updated mapping_rows with no duplicates
    """
    if strategy == "group":
        # Keep mappings as-is but mark them for grouping
        # The code generator will create one table with all columns
        return mapping_rows
    
    elif strategy == "suffix":
        # Add unique suffixes to prevent duplicates
        table_counts = defaultdict(int)
        resolved = []
        
        for mapping in mapping_rows:
            silver_table = normalize_table_name(mapping.get("silver_table", "unknown"))
            bronze_col = mapping.get("bronze_columns", "")
            
            table_counts[silver_table] += 1
            
            # If this is 2nd+ occurrence, add suffix
            if table_counts[silver_table] > 1:
                new_table = f"{silver_table}_from_{bronze_col}"
                mapping["silver_table"] = new_table
            else:
                mapping["silver_table"] = silver_table
            
            resolved.append(mapping)
        
        return resolved
    
    return mapping_rows


def consolidate_single_bronze_split(mapping_rows: List[Dict[str, Any]], split_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    CRITICAL FIX: Consolidate multiple Silver tables from single Bronze source into ONE table.
    
    Example:
    Input: [
        {"bronze_table": "bronze_providers", "silver_table": "silver_location", "bronze_columns": "address"},
        {"bronze_table": "bronze_providers", "silver_table": "silver_practitioner", "bronze_columns": "provider_id"},
        {"bronze_table": "bronze_providers", "silver_table": "silver_organization", "bronze_columns": "org_name"}
    ]
    
    Output: [
        {"bronze_table": "bronze_providers", "silver_table": "silver_providers", "bronze_columns": "address"},
        {"bronze_table": "bronze_providers", "silver_table": "silver_providers", "bronze_columns": "provider_id"},
        {"bronze_table": "bronze_providers", "silver_table": "silver_providers", "bronze_columns": "org_name"}
    ]
    """
    if not split_info.get("should_consolidate"):
        return mapping_rows
    
    bronze_source = split_info["bronze_source"]
    consolidated_name = split_info["consolidated_name"]
    
    print(f"\n CONSOLIDATING: Merging {len(split_info['silver_tables'])} Silver tables into '{consolidated_name}'")
    print(f"   Bronze source: {bronze_source}")
    print(f"   Silver tables being merged: {', '.join(split_info['silver_tables'])}")
    
    consolidated = []
    for mapping in mapping_rows:
        if mapping.get("bronze_table") == bronze_source:
            # Force all mappings from this bronze source to use the consolidated name
            mapping["silver_table"] = consolidated_name
        consolidated.append(mapping)
    
    return consolidated


def detect_duplicate_silver_columns(mapping_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, str]]]:
    """
    CRITICAL: Detect if multiple bronze columns map to the SAME silver column name within a table.
    This causes DUPLICATE_COLUMN_NAMES errors.
    
    Returns:
    {
        "silver_databricks": [
            {"silver_column": "id", "bronze_columns": ["workspace_id", "cluster_id"]},
            {"silver_column": "time", "bronze_columns": ["start_time", "end_time"]}
        ]
    }
    """
    # Group by table, then by silver column
    table_columns = defaultdict(lambda: defaultdict(list))
    
    for mapping in mapping_rows:
        silver_table = normalize_table_name(mapping.get("silver_table", "unknown"))
        silver_col = mapping.get("silver_column", "")
        bronze_col = mapping.get("bronze_columns", "")
        
        if silver_col:  # Only track if silver column is specified
            table_columns[silver_table][silver_col].append(bronze_col)
    
    # Find duplicates
    duplicates = {}
    for table, columns in table_columns.items():
        table_dups = []
        for silver_col, bronze_cols in columns.items():
            if len(bronze_cols) > 1:
                # Multiple bronze columns map to same silver column
                table_dups.append({
                    "silver_column": silver_col,
                    "bronze_columns": bronze_cols
                })
        
        if table_dups:
            duplicates[table] = table_dups
    
    return duplicates


def fix_duplicate_silver_columns(mapping_rows: List[Dict[str, Any]], duplicates: Dict[str, List[Dict[str, str]]]) -> List[Dict[str, Any]]:
    """
    CRITICAL FIX: Resolve duplicate silver column names by adding bronze column suffix.
    
    Example:
    Input: [
        {"bronze_columns": "workspace_id", "silver_column": "id"},
        {"bronze_columns": "cluster_id", "silver_column": "id"}  # Duplicate!
    ]
    
    Output: [
        {"bronze_columns": "workspace_id", "silver_column": "id_workspace_id"},
        {"bronze_columns": "cluster_id", "silver_column": "id_cluster_id"}
    ]
    """
    if not duplicates:
        return mapping_rows
    
    # Build lookup of which columns need fixing
    fix_map = {}  # {(silver_table, silver_column): True}
    for table, dups in duplicates.items():
        for dup in dups:
            fix_map[(table, dup["silver_column"])] = True
    
    print(f"\n🔧 FIXING DUPLICATE SILVER COLUMNS:")
    for table, dups in duplicates.items():
        for dup in dups:
            print(f"   Table '{table}': column '{dup['silver_column']}' has {len(dup['bronze_columns'])} mappings")
            print(f"      Bronze columns: {', '.join(dup['bronze_columns'])}")
    
    # Fix the mappings
    fixed = []
    for mapping in mapping_rows:
        silver_table = normalize_table_name(mapping.get("silver_table", "unknown"))
        silver_col = mapping.get("silver_column", "")
        bronze_col = mapping.get("bronze_columns", "")
        
        # Check if this column needs fixing
        if (silver_table, silver_col) in fix_map:
            # Add bronze column as suffix to make unique
            new_silver_col = f"{silver_col}_{bronze_col}".replace(".", "_").replace("-", "_")
            mapping["silver_column"] = new_silver_col
            print(f" Fixed: {bronze_col} → {new_silver_col} (was: {silver_col})")
        
        fixed.append(mapping)
    
    print()
    return fixed



def validate_gold_references(
    gold_mapping_rows: List[Dict[str, Any]], 
    silver_tables: List[str]
) -> List[Dict[str, Any]]:
    """
    Ensure gold mappings only reference silver tables that exist.
    
    Returns: Filtered gold_mapping_rows with only valid references
    """
    silver_set = set(normalize_table_name(t) for t in silver_tables)
    
    valid_gold = []
    for mapping in gold_mapping_rows:
        source_table = normalize_table_name(mapping.get("source_table", ""))
        if source_table in silver_set:
            valid_gold.append(mapping)
    
    return valid_gold


def generate_join_keys(grouped_mappings: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
    """
    Suggest join keys for gold layer based on column names.
    
    Returns:
    {
        "silver_practitioner": "identifier",  # Primary key candidate
        "silver_person": "row_id"             # Surrogate key
    }
    """
    join_keys = {}
    
    for table, columns in grouped_mappings.items():
        # Look for ID columns
        id_columns = [
            col["silver_column"] for col in columns 
            if "id" in col["silver_column"].lower() or "identifier" in col["silver_column"].lower()
        ]
        
        if id_columns:
            # Use first ID column as join key
            join_keys[table] = id_columns[0]
        else:
            # No natural key, use surrogate
            join_keys[table] = "row_id"
    
    return join_keys


def format_grouped_mappings_for_prompt(grouped: Dict[str, List[Dict[str, Any]]]) -> str:
    """
    Format grouped mappings into readable string for LLM prompt.
    
    Returns formatted string showing how to combine columns:
    '''
    Silver Table: silver_practitioner
    Columns:
      - provider_id → identifier
      - npi_number → npi
      - license_number → license_id
    
    Silver Table: silver_person
    Columns:
      - provider_name → name
      - gender → gender
    '''
    """
    lines = []
    for table, columns in grouped.items():
        lines.append(f"\nSilver Table: {table}")
        lines.append("Columns:")
        for col in columns:
            lines.append(f"  - {col['bronze_columns']} → {col['silver_column']}")
    
    return "\n".join(lines)


def suggest_gold_tables(
    grouped_silver: Dict[str, List[Dict[str, Any]]], 
    kpis: str = ""
) -> List[Dict[str, Any]]:
    """
    Suggest dimensional model gold tables based on silver tables and KPIs.
    
    Returns list of gold table suggestions:
    [
        {
            "gold_table": "gold_dim_provider",
            "type": "dimension",
            "source_tables": ["silver_practitioner", "silver_person"],
            "join_key": "row_id"
        },
        {
            "gold_table": "gold_fact_appointments",
            "type": "fact",
            "source_tables": ["silver_appointment"],
            "measures": ["count", "duration_avg"]
        }
    ]
    """
    suggestions = []
    
    # Identify dimensions (entities with attributes)
    dimension_tables = [t for t in grouped_silver.keys() if any(
        kw in t for kw in ["person", "practitioner", "patient", "organization", "location"]
    )]
    
    # Identify facts (transactions, events)
    fact_tables = [t for t in grouped_silver.keys() if any(
        kw in t for kw in ["appointment", "encounter", "claim", "observation"]
    )]
    
    # Create dimension suggestions
    for dim_table in dimension_tables:
        suggestions.append({
            "gold_table": f"gold_dim_{dim_table.replace('silver_', '')}",
            "type": "dimension",
            "source_tables": [dim_table],
            "join_key": "row_id"
        })
    
    # Create fact suggestions
    for fact_table in fact_tables:
        suggestions.append({
            "gold_table": f"gold_fact_{fact_table.replace('silver_', '')}",
            "type": "fact",
            "source_tables": [fact_table],
            "measures": ["count", "avg", "sum"]
        })
    
    return suggestions


# Main processing function
def process_mappings_for_code_generation(
    mapping_rows: List[Dict[str, Any]],
    gold_mapping_rows: List[Dict[str, Any]] = None,
    strategy: str = "group"
) -> Dict[str, Any]:
    """
    Main entry point: process all mappings and return clean, validated structure.
    
    Returns:
    {
        "silver_grouped": {...},  # Grouped by table
        "silver_tables": [...],   # List of unique tables
        "gold_validated": [...],  # Valid gold mappings
        "join_keys": {...},       # Suggested join keys
        "duplicates_found": {...}, # Any duplicate issues
        "split_detected": {...},   # CRITICAL: Single bronze split info
        "duplicate_columns": {...}, # CRITICAL: Duplicate silver column names
        "formatted_prompt": "..."  # Readable format for LLM
    }
    """
    gold_mapping_rows = gold_mapping_rows or []
    
    # Step 1: Normalize all table names
    for mapping in mapping_rows:
        mapping["silver_table"] = normalize_table_name(mapping.get("silver_table", "unknown"))
    
    # Step 2: CRITICAL CHECK - Detect single Bronze source creating multiple Silver tables
    split_info = detect_single_bronze_split(mapping_rows)
    
    if split_info.get("is_split"):
        print(f"\n🚨 {split_info['error_message']}")
        # Automatically consolidate to prevent join failures
        mapping_rows = consolidate_single_bronze_split(mapping_rows, split_info)
        print(f"✅ Consolidation complete. All mappings now use: {split_info['consolidated_name']}\n")
    
    # Step 3: CRITICAL CHECK - Detect duplicate silver column names (NEW!)
    duplicate_columns = detect_duplicate_silver_columns(mapping_rows)
    
    if duplicate_columns:
        print(f"\n🚨 DUPLICATE SILVER COLUMNS DETECTED!")
        # Automatically fix by adding bronze column suffix
        mapping_rows = fix_duplicate_silver_columns(mapping_rows, duplicate_columns)
        print(f"✅ Duplicate columns fixed with unique suffixes\n")
    
    # Step 4: Detect duplicates (table-level)
    duplicates = detect_duplicate_table_names(mapping_rows)
    
    # Step 5: Resolve duplicates
    resolved_mappings = resolve_duplicate_tables(mapping_rows, strategy=strategy)
    
    # Step 6: Group by silver table
    grouped = group_columns_by_silver_table(resolved_mappings)
    
    # Step 7: Get unique silver tables
    silver_tables = list(grouped.keys())
    
    # Step 8: Validate gold references
    valid_gold = validate_gold_references(gold_mapping_rows, silver_tables)
    
    # Step 9: Generate join keys
    join_keys = generate_join_keys(grouped)
    
    # Step 10: Format for prompt
    formatted = format_grouped_mappings_for_prompt(grouped)
    
    return {
        "silver_grouped": grouped,
        "silver_tables": silver_tables,
        "gold_validated": valid_gold,
        "join_keys": join_keys,
        "duplicates_found": duplicates,
        "split_detected": split_info,  # Critical validation info
        "duplicate_columns": duplicate_columns,  # NEW: Column-level duplicates
        "formatted_prompt": formatted,
        "resolved_mappings": resolved_mappings
    }