# agents/mapping_deduplicator.py
"""
Deduplicate column mappings to prevent duplicate column errors in DLT code.
"""

from typing import List, Dict, Any
from collections import defaultdict


def deduplicate_silver_mappings(mapping_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Detect and resolve duplicate silver column names within the same table.
    
    Problem: Multiple bronze columns mapping to the same silver column creates duplicates.
    Example:
        clinic_id -> practitioner.identifier
        npi_number -> practitioner.identifier  # DUPLICATE!
    
    Solution: Make column names unique by appending bronze column name.
        clinic_id -> practitioner.identifier_clinic_id
        npi_number -> practitioner.identifier_npi_number
    
    Returns:
    {
        "resolved_mappings": [...],  # Updated mappings with unique column names
        "duplicates_found": {...},   # Details of duplicates detected
        "warnings": [...]            # List of warning messages
    }
    """
    
    # Group by (silver_table, silver_column) to find duplicates
    column_usage = defaultdict(list)
    
    for idx, mapping in enumerate(mapping_rows):
        silver_table = mapping.get("silver_table", "").lower()
        silver_column = mapping.get("silver_column", "").lower()
        bronze_column = mapping.get("bronze_columns", "")
        
        key = (silver_table, silver_column)
        column_usage[key].append({
            "index": idx,
            "bronze_column": bronze_column,
            "mapping": mapping
        })
    
    # Detect duplicates
    duplicates_found = {}
    for key, usages in column_usage.items():
        if len(usages) > 1:
            silver_table, silver_column = key
            duplicates_found[key] = [u["bronze_column"] for u in usages]
    
    # Resolve duplicates by making column names unique
    resolved_mappings = []
    warnings = []
    
    for idx, mapping in enumerate(mapping_rows):
        silver_table = mapping.get("silver_table", "").lower()
        silver_column = mapping.get("silver_column", "").lower()
        bronze_column = mapping.get("bronze_columns", "")
        bronze_table = mapping.get("bronze_table", "")
        
        key = (silver_table, silver_column)
        
        # If this column is used multiple times, make it unique
        if key in duplicates_found:
            # Create unique column name by appending bronze column
            # Replace dots with underscores to avoid nested struct issues
            unique_column = f"{silver_column}_{bronze_column}".replace(".", "_")
            
            warnings.append(
                f"⚠️ Duplicate column '{silver_column}' in table '{silver_table}' "
                f"from bronze column '{bronze_column}' → Renamed to '{unique_column}'"
            )
            
            resolved_mappings.append({
                "bronze_table": bronze_table,
                "bronze_columns": bronze_column,
                "silver_table": silver_table,
                "silver_column": unique_column,
                "original_column": silver_column,  # Keep original for reference
                "is_deduplicated": True
            })
        else:
            # No conflict, keep as-is but clean up
            clean_column = silver_column.replace(".", "_")
            
            resolved_mappings.append({
                "bronze_table": bronze_table,
                "bronze_columns": bronze_column,
                "silver_table": silver_table,
                "silver_column": clean_column,
                "original_column": silver_column,
                "is_deduplicated": False
            })
    
    return {
        "resolved_mappings": resolved_mappings,
        "duplicates_found": duplicates_found,
        "warnings": warnings,
        "total_resolved": len(duplicates_found)
    }


def validate_no_duplicates(mapping_rows: List[Dict[str, Any]]) -> bool:
    """
    Check if there are any duplicate column names in the mappings.
    Returns True if no duplicates, False if duplicates exist.
    """
    column_usage = defaultdict(int)
    
    for mapping in mapping_rows:
        silver_table = mapping.get("silver_table", "").lower()
        silver_column = mapping.get("silver_column", "").lower()
        key = (silver_table, silver_column)
        column_usage[key] += 1
    
    # Check for duplicates
    duplicates = {k: v for k, v in column_usage.items() if v > 1}
    
    if duplicates:
        print("❌ Duplicate columns detected:")
        for (table, col), count in duplicates.items():
            print(f"   {table}.{col} appears {count} times")
        return False
    
    print("✅ No duplicate columns found")
    return True


def get_deduplication_report(result: Dict[str, Any]) -> str:
    """
    Generate a human-readable report of the deduplication process.
    """
    lines = ["# Column Deduplication Report\n"]
    
    if result["total_resolved"] == 0:
        lines.append("✅ No duplicate columns found - all mappings are clean!\n")
    else:
        lines.append(f"⚠️ Found and resolved {result['total_resolved']} duplicate column conflicts\n")
        lines.append("\n## Duplicates Detected:\n")
        
        for (table, col), bronze_cols in result["duplicates_found"].items():
            lines.append(f"\n### Table: `{table}`, Column: `{col}`")
            lines.append(f"Bronze columns mapping to this: {', '.join(bronze_cols)}")
        
        lines.append("\n## Resolutions Applied:\n")
        for warning in result["warnings"]:
            lines.append(f"- {warning}")
    
    return "\n".join(lines)