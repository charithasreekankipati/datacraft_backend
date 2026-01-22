from backend.llm_provider import llm



# --- Sensitive Data Classification using LLM ---
def classify_column(col_name):
    """
    Use an LLM to classify a column name as PII, PCI, PHI, or NON_SENSITIVE.
    """
    prompt = f"""
You are a data classification assistant. 
Given a column name, classify it into one of the following categories:
- PII: Personally Identifiable Information (e.g., name, email, phone, address, dob)
- PCI: Payment Card Information (e.g., card number, payment, account, cvv, transaction)
- PHI: Protected Health Information (e.g., health, diagnosis, patient, insurance)
- NON_SENSITIVE: Anything else

Return only one label: PII, PCI, PHI, or NON_SENSITIVE.

Column name: "{col_name}"
Answer:
"""

    # Call the LLM safely
    response = llm.invoke(prompt)

    # Handle model response (depends on how `llm` is implemented)
    if hasattr(response, "content"):
        label = response.content.strip().upper()
    elif isinstance(response, str):
        label = response.strip().upper()
    else:
        label = "NON_SENSITIVE"

    # Fallback validation
    if label not in ["PII", "PCI", "PHI", "NON_SENSITIVE"]:
        label = "NON_SENSITIVE"

    return label




def sanitize_llm_output(raw_value: str):
    """Clean LLM labels so they EXACTLY match dropdown options."""
    if raw_value is None:
        return "NON_SENSITIVE"

    cleaned = (
        str(raw_value)
        .strip()               # remove leading/trailing spaces & \n
        .lower()               # normalize case
        .replace("-", "_")     # fix NON-SENSITIVE → non_sensitive
        .replace(" ", "")      # remove accidental spaces like "P II"
        .replace(".", "")      # remove periods like "P.II"
        .replace('"', "")      # remove quotes
        .replace("'", "")      # remove quotes
        .replace("\t", "")     # remove tabs
    )

    mapping = {
        "non_sensitive": "NON_SENSITIVE",
        "pii": "PII",
        "pci": "PCI",
        "phi": "PHI"
    }

    return mapping.get(cleaned, "NON_SENSITIVE")

