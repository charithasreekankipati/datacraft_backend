"""
MSAL Configuration for Azure AD Authentication
Supports both Public and Confidential Client flows
"""
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from the correct path
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Determine if this is a public client (no secret provided or explicitly set)
is_public = not os.getenv("AZURE_CLIENT_SECRET") or os.getenv("AZURE_IS_PUBLIC_CLIENT", "false").lower() == "true"
 
# Azure AD Configuration
MSAL_CONFIG = {
    "client_id": os.getenv("AZURE_CLIENT_ID"),
    "authority": f"https://login.microsoftonline.com/{os.getenv('AZURE_TENANT_ID')}",
    "redirect_uri": os.getenv("REDIRECT_URI", "http://localhost:8000/api/auth/callback"),
    "scopes": [".default"],  # Use .default for confidential clients, specific scopes for public
    "is_public_client": is_public,
}
 
# Only add client_secret if it exists (for confidential client)
if not is_public and os.getenv("AZURE_CLIENT_SECRET"):
    MSAL_CONFIG["client_secret"] = os.getenv("AZURE_CLIENT_SECRET")
 
# Validate required environment variables
REQUIRED_ENV_VARS = ["AZURE_CLIENT_ID", "AZURE_TENANT_ID"]
 
def validate_msal_config():
    """Validate that all required MSAL environment variables are set"""
    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing environment variables for MSAL: {', '.join(missing)}")
    # If not public client, require client secret
    if not MSAL_CONFIG["is_public_client"] and not MSAL_CONFIG.get("client_secret"):
        raise ValueError("AZURE_CLIENT_SECRET is required for confidential client")
    return True