"""
MSAL Authentication utilities for FastAPI
"""
from typing import Optional, Dict, Any
import msal
from fastapi import HTTPException, Depends, Header
from jwt import decode, PyJWTError
import os
from dotenv import load_dotenv
from pathlib import Path
from functools import lru_cache
import time
from jose import jwt  
import requests




env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


AZURE_AD_ISSUER = f"https://login.microsoftonline.com/{os.getenv('AZURE_TENANT_ID')}/v2.0"
AZURE_AD_JWKS_URL = f"https://login.microsoftonline.com/{os.getenv('AZURE_TENANT_ID')}/discovery/v2.0/keys"


auth_flow_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TIMEOUT = 3600  

def cleanup_expired_flows():
    """Remove expired auth flows from cache"""
    current_time = time.time()
    expired_states = [
        state for state, data in auth_flow_cache.items()
        if current_time - data.get("timestamp", 0) > CACHE_TIMEOUT
    ]
    for state in expired_states:
        del auth_flow_cache[state]

# Dependency for protected routes
async def get_current_user(authorization: str = Header(None)) -> Dict[str, Any]:
    """Dependency to validate user token for protected routes"""
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing authorization header")
    
    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Invalid authentication scheme")
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    
    try:
        claims = auth_manager.verify_frontend_token(token)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    
    # Extract user info from claims
    user_info = {
        "oid": claims.get("oid"),  # Object ID
        "email": claims.get("email"),
        "name": claims.get("name"),
        "given_name": claims.get("given_name"),
    }
    
    if not user_info["oid"]:
        raise HTTPException(status_code=401, detail="Invalid token claims")
    
    return user_info

def validate_token(token: str) -> Dict[str, Any]:
    """Validate JWT token and extract claims"""
    try:
        # Decode without verification (for demo)
        # In production, verify signature with Azure's public keys
        decoded = decode(
            token,
            options={"verify_signature": False},
            algorithms=["RS256"]
        )
        return decoded
    except PyJWTError as e:
        raise HTTPException(
            status_code=401,
            detail=f"Invalid token: {str(e)}"
        )

class MSALAuthManager:
    """Manages MSAL authentication operations"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.app = None
    
    def verify_frontend_token(self, token: str) -> Dict[str, Any]:
        """
        Validates the token received from the React frontend.
        In production, this should verify the signature against Azure's public keys (JWKS).
        """
        try:
            decoded_claims = decode(
                token,
                options={
                    "verify_signature": False,  
                    "verify_aud": False,       
                },
                algorithms=["RS256"]
            )

            # Check for expiration
            current_time = time.time()
            if decoded_claims.get("exp", 0) < current_time:
                raise ValueError("Token has expired")

            return decoded_claims

        except Exception as e:
            print(f"Token validation error: {str(e)}")
            raise ValueError(f"Invalid token: {str(e)}")
        

    def get_msal_app(self):
        """Initialize and cache MSAL app based on client type"""
        if not self.app:
            print(f"DEBUG GET_MSAL_APP: Config = {self.config}")
            print(f"DEBUG GET_MSAL_APP: is_public_client = {self.config.get('is_public_client')}")
            print(f"DEBUG GET_MSAL_APP: client_secret present = {'client_secret' in self.config}")
            
            if self.config.get("is_public_client"):
                # Use PublicClientApplication for public clients (no client_secret)
                print("DEBUG: Using PublicClientApplication")
                self.app = msal.PublicClientApplication(
                    client_id=self.config["client_id"],
                    authority=self.config["authority"]
                )
            else:
                # Use ConfidentialClientApplication for confidential clients (with client_secret)
                print("DEBUG: Using ConfidentialClientApplication")
                self.app = msal.ConfidentialClientApplication(
                    client_id=self.config["client_id"],
                    client_credential=self.config.get("client_secret"),
                    authority=self.config["authority"]
                )
        return self.app
    
    def get_auth_code_flow_params(self):
        """Get authorization code flow parameters"""
        app = self.get_msal_app()
        auth_params = app.initiate_auth_code_flow(
            scopes=self.config["scopes"],
            redirect_uri=self.config["redirect_uri"]
        )
        # Store flow in cache using state as key with timestamp
        state = auth_params.get("state")
        if state:
            auth_flow_cache[state] = {
                "flow": auth_params,
                "timestamp": time.time()
            }
            print(f"DEBUG: Stored auth flow for state: {state}")
            print(f"DEBUG: Cache keys: {list(auth_flow_cache.keys())}")
        return auth_params
    
    def acquire_token_by_auth_code(self, auth_response: Dict[str, str], state: str):
        """Exchange auth code for access token"""
        app = self.get_msal_app()
        
        # Cleanup expired flows first
        cleanup_expired_flows()
        
        print(f"DEBUG ACQUIRE: Looking for state: {state}")
        print(f"DEBUG ACQUIRE: Available states in cache: {list(auth_flow_cache.keys())}")
        print(f"DEBUG ACQUIRE: Cache contents: {auth_flow_cache}")
        
        # Retrieve the stored auth flow using state
        if state not in auth_flow_cache:
            raise ValueError(f"Invalid state: auth flow not found. Available states: {list(auth_flow_cache.keys())}")
        
        auth_flow = auth_flow_cache[state]["flow"]  # Don't remove, keep for retry
        
        try:
            result = app.acquire_token_by_auth_code_flow(
                auth_flow,
                auth_response
            )
            
            if "error" in result:
                raise ValueError(f"Authentication failed: {result.get('error_description')}")
            
            # Only remove after successful token acquisition
            if state in auth_flow_cache:
                del auth_flow_cache[state]
            
            return result
        except Exception as e:
            raise ValueError(f"Token acquisition failed: {str(e)}")

# Initialize auth manager
from backend.auth.msal_config import MSAL_CONFIG
auth_manager = MSALAuthManager(MSAL_CONFIG)
