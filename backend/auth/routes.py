"""
Authentication routes for MSAL login flow
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import RedirectResponse, JSONResponse
from backend.auth.msal_auth import auth_manager, get_current_user
from backend.auth.msal_config import MSAL_CONFIG
from typing import Dict, Any

router = APIRouter(prefix="/api/auth", tags=["Authentication"])

@router.post("/login/microsoft")
async def login_microsoft(payload: Dict[str, str]):
    token = payload.get("access_token")
    user_claims = auth_manager.verify_frontend_token(token)
    return {
        "success": True,
        "data": {
            "user": {"email": user_claims.get("preferred_username"), "id": user_claims.get("oid")},
            "accessToken": token
        }
    }

@router.get("/callback")
async def auth_callback(
    code: str = Query(...),
    state: str = Query(...),
    client_info: str = Query(None),
    session_state: str = Query(None)
):
    """
    Handle OAuth callback from Azure AD.
    Exchange authorization code for access token.
    """
    try:
        print(f"DEBUG CALLBACK: Received state = {state}")
        print(f"DEBUG CALLBACK: Code = {code[:20]}...")
        
        # Build the auth response dict that MSAL expects
        auth_response = {
            "code": code,
            "state": state,
        }
        if client_info:
            auth_response["client_info"] = client_info
        if session_state:
            auth_response["session_state"] = session_state
        
        token_result = auth_manager.acquire_token_by_auth_code(auth_response, state)
        
        if "error" in token_result:
            raise HTTPException(
                status_code=400,
                detail=token_result.get("error_description", "Authentication failed")
            )
        
        access_token = token_result.get("access_token")
        id_token = token_result.get("id_token")
        
        if not access_token:
            raise HTTPException(
                status_code=400,
                detail="No access token received"
            )
        
        return JSONResponse({
            "success": True,
            "access_token": access_token,
            "id_token": id_token,
            "token_type": "Bearer",
            "user": token_result.get("id_token_claims", {})
        })
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Callback failed: {str(e)}")

@router.get("/me")
async def get_current_user_info(current_user: Dict[str, Any] = Depends(get_current_user)):
    """
    Get current authenticated user information.
    Requires valid Bearer token in Authorization header.
    """
    return {
        "success": True,
        "user": current_user
    }

@router.post("/logout")
async def logout():
    """
    Logout endpoint. Client should clear token from storage.
    """
    return {
        "success": True,
        "message": "Logout successful. Please clear your token from local storage."
    }

@router.get("/token-info")
async def get_token_info(current_user: Dict[str, Any] = Depends(get_current_user)):
    """
    Get information about current user token.
    """
    return {
        "success": True,
        "user_id": current_user.get("oid"),
        "email": current_user.get("email"),
        "name": current_user.get("name")
    }
