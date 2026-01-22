# backend/state_store.py
from typing import Dict, Any

# This centralizes the state so routes and main don't conflict
agent_states: Dict[str, Any] = {}