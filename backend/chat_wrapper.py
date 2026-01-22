# chat_wrapper.py
from mlflow.pyfunc import ChatAgent
from backend.agents.agent_state import AgentState
from mlflow.types.agent import (
    ChatAgentMessage,
    ChatAgentResponse,
    ChatAgentChunk,
    ChatContext,
)
from typing import Optional, Generator, Any
from langgraph.graph.state import CompiledStateGraph
from backend.agent import get_multi_agent
from uuid import uuid4

from langchain_core.messages import BaseMessage

class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent_state: Optional[AgentState] = None):
        self.agent: CompiledStateGraph = get_multi_agent()
        # Initialize with provided state, or a fresh empty AgentState if None
        self.agent_state = agent_state or AgentState()

    def _normalize_msg(self, msg: Any) -> dict:
        if isinstance(msg, dict):
            return msg
        if isinstance(msg, BaseMessage):
            return {
                "role": getattr(msg, "role", "assistant"),
                "content": str(getattr(msg, "content", "")),
                "name": getattr(msg, "name", None),
            }
        if hasattr(msg, "__dict__"):
            d = msg.__dict__
            return {
                "role": str(d.get("role", "assistant")),
                "content": str(d.get("content", "")),
                "name": d.get("name", None),
            }
        raise TypeError(f"Unhandled message type: {type(msg)}")

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        # Update messages from context if provided
        if context and context.messages:
            self.agent_state.messages = context.messages

        # FIX: Ensure self.agent_state is not None before calling .dict()
        request = {
            **self.agent_state.dict(exclude_none=True),
            "messages": [m.model_dump(exclude_none=True) for m in messages],
        }
        
        # Update internal state tracking
        self.agent_state = AgentState(**request)

        output_messages = []
        # Run the LangGraph workflow
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                for msg in node_data.get("messages", []):
                    m = self._normalize_msg(msg)
                    output_messages.append(ChatAgentMessage(
                        id=str(uuid4()),
                        role=m["role"],
                        content=m["content"],
                        name=m.get("name"),
                    ))

        return ChatAgentResponse(messages=output_messages)

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        if context and context.messages:
            self.agent_state.messages = context.messages

        # FIX: Ensure self.agent_state is not None before calling .dict()
        request = {
            **self.agent_state.dict(exclude_none=True),
            "messages": [m.model_dump(exclude_none=True) for m in messages],
        }

        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                for msg in node_data.get("messages", []):
                    m = self._normalize_msg(msg)
                    yield ChatAgentChunk(
                        delta=ChatAgentMessage(
                            id=str(uuid4()),
                            role=m["role"],
                            content=m["content"],
                            name=m.get("name", None),
                        )
                    )