#superviser_agent.py
import re
from langchain_core.runnables import RunnableLambda
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel
from typing import Literal
from backend.llm_provider import llm
from backend.agents.agent_state import AgentState

MAX_ITERATIONS = 3

def strip_think_parser(raw_text: str) -> str:
    return re.sub(r"<think>.*?</think>", "", raw_text, flags=re.DOTALL).strip()

def get_supervisor_agent(worker_descriptions: dict):

    formatted_descriptions = "\n".join(
        f"- {name}: {desc}" for name, desc in worker_descriptions.items()
    )

    options = ["FINISH"] + list(worker_descriptions.keys())

    class NextNode(BaseModel):
        next_node: Literal[tuple(options)]

    parser = PydanticOutputParser(pydantic_object=NextNode)

    system_prompt = (
        f"You are a routing agent. Your job is to choose the next agent to handle the user's request.\n\n"
        f"When the user asks to *generate* code, improve code, fix code, or create a pipeline, choose `Coder`.\n"
        f"When the user asks to *explain, show, describe, summarize, reference or ask about existing code*, choose `QNA`.\n"
        f"Questions starting with what / which / explain / describe / tell me about / do you have / already generated should use `QNA`.\n"
        f"Only choose `Coder` for actionable code creation tasks.\n\n"
        f"The valid options are: {', '.join(options)}\n\n"
        f"{formatted_descriptions}\n\n"
        f"Return ONLY a valid JSON object using the key `next_node`, like this:\n"
        f'{{"next_node": "Genie"}}\n\n'
        f"Strict rules:\n"
        f"- No other output\n"
        f"- No explanation\n"
        f"- No markdown or formatting\n"
        f"- Do NOT use Python-style output like next_node='Genie'\n"
        f"- You must return valid JSON only"
    )

    def supervisor_agent_node(state: AgentState) -> AgentState:
        count = state.iteration_count + 1
        if count > MAX_ITERATIONS:
            return state.copy(update={"iteration_count": count, "next_node": "FINISH"})
        
        last_user = next((m for m in reversed(state.messages) if m["role"] == "user"), None)
        if last_user and "what did i upload" in last_user["content"].lower():
            return state.copy(update={
                "iteration_count": count,
                "next_node": "FINISH"
            })

        # Prepare messages
        messages = [{"role": "system", "content": system_prompt}] + state.messages

        supervisor_chain = RunnableLambda(lambda _: messages) | llm.with_structured_output(NextNode)
        result = supervisor_chain.invoke(state)
        next_node = result.next_node

        # Prevent agent loop
        if state.next_node == next_node:
            next_node = "FINISH"

        # debugging
         
        print(f"DEBUG: Next node selected: {next_node}")

        return state.copy(update={
            "iteration_count": count,
            "next_node": next_node
        })

    return supervisor_agent_node, system_prompt
