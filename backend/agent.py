from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from backend.agents.agent_state import AgentState

from backend.agents.supervisor_agent import get_supervisor_agent
from backend.agents.qna_agent import get_qna_agent
from backend.datalake_design.rag_mapper_agent import get_rag_mapper_agent
from backend.utils.helpers import strip_think_parser
from backend.llm_provider import llm
from backend.agents.system_assessment_agent import get_system_assessment_agent


# Active agents only
qna_node, qna_desc = get_qna_agent()
rag_node, rag_desc = get_rag_mapper_agent("fhir")
system_assessment_node, system_assessment_description = get_system_assessment_agent()

worker_descriptions = {
    "QNA": qna_desc,
    "RagMapper": rag_desc,
    "SystemAssessment": system_assessment_description
}

supervisor_node, _ = get_supervisor_agent(worker_descriptions)

workflow = StateGraph(AgentState)

workflow.add_node("QNA", qna_node)
workflow.add_node("RagMapper", rag_node)
workflow.add_node("supervisor", supervisor_node)
workflow.add_node("SystemAssessment", system_assessment_node)

def final_answer_node(state: AgentState) -> AgentState:
    last = next((m for m in reversed(state.messages) if m["role"] == "assistant"), None)
    content = last["content"] if last else "No response generated."
    return state.copy(update={
        "messages": state.messages + [{
            "role": "assistant",
            "content": strip_think_parser(content),
            "name": "final_answer"
        }]
    })

workflow.add_node("final_answer", final_answer_node)

workflow.set_entry_point("supervisor")

for node in worker_descriptions:
    workflow.add_edge(node, "supervisor")

workflow.add_conditional_edges(
    "supervisor",
    lambda x: x.next_node,
    {**{k: k for k in worker_descriptions}, "FINISH": "final_answer"}
)

workflow.add_edge("final_answer", END)

multi_agent = workflow.compile()

def get_multi_agent() -> CompiledStateGraph:
    return multi_agent
