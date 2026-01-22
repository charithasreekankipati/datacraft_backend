# analyze_schema_agent.py

from typing import Dict
from langchain_core.prompts import PromptTemplate
from langchain_core.prompts import ChatPromptTemplate
from backend.llm_provider import llm

def analyze_uploaded_dataframes_and_suggest_kpis(dataframes: Dict[str, object]) -> str:
    output = []
    for name, df in dataframes.items():
        output.append(f"Table: {name}")
        output.append(f"Columns: {', '.join(df.columns)}")
        output.append("")

    prompt = "\n".join(output)
    instructions = (
        "You are a data analyst AI. Based on the schema of the tables below, "
        "infer the business domain (e.g., Healthcare, Finance, Retail) and suggest focus areas.\n"
        "Then list areas of interest relevant to the domain for KPI suggestions.\n"
        "Output format:\n"
        "Domain: <DomainName>\n"
        "Areas: <Area1>, <Area2>, <Area3>, ..."
    )

    chat_prompt = ChatPromptTemplate.from_messages([
        ("system", instructions),
        ("human", prompt)
    ])
    
    chain = chat_prompt | llm
    return chain.invoke({}).content

def suggest_kpis_for_area(dataframes: Dict[str, object], domain: str, area: str) -> str:
    ddl = []
    for name, df in dataframes.items():
        ddl.append(f"Table: {name}")
        ddl.append(f"Columns: {', '.join(df.columns)}")
        ddl.append("")

    schema_text = "\n".join(ddl)

    kpi_prompt = PromptTemplate.from_template("""
You are a Power BI expert analyst.

The following schema describes a set of tables and columns from a {domain} business database.
The business user is interested in tracking KPIs for the focus area: **{area}**

Schema:
{ddl}

Your task:
- Infer relevant relationships across tables
- Generate only 10 high-impact KPIs for the selected area
- Output DAX-formatted measures like: `MeasureName = DAX_Formula`

Rules:
- No Excel functions
- No explanations, markdown, or comments
- Output: 10 DAX formulas, one per line
""")

    chain = kpi_prompt | llm
    return chain.invoke({"domain": domain, "area": area, "ddl": schema_text}).content

def run_schema_analysis_agent(dataframes: Dict[str, object]) -> str:
    return analyze_uploaded_dataframes_and_suggest_kpis(dataframes)

def run_kpi_generation_agent(dataframes: Dict[str, object], domain: str, area: str) -> str:
    return suggest_kpis_for_area(dataframes, domain, area)
