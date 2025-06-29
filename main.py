from dotenv import load_dotenv
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain.agents import create_tool_calling_agent
from langchain.agents import AgentExecutor
from tools import search_tool, wiki_tool, save_tool
import streamlit as st
import os


load_dotenv()
api_key = st.secrets.get("ANTHROPIC_API_KEY")
tavily_api_key = st.secrets.get("tavily", {}).get("api_key")
search_tool = TavilySearchResults(api_key=tavily_api_key)
class ResearchResponse(BaseModel):
    summary:str
    tools_used: list[str]

llm = ChatAnthropic(model = "claude-3-5-sonnet-20241022")
parser = PydanticOutputParser(pydantic_object=ResearchResponse)
prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """
            You are a research assistant that will help generate a research paper.
            Answer the user query and use neccessary tools. 
            Wrap the output in this format and provide no other text\n{format_instructions}
            """,
        ),
        ("placeholder", "{chat_history}"),
        ("human", "{query}"),
        ("placeholder", "{agent_scratchpad}"),
    ]
).partial(format_instructions=parser.get_format_instructions())
tools = [search_tool, wiki_tool, save_tool]


def get_agent(tools):
    return create_tool_calling_agent(llm=llm, prompt=prompt, tools=tools)
