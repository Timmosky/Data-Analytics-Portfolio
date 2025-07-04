import streamlit as st
from langchain_community.tools import WikipediaQueryRun
from langchain_community.utilities import WikipediaAPIWrapper
from langchain.tools import Tool
from langchain.tools.tavily_search import TavilySearchResults
from datetime import datetime
import os

if "tavily" not in st.secrets or "api_key" not in st.secrets["tavily"]:
    raise ValueError("Tavily API key not found in Streamlit secrets.")
tavily_api_key = st.secrets["tavily"]["api_key"]


os.environ["TAVILY_API_KEY"] = st.secrets["tavily"]["api_key"]

tavily_tool = TavilySearchResults()


api_wrapper = WikipediaAPIWrapper(top_k_results=1, doc_content_chars_max=100)
wiki_tool = WikipediaQueryRun(api_wrapper=api_wrapper)

def save_to_txt(data: str, filename: str = "research_output.txt"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_text = f"--- Research Output ---\nTimestamp: {timestamp}\n\n{data}\n\n"

    with open(filename, "a", encoding="utf-8") as f:
        f.write(formatted_text)
    
    return f"Data successfully saved to {filename}"

save_tool = Tool(
    name="save_text_to_file",
    func=save_to_txt,
    description="Saves structured research data to a text file.",
)

