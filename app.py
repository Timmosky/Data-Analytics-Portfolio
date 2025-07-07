import streamlit as st
from tools import tavily_tool, wiki_tool, save_tool
from langchain.agents import AgentExecutor 
from main import create_tool_calling_agent, get_agent, parser, llm, prompt 
import os
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

tools = [tavily_tool, wiki_tool, save_tool]
agent = get_agent(tools)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

st.write("Anthropic Key:", repr(st.secrets.get("anthropic_api_key", "Missing")))
st.write("Tavily Key:", repr(st.secrets.get("tavily", {}).get("api_key", "Missing")))

st.set_page_config(page_title="Agentic AI", layout="centered")
st.title("🧠 Timilehin's AI Agent")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "last_question" not in st.session_state:
    st.session_state.last_question = ""
if "clear_input" not in st.session_state:
    st.session_state.clear_input = False

if st.session_state.clear_input:
    st.session_state.user_input = ""
    st.session_state.clear_input = False

for chat in st.session_state.chat_history:
    st.markdown(f"**You:** {chat['user']}")
    st.markdown(f"**Agent:**\n\n{chat['agent'].summary}")
 

query = st.text_input("Ask your question:", key = "useer_input")

if query and st.session_state.get("last_question") != query:
    with st.spinner("Thinking..."):
        try:
            raw_response = agent_executor.invoke({"query": query})
            structured_response = parser.parse(raw_response.get("output")[0]["text"])
            structured_response.summary = structured_response.summary.replace("\\n", "\n")

            st.session_state.chat_history.append({
                "user": query,
                "agent": structured_response
            })

            st.session_state.last_question = query

            # Reset the input box (this is key!)
            st.session_state.clear_input = True

            st.rerun()

        except Exception as e:
            st.error(f"Error: {e}")

if st.button("🗑️ Clear Chat"):
    st.session_state.chat_history = []
    st.rerun()
