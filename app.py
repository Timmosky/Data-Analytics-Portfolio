import streamlit as st
from tools import search_tool, wiki_tool, save_tool
from langchain.agents import AgentExecutor  # make sure you import this
from main import create_tool_calling_agent, get_agent, parser, llm, prompt  # adjust these as needed
import os
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Setup agent
tools = [search_tool, wiki_tool, save_tool]
agent = get_agent(tools)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# Title
st.set_page_config(page_title="Agentic AI", layout="centered")
st.title("🧠 Timilehin's Agentic AI")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "last_question" not in st.session_state:
    st.session_state.last_question = ""
if "clear_input" not in st.session_state:
    st.session_state.clear_input = False

# Clear the input if flagged to do so
if st.session_state.clear_input:
    st.session_state.user_input = ""
    st.session_state.clear_input = False
# Show chat history
for chat in st.session_state.chat_history:
    st.markdown(f"**You:** {chat['user']}")
    st.markdown(f"**Agent:** {chat['agent']}")

# Input box
query = st.text_input("Ask your question:", key = "useer_input")

# Handle submission
if query and st.session_state.get("last_question") != query:
    with st.spinner("Thinking..."):
        try:
            # 🔁 Run agent
            raw_response = agent_executor.invoke({"query": query})
            structured_response = parser.parse(raw_response.get("output")[0]["text"])

            # 💾 Save Q&A
            st.session_state.chat_history.append({
                "user": query,
                "agent": structured_response
            })

            # 💡 Track last question to avoid re-running
            st.session_state.last_question = query

            # Reset the input box (this is key!)
            st.session_state.clear_input = True

            st.rerun()

        except Exception as e:
            st.error(f"Error: {e}")

if st.button("🗑️ Clear Chat"):
    st.session_state.chat_history = []
    st.rerun()
