import streamlit as st
from config import init_database
from prompts import get_response
from langchain_core.messages import AIMessage, HumanMessage

def setup_sidebar():
    with st.sidebar:
        st.subheader("Settings")
        st.write("This is a simple chat application using Database. Connect to the database and start chatting.")
        
        st.text_input("Host", key="Host")
        st.text_input("Port", key="Port")
        st.text_input("User", key="User")
        st.text_input("Password", type="password", key="Password")
        st.text_input("Database", key="Database")
        
        if st.button("Connect"):
            with st.spinner("Connecting to database..."):
                db = init_database(
                    st.session_state["User"],
                    st.session_state["Password"],
                    st.session_state["Host"],
                    st.session_state["Port"],
                    st.session_state["Database"]
                )
                st.session_state.db = db
                st.success("Connected to database!")

def display_chat():
    for message in st.session_state.chat_history:
        if isinstance(message, AIMessage):
            with st.chat_message("AI"):
                st.markdown(message.content)
        elif isinstance(message, HumanMessage):
            with st.chat_message("Human"):
                st.markdown(message.content)

def handle_user_input():
    user_query = st.chat_input("Type a message...")
    if user_query and user_query.strip() != "":
        st.session_state.chat_history.append(HumanMessage(content=user_query))
        
        with st.chat_message("Human"):
            st.markdown(user_query)
        
        with st.chat_message("AI"):
            response = get_response(user_query, st.session_state.db, st.session_state.chat_history)
            st.markdown(response)
        
        st.session_state.chat_history.append(AIMessage(content=response))

def main():
    st.set_page_config(page_title="Chat with Database", page_icon=":speech_balloon:")
    st.title("Chat with Database")
    
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = [
            AIMessage(content="Hello! I'm a SQL assistant. Ask me anything about your database."),
        ]
    
    
    setup_sidebar()
    display_chat()
    handle_user_input()

    print(st.session_state.chat_history)

if __name__ == "__main__":
    main()
