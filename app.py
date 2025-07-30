import streamlit as st
from typing import Optional
from config.settings import Settings
from services.snowflake_service import SnowflakeService
from services.openai_service import OpenAIService
from services.llm_interface import LLMInterface
from utils.session_manager import SessionManager
from models import Workspace, ChatSession


def get_llm_service(provider: str) -> Optional[LLMInterface]:
    """Factory function to get LLM service instance"""
    try:
        if provider == 'snowflake' and Settings.is_snowflake_configured():
            return SnowflakeService(
                account=Settings.SNOWFLAKE_ACCOUNT,
                user=Settings.SNOWFLAKE_USER,
                password=Settings.SNOWFLAKE_PASSWORD,
                warehouse=Settings.SNOWFLAKE_WAREHOUSE,
                database=Settings.SNOWFLAKE_DATABASE,
                schema=Settings.SNOWFLAKE_SCHEMA
            )
        elif provider == 'openai' and Settings.is_openai_configured():
            return OpenAIService(api_key=Settings.OPENAI_API_KEY)
    except ImportError as e:
        st.error(f"Failed to initialize {provider} service: {str(e)}")
    return None


def main():
    st.set_page_config(
        page_title="AI Chat Assistant",
        page_icon="💬",
        layout="wide"
    )
    
    # Initialize session manager
    session_manager = SessionManager()
    
    # Check if any providers are configured
    available_providers = Settings.get_available_providers()
    if not available_providers:
        st.error("No LLM providers configured. Please set up either Snowflake or OpenAI credentials in your .env file.")
        st.stop()
    
    # Sidebar for workspace and session management
    with st.sidebar:
        st.title("🗂️ Workspaces")
        
        # Provider selection
        st.subheader("LLM Provider")
        provider = st.selectbox(
            "Select Provider",
            options=available_providers,
            index=available_providers.index(Settings.DEFAULT_PROVIDER) if Settings.DEFAULT_PROVIDER in available_providers else 0
        )
        session_manager.set_llm_provider(provider)
        
        # Model selection based on provider
        llm_service = get_llm_service(provider)
        if llm_service:
            models = llm_service.get_available_models()
            default_model = Settings.SNOWFLAKE_MODEL if provider == 'snowflake' else Settings.OPENAI_MODEL
            model = st.selectbox(
                "Select Model",
                options=models,
                index=models.index(default_model) if default_model in models else 0
            )
            session_manager.set_llm_model(model)
        
        st.divider()
        
        # Workspace management
        workspaces = session_manager.get_all_workspaces()
        current_workspace = session_manager.get_current_workspace()
        
        # Create new workspace
        with st.expander("➕ Create Workspace"):
            workspace_name = st.text_input("Workspace Name", key="new_workspace_name")
            workspace_desc = st.text_area("Description (optional)", key="new_workspace_desc")
            if st.button("Create", key="create_workspace"):
                if workspace_name:
                    workspace = session_manager.create_workspace(workspace_name, workspace_desc)
                    session_manager.set_current_workspace(workspace.id)
                    st.rerun()
        
        # List workspaces
        if workspaces:
            st.subheader("Your Workspaces")
            for workspace in workspaces:
                col1, col2 = st.columns([3, 1])
                with col1:
                    if st.button(
                        f"📁 {workspace.name}",
                        key=f"workspace_{workspace.id}",
                        use_container_width=True,
                        type="primary" if current_workspace and current_workspace.id == workspace.id else "secondary"
                    ):
                        session_manager.set_current_workspace(workspace.id)
                        st.rerun()
                with col2:
                    if st.button("🗑️", key=f"delete_workspace_{workspace.id}"):
                        session_manager.delete_workspace(workspace.id)
                        st.rerun()
        
        # Chat sessions for current workspace
        if current_workspace:
            st.divider()
            st.subheader(f"💬 Sessions in {current_workspace.name}")
            
            # Create new session
            with st.expander("➕ Create Session"):
                session_name = st.text_input("Session Name", key="new_session_name")
                if st.button("Create", key="create_session"):
                    if session_name:
                        session = session_manager.create_session(session_name, current_workspace.id)
                        session_manager.set_current_session(session.id)
                        st.rerun()
            
            # List sessions
            sessions = session_manager.get_workspace_sessions(current_workspace.id)
            current_session = session_manager.get_current_session()
            
            for session in sessions:
                col1, col2 = st.columns([3, 1])
                with col1:
                    if st.button(
                        f"💭 {session.name}",
                        key=f"session_{session.id}",
                        use_container_width=True,
                        type="primary" if current_session and current_session.id == session.id else "secondary"
                    ):
                        session_manager.set_current_session(session.id)
                        st.rerun()
                with col2:
                    if st.button("🗑️", key=f"delete_session_{session.id}"):
                        session_manager.delete_session(session.id)
                        st.rerun()
    
    # Main chat interface
    st.title("🤖 AI Chat Assistant")
    
    if not current_workspace:
        st.info("👈 Please create or select a workspace to start chatting")
        return
    
    if not session_manager.get_current_session():
        st.info("👈 Please create or select a chat session to start")
        return
    
    current_session = session_manager.get_current_session()
    
    # Display chat title
    st.subheader(f"Chat: {current_session.name}")
    
    # Clear chat button
    if st.button("🗑️ Clear Chat", key="clear_chat"):
        session_manager.clear_current_session_messages()
        st.rerun()
    
    # Chat messages container
    messages_container = st.container()
    
    # Display chat history
    with messages_container:
        for message in current_session.messages:
            with st.chat_message(message.role):
                st.write(message.content)
                if message.model_used:
                    st.caption(f"Model: {message.model_used} ({message.provider})")
    
    # Chat input
    if prompt := st.chat_input("Type your message here..."):
        # Add user message
        session_manager.add_message_to_current_session("user", prompt)
        
        # Display user message
        with messages_container:
            with st.chat_message("user"):
                st.write(prompt)
        
        # Get LLM response
        try:
            llm_service = get_llm_service(session_manager.get_llm_provider())
            if llm_service:
                # Show spinner while generating response
                with st.spinner("Thinking..."):
                    messages = session_manager.get_current_messages()
                    response = llm_service.complete(
                        messages=messages,
                        model=session_manager.get_llm_model()
                    )
                
                # Add assistant message
                session_manager.add_message_to_current_session(
                    "assistant",
                    response,
                    model_used=session_manager.get_llm_model(),
                    provider=session_manager.get_llm_provider()
                )
                
                # Display assistant message
                with messages_container:
                    with st.chat_message("assistant"):
                        st.write(response)
                        st.caption(f"Model: {session_manager.get_llm_model()} ({session_manager.get_llm_provider()})")
            else:
                st.error("Failed to initialize LLM service")
        except Exception as e:
            st.error(f"Error: {str(e)}")
        
        # Rerun to update the chat display
        st.rerun()


if __name__ == "__main__":
    main()