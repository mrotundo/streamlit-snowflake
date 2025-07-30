import streamlit as st
from typing import Dict, List, Optional
from models import ChatSession
import json
from datetime import datetime


class SessionManager:
    """Manages Streamlit session state for chat sessions"""
    
    def __init__(self):
        self._initialize_state()
    
    def _initialize_state(self):
        """Initialize session state variables"""
        if 'chat_sessions' not in st.session_state:
            st.session_state.chat_sessions = {}
        
        if 'current_session_id' not in st.session_state:
            st.session_state.current_session_id = None
        
        if 'llm_provider' not in st.session_state:
            st.session_state.llm_provider = None
        
        if 'llm_model' not in st.session_state:
            st.session_state.llm_model = None
    
    # Chat Session Management
    def create_session(self, name: str = "New Session") -> ChatSession:
        """Create a new chat session"""
        session = ChatSession(name=name)
        st.session_state.chat_sessions[session.id] = session
        return session
    
    def get_session(self, session_id: str) -> Optional[ChatSession]:
        """Get chat session by ID"""
        return st.session_state.chat_sessions.get(session_id)
    
    def get_all_sessions(self) -> List[ChatSession]:
        """Get all chat sessions sorted by updated time (newest first)"""
        sessions = list(st.session_state.chat_sessions.values())
        return sorted(sessions, key=lambda s: s.updated_at, reverse=True)
    
    def delete_session(self, session_id: str):
        """Delete a chat session"""
        if session_id in st.session_state.chat_sessions:
            del st.session_state.chat_sessions[session_id]
            
            # Reset current session if it was deleted
            if st.session_state.current_session_id == session_id:
                st.session_state.current_session_id = None
    
    def set_current_session(self, session_id: str):
        """Set the current active chat session"""
        if session_id in st.session_state.chat_sessions:
            st.session_state.current_session_id = session_id
    
    def get_current_session(self) -> Optional[ChatSession]:
        """Get the current active chat session"""
        if st.session_state.current_session_id:
            return self.get_session(st.session_state.current_session_id)
        return None
    
    def rename_session(self, session_id: str, new_name: str):
        """Rename a chat session"""
        session = self.get_session(session_id)
        if session:
            session.name = new_name
            session.auto_renamed = True
            session.updated_at = datetime.now()
    
    # LLM Provider Management
    def set_llm_provider(self, provider: str):
        """Set the current LLM provider"""
        st.session_state.llm_provider = provider
    
    def get_llm_provider(self) -> Optional[str]:
        """Get the current LLM provider"""
        return st.session_state.llm_provider
    
    def set_llm_model(self, model: str):
        """Set the current LLM model"""
        st.session_state.llm_model = model
    
    def get_llm_model(self) -> Optional[str]:
        """Get the current LLM model"""
        return st.session_state.llm_model
    
    # Message Management
    def add_message_to_current_session(
        self,
        role: str,
        content: str,
        model_used: Optional[str] = None,
        provider: Optional[str] = None
    ):
        """Add a message to the current session"""
        session = self.get_current_session()
        if session:
            session.add_message(role, content, model_used, provider)
    
    def get_current_messages(self) -> List[Dict[str, str]]:
        """Get messages from current session formatted for API"""
        session = self.get_current_session()
        if session:
            return session.get_messages_for_api()
        return []
    
    def clear_current_session_messages(self):
        """Clear messages from current session"""
        session = self.get_current_session()
        if session:
            session.clear_messages()
    
    def should_auto_rename_session(self, session_id: str) -> bool:
        """Check if a session should be auto-renamed (has messages but not renamed yet)"""
        session = self.get_session(session_id)
        if session:
            return len(session.messages) > 0 and not session.auto_renamed and session.name == "New Session"
        return False