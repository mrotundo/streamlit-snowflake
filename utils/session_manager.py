import streamlit as st
from typing import Dict, List, Optional
from models import Workspace, ChatSession
import json
from datetime import datetime


class SessionManager:
    """Manages Streamlit session state for workspaces and chat sessions"""
    
    def __init__(self):
        self._initialize_state()
    
    def _initialize_state(self):
        """Initialize session state variables"""
        if 'workspaces' not in st.session_state:
            st.session_state.workspaces = {}
        
        if 'chat_sessions' not in st.session_state:
            st.session_state.chat_sessions = {}
        
        if 'current_workspace_id' not in st.session_state:
            st.session_state.current_workspace_id = None
        
        if 'current_session_id' not in st.session_state:
            st.session_state.current_session_id = None
        
        if 'llm_provider' not in st.session_state:
            st.session_state.llm_provider = None
        
        if 'llm_model' not in st.session_state:
            st.session_state.llm_model = None
    
    # Workspace Management
    def create_workspace(self, name: str, description: Optional[str] = None) -> Workspace:
        """Create a new workspace"""
        workspace = Workspace(name=name, description=description)
        st.session_state.workspaces[workspace.id] = workspace
        return workspace
    
    def get_workspace(self, workspace_id: str) -> Optional[Workspace]:
        """Get workspace by ID"""
        return st.session_state.workspaces.get(workspace_id)
    
    def get_all_workspaces(self) -> List[Workspace]:
        """Get all workspaces"""
        return list(st.session_state.workspaces.values())
    
    def delete_workspace(self, workspace_id: str):
        """Delete a workspace and all its sessions"""
        workspace = self.get_workspace(workspace_id)
        if workspace:
            # Delete all sessions in this workspace
            for session_id in workspace.chat_session_ids:
                self.delete_session(session_id)
            
            # Delete the workspace
            del st.session_state.workspaces[workspace_id]
            
            # Reset current workspace if it was deleted
            if st.session_state.current_workspace_id == workspace_id:
                st.session_state.current_workspace_id = None
    
    def set_current_workspace(self, workspace_id: str):
        """Set the current active workspace"""
        if workspace_id in st.session_state.workspaces:
            st.session_state.current_workspace_id = workspace_id
            # Reset current session when switching workspaces
            st.session_state.current_session_id = None
    
    def get_current_workspace(self) -> Optional[Workspace]:
        """Get the current active workspace"""
        if st.session_state.current_workspace_id:
            return self.get_workspace(st.session_state.current_workspace_id)
        return None
    
    # Chat Session Management
    def create_session(self, name: str, workspace_id: str) -> ChatSession:
        """Create a new chat session in a workspace"""
        session = ChatSession(name=name, workspace_id=workspace_id)
        st.session_state.chat_sessions[session.id] = session
        
        # Add session to workspace
        workspace = self.get_workspace(workspace_id)
        if workspace:
            workspace.add_session(session.id)
        
        return session
    
    def get_session(self, session_id: str) -> Optional[ChatSession]:
        """Get chat session by ID"""
        return st.session_state.chat_sessions.get(session_id)
    
    def get_workspace_sessions(self, workspace_id: str) -> List[ChatSession]:
        """Get all sessions in a workspace"""
        workspace = self.get_workspace(workspace_id)
        if workspace:
            return [
                self.get_session(session_id)
                for session_id in workspace.chat_session_ids
                if self.get_session(session_id)
            ]
        return []
    
    def delete_session(self, session_id: str):
        """Delete a chat session"""
        session = self.get_session(session_id)
        if session:
            # Remove from workspace
            workspace = self.get_workspace(session.workspace_id)
            if workspace:
                workspace.remove_session(session_id)
            
            # Delete the session
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