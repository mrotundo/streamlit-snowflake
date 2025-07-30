from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import uuid


class Workspace(BaseModel):
    """Workspace model for organizing chat sessions"""
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    chat_session_ids: List[str] = Field(default_factory=list)
    
    def add_session(self, session_id: str):
        """Add a chat session to this workspace"""
        if session_id not in self.chat_session_ids:
            self.chat_session_ids.append(session_id)
            self.updated_at = datetime.now()
    
    def remove_session(self, session_id: str):
        """Remove a chat session from this workspace"""
        if session_id in self.chat_session_ids:
            self.chat_session_ids.remove(session_id)
            self.updated_at = datetime.now()
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }