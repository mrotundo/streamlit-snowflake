from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from datetime import datetime
import uuid


class ChatMessage(BaseModel):
    """Individual chat message"""
    role: str  # 'user' or 'assistant'
    content: str
    timestamp: datetime = Field(default_factory=datetime.now)
    model_used: Optional[str] = None
    provider: Optional[str] = None
    metadata: Optional[str] = None  # JSON string for agent responses


class ChatSession(BaseModel):
    """Chat session model containing conversation history"""
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    messages: List[ChatMessage] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    auto_renamed: bool = False
    
    def add_message(
        self,
        role: str,
        content: str,
        model_used: Optional[str] = None,
        provider: Optional[str] = None
    ):
        """Add a message to the chat session"""
        message = ChatMessage(
            role=role,
            content=content,
            model_used=model_used,
            provider=provider
        )
        self.messages.append(message)
        self.updated_at = datetime.now()
    
    def get_messages_for_api(self) -> List[Dict[str, str]]:
        """Get messages formatted for LLM API calls"""
        return [
            {"role": msg.role, "content": msg.content}
            for msg in self.messages
        ]
    
    def clear_messages(self):
        """Clear all messages from the session"""
        self.messages = []
        self.updated_at = datetime.now()
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }