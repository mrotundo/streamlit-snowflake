import requests
import json
from typing import List, Dict, Optional
from .llm_interface import LLMInterface


class OpenAIService(LLMInterface):
    """OpenAI API implementation using REST"""
    
    AVAILABLE_MODELS = [
        'gpt-3.5-turbo',
        'gpt-4o-mini',
        'gpt-4o',
        'gpt-4-turbo',
        'gpt-4'
    ]
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.openai.com/v1"
    
    def complete(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None
    ) -> str:
        """Generate completion using OpenAI Chat Completions API"""
        if model is None:
            model = 'gpt-3.5-turbo'
        
        if model not in self.AVAILABLE_MODELS:
            raise ValueError(f"Model {model} not available. Choose from: {self.AVAILABLE_MODELS}")
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'model': model,
            'messages': messages,
            'temperature': temperature
        }
        
        if max_tokens:
            payload['max_tokens'] = max_tokens
        
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code != 200:
                error_data = response.json()
                raise Exception(f"OpenAI API error: {error_data.get('error', {}).get('message', 'Unknown error')}")
            
            data = response.json()
            return data['choices'][0]['message']['content']
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"OpenAI API request error: {str(e)}")
        except KeyError as e:
            raise Exception(f"Unexpected OpenAI response format: {str(e)}")
    
    def get_available_models(self) -> List[str]:
        """Get list of available OpenAI models"""
        return self.AVAILABLE_MODELS.copy()
    
    def validate_connection(self) -> bool:
        """Test OpenAI API connection"""
        headers = {
            'Authorization': f'Bearer {self.api_key}'
        }
        
        try:
            response = requests.get(
                f"{self.base_url}/models",
                headers=headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception:
            return False