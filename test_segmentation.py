#!/usr/bin/env python3
"""Test customer segmentation query"""

import os
import sys
import json
from config.settings import Settings
from services.openai_service import OpenAIService
from agents.agent_registry import agent_registry

def test_segmentation():
    """Test customer segmentation query"""
    print("Testing Customer Segmentation Query")
    print("="*60)
    
    # Get agent
    agent = agent_registry.create_agent("CustomerAnalyticsAgent")
    if not agent:
        print("ERROR: Could not create CustomerAnalyticsAgent")
        return
    
    # Initialize LLM service
    settings = Settings()
    if not settings.is_openai_configured():
        print("ERROR: OpenAI API key not configured")
        return
    
    llm_service = OpenAIService(api_key=settings.OPENAI_API_KEY)
    model = settings.OPENAI_MODEL or "gpt-4o-mini"
    
    query = "Show me our customer segmentation breakdown with average relationship value by segment"
    
    print(f"\nQuery: {query}")
    print("-" * 40)
    
    try:
        # Process query
        response = agent.process(
            query=query,
            llm_service=llm_service,
            model=model
        )
        
        # Print response
        print(f"\n\nResponse:")
        print(response.get('response', 'No response'))
        
        # Check if we got proper analysis
        if response.get('response', '').startswith("I've retrieved the following") or \
           response.get('response', '').startswith("I encountered an issue"):
            print("\n\nDEBUG - Execution Results:")
            exec_results = response.get('execution_results', {})
            for step in exec_results.get('steps_executed', []):
                print(f"\nStep {step['step']}: {step['tool']}")
                print(f"Success: {step['success']}")
                if step.get('error'):
                    print(f"Error: {step['error']}")
                if step.get('output') and isinstance(step['output'], dict):
                    if 'analysis' in step['output']:
                        print("Has analysis key!")
                    elif 'answer' in step['output']:
                        print("Has answer key!")
                    print(f"Output keys: {list(step['output'].keys())[:5]}")
        
    except Exception as e:
        print(f"\nEXCEPTION: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_segmentation()