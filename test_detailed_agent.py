#!/usr/bin/env python3
"""Detailed test script to debug agent responses"""

import os
import sys
import json
from config.settings import Settings
from services.openai_service import OpenAIService
from agents.agent_registry import agent_registry

def test_customer_agent_detailed():
    """Test the customer agent with detailed output"""
    print("Testing CustomerAnalyticsAgent in Detail")
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
    
    query = "Show me customer segmentation breakdown"
    
    print(f"\nQuery: {query}")
    print("-" * 40)
    
    try:
        # Process query
        response = agent.process(
            query=query,
            llm_service=llm_service,
            model=model
        )
        
        print("\nFull Response Structure:")
        print(json.dumps(response, indent=2))
        
        # Check execution results
        if "execution_results" in response:
            exec_results = response["execution_results"]
            print("\n\nExecution Results:")
            print(f"- Success: {exec_results.get('success', False)}")
            print(f"- Steps executed: {len(exec_results.get('steps_executed', []))}")
            
            # Check final output
            final_output = exec_results.get("final_output", {})
            print("\nFinal Output:")
            print(json.dumps(final_output, indent=2))
            
            # Check each step
            print("\n\nStep Details:")
            for step in exec_results.get("steps_executed", []):
                print(f"\nStep {step['step']}: {step['tool']}")
                print(f"Success: {step['success']}")
                if step.get("output"):
                    print(f"Output keys: {list(step['output'].keys()) if isinstance(step['output'], dict) else 'Not a dict'}")
                    if isinstance(step['output'], dict) and 'analysis' in step['output']:
                        print("Analysis found!")
                        print(json.dumps(step['output']['analysis'], indent=2))
        
    except Exception as e:
        print(f"\nEXCEPTION: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_customer_agent_detailed()