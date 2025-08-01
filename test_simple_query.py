#!/usr/bin/env python3
"""Simple test to debug a specific query"""

import os
import sys
import json
from config.settings import Settings
from services.openai_service import OpenAIService
from agents.agent_registry import agent_registry

def test_simple_deposit_query():
    """Test a simple deposit query"""
    print("Testing Simple Deposit Query")
    print("="*60)
    
    # Get agent
    agent = agent_registry.create_agent("DepositAnalyticsAgent")
    if not agent:
        print("ERROR: Could not create DepositAnalyticsAgent")
        return
    
    # Initialize LLM service
    settings = Settings()
    if not settings.is_openai_configured():
        print("ERROR: OpenAI API key not configured")
        return
    
    llm_service = OpenAIService(api_key=settings.OPENAI_API_KEY)
    model = settings.OPENAI_MODEL or "gpt-4o-mini"
    
    query = "What's our total deposit balance?"
    
    print(f"\nQuery: {query}")
    print("-" * 40)
    
    # Create plan first
    plan = agent.create_plan(query, llm_service, model)
    print("\nPlan:")
    print(json.dumps(plan, indent=2))
    
    try:
        # Process query
        response = agent.process(
            query=query,
            llm_service=llm_service,
            model=model
        )
        
        # Print response
        print(f"\n\nResponse: {response.get('response', 'No response')}")
        
        # Check execution results
        if "execution_results" in response:
            exec_results = response["execution_results"]
            final_output = exec_results.get("final_output", {})
            
            print("\n\nFinal Output Type:", type(final_output))
            if final_output:
                print("Final Output Keys:", list(final_output.keys()) if isinstance(final_output, dict) else "Not a dict")
                
            # Print last step details
            steps = exec_results.get("steps_executed", [])
            if steps:
                last_step = steps[-1]
                print(f"\n\nLast Step: {last_step['tool']}")
                print(f"Success: {last_step['success']}")
                if last_step.get('error'):
                    print(f"Error: {last_step['error']}")
                if last_step.get('output'):
                    print(f"Output type: {type(last_step['output'])}")
                    if isinstance(last_step['output'], dict):
                        print(f"Output keys: {list(last_step['output'].keys())}")
        
    except Exception as e:
        print(f"\nEXCEPTION: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_deposit_query()