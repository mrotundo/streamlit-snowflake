#!/usr/bin/env python3
"""Test script for a single agent to debug the issue"""

import os
import sys
import json
from config.settings import Settings
from services.openai_service import OpenAIService
from agents.agent_registry import agent_registry

def test_loan_agent():
    """Test the loan agent specifically"""
    print("Testing LoanPortfolioAgent")
    print("="*60)
    
    # Get agent
    agent = agent_registry.create_agent("LoanPortfolioAgent")
    if not agent:
        print("ERROR: Could not create LoanPortfolioAgent")
        return
    
    # Initialize LLM service
    settings = Settings()
    if not settings.is_openai_configured():
        print("ERROR: OpenAI API key not configured")
        return
    
    llm_service = OpenAIService(api_key=settings.OPENAI_API_KEY)
    model = settings.OPENAI_MODEL or "gpt-4o-mini"
    
    query = "How are my loans performing?"
    
    # First, create the plan
    print(f"\nQuery: {query}")
    print("-" * 40)
    print("\nCreating plan...")
    
    plan = agent.create_plan(query, llm_service, model)
    print("\nGenerated Plan:")
    print(json.dumps(plan, indent=2))
    
    # Now process the query
    print("\nProcessing query...")
    
    try:
        response = agent.process(
            query=query,
            llm_service=llm_service,
            model=model
        )
        
        # Print results
        if "error" in response:
            print(f"\nERROR: {response['error']}")
            if "error_traceback" in response:
                print(f"Traceback: {response['error_traceback']}")
        else:
            print(f"\nResponse: {response.get('response', 'No response text')}")
            
    except Exception as e:
        print(f"\nEXCEPTION: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_loan_agent()