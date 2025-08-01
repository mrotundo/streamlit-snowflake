#!/usr/bin/env python3
"""Final demo test for all banking agents"""

import os
import sys
from config.settings import Settings
from services.openai_service import OpenAIService
from agents.agent_registry import agent_registry

def test_agent(agent_name, query):
    """Test a specific agent with a query"""
    print(f"\n{'='*70}")
    print(f"Agent: {agent_name}")
    print(f"Query: {query}")
    print('='*70)
    
    # Get agent
    agent = agent_registry.create_agent(agent_name)
    if not agent:
        print(f"ERROR: Could not create {agent_name}")
        return
    
    # Initialize LLM service
    settings = Settings()
    if not settings.is_openai_configured():
        print("ERROR: OpenAI API key not configured")
        return
    
    llm_service = OpenAIService(api_key=settings.OPENAI_API_KEY)
    model = settings.OPENAI_MODEL or "gpt-4o-mini"
    
    try:
        # Process query
        response = agent.process(
            query=query,
            llm_service=llm_service,
            model=model
        )
        
        # Print response
        print("\nResponse:")
        print(response.get('response', 'No response'))
        
        # Show basic metrics if available
        if 'data' in response and isinstance(response['data'], dict):
            if 'summary' in response['data']:
                print("\nData Summary:")
                for key, value in list(response['data']['summary'].items())[:3]:
                    print(f"  {key}: {value}")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")

def main():
    """Run demo tests for all agents"""
    print("Banking AI Agents - Final Demo")
    print("="*70)
    
    test_cases = [
        ("CustomerAnalyticsAgent", "Show me customer segmentation breakdown"),
        ("LoanPortfolioAgent", "What's our total loan portfolio value?"),
        ("DepositAnalyticsAgent", "What's our total deposit balance?"),
        ("TransactionInsightsAgent", "Show transaction volume trends")
    ]
    
    for agent_name, query in test_cases:
        test_agent(agent_name, query)
    
    print(f"\n{'='*70}")
    print("Demo Complete!")

if __name__ == "__main__":
    main()