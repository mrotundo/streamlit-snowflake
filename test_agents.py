#!/usr/bin/env python3
"""Test script for the new banking agents"""

import os
import sys
from config.settings import Settings
from services.openai_service import OpenAIService
from agents.agent_registry import agent_registry

# Test queries for each agent
test_queries = {
    "LoanPortfolioAgent": [
        "How are my loans performing?",
        "What's the default rate for our mortgage portfolio?",
        "Show me loan performance this quarter versus last year"
    ],
    "CustomerAnalyticsAgent": [
        "Which customers are at risk of churning?",
        "Show me customer segmentation breakdown",
        "Calculate lifetime value for high-value customers"
    ],
    "DepositAnalyticsAgent": [
        "What's our deposit growth trend?",
        "Analyze deposit stability and liquidity",
        "Show me balance distribution across account types"
    ],
    "TransactionInsightsAgent": [
        "Identify any suspicious transaction patterns",
        "What are the peak transaction times?",
        "Analyze cash flow patterns for business customers"
    ]
}

def test_agent(agent_name, queries):
    """Test a specific agent with given queries"""
    print(f"\n{'='*60}")
    print(f"Testing {agent_name}")
    print('='*60)
    
    # Get agent
    agent = agent_registry.create_agent(agent_name)
    if not agent:
        print(f"ERROR: Could not create agent {agent_name}")
        return
    
    # Initialize LLM service
    settings = Settings()
    if not settings.is_openai_configured():
        print("ERROR: OpenAI API key not configured")
        return
    
    llm_service = OpenAIService(api_key=settings.OPENAI_API_KEY)
    model = settings.OPENAI_MODEL or "gpt-4o-mini"
    
    # Test each query
    for query in queries[:1]:  # Test just first query for brevity
        print(f"\nQuery: {query}")
        print("-" * 40)
        
        try:
            # Process query
            response = agent.process(
                query=query,
                llm_service=llm_service,
                model=model
            )
            
            # Print results
            if "error" in response:
                print(f"ERROR: {response['error']}")
                if "error_traceback" in response:
                    print(f"Traceback: {response['error_traceback']}")
            else:
                print(f"Response: {response.get('response', 'No response text')}")
                
                # Show execution details
                if "execution_results" in response:
                    exec_results = response["execution_results"]
                    print(f"\nExecution Summary:")
                    print(f"- Success: {exec_results.get('success', False)}")
                    print(f"- Steps executed: {len(exec_results.get('steps_executed', []))}")
                    if exec_results.get('errors'):
                        print(f"- Errors: {exec_results['errors']}")
                        
        except Exception as e:
            print(f"EXCEPTION: {str(e)}")
            import traceback
            traceback.print_exc()

def main():
    """Run tests for all agents"""
    print("Testing New Banking Agents with Real Data")
    print("="*60)
    
    # Test each agent
    for agent_name, queries in test_queries.items():
        test_agent(agent_name, queries)
    
    print("\n" + "="*60)
    print("Testing Complete!")

if __name__ == "__main__":
    main()