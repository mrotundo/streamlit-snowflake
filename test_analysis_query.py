#!/usr/bin/env python3
"""Test query that should trigger analysis"""

import os
import sys
import json
from config.settings import Settings
from services.openai_service import OpenAIService
from agents.agent_registry import agent_registry

def test_analysis_query():
    """Test a query that should use analysis"""
    print("Testing Analysis Query")
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
    
    query = "What's the average interest rate on our mortgage loans?"
    
    print(f"\nQuery: {query}")
    print("-" * 40)
    
    try:
        # Process query with debug callback
        def debug_callback(message, level="INFO"):
            print(f"[{level}] {message}")
        
        response = agent.process(
            query=query,
            llm_service=llm_service,
            model=model,
            debug_callback=debug_callback
        )
        
        # Print response
        print(f"\n\nFinal Response: {response.get('response', 'No response')}")
        
        # Check if we got analysis
        if "execution_results" in response:
            exec_results = response["execution_results"]
            final_output = exec_results.get("final_output", {})
            
            if final_output and isinstance(final_output, dict) and "analysis" in final_output:
                print("\n\nAnalysis found in final output!")
                analysis = final_output["analysis"]
                print(f"Answer: {analysis.get('answer', 'No answer')}")
                print(f"Insights: {analysis.get('insights', [])}")
                print(f"Recommendations: {analysis.get('recommendations', [])}")
        
    except Exception as e:
        print(f"\nEXCEPTION: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_analysis_query()