#!/usr/bin/env python3
"""Test customer segmentation query with detailed debugging"""

import os
import sys
import json
from config.settings import Settings
from services.openai_service import OpenAIService
from agents.agent_registry import agent_registry

def test_segmentation_detailed():
    """Test customer segmentation query with detailed output"""
    print("Testing Customer Segmentation Query - Detailed")
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
    
    # Simple segmentation query
    query = "Show me customer segmentation"
    
    print(f"\nQuery: {query}")
    print("-" * 40)
    
    # First create the plan
    plan = agent.create_plan(query, llm_service, model)
    print("\nPlan created:")
    print(f"Goal: {plan.get('goal')}")
    print(f"Steps: {len(plan.get('steps', []))}")
    for step in plan.get('steps', []):
        print(f"  {step['step']}. {step['tool']} -> {step.get('output_key')}")
    
    try:
        # Process query
        response = agent.process(
            query=query,
            llm_service=llm_service,
            model=model
        )
        
        # Check execution results
        exec_results = response.get('execution_results', {})
        final_output = exec_results.get('final_output')
        
        print(f"\n\nFinal Output Type: {type(final_output)}")
        if final_output:
            print(f"Final Output Keys: {list(final_output.keys()) if isinstance(final_output, dict) else 'Not a dict'}")
            if isinstance(final_output, dict) and 'answer' in final_output:
                print(f"\nAnswer: {final_output['answer'][:200]}...")
                print(f"Insights: {len(final_output.get('insights', []))} items")
                print(f"Recommendations: {len(final_output.get('recommendations', []))} items")
        
        print(f"\n\nFormatted Response:")
        print(response.get('response', 'No response'))
        
    except Exception as e:
        print(f"\nEXCEPTION: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_segmentation_detailed()