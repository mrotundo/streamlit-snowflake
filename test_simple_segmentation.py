#!/usr/bin/env python3
"""Test simple customer segmentation"""

import os
import sys
import json
from config.settings import Settings
from services.openai_service import OpenAIService
from agents.agent_registry import agent_registry
from services.data_factory import DataServiceFactory

def test_simple_segmentation():
    """Test customer segmentation with limited data"""
    print("Testing Simple Customer Segmentation")
    print("="*60)
    
    # Get data service
    data_service = DataServiceFactory.create_data_service()
    if not data_service.connect():
        print("ERROR: Could not connect to data service")
        return
    
    # Test CustomerQuery tool directly
    from agents.tools.banking.customer_query_tool import CustomerQueryTool
    customer_tool = CustomerQueryTool(data_service)
    
    print("\nTesting CustomerQuery tool directly...")
    result = customer_tool.execute(query_type="segmentation", limit=5)
    
    if result["success"]:
        print(f"Success! Got {len(result['result'].get('segments', []))} segments")
        print(f"Total customers: {result['result'].get('summary', {}).get('total_customers', 0)}")
        
        # Now test analyze tool with limited data
        settings = Settings()
        if settings.is_openai_configured():
            from services.openai_service import OpenAIService
            from agents.tools.banking.analyze_customer_segments_tool import AnalyzeCustomerSegmentsTool
            
            llm_service = OpenAIService(api_key=settings.OPENAI_API_KEY)
            model = settings.OPENAI_MODEL or "gpt-4o-mini"
            
            analyze_tool = AnalyzeCustomerSegmentsTool(llm_service, model)
            
            # Use only summary data to avoid token limits
            limited_data = {
                "summary": result['result'].get('summary', {}),
                "segments": result['result'].get('segments', [])[:3]  # Only first 3 segments
            }
            
            print("\nTesting AnalyzeCustomerSegments with limited data...")
            analyze_result = analyze_tool.execute(
                segment_data=limited_data,
                analysis_focus="general"
            )
            
            if analyze_result["success"]:
                print("Analysis successful!")
                analysis = analyze_result.get("analysis", {})
                print(f"\nAnswer: {analysis.get('answer', 'No answer')[:200]}...")
                print(f"Insights: {analysis.get('insights', [])[:2]}")  # First 2 insights
                print(f"Recommendations: {analysis.get('recommendations', [])[:2]}")  # First 2 recommendations
            else:
                print(f"Analysis failed: {analyze_result.get('error')}")
    else:
        print(f"Query failed: {result.get('error')}")

if __name__ == "__main__":
    test_simple_segmentation()