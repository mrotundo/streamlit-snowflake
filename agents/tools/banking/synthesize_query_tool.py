from typing import Dict, Any
from agents.tools.base_tool import BaseTool
from services.llm_interface import LLMInterface
import json


class SynthesizeQueryTool(BaseTool):
    """Tool for converting natural language requirements into structured queries"""
    
    def __init__(self, llm_service: LLMInterface, model: str):
        super().__init__(
            name="SynthesizeQuery",
            description="Convert plain text requirements into executable queries"
        )
        self.llm_service = llm_service
        self.model = model
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "requirements": {
                "type": "string",
                "description": "Natural language description of data needed"
            },
            "query_type": {
                "type": "string",
                "description": "Type of query: loan, deposit, customer",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Convert requirements to a structured query"""
        requirements = kwargs.get("requirements", "")
        query_type = kwargs.get("query_type", "general")
        
        try:
            # Use LLM to generate a structured query
            prompt = f"""Convert this natural language requirement into a structured data query.

Requirement: "{requirements}"
Query Type: {query_type}

Generate a JSON query structure that includes:
- entity: The main data entity (loans, deposits, customers)
- filters: Any filtering conditions
- aggregations: Any grouping or summarization needed
- metrics: Specific metrics to calculate
- time_period: Specific time constraints with dates

IMPORTANT TIME HANDLING (Today is July 30, 2025):
- "current quarter" or "this quarter" = Q3 2025 (Jul 1 - Sep 30, 2025)
- "last quarter" = Q2 2025 (Apr 1 - Jun 30, 2025)
- "this quarter last year" = Q3 2024 (Jul 1 - Sep 30, 2024)
- "last year" (for comparison) = Same quarter in 2024
- Be very specific with date ranges in the filters

Example time_period formats:
- {{"start": "2025-07-01", "end": "2025-09-30", "label": "Q3 2025"}}
- {{"quarter": "Q3", "year": 2025}}

Respond with ONLY valid JSON."""

            messages = [
                {"role": "system", "content": "You are a data query expert. Convert requirements to structured queries."},
                {"role": "user", "content": prompt}
            ]
            
            response = self.llm_service.complete(messages, model=self.model, temperature=0.1)
            
            # Parse the JSON response
            try:
                query = json.loads(response)
            except json.JSONDecodeError:
                # Try to extract JSON from the response
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    query = json.loads(json_match.group())
                else:
                    # Fallback query structure
                    query = {
                        "entity": query_type,
                        "filters": {},
                        "aggregations": [],
                        "metrics": ["count", "sum"],
                        "time_period": "current"
                    }
            
            # Add some example enhancements based on common patterns
            if "default" in requirements.lower() and "rate" in requirements.lower():
                query["metrics"].append("default_rate")
            if "trend" in requirements.lower():
                query["aggregations"].append("time_series")
            if "portfolio" in requirements.lower():
                query["aggregations"].append("by_category")
            
            return {
                "success": True,
                "result": {
                    "query": query,
                    "natural_language": requirements,
                    "query_explanation": f"Query to get {query.get('entity', 'data')} with filters: {query.get('filters', {})}"
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "result": {
                    "query": {"entity": query_type, "filters": {}, "metrics": ["count"]},
                    "natural_language": requirements
                }
            }