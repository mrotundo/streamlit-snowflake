from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
import json


class DataStatusAgent(BaseAgent):
    """Agent specialized in data quality and lineage investigation"""
    
    def __init__(self):
        super().__init__(
            name="DataStatusAgent",
            description="I investigate data quality issues and trace data lineage to find root causes"
        )
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """
        Determine if this agent can handle data quality and lineage queries.
        
        Keywords: data quality, lineage, stale data, view issue, job status, data freshness,
                 source table, dependency, last updated, job failed, missing data
        """
        keywords = [
            'data quality', 'data issue', 'lineage', 'job status', 'job fail',
            'view', 'stale', 'fresh', 'updated', 'source', 'dependency',
            'trace', 'investigate', 'wrong data', 'incorrect', 'missing',
            'executive dashboard', 'risk analytics', 'customer summary',
            'not updated', 'delay', 'late', 'problem with view'
        ]
        
        query_lower = query.lower()
        keyword_matches = sum(1 for keyword in keywords if keyword in query_lower)
        
        # Check for specific patterns
        if any(phrase in query_lower for phrase in [
            'data in', 'view is', 'why is', 'when was', 'trace the',
            'check the data', 'investigate', 'data looks'
        ]):
            keyword_matches += 2
        
        # Calculate confidence based on keyword matches
        if keyword_matches >= 3:
            confidence = min(0.95, 0.7 + (keyword_matches * 0.05))
            return True, confidence
        elif keyword_matches >= 1:
            confidence = 0.4 + (keyword_matches * 0.15)
            return True, confidence
        
        return False, 0.0
    
    def get_system_prompt(self) -> str:
        """Get the system prompt for data investigation"""
        return """You are a Data Quality Specialist AI assistant for a banking system.
        
Your expertise includes:
- Investigating data quality issues in views and tables
- Tracing data lineage from views back to source files
- Checking job execution status and history
- Identifying root causes of data problems
- Understanding view dependencies and data flow

When users report data issues, you should:
1. First validate their concern by checking the view/table
2. Trace the lineage to find dependencies
3. Check job status for relevant data loads
4. Identify the root cause
5. Provide clear explanation of the issue

Always be thorough in your investigation and provide actionable insights."""
    
    @property
    def capabilities(self) -> List[str]:
        """List capabilities of the DataStatus agent"""
        return [
            "Investigate data quality issues in views and tables",
            "Trace data lineage from views to source files",
            "Check job execution status and history",
            "Analyze view dependencies",
            "Identify root causes of stale or incorrect data",
            "Monitor data freshness",
            "Explain data flow through the system",
            "Validate data concerns"
        ]
    
    def _extract_view_name(self, query: str) -> str:
        """Extract view name from query"""
        query_lower = query.lower()
        
        # Common view mappings
        view_mappings = {
            'executive dashboard': 'v_executive_dashboard',
            'risk analytics': 'v_risk_analytics',
            'customer summary': 'v_customer_summary',
            'loan portfolio': 'v_loan_portfolio',
            'deposit summary': 'v_deposit_summary',
            'customer products': 'v_customer_products'
        }
        
        # Check for known view names
        for phrase, view_name in view_mappings.items():
            if phrase in query_lower:
                return view_name
        
        # Check if v_ view name is directly mentioned
        import re
        view_match = re.search(r'v_\w+', query, re.IGNORECASE)
        if view_match:
            return view_match.group()
        
        # Default to executive dashboard
        return 'v_executive_dashboard'
    
    def create_plan(
        self,
        query: str,
        llm_service: LLMInterface,
        model: str,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create an execution plan for data investigation"""
        
        # Extract view name from query
        view_name = self._extract_view_name(query)
        
        planning_prompt = f"""Create an execution plan to investigate this data issue: "{query}"

IMPORTANT: When the user mentions:
- "executive dashboard" → use view_name: "v_executive_dashboard"
- "risk analytics" → use view_name: "v_risk_analytics"
- "customer summary" → use view_name: "v_customer_summary"
- If no specific view is mentioned, default to "v_executive_dashboard"

For this query, focus on: {view_name}

Available tools:
1. CheckViewData - Requires: view_name (string), limit (integer, optional)
   - Use this to query a view and validate the user's concern
   - Returns sample data and row counts

2. TraceDataLineage - Requires: object_name (string), object_type (string: 'view' or 'table')
   - Use this to find all dependencies for a view/table
   - Traces back to source tables, jobs, and files
   - Returns complete lineage chain

3. CheckJobStatus - Requires: job_name (string, optional), time_range (string, optional)
   - Use this to check job execution history
   - Returns job runs with status, timing, and errors
   - Can check specific job or all recent jobs

4. AnalyzeDataFreshness - Requires: object_name (string)
   - Use this to check when data was last updated
   - Compares timestamps across related objects
   - Identifies data lag issues

5. ProvideAnalysis - Requires: data (dict), question (string), analysis_type (string, optional)
   - Use this to analyze the findings and provide insights

Create a JSON plan that investigates the data issue step by step.

Example response format:
{{
    "goal": "Investigate why customer count is wrong in executive dashboard",
    "steps": [
        {{
            "step": 1,
            "tool": "CheckViewData",
            "description": "Query v_executive_dashboard to confirm the issue",
            "parameters": {{
                "view_name": "v_executive_dashboard",
                "limit": 10
            }}
        }},
        {{
            "step": 2,
            "tool": "TraceDataLineage",
            "description": "Trace lineage of v_executive_dashboard to find dependencies",
            "parameters": {{
                "object_name": "v_executive_dashboard",
                "object_type": "view"
            }}
        }},
        {{
            "step": 3,
            "tool": "CheckJobStatus",
            "description": "Check status of jobs that load the source tables",
            "parameters": {{
                "time_range": "last 48 hours"
            }}
        }},
        {{
            "step": 4,
            "tool": "ProvideAnalysis",
            "description": "Analyze findings and identify root cause",
            "parameters": {{
                "data": {{
                    "view_data": "${{step_1.result}}",
                    "lineage": "${{step_2.result}}",
                    "job_status": "${{step_3.result}}"
                }},
                "question": query,
                "analysis_type": "root_cause_analysis"
            }}
        }}
    ]
}}

Important: 
- Start by validating the user's concern
- Follow the data lineage systematically
- Check all relevant job statuses
- Provide clear root cause analysis"""
        
        messages = [
            {"role": "system", "content": "You are a data investigation expert. Create detailed plans to trace and diagnose data issues."},
            {"role": "user", "content": planning_prompt}
        ]
        
        response = llm_service.complete(messages, model=model, temperature=0.1)
        
        try:
            # Parse the JSON response
            plan = json.loads(response)
            
            # Validate plan structure
            if not isinstance(plan, dict) or 'steps' not in plan:
                raise ValueError("Invalid plan structure")
            
            # Ensure all steps have required fields
            for step in plan['steps']:
                if 'parameters' not in step:
                    step['parameters'] = {}
            
            return plan
            
        except (json.JSONDecodeError, ValueError) as e:
            # Fallback plan for investigating data issues
            return {
                "goal": f"Investigate data issue: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "CheckViewData",
                        "description": "Check the current state of the reported view/table",
                        "parameters": {
                            "view_name": view_name,  # Use extracted view name
                            "limit": 10
                        }
                    },
                    {
                        "step": 2,
                        "tool": "TraceDataLineage",
                        "description": "Trace data lineage to find dependencies",
                        "parameters": {
                            "object_name": view_name,  # Use extracted view name
                            "object_type": "view"
                        }
                    },
                    {
                        "step": 3,
                        "tool": "CheckJobStatus",
                        "description": "Check recent job execution status",
                        "parameters": {
                            "time_range": "last 24 hours"
                        }
                    },
                    {
                        "step": 4,
                        "tool": "ProvideAnalysis",
                        "description": "Analyze findings and provide insights",
                        "parameters": {
                            "data": {
                                "investigation_results": "${step_1.result}, ${step_2.result}, ${step_3.result}"
                            },
                            "question": query,
                            "analysis_type": "data_quality_investigation"
                        }
                    }
                ]
            }
    
    def _initialize_tools(self, llm_service: LLMInterface, model: str):
        """Initialize tools specific to data status investigation"""
        # Clear existing tools
        self._tools = []
        self.plan_executor.tools_registry = {}
        
        # Import and register data investigation tools
        from agents.tools.banking.check_view_data_tool import CheckViewDataTool
        from agents.tools.banking.trace_data_lineage_tool import TraceDataLineageTool
        from agents.tools.banking.check_job_status_tool import CheckJobStatusTool
        from agents.tools.banking.analyze_data_freshness_tool import AnalyzeDataFreshnessTool
        from agents.tools.banking.provide_analysis_tool import ProvideAnalysisTool
        
        # Create tool instances
        check_view_tool = CheckViewDataTool(self.data_service)
        trace_lineage_tool = TraceDataLineageTool(self.data_service)
        check_job_tool = CheckJobStatusTool(self.data_service)
        analyze_freshness_tool = AnalyzeDataFreshnessTool(self.data_service)
        analysis_tool = ProvideAnalysisTool(llm_service, model)
        
        # Register tools
        self.register_tool(check_view_tool)
        self.register_tool(trace_lineage_tool)
        self.register_tool(check_job_tool)
        self.register_tool(analyze_freshness_tool)
        self.register_tool(analysis_tool)