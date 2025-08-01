from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
import json
import re


class DataDetailsAgent(BaseAgent):
    """Agent specialized in providing detailed information about database views and fields"""
    
    def __init__(self):
        super().__init__(
            name="DataDetailsAgent",
            description="I provide detailed information about database views, columns, and data catalog metadata"
        )
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """
        Determine if this agent can handle queries about view/field details.
        
        Keywords: view details, field info, column description, data dictionary, 
                 catalog, metadata, what is in view, tell me about field
        """
        keywords = [
            'view detail', 'field info', 'column', 'data dictionary',
            'catalog', 'metadata', 'what is in', 'tell me about',
            'describe', 'show me', 'data in', 'fields in',
            'columns in', 'structure', 'schema', 'definition'
        ]
        
        # Specific view name patterns
        view_patterns = [
            r'v_\w+',  # Matches v_executive_dashboard, etc.
            r'executive dashboard',
            r'customer summary',
            r'loan portfolio',
            r'deposit summary',
            r'risk analytics'
        ]
        
        query_lower = query.lower()
        keyword_matches = sum(1 for keyword in keywords if keyword in query_lower)
        
        # Check for view name mentions
        view_mentioned = any(re.search(pattern, query_lower) for pattern in view_patterns)
        
        # Check for specific patterns
        if any(phrase in query_lower for phrase in [
            'what is', 'tell me about', 'show me', 'describe',
            'details about', 'information about', 'columns in',
            'fields in', 'structure of'
        ]):
            keyword_matches += 2
        
        # If asking about a specific field/column
        if 'field' in query_lower or 'column' in query_lower:
            keyword_matches += 2
        
        # Calculate confidence
        if keyword_matches >= 3 or (view_mentioned and keyword_matches >= 1):
            confidence = min(0.95, 0.7 + (keyword_matches * 0.05))
            return True, confidence
        elif keyword_matches >= 2:
            confidence = 0.5 + (keyword_matches * 0.1)
            return True, confidence
        elif view_mentioned:
            return True, 0.6
        
        return False, 0.0
    
    def get_system_prompt(self) -> str:
        """Get the system prompt for data details queries"""
        return """You are a Data Catalog Specialist AI assistant for a banking system.
        
Your expertise includes:
- Providing comprehensive information about database views and tables
- Explaining column/field details including data types and classifications
- Showing example queries and use cases
- Presenting data dictionaries and metadata
- Explaining relationships between views and data lineage

When users ask about views or fields, you should:
1. Retrieve catalog metadata to understand the data structure
2. Provide clear, organized information about the requested item
3. Include practical examples when available
4. Show tabular representations when appropriate
5. Explain business context and technical details

Always provide accurate, detailed information from the data catalog."""
    
    @property
    def capabilities(self) -> List[str]:
        """List capabilities of the DataDetails agent"""
        return [
            "Provide detailed view metadata and descriptions",
            "Explain column/field information including data types",
            "Show data classifications and security levels",
            "Display example queries for views",
            "Present tabular data dictionaries",
            "Show view metrics and statistics",
            "Explain relationships between data elements",
            "Provide business and technical context"
        ]
    
    def _extract_view_and_column(self, query: str) -> tuple[str, Optional[str]]:
        """Extract view name and optionally column name from query"""
        query_lower = query.lower()
        
        # Common view mappings
        view_mappings = {
            'executive dashboard': 'v_executive_dashboard',
            'risk analytics': 'v_risk_analytics',
            'customer summary': 'v_customer_summary',
            'loan portfolio': 'v_loan_portfolio',
            'deposit summary': 'v_deposit_summary',
            'customer products': 'v_customer_products',
            'customer risk': 'v_customer_risk_profile',
            'product performance': 'v_product_performance',
            'customer lifetime': 'v_customer_lifetime_value'
        }
        
        view_name = None
        column_name = None
        
        # Check for known view names
        for phrase, mapped_view in view_mappings.items():
            if phrase in query_lower:
                view_name = mapped_view
                break
        
        # Check if v_ view name is directly mentioned
        if not view_name:
            view_match = re.search(r'v_\w+', query, re.IGNORECASE)
            if view_match:
                view_name = view_match.group().lower()
        
        # Extract column/field name
        # Patterns like "customer_id field", "column customer_id", "the customer_id"
        column_patterns = [
            r'(?:field|column)\s+(\w+)',
            r'the\s+(\w+)\s+(?:field|column)',
            r'(\w+)\s+(?:field|column)\s+in',
            r'about\s+(?:the\s+)?(\w+)\s+(?:field|column)?'
        ]
        
        for pattern in column_patterns:
            match = re.search(pattern, query_lower)
            if match:
                potential_column = match.group(1)
                # Validate it's not a common word
                if potential_column not in ['the', 'a', 'an', 'this', 'that', 'view', 'table']:
                    column_name = potential_column
                    break
        
        # If no view found but column mentioned, default to a common view
        if column_name and not view_name:
            # Try to guess based on column name
            if 'customer' in column_name:
                view_name = 'v_customer_summary'
            elif 'loan' in column_name:
                view_name = 'v_loan_portfolio'
            else:
                view_name = 'v_executive_dashboard'  # Default
        
        return view_name, column_name
    
    def create_plan(
        self,
        query: str,
        llm_service: LLMInterface,
        model: str,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create an execution plan for data details queries"""
        
        # Extract view and column from query
        view_name, column_name = self._extract_view_and_column(query)
        
        # Determine if this is a tabular request
        is_tabular = any(word in query.lower() for word in ['table', 'tabular', 'show me', 'display'])
        
        planning_prompt = f"""Create an execution plan to provide data catalog information for: "{query}"

Extracted context:
- View name: {view_name or "Not specified"}
- Column name: {column_name or "Not specified"}
- Tabular format requested: {is_tabular}

Available tools:
1. GetViewCatalog - Requires: view_name (string)
   - Retrieves view metadata including descriptions, owner, domain
   
2. GetColumnDetails - Requires: view_name (string), column_name (string, optional)
   - Gets column information including types, descriptions, classifications
   
3. GetViewExamples - Requires: view_name (string), example_type (string, optional)
   - Retrieves example queries for the view
   
4. GetViewMetrics - Requires: view_name (string), metric_name (string, optional), days_back (integer, optional)
   - Gets metrics and KPIs for the view
   
5. AnalyzeViewDetails - Requires: catalog_data (dict), column_data (dict, optional), examples_data (dict, optional), metrics_data (dict, optional), user_query (string), response_format (string, optional)
   - Synthesizes all data into a comprehensive response

Create a JSON plan that retrieves and presents the requested information.

Example response format:
{{
    "goal": "Provide details about the executive dashboard view",
    "steps": [
        {{
            "step": 1,
            "tool": "GetViewCatalog",
            "description": "Get view metadata",
            "parameters": {{
                "view_name": "v_executive_dashboard"
            }}
        }},
        {{
            "step": 2,
            "tool": "GetColumnDetails",
            "description": "Get column information",
            "parameters": {{
                "view_name": "v_executive_dashboard"
            }}
        }},
        {{
            "step": 3,
            "tool": "GetViewExamples",
            "description": "Get example queries",
            "parameters": {{
                "view_name": "v_executive_dashboard"
            }}
        }},
        {{
            "step": 4,
            "tool": "AnalyzeViewDetails",
            "description": "Format comprehensive response",
            "parameters": {{
                "catalog_data": "${{step_1.result}}",
                "column_data": "${{step_2.result}}",
                "examples_data": "${{step_3.result}}",
                "user_query": "{query}",
                "response_format": "detailed"
            }}
        }}
    ]
}}

Important:
- If asking about a specific column, include column_name in GetColumnDetails
- For tabular display, set response_format to "tabular" in AnalyzeViewDetails
- Always include the user_query in AnalyzeViewDetails for context"""
        
        messages = [
            {"role": "system", "content": "You are a data catalog expert. Create plans to retrieve and present metadata."},
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
            # Fallback plan based on query type
            if column_name:
                # Specific column query
                return self._create_column_plan(query, view_name, column_name)
            else:
                # General view query
                return self._create_view_plan(query, view_name, is_tabular)
    
    def _create_view_plan(self, query: str, view_name: str, is_tabular: bool) -> Dict[str, Any]:
        """Create a plan for general view information"""
        view_name = view_name or "v_executive_dashboard"  # Default if not found
        
        return {
            "goal": f"Provide comprehensive information about {view_name}",
            "steps": [
                {
                    "step": 1,
                    "tool": "GetViewCatalog",
                    "description": "Retrieve view metadata",
                    "parameters": {
                        "view_name": view_name
                    }
                },
                {
                    "step": 2,
                    "tool": "GetColumnDetails",
                    "description": "Get all column information",
                    "parameters": {
                        "view_name": view_name
                    }
                },
                {
                    "step": 3,
                    "tool": "GetViewExamples",
                    "description": "Get example queries",
                    "parameters": {
                        "view_name": view_name
                    }
                },
                {
                    "step": 4,
                    "tool": "GetViewMetrics",
                    "description": "Get recent metrics",
                    "parameters": {
                        "view_name": view_name,
                        "days_back": 7
                    }
                },
                {
                    "step": 5,
                    "tool": "AnalyzeViewDetails",
                    "description": "Synthesize and format response",
                    "parameters": {
                        "catalog_data": "${step_1_output}",
                        "column_data": "${step_2_output}",
                        "examples_data": "${step_3_output}",
                        "metrics_data": "${step_4_output}",
                        "user_query": query,
                        "response_format": "tabular" if is_tabular else "detailed"
                    }
                }
            ]
        }
    
    def _create_column_plan(self, query: str, view_name: str, column_name: str) -> Dict[str, Any]:
        """Create a plan for specific column information"""
        view_name = view_name or "v_customer_summary"  # Default if not found
        
        return {
            "goal": f"Provide details about {column_name} field in {view_name}",
            "steps": [
                {
                    "step": 1,
                    "tool": "GetViewCatalog",
                    "description": "Get view context",
                    "parameters": {
                        "view_name": view_name
                    }
                },
                {
                    "step": 2,
                    "tool": "GetColumnDetails",
                    "description": f"Get specific details for {column_name}",
                    "parameters": {
                        "view_name": view_name,
                        "column_name": column_name
                    }
                },
                {
                    "step": 3,
                    "tool": "AnalyzeViewDetails",
                    "description": "Format field-focused response",
                    "parameters": {
                        "catalog_data": "${step_1_output}",
                        "column_data": "${step_2_output}",
                        "user_query": query,
                        "response_format": "detailed"
                    }
                }
            ]
        }
    
    def _initialize_tools(self, llm_service: LLMInterface, model: str):
        """Initialize tools specific to data details"""
        # Clear existing tools
        self._tools = []
        self.plan_executor.tools_registry = {}
        
        # Import and register catalog tools
        from agents.tools.banking.get_view_catalog_tool import GetViewCatalogTool
        from agents.tools.banking.get_column_details_tool import GetColumnDetailsTool
        from agents.tools.banking.get_view_examples_tool import GetViewExamplesTool
        from agents.tools.banking.get_view_metrics_tool import GetViewMetricsTool
        from agents.tools.banking.analyze_view_details_tool import AnalyzeViewDetailsTool
        
        # Create tool instances
        catalog_tool = GetViewCatalogTool(self.data_service)
        column_tool = GetColumnDetailsTool(self.data_service)
        examples_tool = GetViewExamplesTool(self.data_service)
        metrics_tool = GetViewMetricsTool(self.data_service)
        analyze_tool = AnalyzeViewDetailsTool(llm_service, model)
        
        # Register tools
        self.register_tool(catalog_tool)
        self.register_tool(column_tool)
        self.register_tool(examples_tool)
        self.register_tool(metrics_tool)
        self.register_tool(analyze_tool)
    
    def _format_execution_response(
        self,
        query: str,
        plan: Dict[str, Any],
        execution_results: Dict[str, Any],
        llm_service: LLMInterface,
        model: str
    ) -> Dict[str, Any]:
        """Format the execution results into a response for data details queries"""
        
        # Get the base response
        response = super()._format_execution_response(query, plan, execution_results, llm_service, model)
        
        # Check if we have analysis results from AnalyzeViewDetails
        steps_executed = execution_results.get("steps_executed", [])
        analysis_step = next((s for s in steps_executed if s.get("tool") == "AnalyzeViewDetails"), None)
        
        if analysis_step and analysis_step.get("success") and analysis_step.get("output"):
            analysis_output = analysis_step["output"]
            
            # Use the formatted summary from the analysis tool
            if "summary" in analysis_output:
                response["response"] = analysis_output["summary"]
            
            # Add structured data if available
            if "field_info" in analysis_output:
                response["data"] = {
                    "field": analysis_output["field_info"],
                    "view_context": analysis_output.get("view_context", {})
                }
            elif "metadata" in analysis_output:
                response["data"] = {
                    "view_metadata": analysis_output["metadata"],
                    "column_count": analysis_output.get("column_count", 0),
                    "has_examples": analysis_output.get("has_examples", False)
                }
            
            # Add table format if available
            if "table_format" in analysis_output:
                response["table_data"] = analysis_output["table_format"]
            
            # Update metadata
            response["metadata"]["data_source"] = "data_catalog"
            response["metadata"]["response_type"] = "field_details" if "field_info" in analysis_output else "view_details"
        
        return response