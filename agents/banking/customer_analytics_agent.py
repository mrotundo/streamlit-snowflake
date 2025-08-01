from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
from services.data_interface import DataInterface
import json


class CustomerAnalyticsAgent(BaseAgent):
    """Agent specialized in customer analytics using real database data"""
    
    def __init__(self, data_service: DataInterface):
        super().__init__(
            name="CustomerAnalyticsAgent",
            description="Performs customer analytics, segmentation, CLV, and churn analysis with real data"
        )
        self.data_service = data_service
    
    def _initialize_tools(self, llm_service: LLMInterface, model: str):
        """Initialize customer analytics specific tools"""
        # Clear existing tools first
        self._tools = []
        self.plan_executor.tools_registry = {}
        from agents.tools.banking.customer_query_tool import CustomerQueryTool
        from agents.tools.banking.transaction_query_tool import TransactionQueryTool
        from agents.tools.banking.analyze_customer_segments_tool import AnalyzeCustomerSegmentsTool
        from agents.tools.banking.analyze_transaction_patterns_tool import AnalyzeTransactionPatternsTool
        
        # Register tools
        self.register_tool(CustomerQueryTool(self.data_service))
        self.register_tool(TransactionQueryTool(self.data_service))
        self.register_tool(AnalyzeCustomerSegmentsTool(llm_service, model))
        self.register_tool(AnalyzeTransactionPatternsTool(llm_service, model))
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Customer segmentation analysis with real data",
            "Customer lifetime value (CLV) calculation",
            "Churn risk assessment and prediction",
            "Product adoption and cross-sell analysis",
            "Customer demographics and behavior analysis",
            "Retention strategy development",
            "Growth opportunity identification",
            "Real-time customer insights"
        ]
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """Determine if this agent can handle customer analytics queries"""
        
        customer_keywords = [
            "customer", "client", "user", "member", "account holder",
            "churn", "retention", "attrition", "lifetime value", "clv", "ltv",
            "segment", "segmentation", "demographic", "behavior", "behavioural",
            "cross-sell", "upsell", "product adoption", "relationship",
            "satisfaction", "nps", "experience", "engagement"
        ]
        
        query_lower = query.lower()
        
        # Check for keyword matches
        keyword_matches = sum(1 for keyword in customer_keywords if keyword in query_lower)
        
        if keyword_matches >= 2:
            return True, 0.9
        elif keyword_matches == 1:
            return True, 0.7
        
        # Check for specific patterns
        if any(phrase in query_lower for phrase in ["who are our", "which customers", "customer analysis"]):
            return True, 0.8
        
        return False, 0.0
    
    def get_system_prompt(self) -> str:
        return """You are a specialized customer analytics AI assistant with access to real banking data. Your expertise includes:
- Customer segmentation using actual customer data
- CLV calculation based on real transaction history
- Churn prediction using behavioral indicators
- Product adoption analysis from actual usage data
- Demographics analysis from customer database
- Data-driven retention and growth strategies

When answering questions:
1. Always use real data from database queries
2. Provide specific metrics and numbers
3. Base recommendations on actual patterns observed
4. Highlight actionable insights from the data
5. Consider data privacy and present aggregated insights

You have access to customer, transaction, loan, and deposit data. Use this to provide accurate, data-driven insights."""
    
    def create_plan(
        self,
        query: str,
        llm_service: LLMInterface,
        model: str,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create an execution plan for customer-related queries"""
        
        planning_prompt = f"""Create an execution plan to answer this customer-related query: "{query}"

Available tools:
1. CustomerQuery - Requires: query_type (string), filters (dict, optional), time_period (dict, optional), group_by (list, optional), limit (number, optional)
2. AnalyzeCustomerSegments - Requires: segment_data (dict), analysis_focus (string, optional), context (dict, optional)
   IMPORTANT: When using AnalyzeCustomerSegments as the final step, set output_key to "analysis"
3. TransactionQuery - Requires: query_type (string), filters (dict, optional), time_period (dict, optional), group_by (list, optional)
4. AnalyzeTransactionPatterns - Requires: transaction_data (dict), analysis_type (string, optional), customer_context (dict, optional)
   IMPORTANT: When using AnalyzeTransactionPatterns as the final step, set output_key to "analysis"

Create a JSON plan with:
- goal: What we're trying to achieve
- steps: Array of steps, each with:
  - step: Step number
  - tool: Tool name to use
  - description: What this step does
  - inputs: Tool inputs (can reference previous outputs with ${{output_key}})
  - output_key: Key to store this step's output
- adaptations: Dictionary with keys:
  - error: What to do if a step fails
  - no_data: What to do if no data is available

Focus on getting customer data to provide accurate insights.

Respond with ONLY valid JSON."""

        messages = [
            {"role": "system", "content": "You are a customer analytics planning expert. Create detailed execution plans."},
            {"role": "user", "content": planning_prompt}
        ]
        
        if conversation_history:
            context = "Previous conversation context:\n"
            for msg in conversation_history[-3:]:
                context += f"{msg['role']}: {msg['content'][:100]}...\n"
            messages[0]["content"] += f"\n\n{context}"
        
        try:
            response = llm_service.complete(messages, model=model, temperature=0.1)
            
            # Ensure response is a string
            if not isinstance(response, str):
                print(f"WARNING: LLM response is not a string: {type(response)}")
                return self._create_default_plan(query)
            
            try:
                plan = json.loads(response)
            except json.JSONDecodeError:
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    try:
                        plan = json.loads(json_match.group())
                    except json.JSONDecodeError:
                        print(f"Failed to parse extracted JSON: {json_match.group()[:100]}...")
                        return self._create_default_plan(query)
                else:
                    print(f"No JSON found in response: {response[:100]}...")
                    return self._create_default_plan(query)
            
            # Validate plan structure
            if not isinstance(plan, dict):
                print(f"Plan is not a dictionary: {type(plan)}")
                return self._create_default_plan(query)
            
            if "goal" not in plan:
                plan["goal"] = f"Answer customer query: {query}"
            if "steps" not in plan or not isinstance(plan["steps"], list) or not plan["steps"]:
                return self._create_default_plan(query)
            
            # Ensure adaptations is a dictionary
            if "adaptations" not in plan or not isinstance(plan["adaptations"], dict):
                plan["adaptations"] = {
                    "error": "Provide general customer insights based on available data",
                    "no_data": "Explain what customer data would be needed for this analysis"
                }
            
            return plan
            
        except Exception as e:
            print(f"Exception in create_plan: {str(e)}")
            return self._create_default_plan(query)
    
    def _create_default_plan(self, query: str) -> Dict[str, Any]:
        """Create a default plan when automatic planning fails"""
        
        query_lower = query.lower()
        
        if any(word in query_lower for word in ["segment", "analysis", "profile", "demographic"]):
            # Customer segmentation plan
            return {
                "goal": f"Analyze customer segments for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "CustomerQuery",
                        "description": "Get customer segmentation data from database",
                        "inputs": {
                            "query_type": "segmentation",
                            "group_by": ["segment"]
                        },
                        "output_key": "segment_data"
                    },
                    {
                        "step": 2,
                        "tool": "AnalyzeCustomerSegments",
                        "description": "Analyze customer segments and patterns",
                        "inputs": {
                            "segment_data": "${segment_data}",
                            "analysis_focus": "general"
                        },
                        "output_key": "analysis"
                    },
                    {
                        "step": 3,
                        "tool": "TransactionQuery",
                        "description": "Get transaction patterns by segment",
                        "inputs": {
                            "query_type": "pattern_detection",
                            "time_period": {"start": "date('now', '-30 days')"}
                        },
                        "output_key": "transaction_patterns"
                    }
                ],
                "adaptations": {
                    "no_data": "Explain customer segmentation best practices",
                    "error": "Provide general customer insights"
                }
            }
        
        elif any(word in query_lower for word in ["churn", "retention", "leaving", "risk"]):
            # Churn analysis plan
            return {
                "goal": f"Analyze customer churn patterns for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "CustomerQuery",
                        "description": "Get at-risk customer data from database",
                        "inputs": {
                            "query_type": "churn_risk",
                            "limit": 100
                        },
                        "output_key": "churn_data"
                    },
                    {
                        "step": 2,
                        "tool": "AnalyzeCustomerSegments",
                        "description": "Analyze churn patterns and develop retention strategies",
                        "inputs": {
                            "segment_data": "${churn_data}",
                            "analysis_focus": "retention_strategies"
                        },
                        "output_key": "analysis"
                    },
                    {
                        "step": 3,
                        "tool": "TransactionQuery",
                        "description": "Analyze transaction behavior of at-risk customers",
                        "inputs": {
                            "query_type": "behavioral_insights",
                            "filters": {"segment": "at_risk"},
                            "time_period": {"start": "date('now', '-90 days')"}
                        },
                        "output_key": "behavior_analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Provide churn prevention strategies",
                    "error": "Offer general retention best practices"
                }
            }
        
        elif any(word in query_lower for word in ["lifetime value", "clv", "ltv", "value"]):
            # Customer lifetime value plan
            return {
                "goal": f"Calculate customer lifetime value for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "CustomerQuery",
                        "description": "Get customer lifetime value data",
                        "inputs": {
                            "query_type": "lifetime_value",
                            "limit": 50
                        },
                        "output_key": "clv_data"
                    },
                    {
                        "step": 2,
                        "tool": "AnalyzeCustomerSegments",
                        "description": "Analyze CLV patterns and opportunities",
                        "inputs": {
                            "segment_data": "${clv_data}",
                            "analysis_focus": "growth_opportunities"
                        },
                        "output_key": "analysis"
                    },
                    {
                        "step": 3,
                        "tool": "CustomerQuery",
                        "description": "Get product adoption data for high-value customers",
                        "inputs": {
                            "query_type": "product_adoption",
                            "filters": {"segment": "high_value"}
                        },
                        "output_key": "product_data"
                    }
                ],
                "adaptations": {
                    "no_data": "Explain CLV calculation methodology",
                    "error": "Provide CLV improvement strategies"
                }
            }
        
        else:
            # General customer analytics plan
            return {
                "goal": f"Provide customer analytics insights for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "CustomerQuery",
                        "description": "Get relevant customer data",
                        "inputs": {
                            "query_type": "demographics",
                            "group_by": ["segment"],
                            "limit": 100
                        },
                        "output_key": "customer_data"
                    },
                    {
                        "step": 2,
                        "tool": "TransactionQuery",
                        "description": "Get customer transaction patterns",
                        "inputs": {
                            "query_type": "pattern_detection",
                            "time_period": {"start": "date('now', '-30 days')"}
                        },
                        "output_key": "transaction_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeTransactionPatterns",
                        "description": "Analyze customer behavior patterns",
                        "inputs": {
                            "transaction_data": "${transaction_data}",
                            "analysis_type": "behavioral_insights",
                            "customer_context": "${customer_data}"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Provide general customer insights",
                    "error": "Offer alternative information sources"
                }
            }
    
    def _identify_used_capabilities(self, query: str) -> List[str]:
        """Identify which capabilities might be used for this query"""
        used = []
        query_lower = query.lower()
        
        capability_keywords = {
            "Customer segmentation analysis": ["segment", "group", "cluster", "categorize"],
            "Customer lifetime value (CLV) calculation": ["lifetime value", "clv", "ltv"],
            "Churn prediction and prevention": ["churn", "retention", "leaving", "attrition"],
            "Customer satisfaction metrics": ["satisfaction", "nps", "happy", "experience"],
            "Cross-sell/up-sell opportunities": ["cross-sell", "upsell", "recommend", "additional"]
        }
        
        for capability, keywords in capability_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                used.append(capability)
        
        # If no specific capabilities identified, mark as general inquiry
        if not used:
            used.append("General banking inquiries")
        
        return used