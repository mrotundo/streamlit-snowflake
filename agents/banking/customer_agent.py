from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
import json


class CustomerAgent(BaseAgent):
    """Agent specialized in customer profiles, segmentation, and behavior analysis"""
    
    def __init__(self):
        super().__init__(
            name="CustomerAgent",
            description="Handles customer analytics, segmentation, satisfaction, and general banking queries"
        )
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Customer segmentation analysis",
            "Customer lifetime value (CLV) calculation",
            "Churn prediction and prevention",
            "Customer satisfaction metrics",
            "Cross-sell/up-sell opportunities",
            "Customer demographics analysis",
            "Banking product recommendations",
            "Customer journey mapping",
            "General banking inquiries"
        ]
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """Determine if this agent can handle customer-related queries"""
        
        customer_keywords = [
            "customer", "client", "user", "member", "satisfaction",
            "churn", "retention", "lifetime value", "clv", "segment",
            "demographic", "behavior", "cross-sell", "upsell",
            "experience", "journey", "persona", "profile"
        ]
        
        query_lower = query.lower()
        
        # Check for keyword matches
        keyword_matches = sum(1 for keyword in customer_keywords if keyword in query_lower)
        
        if keyword_matches >= 2:
            return True, 0.85
        elif keyword_matches == 1:
            return True, 0.6
        
        # This agent also handles general queries as the default
        # If no other specific keywords, still handle with lower confidence
        return True, 0.3
    
    def get_system_prompt(self) -> str:
        return """You are a specialized banking customer analytics AI assistant. Your expertise includes:
- Customer segmentation and profiling
- Customer lifetime value analysis
- Churn prediction and retention strategies
- Customer satisfaction and NPS analysis
- Cross-sell and up-sell opportunity identification
- Demographic and behavioral analysis
- General banking knowledge and assistance

When answering questions:
1. Provide insights based on customer data patterns
2. Suggest actionable strategies for customer engagement
3. Identify opportunities for improving customer experience
4. Use data-driven approaches for recommendations
5. Consider privacy and regulatory requirements

For general banking queries, provide helpful and accurate information while maintaining a professional tone."""
    
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
1. SynthesizeQuery - Requires: requirements (string), query_type (string, optional)
2. RunQuery - Requires: query (dict)
3. ProvideAnalysis - Requires: data (dict), question (string), analysis_type (string, optional)

Create a JSON plan with:
- goal: What we're trying to achieve
- steps: Array of steps, each with:
  - step: Step number
  - tool: Tool name to use
  - description: What this step does
  - inputs: Tool inputs (can reference previous outputs with ${{output_key}})
  - output_key: Key to store this step's output
- adaptations: How to handle errors or missing data

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
                        "tool": "SynthesizeQuery",
                        "description": "Create query for customer segmentation data",
                        "inputs": {
                            "requirements": query,
                            "query_type": "customer"
                        },
                        "output_key": "segment_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get customer segmentation data",
                        "inputs": {
                            "query": "${segment_query.query}"
                        },
                        "output_key": "segment_data"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Analyze customer segments and patterns",
                        "inputs": {
                            "data": "${segment_data}",
                            "question": query,
                            "analysis_type": "segmentation_analysis"
                        },
                        "output_key": "analysis"
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
                        "tool": "SynthesizeQuery",
                        "description": "Create query for churn risk data",
                        "inputs": {
                            "requirements": f"Get customer churn and retention data for: {query}",
                            "query_type": "customer"
                        },
                        "output_key": "churn_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get churn risk data",
                        "inputs": {
                            "query": "${churn_query.query}"
                        },
                        "output_key": "churn_data"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Analyze churn patterns and provide retention strategies",
                        "inputs": {
                            "data": "${churn_data}",
                            "question": query,
                            "analysis_type": "churn_analysis"
                        },
                        "output_key": "analysis"
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
                        "tool": "SynthesizeQuery",
                        "description": "Create query for CLV calculation data",
                        "inputs": {
                            "requirements": f"Get customer value and revenue data for: {query}",
                            "query_type": "customer"
                        },
                        "output_key": "clv_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get customer value data",
                        "inputs": {
                            "query": "${clv_query.query}"
                        },
                        "output_key": "clv_data"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Calculate CLV and provide value insights",
                        "inputs": {
                            "data": "${clv_data}",
                            "question": query,
                            "analysis_type": "clv_analysis"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Explain CLV calculation methodology",
                    "error": "Provide CLV improvement strategies"
                }
            }
        
        else:
            # General customer query plan
            return {
                "goal": f"Answer customer question: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "SynthesizeQuery",
                        "description": "Understand customer data needs",
                        "inputs": {
                            "requirements": query,
                            "query_type": "customer"
                        },
                        "output_key": "data_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get relevant customer data",
                        "inputs": {
                            "query": "${data_query.query}"
                        },
                        "output_key": "customer_info"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Provide comprehensive answer",
                        "inputs": {
                            "data": "${customer_info}",
                            "question": query,
                            "analysis_type": "general"
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