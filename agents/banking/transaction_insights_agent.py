from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
from services.data_interface import DataInterface
import json


class TransactionInsightsAgent(BaseAgent):
    """Agent specialized in transaction analysis and behavioral insights using real data"""
    
    def __init__(self, data_service: DataInterface):
        super().__init__(
            name="TransactionInsightsAgent",
            description="Analyzes transaction patterns, cash flows, and customer behavior using real transaction data"
        )
        self.data_service = data_service
    
    def _initialize_tools(self, llm_service: LLMInterface, model: str):
        """Initialize transaction insights specific tools"""
        # Clear existing tools first
        self._tools = []
        self.plan_executor.tools_registry = {}
        from agents.tools.banking.transaction_query_tool import TransactionQueryTool
        from agents.tools.banking.customer_query_tool import CustomerQueryTool
        from agents.tools.banking.analyze_transaction_patterns_tool import AnalyzeTransactionPatternsTool
        
        # Register tools
        self.register_tool(TransactionQueryTool(self.data_service))
        self.register_tool(CustomerQueryTool(self.data_service))
        self.register_tool(AnalyzeTransactionPatternsTool(llm_service, model))
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Transaction pattern analysis and anomaly detection",
            "Cash flow analysis and forecasting",
            "Spending behavior categorization",
            "Fraud detection and risk identification",
            "Customer journey mapping through transactions",
            "Payment channel analysis and optimization",
            "Real-time transaction monitoring insights",
            "Merchant and category analytics"
        ]
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """Determine if this agent can handle transaction-related queries"""
        
        transaction_keywords = [
            "transaction", "payment", "transfer", "spending", "purchase",
            "cash flow", "money flow", "expense", "income", "debit", "credit",
            "fraud", "suspicious", "unusual activity", "anomaly",
            "pattern", "behavior", "merchant", "category", "channel"
        ]
        
        query_lower = query.lower()
        
        # Check for keyword matches
        keyword_matches = sum(1 for keyword in transaction_keywords if keyword in query_lower)
        
        if keyword_matches >= 2:
            return True, 0.9
        elif keyword_matches == 1:
            return True, 0.7
        
        # Check for specific patterns
        if any(phrase in query_lower for phrase in ["how much spent", "where is money going", "payment patterns"]):
            return True, 0.85
        
        return False, 0.0
    
    def get_system_prompt(self) -> str:
        return """You are a specialized transaction analytics AI assistant with access to real banking transaction data. Your expertise includes:
- Real-time transaction pattern analysis using actual payment data
- Fraud detection based on behavioral anomalies
- Cash flow analysis from transaction history
- Spending categorization and merchant analysis
- Payment channel optimization (digital, ATM, branch)
- Customer behavior insights from transaction patterns
- Regulatory compliance for transaction monitoring

When answering questions:
1. Always use real transaction data from database queries
2. Provide specific metrics on volumes, amounts, and patterns
3. Identify anomalies and suspicious patterns
4. Base behavioral insights on actual transaction history
5. Consider privacy - aggregate data when discussing patterns
6. Highlight actionable insights for fraud prevention and customer engagement

You have access to transaction, customer, deposit, and loan data. Use this to provide accurate, data-driven transaction insights."""
    
    def create_plan(
        self,
        query: str,
        llm_service: LLMInterface,
        model: str,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create an execution plan for transaction-related queries"""
        
        planning_prompt = f"""Create an execution plan to answer this transaction-related query: "{query}"

Available tools:
1. TransactionQuery - Requires: query_type (string), filters (dict, optional), time_period (dict, optional), group_by (list, optional), limit (number, optional)
2. AnalyzeTransactionPatterns - Requires: transaction_data (dict), analysis_type (string, optional), customer_context (dict, optional)
3. CustomerQuery - Requires: query_type (string), filters (dict, optional), time_period (dict, optional)

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

IMPORTANT: When using AnalyzeTransactionPatterns as the final step, set output_key to "analysis"

Focus on getting transaction data to provide accurate insights.

Respond with ONLY valid JSON."""

        messages = [
            {"role": "system", "content": "You are a transaction analysis planning expert. Create detailed execution plans."},
            {"role": "user", "content": planning_prompt}
        ]
        
        if conversation_history:
            context = "Previous conversation context:\n"
            for msg in conversation_history[-3:]:
                context += f"{msg['role']}: {msg['content'][:100]}...\n"
            messages[0]["content"] += f"\n\n{context}"
        
        try:
            response = llm_service.complete(messages, model=model, temperature=0.1)
            
            try:
                plan = json.loads(response)
            except json.JSONDecodeError:
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    plan = json.loads(json_match.group())
                else:
                    plan = self._create_default_plan(query)
            
            # Validate plan structure
            if not isinstance(plan, dict):
                return self._create_default_plan(query)
            
            if "goal" not in plan:
                plan["goal"] = f"Analyze transactions for: {query}"
            if "steps" not in plan or not isinstance(plan["steps"], list) or not plan["steps"]:
                return self._create_default_plan(query)
            
            # Ensure adaptations is a dictionary
            if "adaptations" not in plan or not isinstance(plan["adaptations"], dict):
                plan["adaptations"] = {
                    "error": "Provide general transaction insights based on available data",
                    "no_data": "Explain what transaction data would be needed for this analysis"
                }
            
            return plan
            
        except Exception:
            return self._create_default_plan(query)
    
    def _create_default_plan(self, query: str) -> Dict[str, Any]:
        """Create a default plan when automatic planning fails"""
        
        query_lower = query.lower()
        
        if any(word in query_lower for word in ["fraud", "suspicious", "unusual", "anomaly"]):
            # Fraud detection plan
            return {
                "goal": f"Detect fraudulent or suspicious transactions for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "TransactionQuery",
                        "description": "Get recent transaction anomalies",
                        "inputs": {
                            "query_type": "anomaly_detection",
                            "time_period": {"start": "date('now', '-7 days')"},
                            "limit": 50
                        },
                        "output_key": "anomaly_data"
                    },
                    {
                        "step": 2,
                        "tool": "TransactionQuery",
                        "description": "Get transaction patterns for context",
                        "inputs": {
                            "query_type": "pattern_detection",
                            "time_period": {"start": "date('now', '-30 days')"}
                        },
                        "output_key": "pattern_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeTransactionPatterns",
                        "description": "Analyze suspicious patterns and provide risk assessment",
                        "inputs": {
                            "transaction_data": {
                                "anomalies": "${anomaly_data}",
                                "patterns": "${pattern_data}"
                            },
                            "analysis_type": "fraud_detection"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_anomalies": "Explain fraud detection best practices",
                    "error": "Provide general fraud prevention guidance"
                }
            }
        
        elif any(word in query_lower for word in ["cash flow", "money flow", "income", "expense"]):
            # Cash flow analysis plan
            return {
                "goal": f"Analyze cash flow patterns for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "TransactionQuery",
                        "description": "Get cash flow data",
                        "inputs": {
                            "query_type": "cash_flow",
                            "time_period": {"start": "date('now', '-90 days')"}
                        },
                        "output_key": "cashflow_data"
                    },
                    {
                        "step": 2,
                        "tool": "TransactionQuery",
                        "description": "Get spending by category",
                        "inputs": {
                            "query_type": "category_breakdown",
                            "time_period": {"start": "date('now', '-30 days')"}
                        },
                        "output_key": "category_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeTransactionPatterns",
                        "description": "Analyze cash flow patterns and trends",
                        "inputs": {
                            "transaction_data": {
                                "cashflow": "${cashflow_data}",
                                "categories": "${category_data}"
                            },
                            "analysis_type": "spending_analysis"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "insufficient_data": "Provide cash flow management tips",
                    "error": "Offer general financial planning guidance"
                }
            }
        
        elif any(word in query_lower for word in ["pattern", "behavior", "trend", "habit"]):
            # Behavioral pattern analysis plan
            return {
                "goal": f"Analyze transaction patterns and behaviors for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "TransactionQuery",
                        "description": "Get transaction patterns",
                        "inputs": {
                            "query_type": "pattern_detection",
                            "time_period": {"start": "date('now', '-60 days')"}
                        },
                        "output_key": "pattern_data"
                    },
                    {
                        "step": 2,
                        "tool": "CustomerQuery",
                        "description": "Get customer segment context",
                        "inputs": {
                            "query_type": "demographics",
                            "group_by": ["segment"]
                        },
                        "output_key": "customer_context"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeTransactionPatterns",
                        "description": "Analyze behavioral patterns with customer context",
                        "inputs": {
                            "transaction_data": "${pattern_data}",
                            "analysis_type": "behavioral_insights",
                            "customer_context": "${customer_context}"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_patterns": "Explain typical customer behaviors",
                    "error": "Provide general behavioral insights"
                }
            }
        
        else:
            # General transaction analysis plan
            return {
                "goal": f"Provide transaction insights for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "TransactionQuery",
                        "description": "Get transaction volume and trends",
                        "inputs": {
                            "query_type": "volume_analysis",
                            "time_period": {"start": "date('now', '-30 days')"}
                        },
                        "output_key": "volume_data"
                    },
                    {
                        "step": 2,
                        "tool": "TransactionQuery",
                        "description": "Get transaction patterns",
                        "inputs": {
                            "query_type": "pattern_detection",
                            "time_period": {"start": "date('now', '-30 days')"}
                        },
                        "output_key": "pattern_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeTransactionPatterns",
                        "description": "Provide comprehensive transaction analysis",
                        "inputs": {
                            "transaction_data": {
                                "volume": "${volume_data}",
                                "patterns": "${pattern_data}"
                            },
                            "analysis_type": "comprehensive"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Provide general transaction insights",
                    "error": "Offer alternative analysis approaches"
                }
            }
    
    def _identify_used_capabilities(self, query: str) -> List[str]:
        """Identify which capabilities might be used for this query"""
        used = []
        query_lower = query.lower()
        
        capability_keywords = {
            "Transaction pattern analysis and anomaly detection": ["pattern", "anomaly", "unusual", "detect"],
            "Cash flow analysis and forecasting": ["cash flow", "income", "expense", "forecast"],
            "Spending behavior categorization": ["spending", "category", "merchant", "expense"],
            "Fraud detection and risk identification": ["fraud", "suspicious", "risk", "security"],
            "Customer journey mapping through transactions": ["journey", "behavior", "path", "lifecycle"],
            "Payment channel analysis and optimization": ["channel", "digital", "atm", "branch"],
            "Real-time transaction monitoring insights": ["real-time", "monitor", "alert", "live"],
            "Merchant and category analytics": ["merchant", "vendor", "category", "shop"]
        }
        
        for capability, keywords in capability_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                used.append(capability)
        
        # If no specific capabilities identified, use general
        if not used:
            used.append("Transaction pattern analysis and anomaly detection")
        
        return used