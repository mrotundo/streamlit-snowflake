from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
from services.data_interface import DataInterface
import json


class DepositAnalyticsAgent(BaseAgent):
    """Agent specialized in deposit analytics using real database data"""
    
    def __init__(self, data_service: DataInterface):
        super().__init__(
            name="DepositAnalyticsAgent",
            description="Performs deposit analytics, liquidity analysis, and growth trends using real data"
        )
        self.data_service = data_service
    
    def _initialize_tools(self, llm_service: LLMInterface, model: str):
        """Initialize deposit analytics specific tools"""
        # Clear existing tools first
        self._tools = []
        self.plan_executor.tools_registry = {}
        from agents.tools.banking.deposit_query_tool import DepositQueryTool
        from agents.tools.banking.transaction_query_tool import TransactionQueryTool
        from agents.tools.banking.analyze_deposit_trends_tool import AnalyzeDepositTrendsTool
        
        # Register tools
        self.register_tool(DepositQueryTool(self.data_service))
        self.register_tool(TransactionQueryTool(self.data_service))
        self.register_tool(AnalyzeDepositTrendsTool(llm_service, model))
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Deposit portfolio analysis with real-time data",
            "Balance distribution and concentration analysis",
            "Deposit growth trends and forecasting",
            "Liquidity risk assessment",
            "Interest rate sensitivity analysis",
            "Account activity and engagement metrics",
            "Deposit stability and duration analysis",
            "Competitive positioning and pricing strategy"
        ]
    
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """Determine if this agent can handle deposit-related queries"""
        
        deposit_keywords = [
            "deposit", "saving", "account", "balance", "transaction",
            "checking", "withdrawal", "transfer", "interest", "cd",
            "certificate", "atm", "branch", "statement", "overdraft",
            "minimum balance", "monthly fee", "direct deposit"
        ]
        
        query_lower = query.lower()
        
        # Check for keyword matches
        keyword_matches = sum(1 for keyword in deposit_keywords if keyword in query_lower)
        
        if keyword_matches >= 2:
            return True, 0.9
        elif keyword_matches == 1:
            return True, 0.6
        
        # Use LLM for more nuanced classification
        classification_prompt = f"""Determine if this query is related to deposit accounts, savings, checking, or account management.
Query: "{query}"

Respond with ONLY a number between 0 and 1 indicating confidence that this is deposit-related."""
        
        try:
            messages = [{"role": "user", "content": classification_prompt}]
            response = llm_service.complete(messages, model=model, temperature=0.1, max_tokens=10)
            confidence = float(response.strip())
            return confidence > 0.5, confidence
        except:
            return False, 0.0
    
    def get_system_prompt(self) -> str:
        return """You are a specialized deposit analytics AI assistant with access to real banking data. Your expertise includes:
- Real-time deposit portfolio analysis using actual account data
- Liquidity management based on deposit composition
- Growth trend analysis from historical deposit flows
- Interest rate optimization using market data
- Deposit stability assessment from behavioral patterns
- FDIC insurance coverage and regulatory compliance
- Competitive analysis and pricing strategies

When answering questions:
1. Always use real data from database queries
2. Provide specific metrics on balances, rates, and growth
3. Base liquidity assessments on actual deposit composition
4. Compare current trends to historical patterns
5. Consider regulatory requirements (LCR, NSFR, etc.)
6. Highlight actionable insights for deposit growth

You have access to deposit, customer, and transaction data. Use this to provide accurate, data-driven deposit insights."""
    
    def create_plan(
        self,
        query: str,
        llm_service: LLMInterface,
        model: str,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create an execution plan for deposit-related queries"""
        
        planning_prompt = f"""Create an execution plan to answer this deposit-related query: "{query}"

Available tools:
1. DepositQuery - Requires: query_type (string), filters (dict, optional), time_period (dict, optional), comparison_period (dict, optional), group_by (list, optional)
2. AnalyzeDepositTrends - Requires: deposit_data (dict), analysis_focus (string, optional), market_data (dict, optional)
3. TransactionQuery - Requires: query_type (string), filters (dict, optional), time_period (dict, optional)

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

IMPORTANT: When using AnalyzeDepositTrends as the final step, set output_key to "analysis"

Focus on getting deposit account data to provide accurate insights.

Respond with ONLY valid JSON."""

        messages = [
            {"role": "system", "content": "You are a deposit analysis planning expert. Create detailed execution plans."},
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
                plan["goal"] = f"Answer deposit query: {query}"
            if "steps" not in plan or not isinstance(plan["steps"], list) or not plan["steps"]:
                return self._create_default_plan(query)
            
            # Ensure adaptations is a dictionary
            if "adaptations" not in plan or not isinstance(plan["adaptations"], dict):
                plan["adaptations"] = {
                    "error": "Provide general deposit insights based on available data",
                    "no_data": "Explain what deposit data would be needed for this analysis"
                }
            
            return plan
            
        except Exception:
            return self._create_default_plan(query)
    
    def _create_default_plan(self, query: str) -> Dict[str, Any]:
        """Create a default plan when automatic planning fails"""
        
        query_lower = query.lower()
        
        # For simple total queries, still include analysis
        if any(word in query_lower for word in ["total deposit", "total balance", "how much deposit"]):
            # Balance inquiry plan
            return {
                "goal": f"Get account balance information for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "DepositQuery",
                        "description": "Get account balance and summary data",
                        "inputs": {
                            "query_type": "account_summary",
                            "filters": {}
                        },
                        "output_key": "balance_data"
                    },
                    {
                        "step": 2,
                        "tool": "DepositQuery",
                        "description": "Get balance distribution analysis",
                        "inputs": {
                            "query_type": "balance_distribution"
                        },
                        "output_key": "distribution_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeDepositTrends",
                        "description": "Analyze balance trends and provide insights",
                        "inputs": {
                            "deposit_data": {
                                "summary": "${balance_data}",
                                "distribution": "${distribution_data}"
                            },
                            "analysis_focus": "comprehensive"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Explain how to check account balances",
                    "error": "Provide general balance inquiry guidance"
                }
            }
        
        elif any(word in query_lower for word in ["transaction", "activity", "history"]):
            # Transaction analysis plan
            return {
                "goal": f"Analyze transaction data for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "TransactionQuery",
                        "description": "Get transaction volume and patterns",
                        "inputs": {
                            "query_type": "volume_analysis",
                            "time_period": {"start": "date('now', '-30 days')"}
                        },
                        "output_key": "transaction_data"
                    },
                    {
                        "step": 2,
                        "tool": "DepositQuery",
                        "description": "Get account activity metrics",
                        "inputs": {
                            "query_type": "account_activity",
                            "limit": 100
                        },
                        "output_key": "activity_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeDepositTrends",
                        "description": "Analyze transaction patterns and account activity",
                        "inputs": {
                            "deposit_data": {
                                "transactions": "${transaction_data}",
                                "activity": "${activity_data}"
                            },
                            "analysis_focus": "stability_assessment"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Explain transaction tracking best practices",
                    "error": "Provide general transaction insights"
                }
            }
        
        elif any(word in query_lower for word in ["growth", "trend", "savings"]):
            # Deposit growth analysis plan
            return {
                "goal": f"Analyze deposit growth trends for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "DepositQuery",
                        "description": "Get deposit growth trends",
                        "inputs": {
                            "query_type": "growth_trends",
                            "time_period": {"start": "date('now', '-12 months')"}
                        },
                        "output_key": "growth_data"
                    },
                    {
                        "step": 2,
                        "tool": "DepositQuery",
                        "description": "Get comparison period data",
                        "inputs": {
                            "query_type": "growth_trends",
                            "comparison_period": {"start": "date('now', '-24 months')", "end": "date('now', '-12 months')"}
                        },
                        "output_key": "comparison_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeDepositTrends",
                        "description": "Analyze growth trends and forecast",
                        "inputs": {
                            "deposit_data": "${growth_data}",
                            "analysis_focus": "growth_analysis",
                            "market_data": "${comparison_data}"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Provide savings growth strategies",
                    "error": "Offer general deposit growth insights"
                }
            }
        
        else:
            # General deposit analytics plan
            return {
                "goal": f"Provide deposit analytics insights for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "DepositQuery",
                        "description": "Get comprehensive deposit data",
                        "inputs": {
                            "query_type": "account_summary"
                        },
                        "output_key": "deposit_summary"
                    },
                    {
                        "step": 2,
                        "tool": "DepositQuery",
                        "description": "Get liquidity and stability metrics",
                        "inputs": {
                            "query_type": "liquidity_analysis"
                        },
                        "output_key": "liquidity_data"
                    },
                    {
                        "step": 3,
                        "tool": "AnalyzeDepositTrends",
                        "description": "Provide comprehensive deposit analysis",
                        "inputs": {
                            "deposit_data": {
                                "summary": "${deposit_summary}",
                                "liquidity": "${liquidity_data}"
                            },
                            "analysis_focus": "comprehensive"
                        },
                        "output_key": "analysis"
                    }
                ],
                "adaptations": {
                    "no_data": "Provide general deposit guidance",
                    "error": "Offer alternative information sources"
                }
            }
    
    def _identify_used_capabilities(self, query: str) -> List[str]:
        """Identify which capabilities might be used for this query"""
        used = []
        query_lower = query.lower()
        
        capability_keywords = {
            "Account balance inquiries": ["balance", "how much", "account total"],
            "Transaction history analysis": ["transaction", "history", "activity"],
            "Deposit growth trends": ["growth", "trend", "increase"],
            "Interest earnings calculations": ["interest", "earnings", "yield"],
            "Cash flow analysis": ["cash flow", "inflow", "outflow"]
        }
        
        for capability, keywords in capability_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                used.append(capability)
        
        return used