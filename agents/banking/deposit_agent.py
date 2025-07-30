from typing import List, Dict, Any, Optional
from agents.base_agent import BaseAgent
from services.llm_interface import LLMInterface
import json


class DepositAgent(BaseAgent):
    """Agent specialized in deposit accounts, transactions, and balance management"""
    
    def __init__(self):
        super().__init__(
            name="DepositAgent",
            description="Handles deposit accounts, transactions, balances, and savings analysis"
        )
    
    @property
    def capabilities(self) -> List[str]:
        return [
            "Account balance inquiries",
            "Transaction history analysis",
            "Deposit growth trends",
            "Savings account optimization",
            "CD (Certificate of Deposit) rates",
            "Account type comparisons",
            "Interest earnings calculations",
            "Deposit insurance information",
            "Cash flow analysis"
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
        return """You are a specialized banking deposit accounts AI assistant. Your expertise includes:
- Deposit account management (checking, savings, CDs, money market)
- Transaction analysis and patterns
- Balance optimization strategies
- Interest rate maximization
- Account fee structures and minimization
- FDIC insurance and regulations
- Cash management best practices

When answering questions:
1. Provide clear explanations of account features and benefits
2. Calculate interest earnings when relevant
3. Suggest account optimization strategies
4. Consider regulatory requirements (FDIC limits, etc.)
5. Help identify unusual transaction patterns

Always maintain professional banking standards and help customers maximize their deposit value."""
    
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
            
            if "goal" not in plan:
                plan["goal"] = f"Answer deposit query: {query}"
            if "steps" not in plan or not plan["steps"]:
                plan = self._create_default_plan(query)
            
            return plan
            
        except Exception:
            return self._create_default_plan(query)
    
    def _create_default_plan(self, query: str) -> Dict[str, Any]:
        """Create a default plan when automatic planning fails"""
        
        query_lower = query.lower()
        
        if any(word in query_lower for word in ["balance", "account", "total"]):
            # Balance inquiry plan
            return {
                "goal": f"Get account balance information for: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "SynthesizeQuery",
                        "description": "Create query for account balance data",
                        "inputs": {
                            "requirements": query,
                            "query_type": "deposit"
                        },
                        "output_key": "balance_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get account balance data",
                        "inputs": {
                            "query": "${balance_query.query}"
                        },
                        "output_key": "balance_data"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Analyze and summarize account balances",
                        "inputs": {
                            "data": "${balance_data}",
                            "question": query,
                            "analysis_type": "balance_summary"
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
                        "tool": "SynthesizeQuery",
                        "description": "Create query for transaction data",
                        "inputs": {
                            "requirements": f"Get transaction history for: {query}",
                            "query_type": "deposit"
                        },
                        "output_key": "transaction_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get transaction data",
                        "inputs": {
                            "query": "${transaction_query.query}"
                        },
                        "output_key": "transaction_data"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Analyze transaction patterns",
                        "inputs": {
                            "data": "${transaction_data}",
                            "question": query,
                            "analysis_type": "transaction_analysis"
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
                        "tool": "SynthesizeQuery",
                        "description": "Create query for deposit growth data",
                        "inputs": {
                            "requirements": f"Get deposit growth and trend data for: {query}",
                            "query_type": "deposit"
                        },
                        "output_key": "growth_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get deposit growth data",
                        "inputs": {
                            "query": "${growth_query.query}"
                        },
                        "output_key": "growth_data"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Analyze growth trends and patterns",
                        "inputs": {
                            "data": "${growth_data}",
                            "question": query,
                            "analysis_type": "trend_analysis"
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
            # General deposit query plan
            return {
                "goal": f"Answer deposit question: {query}",
                "steps": [
                    {
                        "step": 1,
                        "tool": "SynthesizeQuery",
                        "description": "Understand deposit data needs",
                        "inputs": {
                            "requirements": query,
                            "query_type": "deposit"
                        },
                        "output_key": "data_query"
                    },
                    {
                        "step": 2,
                        "tool": "RunQuery",
                        "description": "Get relevant deposit data",
                        "inputs": {
                            "query": "${data_query.query}"
                        },
                        "output_key": "deposit_info"
                    },
                    {
                        "step": 3,
                        "tool": "ProvideAnalysis",
                        "description": "Provide comprehensive answer",
                        "inputs": {
                            "data": "${deposit_info}",
                            "question": query,
                            "analysis_type": "general"
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