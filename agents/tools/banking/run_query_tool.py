from typing import Dict, Any
from agents.tools.base_tool import BaseTool
from services.llm_interface import LLMInterface
import json
import random
from datetime import datetime, timedelta


class RunQueryTool(BaseTool):
    """Tool for executing queries and returning mock data"""
    
    def __init__(self, llm_service: LLMInterface, model: str):
        super().__init__(
            name="RunQuery",
            description="Execute structured queries and return data"
        )
        self.llm_service = llm_service
        self.model = model
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "query": {
                "type": "dict",
                "description": "Structured query to execute"
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Execute a query and return mock data"""
        query = kwargs.get("query", {})
        
        try:
            entity = query.get("entity", "data")
            filters = query.get("filters", {})
            metrics = query.get("metrics", ["count"])
            aggregations = query.get("aggregations", [])
            
            # Extract time period if available
            time_period = query.get("time_period", {})
            time_label = "Current Period"
            if isinstance(time_period, dict):
                if "label" in time_period:
                    time_label = time_period["label"]
                elif "quarter" in time_period and "year" in time_period:
                    time_label = f"{time_period['quarter']} {time_period['year']}"
            
            # Use LLM to generate realistic mock data based on the query
            prompt = f"""Generate realistic mock banking data for this query:

Entity: {entity}
Time Period: {time_label}
Filters: {json.dumps(filters)}
Metrics: {metrics}
Aggregations: {aggregations}

Generate a JSON response with:
1. summary_stats: Key metrics as requested for {time_label}
2. data_points: 5-10 sample records
3. trends: Any time-based patterns if requested
4. breakdowns: Any categorical breakdowns if requested
5. period_label: Include "{time_label}" to identify the data period

IMPORTANT: If this is Q3 2024 data, make values about 8-12% lower than Q3 2025 to show growth.
If this is Q3 2025 data, show stronger performance metrics.

Make the data realistic for a mid-size bank. Respond with ONLY valid JSON."""

            messages = [
                {"role": "system", "content": "You are a banking data simulator. Generate realistic mock data."},
                {"role": "user", "content": prompt}
            ]
            
            response = self.llm_service.complete(messages, model=self.model, temperature=0.3)
            
            # Parse the response
            try:
                mock_data = json.loads(response)
            except json.JSONDecodeError:
                # Generate fallback data based on entity type
                mock_data = self._generate_fallback_data(entity, metrics, filters)
            
            # Ensure we have all expected sections
            if "summary_stats" not in mock_data:
                mock_data["summary_stats"] = self._generate_summary_stats(entity, metrics)
            
            # Add metadata
            result = {
                "query_executed": query,
                "row_count": len(mock_data.get("data_points", [])),
                "execution_time": f"{random.uniform(0.1, 0.5):.3f}s",
                "data": mock_data
            }
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "result": {
                    "query_executed": query,
                    "data": self._generate_fallback_data(query.get("entity", "data"), ["count"], {})
                }
            }
    
    def _generate_fallback_data(self, entity: str, metrics: list, filters: dict) -> Dict[str, Any]:
        """Generate fallback mock data when LLM fails"""
        
        if entity.lower() in ["loan", "loans"]:
            return {
                "summary_stats": {
                    "total_loans": 15000,
                    "total_value": 3500000000,
                    "average_loan_size": 233333,
                    "default_rate": 2.1,
                    "average_interest_rate": 5.25
                },
                "data_points": [
                    {
                        "loan_id": f"L{i:05d}",
                        "type": random.choice(["mortgage", "auto", "personal"]),
                        "amount": random.randint(10000, 500000),
                        "interest_rate": round(random.uniform(3.0, 8.0), 2),
                        "status": random.choice(["current", "current", "current", "late", "default"])
                    }
                    for i in range(5)
                ],
                "trends": {
                    "monthly_originations": [
                        {"month": f"2024-{i:02d}", "count": random.randint(1000, 1500)}
                        for i in range(1, 7)
                    ]
                }
            }
        
        elif entity.lower() in ["deposit", "deposits"]:
            return {
                "summary_stats": {
                    "total_accounts": 45000,
                    "total_deposits": 1200000000,
                    "average_balance": 26667,
                    "accounts_opened_mtd": 450
                },
                "data_points": [
                    {
                        "account_id": f"A{i:05d}",
                        "type": random.choice(["checking", "savings", "cd"]),
                        "balance": random.randint(1000, 100000),
                        "opened_date": (datetime.now() - timedelta(days=random.randint(0, 1000))).strftime("%Y-%m-%d")
                    }
                    for i in range(5)
                ]
            }
        
        else:  # customer
            return {
                "summary_stats": {
                    "total_customers": 35000,
                    "active_customers": 31000,
                    "average_products_per_customer": 2.4,
                    "nps_score": 42
                },
                "data_points": [
                    {
                        "customer_id": f"C{i:05d}",
                        "segment": random.choice(["high_value", "growth", "maintain", "at_risk"]),
                        "products_count": random.randint(1, 5),
                        "tenure_years": random.randint(0, 20)
                    }
                    for i in range(5)
                ]
            }
    
    def _generate_summary_stats(self, entity: str, metrics: list) -> Dict[str, Any]:
        """Generate summary statistics based on entity and metrics"""
        stats = {}
        
        if "count" in metrics:
            stats["total_count"] = random.randint(10000, 50000)
        if "sum" in metrics:
            stats["total_value"] = random.randint(1000000, 5000000000)
        if "average" in metrics:
            stats["average_value"] = random.randint(10000, 100000)
        if "default_rate" in metrics:
            stats["default_rate"] = round(random.uniform(1.0, 3.0), 2)
        
        return stats