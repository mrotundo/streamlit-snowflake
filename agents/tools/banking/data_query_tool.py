from typing import Dict, Any, List
from agents.tools.base_tool import BaseTool
import pandas as pd
import json
from datetime import datetime, timedelta


class DataQueryTool(BaseTool):
    """Tool for querying banking data (simulated for demo)"""
    
    def __init__(self):
        super().__init__(
            name="DataQueryTool",
            description="Execute queries against banking data warehouse"
        )
        # In production, this would connect to actual database
        self._init_sample_data()
    
    def _init_sample_data(self):
        """Initialize sample banking data for demonstration"""
        # Sample loan data
        self.loan_data = pd.DataFrame({
            'loan_id': range(1000, 1010),
            'customer_id': [f'C{i:04d}' for i in range(100, 110)],
            'loan_type': ['mortgage', 'personal', 'auto', 'mortgage', 'business', 
                         'personal', 'mortgage', 'auto', 'personal', 'mortgage'],
            'amount': [250000, 15000, 35000, 450000, 100000, 
                      25000, 320000, 42000, 18000, 180000],
            'interest_rate': [4.5, 8.2, 5.9, 4.2, 6.5, 7.8, 4.8, 6.2, 8.5, 5.1],
            'status': ['current', 'current', 'paid', 'current', 'default',
                      'current', 'current', 'current', 'late', 'current'],
            'origination_date': pd.date_range(end=datetime.now(), periods=10, freq='M')
        })
        
        # Sample deposit data
        self.deposit_data = pd.DataFrame({
            'account_id': range(2000, 2015),
            'customer_id': [f'C{i:04d}' for i in range(100, 115)],
            'account_type': ['checking', 'savings', 'checking', 'cd', 'savings',
                           'checking', 'money_market', 'savings', 'checking', 'cd',
                           'savings', 'checking', 'savings', 'cd', 'checking'],
            'balance': [5000, 25000, 1200, 50000, 8500,
                       3200, 75000, 12000, 800, 100000,
                       4500, 2100, 15000, 30000, 6700],
            'opened_date': pd.date_range(end=datetime.now(), periods=15, freq='2M')
        })
        
        # Sample customer data
        self.customer_data = pd.DataFrame({
            'customer_id': [f'C{i:04d}' for i in range(100, 120)],
            'age': [25, 34, 45, 52, 28, 61, 39, 47, 33, 55,
                    42, 29, 38, 51, 46, 35, 58, 31, 44, 27],
            'segment': ['growth', 'high_value', 'maintain', 'high_value', 'growth',
                       'maintain', 'high_value', 'maintain', 'growth', 'high_value',
                       'maintain', 'at_risk', 'growth', 'high_value', 'maintain',
                       'growth', 'maintain', 'at_risk', 'high_value', 'growth'],
            'products_count': [2, 4, 3, 5, 1, 3, 4, 2, 2, 6,
                             3, 1, 2, 5, 3, 2, 3, 1, 4, 2],
            'lifetime_value': [5000, 25000, 12000, 45000, 3000,
                             15000, 35000, 8000, 7000, 65000,
                             11000, 2000, 6000, 38000, 14000,
                             9000, 16000, 1500, 28000, 4000]
        })
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "query_type": {
                "type": "string",
                "description": "Type of query: loan_portfolio, deposit_summary, customer_segment, etc."
            },
            "filters": {
                "type": "dict",
                "description": "Optional filters to apply",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Execute a data query based on the type"""
        query_type = kwargs.get("query_type", "").lower()
        filters = kwargs.get("filters", {})
        
        try:
            if "loan" in query_type:
                result = self._query_loan_data(filters)
            elif "deposit" in query_type:
                result = self._query_deposit_data(filters)
            elif "customer" in query_type:
                result = self._query_customer_data(filters)
            else:
                return {
                    "success": False,
                    "error": f"Unknown query type: {query_type}"
                }
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def _query_loan_data(self, filters: Dict) -> Dict[str, Any]:
        """Query loan portfolio data"""
        df = self.loan_data.copy()
        
        # Apply filters if any
        if filters:
            if 'loan_type' in filters:
                df = df[df['loan_type'] == filters['loan_type']]
            if 'status' in filters:
                df = df[df['status'] == filters['status']]
        
        # Calculate summary statistics
        summary = {
            "total_loans": len(df),
            "total_value": f"${df['amount'].sum():,.0f}",
            "average_loan_size": f"${df['amount'].mean():,.0f}",
            "average_interest_rate": f"{df['interest_rate'].mean():.2f}%",
            "by_type": df.groupby('loan_type')['amount'].agg(['count', 'sum']).to_dict('index'),
            "by_status": df['status'].value_counts().to_dict(),
            "default_rate": f"{(df['status'] == 'default').mean() * 100:.1f}%"
        }
        
        return {
            "summary": summary,
            "data": df.to_dict('records')[:10]  # Return sample records
        }
    
    def _query_deposit_data(self, filters: Dict) -> Dict[str, Any]:
        """Query deposit account data"""
        df = self.deposit_data.copy()
        
        # Apply filters
        if filters:
            if 'account_type' in filters:
                df = df[df['account_type'] == filters['account_type']]
        
        # Calculate summary
        summary = {
            "total_accounts": len(df),
            "total_deposits": f"${df['balance'].sum():,.0f}",
            "average_balance": f"${df['balance'].mean():,.0f}",
            "by_type": df.groupby('account_type')['balance'].agg(['count', 'sum']).to_dict('index'),
            "balance_distribution": {
                "under_5k": len(df[df['balance'] < 5000]),
                "5k_to_25k": len(df[(df['balance'] >= 5000) & (df['balance'] < 25000)]),
                "25k_to_100k": len(df[(df['balance'] >= 25000) & (df['balance'] < 100000)]),
                "over_100k": len(df[df['balance'] >= 100000])
            }
        }
        
        return {
            "summary": summary,
            "data": df.to_dict('records')[:10]
        }
    
    def _query_customer_data(self, filters: Dict) -> Dict[str, Any]:
        """Query customer segmentation data"""
        df = self.customer_data.copy()
        
        # Apply filters
        if filters:
            if 'segment' in filters:
                df = df[df['segment'] == filters['segment']]
        
        # Calculate summary
        summary = {
            "total_customers": len(df),
            "average_age": f"{df['age'].mean():.1f}",
            "average_products": f"{df['products_count'].mean():.1f}",
            "average_clv": f"${df['lifetime_value'].mean():,.0f}",
            "by_segment": df['segment'].value_counts().to_dict(),
            "high_value_customers": len(df[df['lifetime_value'] > 20000]),
            "at_risk_percentage": f"{(df['segment'] == 'at_risk').mean() * 100:.1f}%"
        }
        
        return {
            "summary": summary,
            "data": df.to_dict('records')[:10]
        }