from typing import Dict, Any, Optional, List
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
import pandas as pd
from datetime import datetime


class CustomerQueryTool(BaseTool):
    """Tool for executing customer analytics queries against the database"""
    
    def __init__(self, data_service: DataInterface):
        super().__init__(
            name="CustomerQuery",
            description="Execute customer analytics queries with real database data"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "query_type": {
                "type": "string",
                "description": "Type of customer query: segmentation, lifetime_value, churn_risk, demographics, product_adoption"
            },
            "filters": {
                "type": "dict",
                "description": "Optional filters (segment, credit_score_range, join_date_range, etc.)",
                "optional": True
            },
            "time_period": {
                "type": "dict",
                "description": "Time period for analysis (start, end dates)",
                "optional": True
            },
            "group_by": {
                "type": "list",
                "description": "Fields to group by for aggregation",
                "optional": True
            },
            "limit": {
                "type": "number",
                "description": "Limit number of results",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Execute customer analytics query"""
        query_type = kwargs.get("query_type", "")
        filters = kwargs.get("filters", {})
        time_period = kwargs.get("time_period", {})
        group_by = kwargs.get("group_by", [])
        limit = kwargs.get("limit", 100)
        
        try:
            # Ensure we're connected
            if not self.data_service.validate_connection():
                self.data_service.connect()
            
            # Build and execute query based on type
            if query_type == "segmentation":
                result = self._query_customer_segments(filters, group_by)
            elif query_type == "lifetime_value":
                result = self._query_customer_lifetime_value(filters, limit)
            elif query_type == "churn_risk":
                result = self._query_churn_risk(filters, limit)
            elif query_type == "demographics":
                result = self._query_demographics(filters, group_by)
            elif query_type == "product_adoption":
                result = self._query_product_adoption(filters, time_period)
            else:
                # General customer query
                result = self._query_general_customers(filters, limit)
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "result": {}
            }
    
    def _query_customer_segments(self, filters: Dict, group_by: List[str]) -> Dict[str, Any]:
        """Query customer segmentation data"""
        sql = """
            SELECT 
                segment,
                COUNT(*) as customer_count,
                AVG(credit_score) as avg_credit_score,
                AVG(annual_income) as avg_annual_income,
                AVG(total_relationship_value) as avg_relationship_value,
                SUM(products_count) as total_products,
                AVG(products_count) as avg_products_per_customer
            FROM customers
            WHERE status = 'active'
        """
        
        # Add filters
        if filters:
            if 'segment' in filters:
                sql += f" AND segment = :segment"
            if 'min_credit_score' in filters:
                sql += f" AND credit_score >= :min_credit_score"
        
        sql += " GROUP BY segment ORDER BY customer_count DESC"
        
        df = self.data_service.execute_query(sql, filters)
        
        # Get sample customers for each segment
        segments = df['segment'].tolist()
        segment_samples = {}
        
        for segment in segments[:3]:  # Top 3 segments
            sample_sql = """
                SELECT customer_id, name, credit_score, annual_income, products_count
                FROM customers 
                WHERE segment = :segment AND status = 'active'
                LIMIT 5
            """
            samples = self.data_service.execute_query(sample_sql, {'segment': segment})
            segment_samples[segment] = samples.to_dict('records')
        
        return {
            "summary": {
                "total_segments": len(df),
                "total_customers": int(df['customer_count'].sum()),
                "avg_products_overall": float(df['avg_products_per_customer'].mean())
            },
            "segments": df.to_dict('records'),
            "segment_samples": segment_samples
        }
    
    def _query_customer_lifetime_value(self, filters: Dict, limit: int) -> Dict[str, Any]:
        """Query customer lifetime value data"""
        sql = """
            SELECT 
                c.customer_id,
                c.name,
                c.segment,
                c.credit_score,
                c.join_date,
                COALESCE(cs.total_deposits, 0) as total_deposits,
                COALESCE(cs.total_loan_balance, 0) as total_loan_balance,
                COALESCE(cs.num_accounts, 0) as num_deposit_accounts,
                COALESCE(cs.num_loans, 0) as num_loans,
                COALESCE(cs.total_deposits, 0) + COALESCE(cs.total_loan_balance, 0) * 0.05 as estimated_clv
            FROM customers c
            LEFT JOIN v_customer_summary cs ON c.customer_id = cs.customer_id
            WHERE c.status = 'active'
        """
        
        # Add filters
        params = {}
        if filters:
            if 'segment' in filters:
                sql += " AND c.segment = :segment"
                params['segment'] = filters['segment']
            if 'min_clv' in filters:
                sql += " AND (COALESCE(cs.total_deposits, 0) + COALESCE(cs.total_loan_balance, 0) * 0.05) >= :min_clv"
                params['min_clv'] = filters['min_clv']
        
        sql += " ORDER BY estimated_clv DESC LIMIT :limit"
        params['limit'] = limit
        
        df = self.data_service.execute_query(sql, params)
        
        # Calculate CLV statistics
        clv_stats_sql = """
            SELECT 
                AVG(COALESCE(cs.total_deposits, 0) + COALESCE(cs.total_loan_balance, 0) * 0.05) as avg_clv,
                MAX(COALESCE(cs.total_deposits, 0) + COALESCE(cs.total_loan_balance, 0) * 0.05) as max_clv,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY COALESCE(cs.total_deposits, 0) + COALESCE(cs.total_loan_balance, 0) * 0.05) as median_clv
            FROM customers c
            LEFT JOIN v_customer_summary cs ON c.customer_id = cs.customer_id
            WHERE c.status = 'active'
        """
        
        # For SQLite, use different percentile calculation
        if 'sqlite' in str(type(self.data_service)).lower():
            clv_stats_sql = """
                SELECT 
                    AVG(COALESCE(cs.total_deposits, 0) + COALESCE(cs.total_loan_balance, 0) * 0.05) as avg_clv,
                    MAX(COALESCE(cs.total_deposits, 0) + COALESCE(cs.total_loan_balance, 0) * 0.05) as max_clv
                FROM customers c
                LEFT JOIN v_customer_summary cs ON c.customer_id = cs.customer_id
                WHERE c.status = 'active'
            """
        
        stats_df = self.data_service.execute_query(clv_stats_sql)
        
        return {
            "summary": stats_df.to_dict('records')[0] if not stats_df.empty else {},
            "top_customers": df.to_dict('records'),
            "clv_distribution": {
                "under_10k": len(df[df['estimated_clv'] < 10000]),
                "10k_to_50k": len(df[(df['estimated_clv'] >= 10000) & (df['estimated_clv'] < 50000)]),
                "50k_to_100k": len(df[(df['estimated_clv'] >= 50000) & (df['estimated_clv'] < 100000)]),
                "over_100k": len(df[df['estimated_clv'] >= 100000])
            }
        }
    
    def _query_churn_risk(self, filters: Dict, limit: int) -> Dict[str, Any]:
        """Query customers at risk of churning"""
        sql = """
            SELECT 
                c.customer_id,
                c.name,
                c.segment,
                c.credit_score,
                c.join_date,
                cp.total_products,
                cp.last_account_opened,
                cp.last_loan_originated,
                JULIANDAY('now') - JULIANDAY(COALESCE(cp.last_loan_originated, cp.last_account_opened, c.join_date)) as days_since_last_product,
                CASE 
                    WHEN c.segment = 'at_risk' THEN 0.8
                    WHEN JULIANDAY('now') - JULIANDAY(COALESCE(cp.last_loan_originated, cp.last_account_opened, c.join_date)) > 365 THEN 0.6
                    WHEN cp.total_products = 1 THEN 0.5
                    ELSE 0.2
                END as churn_risk_score
            FROM customers c
            LEFT JOIN v_customer_products cp ON c.customer_id = cp.customer_id
            WHERE c.status = 'active'
        """
        
        # Add filters
        params = {}
        if filters:
            if 'min_risk_score' in filters:
                sql = f"SELECT * FROM ({sql}) t WHERE churn_risk_score >= :min_risk_score"
                params['min_risk_score'] = filters['min_risk_score']
        
        sql += " ORDER BY churn_risk_score DESC LIMIT :limit"
        params['limit'] = limit
        
        df = self.data_service.execute_query(sql, params)
        
        # Calculate risk statistics
        risk_stats = {
            "high_risk_count": len(df[df['churn_risk_score'] >= 0.7]),
            "medium_risk_count": len(df[(df['churn_risk_score'] >= 0.4) & (df['churn_risk_score'] < 0.7)]),
            "low_risk_count": len(df[df['churn_risk_score'] < 0.4]),
            "avg_days_since_activity": float(df['days_since_last_product'].mean()) if not df.empty else 0
        }
        
        return {
            "summary": risk_stats,
            "at_risk_customers": df.to_dict('records'),
            "retention_recommendations": [
                "Offer targeted product bundles to single-product customers",
                "Re-engage customers with no activity in 6+ months",
                "Provide special offers to 'at_risk' segment customers"
            ]
        }
    
    def _query_demographics(self, filters: Dict, group_by: List[str]) -> Dict[str, Any]:
        """Query customer demographics"""
        # Default grouping by segment if not specified
        if not group_by:
            group_by = ['segment']
        
        group_fields = ', '.join(group_by)
        
        sql = f"""
            SELECT 
                {group_fields},
                COUNT(*) as customer_count,
                AVG(credit_score) as avg_credit_score,
                MIN(credit_score) as min_credit_score,
                MAX(credit_score) as max_credit_score,
                AVG(annual_income) as avg_income,
                AVG(JULIANDAY('now') - JULIANDAY(join_date)) / 365 as avg_tenure_years
            FROM customers
            WHERE status = 'active'
        """
        
        # Add filters
        params = {}
        if filters:
            if 'employment_status' in filters:
                sql += " AND employment_status = :employment_status"
                params['employment_status'] = filters['employment_status']
            if 'min_tenure_days' in filters:
                sql += " AND JULIANDAY('now') - JULIANDAY(join_date) >= :min_tenure_days"
                params['min_tenure_days'] = filters['min_tenure_days']
        
        sql += f" GROUP BY {group_fields} ORDER BY customer_count DESC"
        
        df = self.data_service.execute_query(sql, params)
        
        # Get overall statistics
        overall_sql = """
            SELECT 
                COUNT(*) as total_customers,
                AVG(credit_score) as avg_credit_score,
                AVG(annual_income) as avg_annual_income,
                AVG(JULIANDAY('now') - JULIANDAY(join_date)) / 365 as avg_tenure_years
            FROM customers
            WHERE status = 'active'
        """
        
        overall_df = self.data_service.execute_query(overall_sql)
        
        return {
            "overall_stats": overall_df.to_dict('records')[0] if not overall_df.empty else {},
            "demographics_breakdown": df.to_dict('records'),
            "grouping": group_by
        }
    
    def _query_product_adoption(self, filters: Dict, time_period: Dict) -> Dict[str, Any]:
        """Query product adoption patterns"""
        sql = """
            SELECT 
                cp.total_products,
                COUNT(*) as customer_count,
                cp.deposit_accounts,
                cp.loan_accounts
            FROM v_customer_products cp
            JOIN customers c ON cp.customer_id = c.customer_id
            WHERE c.status = 'active'
        """
        
        # Add time period filter if specified
        params = {}
        if time_period and 'start' in time_period:
            sql += " AND (cp.last_account_opened >= :start_date OR cp.last_loan_originated >= :start_date)"
            params['start_date'] = time_period['start']
        
        sql += " GROUP BY cp.total_products, cp.deposit_accounts, cp.loan_accounts ORDER BY cp.total_products"
        
        df = self.data_service.execute_query(sql, params)
        
        # Calculate cross-sell metrics
        cross_sell_sql = """
            SELECT 
                SUM(CASE WHEN deposit_accounts > 0 AND loan_accounts > 0 THEN 1 ELSE 0 END) as customers_with_both,
                SUM(CASE WHEN deposit_accounts > 0 AND loan_accounts = 0 THEN 1 ELSE 0 END) as deposit_only,
                SUM(CASE WHEN deposit_accounts = 0 AND loan_accounts > 0 THEN 1 ELSE 0 END) as loan_only,
                COUNT(*) as total_customers
            FROM v_customer_products cp
            JOIN customers c ON cp.customer_id = c.customer_id
            WHERE c.status = 'active'
        """
        
        cross_sell_df = self.data_service.execute_query(cross_sell_sql)
        
        return {
            "product_distribution": df.to_dict('records'),
            "cross_sell_metrics": cross_sell_df.to_dict('records')[0] if not cross_sell_df.empty else {},
            "opportunities": {
                "deposit_to_loan": int(cross_sell_df['deposit_only'].iloc[0]) if not cross_sell_df.empty else 0,
                "loan_to_deposit": int(cross_sell_df['loan_only'].iloc[0]) if not cross_sell_df.empty else 0
            }
        }
    
    def _query_general_customers(self, filters: Dict, limit: int) -> Dict[str, Any]:
        """General customer query with filters"""
        sql = """
            SELECT 
                c.*,
                cs.num_accounts,
                cs.num_loans,
                cs.total_deposits,
                cs.total_loan_balance
            FROM customers c
            LEFT JOIN v_customer_summary cs ON c.customer_id = cs.customer_id
            WHERE c.status = 'active'
        """
        
        # Add filters
        params = {}
        if filters:
            for key, value in filters.items():
                if key in ['segment', 'employment_status', 'status']:
                    sql += f" AND c.{key} = :{key}"
                    params[key] = value
        
        sql += " ORDER BY c.total_relationship_value DESC LIMIT :limit"
        params['limit'] = limit
        
        df = self.data_service.execute_query(sql, params)
        
        return {
            "customers": df.to_dict('records'),
            "total_count": len(df),
            "filters_applied": filters
        }