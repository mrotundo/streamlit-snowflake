from typing import Dict, Any, Optional, List
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
import pandas as pd
from datetime import datetime, timedelta


class DepositQueryTool(BaseTool):
    """Tool for executing deposit account queries against the database"""
    
    def __init__(self, data_service: DataInterface):
        super().__init__(
            name="DepositQuery",
            description="Execute deposit account queries with real database data"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "query_type": {
                "type": "string",
                "description": "Type of deposit query: account_summary, balance_distribution, growth_trends, account_activity, liquidity_analysis"
            },
            "filters": {
                "type": "dict",
                "description": "Optional filters (account_type, status, balance_range, opened_date_range, etc.)",
                "optional": True
            },
            "time_period": {
                "type": "dict",
                "description": "Time period for analysis (start, end dates)",
                "optional": True
            },
            "comparison_period": {
                "type": "dict",
                "description": "Comparison period for growth analysis",
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
        """Execute deposit account query"""
        query_type = kwargs.get("query_type", "")
        filters = kwargs.get("filters", {})
        time_period = kwargs.get("time_period", {})
        comparison_period = kwargs.get("comparison_period", {})
        group_by = kwargs.get("group_by", [])
        limit = kwargs.get("limit", 100)
        
        try:
            # Ensure we're connected
            if not self.data_service.validate_connection():
                self.data_service.connect()
            
            # Build and execute query based on type
            if query_type == "account_summary":
                result = self._query_account_summary(filters, time_period)
            elif query_type == "balance_distribution":
                result = self._query_balance_distribution(filters)
            elif query_type == "growth_trends":
                result = self._query_growth_trends(time_period, comparison_period)
            elif query_type == "account_activity":
                result = self._query_account_activity(filters, limit)
            elif query_type == "liquidity_analysis":
                result = self._query_liquidity_analysis(filters)
            else:
                # General deposit query
                result = self._query_general_deposits(filters, limit)
            
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
    
    def _query_account_summary(self, filters: Dict, time_period: Dict) -> Dict[str, Any]:
        """Query deposit account summary statistics"""
        # Main summary query
        sql = """
            SELECT 
                COUNT(*) as total_accounts,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(balance) as total_deposits,
                AVG(balance) as avg_balance,
                MEDIAN(balance) as median_balance,
                MAX(balance) as max_balance,
                MIN(balance) as min_balance,
                SUM(CASE WHEN balance = 0 THEN 1 ELSE 0 END) as zero_balance_accounts,
                AVG(interest_rate) as avg_interest_rate
            FROM deposits
            WHERE status = 'active'
        """
        
        # SQLite doesn't have MEDIAN, so we'll handle it differently
        if 'sqlite' in str(type(self.data_service)).lower():
            sql = sql.replace("MEDIAN(balance) as median_balance,", "")
        
        # Add filters
        params = {}
        if filters:
            if 'account_type' in filters:
                sql += " AND account_type = :account_type"
                params['account_type'] = filters['account_type']
            if 'min_balance' in filters:
                sql += " AND balance >= :min_balance"
                params['min_balance'] = filters['min_balance']
        
        if time_period and 'start' in time_period:
            sql += " AND opened_date >= :start_date"
            params['start_date'] = time_period['start']
        
        summary_df = self.data_service.execute_query(sql, params)
        
        # Query by account type
        type_sql = """
            SELECT 
                account_type,
                COUNT(*) as account_count,
                SUM(balance) as total_balance,
                AVG(balance) as avg_balance,
                AVG(interest_rate) as avg_interest_rate,
                SUM(CASE WHEN balance >= minimum_balance THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as meets_minimum_pct
            FROM deposits
            WHERE status = 'active'
            GROUP BY account_type
            ORDER BY total_balance DESC
        """
        
        type_df = self.data_service.execute_query(type_sql)
        
        # New accounts trend
        new_accounts_sql = """
            SELECT 
                strftime('%Y-%m', opened_date) as month,
                COUNT(*) as new_accounts,
                SUM(balance) as initial_deposits
            FROM deposits
            WHERE opened_date >= date('now', '-6 months')
                AND status = 'active'
            GROUP BY strftime('%Y-%m', opened_date)
            ORDER BY month DESC
        """
        
        new_accounts_df = self.data_service.execute_query(new_accounts_sql)
        
        return {
            "summary": summary_df.to_dict('records')[0] if not summary_df.empty else {},
            "by_account_type": type_df.to_dict('records'),
            "new_accounts_trend": new_accounts_df.to_dict('records'),
            "insights": {
                "zero_balance_pct": float(summary_df['zero_balance_accounts'].iloc[0] / summary_df['total_accounts'].iloc[0] * 100) if not summary_df.empty and summary_df['total_accounts'].iloc[0] > 0 else 0,
                "avg_accounts_per_customer": float(summary_df['total_accounts'].iloc[0] / summary_df['unique_customers'].iloc[0]) if not summary_df.empty and summary_df['unique_customers'].iloc[0] > 0 else 0
            }
        }
    
    def _query_balance_distribution(self, filters: Dict) -> Dict[str, Any]:
        """Query deposit balance distribution"""
        # Balance tier distribution
        tier_sql = """
            SELECT 
                CASE 
                    WHEN balance < 1000 THEN 'Under $1K'
                    WHEN balance < 5000 THEN '$1K - $5K'
                    WHEN balance < 10000 THEN '$5K - $10K'
                    WHEN balance < 25000 THEN '$10K - $25K'
                    WHEN balance < 50000 THEN '$25K - $50K'
                    WHEN balance < 100000 THEN '$50K - $100K'
                    WHEN balance < 250000 THEN '$100K - $250K'
                    ELSE '$250K+'
                END as balance_tier,
                COUNT(*) as account_count,
                SUM(balance) as total_balance,
                AVG(balance) as avg_balance,
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM deposits WHERE status = 'active') as pct_of_accounts
            FROM deposits
            WHERE status = 'active'
        """
        
        params = {}
        if filters and 'account_type' in filters:
            tier_sql += " AND account_type = :account_type"
            params['account_type'] = filters['account_type']
        
        tier_sql += """
            GROUP BY balance_tier
            ORDER BY 
                CASE 
                    WHEN balance < 1000 THEN 1
                    WHEN balance < 5000 THEN 2
                    WHEN balance < 10000 THEN 3
                    WHEN balance < 25000 THEN 4
                    WHEN balance < 50000 THEN 5
                    WHEN balance < 100000 THEN 6
                    WHEN balance < 250000 THEN 7
                    ELSE 8
                END
        """
        
        tier_df = self.data_service.execute_query(tier_sql, params)
        
        # Concentration analysis
        concentration_sql = """
            SELECT 
                SUM(CASE WHEN balance >= (SELECT balance FROM deposits WHERE status = 'active' ORDER BY balance DESC LIMIT 1 OFFSET (SELECT COUNT(*) * 0.1 FROM deposits WHERE status = 'active')) THEN balance ELSE 0 END) as top_10pct_balance,
                SUM(CASE WHEN balance >= (SELECT balance FROM deposits WHERE status = 'active' ORDER BY balance DESC LIMIT 1 OFFSET (SELECT COUNT(*) * 0.2 FROM deposits WHERE status = 'active')) THEN balance ELSE 0 END) as top_20pct_balance,
                SUM(balance) as total_balance
            FROM deposits
            WHERE status = 'active'
        """
        
        conc_df = self.data_service.execute_query(concentration_sql)
        
        # FDIC insurance coverage
        fdic_sql = """
            SELECT 
                COUNT(CASE WHEN balance <= 250000 THEN 1 END) as fully_insured_accounts,
                COUNT(CASE WHEN balance > 250000 THEN 1 END) as partially_insured_accounts,
                SUM(CASE WHEN balance > 250000 THEN balance - 250000 ELSE 0 END) as uninsured_deposits,
                SUM(balance) as total_deposits
            FROM deposits
            WHERE status = 'active'
        """
        
        fdic_df = self.data_service.execute_query(fdic_sql)
        
        return {
            "balance_distribution": tier_df.to_dict('records'),
            "concentration_metrics": {
                "top_10pct_of_deposits": float(conc_df['top_10pct_balance'].iloc[0] / conc_df['total_balance'].iloc[0] * 100) if not conc_df.empty and conc_df['total_balance'].iloc[0] > 0 else 0,
                "top_20pct_of_deposits": float(conc_df['top_20pct_balance'].iloc[0] / conc_df['total_balance'].iloc[0] * 100) if not conc_df.empty and conc_df['total_balance'].iloc[0] > 0 else 0
            },
            "fdic_coverage": fdic_df.to_dict('records')[0] if not fdic_df.empty else {},
            "risk_indicators": {
                "uninsured_pct": float(fdic_df['uninsured_deposits'].iloc[0] / fdic_df['total_deposits'].iloc[0] * 100) if not fdic_df.empty and fdic_df['total_deposits'].iloc[0] > 0 else 0
            }
        }
    
    def _query_growth_trends(self, time_period: Dict, comparison_period: Dict) -> Dict[str, Any]:
        """Query deposit growth trends"""
        # Current period growth
        current_sql = """
            SELECT 
                COUNT(*) as accounts_opened,
                SUM(balance) as total_new_deposits,
                AVG(balance) as avg_new_account_balance,
                COUNT(DISTINCT customer_id) as new_customers
            FROM deposits
            WHERE status = 'active'
        """
        
        params_current = {}
        if time_period:
            if 'start' in time_period:
                current_sql += " AND opened_date >= :start_date"
                params_current['start_date'] = time_period['start']
            if 'end' in time_period:
                current_sql += " AND opened_date <= :end_date"
                params_current['end_date'] = time_period['end']
        else:
            # Default to last 30 days
            current_sql += " AND opened_date >= date('now', '-30 days')"
        
        current_df = self.data_service.execute_query(current_sql, params_current)
        
        # Monthly deposit growth
        growth_sql = """
            SELECT 
                strftime('%Y-%m', date) as month,
                account_type,
                SUM(balance) as total_balance,
                COUNT(*) as account_count
            FROM deposits
            WHERE date >= date('now', '-12 months')
                AND status = 'active'
            GROUP BY strftime('%Y-%m', date), account_type
            ORDER BY month, account_type
        """
        
        growth_df = self.data_service.execute_query(growth_sql)
        
        # Calculate month-over-month growth rates
        if not growth_df.empty:
            growth_df['mom_growth'] = growth_df.groupby('account_type')['total_balance'].pct_change() * 100
        
        # Net deposit flow (simplified without transaction table joins)
        flow_sql = """
            SELECT 
                strftime('%Y-%m', date) as month,
                SUM(CASE WHEN balance > 0 THEN balance ELSE 0 END) as deposits_in,
                SUM(CASE WHEN balance < 0 THEN ABS(balance) ELSE 0 END) as deposits_out,
                SUM(balance) as net_flow
            FROM deposits
            WHERE date >= date('now', '-6 months')
            GROUP BY strftime('%Y-%m', date)
            ORDER BY month DESC
        """
        
        flow_df = self.data_service.execute_query(flow_sql)
        
        # Comparison with previous period if specified
        comparison_data = None
        if comparison_period:
            comp_sql = current_sql.replace("opened_date >=", "opened_date >=").replace("opened_date <=", "opened_date <=")
            params_comp = {}
            
            if 'start' in comparison_period:
                params_comp['start_date'] = comparison_period['start']
            if 'end' in comparison_period:
                params_comp['end_date'] = comparison_period['end']
            
            comp_df = self.data_service.execute_query(comp_sql, params_comp)
            
            if not comp_df.empty and not current_df.empty:
                curr = current_df.iloc[0]
                comp = comp_df.iloc[0]
                
                comparison_data = {
                    "accounts_change": ((curr['accounts_opened'] - comp['accounts_opened']) / comp['accounts_opened'] * 100) if comp['accounts_opened'] > 0 else 0,
                    "deposits_change": ((curr['total_new_deposits'] - comp['total_new_deposits']) / comp['total_new_deposits'] * 100) if comp['total_new_deposits'] > 0 else 0,
                    "avg_balance_change": ((curr['avg_new_account_balance'] - comp['avg_new_account_balance']) / comp['avg_new_account_balance'] * 100) if comp['avg_new_account_balance'] > 0 else 0
                }
        
        return {
            "current_period": current_df.to_dict('records')[0] if not current_df.empty else {},
            "comparison": comparison_data,
            "monthly_growth": growth_df.to_dict('records'),
            "deposit_flows": flow_df.to_dict('records'),
            "growth_insights": self._calculate_growth_insights(growth_df)
        }
    
    def _query_account_activity(self, filters: Dict, limit: int) -> Dict[str, Any]:
        """Query account activity patterns"""
        # Active vs dormant accounts
        activity_sql = """
            SELECT 
                CASE 
                    WHEN JULIANDAY('now') - JULIANDAY(last_transaction_date) <= 30 THEN 'Active (0-30 days)'
                    WHEN JULIANDAY('now') - JULIANDAY(last_transaction_date) <= 90 THEN 'Low Activity (31-90 days)'
                    WHEN JULIANDAY('now') - JULIANDAY(last_transaction_date) <= 180 THEN 'Dormant (91-180 days)'
                    WHEN last_transaction_date IS NULL THEN 'Never Used'
                    ELSE 'Inactive (180+ days)'
                END as activity_status,
                COUNT(*) as account_count,
                SUM(balance) as total_balance,
                AVG(balance) as avg_balance
            FROM deposits
            WHERE status = 'active'
            GROUP BY activity_status
            ORDER BY 
                CASE 
                    WHEN JULIANDAY('now') - JULIANDAY(last_transaction_date) <= 30 THEN 1
                    WHEN JULIANDAY('now') - JULIANDAY(last_transaction_date) <= 90 THEN 2
                    WHEN JULIANDAY('now') - JULIANDAY(last_transaction_date) <= 180 THEN 3
                    WHEN last_transaction_date IS NULL THEN 4
                    ELSE 5
                END
        """
        
        activity_df = self.data_service.execute_query(activity_sql)
        
        # Top accounts by balance
        top_accounts_sql = """
            SELECT 
                d.account_id,
                d.customer_id,
                c.name as customer_name,
                d.account_type,
                d.balance,
                d.interest_rate,
                d.opened_date,
                d.last_transaction_date,
                JULIANDAY('now') - JULIANDAY(d.opened_date) as account_age_days
            FROM deposits d
            JOIN customers c ON d.customer_id = c.customer_id
            WHERE d.status = 'active'
        """
        
        params = {}
        if filters:
            if 'account_type' in filters:
                top_accounts_sql += " AND d.account_type = :account_type"
                params['account_type'] = filters['account_type']
            if 'min_balance' in filters:
                top_accounts_sql += " AND d.balance >= :min_balance"
                params['min_balance'] = filters['min_balance']
        
        top_accounts_sql += " ORDER BY d.balance DESC LIMIT :limit"
        params['limit'] = limit
        
        top_accounts_df = self.data_service.execute_query(top_accounts_sql, params)
        
        # Account utilization (overdraft usage)
        utilization_sql = """
            SELECT 
                account_type,
                COUNT(CASE WHEN balance < 0 THEN 1 END) as overdraft_accounts,
                COUNT(*) as total_accounts,
                COUNT(CASE WHEN balance < 0 THEN 1 END) * 100.0 / COUNT(*) as overdraft_usage_pct,
                AVG(CASE WHEN balance < 0 THEN balance ELSE NULL END) as avg_overdraft_amount
            FROM deposits
            WHERE status = 'active'
                AND overdraft_limit > 0
            GROUP BY account_type
        """
        
        utilization_df = self.data_service.execute_query(utilization_sql)
        
        return {
            "activity_distribution": activity_df.to_dict('records'),
            "top_accounts": top_accounts_df.to_dict('records'),
            "overdraft_utilization": utilization_df.to_dict('records'),
            "engagement_metrics": {
                "dormant_account_pct": float(activity_df[activity_df['activity_status'].str.contains('Dormant|Inactive|Never')]['account_count'].sum() / activity_df['account_count'].sum() * 100) if not activity_df.empty else 0,
                "dormant_balance_pct": float(activity_df[activity_df['activity_status'].str.contains('Dormant|Inactive|Never')]['total_balance'].sum() / activity_df['total_balance'].sum() * 100) if not activity_df.empty else 0
            }
        }
    
    def _query_liquidity_analysis(self, filters: Dict) -> Dict[str, Any]:
        """Query liquidity and stability metrics"""
        # Account stability by type
        stability_sql = """
            SELECT 
                account_type,
                COUNT(*) as total_accounts,
                SUM(balance) as total_deposits,
                AVG(JULIANDAY('now') - JULIANDAY(opened_date)) / 365 as avg_account_age_years,
                STDEV(balance) as balance_volatility,
                SUM(CASE WHEN JULIANDAY('now') - JULIANDAY(opened_date) > 365 THEN balance ELSE 0 END) as stable_deposits,
                SUM(CASE WHEN JULIANDAY('now') - JULIANDAY(opened_date) <= 90 THEN balance ELSE 0 END) as hot_money
            FROM deposits
            WHERE status = 'active'
            GROUP BY account_type
        """
        
        # SQLite doesn't have STDEV, use custom calculation
        if 'sqlite' in str(type(self.data_service)).lower():
            stability_sql = stability_sql.replace("STDEV(balance) as balance_volatility,", "")
        
        stability_df = self.data_service.execute_query(stability_sql)
        
        # Interest sensitivity
        rate_sql = """
            SELECT 
                CASE 
                    WHEN interest_rate = 0 THEN 'Non-interest bearing'
                    WHEN interest_rate < 1 THEN 'Low rate (< 1%)'
                    WHEN interest_rate < 2 THEN 'Medium rate (1-2%)'
                    WHEN interest_rate < 3 THEN 'High rate (2-3%)'
                    ELSE 'Premium rate (3%+)'
                END as rate_tier,
                COUNT(*) as account_count,
                SUM(balance) as total_balance,
                AVG(balance) as avg_balance
            FROM deposits
            WHERE status = 'active'
            GROUP BY rate_tier
            ORDER BY interest_rate
        """
        
        rate_df = self.data_service.execute_query(rate_sql)
        
        # Calculate liquidity ratios
        liquidity_sql = """
            SELECT 
                SUM(CASE WHEN account_type = 'checking' THEN balance ELSE 0 END) as demand_deposits,
                SUM(CASE WHEN account_type IN ('savings', 'money_market') THEN balance ELSE 0 END) as savings_deposits,
                SUM(CASE WHEN account_type = 'cd' THEN balance ELSE 0 END) as time_deposits,
                SUM(balance) as total_deposits,
                COUNT(DISTINCT customer_id) as unique_depositors
            FROM deposits
            WHERE status = 'active'
        """
        
        liquidity_df = self.data_service.execute_query(liquidity_sql)
        
        return {
            "stability_metrics": stability_df.to_dict('records'),
            "rate_sensitivity": rate_df.to_dict('records'),
            "liquidity_composition": liquidity_df.to_dict('records')[0] if not liquidity_df.empty else {},
            "liquidity_ratios": self._calculate_liquidity_ratios(liquidity_df),
            "recommendations": [
                "Monitor hot money deposits for potential outflows",
                "Consider rate adjustments for competitive positioning",
                "Diversify funding sources to reduce concentration risk"
            ]
        }
    
    def _query_general_deposits(self, filters: Dict, limit: int) -> Dict[str, Any]:
        """General deposit query with filters"""
        sql = """
            SELECT 
                d.*,
                c.name as customer_name,
                c.segment,
                c.credit_score,
                JULIANDAY('now') - JULIANDAY(d.opened_date) as account_age_days,
                JULIANDAY('now') - JULIANDAY(d.last_transaction_date) as days_since_transaction
            FROM deposits d
            JOIN customers c ON d.customer_id = c.customer_id
            WHERE d.status = 'active'
        """
        
        # Add filters
        params = {}
        if filters:
            for key, value in filters.items():
                if key in ['account_type']:
                    sql += f" AND d.{key} = :{key}"
                    params[key] = value
                elif key == 'min_balance':
                    sql += " AND d.balance >= :min_balance"
                    params['min_balance'] = value
                elif key == 'max_balance':
                    sql += " AND d.balance <= :max_balance"
                    params['max_balance'] = value
        
        sql += " ORDER BY d.balance DESC LIMIT :limit"
        params['limit'] = limit
        
        df = self.data_service.execute_query(sql, params)
        
        return {
            "deposits": df.to_dict('records'),
            "total_count": len(df),
            "filters_applied": filters
        }
    
    def _calculate_growth_insights(self, growth_df: pd.DataFrame) -> List[str]:
        """Calculate insights from growth data"""
        insights = []
        
        if not growth_df.empty and 'mom_growth' in growth_df.columns:
            avg_growth = growth_df['mom_growth'].mean()
            if avg_growth > 5:
                insights.append("Strong deposit growth momentum with average MoM growth > 5%")
            elif avg_growth < 0:
                insights.append("Deposit outflows detected - implement retention strategies")
            
            # Check volatility
            if growth_df['mom_growth'].std() > 10:
                insights.append("High volatility in deposit flows - investigate seasonal patterns")
        
        return insights
    
    def _calculate_liquidity_ratios(self, liquidity_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate key liquidity ratios"""
        if liquidity_df.empty:
            return {}
        
        row = liquidity_df.iloc[0]
        total = row['total_deposits'] if row['total_deposits'] > 0 else 1
        
        return {
            "demand_deposit_ratio": float(row['demand_deposits'] / total * 100),
            "stable_funding_ratio": float((row['savings_deposits'] + row['time_deposits']) / total * 100),
            "avg_deposit_per_customer": float(total / row['unique_depositors']) if row['unique_depositors'] > 0 else 0
        }