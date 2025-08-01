from typing import Dict, Any, Optional, List
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
import pandas as pd
from datetime import datetime, timedelta


class LoanQueryTool(BaseTool):
    """Tool for executing loan portfolio queries against the database"""
    
    def __init__(self, data_service: DataInterface):
        super().__init__(
            name="LoanQuery",
            description="Execute loan portfolio queries with real database data"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "query_type": {
                "type": "string",
                "description": "Type of loan query: portfolio_summary, performance_metrics, risk_analysis, vintage_analysis, delinquency_trends"
            },
            "filters": {
                "type": "dict",
                "description": "Optional filters (loan_type, status, origination_date_range, etc.)",
                "optional": True
            },
            "time_period": {
                "type": "dict",
                "description": "Time period for analysis (start, end dates, or quarter/year)",
                "optional": True
            },
            "comparison_period": {
                "type": "dict",
                "description": "Comparison period for YoY or QoQ analysis",
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
        """Execute loan portfolio query"""
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
            if query_type == "portfolio_summary":
                result = self._query_portfolio_summary(filters, time_period)
            elif query_type == "performance_metrics":
                result = self._query_performance_metrics(filters, time_period, comparison_period)
            elif query_type == "risk_analysis":
                result = self._query_risk_analysis(filters, group_by)
            elif query_type == "vintage_analysis":
                result = self._query_vintage_analysis(filters)
            elif query_type == "delinquency_trends":
                result = self._query_delinquency_trends(time_period)
            else:
                # General loan query
                result = self._query_general_loans(filters, limit)
            
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
    
    def _query_portfolio_summary(self, filters: Dict, time_period: Dict) -> Dict[str, Any]:
        """Query loan portfolio summary statistics"""
        # Base query for current portfolio
        sql = """
            SELECT 
                COUNT(*) as total_loans,
                SUM(amount) as total_originated,
                SUM(remaining_balance) as total_outstanding,
                AVG(interest_rate) as avg_interest_rate,
                AVG(amount) as avg_loan_size,
                SUM(CASE WHEN status = 'default' THEN remaining_balance ELSE 0 END) as default_amount,
                SUM(CASE WHEN status = 'default' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as default_rate,
                SUM(CASE WHEN status = 'late' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as delinquency_rate
            FROM loans
            WHERE 1=1
        """
        
        # Add filters
        params = {}
        if time_period and 'start' in time_period:
            sql += " AND origination_date >= :start_date"
            params['start_date'] = time_period['start']
        if time_period and 'end' in time_period:
            sql += " AND origination_date <= :end_date"
            params['end_date'] = time_period['end']
        if filters:
            if 'loan_type' in filters:
                sql += " AND loan_type = :loan_type"
                params['loan_type'] = filters['loan_type']
            if 'status' in filters:
                sql += " AND status = :status"
                params['status'] = filters['status']
        
        summary_df = self.data_service.execute_query(sql, params)
        
        # Query by loan type
        type_sql = """
            SELECT 
                loan_type,
                COUNT(*) as count,
                SUM(amount) as total_amount,
                SUM(remaining_balance) as outstanding_balance,
                AVG(interest_rate) as avg_rate,
                SUM(CASE WHEN status = 'default' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as default_rate
            FROM loans
            WHERE 1=1
        """
        
        # Add same filters
        if time_period and 'start' in time_period:
            type_sql += " AND origination_date >= :start_date"
        if time_period and 'end' in time_period:
            type_sql += " AND origination_date <= :end_date"
        
        type_sql += " GROUP BY loan_type ORDER BY total_amount DESC"
        
        type_df = self.data_service.execute_query(type_sql, params)
        
        # Query by status
        status_sql = """
            SELECT 
                status,
                COUNT(*) as count,
                SUM(remaining_balance) as total_balance,
                AVG(JULIANDAY('now') - JULIANDAY(origination_date)) as avg_age_days
            FROM loans
            GROUP BY status
            ORDER BY count DESC
        """
        
        status_df = self.data_service.execute_query(status_sql)
        
        return {
            "summary": summary_df.to_dict('records')[0] if not summary_df.empty else {},
            "by_loan_type": type_df.to_dict('records'),
            "by_status": status_df.to_dict('records'),
            "period": self._format_period_label(time_period)
        }
    
    def _query_performance_metrics(self, filters: Dict, time_period: Dict, comparison_period: Dict) -> Dict[str, Any]:
        """Query loan performance metrics with period comparison"""
        # Current period metrics
        current_sql = """
            SELECT 
                COUNT(*) as loans_originated,
                SUM(amount) as volume_originated,
                AVG(interest_rate) as avg_interest_rate,
                AVG(credit_score) as avg_credit_score,
                SUM(CASE WHEN status IN ('default', 'charged_off') THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as default_rate,
                'current' as period_type
            FROM loans l
            JOIN customers c ON l.customer_id = c.customer_id
            WHERE 1=1
        """
        
        params_current = {}
        if time_period:
            if 'start' in time_period:
                current_sql += " AND l.origination_date >= :start_date"
                params_current['start_date'] = time_period['start']
            if 'end' in time_period:
                current_sql += " AND l.origination_date <= :end_date"
                params_current['end_date'] = time_period['end']
        
        current_df = self.data_service.execute_query(current_sql, params_current)
        
        # Comparison period metrics if specified
        comparison_data = None
        if comparison_period:
            comp_sql = current_sql.replace("'current' as period_type", "'comparison' as period_type")
            params_comp = {}
            
            if 'start' in comparison_period:
                comp_sql = comp_sql.replace(":start_date", ":comp_start_date")
                params_comp['comp_start_date'] = comparison_period['start']
            if 'end' in comparison_period:
                comp_sql = comp_sql.replace(":end_date", ":comp_end_date")
                params_comp['comp_end_date'] = comparison_period['end']
            
            comp_df = self.data_service.execute_query(comp_sql, params_comp)
            
            if not comp_df.empty:
                curr = current_df.iloc[0]
                comp = comp_df.iloc[0]
                
                comparison_data = {
                    "volume_change": ((curr['volume_originated'] - comp['volume_originated']) / comp['volume_originated'] * 100) if comp['volume_originated'] > 0 else 0,
                    "count_change": ((curr['loans_originated'] - comp['loans_originated']) / comp['loans_originated'] * 100) if comp['loans_originated'] > 0 else 0,
                    "rate_change": curr['avg_interest_rate'] - comp['avg_interest_rate'],
                    "default_rate_change": curr['default_rate'] - comp['default_rate']
                }
        
        # Monthly trend
        trend_sql = """
            SELECT 
                strftime('%Y-%m', origination_date) as month,
                COUNT(*) as loans_count,
                SUM(amount) as volume,
                AVG(interest_rate) as avg_rate
            FROM loans
            WHERE origination_date >= date('now', '-6 months')
            GROUP BY strftime('%Y-%m', origination_date)
            ORDER BY month
        """
        
        trend_df = self.data_service.execute_query(trend_sql)
        
        return {
            "current_period": current_df.to_dict('records')[0] if not current_df.empty else {},
            "comparison": comparison_data,
            "monthly_trend": trend_df.to_dict('records'),
            "period_labels": {
                "current": self._format_period_label(time_period),
                "comparison": self._format_period_label(comparison_period) if comparison_period else None
            }
        }
    
    def _query_risk_analysis(self, filters: Dict, group_by: List[str]) -> Dict[str, Any]:
        """Query loan risk analysis"""
        # Risk distribution
        risk_sql = """
            SELECT 
                CASE 
                    WHEN c.credit_score >= 750 THEN 'Prime (750+)'
                    WHEN c.credit_score >= 650 THEN 'Near-Prime (650-749)'
                    WHEN c.credit_score >= 550 THEN 'Subprime (550-649)'
                    ELSE 'Deep Subprime (<550)'
                END as risk_tier,
                COUNT(*) as loan_count,
                SUM(l.amount) as total_amount,
                SUM(l.remaining_balance) as outstanding_balance,
                AVG(l.interest_rate) as avg_interest_rate,
                SUM(CASE WHEN l.status = 'default' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as default_rate,
                SUM(CASE WHEN l.status = 'late' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as delinquency_rate
            FROM loans l
            JOIN customers c ON l.customer_id = c.customer_id
            WHERE l.status NOT IN ('paid_off')
            GROUP BY risk_tier
            ORDER BY 
                CASE 
                    WHEN c.credit_score >= 750 THEN 1
                    WHEN c.credit_score >= 650 THEN 2
                    WHEN c.credit_score >= 550 THEN 3
                    ELSE 4
                END
        """
        
        risk_df = self.data_service.execute_query(risk_sql)
        
        # Concentration risk by customer
        concentration_sql = """
            SELECT 
                c.customer_id,
                c.name,
                c.credit_score,
                COUNT(*) as loan_count,
                SUM(l.remaining_balance) as total_exposure,
                SUM(l.remaining_balance) * 100.0 / (SELECT SUM(remaining_balance) FROM loans WHERE status NOT IN ('paid_off')) as pct_of_portfolio
            FROM loans l
            JOIN customers c ON l.customer_id = c.customer_id
            WHERE l.status NOT IN ('paid_off')
            GROUP BY c.customer_id, c.name, c.credit_score
            HAVING total_exposure > 0
            ORDER BY total_exposure DESC
            LIMIT 20
        """
        
        concentration_df = self.data_service.execute_query(concentration_sql)
        
        # Calculate risk metrics
        metrics_sql = """
            SELECT 
                SUM(CASE WHEN l.status = 'default' THEN l.remaining_balance ELSE 0 END) as total_default_exposure,
                SUM(CASE WHEN l.status = 'late' THEN l.remaining_balance ELSE 0 END) as total_delinquent_exposure,
                SUM(l.remaining_balance) as total_exposure,
                COUNT(DISTINCT l.customer_id) as unique_borrowers,
                AVG(l.remaining_balance) as avg_loan_exposure
            FROM loans l
            WHERE l.status NOT IN ('paid_off')
        """
        
        metrics_df = self.data_service.execute_query(metrics_sql)
        
        return {
            "risk_distribution": risk_df.to_dict('records'),
            "concentration_risk": {
                "top_exposures": concentration_df.to_dict('records'),
                "top_10_pct": float(concentration_df['pct_of_portfolio'].head(10).sum()) if not concentration_df.empty else 0
            },
            "portfolio_metrics": metrics_df.to_dict('records')[0] if not metrics_df.empty else {},
            "risk_indicators": {
                "high_risk_pct": float(risk_df[risk_df['risk_tier'].str.contains('Subprime')]['loan_count'].sum() / risk_df['loan_count'].sum() * 100) if not risk_df.empty else 0
            }
        }
    
    def _query_vintage_analysis(self, filters: Dict) -> Dict[str, Any]:
        """Query loan vintage performance"""
        vintage_sql = """
            SELECT 
                strftime('%Y-Q', origination_date) || 
                CAST((CAST(strftime('%m', origination_date) AS INTEGER) - 1) / 3 + 1 AS TEXT) as vintage,
                loan_type,
                COUNT(*) as loans_originated,
                SUM(amount) as origination_volume,
                AVG(interest_rate) as avg_rate,
                SUM(CASE WHEN status = 'default' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as default_rate,
                AVG(JULIANDAY('now') - JULIANDAY(origination_date)) / 30 as avg_age_months
            FROM loans
            WHERE origination_date >= date('now', '-2 years')
        """
        
        params = {}
        if filters and 'loan_type' in filters:
            vintage_sql += " AND loan_type = :loan_type"
            params['loan_type'] = filters['loan_type']
        
        vintage_sql += """
            GROUP BY vintage, loan_type
            ORDER BY vintage DESC, loan_type
        """
        
        vintage_df = self.data_service.execute_query(vintage_sql, params)
        
        # Calculate cumulative default rates by age
        age_default_sql = """
            SELECT 
                CASE 
                    WHEN JULIANDAY('now') - JULIANDAY(origination_date) <= 180 THEN '0-6 months'
                    WHEN JULIANDAY('now') - JULIANDAY(origination_date) <= 365 THEN '6-12 months'
                    WHEN JULIANDAY('now') - JULIANDAY(origination_date) <= 730 THEN '1-2 years'
                    ELSE '2+ years'
                END as age_bucket,
                COUNT(*) as loan_count,
                SUM(CASE WHEN status = 'default' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as default_rate
            FROM loans
            GROUP BY age_bucket
            ORDER BY 
                CASE 
                    WHEN JULIANDAY('now') - JULIANDAY(origination_date) <= 180 THEN 1
                    WHEN JULIANDAY('now') - JULIANDAY(origination_date) <= 365 THEN 2
                    WHEN JULIANDAY('now') - JULIANDAY(origination_date) <= 730 THEN 3
                    ELSE 4
                END
        """
        
        age_df = self.data_service.execute_query(age_default_sql)
        
        return {
            "vintage_performance": vintage_df.to_dict('records'),
            "default_by_age": age_df.to_dict('records'),
            "analysis_insights": [
                "Newer vintages show lower default rates due to seasoning",
                "Monitor 12-24 month loans closely as default risk peaks",
                "Compare vintage performance to identify underwriting improvements"
            ]
        }
    
    def _query_delinquency_trends(self, time_period: Dict) -> Dict[str, Any]:
        """Query delinquency trends over time"""
        # Current delinquency snapshot
        delinq_sql = """
            SELECT 
                CASE 
                    WHEN JULIANDAY('now') - JULIANDAY(last_payment_date) <= 30 THEN 'Current'
                    WHEN JULIANDAY('now') - JULIANDAY(last_payment_date) <= 60 THEN '30 DPD'
                    WHEN JULIANDAY('now') - JULIANDAY(last_payment_date) <= 90 THEN '60 DPD'
                    WHEN JULIANDAY('now') - JULIANDAY(last_payment_date) <= 120 THEN '90 DPD'
                    ELSE '120+ DPD'
                END as delinquency_bucket,
                COUNT(*) as loan_count,
                SUM(remaining_balance) as balance_at_risk,
                AVG(remaining_balance) as avg_balance
            FROM loans
            WHERE status NOT IN ('paid_off', 'default', 'charged_off')
                AND last_payment_date IS NOT NULL
            GROUP BY delinquency_bucket
            ORDER BY 
                CASE 
                    WHEN JULIANDAY('now') - JULIANDAY(last_payment_date) <= 30 THEN 1
                    WHEN JULIANDAY('now') - JULIANDAY(last_payment_date) <= 60 THEN 2
                    WHEN JULIANDAY('now') - JULIANDAY(last_payment_date) <= 90 THEN 3
                    WHEN JULIANDAY('now') - JULIANDAY(last_payment_date) <= 120 THEN 4
                    ELSE 5
                END
        """
        
        delinq_df = self.data_service.execute_query(delinq_sql)
        
        # Roll rate analysis
        roll_rate_sql = """
            SELECT 
                loan_type,
                COUNT(CASE WHEN status = 'late' THEN 1 END) as late_count,
                COUNT(CASE WHEN status = 'default' THEN 1 END) as default_count,
                COUNT(CASE WHEN status IN ('late', 'default') THEN 1 END) as total_delinquent,
                COUNT(*) as total_loans,
                COUNT(CASE WHEN status = 'default' THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(CASE WHEN status = 'late' THEN 1 END), 0) as late_to_default_rate
            FROM loans
            WHERE status NOT IN ('paid_off')
            GROUP BY loan_type
        """
        
        roll_df = self.data_service.execute_query(roll_rate_sql)
        
        return {
            "delinquency_snapshot": delinq_df.to_dict('records'),
            "roll_rates": roll_df.to_dict('records'),
            "total_delinquent": {
                "count": int(delinq_df[delinq_df['delinquency_bucket'] != 'Current']['loan_count'].sum()) if not delinq_df.empty else 0,
                "balance": float(delinq_df[delinq_df['delinquency_bucket'] != 'Current']['balance_at_risk'].sum()) if not delinq_df.empty else 0
            },
            "risk_assessment": "Monitor accounts in 60+ DPD for potential charge-off"
        }
    
    def _query_general_loans(self, filters: Dict, limit: int) -> Dict[str, Any]:
        """General loan query with filters"""
        sql = """
            SELECT 
                l.*,
                c.name as customer_name,
                c.credit_score,
                c.segment,
                JULIANDAY('now') - JULIANDAY(l.origination_date) as loan_age_days
            FROM loans l
            JOIN customers c ON l.customer_id = c.customer_id
            WHERE 1=1
        """
        
        # Add filters
        params = {}
        if filters:
            for key, value in filters.items():
                if key in ['loan_type', 'status']:
                    sql += f" AND l.{key} = :{key}"
                    params[key] = value
                elif key == 'min_amount':
                    sql += " AND l.amount >= :min_amount"
                    params['min_amount'] = value
                elif key == 'max_amount':
                    sql += " AND l.amount <= :max_amount"
                    params['max_amount'] = value
        
        sql += " ORDER BY l.origination_date DESC LIMIT :limit"
        params['limit'] = limit
        
        df = self.data_service.execute_query(sql, params)
        
        return {
            "loans": df.to_dict('records'),
            "total_count": len(df),
            "filters_applied": filters
        }
    
    def _format_period_label(self, period: Dict) -> str:
        """Format period dictionary into readable label"""
        if not period:
            return "All Time"
        
        if 'label' in period:
            return period['label']
        elif 'quarter' in period and 'year' in period:
            return f"{period['quarter']} {period['year']}"
        elif 'start' in period and 'end' in period:
            return f"{period['start']} to {period['end']}"
        else:
            return "Custom Period"