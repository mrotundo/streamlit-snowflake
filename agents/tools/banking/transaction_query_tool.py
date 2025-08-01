from typing import Dict, Any, Optional, List
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
import pandas as pd
from datetime import datetime, timedelta


class TransactionQueryTool(BaseTool):
    """Tool for executing transaction analysis queries against the database"""
    
    def __init__(self, data_service: DataInterface):
        super().__init__(
            name="TransactionQuery",
            description="Execute transaction analysis queries with real database data"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "query_type": {
                "type": "string",
                "description": "Type of transaction query: volume_analysis, pattern_detection, cash_flow, category_breakdown, anomaly_detection"
            },
            "filters": {
                "type": "dict",
                "description": "Optional filters (transaction_type, category, amount_range, date_range, customer_id, etc.)",
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
        """Execute transaction analysis query"""
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
            if query_type == "volume_analysis":
                result = self._query_volume_analysis(filters, time_period)
            elif query_type == "pattern_detection":
                result = self._query_pattern_detection(filters, time_period)
            elif query_type == "cash_flow":
                result = self._query_cash_flow(filters, time_period)
            elif query_type == "category_breakdown":
                result = self._query_category_breakdown(filters, time_period)
            elif query_type == "anomaly_detection":
                result = self._query_anomaly_detection(filters, limit)
            else:
                # General transaction query
                result = self._query_general_transactions(filters, limit)
            
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
    
    def _query_volume_analysis(self, filters: Dict, time_period: Dict) -> Dict[str, Any]:
        """Query transaction volume and trends"""
        # Overall volume statistics
        volume_sql = """
            SELECT 
                COUNT(*) as total_transactions,
                SUM(amount) as total_volume,
                AVG(amount) as avg_transaction_amount,
                MAX(amount) as max_transaction,
                MIN(amount) as min_transaction,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(DISTINCT DATE(transaction_date)) as active_days
            FROM transactions
            WHERE 1=1
        """
        
        # Add filters
        params = {}
        if time_period:
            if 'start' in time_period:
                volume_sql += " AND transaction_date >= :start_date"
                params['start_date'] = time_period['start']
            if 'end' in time_period:
                volume_sql += " AND transaction_date <= :end_date"
                params['end_date'] = time_period['end']
        else:
            # Default to last 30 days
            volume_sql += " AND transaction_date >= date('now', '-30 days')"
        
        if filters:
            if 'transaction_type' in filters:
                volume_sql += " AND transaction_type = :transaction_type"
                params['transaction_type'] = filters['transaction_type']
        
        volume_df = self.data_service.execute_query(volume_sql, params)
        
        # Daily volume trend
        daily_sql = """
            SELECT 
                DATE(transaction_date) as transaction_day,
                COUNT(*) as daily_count,
                SUM(amount) as daily_volume,
                AVG(amount) as avg_amount,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM transactions
            WHERE transaction_date >= date('now', '-30 days')
        """
        
        if filters and 'transaction_type' in filters:
            daily_sql += " AND transaction_type = :transaction_type"
        
        daily_sql += " GROUP BY DATE(transaction_date) ORDER BY transaction_day DESC"
        
        daily_df = self.data_service.execute_query(daily_sql, params)
        
        # Transaction type distribution
        type_sql = """
            SELECT 
                transaction_type,
                COUNT(*) as count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transactions WHERE transaction_date >= date('now', '-30 days')) as pct_of_transactions
            FROM transactions
            WHERE transaction_date >= date('now', '-30 days')
            GROUP BY transaction_type
            ORDER BY count DESC
        """
        
        type_df = self.data_service.execute_query(type_sql)
        
        # Calculate volume trends
        trends = {}
        if not daily_df.empty:
            recent_avg = daily_df.head(7)['daily_volume'].mean()
            previous_avg = daily_df.tail(7)['daily_volume'].mean()
            trends['weekly_trend'] = ((recent_avg - previous_avg) / previous_avg * 100) if previous_avg > 0 else 0
        
        return {
            "summary": volume_df.to_dict('records')[0] if not volume_df.empty else {},
            "daily_trend": daily_df.to_dict('records'),
            "by_type": type_df.to_dict('records'),
            "trends": trends,
            "period": self._format_period_label(time_period)
        }
    
    def _query_pattern_detection(self, filters: Dict, time_period: Dict) -> Dict[str, Any]:
        """Detect transaction patterns and behaviors"""
        # Time-of-day patterns
        hourly_sql = """
            SELECT 
                strftime('%H', transaction_date) as hour_of_day,
                COUNT(*) as transaction_count,
                SUM(amount) as total_volume,
                AVG(amount) as avg_amount
            FROM transactions
            WHERE transaction_date >= date('now', '-30 days')
            GROUP BY strftime('%H', transaction_date)
            ORDER BY hour_of_day
        """
        
        hourly_df = self.data_service.execute_query(hourly_sql)
        
        # Day of week patterns
        dow_sql = """
            SELECT 
                CASE strftime('%w', transaction_date)
                    WHEN '0' THEN 'Sunday'
                    WHEN '1' THEN 'Monday'
                    WHEN '2' THEN 'Tuesday'
                    WHEN '3' THEN 'Wednesday'
                    WHEN '4' THEN 'Thursday'
                    WHEN '5' THEN 'Friday'
                    WHEN '6' THEN 'Saturday'
                END as day_of_week,
                strftime('%w', transaction_date) as dow_num,
                COUNT(*) as transaction_count,
                SUM(amount) as total_volume,
                AVG(amount) as avg_amount
            FROM transactions
            WHERE transaction_date >= date('now', '-30 days')
            GROUP BY strftime('%w', transaction_date)
            ORDER BY dow_num
        """
        
        dow_df = self.data_service.execute_query(dow_sql)
        
        # Frequency patterns by customer
        frequency_sql = """
            SELECT 
                customer_id,
                COUNT(*) as transaction_count,
                COUNT(DISTINCT DATE(transaction_date)) as active_days,
                AVG(amount) as avg_transaction_amount,
                SUM(amount) as total_volume,
                MIN(transaction_date) as first_transaction,
                MAX(transaction_date) as last_transaction,
                JULIANDAY(MAX(transaction_date)) - JULIANDAY(MIN(transaction_date)) + 1 as customer_tenure_days
            FROM transactions
            WHERE transaction_date >= date('now', '-90 days')
            GROUP BY customer_id
            HAVING transaction_count > 5
            ORDER BY transaction_count DESC
            LIMIT 20
        """
        
        frequency_df = self.data_service.execute_query(frequency_sql)
        
        # Recurring transaction patterns
        recurring_sql = """
            SELECT 
                customer_id,
                category,
                description,
                COUNT(*) as occurrence_count,
                AVG(amount) as avg_amount,
                STDEV(amount) as amount_variance,
                GROUP_CONCAT(DISTINCT strftime('%d', transaction_date)) as transaction_days
            FROM transactions
            WHERE transaction_date >= date('now', '-90 days')
                AND category IN ('utilities', 'subscription', 'loan_payment', 'rent', 'insurance')
            GROUP BY customer_id, category, description
            HAVING occurrence_count >= 3
        """
        
        # SQLite doesn't have STDEV
        if 'sqlite' in str(type(self.data_service)).lower():
            recurring_sql = recurring_sql.replace("STDEV(amount) as amount_variance,", "")
        
        recurring_df = self.data_service.execute_query(recurring_sql)
        
        return {
            "hourly_patterns": hourly_df.to_dict('records'),
            "weekly_patterns": dow_df.to_dict('records'),
            "customer_frequency": frequency_df.to_dict('records'),
            "recurring_transactions": recurring_df.to_dict('records'),
            "insights": self._generate_pattern_insights(hourly_df, dow_df)
        }
    
    def _query_cash_flow(self, filters: Dict, time_period: Dict) -> Dict[str, Any]:
        """Analyze cash flow patterns"""
        # Monthly cash flow
        monthly_sql = """
            SELECT 
                strftime('%Y-%m', transaction_date) as month,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as inflows,
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as outflows,
                SUM(amount) as net_flow,
                COUNT(CASE WHEN amount > 0 THEN 1 END) as inflow_count,
                COUNT(CASE WHEN amount < 0 THEN 1 END) as outflow_count
            FROM transactions
            WHERE transaction_date >= date('now', '-12 months')
        """
        
        params = {}
        if filters and 'customer_id' in filters:
            monthly_sql += " AND customer_id = :customer_id"
            params['customer_id'] = filters['customer_id']
        
        monthly_sql += " GROUP BY strftime('%Y-%m', transaction_date) ORDER BY month DESC"
        
        monthly_df = self.data_service.execute_query(monthly_sql, params)
        
        # Category-wise cash flow
        category_sql = """
            SELECT 
                category,
                transaction_type,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as inflows,
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as outflows,
                SUM(amount) as net_flow,
                COUNT(*) as transaction_count
            FROM transactions
            WHERE transaction_date >= date('now', '-30 days')
        """
        
        if filters and 'customer_id' in filters:
            category_sql += " AND customer_id = :customer_id"
        
        category_sql += " GROUP BY category, transaction_type ORDER BY ABS(net_flow) DESC"
        
        category_df = self.data_service.execute_query(category_sql, params)
        
        # Customer cash flow summary
        customer_sql = """
            SELECT 
                c.customer_id,
                c.name,
                c.segment,
                SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) as total_inflows,
                SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END) as total_outflows,
                SUM(t.amount) as net_cash_flow,
                COUNT(*) as transaction_count,
                AVG(t.amount) as avg_transaction
            FROM transactions t
            JOIN customers c ON t.customer_id = c.customer_id
            WHERE t.transaction_date >= date('now', '-30 days')
            GROUP BY c.customer_id, c.name, c.segment
            HAVING ABS(net_cash_flow) > 1000
            ORDER BY ABS(net_cash_flow) DESC
            LIMIT 20
        """
        
        customer_df = self.data_service.execute_query(customer_sql)
        
        # Calculate cash flow metrics
        metrics = {}
        if not monthly_df.empty:
            metrics['avg_monthly_inflow'] = float(monthly_df['inflows'].mean())
            metrics['avg_monthly_outflow'] = float(monthly_df['outflows'].mean())
            metrics['avg_monthly_net'] = float(monthly_df['net_flow'].mean())
            metrics['volatility'] = float(monthly_df['net_flow'].std()) if len(monthly_df) > 1 else 0
        
        return {
            "monthly_cash_flow": monthly_df.to_dict('records'),
            "category_flow": category_df.to_dict('records'),
            "top_customers_by_flow": customer_df.to_dict('records'),
            "metrics": metrics,
            "recommendations": self._generate_cash_flow_recommendations(metrics)
        }
    
    def _query_category_breakdown(self, filters: Dict, time_period: Dict) -> Dict[str, Any]:
        """Analyze transactions by category"""
        # Category summary
        category_sql = """
            SELECT 
                category,
                COUNT(*) as transaction_count,
                SUM(ABS(amount)) as total_volume,
                AVG(ABS(amount)) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transactions WHERE transaction_date >= date('now', '-30 days')) as pct_of_transactions
            FROM transactions
            WHERE transaction_date >= date('now', '-30 days')
        """
        
        params = {}
        if filters:
            if 'transaction_type' in filters:
                category_sql += " AND transaction_type = :transaction_type"
                params['transaction_type'] = filters['transaction_type']
        
        category_sql += " GROUP BY category ORDER BY total_volume DESC"
        
        category_df = self.data_service.execute_query(category_sql, params)
        
        # Category trends over time
        trend_sql = """
            SELECT 
                strftime('%Y-%m', transaction_date) as month,
                category,
                COUNT(*) as transaction_count,
                SUM(ABS(amount)) as volume
            FROM transactions
            WHERE transaction_date >= date('now', '-6 months')
                AND category IN (
                    SELECT category 
                    FROM transactions 
                    WHERE transaction_date >= date('now', '-30 days')
                    GROUP BY category 
                    ORDER BY COUNT(*) DESC 
                    LIMIT 5
                )
            GROUP BY strftime('%Y-%m', transaction_date), category
            ORDER BY month DESC, volume DESC
        """
        
        trend_df = self.data_service.execute_query(trend_sql)
        
        # Customer category preferences
        preference_sql = """
            SELECT 
                c.segment,
                t.category,
                COUNT(DISTINCT t.customer_id) as customer_count,
                COUNT(*) as transaction_count,
                AVG(ABS(t.amount)) as avg_amount
            FROM transactions t
            JOIN customers c ON t.customer_id = c.customer_id
            WHERE t.transaction_date >= date('now', '-30 days')
            GROUP BY c.segment, t.category
            HAVING transaction_count > 10
            ORDER BY c.segment, transaction_count DESC
        """
        
        preference_df = self.data_service.execute_query(preference_sql)
        
        return {
            "category_summary": category_df.to_dict('records'),
            "category_trends": trend_df.to_dict('records'),
            "segment_preferences": preference_df.to_dict('records'),
            "insights": {
                "top_category": category_df.iloc[0]['category'] if not category_df.empty else None,
                "concentration": float(category_df.head(3)['pct_of_transactions'].sum()) if not category_df.empty else 0
            }
        }
    
    def _query_anomaly_detection(self, filters: Dict, limit: int) -> Dict[str, Any]:
        """Detect unusual transactions and patterns"""
        # Large transactions (outliers)
        outlier_sql = """
            WITH stats AS (
                SELECT 
                    AVG(ABS(amount)) as avg_amount,
                    AVG(ABS(amount)) + 3 * STDEV(ABS(amount)) as upper_threshold,
                    AVG(ABS(amount)) - 3 * STDEV(ABS(amount)) as lower_threshold
                FROM transactions
                WHERE transaction_date >= date('now', '-30 days')
            )
            SELECT 
                t.transaction_id,
                t.customer_id,
                c.name as customer_name,
                t.transaction_date,
                t.transaction_type,
                t.category,
                t.amount,
                t.description,
                s.avg_amount,
                ABS(t.amount) / s.avg_amount as times_avg
            FROM transactions t
            JOIN customers c ON t.customer_id = c.customer_id
            CROSS JOIN stats s
            WHERE t.transaction_date >= date('now', '-7 days')
                AND ABS(t.amount) > s.upper_threshold
            ORDER BY ABS(t.amount) DESC
            LIMIT :limit
        """
        
        # SQLite doesn't have STDEV, use simpler approach
        if 'sqlite' in str(type(self.data_service)).lower():
            outlier_sql = """
            WITH stats AS (
                SELECT 
                    AVG(ABS(amount)) as avg_amount,
                    MAX(ABS(amount)) * 0.8 as threshold
                FROM transactions
                WHERE transaction_date >= date('now', '-30 days')
            )
            SELECT 
                t.transaction_id,
                t.customer_id,
                c.name as customer_name,
                t.transaction_date,
                t.transaction_type,
                t.category,
                t.amount,
                t.description,
                s.avg_amount,
                ABS(t.amount) / s.avg_amount as times_avg
            FROM transactions t
            JOIN customers c ON t.customer_id = c.customer_id
            CROSS JOIN stats s
            WHERE t.transaction_date >= date('now', '-7 days')
                AND ABS(t.amount) > s.threshold
            ORDER BY ABS(t.amount) DESC
            LIMIT :limit
            """
        
        params = {'limit': limit}
        outlier_df = self.data_service.execute_query(outlier_sql, params)
        
        # Unusual time transactions
        unusual_time_sql = """
            SELECT 
                t.transaction_id,
                t.customer_id,
                c.name as customer_name,
                t.transaction_date,
                strftime('%H:%M', t.transaction_date) as transaction_time,
                t.amount,
                t.category,
                t.description
            FROM transactions t
            JOIN customers c ON t.customer_id = c.customer_id
            WHERE t.transaction_date >= date('now', '-7 days')
                AND (
                    CAST(strftime('%H', t.transaction_date) AS INTEGER) < 6
                    OR CAST(strftime('%H', t.transaction_date) AS INTEGER) >= 23
                )
            ORDER BY t.transaction_date DESC
            LIMIT :limit
        """
        
        unusual_time_df = self.data_service.execute_query(unusual_time_sql, params)
        
        # Rapid succession transactions
        rapid_sql = """
            WITH successive_trans AS (
                SELECT 
                    t1.transaction_id,
                    t1.customer_id,
                    t1.transaction_date as trans_date,
                    t1.amount,
                    t1.category,
                    t2.transaction_date as next_trans_date,
                    t2.amount as next_amount,
                    (JULIANDAY(t2.transaction_date) - JULIANDAY(t1.transaction_date)) * 24 * 60 as minutes_apart
                FROM transactions t1
                JOIN transactions t2 ON t1.customer_id = t2.customer_id
                    AND t2.transaction_date > t1.transaction_date
                    AND t2.transaction_date <= datetime(t1.transaction_date, '+1 hour')
                WHERE t1.transaction_date >= date('now', '-7 days')
            )
            SELECT 
                st.transaction_id,
                st.customer_id,
                c.name as customer_name,
                st.trans_date,
                st.amount,
                st.category,
                st.minutes_apart,
                COUNT(*) as rapid_count
            FROM successive_trans st
            JOIN customers c ON st.customer_id = c.customer_id
            WHERE st.minutes_apart < 5
            GROUP BY st.customer_id, DATE(st.trans_date)
            ORDER BY rapid_count DESC
            LIMIT :limit
        """
        
        rapid_df = self.data_service.execute_query(rapid_sql, params)
        
        return {
            "large_transactions": outlier_df.to_dict('records'),
            "unusual_time_transactions": unusual_time_df.to_dict('records'),
            "rapid_transactions": rapid_df.to_dict('records'),
            "anomaly_summary": {
                "outliers_detected": len(outlier_df),
                "after_hours_detected": len(unusual_time_df),
                "rapid_succession_detected": len(rapid_df)
            },
            "risk_indicators": "Review flagged transactions for potential fraud or unusual activity"
        }
    
    def _query_general_transactions(self, filters: Dict, limit: int) -> Dict[str, Any]:
        """General transaction query with filters"""
        sql = """
            SELECT 
                t.*,
                c.name as customer_name,
                c.segment,
                d.account_type,
                l.loan_type
            FROM transactions t
            JOIN customers c ON t.customer_id = c.customer_id
            LEFT JOIN deposits d ON t.account_id = d.account_id
            LEFT JOIN loans l ON t.loan_id = l.loan_id
            WHERE 1=1
        """
        
        # Add filters
        params = {}
        if filters:
            for key, value in filters.items():
                if key in ['transaction_type', 'category']:
                    sql += f" AND t.{key} = :{key}"
                    params[key] = value
                elif key == 'customer_id':
                    sql += " AND t.customer_id = :customer_id"
                    params['customer_id'] = value
                elif key == 'min_amount':
                    sql += " AND ABS(t.amount) >= :min_amount"
                    params['min_amount'] = value
                elif key == 'date_from':
                    sql += " AND t.transaction_date >= :date_from"
                    params['date_from'] = value
        
        sql += " ORDER BY t.transaction_date DESC LIMIT :limit"
        params['limit'] = limit
        
        df = self.data_service.execute_query(sql, params)
        
        return {
            "transactions": df.to_dict('records'),
            "total_count": len(df),
            "filters_applied": filters
        }
    
    def _format_period_label(self, period: Dict) -> str:
        """Format period dictionary into readable label"""
        if not period:
            return "Last 30 Days"
        
        if 'start' in period and 'end' in period:
            return f"{period['start']} to {period['end']}"
        elif 'start' in period:
            return f"Since {period['start']}"
        else:
            return "Custom Period"
    
    def _generate_pattern_insights(self, hourly_df: pd.DataFrame, dow_df: pd.DataFrame) -> List[str]:
        """Generate insights from pattern data"""
        insights = []
        
        if not hourly_df.empty:
            peak_hour = hourly_df.loc[hourly_df['transaction_count'].idxmax()]['hour_of_day']
            insights.append(f"Peak transaction hour is {peak_hour}:00")
        
        if not dow_df.empty:
            peak_day = dow_df.loc[dow_df['transaction_count'].idxmax()]['day_of_week']
            insights.append(f"Highest transaction volume on {peak_day}s")
        
        return insights
    
    def _generate_cash_flow_recommendations(self, metrics: Dict) -> List[str]:
        """Generate cash flow recommendations"""
        recommendations = []
        
        if metrics.get('avg_monthly_net', 0) < 0:
            recommendations.append("Net negative cash flow detected - investigate spending patterns")
        
        if metrics.get('volatility', 0) > metrics.get('avg_monthly_inflow', 1) * 0.5:
            recommendations.append("High cash flow volatility - consider stabilizing income sources")
        
        recommendations.append("Monitor large outflows and categorize for better budgeting")
        
        return recommendations