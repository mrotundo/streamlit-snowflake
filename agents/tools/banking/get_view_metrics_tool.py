from typing import Dict, Any, Optional
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
import pandas as pd
from datetime import datetime, timedelta


class GetViewMetricsTool(BaseTool):
    """Tool for retrieving metrics and KPIs for views from the data catalog"""
    
    def __init__(self, data_service: Optional[DataInterface] = None):
        super().__init__(
            name="GetViewMetrics",
            description="Get metrics and KPIs associated with database views"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "view_name": {
                "type": "string",
                "description": "Name of the view to get metrics for"
            },
            "metric_name": {
                "type": "string",
                "description": "Specific metric name to retrieve (optional)",
                "optional": True
            },
            "days_back": {
                "type": "integer",
                "description": "Number of days back to retrieve metrics for (default: 30)",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Retrieve metrics from the catalog"""
        view_name = kwargs.get("view_name", "").lower()
        metric_name = kwargs.get("metric_name", "")
        days_back = kwargs.get("days_back", 30)
        
        if not view_name:
            return {
                "success": False,
                "error": "view_name is required",
                "result": {}
            }
        
        if not self.data_service or not self.data_service.validate_connection():
            return {
                "success": False,
                "error": "No data service connection available",
                "result": {}
            }
        
        try:
            # Calculate date range
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
            
            # Build query based on parameters
            if metric_name:
                metrics_query = """
                    SELECT 
                        metric_name,
                        column_name,
                        metric_value,
                        metric_date,
                        created_at
                    FROM data_catalog_metrics
                    WHERE LOWER(view_name) = :view_name
                    AND LOWER(metric_name) = :metric_name
                    AND metric_date >= :start_date
                    ORDER BY metric_date DESC
                """
                params = {
                    "view_name": view_name,
                    "metric_name": metric_name.lower(),
                    "start_date": start_date
                }
            else:
                metrics_query = """
                    SELECT 
                        metric_name,
                        column_name,
                        metric_value,
                        metric_date,
                        created_at
                    FROM data_catalog_metrics
                    WHERE LOWER(view_name) = :view_name
                    AND metric_date >= :start_date
                    ORDER BY metric_name, metric_date DESC
                """
                params = {
                    "view_name": view_name,
                    "start_date": start_date
                }
            
            # Execute query
            df = self.data_service.execute_query(metrics_query, params=params)
            
            if df.empty:
                # Try with v_ prefix variation
                if view_name.startswith('v_'):
                    alt_view_name = view_name[2:]
                else:
                    alt_view_name = f'v_{view_name}'
                
                params["view_name"] = alt_view_name
                df = self.data_service.execute_query(metrics_query, params=params)
                
                if not df.empty:
                    view_name = alt_view_name
            
            if df.empty:
                return {
                    "success": True,
                    "result": {
                        "found": False,
                        "view_name": view_name,
                        "message": f"No metrics found for view '{view_name}' in the last {days_back} days"
                    }
                }
            
            # Process results
            metrics_by_name = {}
            all_metrics = []
            
            for _, row in df.iterrows():
                metric = {
                    "name": row['metric_name'],
                    "column": row['column_name'] if pd.notna(row['column_name']) else None,
                    "value": row['metric_value'],
                    "date": str(row['metric_date']) if pd.notna(row['metric_date']) else "Unknown",
                    "created_at": str(row['created_at']) if pd.notna(row['created_at']) else "Unknown"
                }
                all_metrics.append(metric)
                
                # Group by metric name for analysis
                if row['metric_name'] not in metrics_by_name:
                    metrics_by_name[row['metric_name']] = []
                metrics_by_name[row['metric_name']].append(metric)
            
            # Calculate trends and latest values
            metrics_summary = []
            for name, metric_list in metrics_by_name.items():
                # Sort by date
                sorted_metrics = sorted(metric_list, key=lambda x: x['date'], reverse=True)
                latest = sorted_metrics[0]
                
                summary = {
                    "metric_name": name,
                    "latest_value": latest['value'],
                    "latest_date": latest['date'],
                    "column": latest['column'],
                    "data_points": len(metric_list)
                }
                
                # Calculate trend if we have multiple data points
                if len(metric_list) > 1:
                    try:
                        # Convert values to float for numeric metrics
                        values = []
                        for m in sorted_metrics:
                            # Handle percentage values
                            if '%' in str(m['value']):
                                values.append(float(m['value'].replace('%', '')))
                            else:
                                values.append(float(m['value']))
                        
                        if len(values) >= 2:
                            current = values[0]
                            previous = values[1]
                            if previous != 0:
                                change_pct = ((current - previous) / abs(previous)) * 100
                                summary['trend'] = {
                                    "direction": "up" if change_pct > 0 else "down" if change_pct < 0 else "stable",
                                    "change_percent": round(change_pct, 2),
                                    "previous_value": sorted_metrics[1]['value']
                                }
                    except (ValueError, TypeError):
                        # Non-numeric metric, can't calculate trend
                        pass
                
                metrics_summary.append(summary)
            
            # Generate insights
            insights = self._generate_metric_insights(metrics_summary, view_name)
            
            result = {
                "found": True,
                "view_name": view_name,
                "metric_count": len(metrics_by_name),
                "date_range": {
                    "start": str(start_date),
                    "end": str(end_date),
                    "days": days_back
                },
                "metrics_summary": metrics_summary,
                "all_metrics": all_metrics if metric_name else None,  # Include raw data only for specific metric queries
                "insights": insights
            }
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error retrieving metrics: {str(e)}",
                "result": {
                    "view_name": view_name,
                    "error_details": str(e)
                }
            }
    
    def _generate_metric_insights(self, metrics_summary: list, view_name: str) -> list:
        """Generate insights based on metrics data"""
        insights = []
        
        # Look for concerning trends
        declining_metrics = [m for m in metrics_summary if m.get('trend', {}).get('direction') == 'down']
        improving_metrics = [m for m in metrics_summary if m.get('trend', {}).get('direction') == 'up']
        
        if declining_metrics:
            for metric in declining_metrics:
                if 'growth' in metric['metric_name'].lower() or 'rate' in metric['metric_name'].lower():
                    insights.append(f"{metric['metric_name']} is declining by {abs(metric['trend']['change_percent'])}%")
        
        if improving_metrics:
            for metric in improving_metrics:
                if 'default' not in metric['metric_name'].lower():  # Don't celebrate increasing default rates
                    insights.append(f"{metric['metric_name']} has improved by {metric['trend']['change_percent']}%")
        
        # Check for specific metric thresholds
        for metric in metrics_summary:
            if 'default_rate' in metric['metric_name'].lower():
                try:
                    value = float(metric['latest_value'].replace('%', ''))
                    if value > 5:
                        insights.append(f"Default rate ({value}%) is above typical threshold of 5%")
                except:
                    pass
            
            if 'growth_rate' in metric['metric_name'].lower():
                try:
                    value = float(metric['latest_value'].replace('%', ''))
                    if value < 0:
                        insights.append(f"Negative growth rate detected: {value}%")
                except:
                    pass
        
        # Add general insights if none specific
        if not insights:
            if metrics_summary:
                insights.append(f"Tracking {len(metrics_summary)} metrics for {view_name}")
                insights.append("All metrics appear to be within normal ranges")
            else:
                insights.append("No recent metrics available for analysis")
        
        return insights