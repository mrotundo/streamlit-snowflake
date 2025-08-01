from typing import Dict, Any, Optional
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
import pandas as pd


class CheckViewDataTool(BaseTool):
    """Tool for querying views to validate data concerns"""
    
    def __init__(self, data_service: Optional[DataInterface] = None):
        super().__init__(
            name="CheckViewData",
            description="Query a view or table to check its current data state"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "view_name": {
                "type": "string",
                "description": "Name of the view or table to query"
            },
            "limit": {
                "type": "integer",
                "description": "Number of rows to return (default: 10)",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Query a view and return sample data with statistics"""
        view_name = kwargs.get("view_name", "")
        limit = kwargs.get("limit", 10)
        
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
            # Get total row count
            count_query = f"SELECT COUNT(*) as total_count FROM {view_name}"
            count_df = self.data_service.execute_query(count_query)
            total_count = count_df.iloc[0]['total_count'] if not count_df.empty else 0
            
            # Get sample data
            sample_query = f"SELECT * FROM {view_name} LIMIT {limit}"
            sample_df = self.data_service.execute_query(sample_query)
            
            # Get column information
            columns_info = []
            if not sample_df.empty:
                for col in sample_df.columns:
                    col_info = {
                        "name": col,
                        "type": str(sample_df[col].dtype),
                        "sample_values": sample_df[col].dropna().head(3).tolist()
                    }
                    
                    # Add statistics for numeric columns
                    if pd.api.types.is_numeric_dtype(sample_df[col]):
                        col_info["min"] = float(sample_df[col].min()) if not sample_df[col].isna().all() else None
                        col_info["max"] = float(sample_df[col].max()) if not sample_df[col].isna().all() else None
                        col_info["mean"] = float(sample_df[col].mean()) if not sample_df[col].isna().all() else None
                    
                    columns_info.append(col_info)
            
            # Check for specific data quality indicators
            quality_checks = []
            
            # Check if view is empty
            if total_count == 0:
                quality_checks.append({
                    "check": "Empty View",
                    "status": "WARNING",
                    "message": f"{view_name} has no data"
                })
            
            # Check for null values in key columns
            if not sample_df.empty:
                for col in sample_df.columns:
                    null_count = sample_df[col].isna().sum()
                    if null_count > 0:
                        quality_checks.append({
                            "check": f"Null Values in {col}",
                            "status": "INFO",
                            "message": f"{null_count} null values found in {col}"
                        })
            
            # Special checks for specific views
            if 'executive_dashboard' in view_name.lower() and not sample_df.empty:
                # Check for reasonable customer counts
                if 'total_customers' in sample_df.columns:
                    customer_count = sample_df['total_customers'].iloc[0]
                    if customer_count < 100:
                        quality_checks.append({
                            "check": "Low Customer Count",
                            "status": "WARNING",
                            "message": f"Customer count ({customer_count}) seems unusually low"
                        })
            
            result = {
                "view_name": view_name,
                "total_rows": total_count,
                "sample_size": len(sample_df),
                "columns": columns_info,
                "sample_data": sample_df.to_dict('records'),
                "quality_checks": quality_checks
            }
            
            # Add timestamp information if available
            timestamp_columns = [col for col in sample_df.columns if 'date' in col.lower() or 'time' in col.lower()]
            if timestamp_columns and not sample_df.empty:
                timestamps = {}
                for col in timestamp_columns:
                    if pd.api.types.is_datetime64_any_dtype(sample_df[col]):
                        timestamps[col] = {
                            "min": str(sample_df[col].min()),
                            "max": str(sample_df[col].max())
                        }
                result["timestamp_info"] = timestamps
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error querying {view_name}: {str(e)}",
                "result": {
                    "view_name": view_name,
                    "error_details": str(e)
                }
            }