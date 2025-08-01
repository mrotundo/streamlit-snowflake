from typing import Dict, Any, Optional, List
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
import pandas as pd


class GetColumnDetailsTool(BaseTool):
    """Tool for retrieving column metadata from the data catalog"""
    
    def __init__(self, data_service: Optional[DataInterface] = None):
        super().__init__(
            name="GetColumnDetails",
            description="Get detailed information about columns in a view from the catalog"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "view_name": {
                "type": "string",
                "description": "Name of the view to get column information for"
            },
            "column_name": {
                "type": "string",
                "description": "Specific column name to get details for (optional - if not provided, returns all columns)",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Retrieve column metadata from the catalog"""
        view_name = kwargs.get("view_name", "").lower()
        column_name = kwargs.get("column_name", "").lower() if kwargs.get("column_name") else None
        
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
            # Build query based on whether we want specific column or all columns
            if column_name:
                columns_query = """
                    SELECT 
                        column_name,
                        column_type,
                        column_order,
                        is_nullable,
                        is_primary_key,
                        business_description,
                        technical_description,
                        data_classification,
                        example_values,
                        valid_values
                    FROM data_catalog_columns
                    WHERE LOWER(view_name) = :view_name
                    AND LOWER(column_name) = :column_name
                    ORDER BY column_order
                """
                params = {"view_name": view_name, "column_name": column_name}
            else:
                columns_query = """
                    SELECT 
                        column_name,
                        column_type,
                        column_order,
                        is_nullable,
                        is_primary_key,
                        business_description,
                        technical_description,
                        data_classification,
                        example_values,
                        valid_values
                    FROM data_catalog_columns
                    WHERE LOWER(view_name) = :view_name
                    ORDER BY column_order
                """
                params = {"view_name": view_name}
            
            # Execute query
            df = self.data_service.execute_query(columns_query, params=params)
            
            if df.empty:
                # Try with v_ prefix variation
                if view_name.startswith('v_'):
                    alt_view_name = view_name[2:]
                else:
                    alt_view_name = f'v_{view_name}'
                
                params["view_name"] = alt_view_name
                df = self.data_service.execute_query(columns_query, params=params)
                
                if not df.empty:
                    view_name = alt_view_name
            
            if df.empty:
                if column_name:
                    message = f"No column '{column_name}' found in view '{view_name}'"
                else:
                    message = f"No columns found for view '{view_name}'"
                
                return {
                    "success": True,
                    "result": {
                        "found": False,
                        "view_name": view_name,
                        "column_name": column_name,
                        "message": message
                    }
                }
            
            # Process results
            columns = []
            for _, row in df.iterrows():
                col_info = {
                    "name": row['column_name'],
                    "type": row['column_type'],
                    "order": int(row['column_order']) if pd.notna(row['column_order']) else 0,
                    "nullable": bool(row['is_nullable']) if pd.notna(row['is_nullable']) else True,
                    "primary_key": bool(row['is_primary_key']) if pd.notna(row['is_primary_key']) else False,
                    "descriptions": {
                        "business": row['business_description'] if pd.notna(row['business_description']) else "No business description available",
                        "technical": row['technical_description'] if pd.notna(row['technical_description']) else "No technical description available"
                    },
                    "classification": row['data_classification'] if pd.notna(row['data_classification']) else "Unclassified",
                    "examples": row['example_values'].split(', ') if pd.notna(row['example_values']) else [],
                    "valid_values": row['valid_values'].split(', ') if pd.notna(row['valid_values']) else None
                }
                columns.append(col_info)
            
            # Get column statistics if available
            if column_name and len(columns) == 1:
                # Single column details
                result = {
                    "found": True,
                    "view_name": view_name,
                    "column_details": columns[0],
                    "context": self._get_column_context(view_name, column_name)
                }
            else:
                # Multiple columns
                result = {
                    "found": True,
                    "view_name": view_name,
                    "column_count": len(columns),
                    "columns": columns,
                    "summary": self._generate_columns_summary(columns)
                }
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error retrieving column details: {str(e)}",
                "result": {
                    "view_name": view_name,
                    "column_name": column_name,
                    "error_details": str(e)
                }
            }
    
    def _get_column_context(self, view_name: str, column_name: str) -> Dict[str, Any]:
        """Get additional context about how this column is used"""
        context = {
            "usage_hints": []
        }
        
        # Add usage hints based on column name patterns
        if 'id' in column_name.lower():
            context["usage_hints"].append("This appears to be an identifier field - use for joining or filtering specific records")
        if 'date' in column_name.lower() or 'time' in column_name.lower():
            context["usage_hints"].append("This is a temporal field - useful for time-based analysis and filtering")
        if 'amount' in column_name.lower() or 'value' in column_name.lower() or 'balance' in column_name.lower():
            context["usage_hints"].append("This is a monetary field - use for financial calculations and aggregations")
        if 'status' in column_name.lower():
            context["usage_hints"].append("This is a status field - use for filtering by state or condition")
        if 'name' in column_name.lower():
            context["usage_hints"].append("This is a descriptive field - use for display and searching")
        
        return context
    
    def _generate_columns_summary(self, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate a summary of the columns"""
        summary = {
            "total_columns": len(columns),
            "key_columns": [],
            "classification_breakdown": {},
            "data_types": {}
        }
        
        for col in columns:
            # Track key columns
            if col.get("primary_key"):
                summary["key_columns"].append(col["name"])
            
            # Track classifications
            classification = col.get("classification", "Unclassified")
            summary["classification_breakdown"][classification] = summary["classification_breakdown"].get(classification, 0) + 1
            
            # Track data types
            data_type = col.get("type", "Unknown")
            # Simplify data type for grouping
            if "VARCHAR" in data_type or "TEXT" in data_type:
                simple_type = "Text"
            elif "NUMBER" in data_type or "INTEGER" in data_type or "DECIMAL" in data_type:
                simple_type = "Numeric"
            elif "DATE" in data_type or "TIME" in data_type:
                simple_type = "DateTime"
            elif "BOOLEAN" in data_type:
                simple_type = "Boolean"
            else:
                simple_type = "Other"
            
            summary["data_types"][simple_type] = summary["data_types"].get(simple_type, 0) + 1
        
        return summary