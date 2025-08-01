from typing import Dict, Any, Optional
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
import pandas as pd


class GetViewExamplesTool(BaseTool):
    """Tool for retrieving example queries for views from the data catalog"""
    
    def __init__(self, data_service: Optional[DataInterface] = None):
        super().__init__(
            name="GetViewExamples",
            description="Get example queries and use cases for a database view"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "view_name": {
                "type": "string",
                "description": "Name of the view to get examples for"
            },
            "example_type": {
                "type": "string",
                "description": "Type of examples to retrieve (optional - e.g., 'Basic Query', 'Filtering', 'Aggregation')",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Retrieve example queries from the catalog"""
        view_name = kwargs.get("view_name", "").lower()
        example_type = kwargs.get("example_type", "")
        
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
            # Build query based on whether we want specific type or all examples
            if example_type:
                examples_query = """
                    SELECT 
                        example_type,
                        example_query,
                        example_description,
                        business_context,
                        created_by,
                        created_at
                    FROM data_catalog_examples
                    WHERE LOWER(view_name) = :view_name
                    AND LOWER(example_type) = :example_type
                    ORDER BY created_at DESC
                """
                params = {"view_name": view_name, "example_type": example_type.lower()}
            else:
                examples_query = """
                    SELECT 
                        example_type,
                        example_query,
                        example_description,
                        business_context,
                        created_by,
                        created_at
                    FROM data_catalog_examples
                    WHERE LOWER(view_name) = :view_name
                    ORDER BY example_type, created_at DESC
                """
                params = {"view_name": view_name}
            
            # Execute query
            df = self.data_service.execute_query(examples_query, params=params)
            
            if df.empty:
                # Try with v_ prefix variation
                if view_name.startswith('v_'):
                    alt_view_name = view_name[2:]
                else:
                    alt_view_name = f'v_{view_name}'
                
                params["view_name"] = alt_view_name
                df = self.data_service.execute_query(examples_query, params=params)
                
                if not df.empty:
                    view_name = alt_view_name
            
            if df.empty:
                return {
                    "success": True,
                    "result": {
                        "found": False,
                        "view_name": view_name,
                        "message": f"No examples found for view '{view_name}'"
                    }
                }
            
            # Process results
            examples = []
            example_types = set()
            
            for _, row in df.iterrows():
                example = {
                    "type": row['example_type'],
                    "query": row['example_query'],
                    "description": row['example_description'] if pd.notna(row['example_description']) else "No description available",
                    "business_context": row['business_context'] if pd.notna(row['business_context']) else "No business context provided",
                    "created_by": row['created_by'] if pd.notna(row['created_by']) else "Unknown",
                    "created_at": str(row['created_at']) if pd.notna(row['created_at']) else "Unknown"
                }
                examples.append(example)
                example_types.add(row['example_type'])
            
            # Generate usage tips based on available examples
            usage_tips = self._generate_usage_tips(examples, view_name)
            
            result = {
                "found": True,
                "view_name": view_name,
                "example_count": len(examples),
                "example_types": list(example_types),
                "examples": examples,
                "usage_tips": usage_tips
            }
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error retrieving examples: {str(e)}",
                "result": {
                    "view_name": view_name,
                    "error_details": str(e)
                }
            }
    
    def _generate_usage_tips(self, examples: list, view_name: str) -> list:
        """Generate helpful usage tips based on available examples"""
        tips = []
        
        # Check what types of examples we have
        example_types = {ex['type'] for ex in examples}
        
        if 'Basic Query' in example_types:
            tips.append("Start with a basic SELECT * query to explore the data structure")
        
        if 'Filtering' in example_types:
            tips.append("Use WHERE clauses to filter data based on specific criteria")
        
        if 'Aggregation' in example_types:
            tips.append("This view supports aggregation functions like SUM, AVG, COUNT")
        
        if 'Risk Analysis' in example_types:
            tips.append("This view is optimized for risk assessment queries")
        
        if 'Top Customers' in example_types:
            tips.append("Use ORDER BY and LIMIT to find top performers or outliers")
        
        # Add view-specific tips
        if 'executive' in view_name.lower():
            tips.append("This view provides high-level metrics suitable for executive reporting")
        elif 'customer' in view_name.lower():
            tips.append("This view focuses on customer-level data and relationships")
        elif 'loan' in view_name.lower():
            tips.append("This view contains loan-specific metrics and performance data")
        elif 'risk' in view_name.lower():
            tips.append("This view is designed for risk analysis and monitoring")
        
        # General tips
        if not tips:
            tips.append("Review the example queries to understand common usage patterns")
            tips.append("Consider the business context when formulating your queries")
        
        return tips