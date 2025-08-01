from typing import Dict, Any, Optional
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
import pandas as pd


class GetViewCatalogTool(BaseTool):
    """Tool for retrieving view metadata from the data catalog"""
    
    def __init__(self, data_service: Optional[DataInterface] = None):
        super().__init__(
            name="GetViewCatalog",
            description="Retrieve comprehensive metadata about a database view from the catalog"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "view_name": {
                "type": "string",
                "description": "Name of the view to get catalog information for"
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Retrieve view metadata from the catalog"""
        view_name = kwargs.get("view_name", "").lower()
        
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
            # Query view metadata from catalog
            catalog_query = """
                SELECT 
                    view_name,
                    view_type,
                    business_description,
                    technical_description,
                    owner,
                    data_domain,
                    refresh_frequency,
                    last_refreshed,
                    row_count,
                    created_at,
                    updated_at
                FROM data_catalog_views
                WHERE LOWER(view_name) = :view_name
            """
            
            # Execute query with named parameter
            df = self.data_service.execute_query(
                catalog_query,
                params={"view_name": view_name}
            )
            
            if df.empty:
                # Try without v_ prefix if not found
                if view_name.startswith('v_'):
                    alt_view_name = view_name[2:]
                else:
                    alt_view_name = f'v_{view_name}'
                
                df = self.data_service.execute_query(
                    catalog_query,
                    params={"view_name": alt_view_name}
                )
                
                if not df.empty:
                    view_name = alt_view_name
            
            if df.empty:
                return {
                    "success": True,
                    "result": {
                        "found": False,
                        "view_name": view_name,
                        "message": f"No catalog information found for view '{view_name}'"
                    }
                }
            
            # Convert to dictionary
            view_info = df.iloc[0].to_dict()
            
            # Clean up timestamps for JSON serialization
            for col in ['last_refreshed', 'created_at', 'updated_at']:
                if col in view_info and pd.notna(view_info[col]):
                    view_info[col] = str(view_info[col])
            
            # Get related views count
            related_query = """
                SELECT COUNT(DISTINCT view_name) as related_count
                FROM data_catalog_views
                WHERE data_domain = :domain
                AND LOWER(view_name) != :view_name
            """
            
            related_df = self.data_service.execute_query(
                related_query,
                params={
                    "domain": view_info.get('data_domain', ''),
                    "view_name": view_name
                }
            )
            
            related_count = related_df.iloc[0]['related_count'] if not related_df.empty else 0
            
            result = {
                "found": True,
                "view_name": view_info['view_name'],
                "metadata": {
                    "type": view_info.get('view_type', 'Unknown'),
                    "owner": view_info.get('owner', 'Unknown'),
                    "domain": view_info.get('data_domain', 'Unknown'),
                    "descriptions": {
                        "business": view_info.get('business_description', 'No business description available'),
                        "technical": view_info.get('technical_description', 'No technical description available')
                    },
                    "data_info": {
                        "row_count": view_info.get('row_count', 0),
                        "refresh_frequency": view_info.get('refresh_frequency', 'Unknown'),
                        "last_refreshed": view_info.get('last_refreshed', 'Unknown')
                    },
                    "catalog_info": {
                        "created_at": view_info.get('created_at', 'Unknown'),
                        "updated_at": view_info.get('updated_at', 'Unknown'),
                        "related_views_in_domain": related_count
                    }
                }
            }
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error retrieving catalog info for {view_name}: {str(e)}",
                "result": {
                    "view_name": view_name,
                    "error_details": str(e)
                }
            }