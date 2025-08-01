from typing import Dict, Any, Optional, List
from agents.tools.base_tool import BaseTool
from services.llm_interface import LLMInterface
import json
import pandas as pd


class AnalyzeViewDetailsTool(BaseTool):
    """Tool for analyzing and synthesizing catalog information into comprehensive responses"""
    
    def __init__(self, llm_service: Optional[LLMInterface] = None, model: str = "gpt-4"):
        super().__init__(
            name="AnalyzeViewDetails",
            description="Analyze and format catalog data into user-friendly responses"
        )
        self.llm_service = llm_service
        self.model = model
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "catalog_data": {
                "type": "dict",
                "description": "View catalog metadata from GetViewCatalog tool"
            },
            "column_data": {
                "type": "dict",
                "description": "Column details from GetColumnDetails tool",
                "optional": True
            },
            "examples_data": {
                "type": "dict",
                "description": "Example queries from GetViewExamples tool",
                "optional": True
            },
            "metrics_data": {
                "type": "dict",
                "description": "Metrics data from GetViewMetrics tool",
                "optional": True
            },
            "user_query": {
                "type": "string",
                "description": "The original user query for context"
            },
            "response_format": {
                "type": "string",
                "description": "Desired format: 'detailed', 'summary', or 'tabular'",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Analyze and synthesize catalog information"""
        catalog_data = kwargs.get("catalog_data", {})
        column_data = kwargs.get("column_data", {})
        examples_data = kwargs.get("examples_data", {})
        metrics_data = kwargs.get("metrics_data", {})
        user_query = kwargs.get("user_query", "")
        response_format = kwargs.get("response_format", "detailed")
        
        # Extract data from nested tool outputs if needed
        if isinstance(catalog_data, dict) and "result" in catalog_data:
            catalog_data = catalog_data.get("result", {})
        if isinstance(column_data, dict) and "result" in column_data:
            column_data = column_data.get("result", {})
        if isinstance(examples_data, dict) and "result" in examples_data:
            examples_data = examples_data.get("result", {})
        if isinstance(metrics_data, dict) and "result" in metrics_data:
            metrics_data = metrics_data.get("result", {})
        
        if not catalog_data or not catalog_data.get("found", False):
            return {
                "success": False,
                "error": "No catalog data provided or view not found",
                "result": {}
            }
        
        try:
            # Determine the type of response needed based on user query
            query_lower = user_query.lower()
            is_field_query = any(word in query_lower for word in ['field', 'column', 'attribute'])
            is_tabular_request = any(word in query_lower for word in ['table', 'tabular', 'show me'])
            
            # Build the response based on query type
            if is_field_query and column_data and column_data.get("column_details"):
                # Single field/column query
                response = self._format_field_response(
                    catalog_data, 
                    column_data,
                    user_query
                )
            elif is_tabular_request or response_format == "tabular":
                # Tabular representation requested
                response = self._format_tabular_response(
                    catalog_data,
                    column_data,
                    examples_data,
                    metrics_data
                )
            else:
                # General view information
                response = self._format_view_response(
                    catalog_data,
                    column_data,
                    examples_data,
                    metrics_data,
                    response_format
                )
            
            # Add sample data if requested
            if 'sample' in query_lower or 'example data' in query_lower:
                response["sample_data_query"] = self._generate_sample_query(
                    catalog_data.get("view_name", ""),
                    column_data
                )
            
            return {
                "success": True,
                "result": response
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error analyzing view details: {str(e)}",
                "result": {}
            }
    
    def _format_field_response(self, catalog_data: Dict, column_data: Dict, user_query: str) -> Dict[str, Any]:
        """Format response for a specific field/column query"""
        view_name = catalog_data.get("view_name", "")
        view_metadata = catalog_data.get("metadata", {})
        column_details = column_data.get("column_details", {})
        
        # Build the summary
        summary_parts = []
        
        # Column basic info
        summary_parts.append(f"**{column_details['name']}** in {view_name}")
        summary_parts.append(f"- Type: {column_details['type']}")
        summary_parts.append(f"- Nullable: {'Yes' if column_details['nullable'] else 'No'}")
        
        if column_details.get('primary_key'):
            summary_parts.append("- **Primary Key**")
        
        # Descriptions
        summary_parts.append(f"\n**Business Description:**\n{column_details['descriptions']['business']}")
        summary_parts.append(f"\n**Technical Description:**\n{column_details['descriptions']['technical']}")
        
        # Data classification
        summary_parts.append(f"\n**Data Classification:** {column_details['classification']}")
        
        # Examples and valid values
        if column_details.get('examples'):
            summary_parts.append(f"\n**Example Values:** {', '.join(column_details['examples'])}")
        
        if column_details.get('valid_values'):
            summary_parts.append(f"\n**Valid Values:** {', '.join(column_details['valid_values'])}")
        
        # Context about the view
        summary_parts.append(f"\n**View Context:**")
        summary_parts.append(f"The {view_name} view is a {view_metadata['type']} that {view_metadata['descriptions']['business']}")
        
        # Usage hints
        if column_data.get('context', {}).get('usage_hints'):
            summary_parts.append(f"\n**Usage Hints:**")
            for hint in column_data['context']['usage_hints']:
                summary_parts.append(f"- {hint}")
        
        return {
            "summary": "\n".join(summary_parts),
            "field_info": column_details,
            "view_context": {
                "view_name": view_name,
                "view_type": view_metadata['type'],
                "owner": view_metadata['owner']
            }
        }
    
    def _format_view_response(self, catalog_data: Dict, column_data: Dict, 
                            examples_data: Dict, metrics_data: Dict,
                            response_format: str) -> Dict[str, Any]:
        """Format response for general view information"""
        view_name = catalog_data.get("view_name", "")
        metadata = catalog_data.get("metadata", {})
        
        # Build comprehensive summary
        summary_parts = []
        
        # View overview
        summary_parts.append(f"# {view_name}")
        summary_parts.append(f"\n**Type:** {metadata['type']}")
        summary_parts.append(f"**Owner:** {metadata['owner']}")
        summary_parts.append(f"**Domain:** {metadata['domain']}")
        
        # Descriptions
        summary_parts.append(f"\n## Description")
        summary_parts.append(metadata['descriptions']['business'])
        
        if response_format == "detailed":
            summary_parts.append(f"\n**Technical Details:** {metadata['descriptions']['technical']}")
        
        # Data information
        data_info = metadata.get('data_info', {})
        summary_parts.append(f"\n## Data Information")
        summary_parts.append(f"- **Row Count:** {data_info.get('row_count', 'Unknown'):,}")
        summary_parts.append(f"- **Refresh Frequency:** {data_info.get('refresh_frequency', 'Unknown')}")
        summary_parts.append(f"- **Last Refreshed:** {data_info.get('last_refreshed', 'Unknown')}")
        
        # Columns summary
        if column_data and column_data.get('found'):
            summary_parts.append(f"\n## Columns ({column_data.get('column_count', 0)})")
            
            if response_format == "summary":
                # Just show column names and types
                columns = column_data.get('columns', [])[:10]  # Show first 10
                for col in columns:
                    summary_parts.append(f"- **{col['name']}** ({col['type']})")
                if len(column_data.get('columns', [])) > 10:
                    summary_parts.append(f"- ... and {len(column_data['columns']) - 10} more columns")
            else:
                # Show detailed column info
                self._add_detailed_columns(summary_parts, column_data)
        
        # Examples
        if examples_data and examples_data.get('found'):
            summary_parts.append(f"\n## Example Queries")
            for example in examples_data.get('examples', [])[:3]:  # Show first 3
                summary_parts.append(f"\n**{example['type']}:**")
                summary_parts.append(f"```sql\n{example['query']}\n```")
                summary_parts.append(f"*{example['description']}*")
        
        # Metrics insights
        if metrics_data and metrics_data.get('found'):
            summary_parts.append(f"\n## Current Metrics")
            for insight in metrics_data.get('insights', []):
                summary_parts.append(f"- {insight}")
        
        # Usage tips
        if examples_data and examples_data.get('usage_tips'):
            summary_parts.append(f"\n## Usage Tips")
            for tip in examples_data['usage_tips']:
                summary_parts.append(f"- {tip}")
        
        return {
            "summary": "\n".join(summary_parts),
            "metadata": metadata,
            "column_count": column_data.get('column_count', 0) if column_data else 0,
            "has_examples": bool(examples_data and examples_data.get('found')),
            "has_metrics": bool(metrics_data and metrics_data.get('found'))
        }
    
    def _format_tabular_response(self, catalog_data: Dict, column_data: Dict,
                                examples_data: Dict, metrics_data: Dict) -> Dict[str, Any]:
        """Format response as tabular representation"""
        view_name = catalog_data.get("view_name", "")
        
        # Create column table
        column_table = []
        if column_data and column_data.get('columns'):
            column_table = [
                "| Column Name | Type | Nullable | Description | Classification |",
                "|-------------|------|----------|-------------|----------------|"
            ]
            
            for col in column_data['columns']:
                nullable = "Yes" if col['nullable'] else "No"
                desc = col['descriptions']['business'][:50] + "..." if len(col['descriptions']['business']) > 50 else col['descriptions']['business']
                
                column_table.append(
                    f"| {col['name']} | {col['type']} | {nullable} | {desc} | {col['classification']} |"
                )
        
        # Build summary with table
        summary_parts = [
            f"# {view_name} - Data Dictionary",
            f"\n{catalog_data['metadata']['descriptions']['business']}",
            f"\n## Table Structure",
            "\n".join(column_table) if column_table else "No column information available",
            f"\n## Quick Facts",
            f"- **Total Columns:** {column_data.get('column_count', 0) if column_data else 'Unknown'}",
            f"- **Row Count:** {catalog_data['metadata']['data_info'].get('row_count', 'Unknown'):,}",
            f"- **Owner:** {catalog_data['metadata']['owner']}",
            f"- **Refresh:** {catalog_data['metadata']['data_info'].get('refresh_frequency', 'Unknown')}"
        ]
        
        # Add sample query
        if examples_data and examples_data.get('examples'):
            summary_parts.append(f"\n## Sample Query")
            example = examples_data['examples'][0]
            summary_parts.append(f"```sql\n{example['query']}\n```")
        
        return {
            "summary": "\n".join(summary_parts),
            "table_format": column_table,
            "view_name": view_name
        }
    
    def _add_detailed_columns(self, summary_parts: List[str], column_data: Dict):
        """Add detailed column information to summary"""
        columns = column_data.get('columns', [])
        col_summary = column_data.get('summary', {})
        
        # Show classification breakdown
        if col_summary.get('classification_breakdown'):
            summary_parts.append("\n**Data Classifications:**")
            for classification, count in col_summary['classification_breakdown'].items():
                summary_parts.append(f"- {classification}: {count} columns")
        
        # Show key columns
        if col_summary.get('key_columns'):
            summary_parts.append(f"\n**Key Columns:** {', '.join(col_summary['key_columns'])}")
        
        # List columns by category
        summary_parts.append("\n**Column Details:**")
        
        # Group columns by type
        text_cols = [c for c in columns if 'VARCHAR' in c['type'] or 'TEXT' in c['type']]
        numeric_cols = [c for c in columns if any(t in c['type'] for t in ['NUMBER', 'INTEGER', 'DECIMAL'])]
        date_cols = [c for c in columns if 'DATE' in c['type'] or 'TIME' in c['type']]
        
        if text_cols:
            summary_parts.append("\n*Text Columns:*")
            for col in text_cols[:5]:  # Show first 5
                summary_parts.append(f"- **{col['name']}** - {col['descriptions']['business']}")
        
        if numeric_cols:
            summary_parts.append("\n*Numeric Columns:*")
            for col in numeric_cols[:5]:
                summary_parts.append(f"- **{col['name']}** - {col['descriptions']['business']}")
        
        if date_cols:
            summary_parts.append("\n*Date/Time Columns:*")
            for col in date_cols:
                summary_parts.append(f"- **{col['name']}** - {col['descriptions']['business']}")
    
    def _generate_sample_query(self, view_name: str, column_data: Dict) -> str:
        """Generate a sample query to show data"""
        if not column_data or not column_data.get('columns'):
            return f"SELECT * FROM {view_name} LIMIT 10"
        
        # Select interesting columns
        columns = column_data['columns']
        selected_cols = []
        
        # Try to include key columns
        for col in columns:
            if col.get('primary_key') or 'id' in col['name'].lower():
                selected_cols.append(col['name'])
        
        # Add some descriptive columns
        for col in columns[:7]:  # Limit to 7 columns total
            if col['name'] not in selected_cols:
                selected_cols.append(col['name'])
        
        if selected_cols:
            return f"SELECT {', '.join(selected_cols)} FROM {view_name} LIMIT 10"
        else:
            return f"SELECT * FROM {view_name} LIMIT 10"