from typing import Dict, Any, List
from agents.tools.base_tool import BaseTool
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd


class VisualizationTool(BaseTool):
    """Tool for creating banking data visualizations"""
    
    def __init__(self):
        super().__init__(
            name="VisualizationTool",
            description="Generate charts and visualizations for banking data"
        )
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "chart_type": {
                "type": "string",
                "description": "Type of chart: pie, bar, line, scatter, etc."
            },
            "data": {
                "type": "dict",
                "description": "Data to visualize"
            },
            "title": {
                "type": "string",
                "description": "Chart title",
                "optional": True
            },
            "x_label": {
                "type": "string",
                "description": "X-axis label",
                "optional": True
            },
            "y_label": {
                "type": "string",
                "description": "Y-axis label",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Create a visualization based on parameters"""
        chart_type = kwargs.get("chart_type", "bar").lower()
        data = kwargs.get("data", {})
        title = kwargs.get("title", "Banking Analytics")
        
        try:
            if chart_type == "pie":
                fig = self._create_pie_chart(data, title)
            elif chart_type == "bar":
                fig = self._create_bar_chart(data, title, kwargs.get("x_label"), kwargs.get("y_label"))
            elif chart_type == "line":
                fig = self._create_line_chart(data, title, kwargs.get("x_label"), kwargs.get("y_label"))
            elif chart_type == "scatter":
                fig = self._create_scatter_plot(data, title, kwargs.get("x_label"), kwargs.get("y_label"))
            else:
                return {
                    "success": False,
                    "error": f"Unsupported chart type: {chart_type}"
                }
            
            # Configure layout
            fig.update_layout(
                template="plotly_white",
                font=dict(size=12),
                margin=dict(l=50, r=50, t=80, b=50)
            )
            
            return {
                "success": True,
                "result": fig
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def _create_pie_chart(self, data: Dict, title: str) -> go.Figure:
        """Create a pie chart"""
        # Expecting data like {"labels": [...], "values": [...]}
        if isinstance(data, dict) and "labels" not in data:
            # Convert dict to labels/values format
            labels = list(data.keys())
            values = list(data.values())
        else:
            labels = data.get("labels", [])
            values = data.get("values", [])
        
        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hovertemplate='%{label}: %{value}<br>%{percent}<extra></extra>'
        )])
        
        fig.update_layout(title=title)
        return fig
    
    def _create_bar_chart(self, data: Dict, title: str, x_label: str = None, y_label: str = None) -> go.Figure:
        """Create a bar chart"""
        if isinstance(data, dict) and "x" not in data:
            # Convert dict to x/y format
            x = list(data.keys())
            y = list(data.values())
        else:
            x = data.get("x", [])
            y = data.get("y", [])
        
        fig = go.Figure(data=[go.Bar(
            x=x,
            y=y,
            text=[f'{v:,.0f}' if isinstance(v, (int, float)) else str(v) for v in y],
            textposition='auto',
        )])
        
        fig.update_layout(
            title=title,
            xaxis_title=x_label or "Category",
            yaxis_title=y_label or "Value"
        )
        
        return fig
    
    def _create_line_chart(self, data: Dict, title: str, x_label: str = None, y_label: str = None) -> go.Figure:
        """Create a line chart"""
        x = data.get("x", [])
        
        # Handle multiple lines
        if "lines" in data:
            fig = go.Figure()
            for line_name, y_values in data["lines"].items():
                fig.add_trace(go.Scatter(
                    x=x,
                    y=y_values,
                    mode='lines+markers',
                    name=line_name
                ))
        else:
            y = data.get("y", [])
            fig = go.Figure(data=[go.Scatter(
                x=x,
                y=y,
                mode='lines+markers'
            )])
        
        fig.update_layout(
            title=title,
            xaxis_title=x_label or "Time",
            yaxis_title=y_label or "Value"
        )
        
        return fig
    
    def _create_scatter_plot(self, data: Dict, title: str, x_label: str = None, y_label: str = None) -> go.Figure:
        """Create a scatter plot"""
        x = data.get("x", [])
        y = data.get("y", [])
        
        # Optional: size and color dimensions
        marker_dict = {}
        if "size" in data:
            marker_dict["size"] = data["size"]
        if "color" in data:
            marker_dict["color"] = data["color"]
            marker_dict["colorscale"] = "Viridis"
            marker_dict["showscale"] = True
        
        fig = go.Figure(data=[go.Scatter(
            x=x,
            y=y,
            mode='markers',
            marker=marker_dict if marker_dict else None,
            text=data.get("labels", None),
            hovertemplate='%{text}<br>X: %{x}<br>Y: %{y}<extra></extra>' if "labels" in data else None
        )])
        
        fig.update_layout(
            title=title,
            xaxis_title=x_label or "X",
            yaxis_title=y_label or "Y"
        )
        
        return fig