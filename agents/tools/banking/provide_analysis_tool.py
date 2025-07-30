from typing import Dict, Any
from agents.tools.base_tool import BaseTool
from services.llm_interface import LLMInterface
import json


class ProvideAnalysisTool(BaseTool):
    """Tool for analyzing data and providing insights"""
    
    def __init__(self, llm_service: LLMInterface, model: str):
        super().__init__(
            name="ProvideAnalysis",
            description="Analyze data to answer specific questions and provide insights"
        )
        self.llm_service = llm_service
        self.model = model
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "data": {
                "type": "dict",
                "description": "Data to analyze"
            },
            "question": {
                "type": "string",
                "description": "Specific question to answer with the data"
            },
            "analysis_type": {
                "type": "string",
                "description": "Type of analysis: trend, comparison, summary, recommendation",
                "optional": True
            },
            "comparison_data": {
                "type": "dict",
                "description": "Additional data for comparison analysis",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Analyze data and provide insights"""
        data = kwargs.get("data", {})
        question = kwargs.get("question", "")
        analysis_type = kwargs.get("analysis_type", "general")
        comparison_data = kwargs.get("comparison_data", None)
        
        # If comparison_data is provided, restructure data for comparison
        if comparison_data and analysis_type == "time_comparison":
            data = {
                "current_period": data,
                "comparison_period": comparison_data
            }
        
        try:
            # Prepare the data summary for analysis
            data_summary = self._summarize_data(data)
            
            # Use LLM to analyze and answer the question
            is_comparison = data_summary.get("is_comparison", False)
            
            if is_comparison:
                prompt = f"""Analyze this banking data comparison to answer the question.

Question: "{question}"
Analysis Type: time_comparison

Data Summary:
{json.dumps(data_summary, indent=2)}

Focus on:
1. Compare the current period vs comparison period performance
2. Calculate and explain percentage changes
3. Identify significant improvements or declines
4. Provide context for the changes
5. Suggest actions based on the comparison

Format your response as JSON with:
- answer: Direct answer comparing the periods
- insights: List of key insights about the comparison
- trends: Performance trends between periods
- recommendations: Actions based on the comparison
- risks: Any concerning trends

Be specific with numbers and percentages. Respond with ONLY valid JSON."""
            else:
                prompt = f"""Analyze this banking data to answer the question.

Question: "{question}"
Analysis Type: {analysis_type}

Data Summary:
{json.dumps(data_summary, indent=2)}

Provide:
1. Direct answer to the question
2. Key insights from the data
3. Relevant trends or patterns
4. Recommendations if applicable
5. Any risks or concerns

Be specific and use the actual numbers from the data. Format your response as JSON with sections for:
- answer: Direct answer to the question
- insights: List of key insights
- trends: Any identified trends
- recommendations: List of actionable recommendations
- risks: Any identified risks

Respond with ONLY valid JSON."""

            messages = [
                {"role": "system", "content": "You are a banking data analyst. Analyze data and provide actionable insights."},
                {"role": "user", "content": prompt}
            ]
            
            response = self.llm_service.complete(messages, model=self.model, temperature=0.2)
            
            # Parse the analysis
            try:
                analysis = json.loads(response)
            except json.JSONDecodeError:
                # Try to extract insights from the response
                analysis = self._extract_analysis_from_text(response, question, data_summary)
            
            # Ensure all expected sections exist
            analysis = self._ensure_analysis_structure(analysis)
            
            # Add metadata
            result = {
                "question": question,
                "data_analyzed": {
                    "row_count": data.get("row_count", 0),
                    "metrics_available": list(data_summary.get("summary_stats", {}).keys())
                },
                "analysis": analysis,
                "confidence": self._calculate_confidence(data, analysis)
            }
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "result": {
                    "question": question,
                    "analysis": {
                        "answer": "Unable to analyze the data due to an error.",
                        "insights": ["Data analysis failed"],
                        "recommendations": ["Please check the data format and try again"]
                    }
                }
            }
    
    def _summarize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract key information from the data for analysis"""
        summary = {}
        
        # Handle different data structures
        if isinstance(data, dict):
            # Check if this is a comparison structure
            if "current_period" in data and "comparison_period" in data:
                # Handle period comparison data
                summary["is_comparison"] = True
                summary["current_period"] = self._extract_period_data(data["current_period"])
                summary["comparison_period"] = self._extract_period_data(data["comparison_period"])
                
                # Calculate differences
                if summary["current_period"].get("summary_stats") and summary["comparison_period"].get("summary_stats"):
                    summary["changes"] = self._calculate_changes(
                        summary["current_period"]["summary_stats"],
                        summary["comparison_period"]["summary_stats"]
                    )
            elif "data" in data:
                # This is likely output from RunQuery
                inner_data = data["data"]
                summary["summary_stats"] = inner_data.get("summary_stats", {})
                summary["data_points_count"] = len(inner_data.get("data_points", []))
                summary["trends"] = inner_data.get("trends", {})
                summary["breakdowns"] = inner_data.get("breakdowns", {})
                summary["period_label"] = inner_data.get("period_label", "Current Period")
                
                # Add sample data points for context
                if "data_points" in inner_data and inner_data["data_points"]:
                    summary["sample_records"] = inner_data["data_points"][:3]
            else:
                # Direct data structure
                summary = data
        
        return summary
    
    def _extract_period_data(self, period_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from a period result"""
        if isinstance(period_data, dict):
            if "result" in period_data:
                # This is output from a tool execution
                return period_data["result"]
            elif "data" in period_data:
                return period_data["data"]
        return period_data
    
    def _calculate_changes(self, current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate percentage changes between periods"""
        changes = {}
        for key in current:
            if key in previous and isinstance(current[key], (int, float)) and isinstance(previous[key], (int, float)):
                if previous[key] != 0:
                    change_pct = ((current[key] - previous[key]) / previous[key]) * 100
                    changes[key] = {
                        "current": current[key],
                        "previous": previous[key],
                        "change_percent": round(change_pct, 2),
                        "change_absolute": current[key] - previous[key]
                    }
        return changes
    
    def _extract_analysis_from_text(self, text: str, question: str, data: Dict) -> Dict[str, Any]:
        """Extract analysis components from text response"""
        # Basic extraction when JSON parsing fails
        return {
            "answer": f"Based on the data: {text[:200]}...",
            "insights": [
                "Data shows significant patterns",
                f"Total records analyzed: {data.get('data_points_count', 'unknown')}"
            ],
            "trends": ["Further analysis needed for trend identification"],
            "recommendations": ["Review the complete dataset for more detailed insights"],
            "risks": ["Limited data availability may affect accuracy"]
        }
    
    def _ensure_analysis_structure(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure the analysis has all expected sections"""
        default_structure = {
            "answer": "Analysis completed",
            "insights": [],
            "trends": [],
            "recommendations": [],
            "risks": []
        }
        
        # Merge with defaults
        for key, default_value in default_structure.items():
            if key not in analysis:
                analysis[key] = default_value
            elif not analysis[key]:  # Empty list or string
                analysis[key] = default_value
        
        return analysis
    
    def _calculate_confidence(self, data: Dict, analysis: Dict) -> float:
        """Calculate confidence score based on data quality and analysis completeness"""
        confidence = 0.5  # Base confidence
        
        # Increase confidence based on data availability
        if data.get("row_count", 0) > 0:
            confidence += 0.1
        if data.get("data", {}).get("summary_stats"):
            confidence += 0.2
        if len(analysis.get("insights", [])) > 2:
            confidence += 0.1
        if analysis.get("answer") and len(analysis["answer"]) > 50:
            confidence += 0.1
        
        return min(confidence, 0.95)  # Cap at 95%