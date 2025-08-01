from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from services.llm_interface import LLMInterface
from services.data_interface import DataInterface
from .plan_executor import PlanExecutor
import json


class BaseAgent(ABC):
    """Abstract base class for all agents in the banking system"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self._tools = []
        self.plan_executor = PlanExecutor()
        self.data_service = None
    
    @abstractmethod
    def can_handle(self, query: str, llm_service: LLMInterface, model: str) -> tuple[bool, float]:
        """
        Determine if this agent can handle the given query.
        
        Args:
            query: The user's input query
            llm_service: LLM service for analysis if needed
            model: Model to use for analysis
            
        Returns:
            Tuple of (can_handle: bool, confidence: float between 0-1)
        """
        pass
    
    def process(
        self, 
        query: str, 
        llm_service: LLMInterface, 
        model: str,
        conversation_history: List[Dict[str, str]] = None,
        debug_callback: callable = None,
        data_service: Optional[DataInterface] = None
    ) -> Dict[str, Any]:
        """
        Process the query using plan-based execution.
        
        Args:
            query: The user's input query
            llm_service: LLM service to use
            model: Model to use
            conversation_history: Previous messages in the conversation
            
        Returns:
            Dictionary containing:
                - response: The text response
                - data: Any data retrieved (optional)
                - visualizations: Any charts/graphs (optional)
                - metadata: Additional information (optional)
                - plan: The execution plan (optional)
                - execution_results: Plan execution details (optional)
        """
        try:
            # Set debug callback if provided
            if debug_callback:
                self.plan_executor.set_debug_callback(debug_callback)
            
            # Set data service if provided
            if data_service:
                self.data_service = data_service
            
            # Initialize tools for this execution
            self._initialize_tools(llm_service, model)
            
            # Create execution plan
            plan = self.create_plan(query, llm_service, model, conversation_history)
            if debug_callback:
                if isinstance(plan, dict):
                    debug_callback(f"Plan created with {len(plan.get('steps', []))} steps")
                else:
                    debug_callback(f"Invalid plan format: {type(plan).__name__}", "ERROR")
                    raise ValueError(f"Plan must be a dictionary, got {type(plan).__name__}")
            
            # Execute the plan
            execution_results = self.plan_executor.execute_plan(plan)
            
            # Format the response based on execution results
            return self._format_execution_response(query, plan, execution_results, llm_service, model)
        except Exception as e:
            # Return error response with debugging info
            import traceback
            return {
                "response": f"I encountered an error while processing your request: {str(e)}",
                "agent": self.name,
                "error": str(e),
                "error_traceback": traceback.format_exc(),
                "plan": getattr(locals(), 'plan', None),
                "execution_results": getattr(locals(), 'execution_results', None)
            }
    
    @abstractmethod
    def get_system_prompt(self) -> str:
        """Get the system prompt for this agent"""
        pass
    
    @property
    def capabilities(self) -> List[str]:
        """List of capabilities this agent provides"""
        return []
    
    @abstractmethod
    def create_plan(
        self,
        query: str,
        llm_service: LLMInterface,
        model: str,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Create an execution plan for the query.
        
        Args:
            query: The user's query
            llm_service: LLM service for planning
            model: Model to use
            conversation_history: Previous conversation context
            
        Returns:
            Execution plan with steps
        """
        pass
    
    def register_tool(self, tool):
        """Register a tool for this agent to use"""
        self._tools.append(tool)
        self.plan_executor.register_tool(tool)
    
    def get_tools(self):
        """Get all registered tools"""
        return self._tools
    
    def _initialize_tools(self, llm_service: LLMInterface, model: str):
        """Initialize tools for this execution"""
        # Always reinitialize to ensure correct LLM service and model
        # Clear existing tools
        self._tools = []
        self.plan_executor.tools_registry = {}
        
        # Import and register banking tools
        from .tools.banking.synthesize_query_tool import SynthesizeQueryTool
        from .tools.banking.run_query_tool import RunQueryTool
        from .tools.banking.provide_analysis_tool import ProvideAnalysisTool
        
        # Create tool instances
        synthesize_tool = SynthesizeQueryTool(llm_service, model)
        run_query_tool = RunQueryTool(llm_service, model, self.data_service)
        analysis_tool = ProvideAnalysisTool(llm_service, model)
        
        # Register tools
        self.register_tool(synthesize_tool)
        self.register_tool(run_query_tool)
        self.register_tool(analysis_tool)
    
    def _format_execution_response(
        self,
        query: str,
        plan: Dict[str, Any],
        execution_results: Dict[str, Any],
        llm_service: LLMInterface,
        model: str
    ) -> Dict[str, Any]:
        """Format the execution results into a response"""
        
        # Extract key information from execution
        final_output = execution_results.get("final_output", {})
        success = execution_results.get("success", False)
        errors = execution_results.get("errors", [])
        
        if success and final_output:
            # Get the analysis from the final step
            if isinstance(final_output, dict) and "analysis" in final_output:
                analysis = final_output["analysis"]
                response_text = analysis.get("answer", "Analysis completed.")
                
                # Add insights and recommendations
                if analysis.get("insights"):
                    response_text += "\n\n**Key Insights:**\n"
                    for insight in analysis["insights"]:
                        response_text += f"• {insight}\n"
                
                if analysis.get("recommendations"):
                    response_text += "\n**Recommendations:**\n"
                    for rec in analysis["recommendations"]:
                        response_text += f"• {rec}\n"
            else:
                # No analysis tool was used, try to format the raw data
                response_text = self._format_raw_data_response(query, final_output, plan)
        else:
            response_text = f"I encountered an issue while processing your request: {', '.join(errors)}"
        
        # Build response
        response = {
            "response": response_text,
            "agent": self.name,
            "plan": plan,
            "execution_results": execution_results
        }
        
        # Add data if available
        if final_output and isinstance(final_output, dict):
            if "data_analyzed" in final_output:
                response["data"] = final_output["data_analyzed"]
            elif "result" in final_output:
                response["data"] = final_output["result"]
        
        # Add metadata
        response["metadata"] = {
            "agent_type": self.name.lower().replace("agent", "_specialist"),
            "plan_executed": success,
            "steps_completed": len(execution_results.get("steps_executed", [])),
            "confidence": final_output.get("confidence", 0.0) if isinstance(final_output, dict) else 0.0
        }
        
        return response
    
    def _format_raw_data_response(self, query: str, data: Dict[str, Any], plan: Dict[str, Any]) -> str:
        """Format raw data when no analysis tool was used"""
        response_text = "I've retrieved the following information:\n\n"
        
        # Check for common data patterns
        if "total_balance" in data:
            response_text += f"**Total Balance:** ${data['total_balance']:,.2f}\n"
        
        if "total_count" in data:
            response_text += f"**Total Records:** {data['total_count']:,}\n"
        
        if "summary" in data and isinstance(data["summary"], dict):
            response_text += "**Summary:**\n"
            for key, value in data["summary"].items():
                key_formatted = key.replace("_", " ").title()
                if isinstance(value, (int, float)):
                    if "amount" in key or "balance" in key or "value" in key:
                        response_text += f"• {key_formatted}: ${value:,.2f}\n"
                    else:
                        response_text += f"• {key_formatted}: {value:,}\n"
                else:
                    response_text += f"• {key_formatted}: {value}\n"
        
        if "segments" in data and isinstance(data["segments"], list):
            response_text += "\n**Segments:**\n"
            for segment in data["segments"][:5]:  # Show top 5
                if "segment" in segment:
                    response_text += f"\n{segment['segment'].title()}:\n"
                    for key, value in segment.items():
                        if key != "segment":
                            key_formatted = key.replace("_", " ").title()
                            if isinstance(value, (int, float)):
                                if "amount" in key or "balance" in key or "value" in key or "income" in key:
                                    response_text += f"  • {key_formatted}: ${value:,.2f}\n"
                                else:
                                    response_text += f"  • {key_formatted}: {value:,.0f}\n"
        
        # If we have raw records
        if any(key in data for key in ["loans", "deposits", "customers", "transactions"]):
            for record_type in ["loans", "deposits", "customers", "transactions"]:
                if record_type in data and isinstance(data[record_type], list) and data[record_type]:
                    response_text += f"\n**Sample {record_type.title()}:**\n"
                    # Show first few records
                    for i, record in enumerate(data[record_type][:3]):
                        response_text += f"\n{i+1}. "
                        # Show key fields
                        if "id" in record:
                            response_text += f"ID: {record['id']}"
                        if "amount" in record:
                            response_text += f", Amount: ${record['amount']:,.2f}"
                        if "balance" in record:
                            response_text += f", Balance: ${record['balance']:,.2f}"
                        if "status" in record:
                            response_text += f", Status: {record['status']}"
                        response_text += "\n"
        
        return response_text
    
    def format_response(
        self, 
        text: str, 
        data: Optional[Any] = None,
        visualizations: Optional[List[Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Helper method to format agent responses consistently"""
        response = {
            "response": text,
            "agent": self.name
        }
        
        if data is not None:
            response["data"] = data
            
        if visualizations:
            response["visualizations"] = visualizations
            
        if metadata:
            response["metadata"] = metadata
            
        return response