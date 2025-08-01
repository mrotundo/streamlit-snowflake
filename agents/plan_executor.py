from typing import Dict, Any, List, Optional
import re
import json
from agents.tools.base_tool import BaseTool


class PlanExecutor:
    """Executes agent plans step by step with variable resolution and adaptation"""
    
    def __init__(self):
        self.execution_context = {}
        self.tools_registry = {}
        self.debug_callback = None
    
    def set_debug_callback(self, callback):
        """Set a callback function for debug logging"""
        self.debug_callback = callback
    
    def _log_debug(self, message: str, level: str = "INFO"):
        """Log a debug message if callback is available"""
        if self.debug_callback:
            self.debug_callback(message, level)
    
    def register_tool(self, tool: BaseTool):
        """Register a tool that can be used in plans"""
        self.tools_registry[tool.name] = tool
    
    def execute_plan(
        self, 
        plan: Dict[str, Any], 
        on_step_complete: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Execute a plan step by step.
        
        Args:
            plan: The execution plan with steps
            on_step_complete: Optional callback after each step
            
        Returns:
            Execution results including outputs and any errors
        """
        results = {
            "goal": plan.get("goal", ""),
            "steps_executed": [],
            "final_output": None,
            "success": True,
            "errors": [],
            "debug_logs": []
        }
        
        # Clear context for new execution
        self.execution_context = {}
        
        steps = plan.get("steps", [])
        results["debug_logs"].append(f"Executing plan with {len(steps)} steps")
        
        for step in steps:
            step_desc = f"step {step.get('step', '?')}: {step.get('tool', '')}"
            results["debug_logs"].append(f"Executing {step_desc}")
            self._log_debug(f"Executing {step_desc}")
            
            step_result = self._execute_step(step)
            results["steps_executed"].append(step_result)
            
            # Call callback if provided
            if on_step_complete:
                on_step_complete(step_result)
            
            # Check if step failed
            if not step_result["success"]:
                results["success"] = False
                error_msg = f"Step {step.get('step', '?')} failed: {step_result.get('error', 'Unknown error')}"
                results["errors"].append(error_msg)
                results["debug_logs"].append(f"ERROR: {error_msg}")
                self._log_debug(error_msg, "ERROR")
                
                # Try adaptation strategy if available
                if "adaptations" in plan and "error" in plan["adaptations"]:
                    results["debug_logs"].append("Trying adaptation strategy...")
                    self._log_debug("Trying adaptation strategy...")
                    adaptation_result = self._apply_adaptation(plan["adaptations"]["error"], step, step_result)
                    if adaptation_result:
                        results["steps_executed"].append(adaptation_result)
                else:
                    break  # Stop execution on error without adaptation
            else:
                results["debug_logs"].append(f"Step {step.get('step', '?')} completed successfully")
                self._log_debug(f"Step {step.get('step', '?')} completed successfully")
        
        # Set final output
        if results["steps_executed"]:
            last_step = results["steps_executed"][-1]
            if last_step["success"] and last_step.get("output"):
                results["final_output"] = last_step["output"]
        
        return results
    
    def _execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single step of the plan"""
        step_num = step.get("step", "?")
        tool_name = step.get("tool", "")
        description = step.get("description", "")
        inputs = step.get("inputs", step.get("parameters", {}))
        output_key = step.get("output_key", f"step_{step_num}_output")
        
        result = {
            "step": step_num,
            "tool": tool_name,
            "description": description,
            "success": False,
            "output": None,
            "error": None
        }
        
        try:
            # Resolve variables in inputs
            resolved_inputs = self._resolve_variables(inputs)
            
            # Get the tool
            tool = self.tools_registry.get(tool_name)
            if not tool:
                raise ValueError(f"Tool '{tool_name}' not found in registry")
            
            # Validate parameters
            is_valid, error_msg = tool.validate_parameters(**resolved_inputs)
            if not is_valid:
                raise ValueError(f"Invalid parameters: {error_msg}")
            
            # Execute the tool
            tool_result = tool.execute(**resolved_inputs)
            
            if tool_result["success"]:
                result["success"] = True
                result["output"] = tool_result["result"]
                
                # Store output in context for future steps
                self.execution_context[output_key] = tool_result["result"]
            else:
                result["error"] = tool_result.get("error", "Tool execution failed")
        
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    def _resolve_variables(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve variable references in inputs (e.g., ${variable_name})"""
        resolved = {}
        
        try:
            for key, value in inputs.items():
                if isinstance(value, str):
                    # Look for ${variable} patterns
                    resolved[key] = self._replace_variables(value)
                elif isinstance(value, dict):
                    # Recursively resolve nested dictionaries
                    resolved[key] = self._resolve_variables(value)
                elif isinstance(value, list):
                    # Resolve variables in lists
                    resolved[key] = [
                        self._replace_variables(item) if isinstance(item, str) else item
                        for item in value
                    ]
                else:
                    resolved[key] = value
            
            return resolved
        except Exception as e:
            print(f"Error resolving variables: {str(e)}")
            print(f"Inputs: {inputs}")
            print(f"Execution context keys: {list(self.execution_context.keys())}")
            raise
    
    def _replace_variables(self, text: str) -> Any:
        """Replace ${variable} references with actual values from context"""
        # If the entire string is a variable reference, return the actual object
        if isinstance(text, str) and text.startswith("${") and text.endswith("}") and text.count("${") == 1:
            var_name = text[2:-1]
            return self._get_nested_value(var_name)
        
        # If not a string, return as is
        if not isinstance(text, str):
            return text
            
        # Otherwise, do string replacement
        def replacer(match):
            var_name = match.group(1)
            value = self._get_nested_value(var_name)
            return str(value) if value is not None else match.group(0)
        
        return re.sub(r'\$\{([^}]+)\}', replacer, text)
    
    def _get_nested_value(self, path: str) -> Any:
        """Get a value from context using dot notation (e.g., 'step1.data.total')"""
        parts = path.split('.')
        value = self.execution_context
        
        try:
            for i, part in enumerate(parts):
                if isinstance(value, dict):
                    if part in value:
                        value = value[part]
                    else:
                        # Debug: show what keys are available
                        available_keys = list(value.keys()) if isinstance(value, dict) else []
                        print(f"Key '{part}' not found in context at path '{'.'.join(parts[:i])}'")
                        print(f"Available keys: {available_keys}")
                        return None
                else:
                    # If we're trying to access a property on a non-dict, it's an error
                    print(f"Cannot access '{part}' on non-dict value of type {type(value).__name__}")
                    print(f"Value: {value}")
                    return None
            
            return value
        except (TypeError, KeyError, AttributeError) as e:
            # Log the error for debugging
            print(f"Error getting nested value for path '{path}': {str(e)}")
            print(f"Current context keys: {list(self.execution_context.keys())}")
            return None
    
    def _apply_adaptation(self, adaptation_strategy: str, failed_step: Dict, step_result: Dict) -> Optional[Dict[str, Any]]:
        """Apply an adaptation strategy when a step fails"""
        # This is a simple implementation - could be extended
        adaptation_result = {
            "step": f"{failed_step.get('step', '?')}_adaptation",
            "tool": "adaptation",
            "description": f"Adaptation: {adaptation_strategy}",
            "success": True,
            "output": {
                "adaptation_applied": adaptation_strategy,
                "original_error": step_result.get("error", "Unknown error"),
                "message": adaptation_strategy
            }
        }
        
        return adaptation_result
    
    def get_context_variable(self, key: str) -> Any:
        """Get a variable from the execution context"""
        return self.execution_context.get(key)
    
    def set_context_variable(self, key: str, value: Any):
        """Set a variable in the execution context"""
        self.execution_context[key] = value