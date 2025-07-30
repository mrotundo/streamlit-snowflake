import streamlit as st
from typing import Optional, Dict, Any
from config.settings import Settings
from services.snowflake_service import SnowflakeService
from services.openai_service import OpenAIService
from services.llm_interface import LLMInterface
from utils.session_manager import SessionManager
from models import ChatSession
from agents.agent_registry import agent_registry
import json
from datetime import datetime


def get_llm_service(provider: str) -> Optional[LLMInterface]:
    """Factory function to get LLM service instance"""
    try:
        if provider == 'snowflake' and Settings.is_snowflake_configured():
            return SnowflakeService(
                account=Settings.SNOWFLAKE_ACCOUNT,
                user=Settings.SNOWFLAKE_USER,
                password=Settings.SNOWFLAKE_PASSWORD,
                warehouse=Settings.SNOWFLAKE_WAREHOUSE,
                database=Settings.SNOWFLAKE_DATABASE,
                schema=Settings.SNOWFLAKE_SCHEMA
            )
        elif provider == 'openai' and Settings.is_openai_configured():
            return OpenAIService(api_key=Settings.OPENAI_API_KEY)
    except ImportError as e:
        st.error(f"Failed to initialize {provider} service: {str(e)}")
    return None


def generate_session_name(first_message: str, llm_service: LLMInterface, model: str) -> str:
    """Generate a brief session name based on the first message"""
    try:
        prompt = f"Generate a very brief 3-5 word title for a chat that starts with this message: '{first_message}'. Only return the title, nothing else."
        messages = [{"role": "user", "content": prompt}]
        title = llm_service.complete(messages, model=model, temperature=0.7, max_tokens=20)
        # Clean up the title
        title = title.strip().strip('"').strip("'")
        # Limit length
        if len(title) > 50:
            title = title[:47] + "..."
        return title
    except Exception:
        # Fallback to a simple truncation of the first message
        return first_message[:30] + "..." if len(first_message) > 30 else first_message


def display_agent_response(response: Dict[str, Any]):
    """Display an agent response including planning thoughts and execution details"""
    
    # Show the execution plan if available
    if "plan" in response and response["plan"]:
        with st.expander("🧠 Agent's Thought Process", expanded=True):
            plan = response["plan"]
            st.markdown(f"**Goal:** {plan.get('goal', 'Process query')}")
            
            st.markdown("**Execution Plan:**")
            for step in plan.get("steps", []):
                st.markdown(f"{step['step']}. **{step['tool']}**: {step['description']}")
    
    # Show execution results if available
    if "execution_results" in response and response["execution_results"]:
        with st.expander("🔧 Tool Execution Details", expanded=False):
            execution = response["execution_results"]
            
            for step_result in execution.get("steps_executed", []):
                col1, col2 = st.columns([1, 5])
                with col1:
                    if step_result["success"]:
                        st.success(f"Step {step_result['step']}")
                    else:
                        st.error(f"Step {step_result['step']}")
                
                with col2:
                    st.markdown(f"**{step_result['tool']}**: {step_result['description']}")
                    if step_result.get("output"):
                        with st.container():
                            if isinstance(step_result["output"], dict):
                                # Show a summary of the output
                                if "query" in step_result["output"]:
                                    st.code(step_result["output"]["query"], language="sql")
                                elif "analysis" in step_result["output"]:
                                    st.json(step_result["output"]["analysis"])
                                elif "data" in step_result["output"]:
                                    st.caption(f"Data retrieved: {step_result['output'].get('row_count', 'N/A')} rows")
                                else:
                                    st.json(step_result["output"])
                            else:
                                st.text(str(step_result["output"])[:200] + "..." if len(str(step_result["output"])) > 200 else str(step_result["output"]))
                    
                    if step_result.get("error"):
                        st.error(f"Error: {step_result['error']}")
    
    # Display the main response
    st.markdown("### 💬 Response")
    st.write(response.get("response", ""))
    
    # Display any data tables
    if "data" in response and response["data"]:
        if isinstance(response["data"], dict) and "summary" in response["data"]:
            st.subheader("📊 Data Summary")
            # Display summary metrics
            summary = response["data"]["summary"]
            
            # Create columns for metrics
            cols = st.columns(3)
            metric_count = 0
            for key, value in summary.items():
                if isinstance(value, (str, int, float)) and not key.startswith("by_"):
                    with cols[metric_count % 3]:
                        st.metric(label=key.replace("_", " ").title(), value=value)
                    metric_count += 1
            
            # Display breakdown data
            for key, value in summary.items():
                if key.startswith("by_") and isinstance(value, dict):
                    st.subheader(f"📈 {key.replace('_', ' ').title()}")
                    st.json(value)
    
    # Display visualizations
    if "visualizations" in response and response["visualizations"]:
        st.subheader("📊 Visualizations")
        for viz in response["visualizations"]:
            if hasattr(viz, 'show'):  # Plotly figure
                st.plotly_chart(viz, use_container_width=True)
            else:
                st.write(viz)
    
    # Show which agent handled the request
    if "agent" in response:
        st.caption(f"Handled by: {response['agent']}")


def main():
    st.set_page_config(
        page_title="Banking AI Assistant",
        page_icon="🏦",
        layout="wide"
    )
    
    # Initialize session manager
    session_manager = SessionManager()
    
    # Initialize debug logs in session state
    if 'debug_logs' not in st.session_state:
        st.session_state.debug_logs = []
    
    def add_debug_log(message: str, level: str = "INFO"):
        """Add a message to the debug log"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        st.session_state.debug_logs.append(f"[{timestamp}] [{level}] {message}")
        # Keep only last 100 logs
        if len(st.session_state.debug_logs) > 100:
            st.session_state.debug_logs = st.session_state.debug_logs[-100:]
    
    # Initialize agent registry (this will auto-discover agents)
    router = agent_registry.get_router()
    
    # Check if any providers are configured
    available_providers = Settings.get_available_providers()
    if not available_providers:
        st.error("No LLM providers configured. Please set up either Snowflake or OpenAI credentials in your .env file.")
        st.stop()
    
    # Set default provider and model if not set
    if not session_manager.get_llm_provider():
        default_provider = Settings.DEFAULT_PROVIDER if Settings.DEFAULT_PROVIDER in available_providers else available_providers[0]
        session_manager.set_llm_provider(default_provider)
        
    if not session_manager.get_llm_model():
        provider = session_manager.get_llm_provider()
        default_model = Settings.SNOWFLAKE_MODEL if provider == 'snowflake' else Settings.OPENAI_MODEL
        session_manager.set_llm_model(default_model)
    
    # Sidebar for session management
    with st.sidebar:
        st.title("💬 Chat Sessions")
        
        # New session button
        if st.button("➕ New Session", use_container_width=True, type="primary"):
            session = session_manager.create_session()
            session_manager.set_current_session(session.id)
            st.rerun()
        
        st.divider()
        
        # List sessions with boxy style
        sessions = session_manager.get_all_sessions()
        current_session = session_manager.get_current_session()
        
        if sessions:
            for session in sessions:
                # Create a container for each session with border
                with st.container():
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        # Custom CSS for boxy style
                        button_style = "primary" if current_session and current_session.id == session.id else "secondary"
                        if st.button(
                            session.name,
                            key=f"session_{session.id}",
                            use_container_width=True,
                            type=button_style
                        ):
                            session_manager.set_current_session(session.id)
                            st.rerun()
                    with col2:
                        if st.button("🗑️", key=f"delete_session_{session.id}"):
                            session_manager.delete_session(session.id)
                            st.rerun()
                    
                    # Show session info
                    st.caption(f"Updated: {session.updated_at.strftime('%Y-%m-%d %H:%M')}")
                    st.markdown("---")
        else:
            st.info("No sessions yet. Click 'New Session' to start.")
        
        # Agent Information
        with st.expander("🤖 Available Agents"):
            agent_info = agent_registry.get_agent_info()
            for agent_name, info in agent_info.items():
                st.markdown(f"**{info['name']}**")
                st.caption(info['description'])
                if info['capabilities']:
                    st.markdown("Capabilities:")
                    for cap in info['capabilities'][:3]:  # Show first 3
                        st.markdown(f"• {cap}")
                    if len(info['capabilities']) > 3:
                        st.caption(f"...and {len(info['capabilities']) - 3} more")
                st.markdown("---")
        
        # Debug Panel
        with st.expander("🐛 Debug Logs", expanded=False):
            if st.button("Clear Logs", key="clear_debug_logs"):
                st.session_state.debug_logs = []
                st.rerun()
            
            # Display logs in reverse order (newest first)
            if st.session_state.debug_logs:
                log_container = st.container()
                with log_container:
                    for log in reversed(st.session_state.debug_logs):
                        if "[ERROR]" in log:
                            st.code(log, language="")
                        elif "[WARNING]" in log:
                            st.warning(log)
                        else:
                            st.text(log)
            else:
                st.info("No debug logs yet")
        
        # Advanced Settings at the bottom
        with st.expander("⚙️ Advanced Settings"):
            # Provider selection
            provider = st.selectbox(
                "LLM Provider",
                options=available_providers,
                index=available_providers.index(session_manager.get_llm_provider()) if session_manager.get_llm_provider() in available_providers else 0
            )
            if provider != session_manager.get_llm_provider():
                session_manager.set_llm_provider(provider)
                # Update model when provider changes
                default_model = Settings.SNOWFLAKE_MODEL if provider == 'snowflake' else Settings.OPENAI_MODEL
                session_manager.set_llm_model(default_model)
                add_debug_log(f"Provider changed to: {provider}")
            
            # Model selection based on provider
            llm_service = get_llm_service(provider)
            if llm_service:
                models = llm_service.get_available_models()
                current_model = session_manager.get_llm_model()
                model = st.selectbox(
                    "Model",
                    options=models,
                    index=models.index(current_model) if current_model in models else 0
                )
                if model != session_manager.get_llm_model():
                    session_manager.set_llm_model(model)
                    add_debug_log(f"Model changed to: {model}")
    
    # Main chat interface
    st.title("🏦 Banking AI Assistant")
    st.caption("Powered by specialized agents for loans, deposits, and customer analytics")
    
    # Create a session if none exists
    if not sessions:
        session = session_manager.create_session()
        session_manager.set_current_session(session.id)
        st.rerun()
    
    # Ensure we have a current session
    if not session_manager.get_current_session():
        if sessions:
            session_manager.set_current_session(sessions[0].id)
            st.rerun()
    
    current_session = session_manager.get_current_session()
    
    if current_session:
        # Display session name in smaller text
        st.caption(f"Current session: {current_session.name}")
        
        # Clear chat button
        col1, col2, col3 = st.columns([1, 1, 4])
        with col1:
            if st.button("🗑️ Clear Chat", key="clear_chat"):
                session_manager.clear_current_session_messages()
                st.rerun()
        
        # Chat messages container
        messages_container = st.container()
        
        # Display chat history
        with messages_container:
            for message in current_session.messages:
                with st.chat_message(message.role):
                    # Check if this is an agent response with special formatting
                    if message.role == "assistant" and hasattr(message, 'metadata') and message.metadata:
                        if isinstance(message.metadata, str):
                            try:
                                metadata = json.loads(message.metadata)
                                if "agent_response" in metadata:
                                    display_agent_response(metadata["agent_response"])
                                else:
                                    st.write(message.content)
                            except:
                                st.write(message.content)
                        else:
                            st.write(message.content)
                    else:
                        st.write(message.content)
        
        # Chat input
        if prompt := st.chat_input("Ask about loans, deposits, customers, or any banking topic..."):
            add_debug_log(f"User query: {prompt}")
            
            # Add user message
            session_manager.add_message_to_current_session("user", prompt)
            
            # Display user message
            with messages_container:
                with st.chat_message("user"):
                    st.write(prompt)
                
                # Create a placeholder for the assistant's response
                assistant_message_placeholder = st.empty()
            
            # Get LLM service
            llm_service = get_llm_service(session_manager.get_llm_provider())
            if not llm_service:
                add_debug_log("Failed to initialize LLM service", "ERROR")
                st.error("Failed to initialize LLM service")
                st.stop()
            
            add_debug_log(f"LLM service initialized: {session_manager.get_llm_provider()}")
            
            # Route to appropriate agent
            with st.spinner("Finding the right expert..."):
                add_debug_log("Routing query to appropriate agent...")
                selected_agent, confidence = router.route(
                    prompt, 
                    llm_service, 
                    session_manager.get_llm_model()
                )
            
            if selected_agent:
                # Show which agent is handling the request
                add_debug_log(f"Agent selected: {selected_agent.name} (confidence: {confidence:.2f})")
                st.info(f"🤖 {selected_agent.name} is handling your request (confidence: {confidence:.0%})")
                
                # Get response from agent
                try:
                    with st.spinner(f"{selected_agent.name} is thinking..."):
                        # Get conversation history
                        conversation_history = session_manager.get_current_messages()[:-1]  # Exclude current message
                        add_debug_log(f"Processing with {selected_agent.name}, Model: {session_manager.get_llm_model()}")
                        
                        # Process with agent
                        agent_response = selected_agent.process(
                            prompt,
                            llm_service,
                            session_manager.get_llm_model(),
                            conversation_history,
                            debug_callback=add_debug_log
                        )
                        
                        add_debug_log(f"Agent response received: {bool(agent_response)}")
                        if agent_response:
                            add_debug_log(f"Response keys: {list(agent_response.keys())}")
                            if "error" in agent_response:
                                add_debug_log(f"Agent error: {agent_response['error']}", "ERROR")
                    
                    # Extract text response
                    response_text = agent_response.get("response", "I couldn't generate a response.")
                    add_debug_log(f"Response text length: {len(response_text)}")
                    
                    # Add assistant message with metadata
                    session_manager.add_message_to_current_session(
                        "assistant",
                        response_text,
                        model_used=session_manager.get_llm_model(),
                        provider=session_manager.get_llm_provider()
                    )
                    
                    # Store agent response metadata (for visualization)
                    if current_session.messages:
                        current_session.messages[-1].metadata = json.dumps({"agent_response": agent_response})
                    
                    # Auto-rename session after first message if needed
                    if session_manager.should_auto_rename_session(current_session.id):
                        new_name = generate_session_name(prompt, llm_service, session_manager.get_llm_model())
                        session_manager.rename_session(current_session.id, new_name)
                        add_debug_log(f"Session renamed to: {new_name}")
                    
                    # Log execution details if available
                    if "execution_results" in agent_response and agent_response["execution_results"]:
                        exec_results = agent_response["execution_results"]
                        if isinstance(exec_results, dict) and "steps_executed" in exec_results:
                            for step in exec_results["steps_executed"]:
                                if step.get("success"):
                                    add_debug_log(f"Step {step.get('step')} ({step.get('tool')}): SUCCESS")
                                else:
                                    add_debug_log(f"Step {step.get('step')} ({step.get('tool')}): FAILED - {step.get('error')}", "ERROR")
                    
                    # Display agent response with any visualizations
                    with assistant_message_placeholder.container():
                        with st.chat_message("assistant"):
                            display_agent_response(agent_response)
                    
                except Exception as e:
                    import traceback
                    error_msg = f"Error: {str(e)}"
                    add_debug_log(error_msg, "ERROR")
                    add_debug_log(f"Traceback: {traceback.format_exc()}", "ERROR")
                    
                    with assistant_message_placeholder.container():
                        st.error(error_msg)
            else:
                add_debug_log("No appropriate agent found for query", "WARNING")
                st.error("No appropriate agent found for your request.")
            
            # Rerun to update the chat display
            st.rerun()


if __name__ == "__main__":
    main()