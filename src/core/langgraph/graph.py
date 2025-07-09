from typing import Any, Dict, Tuple, Optional
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.runnables import RunnableConfig

from .state import SQLGeneratorState


class GraphManager:
    """Manages the LangGraph for SQL generation"""
    
    def __init__(self, prompts_manager, memory_manager, llm):
        self.prompts_manager = prompts_manager
        self.memory_manager = memory_manager
        self.llm = llm
        self.checkpointer = MemorySaver()
        self.schema_context = None
        self.example_patterns = None
        
    def create_graph(self) -> StateGraph:
        """Create the LangGraph for SQL generation"""
        graph = StateGraph(SQLGeneratorState)
        
        # Add nodes
        graph.add_node("generate_sql", self._generate_sql_node)
        graph.add_node("validate_sql", self._validate_sql_node)
        graph.add_node("generate_response", self._generate_response_node)
        graph.add_node("handle_error", self._handle_error_node)
        
        # Add edges
        graph.add_edge(START, "generate_sql")
        graph.add_conditional_edges(
            "generate_sql",
            self._should_validate,
            {
                "validate": "validate_sql",
                "respond": "generate_response",
                "error": "handle_error"
            }
        )
        graph.add_conditional_edges(
            "validate_sql",
            self._validation_result,
            {
                "success": "generate_response",
                "retry": "generate_sql",
                "error": "handle_error"
            }
        )
        graph.add_edge("generate_response", END)
        graph.add_edge("handle_error", END)
        
        return graph.compile(checkpointer=self.checkpointer)
    
    async def _generate_sql_node(self, state: SQLGeneratorState, config: RunnableConfig) -> SQLGeneratorState:
        """Generate SQL from the question"""
        try:
            # Prepare context
            if not self.schema_context:
                self._prepare_schema_context()
            if not self.example_patterns:
                self.example_patterns = self._generate_example_patterns()
            
            memory_context = self.memory_manager.get_memory_context(state["question"]) if self.memory_manager.use_memory else ""
            
            # Generate SQL using the prompt
            prompt_values = {
                "schema": self.schema_context,
                "question": state["question"],
                "examples": self.example_patterns
            }
            if self.memory_manager.use_memory:
                prompt_values["memory"] = memory_context
            
            response = await self.llm.ainvoke(
                self.prompts_manager.sql_prompt.format_messages(**prompt_values),
                config
            )
            
            sql = self._extract_response_content(response)
            
            return {
                **state,
                "sql": sql,
                "schema": self.schema_context,
                "examples": self.example_patterns,
                "memory": memory_context
            }
        except Exception as e:
            return {
                **state,
                "error": str(e)
            }
    
    async def _validate_sql_node(self, state: SQLGeneratorState, config: RunnableConfig) -> SQLGeneratorState:
        """Validate and fix SQL if needed"""
        try:
            is_valid, error_msg = self._validate_sql(state["sql"])
            
            if not is_valid and state["validation_attempts"] < 2:
                # Try to fix the SQL
                prompt_values = {
                    "schema": state["schema"],
                    "sql": state["sql"],
                    "error": error_msg
                }
                if self.memory_manager.use_memory:
                    prompt_values["memory"] = state["memory"]
                
                response = await self.llm.ainvoke(
                    self.prompts_manager.validation_prompt.format_messages(**prompt_values),
                    config
                )
                
                fixed_sql = self._extract_response_content(response)
                
                return {
                    **state,
                    "sql": fixed_sql,
                    "error": error_msg,
                    "validation_attempts": state["validation_attempts"] + 1
                }
            else:
                return {
                    **state,
                    "error": error_msg if not is_valid else None
                }
        except Exception as e:
            return {
                **state,
                "error": str(e)
            }
    
    async def _generate_response_node(self, state: SQLGeneratorState, config: RunnableConfig) -> SQLGeneratorState:
        """Generate final response based on SQL and results"""
        try:
            # Execute SQL if no error
            if not state.get("error"):
                # This would typically execute the SQL and get results
                # For now, we'll just format the response
                results = state.get("results", [])
                
                prompt_values = {
                    "schema": state["schema"],
                    "question": state["question"],
                    "sql": state["sql"],
                    "results": str(results)
                }
                if self.memory_manager.use_memory:
                    prompt_values["memory"] = state["memory"]
                
                response = await self.llm.ainvoke(
                    self.prompts_manager.text_response_prompt.format_messages(**prompt_values),
                    config
                )
                
                response_text = self._extract_response_content(response)
                
                return {
                    **state,
                    "response": response_text
                }
            else:
                return {
                    **state,
                    "response": f"Error: {state['error']}"
                }
        except Exception as e:
            return {
                **state,
                "error": str(e),
                "response": f"Error generating response: {str(e)}"
            }
    
    async def _handle_error_node(self, state: SQLGeneratorState, config: RunnableConfig) -> SQLGeneratorState:
        """Handle errors in the SQL generation process"""
        return {
            **state,
            "response": f"I encountered an error: {state.get('error', 'Unknown error')}"
        }
    
    def _should_validate(self, state: SQLGeneratorState) -> str:
        """Determine if SQL should be validated"""
        if state.get("error"):
            return "error"
        elif state.get("sql"):
            return "validate"
        else:
            return "error"
    
    def _validation_result(self, state: SQLGeneratorState) -> str:
        """Determine the result of validation"""
        if state.get("error") and state["validation_attempts"] < 2:
            return "retry"
        elif state.get("error"):
            return "error"
        else:
            return "success"
    
    def _validate_sql(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Basic SQL validation - this would need to be implemented with actual validation logic"""
        # This is a placeholder - in the actual implementation, 
        # you would validate the SQL against the database schema
        try:
            # Basic syntax checks
            if not sql or not sql.strip():
                return False, "Empty SQL query"
            
            # Check for basic SQL structure
            sql_upper = sql.upper()
            if not any(keyword in sql_upper for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
                return False, "SQL must contain a valid command (SELECT, INSERT, UPDATE, DELETE)"
            
            # Check for balanced parentheses
            if sql.count('(') != sql.count(')'):
                return False, "Unbalanced parentheses in SQL"
            
            # Check for basic semicolon issues
            if sql.count(';') > 1:
                return False, "Multiple statements not allowed"
            
            return True, None
            
        except Exception as e:
            return False, f"SQL validation error: {str(e)}"
    
    def _extract_response_content(self, response) -> str:
        """Extract content from LLM response"""
        try:
            if hasattr(response, 'content'):
                return response.content
            elif hasattr(response, 'text'):
                return response.text
            elif isinstance(response, str):
                return response
            else:
                return str(response)
        except Exception as e:
            print(f"Error extracting response content: {e}")
            return ""
    
    def _prepare_schema_context(self) -> None:
        """Prepare schema context - placeholder for actual implementation"""
        # This would be implemented with actual database schema retrieval
        self.schema_context = "Database schema would be loaded here"
    
    def _generate_example_patterns(self) -> str:
        """Generate example patterns - placeholder for actual implementation"""
        # This would be implemented with actual example generation
        return "Example SQL patterns would be generated here"
    
    def set_schema_context(self, schema_context: str) -> None:
        """Set the schema context"""
        self.schema_context = schema_context
    
    def set_example_patterns(self, example_patterns: str) -> None:
        """Set the example patterns"""
        self.example_patterns = example_patterns 