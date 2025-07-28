from typing import Any, Dict, Optional
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.runnables import RunnableConfig

from .state import SQLGeneratorState


class GraphManager:
    """Manages the LangGraph for SQL generation"""
    
    def __init__(self, prompts_manager, memory_manager, llm, analytical_manager=None):
        self.prompts_manager = prompts_manager
        self.memory_manager = memory_manager
        self.llm = llm
        self.analytical_manager = analytical_manager
        self.checkpointer = MemorySaver()
        self.schema_context = None
        self.example_patterns = None
        
    def create_graph(self) -> StateGraph:
        """Create the LangGraph for SQL generation"""
        graph = StateGraph(SQLGeneratorState)
        
        # Add nodes
        graph.add_node("route_query", self._route_query_node)
        graph.add_node("handle_error", self._handle_error_node)
        
        # Add analytical nodes
        graph.add_node("generate_analytical_questions", self._generate_analytical_questions_node)
        graph.add_node("execute_analytical_workflow", self._execute_analytical_workflow_node)
        graph.add_node("generate_comprehensive_analysis", self._generate_comprehensive_analysis_node)
        
        # Add edges
        graph.add_edge(START, "route_query")
        graph.add_conditional_edges(
            "route_query",
            self._route_decision,
            {
                "analytical": "generate_analytical_questions",
                "error": "handle_error"
            }
        )
        

        
        # Analytical workflow edges
        graph.add_conditional_edges(
            "generate_analytical_questions",
            self._analytical_questions_result,
            {
                "execute": "execute_analytical_workflow",
                "error": "handle_error"
            }
        )
        graph.add_conditional_edges(
            "execute_analytical_workflow",
            self._analytical_execution_result,
            {
                "analyze": "generate_comprehensive_analysis",
                "error": "handle_error"
            }
        )
        
        # End nodes
        graph.add_edge("generate_comprehensive_analysis", END)
        graph.add_edge("handle_error", END)
        
        return graph.compile(checkpointer=self.checkpointer)
    

    
    async def _handle_error_node(self, state: SQLGeneratorState, config: RunnableConfig) -> SQLGeneratorState:
        """Handle errors in the SQL generation process"""
        return {
            **state,
            "response": f"I encountered an error: {state.get('error', 'Unknown error')}"
        }
    

    
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
    
    # Analytical workflow nodes
    async def _route_query_node(self, state: SQLGeneratorState, config: RunnableConfig) -> SQLGeneratorState:
        """Route query to analytical workflow"""
        try:
            return {
                **state,
                "is_conversational": False,
                "requires_analysis": True,
                "workflow_type": "analytical",
                "analytical_questions": [],
                "analytical_results": [],
                "comprehensive_analysis": ""
            }
        except Exception as e:
            return {
                **state,
                "error": f"Error routing query: {str(e)}",
                "workflow_type": "error"
            }
    

    
    async def _generate_analytical_questions_node(self, state: SQLGeneratorState, config: RunnableConfig) -> SQLGeneratorState:
        """Generate analytical questions for comprehensive analysis"""
        try:
            if not self.analytical_manager:
                return {
                    **state,
                    "error": "Analytical manager not available"
                }
            
            # Prepare schema context
            if not self.schema_context:
                self._prepare_schema_context()
            
            # Generate analytical questions
            result = await self.analytical_manager.generate_analytical_questions(
                state["question"], 
                self.schema_context
            )
            
            if result["success"]:
                return {
                    **state,
                    "analytical_questions": result["questions"],
                    "schema": self.schema_context
                }
            else:
                return {
                    **state,
                    "error": result.get("error", "Failed to generate analytical questions")
                }
        except Exception as e:
            return {
                **state,
                "error": f"Error generating analytical questions: {str(e)}"
            }
    
    async def _execute_analytical_workflow_node(self, state: SQLGeneratorState, config: RunnableConfig) -> SQLGeneratorState:
        """Execute the analytical workflow"""
        try:
            if not self.analytical_manager:
                return {
                    **state,
                    "error": "Analytical manager not available"
                }
            
            # Execute analytical workflow
            result = await self.analytical_manager.execute_analytical_workflow(
                state["question"],
                state["analytical_questions"],
                state["schema"]
            )
            
            if result["success"]:
                return {
                    **state,
                    "analytical_results": result["analytical_results"],
                    "comprehensive_analysis": result["comprehensive_analysis"]
                }
            else:
                return {
                    **state,
                    "error": result.get("error", "Failed to execute analytical workflow")
                }
        except Exception as e:
            return {
                **state,
                "error": f"Error executing analytical workflow: {str(e)}"
            }
    
    async def _generate_comprehensive_analysis_node(self, state: SQLGeneratorState, config: RunnableConfig) -> SQLGeneratorState:
        """Generate comprehensive analysis from analytical results"""
        try:
            # The comprehensive analysis should already be generated in the execution step
            # This node just formats the final response
            return {
                **state,
                "response": state["comprehensive_analysis"]
            }
        except Exception as e:
            return {
                **state,
                "error": f"Error generating comprehensive analysis: {str(e)}",
                "response": f"Error generating comprehensive analysis: {str(e)}"
            }
    
    # Routing decision functions
    def _route_decision(self, state: SQLGeneratorState) -> str:
        """Determine which workflow to use - always analytical unless error"""
        if state.get("error"):
            return "error"
        else:
            return "analytical"
    
    def _analytical_questions_result(self, state: SQLGeneratorState) -> str:
        """Determine the result of analytical questions generation"""
        if state.get("error"):
            return "error"
        elif state.get("analytical_questions"):
            return "execute"
        else:
            return "error"
    
    def _analytical_execution_result(self, state: SQLGeneratorState) -> str:
        """Determine the result of analytical execution"""
        if state.get("error"):
            return "error"
        elif state.get("analytical_results"):
            return "analyze"
        else:
            return "error" 