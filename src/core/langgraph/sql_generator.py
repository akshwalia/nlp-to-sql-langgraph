import os
import logging
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from pydantic import SecretStr

from src.core.database import get_database_analyzer
from src.observability.langfuse_config import observe_function

# Set up logging
logger = logging.getLogger(__name__)

# Import all the modular components
from .prompts import PromptsManager
from .memory import MemoryManager
from .cache import CacheManager
from .session_context import SessionContextManager
from .sql_generation import SQLGenerationManager
from .execution import ExecutionManager
from .analytical_manager import AnalyticalManager
from .graph import GraphManager

class SmartSQLGenerator:
    """
    AI-powered SQL query generator that works with hardcoded PBTest database and IT_Professional_Services
    - Simplified version without workspace management
    """
    
    def __init__(
        self,
        model_name: str = "gpt-4",
        use_cache: bool = True,
        cache_file: str = "query_cache.json",
        use_memory: bool = True,
        memory_persist_dir: str = "./memory_store"
    ):
        """
        Initialize the SQL generator with simplified configuration
        
        Args:
            model_name: Generative AI model to use
            use_cache: Whether to cache query results
            cache_file: Path to the query cache file
            use_memory: Whether to use conversation memory
            memory_persist_dir: Directory to persist memory embeddings
        """
        load_dotenv()
        self.model_name = model_name
        
        # Get the hardcoded database analyzer
        self.db_analyzer = get_database_analyzer()
        
        # Initialize Azure OpenAI
        self._initialize_azure_openai()
        
        # Initialize all modular components
        self.prompts_manager = PromptsManager(use_memory=use_memory)
        self.memory_manager = MemoryManager(use_memory=use_memory, memory_persist_dir=memory_persist_dir)
        self.cache_manager = CacheManager(use_cache=use_cache, cache_file=cache_file)
        self.session_context_manager = SessionContextManager()
        
        # Initialize managers that depend on other components
        self.sql_generation_manager = SQLGenerationManager(
            self.prompts_manager, self.memory_manager, self.cache_manager, self.llm
        )
        
        # Set the database analyzer for column exploration
        self.sql_generation_manager.set_db_analyzer(self.db_analyzer)
        self.execution_manager = ExecutionManager(
            self.db_analyzer, self.session_context_manager
        )
        
        # Initialize analytical manager
        self.analytical_manager = AnalyticalManager(
            self.memory_manager, self.prompts_manager
        )
        
        # Set the LLM and managers for the analytical manager
        self.analytical_manager.set_llm(self.llm)
        self.analytical_manager.set_managers(
            self.sql_generation_manager, self.execution_manager
        )
        
        # Initialize graph manager
        self.graph_manager = GraphManager(
            self.prompts_manager, self.memory_manager, self.llm, self.analytical_manager
        )
        
        # Create the LangGraph
        self.graph = self.graph_manager.create_graph()
        
        # Prepare initial context
        self._prepare_initial_context()
    
    def _initialize_azure_openai(self):
        """Initialize Azure OpenAI client"""
        # Azure OpenAI configuration
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_key = os.getenv("AZURE_OPENAI_API_KEY")
        api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")
        deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4")
        
        logger.info(f"Initializing Azure OpenAI with deployment: {deployment_name}")
        
        self.llm = AzureChatOpenAI(
            azure_endpoint=azure_endpoint,
            azure_deployment=deployment_name,
            api_version=api_version,
            api_key=SecretStr(api_key),
            temperature=0.3,
            model_name=self.model_name,
            timeout=60,
            max_retries=2
        )
        
        logger.info("Azure OpenAI initialized successfully")
    
    def _prepare_initial_context(self):
        """Prepare initial context for the SQL generator"""
        # Prepare schema context for the SQL generation manager
        self.sql_generation_manager.prepare_schema_context(self.db_analyzer)
        logger.info("Initial context prepared successfully")
    
    @observe_function("sql_generation")
    async def generate_sql(self, question: str) -> Dict[str, Any]:
        """Generate SQL query from natural language question"""
        return await self.sql_generation_manager.generate_sql(question, self.db_analyzer)
    
    @observe_function("sql_query_execution")
    async def execute_query(self, question: str, auto_fix: bool = True, max_attempts: int = 2) -> Dict[str, Any]:
        """Execute SQL query with error handling"""
        try:
            # Generate SQL first
            sql_result = await self.generate_sql(question)
            
            if not sql_result["success"]:
                return {
                    "success": False,
                    "sql": "",
                    "results": [],
                    "error": sql_result["error"],
                    "question": question,
                    "execution_time": 0,
                    "row_count": 0
                }
            
            # Execute the SQL
            execution_result = await self.execution_manager.execute_query(
                question, sql_result["sql"], auto_fix, max_attempts
            )
            
            return execution_result
            
        except Exception as e:
            return {
                "success": False,
                "sql": "",
                "results": [],
                "error": f"Error in query execution: {str(e)}",
                "question": question,
                "execution_time": 0,
                "row_count": 0
            }
    
    async def process_unified_query(self, question: str, user_role: str = "viewer", edit_mode_enabled: bool = False) -> Dict[str, Any]:
        """Process a unified query with full functionality"""
        try:
            # All questions route to analytical workflow
            return await self._process_analytical_workflow(question)
                
        except Exception as e:
            return {
                "success": False,
                "question": question,
                "error": f"Error processing unified query: {str(e)}",
                "text": "",
                "execution_time": 0,
                "sql": "",
                "results": [],
                "query_type": "retrieve",
                "is_conversational": False,
                "source": "ai",
                "confidence": 0
            }
    
    async def _process_analytical_workflow(self, question: str) -> Dict[str, Any]:
        """Process query using comprehensive analytical workflow"""
        logger.info(f"Starting analytical workflow for question: '{question}'")
        
        try:
            # Check if schema_context is available
            if not self.sql_generation_manager.schema_context:
                error_msg = "Schema context not available for analytical workflow"
                logger.error(error_msg)
                
                return self._create_error_response(question, error_msg)
            
            logger.info(f"Schema context available: {len(self.sql_generation_manager.schema_context)} characters")
            
            # Generate analytical questions
            logger.info("Generating analytical questions...")
            
            questions_result = await self.analytical_manager.generate_analytical_questions(
                question, 
                self.sql_generation_manager.schema_context
            )
            
            if not questions_result["success"]:
                error_msg = questions_result.get("error", "Failed to generate analytical questions")
                logger.error(f"Failed to generate analytical questions: {error_msg}")
                
                return self._create_error_response(question, error_msg)
            
            total_questions = len(questions_result["questions"])
            logger.info(f"Generated {total_questions} analytical questions")
            
            # Execute analytical workflow
            logger.info("Executing analytical workflow...")
            
            workflow_result = await self.analytical_manager.execute_analytical_workflow(
                question,
                questions_result["questions"],
                self.sql_generation_manager.schema_context
            )
            
            if not workflow_result["success"]:
                error_msg = workflow_result.get("error", "Failed to execute analytical workflow")
                logger.error(f"Failed to execute analytical workflow: {error_msg}")
                
                return self._create_error_response(question, error_msg)
            
            successful_executions = workflow_result.get("successful_executions", 0)
            failed_executions = workflow_result.get("failed_executions", 0)
            total_execution_time = workflow_result.get("total_execution_time", 0)
            
            logger.info(f"Workflow executed: {successful_executions} successful, {failed_executions} failed, {total_execution_time:.2f}s total")
            
            # Generate comprehensive analysis
            logger.info("Generating comprehensive analysis...")
            
            analysis_result = await self.analytical_manager.generate_comprehensive_analysis(
                question,
                workflow_result["analytical_results"],
                self.sql_generation_manager.schema_context
            )
            
            if analysis_result["success"]:
                analysis_length = len(analysis_result["analysis"])
                logger.info(f"Comprehensive analysis generated: {analysis_length} characters")
                
                # Log final statistics
                logger.info(f"Analytical workflow completed successfully:")
                logger.info(f"   - Questions generated: {total_questions}")
                logger.info(f"   - Successful executions: {successful_executions}")
                logger.info(f"   - Failed executions: {failed_executions}")
                logger.info(f"   - Total execution time: {total_execution_time:.2f}s")
                logger.info(f"   - Analysis length: {analysis_length} characters")
                
                return {
                    "success": True,
                    "question": question,
                    "text": analysis_result["analysis"],
                    "sql": "",
                    "results": workflow_result["analytical_results"],
                    "execution_time": total_execution_time,
                    "row_count": len(workflow_result["analytical_results"]),
                    "query_type": "analytical",
                    "is_conversational": False,
                    "source": "ai",
                    "confidence": 95,
                    "visualization_recommendations": {"is_visualizable": False, "recommended_charts": []},
                    "analytical_questions": questions_result["questions"],
                    "analytical_results": workflow_result["analytical_results"]
                }
            else:
                error_msg = analysis_result.get("error", "Failed to generate comprehensive analysis")
                logger.error(f"Failed to generate comprehensive analysis: {error_msg}")
                
                return self._create_error_response(question, error_msg)
                
        except Exception as e:
            logger.error(f"Error processing analytical workflow: {str(e)}")
            
            return self._create_error_response(question, f"Error processing analytical workflow: {str(e)}")
    
    def _create_error_response(self, question: str, error_msg: str) -> Dict[str, Any]:
        """Create a standardized error response"""
        return {
            "success": False,
            "question": question,
            "error": error_msg,
            "text": "",
            "execution_time": 0,
            "sql": "",
            "results": [],
            "query_type": "analytical",
            "is_conversational": False,
            "source": "ai",
            "confidence": 0
        }
    
    def get_paginated_results(self, table_id: str, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """Get paginated results"""
        return self.session_context_manager.get_paginated_results(table_id, page, page_size)
    
    def clear_cache(self) -> None:
        """Clear the query cache"""
        self.cache_manager.clear_cache()
    
    def clear_session_context(self) -> None:
        """Clear session context"""
        self.session_context_manager.clear_session_context()
    
    def get_session_stats(self) -> Dict[str, Any]:
        """Get session statistics"""
        return self.session_context_manager.get_session_stats()


# For backward compatibility, create aliases to the old class name
SQLGenerator = SmartSQLGenerator