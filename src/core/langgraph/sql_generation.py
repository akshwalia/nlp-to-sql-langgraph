import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from langchain_core.language_models import BaseLanguageModel
from .prompts import PromptsManager
from .memory import MemoryManager
from .cache import CacheManager
from ...observability.langfuse_config import observe_function

# Set up logging
logger = logging.getLogger(__name__)

class SQLGenerationManager:
    """Manages SQL generation from natural language questions"""
    
    def __init__(self, prompts_manager, memory_manager, cache_manager, llm):
        self.prompts_manager = prompts_manager
        self.memory_manager = memory_manager
        self.cache_manager = cache_manager
        self.llm = llm
        self.schema_context = None
        self.db_analyzer = None  # Will be set during initialization
        
        logger.info("SQLGenerationManager initialized")
    
    def set_db_analyzer(self, db_analyzer):
        """Set the database analyzer for column exploration"""
        self.db_analyzer = db_analyzer

    def _is_numeric_column(self, column_name: str) -> bool:
        """
        Check if a column is numeric by querying the SQLite database using PRAGMA table_info
        
        Args:
            column_name: Name of the column to check
            
        Returns:
            True if the column is numeric, False otherwise
        """
        if not self.db_analyzer:
            return False
        
        try:
            engine = self.db_analyzer.analyzer.engine
            table_name = self.db_analyzer.analyzer.table_name
            
            with engine.connect() as connection:
                from sqlalchemy import text
                
                # Use SQLite's PRAGMA table_info to get column data type
                query = text(f'PRAGMA table_info("{table_name}")')
                
                result = connection.execute(query)
                columns = result.fetchall()
                
                for column in columns:
                    # SQLite PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
                    if column[1] == column_name:  # column[1] is the name
                        data_type = column[2].upper()  # column[2] is the type
                        # SQLite numeric types
                        sqlite_numeric_types = [
                            'INTEGER', 'INT', 'REAL', 'FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL',
                            'BIGINT', 'SMALLINT', 'TINYINT', 'MEDIUMINT'
                        ]
                        return any(numeric_type in data_type for numeric_type in sqlite_numeric_types)
                
        except Exception as e:
            logger.error(f"Error checking if column {column_name} is numeric: {e}")
            # Fallback: check if column name suggests it's numeric
            numeric_indicators = ['rate', 'amount', 'cost', 'price', 'salary', 'wage', 'fee', 'count', 'number', 'id', 'year', 'age']
            return any(indicator in column_name.lower() for indicator in numeric_indicators)
        
        return False

    def get_column_distinct_values(self, column_name: str, limit: int = 50) -> Dict[str, Any]:
        """
        Get distinct values for a specific column
        
        Args:
            column_name: Name of the column to explore
            limit: Maximum number of values to return (default: 50)
            
        Returns:
            Dictionary containing distinct values and metadata
        """
        logger.info(f"Getting distinct values for column: {column_name}")
        
        if not self.db_analyzer:
            logger.error("Database analyzer not available for column exploration")
            return {
                "success": False,
                "error": "Database analyzer not available",
                "column": column_name,
                "values": [],
                "count": 0
            }
        
        try:
            # Check if this is a numeric column and skip it
            if self._is_numeric_column(column_name):
                logger.info(f"Skipping numeric column exploration for {column_name} to avoid context bloat")
                return {
                    "success": False,
                    "error": f"Column {column_name} is numeric and exploration is skipped to prevent context bloat",
                    "column": column_name,
                    "values": [],
                    "count": 0,
                    "skipped_reason": "numeric_column"
                }
            
            # Use the database analyzer's engine to get distinct values
            engine = self.db_analyzer.analyzer.engine
            table_name = self.db_analyzer.analyzer.table_name
            
            with engine.connect() as connection:
                from sqlalchemy import text
                
                # Get distinct values with count (SQLite doesn't use schema prefixes)
                query = text(f"""
                    SELECT DISTINCT "{column_name}", COUNT(*) as frequency
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    GROUP BY "{column_name}"
                    ORDER BY frequency DESC, "{column_name}"
                    LIMIT {limit}
                """)
                
                result = connection.execute(query)
                values_with_count = []
                total_count = 0
                
                for row in result:
                    value = str(row[0])
                    frequency = row[1]
                    values_with_count.append({
                        "value": value,
                        "frequency": frequency
                    })
                    total_count += frequency
                
                # Get total distinct count (SQLite doesn't use schema prefixes)
                total_distinct_query = text(f"""
                    SELECT COUNT(DISTINCT "{column_name}") as total_distinct
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                """)
                
                distinct_result = connection.execute(total_distinct_query)
                total_distinct = distinct_result.scalar()
                
                logger.info(f"Retrieved {len(values_with_count)} distinct values for {column_name}")
                
                return {
                    "success": True,
                    "column": column_name,
                    "values": values_with_count,
                    "count": len(values_with_count),
                    "total_distinct": total_distinct,
                    "showing_top": min(limit, total_distinct),
                    "has_more": total_distinct > limit
                }
                
        except Exception as e:
            logger.error(f"Error getting distinct values for {column_name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "column": column_name,
                "values": [],
                "count": 0
            }

    @observe_function("proactive_column_exploration")
    async def proactive_column_exploration(self, question: str, identified_columns: List[str]) -> Dict[str, Any]:
        """
        Proactively explore identified columns to get enum values before SQL generation.
        This is the second step in the enhanced workflow.
        
        Args:
            question: The natural language question
            identified_columns: List of column names identified as relevant
            
        Returns:
            Dictionary containing exploration results for each column
        """
        try:
            logger.info(f"Starting proactive column exploration for question: '{question}'")
            logger.info(f"Columns to explore: {identified_columns}")
            
            exploration_results = {}
            
            for column in identified_columns:
                # Skip numeric columns
                if self._is_numeric_column(column):
                    logger.info(f"Skipping numeric column: {column}")
                    continue
                    
                # Get distinct values for this column
                column_data = self.get_column_distinct_values(column)
                
                if column_data['success'] and column_data['values']:
                    exploration_results[column] = column_data
                    logger.info(f"Explored {column}: found {len(column_data['values'])} distinct values")
                else:
                    logger.warning(f"Failed to explore column {column}: {column_data.get('error', 'No values found')}")
            
            logger.info(f"Proactive exploration completed for {len(exploration_results)} columns")
            return exploration_results
            
        except Exception as e:
            logger.error(f"Error during proactive column exploration: {e}")
            return {}

    @observe_function("identify_relevant_columns")
    async def identify_relevant_columns(self, question: str) -> List[str]:
        """
        Identify columns that are relevant for filtering based on the question.
        This is the first step in the enhanced workflow.
        
        Args:
            question: The natural language question
            
        Returns:
            List of column names that are relevant for filtering
        """
        try:
            logger.info(f"Identifying relevant columns for question: '{question}'")
            
            # Prepare prompt for column identification
            prompt = f"""You are an expert database analyst who specializes in identifying relevant columns for filtering based on natural language questions.

Given the following database schema and user question, identify which columns are most likely to contain values that would be used for filtering or searching to answer the question.

### DATABASE SCHEMA:
{self.schema_context}

### USER QUESTION:
{question}

### INSTRUCTIONS:
1. Analyze the question to understand what the user is looking for
2. Identify columns that would contain values mentioned in the question or related concepts
3. Focus on categorical columns that would be used in WHERE clauses
4. Exclude numeric columns (rates, amounts, counts) unless explicitly mentioned as filter criteria
5. Include columns that might contain synonyms or related terms to what the user is asking about
6. Consider role-related columns, location columns, industry columns, etc.

### EXAMPLE:
Question: "What are the rates for SAP Developers?"
Relevant columns: ["normalized_role_title", "role_title_from_supplier", "role_specialization", "role_title_group"]

Question: "How much do Python developers earn in India?"
Relevant columns: ["normalized_role_title", "role_title_from_supplier", "skill_category", "country_of_work", "location"]

### OUTPUT FORMAT:
Return a JSON object with a "columns" array containing the column names:
{{"columns": ["column1", "column2", "column3"]}}

Do not include any explanatory text, markdown formatting, or code blocks outside the JSON."""

            # Get column identification from LLM
            response = await self.llm.ainvoke([{"role": "user", "content": prompt}])
            response_text = self._extract_response_content(response)
            
            # Parse the JSON response
            try:
                response_data = json.loads(response_text)
                columns = response_data.get("columns", [])
                logger.info(f"Identified {len(columns)} relevant columns: {columns}")
                return columns
            except json.JSONDecodeError:
                logger.error(f"Failed to parse column identification response: {response_text}")
                return []
            
        except Exception as e:
            logger.error(f"Error identifying relevant columns: {e}")
            return []

    def prepare_schema_context(self, db_analyzer) -> None:
        """Prepare schema context for SQL generation"""
        try:
            # Get the database schema context (use get_rich_schema_context instead of get_schema_context)
            schema_context = db_analyzer.get_rich_schema_context()
            
            # The rich schema context is already formatted, so use it directly
            self.schema_context = schema_context
            
        except Exception as e:
            logger.error(f"Error preparing schema context: {e}")
            self.schema_context = "Error loading schema information"
    
    def _extract_response_content(self, response) -> str:
        """Extract content from LLM response"""
        try:
            if hasattr(response, 'content'):
                return response.content.strip()
            elif hasattr(response, 'text'):
                return response.text.strip()
            elif isinstance(response, str):
                return response.strip()
            else:
                return str(response).strip()
        except Exception as e:
            logger.error(f"Error extracting response content: {e}")
            return "" 