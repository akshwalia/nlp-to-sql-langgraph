import json
import logging
import traceback
from decimal import Decimal
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from langchain_core.language_models import BaseLanguageModel
from ..database.connection.workspace_manager import WorkspaceManager
from .memory import MemoryManager
from .prompts import PromptsManager
from .sql_generation import SQLGenerationManager
from .execution import ExecutionManager
from ...observability.langfuse_config import observe_function

# Set up logging
logger = logging.getLogger(__name__)

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles Decimal, datetime, and other non-serializable types"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return super().default(obj)

class AnalyticalManager:
    """Manages analytical question generation and comprehensive analysis"""
    
    def __init__(self, workspace_manager: WorkspaceManager, memory_manager: MemoryManager, prompts_manager: PromptsManager):
        self.workspace_manager = workspace_manager
        self.memory_manager = memory_manager
        self.prompts_manager = prompts_manager
        self.llm = None
        self.sql_generation_manager = None
        self.execution_manager = None
        
        logger.info("AnalyticalManager initialized")
    
    def set_llm(self, llm: BaseLanguageModel):
        """Set the language model for analytical processing"""
        self.llm = llm
        logger.info(f"LLM set for AnalyticalManager: {type(llm).__name__}")
    
    def set_managers(self, sql_generation_manager: SQLGenerationManager, execution_manager: ExecutionManager):
        """Set the SQL generation and execution managers"""
        self.sql_generation_manager = sql_generation_manager
        self.execution_manager = execution_manager
        logger.info("SQL generation and execution managers set for AnalyticalManager")
        
    def _has_null_aggregation_results(self, results: List[Dict]) -> bool:
        """
        Check if query results contain null values from aggregation functions
        
        Args:
            results: Query results to check
            
        Returns:
            True if results contain null aggregation values, False otherwise
        """
        if not results or len(results) == 0:
            return False
        
        # Check each result row for null values
        for row in results:
            if isinstance(row, dict):
                for key, value in row.items():
                    # Check if this looks like an aggregation result (avg, sum, count, etc.)
                    key_lower = key.lower()
                    if any(agg_func in key_lower for agg_func in ['avg', 'sum', 'count', 'min', 'max', 'total', 'mean']):
                        if value is None:
                            logger.info(f"Found null aggregation result: {key} = {value}")
                            return True
        
        return False
    
    def _extract_columns_from_sql(self, sql: str, schema_context: str = "") -> List[str]:
        """
        Extract column names from SQL using database context and string matching
        
        Args:
            sql: SQL query string
            schema_context: Database schema context containing column names
            
        Returns:
            List of column names found in the SQL query
        """
        try:
            columns_found = []
            
            if not schema_context:
                logger.warning("No schema context provided for column extraction")
                return columns_found
            
            # Extract all column names from schema context
            schema_columns = []
            lines = schema_context.split('\n')
            in_columns_section = False
            
            for line in lines:
                if line.strip() == "COLUMNS:":
                    in_columns_section = True
                    continue
                elif in_columns_section and line.strip() and not line.strip().startswith('- '):
                    # We've left the COLUMNS section (line doesn't start with '- ')
                    break
                elif in_columns_section and line.strip().startswith('- '):
                    # Extract column name from lines like "- column_name: TYPE (Nullable: True)"
                    parts = line.split(':')
                    if len(parts) >= 2:
                        column_name = parts[0].strip().replace('- ', '')
                        schema_columns.append(column_name)
            
            logger.debug(f"Schema columns found: {schema_columns}")
            
            # Convert SQL to lowercase for case-insensitive matching
            sql_lower = sql.lower()
            
            # Check each schema column to see if it appears in the SQL
            for column_name in schema_columns:
                # Check if column name appears in the SQL (case-insensitive)
                column_lower = column_name.lower()
                
                # Simple substring check - if the column name appears in the SQL
                if column_lower in sql_lower:
                    # Additional check to avoid false positives by checking for word-like boundaries
                    import re
                    # Create a pattern that matches the column name with non-alphanumeric boundaries
                    pattern = r'(?<![a-zA-Z0-9_])' + re.escape(column_lower) + r'(?![a-zA-Z0-9_])'
                    if re.search(pattern, sql_lower):
                        columns_found.append(column_name)
                        logger.debug(f"Found column '{column_name}' in SQL")
            
            logger.info(f"Extracted {len(columns_found)} columns from SQL using schema context: {columns_found}")
            return columns_found
            
        except Exception as e:
            logger.error(f"Error extracting columns from SQL using schema context: {e}")
            return []

    def _extract_relevant_columns(self, question: str, schema_context: str, failed_queries: Optional[List[Dict]] = None) -> List[str]:
        """Extract potential column names that might be relevant to the question, excluding numeric columns"""
        relevant_columns = []
        
        try:
            # If we have failed queries, extract columns from their SQL first
            if failed_queries:
                logger.info(f"Extracting columns from {len(failed_queries)} failed queries")
                for failed_query in failed_queries:
                    sql = failed_query.get("sql", "")
                    if sql:
                        sql_columns = self._extract_columns_from_sql(sql, schema_context)
                        relevant_columns.extend(sql_columns)
                        logger.info(f"Found columns from failed query: {sql_columns}")
            
            # If we have columns from failed queries, use those and return early
            if relevant_columns:
                # Get numeric columns to exclude them from exploration
                numeric_columns = self._extract_numeric_columns_from_schema(schema_context)
                logger.info(f"Excluding {len(numeric_columns)} numeric columns from exploration: {numeric_columns}")
                
                # Filter out numeric columns
                filtered_columns = [col for col in set(relevant_columns) if col not in numeric_columns]
                logger.info(f"Selected {len(filtered_columns)} categorical columns from failed queries: {filtered_columns}")
                return filtered_columns[:5]  # Limit to top 5 most relevant columns
            
            # Fallback to question-based extraction (original logic)
            # Common patterns for role-related columns
            role_patterns = ["role", "title", "job", "position", "designation"]
            location_patterns = ["country", "location", "region", "city", "site"]
            rate_patterns = ["rate", "salary", "wage", "cost", "price", "hourly"]
            
            question_lower = question.lower()
            
            # Extract all column names from schema context
            all_columns = []
            lines = schema_context.split('\n')
            in_columns_section = False
            
            for line in lines:
                if line.strip() == "COLUMNS:":
                    in_columns_section = True
                    continue
                elif in_columns_section and line.strip() and not line.startswith('  -'):
                    break
                elif in_columns_section and line.strip().startswith('- '):
                    parts = line.split(':')
                    if len(parts) >= 2:
                        column_name = parts[0].strip().replace('- ', '')
                        all_columns.append(column_name)
            
            # Get numeric columns to exclude them from exploration
            numeric_columns = self._extract_numeric_columns_from_schema(schema_context)
            logger.info(f"Excluding {len(numeric_columns)} numeric columns from exploration: {numeric_columns}")
            
            # Find relevant columns based on question content, excluding numeric ones
            for column in all_columns:
                # Skip numeric columns to avoid huge context
                if column in numeric_columns:
                    continue
                    
                column_lower = column.lower()
                
                # Check if any pattern matches
                if any(pattern in column_lower for pattern in role_patterns):
                    relevant_columns.append(column)
                elif any(pattern in column_lower for pattern in location_patterns):
                    relevant_columns.append(column)
                elif any(pattern in column_lower for pattern in rate_patterns):
                    relevant_columns.append(column)
                elif any(word in column_lower for word in question_lower.split() if len(word) > 2):
                    relevant_columns.append(column)
            
            logger.info(f"Selected {len(relevant_columns)} categorical columns for exploration: {relevant_columns}")
            return relevant_columns[:5]  # Limit to top 5 most relevant columns
        except Exception as e:
            logger.error(f"Error extracting relevant columns: {e}")
            return []
    
    def _extract_numeric_columns_from_schema(self, schema_context: str) -> List[str]:
        """Extract numeric column names from schema context to exclude them from exploration"""
        numeric_columns = []
        
        try:
            lines = schema_context.split('\n')
            in_columns_section = False
            
            for line in lines:
                # Check if we're in the COLUMNS section
                if line.strip() == "COLUMNS:":
                    in_columns_section = True
                    continue
                elif in_columns_section and line.strip() and not line.startswith('  -'):
                    # We've left the COLUMNS section
                    break
                elif in_columns_section and line.strip().startswith('- '):
                    # Extract column name and type from lines like "  - column_name: TYPE (Nullable: True)"
                    parts = line.split(':')
                    if len(parts) >= 2:
                        column_name = parts[0].strip().replace('- ', '')
                        type_info = parts[1].strip()
                        
                        # Check if it's a numeric type
                        if any(numeric_type in type_info.upper() for numeric_type in [
                            'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'SERIAL', 'BIGSERIAL',
                            'FLOAT', 'DOUBLE', 'REAL', 'NUMERIC', 'DECIMAL', 'MONEY'
                        ]):
                            numeric_columns.append(column_name)
            
        except Exception as e:
            logger.error(f"Error extracting numeric columns: {e}")
        
        return numeric_columns
    
    async def _enhance_query_with_column_exploration(self, question: str, schema_context: str, failed_queries: Optional[List[Dict]] = None) -> List[Dict]:
        """
        Use column exploration to enhance context and let LLM generate better queries
        
        Args:
            question: The original question
            schema_context: Database schema context
            failed_queries: List of queries that failed or returned no results
            
        Returns:
            List of enhanced queries generated by LLM with column exploration context
        """
        logger.info(f"Enhancing query with column exploration for: {question}")
        
        if not self.sql_generation_manager or not hasattr(self.sql_generation_manager, 'explore_column_values'):
            logger.warning("Column exploration not available")
            return []
        
        # Extract relevant columns from question and schema
        relevant_columns = self._extract_relevant_columns(question, schema_context, failed_queries)
        
        if not relevant_columns:
            logger.warning("No relevant columns found for exploration")
            return []
        
        # Explore column values
        exploration_results = self.sql_generation_manager.explore_column_values(question, relevant_columns)
        
        # Build enhanced context with actual column values
        enhanced_context = self._build_enhanced_context(exploration_results, question)
        
        if not enhanced_context:
            logger.warning("No enhanced context generated from column exploration")
            return []
        
        # Send enhanced context to LLM to generate new queries
        return await self._generate_contextual_queries_with_enhancement(question, schema_context, enhanced_context)
    
    def _build_enhanced_context(self, exploration_results: Dict[str, Any], question: str) -> str:
        """
        Build enhanced context string with discovered column values for LLM
        
        Args:
            exploration_results: Results from column exploration
            question: Original question for context
            
        Returns:
            Enhanced context string with actual column values
        """
        context_parts = []
        context_parts.append("COLUMN EXPLORATION RESULTS:")
        context_parts.append(f"Based on your question '{question}', here are the actual values found in relevant columns:")
        context_parts.append("")
        
        for column_name, exploration_result in exploration_results.items():
            if not exploration_result.get("success", False):
                context_parts.append(f"- {column_name}: [Error: {exploration_result.get('error', 'Unknown error')}]")
                continue
            
            values = exploration_result.get("values", [])
            if not values:
                context_parts.append(f"- {column_name}: [No values found]")
                continue
            
            total_distinct = exploration_result.get("total_distinct", 0)
            showing_count = exploration_result.get("count", 0)
            
            context_parts.append(f"- {column_name}: {total_distinct} total distinct values (showing top {showing_count}):")
            
            # Find values that match the question context
            question_lower = question.lower()
            question_words = [word.strip() for word in question_lower.split() if len(word.strip()) > 2]
            matching_values = []
            other_values = []
            
            for value_info in values:
                value = value_info["value"]
                frequency = value_info["frequency"]
                value_lower = value.lower()
                
                # Check if value matches question context - improved matching
                is_match = False
                
                # Check for direct substring matches
                for word in question_words:
                    if word in value_lower or value_lower in word:
                        is_match = True
                        break
                
                # Check for partial word matches (e.g., "BI" in "BI Developer" when question has "bi")
                value_words = [w.strip() for w in value_lower.split() if len(w.strip()) > 1]
                for q_word in question_words:
                    for v_word in value_words:
                        if q_word in v_word or v_word in q_word:
                            is_match = True
                            break
                    if is_match:
                        break
                
                if is_match:
                    matching_values.append(f"    * '{value}' (frequency: {frequency}) [MATCHES QUESTION]")
                else:
                    other_values.append(f"    - '{value}' (frequency: {frequency})")
            
            # Show matching values first
            if matching_values:
                context_parts.append("  RELEVANT VALUES:")
                context_parts.extend(matching_values)
            
            # Show other high-frequency values
            if other_values:
                context_parts.append("  OTHER HIGH-FREQUENCY VALUES:")
                context_parts.extend(other_values[:5])  # Show top 5 other values
            
            context_parts.append("")
        
        context_parts.append("CRITICAL INSTRUCTIONS FOR USING THESE VALUES:")
        context_parts.append("- **MANDATORY**: Use the EXACT column values shown above in your WHERE clauses")
        context_parts.append("- **DO NOT EXPAND**: Do not expand abbreviations (e.g., 'BI Developer' must stay 'BI Developer', NOT 'Business Intelligence Developer')")
        context_parts.append("- **DO NOT INTERPRET**: Do not interpret or rephrase values (e.g., 'Dev' must stay 'Dev', NOT 'Developer')")
        context_parts.append("- **EXACT MATCH ONLY**: Copy the values character-for-character exactly as shown")
        context_parts.append("- **CASE SENSITIVE**: Preserve exact spelling, casing, and punctuation")
        context_parts.append("- **PREFER MATCHES**: Use values marked with [MATCHES QUESTION] as they are most relevant")
        context_parts.append("- **FALLBACK**: If no exact matches, use the highest frequency values exactly as shown")
        context_parts.append("- **CRITICAL - NO LIKE PATTERNS**: Since exact values are provided, use equality operators (=) NOT LIKE patterns")
        context_parts.append("- **LIKE ONLY WHEN NO EXACT VALUES**: Use LIKE patterns ONLY when no exact values are available for the concept")
        context_parts.append("")
        context_parts.append("EXAMPLES OF CORRECT USAGE:")
        context_parts.append("- If you see: 'BI Developer' (frequency: 39) [MATCHES QUESTION]")
        context_parts.append("- ✅ CORRECT: WHERE normalized_role_title = 'BI Developer'")
        context_parts.append("- ❌ WRONG: WHERE normalized_role_title = 'Business Intelligence Developer'")
        context_parts.append("- ❌ WRONG: WHERE normalized_role_title LIKE '%Developer%'")
        context_parts.append("- ❌ WRONG: WHERE role_title_from_supplier LIKE '%BI%'")
        context_parts.append("")
        context_parts.append("- If you see: 'SAP' (frequency: 145) [MATCHES QUESTION]")
        context_parts.append("- ✅ CORRECT: WHERE role_specialization = 'SAP'")
        context_parts.append("- ❌ WRONG: WHERE role_specialization LIKE '%SAP%'")
        context_parts.append("")
        
        return "\n".join(context_parts)
    
    async def _generate_contextual_queries_with_enhancement(self, question: str, schema_context: str, enhanced_context: str) -> List[Dict]:
        """
        Generate contextual queries using LLM with enhanced column exploration context
        
        Args:
            question: Original question
            schema_context: Database schema context
            enhanced_context: Enhanced context with actual column values
            
        Returns:
            List of queries generated by LLM with enhanced context
        """
        if not self.llm:
            logger.warning("LLM not available for enhanced query generation")
            return []
            
        try:
            logger.info(f"Generating enhanced contextual queries with column exploration context")
            
            # Create enhanced schema context by combining original schema with exploration results
            combined_context = f"{schema_context}\n\n{enhanced_context}"
            
            # Get memory context
            memory_context = self.memory_manager.get_memory_context(question) if self.memory_manager.use_memory else ""
            
            # Prepare prompt values for enhanced query generation
            prompt_values = {
                "question": question,
                "schema": combined_context,
                "memory": memory_context
            }
            
            # Generate enhanced contextual queries using LLM
            response = await self.llm.ainvoke(
                self.prompts_manager.contextual_query_generation_prompt.format_messages(**prompt_values)
            )
            
            queries_text = self._extract_response_content(response)
            
            json_text = self._extract_json_from_response(queries_text)
            
            try:
                queries_data = json.loads(json_text)
                contextual_queries = queries_data.get("queries", [])
                
                logger.info(f"Generated {len(contextual_queries)} enhanced contextual queries")
                
                # Mark queries as enhanced with exploration
                for query in contextual_queries:
                    query["enhanced_with_exploration"] = True
                
                return contextual_queries
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse enhanced queries JSON: {e}")
                return []
                
        except Exception as e:
            logger.error(f"Error generating enhanced contextual queries: {e}")
            return []
         
    @observe_function("analytical_questions_generation")
    async def generate_analytical_questions(self, user_query: str, schema_context: str = "") -> Dict[str, Any]:
        """Generate analytical questions for a user query"""
        logger.info(f"🔍 Starting analytical questions generation for query: '{user_query}'")
        
        if not self.llm:
            error_msg = "LLM not set for analytical questions generation"
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg, "questions": []}
        
        try:
            print(f"🔍 DEBUG: Starting analytical questions generation for query: '{user_query}'")
            
            # Get memory context
            memory_context = self.memory_manager.get_memory_context(user_query) if self.memory_manager.use_memory else ""
            print(f"🔍 DEBUG: Memory context retrieved: {len(memory_context) if memory_context else 0} characters")
            logger.debug(f"Memory context length: {len(memory_context) if memory_context else 0}")
            
            # Prepare prompt values
            prompt_values = {
                "schema": schema_context,
                "user_query": user_query,
                "memory": memory_context
            }
            print(f"🔍 DEBUG: Prompt values prepared with schema: {len(schema_context) if schema_context else 0} characters")
            logger.debug(f"Schema context length: {len(schema_context) if schema_context else 0}")
            
            # Generate analytical questions
            print(f"🔍 DEBUG: Calling LLM for analytical questions generation...")
            logger.info("Calling LLM for analytical questions generation")
            
            response = await self.llm.ainvoke(
                self.prompts_manager.analytical_questions_prompt.format_messages(**prompt_values)
            )
            print(f"🔍 DEBUG: LLM response received. Type: {type(response)}")
            logger.debug(f"LLM response type: {type(response)}")
            
            questions_text = self._extract_response_content(response)
            print(f"🔍 DEBUG: Raw response text extracted ({len(questions_text)} chars):")
            print(f"🔍 DEBUG: Raw response content: '{questions_text}'")
            logger.debug(f"Raw response extracted: {len(questions_text)} characters")
            
            # Clean the response to extract JSON
            json_text = self._extract_json_from_response(questions_text)
            print(f"🔍 DEBUG: JSON text after cleaning ({len(json_text)} chars):")
            print(f"🔍 DEBUG: Cleaned JSON content: '{json_text}'")
            logger.debug(f"Cleaned JSON length: {len(json_text)} characters")
            
            # Parse JSON
            try:
                questions_data = json.loads(json_text)
                print(f"🔍 DEBUG: Successfully parsed JSON. Type: {type(questions_data)}")
                logger.info(f"Successfully parsed JSON response")
                
                if isinstance(questions_data, dict) and "questions" in questions_data:
                    questions = questions_data["questions"]
                    print(f"🔍 DEBUG: Found {len(questions)} questions in response")
                    logger.info(f"Found {len(questions)} analytical questions")
                    
                    return {
                        "success": True,
                        "questions": questions,
                        "total_questions": len(questions),
                        "user_query": user_query
                    }
                else:
                    logger.warning("JSON response doesn't contain 'questions' key, using fallback")
                    return self._extract_questions_fallback(questions_text, user_query)
                    
            except json.JSONDecodeError as e:
                print(f"🔍 DEBUG: JSON parsing failed: {e}")
                logger.warning(f"JSON parsing failed: {e}, using fallback method")
                return self._extract_questions_fallback(questions_text, user_query)
            
        except Exception as e:
            print(f"🔍 DEBUG: Exception in generate_analytical_questions: {e}")
            print(f"🔍 DEBUG: Exception type: {type(e)}")
            
            traceback_str = traceback.format_exc()
            print(f"🔍 DEBUG: Full traceback:\n{traceback_str}")
            
            logger.error(f"Error generating analytical questions: {e}")
            logger.error(f"Full traceback: {traceback_str}")
            
            return {
                "success": False,
                "error": f"Error generating analytical questions: {str(e)}",
                "questions": [],
                "user_query": user_query
            }
    
    @observe_function("analytical_workflow_execution")
    async def execute_analytical_workflow(self, user_query: str, questions: List[Dict], schema_context: str = "") -> Dict[str, Any]:
        """Execute the full analytical workflow with intelligent query planning and scoring"""
        logger.info(f"🔍 Starting intelligent analytical workflow for query: '{user_query}' with {len(questions)} questions")
        
        if not self.sql_generation_manager or not self.execution_manager:
            error_msg = "SQL generation or execution manager not set"
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg, "analytical_results": []}

        try:
            analytical_results = []
            successful_executions = 0
            failed_executions = 0
            total_execution_time = 0
            
            # Execute each analytical question with intelligent planning
            for i, question_data in enumerate(questions):
                question = question_data.get("question", "") if isinstance(question_data, dict) else str(question_data)
                priority = question_data.get("priority", "medium") if isinstance(question_data, dict) else "medium"
                
                logger.info(f"🔍 Processing question {i+1}/{len(questions)}: {question}")
                print(f"🔍 DEBUG: Processing analytical question {i+1}/{len(questions)}: '{question}' (Priority: {priority})")
                
                # Step 1: ALWAYS do proactive column exploration first (regardless of query complexity)
                print(f"🔍 DEBUG: Starting proactive column exploration for question {i+1}")
                
                # Identify relevant columns for filtering
                identified_columns = []
                exploration_results = {}
                enhanced_schema_context = schema_context
                
                if self.sql_generation_manager:
                    identified_columns = await self.sql_generation_manager.identify_relevant_columns(question)
                    logger.info(f"🔍 Identified {len(identified_columns)} relevant columns: {identified_columns}")
                    print(f"🔍 DEBUG: Identified columns: {identified_columns}")
                    
                    # Proactively explore the identified columns
                    if identified_columns:
                        exploration_results = await self.sql_generation_manager.proactive_column_exploration(
                            question, identified_columns
                        )
                        logger.info(f"🔍 Explored {len(exploration_results)} columns")
                        print(f"🔍 DEBUG: Explored {len(exploration_results)} columns with values")
                        
                        # Build enhanced context with exploration results
                        enhanced_context = self._build_enhanced_context(exploration_results, question)
                        if enhanced_context:
                            enhanced_schema_context = schema_context + f"\n\n### COLUMN EXPLORATION RESULTS:\n{enhanced_context}"
                            print(f"🔍 DEBUG: Enhanced context created with {len(enhanced_context)} characters")
                
                # Step 2: Intelligent Query Planning - Let LLM decide if multiple queries are needed (using enhanced context)
                planning_result = await self._plan_query_approach(question, enhanced_schema_context)
                
                if planning_result["needs_multiple_queries"]:
                    logger.info(f"📊 LLM decided multiple queries needed for question {i+1}: {planning_result['reasoning']}")
                    print(f"🔍 DEBUG: Multiple queries needed: {planning_result['reasoning']}")
                    
                    # Generate contextual queries using enhanced schema context
                    contextual_queries = await self._generate_contextual_queries(question, enhanced_schema_context)
                    
                    if contextual_queries:
                        # Execute all contextual queries
                        query_results = await self._execute_multiple_queries(contextual_queries, question)
                        
                        # Step 2: Score query results for quality and relevance
                        scored_results = await self._score_query_results(query_results, question)
                        
                        # Step 3: Filter and prioritize based on scores
                        filtered_results = self._filter_by_quality_score(scored_results)
                        
                        if filtered_results:
                            successful_executions += 1
                            total_execution_time += sum(r.get("execution_time", 0) for r in filtered_results)
                            
                            # Combine results with weights based on scores
                            all_results = []
                            total_score = sum(r.get("quality_score", 0) for r in filtered_results)
                            
                            for result in filtered_results:
                                if result["success"] and result["results"]:
                                    weight = result.get("quality_score", 0) / total_score if total_score > 0 else 1.0
                                    result["weight"] = weight
                                    all_results.extend(result["results"])
                            
                            analytical_results.append({
                                "question": question,
                                "priority": priority,
                                "sql": f"Intelligent multi-query approach ({len(filtered_results)} high-quality queries)",
                                "execution_success": True,
                                "results": all_results,
                                "individual_queries": filtered_results,
                                "error": None,
                                "row_count": len(all_results),
                                "execution_time": sum(r.get("execution_time", 0) for r in filtered_results),
                                "successful_queries": len(filtered_results),
                                "total_queries": len(contextual_queries),
                                "approach": "intelligent_multi_query",
                                "planning_reasoning": planning_result["reasoning"]
                            })
                        else:
                            failed_executions += 1
                            analytical_results.append({
                                "question": question,
                                "priority": priority,
                                "sql": "Multiple queries attempted but none met quality threshold",
                                "execution_success": False,
                                "results": [],
                                "error": "No high-quality results found",
                                "approach": "intelligent_multi_query_failed"
                            })
                    else:
                        # Fallback to single query
                        result = await self._execute_single_query(question)
                        analytical_results.append(result)
                        if result["execution_success"]:
                            successful_executions += 1
                        else:
                            failed_executions += 1
                else:
                    logger.info(f"📝 LLM decided single query sufficient for question {i+1}: {planning_result['reasoning']}")
                    print(f"🔍 DEBUG: Single query sufficient: {planning_result['reasoning']}")
                    
                    # Execute single optimized query using enhanced context
                    result = await self._execute_single_query_with_enhancement(question, enhanced_schema_context)
                    result["approach"] = "intelligent_single_query"
                    result["planning_reasoning"] = planning_result["reasoning"]
                    result["enhanced_with_exploration"] = len(exploration_results) > 0
                    result["explored_columns"] = list(exploration_results.keys()) if exploration_results else []
                    analytical_results.append(result)
                    
                    if result["execution_success"]:
                        successful_executions += 1
                        total_execution_time += result.get("execution_time", 0)
                    else:
                        failed_executions += 1
            
            logger.info(f"🔍 Intelligent analytical workflow completed: {successful_executions} successful, {failed_executions} failed, total time: {total_execution_time:.2f}s")
            print(f"🔍 DEBUG: Intelligent analytical workflow completed: {successful_executions} successful, {failed_executions} failed")
            
            return {
                "success": True,
                "user_query": user_query,
                "analytical_results": analytical_results,
                "total_questions": len(questions),
                "successful_executions": successful_executions,
                "failed_executions": failed_executions,
                "total_execution_time": total_execution_time,
                "approach": "intelligent_workflow"
            }
            
        except Exception as e:
            logger.error(f"Error executing intelligent analytical workflow: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            return {
                "success": False,
                "error": f"Error executing intelligent analytical workflow: {str(e)}",
                "user_query": user_query,
                "analytical_results": [],
                "total_questions": 0,
                "successful_executions": 0,
                "failed_executions": 0,
                "total_execution_time": 0
            }

    async def _plan_query_approach(self, question: str, schema_context: str) -> Dict[str, Any]:
        """Let LLM decide if multiple queries are needed for this question"""
        if not self.llm:
            return {
                "needs_multiple_queries": False,
                "reasoning": "LLM not available for query planning",
                "suggested_explorations": []
            }
            
        try:
            logger.info(f"🤔 Planning query approach for: {question}")
            
            prompt_values = {
                "question": question,
                "schema": schema_context
            }
            
            response = await self.llm.ainvoke(
                self.prompts_manager.query_planning_prompt.format_messages(**prompt_values)
            )
            
            planning_text = self._extract_response_content(response)
            planning_json = self._extract_json_from_response(planning_text)
            
            try:
                planning_result = json.loads(planning_json)
                logger.info(f"✅ Query planning completed: {planning_result.get('reasoning', 'No reasoning provided')}")
                return planning_result
            except json.JSONDecodeError:
                logger.warning("Failed to parse planning response, defaulting to single query")
                return {
                    "needs_multiple_queries": False,
                    "reasoning": "Failed to parse LLM planning response",
                    "suggested_explorations": []
                }
        except Exception as e:
            logger.error(f"Error in query planning: {e}")
            return {
                "needs_multiple_queries": False,
                "reasoning": f"Planning error: {str(e)}",
                "suggested_explorations": []
            }

    async def _generate_contextual_queries(self, question: str, schema_context: str) -> List[Dict]:
        """Generate contextual queries for a given question"""
        if not self.llm:
            logger.warning("LLM not available for contextual query generation")
            return []
            
        try:
            logger.info(f"Generating contextual queries for: '{question}'")
            
            # Get memory context
            memory_context = self.memory_manager.get_memory_context(question) if self.memory_manager.use_memory else ""
            
            # Prepare the enhanced prompt
            prompt_values = {
                "schema": schema_context,
                "question": question
            }
            
            # Include memory context if available
            if memory_context:
                prompt_values["memory"] = memory_context
            
            # Generate queries using the contextual prompt
            response = await self.llm.ainvoke(
                self.prompts_manager.contextual_query_generation_prompt.format_messages(**prompt_values)
            )
            
            response_text = self._extract_response_content(response)
            
            # Parse the JSON response
            try:
                import json
                response_data = json.loads(response_text)
                queries = response_data.get("queries", [])
                
                logger.info(f"Generated {len(queries)} contextual queries")
                return queries
                
            except json.JSONDecodeError:
                logger.error(f"Failed to parse contextual queries response: {response_text}")
                return []
            
        except Exception as e:
            logger.error(f"Error generating contextual queries: {e}")
            return []

    @observe_function("enhanced_contextual_queries")
    async def _generate_enhanced_contextual_queries(self, question: str, schema_context: str) -> List[Dict]:
        """
        Generate contextual queries using the enhanced workflow with proactive column exploration.
        This is the enhanced version that identifies relevant columns first, then explores them.
        
        Args:
            question: The natural language question
            schema_context: The database schema context
            
        Returns:
            List of generated query dictionaries
        """
        try:
            logger.info(f"Starting enhanced contextual query generation for: '{question}'")
            
            # Step 1: Identify relevant columns for filtering
            if self.sql_generation_manager:
                identified_columns = await self.sql_generation_manager.identify_relevant_columns(question)
                logger.info(f"Identified {len(identified_columns)} relevant columns: {identified_columns}")
            else:
                logger.warning("No SQL generation manager available for column identification")
                identified_columns = []
            
            # Step 2: Proactively explore the identified columns
            exploration_results = {}
            if identified_columns and self.sql_generation_manager:
                exploration_results = await self.sql_generation_manager.proactive_column_exploration(
                    question, identified_columns
                )
                logger.info(f"Explored {len(exploration_results)} columns")
            
            # Step 3: Build enhanced context with exploration results
            enhanced_context = self._build_enhanced_context(exploration_results, question)
            
            # Step 4: Generate queries with enhanced context
            enhanced_schema_context = schema_context
            if enhanced_context:
                enhanced_schema_context += f"\n\n### COLUMN EXPLORATION RESULTS:\n{enhanced_context}"
            
            return await self._generate_contextual_queries(question, enhanced_schema_context)
            
        except Exception as e:
            logger.error(f"Error generating enhanced contextual queries: {e}")
            # Fallback to regular contextual queries
            return await self._generate_contextual_queries(question, schema_context)

    async def _execute_multiple_queries(self, contextual_queries: List[Dict], question: str) -> List[Dict]:
        """Execute multiple queries and return results with column exploration retry"""
        query_results = []
        failed_queries = []
        
        for i, query_info in enumerate(contextual_queries):
            sql = query_info["sql"]
            description = query_info["description"]
            query_type = query_info["type"]
            
            logger.debug(f"Executing query {i+1}/{len(contextual_queries)}: {description}")
            print(f"🔍 DEBUG: Executing query {i+1}: {description}")
            print(f"🔍 DEBUG: SQL: {sql}")
            
            if not self.execution_manager:
                print(f"🔍 DEBUG: No execution manager available!")
                exec_result = {"success": False, "error": "No execution manager available"}
            else:
                print(f"🔍 DEBUG: Calling execution manager...")
                exec_result = await self.execution_manager.execute_query(description, sql)
                print(f"🔍 DEBUG: Execution result: success={exec_result.get('success')}, error={exec_result.get('error')}")
            
            query_result = {
                "query_description": description,
                "query_type": query_type,
                "sql": sql,
                "results": exec_result["results"] if exec_result["success"] else [],
                "row_count": exec_result.get("row_count", 0),
                "execution_time": exec_result.get("execution_time", 0),
                "success": exec_result["success"],
                "error": exec_result.get("error") if not exec_result["success"] else None
            }
            
            # Check if query failed, returned no results, or has null aggregation results
            has_null_aggregation = exec_result["success"] and self._has_null_aggregation_results(exec_result.get("results", []))
            
            if not exec_result["success"] or exec_result.get("row_count", 0) == 0 or has_null_aggregation:
                failed_queries.append(query_result)
                if has_null_aggregation:
                    logger.warning(f"Query {i+1} returned null aggregation results: {description}")
                    print(f"🔍 DEBUG: Query {i+1} returned null aggregation results, will attempt column exploration")
                else:
                    logger.warning(f"Query {i+1} failed or returned no results: {description}")
                    print(f"🔍 DEBUG: Query {i+1} failed or returned no results, will attempt column exploration")
            else:
                query_results.append(query_result)
        
        # If some queries failed, returned no results, or had null aggregation results, try column exploration
        if failed_queries and hasattr(self, '_enhance_query_with_column_exploration'):
            logger.info(f"Attempting column exploration for {len(failed_queries)} failed queries")
            print(f"🔍 DEBUG: Attempting column exploration for {len(failed_queries)} failed queries")
            
            try:
                # Use the schema context from the SQL generation manager
                schema_context = ""
                if self.sql_generation_manager and self.sql_generation_manager.schema_context:
                    schema_context = self.sql_generation_manager.schema_context
                
                # Generate enhanced queries using column exploration with failed queries
                enhanced_queries = await self._enhance_query_with_column_exploration(
                    question, schema_context, failed_queries
                )
                
                if enhanced_queries:
                    logger.info(f"Generated {len(enhanced_queries)} enhanced queries, executing...")
                    print(f"🔍 DEBUG: Generated {len(enhanced_queries)} enhanced queries, executing...")
                    
                    # Execute enhanced queries
                    for enhanced_query in enhanced_queries:
                        sql = enhanced_query["sql"]
                        description = enhanced_query["description"]
                        query_type = enhanced_query["type"]
                        
                        logger.debug(f"Executing enhanced query: {description}")
                        print(f"🔍 DEBUG: Executing enhanced query: {description}")
                        print(f"🔍 DEBUG: Enhanced SQL: {sql}")
                        
                        if not self.execution_manager:
                            logger.error("Execution manager not available for enhanced query")
                            print(f"🔍 DEBUG: Execution manager not available for enhanced query")
                            continue
                        
                        exec_result = await self.execution_manager.execute_query(description, sql)
                        print(f"🔍 DEBUG: Enhanced execution result: success={exec_result.get('success')}, row_count={exec_result.get('row_count', 0)}")
                        
                        enhanced_result = {
                            "query_description": description,
                            "query_type": query_type,
                            "sql": sql,
                            "results": exec_result["results"] if exec_result["success"] else [],
                            "row_count": exec_result.get("row_count", 0),
                            "execution_time": exec_result.get("execution_time", 0),
                            "success": exec_result["success"],
                            "error": exec_result.get("error") if not exec_result["success"] else None,
                            "enhanced_with_exploration": True  # Mark as enhanced by LLM with column exploration
                        }
                        
                        # Check for null aggregation results in enhanced queries too
                        has_null_aggregation = exec_result["success"] and self._has_null_aggregation_results(exec_result.get("results", []))
                        
                        if exec_result["success"] and exec_result.get("row_count", 0) > 0 and not has_null_aggregation:
                            query_results.append(enhanced_result)
                            logger.info(f"Enhanced query succeeded with {exec_result.get('row_count', 0)} rows")
                            print(f"🔍 DEBUG: Enhanced query succeeded with {exec_result.get('row_count', 0)} rows")
                        else:
                            if has_null_aggregation:
                                logger.warning(f"Enhanced query returned null aggregation results: {description}")
                                print(f"🔍 DEBUG: Enhanced query returned null aggregation results")
                            else:
                                logger.warning(f"Enhanced query also failed or returned no results: {description}")
                                print(f"🔍 DEBUG: Enhanced query also failed or returned no results")
                else:
                    logger.warning("No enhanced queries generated from column exploration")
                    print(f"🔍 DEBUG: No enhanced queries generated from column exploration")
                    
            except Exception as e:
                logger.error(f"Error during column exploration: {e}")
                print(f"🔍 DEBUG: Error during column exploration: {e}")
        
        return query_results

    async def _score_query_results(self, query_results: List[Dict], original_question: str) -> List[Dict]:
        """Score query results for quality and relevance"""
        if not self.llm:
            # Assign default scores based on row count and success
            for result in query_results:
                if result["success"]:
                    result["quality_score"] = min(50 + result["row_count"], 100)
                else:
                    result["quality_score"] = 0
                result["score_reasoning"] = "LLM not available for scoring"
                result["key_insights"] = []
            return query_results
            
        try:
            logger.info(f"📊 Scoring {len(query_results)} query results for relevance and quality")
            
            # Prepare query results summary for scoring
            results_summary = []
            for result in query_results:
                if result["success"]:
                    results_summary.append({
                        "query_description": result["query_description"],
                        "row_count": result["row_count"],
                        "execution_time": result["execution_time"],
                        "sample_results": result["results"][:3] if result["results"] else []
                    })
                else:
                    results_summary.append({
                        "query_description": result["query_description"],
                        "error": result["error"],
                        "row_count": 0
                    })
            
            prompt_values = {
                "original_question": original_question,
                "query_results": json.dumps(results_summary, indent=2, cls=DecimalEncoder)
            }
            
            response = await self.llm.ainvoke(
                self.prompts_manager.query_scoring_prompt.format_messages(**prompt_values)
            )
            
            scoring_text = self._extract_response_content(response)
            scoring_json = self._extract_json_from_response(scoring_text)
            
            try:
                scoring_result = json.loads(scoring_json)
                scores = scoring_result.get("scores", [])
                
                # Apply scores to query results
                for i, result in enumerate(query_results):
                    if i < len(scores):
                        result["quality_score"] = scores[i].get("score", 0)
                        result["score_reasoning"] = scores[i].get("reasoning", "")
                        result["key_insights"] = scores[i].get("key_insights", [])
                    else:
                        result["quality_score"] = 0
                        result["score_reasoning"] = "No score provided"
                        result["key_insights"] = []
                
                best_score = max(s.get('score', 0) for s in scores) if scores else 0
                logger.info(f"✅ Query scoring completed. Best score: {best_score}")
                return query_results
                
            except json.JSONDecodeError:
                logger.warning("Failed to parse scoring response, assigning default scores")
                # Assign default scores based on row count and success
                for result in query_results:
                    if result["success"]:
                        result["quality_score"] = min(50 + result["row_count"], 100)
                    else:
                        result["quality_score"] = 0
                    result["score_reasoning"] = "Default scoring due to parse error"
                    result["key_insights"] = []
                return query_results
                
        except Exception as e:
            logger.error(f"Error scoring query results: {e}")
            # Assign default scores
            for result in query_results:
                result["quality_score"] = 50 if result["success"] else 0
                result["score_reasoning"] = f"Error in scoring: {str(e)}"
                result["key_insights"] = []
            return query_results

    def _filter_by_quality_score(self, scored_results: List[Dict], min_score: int = 60) -> List[Dict]:
        """Filter query results by quality score and keep only the best ones"""
        # Filter out failed queries and low-scoring queries
        valid_results = [r for r in scored_results if r["success"] and r.get("quality_score", 0) >= min_score]
        
        if not valid_results:
            # If no results meet the threshold, lower it and try again
            min_score = 30
            valid_results = [r for r in scored_results if r["success"] and r.get("quality_score", 0) >= min_score]
        
        if not valid_results:
            # If still no results, take the best successful ones
            successful_results = [r for r in scored_results if r["success"]]
            if successful_results:
                valid_results = sorted(successful_results, key=lambda x: x.get("quality_score", 0), reverse=True)[:3]
        
        # Sort by score and take top results
        filtered_results = sorted(valid_results, key=lambda x: x.get("quality_score", 0), reverse=True)
        
        logger.info(f"🎯 Filtered to {len(filtered_results)} high-quality results (min score: {min_score})")
        print(f"🔍 DEBUG: Filtering details:")
        print(f"  Input results: {len(scored_results)}")
        print(f"  Min score threshold: {min_score}")
        for i, result in enumerate(scored_results):
            score = result.get("quality_score", 0)
            success = result.get("success", False)
            print(f"  Result {i+1}: Score={score}, Success={success}, Meets threshold={score >= min_score and success}")
        
        for result in filtered_results:
            logger.debug(f"  - {result['query_description']}: Score {result.get('quality_score', 0)}")
        
        return filtered_results

    async def _execute_single_query(self, question: str) -> Dict[str, Any]:
        """Execute a single optimized query for the question using contextual generation"""
        try:
            # Check if required managers are available
            if not self.sql_generation_manager or not self.execution_manager:
                return {
                    "question": question,
                    "priority": "medium",
                    "sql": "",
                    "execution_success": False,
                    "results": [],
                    "error": "Required managers not available",
                    "row_count": 0,
                    "execution_time": 0,
                    "quality_score": 0
                }
            
            # Use contextual query generation even for single queries
            schema_context = self.sql_generation_manager.schema_context or ""
            contextual_queries = await self._generate_contextual_queries(question, schema_context)
            
            if contextual_queries:
                # Use the first (best) contextual query
                best_query = contextual_queries[0]
                exec_result = await self.execution_manager.execute_query(best_query["description"], best_query["sql"])
                
                return {
                    "question": question,
                    "priority": "medium",
                    "sql": best_query["sql"],
                    "execution_success": exec_result["success"],
                    "results": exec_result["results"] if exec_result["success"] else [],
                    "error": exec_result.get("error") if not exec_result["success"] else None,
                    "row_count": exec_result.get("row_count", 0),
                    "execution_time": exec_result.get("execution_time", 0),
                    "quality_score": 75 if exec_result["success"] else 0  # Default score for single queries
                }
            else:
                # Fallback to traditional SQL generation if contextual fails
                sql_result = await self.sql_generation_manager.generate_sql(question, None)
            
            if sql_result["success"]:
                exec_result = await self.execution_manager.execute_query(question, sql_result["sql"])
                
                return {
                    "question": question,
                    "priority": "medium",
                    "sql": sql_result["sql"],
                    "execution_success": exec_result["success"],
                    "results": exec_result["results"] if exec_result["success"] else [],
                    "error": exec_result.get("error") if not exec_result["success"] else None,
                    "row_count": exec_result.get("row_count", 0),
                    "execution_time": exec_result.get("execution_time", 0),
                        "quality_score": 75 if exec_result["success"] else 0
                }
            else:
                return {
                    "question": question,
                    "priority": "medium",
                    "sql": sql_result.get("sql", ""),
                    "execution_success": False,
                    "results": [],
                    "error": sql_result.get("error", "SQL generation failed"),
                    "row_count": 0,
                    "execution_time": 0,
                    "quality_score": 0
                }
        except Exception as e:
            return {
                "question": question,
                "priority": "medium",
                "sql": "",
                "execution_success": False,
                "results": [],
                "error": f"Single query execution error: {str(e)}",
                "row_count": 0,
                "execution_time": 0,
                "quality_score": 0
            }

    async def _execute_single_query_with_enhancement(self, question: str, enhanced_schema_context: str) -> Dict[str, Any]:
        """Execute a single optimized query for the question using enhanced contextual generation with column exploration"""
        try:
            # Check if required managers are available
            if not self.sql_generation_manager or not self.execution_manager:
                return {
                    "question": question,
                    "priority": "medium",
                    "sql": "",
                    "execution_success": False,
                    "results": [],
                    "error": "Required managers not available",
                    "row_count": 0,
                    "execution_time": 0,
                    "quality_score": 0
                }
            
            logger.info(f"🔍 Executing single query with enhanced context for: '{question}'")
            print(f"🔍 DEBUG: Using enhanced schema context with column exploration results")
            
            # Use enhanced contextual query generation with column exploration results
            contextual_queries = await self._generate_contextual_queries(question, enhanced_schema_context)
            
            if contextual_queries:
                # Use the first (best) contextual query
                best_query = contextual_queries[0]
                logger.info(f"🎯 Using enhanced contextual query: {best_query['description']}")
                print(f"🔍 DEBUG: Enhanced query SQL: {best_query['sql'][:100]}...")
                
                exec_result = await self.execution_manager.execute_query(best_query["description"], best_query["sql"])
                
                return {
                    "question": question,
                    "priority": "medium",
                    "sql": best_query["sql"],
                    "execution_success": exec_result["success"],
                    "results": exec_result["results"] if exec_result["success"] else [],
                    "error": exec_result.get("error") if not exec_result["success"] else None,
                    "row_count": exec_result.get("row_count", 0),
                    "execution_time": exec_result.get("execution_time", 0),
                    "quality_score": 80 if exec_result["success"] else 0,  # Higher score for enhanced queries
                    "enhanced_context_used": True
                }
            else:
                # Fallback to traditional SQL generation if contextual fails
                logger.warning("🔍 Enhanced contextual query generation failed, falling back to traditional approach")
                sql_result = await self.sql_generation_manager.generate_sql(question, None)
            
            if sql_result["success"]:
                exec_result = await self.execution_manager.execute_query(question, sql_result["sql"])
                
                return {
                    "question": question,
                    "priority": "medium",
                    "sql": sql_result["sql"],
                    "execution_success": exec_result["success"],
                    "results": exec_result["results"] if exec_result["success"] else [],
                    "error": exec_result.get("error") if not exec_result["success"] else None,
                    "row_count": exec_result.get("row_count", 0),
                    "execution_time": exec_result.get("execution_time", 0),
                    "quality_score": 75 if exec_result["success"] else 0,
                    "enhanced_context_used": False
                }
            else:
                return {
                    "question": question,
                    "priority": "medium",
                    "sql": sql_result.get("sql", ""),
                    "execution_success": False,
                    "results": [],
                    "error": sql_result.get("error", "SQL generation failed"),
                    "row_count": 0,
                    "execution_time": 0,
                    "quality_score": 0,
                    "enhanced_context_used": False
                }
        except Exception as e:
            logger.error(f"Enhanced single query execution error: {str(e)}")
            return {
                "question": question,
                "priority": "medium",
                "sql": "",
                "execution_success": False,
                "results": [],
                "error": f"Enhanced single query execution error: {str(e)}",
                "row_count": 0,
                "execution_time": 0,
                "quality_score": 0,
                "enhanced_context_used": False
            }

    @observe_function("comprehensive_analysis_generation")
    async def generate_comprehensive_analysis(self, user_query: str, analytical_results: List[Dict], schema_context: str = "") -> Dict[str, Any]:
        """Generate comprehensive analysis based on all analytical results"""
        logger.info(f"🔍 Starting comprehensive analysis generation for query: '{user_query}' with {len(analytical_results)} results")
        
        if not self.llm:
            error_msg = "LLM not set for comprehensive analysis generation"
            logger.error(f"❌ {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "analysis": f"I encountered an error: {error_msg}",
                "user_query": user_query,
                "results_processed": 0,
                "successful_results": 0,
                "total_rows": 0
            }
        
        try:
            # Get memory context
            memory_context = self.memory_manager.get_memory_context(user_query) if self.memory_manager.use_memory else ""
            logger.debug(f"Memory context retrieved: {len(memory_context) if memory_context else 0} characters")
            
            # Prepare results summary for the prompt
            results_summary = []
            successful_results = 0
            total_rows = 0
            
            for i, result in enumerate(analytical_results):
                if result["execution_success"]:
                    successful_results += 1
                    total_rows += result["row_count"]
                    
                    # Limit results to first 10 rows for context and ensure JSON serializable
                    limited_results = result["results"][:10] if result["results"] else []
                    
                    logger.debug(f"Processing result {i+1}: {result['row_count']} rows, question: {result['question'][:50]}...")
                    print(f"🔍 DEBUG: Processing analytical result {i+1}: {result['row_count']} rows")
                    
                    results_summary.append({
                        "question": result["question"],
                        "sql": result["sql"],
                        "results": limited_results,
                        "row_count": result["row_count"],
                        "execution_time": result["execution_time"]
                    })
                else:
                    logger.warning(f"Skipping failed result {i+1}: {result.get('error', 'Unknown error')}")
                    print(f"🔍 DEBUG: Skipping failed result {i+1}: {result.get('error', 'Unknown error')}")
                    
                    results_summary.append({
                        "question": result["question"],
                        "error": result["error"]
                    })
            
            logger.info(f"Results summary prepared: {successful_results} successful results, {total_rows} total rows")
            print(f"🔍 DEBUG: Results summary prepared: {successful_results} successful results, {total_rows} total rows")
            
            # Convert results to JSON string with custom encoder to handle Decimal objects
            try:
                analytical_results_json = json.dumps(results_summary, indent=2, cls=DecimalEncoder)
                logger.debug(f"Results successfully serialized to JSON: {len(analytical_results_json)} characters")
                print(f"🔍 DEBUG: Results successfully serialized to JSON: {len(analytical_results_json)} characters")
            except Exception as json_error:
                logger.error(f"Error serializing results to JSON: {json_error}")
                print(f"🔍 DEBUG: Error serializing results to JSON: {json_error}")
                
                # Fallback: convert to string representation
                analytical_results_json = str(results_summary)
                logger.warning("Using string representation as fallback for JSON serialization")
            
            # Prepare prompt values
            prompt_values = {
                "schema": schema_context,
                "user_query": user_query,
                "analytical_results": analytical_results_json,
                "total_questions": len(analytical_results),
                "successful_queries": successful_results,
                "memory": memory_context
            }
            
            logger.info(f"Calling LLM for comprehensive analysis generation")
            print(f"🔍 DEBUG: Calling LLM for comprehensive analysis generation")
            
            # Generate comprehensive analysis
            response = await self.llm.ainvoke(
                self.prompts_manager.comprehensive_analysis_prompt.format_messages(**prompt_values)
            )
            
            analysis = self._extract_response_content(response)
            logger.info(f"✅ Comprehensive analysis generated: {len(analysis)} characters")
            print(f"🔍 DEBUG: Comprehensive analysis generated: {len(analysis)} characters")
            
            return {
                "success": True,
                "analysis": analysis,
                "user_query": user_query,
                "results_processed": len(analytical_results),
                "successful_results": successful_results,
                "total_rows": total_rows
            }
            
        except Exception as e:
            logger.error(f"Error generating comprehensive analysis: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            print(f"🔍 DEBUG: Error generating comprehensive analysis: {str(e)}")
            print(f"🔍 DEBUG: Full traceback:\n{traceback.format_exc()}")
            
            return {
                "success": False,
                "error": f"Error generating comprehensive analysis: {str(e)}",
                "analysis": f"I encountered an error while analyzing the results: {str(e)}",
                "user_query": user_query,
                "results_processed": 0,
                "successful_results": 0,
                "total_rows": 0
            }
    
    def _extract_response_content(self, response) -> str:
        """Extract content from LLM response"""
        try:
            print(f"🔍 DEBUG: _extract_response_content called with response type: {type(response)}")
            
            if hasattr(response, 'content'):
                content = response.content.strip()
                print(f"🔍 DEBUG: Extracted content from .content attribute: '{content[:100]}...'")
                return content
            elif hasattr(response, 'text'):
                content = response.text.strip()
                print(f"🔍 DEBUG: Extracted content from .text attribute: '{content[:100]}...'")
                return content
            elif isinstance(response, str):
                content = response.strip()
                print(f"🔍 DEBUG: Response is string: '{content[:100]}...'")
                return content
            else:
                content = str(response).strip()
                print(f"🔍 DEBUG: Converted response to string: '{content[:100]}...'")
                return content
        except Exception as e:
            print(f"🔍 DEBUG: Error extracting response content: {e}")
            logger.error(f"Error extracting response content: {e}")
            return ""
    
    def _extract_json_from_response(self, response_text: str) -> str:
        """Extract JSON from response text, handling markdown code blocks"""
        try:
            print(f"🔍 DEBUG: _extract_json_from_response called with text length: {len(response_text)}")
            
            # Remove markdown code blocks
            import re
            
            # Pattern to match ```json ... ``` or ``` ... ```
            json_pattern = r'```(?:json)?\s*\n?(.*?)\n?```'
            match = re.search(json_pattern, response_text, re.DOTALL)
            
            if match:
                json_text = match.group(1).strip()
                print(f"🔍 DEBUG: Found JSON in code block: {len(json_text)} chars")
                return json_text
            
            # If no code block, try to find JSON-like content
            json_pattern = r'\{.*"questions".*\}'
            match = re.search(json_pattern, response_text, re.DOTALL)
            
            if match:
                json_text = match.group(0).strip()
                print(f"🔍 DEBUG: Found JSON-like content: {len(json_text)} chars")
                return json_text
            
            # If nothing found, return original text
            print(f"🔍 DEBUG: No JSON pattern found, returning original text")
            return response_text.strip()
            
        except Exception as e:
            print(f"🔍 DEBUG: Error in _extract_json_from_response: {e}")
            logger.error(f"Error extracting JSON from response: {e}")
            return response_text.strip()

    def _extract_questions_fallback(self, response_text: str, user_query: str) -> Dict[str, Any]:
        """Fallback method to extract questions from malformed responses"""
        try:
            print(f"🔍 DEBUG: _extract_questions_fallback called")
            
            # Try to find question-like patterns
            import re
            
            # Pattern to match numbered questions
            question_patterns = [
                r'(\d+\.?\s*["\']?([^"\']+)["\']?\s*[,\s]*["\']?(high|medium|low)["\']?)',
                r'question["\']?\s*:\s*["\']([^"\']+)["\']',
                r'([^.!?]*\?)',  # Find question marks
            ]
            
            questions = []
            for pattern in question_patterns:
                matches = re.findall(pattern, response_text, re.IGNORECASE)
                if matches:
                    print(f"🔍 DEBUG: Found {len(matches)} matches with pattern: {pattern}")
                    for match in matches:
                        if isinstance(match, tuple) and len(match) > 0:
                            question_text = match[1] if len(match) > 1 else match[0]
                            priority = match[2] if len(match) > 2 else "medium"
                        else:
                            question_text = str(match)
                            priority = "medium"
                        
                        if question_text and len(question_text.strip()) > 10:
                            questions.append({
                                "question": question_text.strip(),
                                "priority": priority
                            })
                    break
            
            # If no questions found, create default analytical questions
            if not questions:
                print(f"🔍 DEBUG: No questions found in fallback, creating default questions")
                questions = [
                    {"question": f"What is the overall {user_query}?", "priority": "high"},
                    {"question": f"What are the trends related to {user_query}?", "priority": "medium"},
                    {"question": f"What are the key insights about {user_query}?", "priority": "medium"}
                ]
            
            logger.info(f"Fallback extraction found {len(questions)} questions")
            print(f"🔍 DEBUG: Fallback extraction completed: {len(questions)} questions")
            
            return {
                "success": True,
                "questions": questions,
                "total_questions": len(questions),
                "user_query": user_query,
                "extraction_method": "fallback"
            }
            
        except Exception as e:
            print(f"🔍 DEBUG: Error in _extract_questions_fallback: {e}")
            logger.error(f"Error in fallback question extraction: {e}")
            
            # Last resort: create very basic questions
            default_questions = [
                {"question": f"Provide analysis for: {user_query}", "priority": "high"}
            ]
            
            return {
                "success": True,
                "questions": default_questions,
                "total_questions": 1,
                "user_query": user_query,
                "extraction_method": "default"
            } 