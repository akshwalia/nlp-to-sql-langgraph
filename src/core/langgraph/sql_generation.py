import re
from typing import Dict, Any, Tuple, Optional, List
from src.observability.langfuse_config import observe_function


class SQLGenerationManager:
    """Manages SQL generation and validation"""
    
    def __init__(self, prompts_manager, memory_manager, cache_manager, llm):
        self.prompts_manager = prompts_manager
        self.memory_manager = memory_manager
        self.cache_manager = cache_manager
        self.llm = llm
        self.schema_context = None
        self.example_patterns = None
    
    def prepare_schema_context(self, db_analyzer) -> None:
        """Prepare schema context for SQL generation"""
        try:
            # Get the database schema context (use get_rich_schema_context instead of get_schema_context)
            schema_context = db_analyzer.get_rich_schema_context()
            
            # The rich schema context is already formatted, so use it directly
            self.schema_context = schema_context
            
        except Exception as e:
            print(f"Error preparing schema context: {e}")
            self.schema_context = "Error loading schema information"
    
    def generate_example_patterns(self, db_analyzer) -> str:
        """Generate example SQL patterns based on the database schema"""
        try:
            # Since DatabaseAnalyzer doesn't have get_example_queries method,
            # we'll create basic example patterns based on the schema
            if not hasattr(db_analyzer, 'get_example_queries'):
                # Generate basic patterns based on available schema information
                basic_examples = [
                    {
                        'description': 'Select all records from a table',
                        'sql': 'SELECT * FROM table_name;'
                    },
                    {
                        'description': 'Select specific columns with conditions',
                        'sql': 'SELECT column1, column2 FROM table_name WHERE condition;'
                    },
                    {
                        'description': 'Join two tables',
                        'sql': 'SELECT t1.column1, t2.column2 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.table1_id;'
                    },
                    {
                        'description': 'Aggregate data with GROUP BY',
                        'sql': 'SELECT column1, COUNT(*) FROM table_name GROUP BY column1;'
                    },
                    {
                        'description': 'Filter with date conditions',
                        'sql': "SELECT * FROM table_name WHERE date_column >= '2023-01-01';"
                    }
                ]
                
                formatted_examples = []
                for example in basic_examples:
                    description = example.get('description', '')
                    sql = example.get('sql', '')
                    formatted_examples.append(f"-- {description}\n{sql}\n")
                
                return "\n".join(formatted_examples)
            else:
                # Use the method if it exists (for future compatibility)
                examples = db_analyzer.get_example_queries()
                
                formatted_examples = []
                for example in examples:
                    description = example.get('description', '')
                    sql = example.get('sql', '')
                    formatted_examples.append(f"-- {description}\n{sql}\n")
                
                return "\n".join(formatted_examples)
            
        except Exception as e:
            print(f"Error generating example patterns: {e}")
            return """-- Basic SQL Examples
-- Select all records from a table
SELECT * FROM table_name;

-- Select specific columns with conditions
SELECT column1, column2 FROM table_name WHERE condition;

-- Join two tables
SELECT t1.column1, t2.column2 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.table1_id;

-- Aggregate data with GROUP BY
SELECT column1, COUNT(*) FROM table_name GROUP BY column1;

-- Filter with date conditions
SELECT * FROM table_name WHERE date_column >= '2023-01-01';"""
    
    def validate_sql(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Validate SQL query for basic syntax and structure"""
        try:
            # Basic validation checks
            if not sql or not sql.strip():
                return False, "Empty SQL query"
            
            # Remove comments and extra whitespace
            sql_clean = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
            sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
            sql_clean = sql_clean.strip()
            
            if not sql_clean:
                return False, "SQL query contains only comments"
            
            # Check for basic SQL structure
            sql_upper = sql_clean.upper()
            valid_starts = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH']
            
            if not any(sql_upper.startswith(start) for start in valid_starts):
                return False, "SQL must start with a valid command (SELECT, INSERT, UPDATE, DELETE, WITH)"
            
            # Check for balanced parentheses
            if sql_clean.count('(') != sql_clean.count(')'):
                return False, "Unbalanced parentheses in SQL"
            
            # Check for balanced quotes
            single_quotes = sql_clean.count("'")
            if single_quotes % 2 != 0:
                return False, "Unbalanced single quotes in SQL"
            
            double_quotes = sql_clean.count('"')
            if double_quotes % 2 != 0:
                return False, "Unbalanced double quotes in SQL"
            
            # Check for multiple statements (should be single statement)
            statements = [stmt.strip() for stmt in sql_clean.split(';') if stmt.strip()]
            if len(statements) > 1:
                return False, "Multiple SQL statements not allowed"
            
            # Check for basic FROM clause in SELECT statements
            if sql_upper.startswith('SELECT'):
                if 'FROM' not in sql_upper:
                    return False, "SELECT statement must include FROM clause"
            
            # Check for basic VALUES clause in INSERT statements
            if sql_upper.startswith('INSERT'):
                if 'VALUES' not in sql_upper and 'SELECT' not in sql_upper:
                    return False, "INSERT statement must include VALUES clause or SELECT statement"
            
            # Check for WHERE clause in UPDATE/DELETE statements
            if sql_upper.startswith(('UPDATE', 'DELETE')):
                if 'WHERE' not in sql_upper:
                    return False, "UPDATE/DELETE statements should include WHERE clause for safety"
            
            return True, None
            
        except Exception as e:
            return False, f"SQL validation error: {str(e)}"
    
    @observe_function("sql_generation")
    async def generate_sql(self, question: str, db_analyzer) -> Dict[str, Any]:
        """Generate SQL query from natural language question"""
        try:
            # Check cache first
            cached_result = self.cache_manager.get_cached_result(question)
            if cached_result:
                return cached_result
            
            # Prepare context if not already prepared
            if not self.schema_context:
                self.prepare_schema_context(db_analyzer)
            if not self.example_patterns:
                self.example_patterns = self.generate_example_patterns(db_analyzer)
            
            # Get memory context
            memory_context = self.memory_manager.get_memory_context(question) if self.memory_manager.use_memory else ""
            
            # Prepare prompt values
            prompt_values = {
                "schema": self.schema_context,
                "question": question,
                "examples": self.example_patterns
            }
            
            if self.memory_manager.use_memory:
                prompt_values["memory"] = memory_context
            
            # Generate SQL
            response = await self.llm.ainvoke(
                self.prompts_manager.sql_prompt.format_messages(**prompt_values)
            )
            
            sql = self._extract_response_content(response)
            
            # Validate the generated SQL
            is_valid, error_msg = self.validate_sql(sql)
            
            result = {
                "success": is_valid,
                "sql": sql,
                "error": error_msg,
                "question": question,
                "schema_context": self.schema_context,
                "examples": self.example_patterns,
                "memory_context": memory_context
            }
            
            # Cache the result
            self.cache_manager.cache_result(question, result)
            
            return result
            
        except Exception as e:
            error_result = {
                "success": False,
                "sql": "",
                "error": f"Error generating SQL: {str(e)}",
                "question": question,
                "schema_context": self.schema_context or "",
                "examples": self.example_patterns or "",
                "memory_context": ""
            }
            
            return error_result
    
    @observe_function("sql_fix")
    def fix_sql(self, sql: str, error: str) -> Dict[str, Any]:
        """Fix SQL query based on error message"""
        try:
            # Prepare prompt values
            prompt_values = {
                "schema": self.schema_context or "",
                "sql": sql,
                "error": error
            }
            
            if self.memory_manager.use_memory:
                prompt_values["memory"] = self.memory_manager.get_memory_context(f"Fix SQL error: {error}")
            
            # Generate fixed SQL
            response = self.llm.invoke(
                self.prompts_manager.validation_prompt.format_messages(**prompt_values)
            )
            
            fixed_sql = self._extract_response_content(response)
            
            # Validate the fixed SQL
            is_valid, error_msg = self.validate_sql(fixed_sql)
            
            return {
                "success": is_valid,
                "sql": fixed_sql,
                "error": error_msg,
                "original_sql": sql,
                "original_error": error
            }
            
        except Exception as e:
            return {
                "success": False,
                "sql": sql,
                "error": f"Error fixing SQL: {str(e)}",
                "original_sql": sql,
                "original_error": error
            }
    
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
            print(f"Error extracting response content: {e}")
            return ""
    
    def analyze_question(self, question: str) -> Dict[str, Any]:
        """Analyze the question to understand its characteristics"""
        try:
            analysis = {
                "question": question,
                "question_type": self._determine_question_type(question),
                "complexity": self._assess_complexity(question),
                "entities": self._extract_entities(question),
                "intent": self._determine_intent(question),
                "requires_aggregation": self._requires_aggregation(question),
                "requires_joins": self._requires_joins(question),
                "time_based": self._is_time_based(question)
            }
            
            return analysis
            
        except Exception as e:
            print(f"Error analyzing question: {e}")
            return {
                "question": question,
                "question_type": "unknown",
                "complexity": "simple",
                "entities": [],
                "intent": "unknown",
                "requires_aggregation": False,
                "requires_joins": False,
                "time_based": False
            }
    
    def _determine_question_type(self, question: str) -> str:
        """Determine the type of question"""
        question_lower = question.lower()
        
        if any(word in question_lower for word in ['show', 'list', 'get', 'find', 'display']):
            return "retrieval"
        elif any(word in question_lower for word in ['count', 'how many', 'number of']):
            return "count"
        elif any(word in question_lower for word in ['sum', 'total', 'average', 'mean', 'max', 'min']):
            return "aggregation"
        elif any(word in question_lower for word in ['compare', 'versus', 'difference']):
            return "comparison"
        elif any(word in question_lower for word in ['trend', 'over time', 'change']):
            return "trend"
        elif any(word in question_lower for word in ['top', 'bottom', 'highest', 'lowest']):
            return "ranking"
        else:
            return "general"
    
    def _assess_complexity(self, question: str) -> str:
        """Assess the complexity of the question"""
        question_lower = question.lower()
        
        # Simple indicators
        simple_indicators = ['show', 'list', 'get', 'find', 'what is', 'who is']
        
        # Medium indicators
        medium_indicators = ['count', 'sum', 'average', 'group by', 'order by', 'filter']
        
        # Complex indicators
        complex_indicators = ['compare', 'analyze', 'trend', 'correlation', 'multiple', 'complex']
        
        # Check for complex indicators first
        if any(indicator in question_lower for indicator in complex_indicators):
            return "complex"
        elif any(indicator in question_lower for indicator in medium_indicators):
            return "medium"
        else:
            return "simple"
    
    def _extract_entities(self, question: str) -> List[Dict[str, str]]:
        """Extract entities from the question"""
        entities = []
        
        # Extract quoted strings
        quoted_strings = re.findall(r'"([^"]+)"', question)
        quoted_strings.extend(re.findall(r"'([^']+)'", question))
        
        for entity in quoted_strings:
            entities.append({
                "type": "quoted_string",
                "value": entity
            })
        
        # Extract numbers
        numbers = re.findall(r'\b\d+(?:\.\d+)?\b', question)
        for number in numbers:
            entities.append({
                "type": "number",
                "value": number
            })
        
        # Extract dates
        dates = re.findall(r'\b\d{4}-\d{2}-\d{2}\b', question)
        dates.extend(re.findall(r'\b\d{1,2}\/\d{1,2}\/\d{2,4}\b', question))
        
        for date in dates:
            entities.append({
                "type": "date",
                "value": date
            })
        
        return entities
    
    def _determine_intent(self, question: str) -> str:
        """Determine the intent of the question"""
        question_lower = question.lower()
        
        if any(word in question_lower for word in ['show', 'get', 'find', 'list']):
            return "retrieve"
        elif any(word in question_lower for word in ['count', 'how many']):
            return "count"
        elif any(word in question_lower for word in ['sum', 'total', 'calculate']):
            return "calculate"
        elif any(word in question_lower for word in ['compare', 'versus']):
            return "compare"
        elif any(word in question_lower for word in ['analyze', 'analysis']):
            return "analyze"
        else:
            return "general"
    
    def _requires_aggregation(self, question: str) -> bool:
        """Check if question requires aggregation functions"""
        question_lower = question.lower()
        aggregation_keywords = ['sum', 'count', 'average', 'mean', 'max', 'min', 'total', 'how many']
        
        return any(keyword in question_lower for keyword in aggregation_keywords)
    
    def _requires_joins(self, question: str) -> bool:
        """Check if question likely requires joins"""
        question_lower = question.lower()
        
        # Look for multiple entity types that might require joins
        entity_indicators = ['customer', 'order', 'product', 'employee', 'department', 'category']
        found_entities = [entity for entity in entity_indicators if entity in question_lower]
        
        return len(found_entities) > 1
    
    def _is_time_based(self, question: str) -> bool:
        """Check if question is time-based"""
        question_lower = question.lower()
        time_keywords = ['date', 'time', 'year', 'month', 'day', 'week', 'today', 'yesterday', 'last', 'recent']
        
        return any(keyword in question_lower for keyword in time_keywords)
    
    def refresh_schema_context(self, db_analyzer) -> bool:
        """Refresh the schema context from database"""
        try:
            self.prepare_schema_context(db_analyzer)
            self.example_patterns = self.generate_example_patterns(db_analyzer)
            return True
        except Exception as e:
            print(f"Error refreshing schema context: {e}")
            return False 