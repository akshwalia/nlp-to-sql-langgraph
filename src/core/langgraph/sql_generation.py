import json
import logging
import re
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
        self.example_patterns = None
        self.enum_context = {}  # Store enum-like column information
        self.db_analyzer = None  # Will be set during initialization
        
        logger.info("SQLGenerationManager initialized")
    
    def set_db_analyzer(self, db_analyzer):
        """Set the database analyzer for column exploration"""
        self.db_analyzer = db_analyzer

    def _is_numeric_column(self, column_name: str) -> bool:
        """
        Check if a column is numeric by querying the database information schema
        
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
            schema_name = self.db_analyzer.analyzer.schema_name
            
            with engine.connect() as connection:
                from sqlalchemy import text
                
                # Query information schema to get column data type
                query = text(f"""
                    SELECT data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema_name}' 
                    AND table_name = '{table_name}' 
                    AND column_name = '{column_name}'
                """)
                
                result = connection.execute(query)
                row = result.fetchone()
                
                if row:
                    data_type = row[0].upper()
                    numeric_types = [
                        'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'SERIAL', 'BIGSERIAL',
                        'FLOAT', 'DOUBLE', 'REAL', 'NUMERIC', 'DECIMAL', 'MONEY',
                        'DOUBLE PRECISION', 'FLOAT8', 'FLOAT4', 'INT2', 'INT4', 'INT8'
                    ]
                    return any(numeric_type in data_type for numeric_type in numeric_types)
                
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
            schema_name = self.db_analyzer.analyzer.schema_name
            
            with engine.connect() as connection:
                from sqlalchemy import text
                
                # Get distinct values with count
                query = text(f"""
                    SELECT DISTINCT "{column_name}", COUNT(*) as frequency
                    FROM "{schema_name}"."{table_name}"
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
                
                # Get total distinct count
                total_distinct_query = text(f"""
                    SELECT COUNT(DISTINCT "{column_name}") as total_distinct
                    FROM "{schema_name}"."{table_name}"
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
    
    def explore_column_values(self, question: str, potential_columns: List[str]) -> Dict[str, Any]:
        """
        Explore distinct values from potential columns that might contain relevant data
        
        Args:
            question: The natural language question
            potential_columns: List of column names to explore
            
        Returns:
            Dictionary containing exploration results for each column
        """
        logger.info(f"Exploring column values for question: '{question}'")
        logger.info(f"Potential columns to explore: {potential_columns}")
        
        exploration_results = {}
        
        for column in potential_columns:
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
        
        return exploration_results

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

    def prepare_schema_context(self, db_analyzer) -> None:
        """Prepare schema context for SQL generation"""
        try:
            # Get the database schema context (use get_rich_schema_context instead of get_schema_context)
            schema_context = db_analyzer.get_rich_schema_context()
            
            # The rich schema context is already formatted, so use it directly
            self.schema_context = schema_context
            
            # Extract ENUM-like information for exploratory queries
            self._extract_enum_context(schema_context)
            
        except Exception as e:
            print(f"Error preparing schema context: {e}")
            self.schema_context = "Error loading schema information"
    
    def _extract_enum_context(self, schema_context: str) -> None:
        """Extract ENUM-like column information from schema context"""
        try:
            logger.info("Extracting ENUM context from schema")
            
            # Parse the schema context to extract enum-like columns and their values
            lines = schema_context.split('\n')
            current_column = None
            
            for line in lines:
                # Look for enum-like column definitions
                if "unique values" in line and ":" in line:
                    # Extract column name - pattern like "  - role_title: 25 unique values"
                    parts = line.split(':')
                    if len(parts) >= 2:
                        column_part = parts[0].strip()
                        # Remove leading "- " if present
                        if column_part.startswith('- '):
                            column_part = column_part[2:]
                        current_column = column_part
                        self.enum_context[current_column] = {"values": [], "variations": []}
                
                elif current_column and line.strip().startswith("Values:"):
                    # Extract values - pattern like "    Values: 'Consultant', 'Developer', 'Manager'"
                    values_part = line.split("Values:")[1].strip()
                    # Parse quoted values
                    values = []
                    for match in re.findall(r"'([^']*)'", values_part):
                        values.append(match)
                    
                    if values:
                        self.enum_context[current_column]["values"] = values
                        # Generate variations for each value
                        self.enum_context[current_column]["variations"] = self._generate_search_variations(values)
                        
                        logger.debug(f"Found enum column {current_column} with {len(values)} values: {values[:5]}...")
            
            logger.info(f"Extracted ENUM context for {len(self.enum_context)} columns")
            
        except Exception as e:
            logger.error(f"Error extracting ENUM context: {e}")
            self.enum_context = {}
    
    def _generate_search_variations(self, values: List[str]) -> Dict[str, List[str]]:
        """Generate search term variations for fuzzy matching"""
        variations = {}
        
        for value in values:
            value_lower = value.lower()
            
            # Generate variations for common search terms
            search_terms = []
            
            # Direct value
            search_terms.append(value_lower)
            
            # Common consultant variations
            if any(term in value_lower for term in ['consultant', 'consult']):
                search_terms.extend(['consultant', 'consulting', 'consult', 'advisory', 'advisor'])
            
            # Common developer variations  
            if any(term in value_lower for term in ['developer', 'develop', 'engineer', 'programmer']):
                search_terms.extend(['developer', 'engineer', 'programmer', 'dev', 'software'])
            
            # Common manager variations
            if any(term in value_lower for term in ['manager', 'lead', 'senior', 'principal']):
                search_terms.extend(['manager', 'lead', 'senior', 'principal', 'director'])
            
            # Common analyst variations
            if any(term in value_lower for term in ['analyst', 'analysis']):
                search_terms.extend(['analyst', 'analysis', 'data', 'business'])
            
            # Remove duplicates and store
            for term in set(search_terms):
                if term not in variations:
                    variations[term] = []
                variations[term].append(value)
        
        return variations

    @observe_function("exploratory_sql_generation")
    async def generate_exploratory_sql(self, question: str, search_terms: List[str]) -> List[Dict[str, Any]]:
        """Generate multiple SQL queries using ENUM context and fuzzy matching"""
        logger.info(f"Generating exploratory SQL for question: '{question}' with search terms: {search_terms}")
        
        queries = []
        
        try:
            # For each search term, look across all enum columns for potential matches
            for search_term in search_terms:
                term_lower = search_term.lower()
                
                # Find matching columns and values
                matching_columns = []
                
                for column_name, enum_info in self.enum_context.items():
                    column_matches = []
                    
                    # Check direct value matches
                    for value in enum_info["values"]:
                        if term_lower in value.lower():
                            column_matches.append(value)
                    
                    # Check variation matches
                    for variation_term, matching_values in enum_info["variations"].items():
                        if term_lower in variation_term or variation_term in term_lower:
                            column_matches.extend(matching_values)
                    
                    # Remove duplicates
                    column_matches = list(set(column_matches))
                    
                    if column_matches:
                        matching_columns.append({
                            "column": column_name,
                            "matches": column_matches
                        })
                
                # Generate queries for each matching column
                for match in matching_columns:
                    column_name = match["column"]
                    matching_values = match["matches"]
                    
                    # Generate different types of queries
                    base_queries = self._generate_base_queries(question, column_name, matching_values)
                    queries.extend(base_queries)
            
            # Remove duplicate queries
            unique_queries = []
            seen_sql = set()
            
            for query in queries:
                sql_normalized = re.sub(r'\s+', ' ', query["sql"].strip().lower())
                if sql_normalized not in seen_sql:
                    seen_sql.add(sql_normalized)
                    unique_queries.append(query)
            
            logger.info(f"Generated {len(unique_queries)} unique exploratory SQL queries")
            return unique_queries
            
        except Exception as e:
            logger.error(f"Error generating exploratory SQL: {e}")
            return []
    
    def _extract_numeric_columns(self) -> List[str]:
        """Extract numeric column names from schema context"""
        numeric_columns = []
        
        if not self.schema_context:
            return numeric_columns
        
        try:
            lines = self.schema_context.split('\n')
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
            
            logger.info(f"Extracted {len(numeric_columns)} numeric columns: {numeric_columns}")
            
        except Exception as e:
            logger.error(f"Error extracting numeric columns: {e}")
        
        return numeric_columns

    def _extract_all_columns(self) -> List[str]:
        """Extract all column names from schema context"""
        columns = []
        
        if not self.schema_context:
            return columns
        
        try:
            lines = self.schema_context.split('\n')
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
                    # Extract column name from lines like "  - column_name: TYPE (Nullable: True)"
                    parts = line.split(':')
                    if len(parts) >= 2:
                        column_name = parts[0].strip().replace('- ', '')
                        columns.append(column_name)
            
            logger.info(f"Extracted {len(columns)} total columns: {columns}")
            
        except Exception as e:
            logger.error(f"Error extracting columns: {e}")
        
        return columns

    def _generate_base_queries(self, question: str, column_name: str, matching_values: List[str]) -> List[Dict[str, Any]]:
        """Generate base SQL queries for a specific column and values"""
        queries = []
        
        # Determine intent from question
        question_lower = question.lower()
        
        # Get actual numeric columns from schema context
        numeric_columns = self._extract_numeric_columns()
        
        # For each matching value, generate appropriate queries
        for value in matching_values:
            
            # COUNT query
            if any(word in question_lower for word in ['how many', 'count', 'number of']):
                queries.append({
                    "type": "count",
                    "sql": f"SELECT COUNT(*) FROM public.\"IT_Professional_Services\" WHERE {column_name} = '{value}';",
                    "description": f"Count of records where {column_name} = '{value}'"
                })
            
            # AVERAGE query - use actual numeric columns
            if any(word in question_lower for word in ['average', 'avg', 'mean']):
                for numeric_col in numeric_columns:
                    queries.append({
                        "type": "average",
                        "sql": f"SELECT AVG({numeric_col}) FROM public.\"IT_Professional_Services\" WHERE {column_name} = '{value}';",
                        "description": f"Average {numeric_col} for {column_name} = '{value}'"
                    })
            
            # MIN/MAX queries - use actual numeric columns
            if any(word in question_lower for word in ['minimum', 'min', 'lowest', 'maximum', 'max', 'highest', 'distribution', 'range']):
                for numeric_col in numeric_columns:
                    queries.append({
                        "type": "min_max",
                        "sql": f"SELECT MIN({numeric_col}) as min_{numeric_col}, MAX({numeric_col}) as max_{numeric_col} FROM public.\"IT_Professional_Services\" WHERE {column_name} = '{value}';",
                        "description": f"Min/Max {numeric_col} for {column_name} = '{value}'"
                    })
            
            # TOP/BOTTOM queries - use actual numeric columns
            if any(word in question_lower for word in ['top', 'highest', 'best', 'bottom', 'lowest', 'worst']):
                for numeric_col in numeric_columns:
                    order_direction = "DESC" if any(word in question_lower for word in ['top', 'highest', 'best']) else "ASC"
                    queries.append({
                        "type": "ranking",
                        "sql": f"SELECT * FROM public.\"IT_Professional_Services\" WHERE {column_name} = '{value}' ORDER BY {numeric_col} {order_direction} LIMIT 10;",
                        "description": f"{'Top' if order_direction == 'DESC' else 'Bottom'} 10 records by {numeric_col} for {column_name} = '{value}'"
                    })
        
        # Also generate fuzzy matching queries using LIKE
        if matching_values:
            # Generate LIKE queries for partial matches
            for value in matching_values[:3]:  # Limit to first 3 values to avoid too many queries
                
                if any(word in question_lower for word in ['average', 'avg', 'mean']):
                    for numeric_col in numeric_columns:
                        queries.append({
                            "type": "fuzzy_average",
                            "sql": f"SELECT AVG({numeric_col}) FROM public.\"IT_Professional_Services\" WHERE {column_name} ILIKE '%{value}%';",
                            "description": f"Average {numeric_col} for {column_name} containing '{value}'"
                        })
                
                if any(word in question_lower for word in ['how many', 'count', 'number of']):
                    queries.append({
                        "type": "fuzzy_count",
                        "sql": f"SELECT COUNT(*) FROM public.\"IT_Professional_Services\" WHERE {column_name} ILIKE '%{value}%';",
                        "description": f"Count of records where {column_name} contains '{value}'"
                    })
        
        return queries

    def _extract_search_terms(self, question: str) -> List[str]:
        """Extract potential search terms from the question"""
        question_lower = question.lower()
        
        search_terms = []
        
        # Common consultant-related terms
        if any(term in question_lower for term in ['consultant', 'consulting', 'consult']):
            search_terms.extend(['consultant', 'consulting', 'consult', 'advisory', 'advisor'])
        
        # Common developer-related terms
        if any(term in question_lower for term in ['developer', 'engineer', 'programmer', 'dev']):
            search_terms.extend(['developer', 'engineer', 'programmer', 'dev', 'software'])
        
        # Common manager-related terms
        if any(term in question_lower for term in ['manager', 'lead', 'senior', 'principal']):
            search_terms.extend(['manager', 'lead', 'senior', 'principal', 'director'])
        
        # Common analyst-related terms
        if any(term in question_lower for term in ['analyst', 'analysis', 'data']):
            search_terms.extend(['analyst', 'analysis', 'data', 'business'])
        
        # Geographic terms
        if any(term in question_lower for term in ['location', 'country', 'region', 'city']):
            search_terms.extend(['location', 'country', 'region', 'city', 'geographic'])
        
        # If no specific terms found, try to extract key nouns
        if not search_terms:
            # Extract potential key terms (simple approach)
            words = re.findall(r'\b[a-zA-Z]{3,}\b', question_lower)
            # Filter out common words
            stopwords = ['the', 'and', 'are', 'for', 'what', 'how', 'does', 'can', 'you', 'give', 'show', 'tell', 'average', 'rate', 'hourly']
            search_terms = [word for word in words if word not in stopwords]
        
        return list(set(search_terms))  # Remove duplicates

    @observe_function("sql_generation")
    async def generate_sql(self, question: str, db_analyzer) -> Dict[str, Any]:
        """Generate SQL query from natural language question using exploratory approach"""
        try:
            logger.info(f"Generating SQL for question: '{question}'")
            
            # Check cache first
            cached_result = self.cache_manager.get_cached_result(question)
            if cached_result:
                logger.info("Using cached result")
                return cached_result
            
            # Prepare context if not already prepared
            if not self.schema_context:
                self.prepare_schema_context(db_analyzer)
            if not self.example_patterns:
                self.example_patterns = self.generate_example_patterns(db_analyzer)
            
            # Extract search terms from the question
            search_terms = self._extract_search_terms(question)
            logger.info(f"Extracted search terms: {search_terms}")
            
            # Generate exploratory SQL queries
            exploratory_queries = await self.generate_exploratory_sql(question, search_terms)
            
            if exploratory_queries:
                # Use the first/best exploratory query
                best_query = exploratory_queries[0]
                logger.info(f"Selected exploratory query: {best_query['description']}")
                
                result = {
                    "success": True,
                    "sql": best_query["sql"],
                    "error": None,
                    "question": question,
                    "query_type": best_query["type"],
                    "description": best_query["description"],
                    "exploratory_queries": exploratory_queries,  # Include all generated queries
                    "search_terms": search_terms,
                    "schema_context": self.schema_context,
                    "examples": self.example_patterns
                }
                
                # Cache the result
                self.cache_manager.cache_result(question, result)
                return result
            
            else:
                # Fallback to traditional LLM-based approach
                logger.info("No exploratory queries generated, falling back to LLM approach")
                return await self._generate_sql_with_llm(question)
            
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            error_result = {
                "success": False,
                "sql": "",
                "error": f"Error generating SQL: {str(e)}",
                "question": question,
                "schema_context": self.schema_context or "",
                "examples": self.example_patterns or "",
                "search_terms": []
            }
            
            return error_result

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
                import json
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

    async def _generate_sql_with_llm(self, question: str) -> Dict[str, Any]:
        """Fallback method for SQL generation"""
        return {
            "success": False,
            "sql": "",
            "error": "SQL generation fallback failed",
            "question": question,
            "query_type": "fallback",
            "schema_context": self.schema_context or "",
            "examples": self.example_patterns or "",
            "memory": "",
            "source": "llm_fallback"
        }
    
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

    def generate_example_patterns(self, db_analyzer) -> str:
        """Generate example SQL patterns based on database schema"""
        try:
            # Get table information
            table_info = db_analyzer.get_table_info() if hasattr(db_analyzer, 'get_table_info') else {}
            
            # Generate example patterns based on schema
            examples = []
            
            # Basic SELECT examples
            examples.append("-- Basic query:\nSELECT * FROM public.\"IT_Professional_Services\" LIMIT 10;")
            
            # COUNT examples
            examples.append("-- Count records:\nSELECT COUNT(*) FROM public.\"IT_Professional_Services\";")
            
            # GROUP BY examples
            examples.append("-- Group by analysis:\nSELECT role_title_group, COUNT(*) FROM public.\"IT_Professional_Services\" GROUP BY role_title_group;")
            
            # WHERE with LIKE examples  
            examples.append("-- Filter with LIKE:\nSELECT * FROM public.\"IT_Professional_Services\" WHERE role_title_group ILIKE '%consultant%';")
            
            # Aggregation examples
            examples.append("-- Average calculation:\nSELECT AVG(hourly_rate_in_usd) FROM public.\"IT_Professional_Services\" WHERE hourly_rate_in_usd > 0;")
            
            # ORDER BY examples
            examples.append("-- Sort by value:\nSELECT * FROM public.\"IT_Professional_Services\" ORDER BY hourly_rate_in_usd DESC LIMIT 5;")
            
            return "\n\n".join(examples)
            
        except Exception as e:
            print(f"Error generating example patterns: {e}")
            # Return basic examples as fallback
            return """-- Basic query:
SELECT * FROM public."IT_Professional_Services" LIMIT 10;

-- Count records:
SELECT COUNT(*) FROM public."IT_Professional_Services";

-- Group by analysis:
SELECT role_title_group, COUNT(*) FROM public."IT_Professional_Services" GROUP BY role_title_group;""" 