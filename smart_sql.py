import os
import time
import hashlib
import json
import re
import uuid
import asyncio
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any, Union

import openai
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from typing_extensions import TypedDict
from langchain_core.messages import HumanMessage, AIMessage
from langchain_google_genai import GoogleGenerativeAIEmbeddings

from db_analyzer import DatabaseAnalyzer
from langfuse_config import (
    langfuse_manager, 
    create_langfuse_trace, 
    observe_function
)


class SQLGeneratorState(TypedDict):
    """State for the SQL generator graph"""
    question: str
    schema: str
    examples: str
    memory: str
    sql: str
    results: List[Dict]
    error: Optional[str]
    response: str
    validation_attempts: int


class SmartSQLGenerator:
    """
    AI-powered SQL query generator that works with any PostgreSQL database
    without relying on predefined templates
    """
    
    def __init__(
        self,
        db_analyzer: DatabaseAnalyzer,
        model_name: str = "gpt-4",
        use_cache: bool = True,
        cache_file: str = "query_cache.json",
        use_memory: bool = True,
        memory_persist_dir: str = "./memory_store"
    ):
        """
        Initialize the SQL generator
        
        Args:
            db_analyzer: Database analyzer instance
            model_name: Generative AI model to use
            use_cache: Whether to cache query results
            cache_file: Path to the query cache file
            use_memory: Whether to use conversation memory
            memory_persist_dir: Directory to persist memory embeddings
        """
        load_dotenv()
        self.db_analyzer = db_analyzer
        self.model_name = model_name
        self.use_cache = use_cache
        self.cache_file = cache_file
        self.cache = self._load_cache() if use_cache else {}
        self.use_memory = use_memory
        
        # Data store for paginated results
        self.paginated_results = {}
        
        # Session-specific memory for tracking conversation context
        self.session_context = {
            "user_info": {},
            "query_sequence": [],
            "important_values": {},
            "last_query_result": None,
            "entity_mentions": {},
            "text_responses": [],  # Store text responses
            "multi_query_results": []  # Store results from multiple queries
        }
        
        # Initialize Azure OpenAI
        self.azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
        self.deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", model_name)
        
        if not self.azure_endpoint or not self.api_key:
            raise ValueError("AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY environment variables must be set")
        
        # Configure Azure OpenAI
        openai.api_type = "azure"
        openai.api_base = self.azure_endpoint
        openai.api_version = self.api_version
        openai.api_key = self.api_key
        
        # Initialize LangGraph with Azure OpenAI
        # Note: Langfuse 3.x doesn't support LangChain callbacks
        # Use @observe decorators on methods instead
        self.llm = AzureChatOpenAI(
            azure_endpoint=self.azure_endpoint,
            azure_deployment=self.deployment_name,
            api_version=self.api_version,
            api_key=self.api_key,
            temperature=0
        )
        
        # Initialize memory system if enabled
        self.memory = None
        if use_memory:
            self.memory = self._initialize_memory(memory_persist_dir)
            
        # Define SQL generation prompt with memory context
        memory_var = "{memory}\n\n" if use_memory else ""
        self.sql_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert SQL developer specializing in PostgreSQL databases. Your job is to translate natural language questions into precise and efficient SQL queries.

{memory_var}### DATABASE SCHEMA:
{{schema}}

### EXAMPLES OF GOOD SQL PATTERNS:
{{examples}}

### GUIDELINES:
1. Create only PostgreSQL-compatible SQL
2. Focus on writing efficient queries
3. Use proper table aliases for clarity
4. Include appropriate JOINs based on database relationships
5. Include comments explaining complex parts of your query
6. **IMPORTANT - QUOTING RULES**: 
   - NEVER quote table names or schema names (e.g., use `production.product` NOT `"production.product"`)
   - ONLY quote column names that contain spaces, special characters, or reserved words
   - Standard table/schema names should be unquoted to avoid case-sensitivity issues
7. NEVER use any placeholder values in your final query
8. Use any available user information (name, role, IDs) from memory to personalize the query if applicable
9. Use specific values from previous query results when referenced (e.g., "this product", "these customers", "that date")
10. For follow-up questions or refinements, maintain the filters and conditions from the previous query
11. If the follow-up question is only changing which columns to display, KEEP ALL WHERE CONDITIONS from the previous query
12. When user asks for "this" or refers to previous results implicitly, use the context from the previous query
13. When user refers to "those" or "these" results with terms like "highest" or "lowest", ONLY consider the exact rows from the previous result set, NOT the entire table
14. If IDs from previous results are provided in the memory context, use them in a WHERE clause to limit exactly to those rows
15. Only those tables must be joined that have a foreign key relationship with the table being queried
16. IMPORTANT: When the user asks for "all" or "list all" data, DO NOT use aggregation functions (SUM, COUNT, AVG) unless explicitly requested. Return the raw data rows.
17. When the user asks to "show" or "list" data without explicitly asking for aggregation, return the individual rows rather than summary statistics.

### OUTPUT FORMAT:
Provide ONLY the SQL query with no additional text, explanation, or markdown formatting."""),
            ("human", "Convert the following question into a single PostgreSQL SQL query:\n{question}")
        ])
        
        # Define validation prompt
        self.validation_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert SQL developer specializing in PostgreSQL databases. Your job is to fix SQL query errors.

{memory_var}### DATABASE SCHEMA:
{{schema}}

### GUIDELINES:
1. Create only PostgreSQL-compatible SQL
2. Maintain the original query intent
3. Fix any syntax errors, typos, or invalid column references
4. **IMPORTANT - QUOTING RULES**: 
   - NEVER quote table names or schema names (e.g., use `production.product` NOT `"production.product"`)
   - ONLY quote column names that contain spaces, special characters, or reserved words
   - Standard table/schema names should be unquoted to avoid case-sensitivity issues
5. NEVER use any placeholder values in your final query
6. Use any available user information (name, role, IDs) from memory to personalize the query if applicable

### OUTPUT FORMAT:
Provide ONLY the corrected SQL query with no additional text, explanation, or markdown formatting."""),
            ("human", "Fix the following SQL query:\n```sql\n{sql}\n```\n\nError message: {error}")
        ])
        
        # Initialize LangGraph
        self.checkpointer = MemorySaver()
        self.graph = self._create_graph()

        # Define text response prompt
        self.text_response_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are a helpful database assistant who helps answer questions about data.

{memory_var}### DATABASE SCHEMA:
{{schema}}

### TASK:
Based on the question and the SQL query results, provide a natural language response.

### GUIDELINES:
1. If the user asked for specific data analysis, provide insights and analysis based on the results
2. If the user asked a question that doesn't require SQL, answer it directly based on your knowledge
3. Keep your response concise and focused on answering the question
4. Include relevant numbers and metrics from the results if available
5. Format numeric values appropriately (e.g., large numbers with commas, percentages, currencies)
6. For small result sets, you can mention specific data points
7. For large result sets, summarize the overall trends or patterns
8. If the results are empty, explain what that means in context
9. Use the schema information to provide more context when needed
10. If there was an error in the SQL query, explain what might have gone wrong
11. For questions about the system itself:
   - If asked about what LLM you're using, say you're using Google's Gemini model
   - If asked about the system architecture, explain it's a natural language to SQL system using LLMs
   - If asked about capabilities, explain you can translate natural language to SQL and analyze data
   - Be honest and straightforward about your capabilities and limitations

### OUTPUT FORMAT:
Provide a natural language response that directly answers the user's question. Be helpful, clear, and concise."""),
            ("human", "Question: {question}\n\nSQL Query:\n```sql\n{sql}\n```\n\nResults:\n{results}\n\nProvide a helpful response based on the data.")
        ])

        # Define conversation prompt for non-SQL conversations
        self.conversation_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are a helpful database assistant who helps answer questions about data and databases.

{memory_var}### DATABASE SCHEMA:
{{schema}}

### TASK:
Respond to the user's question. This appears to be a general question that doesn't require generating SQL.

### GUIDELINES:
1. If the question is about databases or data concepts, provide a helpful explanation
2. If the question is about the schema or structure of the database, refer to the schema information
3. If the question is unrelated to databases, provide a general helpful response
4. Be concise and direct in your response
5. Use your knowledge about databases and SQL when relevant
6. If the question might benefit from executing SQL but is currently phrased as a conversation, suggest what specific data the user might want to query
7. For questions about the system itself:
   - If asked about what LLM you're using, say you're using Google's Gemini model
   - If asked about the system architecture, explain it's a natural language to SQL system using LLMs
   - If asked about capabilities, explain you can translate natural language to SQL and analyze data
   - Be honest and straightforward about your capabilities and limitations

### OUTPUT FORMAT:
Provide a natural language response that directly answers the user's question. Be helpful, clear, and concise."""),
            ("human", "{question}")
        ])

        # Define multi-query analysis prompt
        self.analysis_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert data analyst who helps analyze database query results.

{memory_var}### DATABASE SCHEMA:
{{schema}}

### TASK:
Analyze the query results to answer the user's question. The user has requested a complex analysis that required multiple queries.

### GUIDELINES:
1. Compare and analyze data across the multiple tables provided
2. Identify trends, anomalies, or patterns in the data
3. Provide insights that directly answer the user's question
4. Reference specific numbers from the results to support your analysis
5. Use proper statistical reasoning when making comparisons
6. For time-based comparisons (such as year-over-year), calculate and explain percentage changes
7. Be concise yet thorough in your explanation
8. Format numbers appropriately (with commas for thousands, percentages with % sign)
9. If appropriate, suggest potential reasons for patterns observed in the data
10. When analyzing financial data, consider both absolute and relative changes
11. For comparisons between periods, highlight significant changes and potential causes

### OUTPUT FORMAT:
Provide a thorough analysis of the data that directly answers the user's question. Be insightful, clear, and data-driven."""),
            ("human", "Question: {question}\n\nQuery Results:\n{tables_info}\n\nProvide analysis based on the data.")
        ])

        # Define query planning prompt for complex questions
        self.query_planner_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert SQL database analyst with deep knowledge of PostgreSQL databases. Your job is to plan the SQL queries needed to answer complex questions.

{memory_var}### DATABASE SCHEMA:
{{schema}}

### TASK:
Identify if the user's question requires a single SQL query or multiple SQL queries for proper analysis. 
For simple data retrieval, a single query is sufficient. For questions involving comparison, trend analysis, or "explain why" type questions, multiple queries are typically needed.

### GUIDELINES:
1. Analyze if the question requires multiple SQL queries or just a single query
2. For comparison questions (e.g., "compare to last year", "why are sales down"), plan multiple queries
3. Identify specific time periods, metrics, or entities that need to be queried separately

### OUTPUT FORMAT:
Answer in key-value format, following this exact format:

NEEDS_MULTIPLE_QUERIES: yes/no
NUMBER_OF_QUERIES_NEEDED: [number]
QUERY_PLAN:
- Query 1: [description of first query]
- Query 2: [description of second query]
...
ANALYSIS_APPROACH: [brief description of how these queries will help answer the question]"""),
            ("human", "{question}")
        ])
        
        # Prepare schema context
        self.schema_context = None
        self.example_patterns = None
        
        # Define edit mode prompts
        self._initialize_edit_mode_prompts()
    
    def _create_graph(self) -> StateGraph:
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
            
            memory_context = self._get_memory_context(state["question"]) if self.use_memory else ""
            
            # Generate SQL using the prompt
            prompt_values = {
                "schema": self.schema_context,
                "question": state["question"],
                "examples": self.example_patterns
            }
            if self.use_memory:
                prompt_values["memory"] = memory_context
            
            response = await self.llm.ainvoke(
                self.sql_prompt.format_messages(**prompt_values),
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
                if self.use_memory:
                    prompt_values["memory"] = state["memory"]
                
                response = await self.llm.ainvoke(
                    self.validation_prompt.format_messages(**prompt_values),
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
                if self.use_memory:
                    prompt_values["memory"] = state["memory"]
                
                response = await self.llm.ainvoke(
                    self.text_response_prompt.format_messages(**prompt_values),
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
    
    def _initialize_edit_mode_prompts(self):
        """Initialize prompts for edit mode operations"""
        memory_var = "{memory}\n\n" if self.use_memory else ""
        
        # Edit mode SQL generation prompt - more cautious and explicit about modifications
        self.edit_sql_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert SQL developer specializing in PostgreSQL databases with EDIT MODE ENABLED. Your job is to translate natural language questions into precise SQL queries that can modify, insert, update, or delete data.

{memory_var}### DATABASE SCHEMA:
{{schema}}

### EXAMPLES OF GOOD SQL PATTERNS:
{{examples}}

### GUIDELINES FOR EDIT MODE:
1. **WRITE OPERATIONS ALLOWED**: You can generate INSERT, UPDATE, DELETE, and SELECT queries
2. **BE CAUTIOUS**: For destructive operations (UPDATE/DELETE), always include appropriate WHERE clauses to limit scope
3. **EXPLICIT CONDITIONS**: Never generate UPDATE or DELETE without specific WHERE conditions unless explicitly requested for all records
4. **DATA VALIDATION**: Consider data integrity and foreign key constraints when generating modification queries
5. **TRANSACTION SAFETY**: Design queries that are safe and won't cause unintended data loss
6. **SPECIFIC ACTIONS**: If the user asks to "add", "insert", "create" → use INSERT; "update", "modify", "change" → use UPDATE; "delete", "remove" → use DELETE
7. **REQUIRE SPECIFICITY**: For UPDATE/DELETE operations, require specific identifiers (IDs, names, etc.) in the question
8. **BATCH OPERATIONS**: For bulk operations, be explicit about what records will be affected
9. **POSTGRESQL SYNTAX**: Use proper PostgreSQL syntax including RETURNING clauses when appropriate
10. **SAFETY FIRST**: If the request is ambiguous about which records to modify, ask for clarification rather than making assumptions
11. **MULTI QUERY**: If you generate multiple queries to meet the goal, each query must be separated by "<----->"
12. **EXAMPLES**: Use the examples provided to guide your SQL generation.
13. No need to give enclosing ```sql tags for the queries.

### EXAMPLES:
 "INSERT INTO person.businessentity (rowguid, modifieddate)\nVALUES (gen_random_uuid(), NOW())\nRETURNING businessentityid;\n<----->\n\nINSERT INTO person.person (businessentityid, persontype, namestyle, firstname, lastname, emailpromotion, rowguid, modifieddate)\nVALUES (\n  (SELECT businessentityid FROM person.businessentity ORDER BY businessentityid DESC LIMIT 1),\n  'EM',\n  0,\n  'Farhan',\n  'Akhtar',\n  0,\n  gen_random_uuid(),\n  NOW()\n)\nRETURNING businessentityid;\n\n<----->\n\nINSERT INTO sales.customer (customerid, personid, territoryid, rowguid, modifieddate)\nVALUES (\n  (SELECT COALESCE(MAX(customerid), 0) + 1 FROM sales.customer),\n  (SELECT businessentityid FROM person.person ORDER BY businessentityid DESC LIMIT 1),\n  (SELECT territoryid FROM sales.salesterritory WHERE name = 'Northwest'),\n  gen_random_uuid(),\n  NOW()\n)\nRETURNING customerid;\nn<----->\n\nINSERT INTO sales.salesorderheader (salesorderid, revisionnumber, orderdate, duedate, status, onlineorderflag, customerid, billtoaddressid, shiptoaddressid, shipmethodid, subtotal, taxamt, freight, rowguid, modifieddate)\nVALUES (\n  (SELECT COALESCE(MAX(salesorderid), 43658) + 1 FROM sales.salesorderheader),\n  1,\n  NOW(),\n  NOW() + INTERVAL '7 days',\n  5,\n  FALSE,\n  (SELECT customerid FROM sales.customer ORDER BY customerid DESC LIMIT 1),\n  (SELECT addressid FROM person.address LIMIT 1),\n  (SELECT addressid FROM person.address LIMIT 1),\n  (SELECT shipmethodid FROM purchasing.shipmethod LIMIT 1),\n  0.00,\n  0.00,\n  0.00,\n  gen_random_uuid(),\n  NOW()\n);"

### IMPORTANT SAFETY RULES:
- Never generate UPDATE or DELETE without WHERE clauses unless explicitly requested for all records
- Always validate that the requested operation makes sense given the schema
- For INSERT operations, ensure all required fields are provided or have defaults
- Use transactions implicitly by designing safe, atomic operations

### OUTPUT FORMAT:
Provide ONLY the SQL query with no additional text, explanation, or markdown formatting."""),
            ("human", "Convert the following question into a PostgreSQL SQL query. This is an EDIT MODE request, so you can generate INSERT, UPDATE, DELETE, or SELECT queries as appropriate:\n{question}")
        ])
        
        # Edit mode verification prompt - double-checks the generated SQL
        self.edit_verification_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are a database safety expert reviewing SQL queries for edit operations. Your job is to verify that the SQL query is safe, correct, and matches the user's intent.

### DATABASE SCHEMA:
{{schema}}

### VERIFICATION CHECKLIST:
Analyze the SQL query and provide a verification report covering these aspects:

1. **SAFETY CHECK**: 
   - Does the query have appropriate WHERE clauses for UPDATE/DELETE operations?
   - Will this query affect only the intended records?
   - Are there any risks of unintended data loss or corruption?

2. **CORRECTNESS CHECK**:
   - Does the SQL syntax appear correct for PostgreSQL?
   - Are all referenced tables and columns valid according to the schema?
   - Does the query logic match the user's request?

3. **COMPLETENESS CHECK**:
   - Does the query fully address the user's request?
   - Are all required fields included for INSERT operations?
   - Are data types and constraints respected?

4. **IMPACT ASSESSMENT**:
   - How many records will likely be affected?
   - What are the potential consequences of this operation?
   - Are there any dependencies or cascading effects to consider?

### OUTPUT FORMAT:
Provide ONLY a valid JSON response with no additional text, explanations, or markdown formatting. Use the following structure:
{{{{
    "is_safe": true,
    "is_correct": true,
    "safety_issues": [],
    "correctness_issues": [],
    "impact_assessment": "description of what this query will do",
    "estimated_affected_records": "estimate or 'unknown'",
    "recommendations": [],
    "overall_verdict": "SAFE_TO_EXECUTE",
    "explanation": "brief explanation of the verdict"
}}}}

IMPORTANT: Return ONLY the JSON object above with your actual values. Do not include any explanatory text, markdown formatting, or code blocks."""),
            ("human", "### ORIGINAL USER REQUEST:\n\"{original_question}\"\n\n### GENERATED SQL QUERY:\n```sql\n{sql}\n```\n\nPlease verify this SQL query for safety and correctness.")
        ])
        
        # Create edit mode chains
        self.edit_sql_chain = self.edit_sql_prompt | self.llm
        self.edit_verification_chain = self.edit_verification_prompt | self.llm
    
    def _initialize_memory(self, persist_dir: str) -> Optional[Chroma]:
        """Initialize vector store memory for LangGraph with Gemini embeddings"""
        try:
            # Ensure the directory exists
            os.makedirs(persist_dir, exist_ok=True)
            
            # Initialize Gemini embeddings
            gemini_api_key = os.getenv("GOOGLE_API_KEY")
            if not gemini_api_key:
                print("Warning: GOOGLE_API_KEY not found. Memory functionality will be disabled.")
                return None
            
            # Create Gemini embeddings
            embeddings = GoogleGenerativeAIEmbeddings(
                model="models/embedding-001",
                google_api_key=gemini_api_key
            )
            
            # Create or load the vector store with Gemini embeddings
            vectorstore = Chroma(
                persist_directory=persist_dir,
                collection_name="sql_conversation_memory",
                embedding_function=embeddings
            )
            
            return vectorstore
        except Exception as e:
            print(f"Error initializing memory: {e}")
            return None
    
    def _store_in_memory(self, question: str, sql: str, results: Any = None) -> None:
        """Store the question, generated SQL and results in memory"""
        if not self.memory or not self.use_memory:
            return
            
        try:
            # Create document with question and SQL
            content = f"Question: {question}\nSQL: {sql}"
            
            # Extract and store personal information
            personal_info = self._extract_personal_info(question, results)
            if personal_info:
                content = f"{personal_info}\n\n{content}"
            
            # Add result summary if available
            if results:
                try:
                    # Count rows or summarize results
                    if isinstance(results, list) and results:
                        num_rows = len(results)
                        sample = results[0] if results else {}
                        columns = list(sample.keys()) if isinstance(sample, dict) else []
                        result_summary = f"\nReturned {num_rows} rows with columns: {', '.join(columns)}"
                        
                        # Include sample results (first 3 rows at most)
                        if num_rows > 0:
                            result_summary += "\nSample results:"
                            for i, row in enumerate(results[:3]):
                                result_summary += f"\nRow {i+1}: {str(row)}"
                        
                        content += result_summary
                except Exception as e:
                    print(f"Error summarizing results: {e}")
                
            # Store in memory as a document
            doc = Document(page_content=content, metadata={"question": question})
            self.memory.add_documents([doc])
        except Exception as e:
            print(f"Error storing in memory: {e}")

    def _extract_personal_info(self, question: str, results: Any = None) -> str:
        """Extract personal information from user queries or results"""
        personal_info = []
        
        # Check for name information
        name_patterns = [
            r"my name is (?P<name>[\w\s]+)",
            r"I am (?P<name>[\w\s]+)",
            r"I'm (?P<name>[\w\s]+)",
            r"call me (?P<name>[\w\s]+)"
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                name = match.group("name").strip()
                personal_info.append(f"User name: {name}")
                break
        
        # Check for other personal identifiers in the question
        if "my" in question.lower():
            id_patterns = [
                r"my (?P<id_type>user|customer|employee|sales|account|order|client|supplier|vendor) (?P<id_value>\w+)",
                r"my (?P<id_type>user|customer|employee|sales|account|order|client|supplier|vendor) id is (?P<id_value>\w+)",
                r"my (?P<id_type>user|customer|employee|sales|account|order|client|supplier|vendor) number is (?P<id_value>\w+)",
            ]
            
            for pattern in id_patterns:
                match = re.search(pattern, question, re.IGNORECASE)
                if match:
                    id_type = match.group("id_type").strip()
                    id_value = match.group("id_value").strip()
                    personal_info.append(f"User {id_type} ID: {id_value}")
                    break
        
        # Check for personal context "I am a X"
        role_patterns = [
            r"I am a (?P<role>[\w\s]+)",
            r"I'm a (?P<role>[\w\s]+)",
            r"I work as a (?P<role>[\w\s]+)",
            r"my role is (?P<role>[\w\s]+)"
        ]
        
        for pattern in role_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                role = match.group("role").strip()
                personal_info.append(f"User role: {role}")
                break
        
        if personal_info:
            return "\n".join(personal_info)
        return ""

    def _extract_response_content(self, response) -> str:
        """Extract content from LangChain response in a consistent way"""
        try:
            if hasattr(response, 'content'):
                content = response.content
                if isinstance(content, str):
                    return content
                else:
                    return str(content)
            elif isinstance(response, dict) and "text" in response:
                return str(response["text"])
            elif isinstance(response, str):
                return response
            else:
                return str(response)
        except Exception as e:
            # If extraction fails, return a safe fallback
            print(f"Warning: Failed to extract response content: {e}")
            return "Response generated but could not be extracted properly."
    
    def _get_memory_context(self, question: str) -> str:
        """Retrieve relevant context from memory for a question"""
        if not self.memory or not self.use_memory:
            return ""
            
        try:
            # Get relevant memories using similarity search
            docs = self.memory.similarity_search(question, k=5)
            memory_content = "\n".join([doc.page_content for doc in docs])
            return memory_content
        except Exception as e:
            print(f"Error retrieving from memory: {e}")
            return ""
    
    def _load_cache(self) -> Dict:
        """Load query cache from disk"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}
        return {}
    
    def _save_cache(self) -> None:
        """Save query cache to disk"""
        if self.use_cache:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f)
    
    def _get_question_hash(self, question: str) -> str:
        """Generate a hash for a question to use as cache key"""
        # Normalize the question: lowercase, remove punctuation, extra spaces
        normalized = re.sub(r'[^\w\s]', '', question.lower())
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def _prepare_schema_context(self) -> None:
        """Prepare the database schema context for the AI model"""
        if not self.schema_context:
            # Get rich schema information for context
            self.schema_context = self.db_analyzer.get_rich_schema_context()
            
            # Generate example query patterns based on the schema
            self.example_patterns = self._generate_example_patterns()
    
    def _generate_example_patterns(self) -> str:
        """
        Generate example SQL patterns based on the analyzed schema
        
        Returns:
            String with example SQL patterns
        """
        if not self.db_analyzer.schema_info:
            self.db_analyzer.analyze_schema()
        
        schema = self.db_analyzer.schema_info
        examples = []
        
        # Get table names for examples
        table_names = list(schema["tables"].keys())
        if not table_names:
            return "No tables found in the database."
        
        # Find tables with time-related columns for time series examples
        time_tables = []
        for table_name, table_info in schema["tables"].items():
            for col in table_info["columns"]:
                if "date" in col["type"].lower() or "time" in col["type"].lower():
                    time_tables.append((table_name, col["name"]))
                    break
        
        # Example 1: Basic query with filtering
        if table_names:
            table = schema["tables"][table_names[0]]
            if table["columns"]:
                col1 = table["columns"][0]["name"]
                filter_col = None
                
                # Find a good column to filter on
                for col in table["columns"]:
                    if any(t in col["type"].lower() for t in ["varchar", "text", "char"]):
                        filter_col = col["name"]
                        break
                
                if not filter_col and len(table["columns"]) > 1:
                    filter_col = table["columns"][1]["name"]
                
                if filter_col:
                    examples.append(f"""Example 1: Simple filtering
Query: "Show me all {table_names[0]} where {filter_col} contains 'example'"
SQL:
```sql
SELECT * 
FROM "{table_names[0]}"
WHERE "{filter_col}" LIKE '%example%'
LIMIT 10;
```
""")
        
        # Example 2: Aggregation
        if table_names:
            table = schema["tables"][table_names[0]]
            numeric_col = None
            group_col = None
            
            # Find numeric and categorical columns
            for col in table["columns"]:
                if any(t in col["type"].lower() for t in ["int", "float", "numeric", "decimal"]):
                    numeric_col = col["name"]
                elif any(t in col["type"].lower() for t in ["varchar", "text", "char"]) and not group_col:
                    group_col = col["name"]
            
            if numeric_col and group_col:
                examples.append(f"""Example 2: Aggregation with grouping
Query: "Calculate the total {numeric_col} grouped by {group_col}"
SQL:
```sql
SELECT "{group_col}", SUM("{numeric_col}") as total_{numeric_col}
FROM "{table_names[0]}"
GROUP BY "{group_col}"
ORDER BY total_{numeric_col} DESC
LIMIT 10;
```
""")
        
        # Example 3: Joins between related tables
        if len(schema["relationships"]) > 0:
            rel = schema["relationships"][0]
            src_table = rel["source_table"]
            tgt_table = rel["target_table"]
            src_col = rel["source_columns"][0]
            tgt_col = rel["target_columns"][0]
            
            # Find a column from each table to display
            src_display_col = next((col["name"] for col in schema["tables"][src_table]["columns"] 
                                  if col["name"] != src_col and not col["name"].endswith("_id")), src_col)
            tgt_display_col = next((col["name"] for col in schema["tables"][tgt_table]["columns"] 
                                  if col["name"] != tgt_col and not col["name"].endswith("_id")), tgt_col)
            
            examples.append(f"""Example 3: Joining related tables
Query: "Show {src_table} with their related {tgt_table}"
SQL:
```sql
SELECT s."{src_display_col}", t."{tgt_display_col}"
FROM "{src_table}" s
JOIN "{tgt_table}" t ON s."{src_col}" = t."{tgt_col}"
LIMIT 10;
```
""")
        
        # Example 4: Time series analysis
        if time_tables:
            table_name, date_col = time_tables[0]
            
            # Find a numeric column to aggregate
            numeric_col = None
            for col in schema["tables"][table_name]["columns"]:
                if any(t in col["type"].lower() for t in ["int", "float", "numeric", "decimal"]):
                    numeric_col = col["name"]
                    break
            
            if numeric_col:
                examples.append(f"""Example 4: Time series analysis
Query: "Show monthly totals of {numeric_col} in {table_name}"
SQL:
```sql
SELECT 
    DATE_TRUNC('month', "{date_col}") AS month,
    SUM("{numeric_col}") AS total_{numeric_col}
FROM "{table_name}"
GROUP BY DATE_TRUNC('month', "{date_col}")
ORDER BY month DESC;
```
""")
        
        # Example 5: Subqueries and complex filtering
        if table_names and len(table_names) > 1:
            table1 = table_names[0]
            table2 = table_names[1]
            
            # Find primary keys
            pk1 = next((col["name"] for col in schema["tables"][table1]["columns"] if col.get("primary_key")), None)
            
            if pk1:
                examples.append(f"""Example 5: Subqueries
Query: "Find {table1} that have more than 5 associated {table2}"
SQL:
```sql
SELECT t1.*
FROM "{table1}" t1
WHERE (
    SELECT COUNT(*)
    FROM "{table2}" t2
    WHERE t2."{table1}_{pk1}" = t1."{pk1}"
) > 5;
```
""")
        
        return "\n".join(examples)
    
    def _validate_sql(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Validate the SQL query for basic safety and syntax
        
        Args:
            sql: SQL query to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for dangerous operations
        dangerous_patterns = [
            r'\bDROP\b\s+(?:\bDATABASE\b|\bSCHEMA\b)',
            r'\bTRUNCATE\b',
            r'\bDELETE\b\s+FROM\b\s+\w+\s*(?!\bWHERE\b)',  # DELETE without WHERE
            r'\bUPDATE\b\s+\w+\s+SET\b\s+(?:\w+\s*=\s*\w+)(?:\s*,\s*\w+\s*=\s*\w+)*\s*(?!\bWHERE\b)',  # UPDATE without WHERE
            r'\bCREATE\b\s+(?:\bDATABASE\b|\bSCHEMA\b)',
            r'\bDROP\b\s+\bTABLE\b',
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                return False, f"Query contains potentially dangerous operation: {pattern}"
        
        # Basic syntax check
        if not re.search(r'\bSELECT\b', sql, re.IGNORECASE):
            return False, "Only SELECT queries are allowed"
        
        # Check for unresolved placeholders
        if re.search(r'\{.*?\}', sql):
            return False, "Query contains unresolved placeholders"
        
        return True, None
    
    def _analyze_question(self, question: str) -> Dict[str, Any]:
        """
        Analyze the question to extract key information
        
        Args:
            question: Natural language question
            
        Returns:
            Dictionary with analysis results
        """
        # TODO: Implement more sophisticated question analysis
        # For now, just return basic info
        analysis = {
            "complexity": "simple" if len(question.split()) < 10 else "complex",
            "keywords": [],
        }
        
        # Extract key business intelligence terms
        bi_terms = [
            "total", "average", "count", "sum", "group by", "trend",
            "compare", "ratio", "percentage", "growth", "rank", 
            "top", "bottom", "highest", "lowest"
        ]
        
        for term in bi_terms:
            if term in question.lower():
                analysis["keywords"].append(term)
        
        return analysis
    
    @observe_function("sql_generation")
    async def generate_sql(self, question: str) -> Dict[str, Any]:
        """
        Generate a SQL query from a natural language question using LangGraph
        
        Args:
            question: Natural language question
            
        Returns:
            Dictionary with query generation details
        """
        start_time = time.time()
        
        # Check cache first if enabled
        if self.use_cache:
            question_hash = self._get_question_hash(question)
            if question_hash in self.cache:
                cached_result = self.cache[question_hash].copy()
                cached_result["source"] = f"{cached_result['source']} (cached)"
                cached_result["execution_time"] = 0
                return cached_result
        
        # Prepare initial state
        initial_state = {
            "question": question,
            "schema": "",
            "examples": "",
            "memory": "",
            "sql": "",
            "results": [],
            "error": None,
            "response": "",
            "validation_attempts": 0
        }
        
        try:
            # Run the LangGraph workflow
            config = {"configurable": {"thread_id": str(uuid.uuid4())}}
            final_state = await self.graph.ainvoke(initial_state, config)
            
            # Extract results
            success = final_state.get("error") is None
            sql = final_state.get("sql", "")
            error = final_state.get("error")
            
            result = {
                "success": success,
                "sql": sql,
                "source": "ai",
                "confidence": 90 if success else 50,
                "error": error,
                "execution_time": time.time() - start_time
            }
            
            # Cache the result if caching is enabled and successful
            if self.use_cache and success:
                self.cache[question_hash] = result.copy()
                self._save_cache()
            
            return result
            
        except Exception as e:
            return {
                "success": False,
                "sql": None,
                "source": "ai",
                "confidence": 0,
                "error": str(e),
                "execution_time": time.time() - start_time
            }
    
    @observe_function("sql_fix")
    def fix_sql(self, sql: str, error: str) -> Dict[str, Any]:
        """
        Try to fix a SQL query that generated an error
        
        Args:
            sql: Original SQL query
            error: Error message from database
            
        Returns:
            Dictionary with fixed query details
        """
        start_time = time.time()
        
        try:
            # Prepare schema context if not already prepared
            self._prepare_schema_context()
            
            # Prepare params for validation chain
            params = {
                "schema": self.schema_context,
                "sql": sql,
                "error": error
            }
            
            # Add memory if enabled
            if self.use_memory:
                memory_context = self._prepare_memory_for_query(f"Fix SQL error: {error}") or ""
                params["memory"] = memory_context
            
            # Use validation chain to fix the SQL
            response = self.validation_chain.invoke(params)
            
            fixed_sql = self._extract_response_content(response).strip()
            
            # Clean up response
            if fixed_sql.startswith('```') and fixed_sql.endswith('```'):
                fixed_sql = fixed_sql[3:-3].strip()
            if fixed_sql.startswith('sql') or fixed_sql.startswith('SQL'):
                fixed_sql = fixed_sql[3:].strip()
            
            # Validate the fixed SQL
            is_valid, validation_error = self._validate_sql(fixed_sql)
            if not is_valid:
                return {
                    "success": False,
                    "sql": sql,
                    "fixed_sql": fixed_sql,
                    "source": "ai",
                    "confidence": 40,
                    "error": validation_error,
                    "execution_time": time.time() - start_time
                }
            
            return {
                "success": True,
                "sql": sql,
                "fixed_sql": fixed_sql,
                "source": "ai",
                "confidence": 70,
                "execution_time": time.time() - start_time
            }
            
        except Exception as e:
            return {
                "success": False,
                "sql": sql,
                "fixed_sql": None,
                "source": "ai",
                "confidence": 0,
                "error": str(e),
                "execution_time": time.time() - start_time
            }
    
    @observe_function("text_response_generation")
    def generate_text_response(self, question: str, sql: str = None, results: Any = None) -> Dict[str, Any]:
        """
        Generate a natural language text response based on the question, SQL query, and results
        
        Args:
            question: Natural language question
            sql: SQL query (if available)
            results: Query results (if available)
            
        Returns:
            Dictionary with text generation details
        """
        start_time = time.time()
        
        # Prepare schema context if not already prepared
        self._prepare_schema_context()
        
        try:
            # Determine if this is a conversational query or needs SQL analysis
            is_sql_related = sql is not None and results is not None
            
            if is_sql_related:
                # Format results for the model with safe escaping
                try:
                    results_formatted = self._format_results_for_display(results)
                    # Clean the formatted results to avoid any template issues
                    results_formatted = self._clean_for_template(results_formatted)
                except Exception as format_error:
                    print(f"Error formatting results: {format_error}")
                    # Create a safe fallback description
                    results_formatted = self._create_safe_results_summary(results)
                
                # Create prompt for text response generation with SQL analysis
                memory_context = ""
                if self.use_memory:
                    memory_context = self._prepare_memory_for_query(question) or ""
                
                # Create system message with safe formatting
                system_message = "You are an expert data analyst. Your task is to provide a natural language response based on the user's question, the SQL query generated, and the results obtained."
                
                if memory_context:
                    system_message += f"\n\n{memory_context}"
                
                system_message += f"""

### DATABASE SCHEMA:
{self.schema_context}

### TASK:
Based on the user's question, the SQL query, and the results, provide a clear, informative response that:
1. Directly answers the user's question
2. Summarizes the key findings from the data
3. Provides insights or observations about the results
4. Uses business-friendly language (avoid technical jargon)
5. Highlights important numbers, trends, or patterns

### GUIDELINES:
- Be concise but comprehensive
- Use specific numbers and facts from the results
- If the results are empty, explain what that means
- If there are interesting patterns, point them out
- Format numbers appropriately (e.g., currency, percentages)
- Use bullet points or lists for multiple findings"""

                # Create human message with template variables
                prompt = ChatPromptTemplate.from_messages([
                    ("system", system_message),
                    ("human", """User Question: {question}

SQL Query: {sql}

Results: {results}

Please provide a natural language response based on this information.""")
                ])
                
                # Generate text response with SQL analysis
                try:
                    formatted_messages = prompt.format_messages(
                        question=question,
                        sql=sql,
                        results=results_formatted
                    )
                    response = self.llm.invoke(formatted_messages)
                except Exception as prompt_error:
                    print(f"Error in prompt formatting for SQL analysis: {prompt_error}")
                    # Create a simple fallback response with safe summary
                    safe_summary = self._create_safe_results_summary(results)
                    fallback_prompt = ChatPromptTemplate.from_messages([
                        ("system", "You are a data analyst. Provide a brief summary of the query results."),
                        ("human", "Question: {question}\n\nResults summary: {summary}")
                    ])
                    response = self.llm.invoke(fallback_prompt.format_messages(question=question, summary=safe_summary))
                
            else:
                # This is a conversational query, no SQL needed
                memory_context = ""
                if self.use_memory:
                    memory_context = self._prepare_memory_for_query(question) or ""
                
                # Create system message with safe formatting
                system_message = "You are a helpful assistant for a database query system. You can answer general questions about databases, SQL, data analysis, or provide conversational responses."
                
                if memory_context:
                    system_message += f"\n\n{memory_context}"
                
                system_message += f"""

### DATABASE SCHEMA:
{self.schema_context}

### TASK:
Provide a helpful response to the user's question. If the question is about the database or data analysis, use your knowledge to provide accurate information. If it's a general question, provide a friendly and helpful response."""

                prompt = ChatPromptTemplate.from_messages([
                    ("system", system_message),
                    ("human", "User Question: {question}")
                ])
                
                # Generate conversational response
                try:
                    formatted_messages = prompt.format_messages(question=question)
                    response = self.llm.invoke(formatted_messages)
                except Exception as prompt_error:
                    print(f"Error in prompt formatting for conversational response: {prompt_error}")
                    # Create a simple fallback response
                    fallback_prompt = ChatPromptTemplate.from_messages([
                        ("system", "You are a helpful assistant."),
                        ("human", "Please respond to: {question}")
                    ])
                    response = self.llm.invoke(fallback_prompt.format_messages(question=question))
            
            text_response = self._extract_response_content(response).strip()
            
            # Store in session context for future reference
            if self.session_context:
                self.session_context["text_responses"].append({
                    "question": question,
                    "text": text_response,
                    "timestamp": time.time()
                })
                
                # Limit stored responses to last 10
                if len(self.session_context["text_responses"]) > 10:
                    self.session_context["text_responses"] = self.session_context["text_responses"][-10:]
            
            # Store in memory if enabled
            if self.use_memory:
                self._store_text_in_memory(question, text_response, sql, results)
            
            return {
                "success": True,
                "text": text_response,
                "generation_time": time.time() - start_time
            }
            
        except Exception as e:
            return {
                "success": False,
                "text": f"Error generating text response: {str(e)}",
                "generation_time": time.time() - start_time
            }
    
    def _format_results_for_display(self, results: Any) -> str:
        """Format query results for display in text responses"""
        if not results:
            return "No results returned."
        
        try:
            # For list results, format as a table or list
            if isinstance(results, list):
                if not results:
                    return "Empty result set."
                
                # First try to format manually to avoid pandas issues
                if len(results) <= 10:
                    # For small result sets, format manually
                    return self._format_results_manually(results)
                
                # For larger result sets, try pandas but with error handling
                try:
                    import pandas as pd
                    
                    # Clean the data before creating DataFrame
                    cleaned_results = []
                    for row in results:
                        if isinstance(row, dict):
                            # Clean each row to ensure all values are serializable
                            cleaned_row = {}
                            for key, value in row.items():
                                # Convert problematic types to strings
                                if value is None:
                                    cleaned_row[key] = "NULL"
                                elif isinstance(value, (int, float, str, bool)):
                                    cleaned_row[key] = value
                                else:
                                    cleaned_row[key] = str(value)
                            cleaned_results.append(cleaned_row)
                        else:
                            cleaned_results.append({"value": str(row)})
                    
                    df = pd.DataFrame(cleaned_results)
                    
                    # Format with reasonable width constraints
                    with pd.option_context('display.max_rows', 20, 'display.max_columns', 20, 'display.width', 1000):
                        formatted = df.to_string(index=False)
                        
                        # If too long, truncate
                        if len(formatted) > 4000:
                            formatted = formatted[:4000] + "\n... [truncated]"
                        
                        return formatted
                        
                except Exception as pandas_error:
                    # If pandas fails, fall back to manual formatting
                    print(f"Pandas formatting failed: {pandas_error}, falling back to manual formatting")
                    return self._format_results_manually(results[:20])  # Limit to 20 rows
            else:
                # For other types, use string representation
                return str(results)
        except Exception as e:
            # Last resort: return a basic description
            try:
                if isinstance(results, list) and results:
                    return f"Query returned {len(results)} row(s). Sample: {str(results[0])[:200]}..."
                else:
                    return f"Query results: {str(results)[:200]}..."
            except:
                return "Query completed but results could not be formatted for display."
    
    def _format_results_manually(self, results: list) -> str:
        """Manually format results as a simple table"""
        if not results:
            return "No results found."
        
        try:
            # Get column names from first row
            if isinstance(results[0], dict):
                columns = list(results[0].keys())
                
                # Create header
                formatted = " | ".join(columns) + "\n"
                formatted += "-" * len(formatted) + "\n"
                
                # Add rows
                for row in results:
                    row_values = []
                    for col in columns:
                        value = row.get(col, "")
                        # Handle None and convert all to string
                        if value is None:
                            row_values.append("NULL")
                        else:
                            row_values.append(str(value))
                    formatted += " | ".join(row_values) + "\n"
                
                return formatted
            else:
                # Simple list of values
                return "\n".join(str(item) for item in results)
                
        except Exception as e:
            return f"Manual formatting failed: {e}. Results count: {len(results)}"
    
    def _clean_for_template(self, text: str) -> str:
        """Clean text to make it safe for template formatting"""
        if not text:
            return ""
        
        try:
            # Replace problematic characters that could break template formatting
            cleaned = text.replace("{", "{{").replace("}", "}}")
            
            # Remove or escape other problematic characters
            # Replace single quotes that might be causing issues
            cleaned = cleaned.replace("'", "`")
            
            # Replace double quotes with escaped versions
            cleaned = cleaned.replace('"', '""')
            
            # Remove any control characters that might cause issues
            cleaned = ''.join(char for char in cleaned if ord(char) >= 32 or char in '\n\r\t')
            
            return cleaned
        except Exception as e:
            print(f"Error cleaning text for template: {e}")
            # Return a very safe fallback
            return "Results formatted but contain special characters that cannot be displayed."
    
    def _create_safe_results_summary(self, results) -> str:
        """Create a completely safe summary of results for template usage"""
        try:
            if not results:
                return "No results returned from the query."
            
            if isinstance(results, list):
                num_rows = len(results)
                if num_rows == 0:
                    return "Query executed successfully but returned no rows."
                
                # Get column names safely
                columns = []
                if results and isinstance(results[0], dict):
                    columns = list(results[0].keys())
                
                # Create safe summary
                summary = f"Query returned {num_rows} row"
                if num_rows != 1:
                    summary += "s"
                
                if columns:
                    # Clean column names to be safe
                    safe_columns = []
                    for col in columns[:5]:  # Limit to first 5 columns
                        safe_col = str(col).replace("'", "").replace('"', '').replace("{", "").replace("}", "")
                        safe_columns.append(safe_col)
                    
                    summary += f" with columns: {', '.join(safe_columns)}"
                    if len(columns) > 5:
                        summary += f" and {len(columns) - 5} more"
                
                # Add sample data safely
                if num_rows == 1:
                    summary += ". Single row result."
                elif num_rows <= 10:
                    summary += f". All {num_rows} rows returned."
                else:
                    summary += f". Showing first 10 of {num_rows} rows."
                
                return summary
            else:
                return "Query completed successfully with results."
                
        except Exception as e:
            print(f"Error creating safe results summary: {e}")
            return "Query executed successfully."
    
    def _store_text_in_memory(self, question: str, text_response: str, sql: str = None, results: Any = None) -> None:
        """Store text responses in memory for future reference"""
        if not self.memory or not self.use_memory:
            return
            
        try:
            # Create content with question and response
            content = f"Question: {question}\nResponse: {text_response}"
            
            # Add SQL context if available
            if sql:
                content += f"\nSQL: {sql}"
            
            # Add result summary if available (brief)
            if results and isinstance(results, list):
                num_rows = len(results)
                content += f"\nReturned {num_rows} rows"
            
            # Store in memory as a document
            doc = Document(page_content=content, metadata={"question": question, "type": "text_response"})
            self.memory.add_documents([doc])
        except Exception as e:
            print(f"Error storing text in memory: {e}")

    def _is_analysis_question(self, question: str) -> bool:
        """
        Check if the question requires data analysis with potentially multiple queries
        
        Args:
            question: The natural language question
            
        Returns:
            True if the question requires analysis with potentially multiple queries
        """
        # Keywords indicating analysis
        analysis_keywords = [
            "analyze", "analysis", "compare", "comparison", "trend", 
            "why", "reason", "explain", "growth", "decline", "difference",
            "versus", "vs", "against", "performance", "over time",
            "year over year", "month over month", "quarter over quarter",
            "decreasing", "increasing", "dropping", "rising", "falling", 
            "higher", "lower", "better", "worse", "improved", "deteriorated"
        ]
        
        # Check for comparison between time periods
        time_comparisons = [
            r"compare.*last\s+(year|month|quarter|week|period)",
            r"compare.*previous\s+(year|month|quarter|week|period)",
            r"(higher|lower|more|less|greater|fewer).*than\s+(last|previous)\s+(year|month|quarter|week|period)",
            r"(increase|decrease|change|drop|rise|fall)\s+from\s+(last|previous)\s+(year|month|quarter|week|period)",
            r"(increase|decrease|change|drop|rise|fall)\s+since\s+(last|previous)\s+(year|month|quarter|week|period)",
            r"(increase|decrease|change|drop|rise|fall)\s+compared to\s+(last|previous)\s+(year|month|quarter|week|period)",
            r"(this|current)\s+(year|month|quarter|week|period).*compared to",
            r"(this|current).*vs\.?\s+(last|previous)",
            r"why\s+(is|are|were|was|have|has|had)\s+.*(increase|decrease|change|higher|lower|more|less|drop|rise|fall)"
        ]
        
        # Check if the question starts with "why" - these almost always need analysis
        if question.lower().strip().startswith("why"):
            return True
            
        # Check if the question contains analysis keywords
        question_lower = question.lower()
        
        # Strong indicators of needing analysis
        if any(keyword in question_lower for keyword in analysis_keywords):
            return True
            
        # Check for time comparison patterns
        for pattern in time_comparisons:
            if re.search(pattern, question_lower):
                return True
                
        return False
    
    def _is_why_question(self, question: str) -> bool:
        """
        Check if the question is a 'why' question that requires deeper causal analysis
        
        Args:
            question: The natural language question
            
        Returns:
            True if this is a 'why' question requiring causal analysis
        """
        question_lower = question.lower().strip()
        
        # Direct "why" questions
        if question_lower.startswith("why"):
            return True
            
        # Questions about reasons or causes
        reason_patterns = [
            r"what\s+(is|are|were|was)\s+the\s+reason",
            r"what\s+caused",
            r"reason\s+for",
            r"cause\s+of",
            r"explain\s+why",
            r"how\s+come",
            r"tell\s+me\s+why",
            r"factors\s+(behind|causing)",
            r"what\s+explains",
        ]
        
        for pattern in reason_patterns:
            if re.search(pattern, question_lower):
                return True
                
        return False
        
    async def handle_why_question(self, question: str) -> Dict[str, Any]:
        """
        Handle 'why' questions that require deeper causal analysis between time periods
        
        Args:
            question: The natural language question asking for causal analysis
            
        Returns:
            Dictionary with execution results including comparative analysis
        """
        start_time = time.time()
        
        print(f"Handling 'why' question: {question}")
        
        # Step 1: Determine the key metrics and time periods that need to be compared
        time_periods_to_analyze = self._extract_time_periods_from_question(question)
        
        if not time_periods_to_analyze:
            # Default to comparing current period with previous period
            time_periods_to_analyze = [
                {"name": "Current period", "description": "Most recent data"},
                {"name": "Previous period", "description": "Period before the most recent data for comparison"}
            ]
        
        # Step 2: Generate queries for each time period
        query_results = []
        tables_info = []
        
        for period in time_periods_to_analyze:
            # Construct a query specific to this time period
            period_question = f"Show the relevant metrics for {period['name']}: {period['description']}"
            
            # Add the original question for context
            period_question += f". This is part of answering: {question}"
            
            # Generate SQL for this specific time period
            sql_generation = await self.generate_sql(period_question)
            
            if not sql_generation["success"] or not sql_generation.get("sql"):
                print(f"Failed to generate SQL for {period['name']}")
                continue
                
            sql = sql_generation["sql"]
            
            # Execute the SQL
            success, results, error = self.db_analyzer.execute_query(sql)
            
            if not success or not results:
                print(f"Failed to get results for {period['name']}: {error}")
                continue
                
            # Store the successful query and results
            query_results.append({
                "query_name": period["name"],
                "sql": sql,
                "results": results,
                "row_count": len(results),
                "description": period["description"]
            })
        
        # If we don't have at least two successful queries for comparison, try a combined approach
        if len(query_results) < 2:
            print("Insufficient period data, attempting combined query approach")
            
            # Generate a single comprehensive query that includes time period as a dimension
            combined_question = f"Show data across different time periods to analyze {question}"
            sql_generation = await self.generate_sql(combined_question)
            
            if sql_generation["success"] and sql_generation.get("sql"):
                sql = sql_generation["sql"]
                success, results, error = self.db_analyzer.execute_query(sql)
                
                if success and results:
                    query_results.append({
                        "query_name": "Combined period analysis",
                        "sql": sql,
                        "results": results,
                        "row_count": len(results),
                        "description": "Combined analysis across time periods"
                    })
        
        # If still no successful queries, return error
        if not query_results:
            print("No successful queries for 'why' analysis")
            return {
                "success": False,
                "question": question,
                "error": "Failed to retrieve data for analysis",
                "execution_time": time.time() - start_time
            }
        
        # Step 3: Process results for analysis
        print(f"Preparing {len(query_results)} query results for 'why' analysis")
        for qr in query_results:
            results = qr["results"]
            query_name = qr["query_name"]
            
            # Apply the same large result handling as in execute_multi_query_analysis
            if len(results) > 100:
                # Include statistics for large result sets
                import pandas as pd
                import numpy as np
                
                df = pd.DataFrame(results)
                
                # Get statistics
                stats = {}
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    stats[col] = {
                        "min": df[col].min() if not pd.isna(df[col].min()) else "N/A",
                        "max": df[col].max() if not pd.isna(df[col].max()) else "N/A",
                        "mean": df[col].mean() if not pd.isna(df[col].mean()) else "N/A",
                        "median": df[col].median() if not pd.isna(df[col].median()) else "N/A"
                    }
                
                # Sample rows
                sampled_rows = []
                sampled_rows.extend(results[:20])
                
                if len(results) > 40:
                    sampled_rows.append({"note": "... skipping middle rows ..."})
                    sampled_rows.extend(results[-20:])
                
                table_formatted = (
                    f"LARGE RESULT SET: {len(results)} rows total, showing first 20 and last 20 rows.\n\n"
                    f"STATISTICS FOR NUMERIC COLUMNS:\n{json.dumps(stats, indent=2)}\n\n"
                    f"SAMPLED ROWS:\n{self._format_results_for_display(sampled_rows)}"
                )
            else:
                table_formatted = self._format_results_for_display(results)
            
            # Add formatted table to tables_info
            tables_info.append(
                f"### {query_name} ###\n"
                f"DESCRIPTION: {qr.get('description', '')}\n"
                f"SQL:\n```sql\n{qr['sql']}\n```\n\n"
                f"RESULTS ({qr['row_count']} rows):\n{table_formatted}\n"
            )
        
        # Step 4: Generate specialized causal analysis with focus on explaining "why"
        print("Generating causal analysis for 'why' question")
        tables_info_text = "\n\n".join(tables_info)
        
        # Create specialized prompt that focuses on explaining causes
        causal_analysis_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert data analyst specialized in explaining causes and reasons behind business trends.

{"{{memory}}\n\n" if self.use_memory else ""}### DATABASE SCHEMA:
{{schema}}

### TASK:
Provide a detailed causal analysis answering the user's "why" question. Focus on explaining the reasons behind the observed trend or phenomenon.

### GUIDELINES:
1. Identify the key metrics that have changed between time periods
2. Calculate the percentage changes for important metrics
3. Look for patterns or anomalies that could explain the changes
4. Consider multiple potential causes for the observed patterns
5. Analyze both direct and indirect factors that might contribute
6. Compare metrics across different dimensions (time, products, regions, etc.)
7. Explain which factors appear most significant based on the data
8. Be specific about the magnitude and direction of changes
9. Use concrete numbers and percentages from the data to support your explanation
10. Rank the likely causes in order of impact when possible
11. Acknowledge limitations in the analysis if the data doesn't fully explain the trend

### OUTPUT FORMAT:
Provide a thorough analysis that directly answers why the observed trend is happening. Structure your response with clear sections covering different potential causes."""),
            ("human", "### USER QUESTION:\n\"{question}\"\n\n### QUERY RESULTS:\n{tables_info}")
        ])
        
        # Create a temporary chain for the causal analysis
        causal_chain = causal_analysis_prompt | self.llm
        
        # Prepare params for analysis
        params = {
            "schema": self.schema_context,
            "question": question,
            "tables_info": tables_info_text
        }
        
        # Add memory if enabled
        if self.use_memory:
            memory_context = self._prepare_memory_for_query(question) or ""
            params["memory"] = memory_context
        
        # Generate the causal analysis
        analysis_response = causal_chain.invoke(params)
        
        analysis_text = self._extract_response_content(analysis_response).strip()
        print("Causal analysis generated successfully")
        
        # Step 5: Store in memory and session context
        if self.use_memory:
            self._store_text_in_memory(question, analysis_text)
            
            for qr in query_results:
                self._store_in_memory(
                    f"Data for why analysis: {question} - {qr['query_name']}", 
                    qr['sql'], 
                    qr['results']
                )
        
        # Store in session context for future reference
        self.session_context["multi_query_results"] = query_results
        self.session_context["text_responses"].append({
            "question": question,
            "text": analysis_text,
            "timestamp": time.time()
        })
        
        print(f"'Why' analysis completed in {time.time() - start_time:.2f} seconds")
        
        # Step 6: Return the full result with all tables
        return {
            "success": True,
            "question": question,
            "is_multi_query": True,
            "is_why_analysis": True,
            "tables": query_results,
            "text": analysis_text,
            "execution_time": time.time() - start_time
        }
    
    def _extract_time_periods_from_question(self, question: str) -> List[Dict[str, str]]:
        """
        Extract time periods to analyze from the question
        
        Args:
            question: The natural language question
            
        Returns:
            List of time periods to analyze with name and description
        """
        question_lower = question.lower()
        time_periods = []
        
        # Common time period patterns
        period_patterns = {
            "current_year": r"(this|current|present)\s+year",
            "last_year": r"last\s+year|previous\s+year|year\s+ago",
            "current_quarter": r"(this|current|present)\s+quarter",
            "last_quarter": r"last\s+quarter|previous\s+quarter|quarter\s+ago",
            "current_month": r"(this|current|present)\s+month",
            "last_month": r"last\s+month|previous\s+month|month\s+ago"
        }
        
        for period_id, pattern in period_patterns.items():
            if re.search(pattern, question_lower):
                if "current" in period_id or "this" in period_id:
                    name = period_id.replace("_", " ").replace("current", "current").title()
                    time_periods.append({
                        "name": name,
                        "description": f"Data for {name}"
                    })
                elif "last" in period_id or "previous" in period_id:
                    name = period_id.replace("_", " ").replace("last", "previous").title()
                    time_periods.append({
                        "name": name,
                        "description": f"Data for {name}"
                    })
        
        # If we found specific periods, return them
        if time_periods:
            return time_periods
            
        # Default time periods if none were explicitly mentioned
        return [
            {"name": "Current Period", "description": "Most recent data"},
            {"name": "Previous Period", "description": "Period before the most recent data"}
        ]
    
    def _is_sql_question(self, question: str) -> bool:
        """
        Determine if a question requires SQL generation or is conversational
        
        Args:
            question: The natural language question
            
        Returns:
            True if the question likely requires SQL, False if it's conversational
        """
        # Keywords indicating SQL is needed
        sql_keywords = [
            "show me", "list", "find", "query", "select", "data", "database",
            "table", "record", "rows", "search", "get", "fetch", "retrieve",
            "count", "sum", "average", "total", "calculate", "analyze", "report",
            "compare", "filter", "sort", "order", "group", "join", "where",
            "how many", "which", "when", "sales", "customer", "order", "product"
        ]
        
        # Keywords indicating conversation
        conversation_keywords = [
            "hello", "hi ", "hey", "thanks", "thank you", "help", "explain",
            "what is", "how do", "why", "can you", "please", "would", "could",
            "definition", "mean", "define"
        ]
        
        # Question about the database structure
        schema_keywords = [
            "schema", "structure", "tables", "columns", "relationships", 
            "foreign keys", "primary keys"
        ]
        
        # Check for SQL-like patterns
        question_lower = question.lower()
        
        # Direct check for schema queries
        if any(keyword in question_lower for keyword in schema_keywords):
            return False  # Schema questions can be answered conversationally
            
        # Check for conversation patterns
        if any(keyword in question_lower for keyword in conversation_keywords):
            # If it also has SQL keywords, it might be asking how to query something
            if any(keyword in question_lower for keyword in sql_keywords):
                # If it contains "how to" or similar, it's likely asking about SQL, not for SQL
                return not any(phrase in question_lower for phrase in ["how to", "how do i", "how can i", "explain how"])
            return False
            
        # Check for SQL patterns
        if any(keyword in question_lower for keyword in sql_keywords):
            return True
            
        # Default to conversational for ambiguous cases
        return False

    def plan_queries(self, question: str) -> Dict[str, Any]:
        """
        Plan the queries needed for a complex analysis question
        
        Args:
            question: The natural language question
            
        Returns:
            Dictionary with query planning details
        """
        # Prepare schema context if not already prepared
        self._prepare_schema_context()
        
        # Default query plan to return in case of errors
        default_plan = {
            "is_multi_query": False,
            "query_plan": [
                {
                    "query_name": "Default query",
                    "description": "Single query to answer the question",
                    "time_period": "current",
                    "key_metrics": []
                }
            ],
            "analysis_approach": "Direct single query approach"
        }
        
        # Prepare params for query planning
        params = {
            "schema": self.schema_context,
            "question": question
        }
        
        # Add memory if enabled
        if self.use_memory:
            memory_context = self._prepare_memory_for_query(question) or ""
            params["memory"] = memory_context
        
        try:
            # Generate query plan
            response = self.query_planner_chain.invoke(params)
            
            response_text = self._extract_response_content(response).strip()
                
            # Parse the simple key-value format
            needs_multiple = False
            num_queries = 1
            query_plan = []
            analysis_approach = "Direct query approach"
            
            # Extract information using regex patterns
            needs_multiple_match = re.search(r'NEEDS_MULTIPLE_QUERIES:\s*(yes|no|true|false)', response_text, re.IGNORECASE)
            if needs_multiple_match:
                value = needs_multiple_match.group(1).lower()
                needs_multiple = value == "yes" or value == "true"
            
            num_queries_match = re.search(r'NUMBER_OF_QUERIES_NEEDED:\s*(\d+)', response_text, re.IGNORECASE)
            if num_queries_match:
                try:
                    num_queries = int(num_queries_match.group(1))
                except ValueError:
                    num_queries = 1
            
            # Extract query descriptions
            query_matches = re.findall(r'-\s*Query\s+\d+:\s*(.+?)(?=\n-|\nANALYSIS_APPROACH:|$)', response_text, re.DOTALL)
            if query_matches:
                for i, description in enumerate(query_matches):
                    query_plan.append({
                        "query_name": f"Query {i+1}",
                        "description": description.strip(),
                        "time_period": "not specified",
                        "key_metrics": []
                    })
            else:
                # Fallback to default query plan
                query_plan = default_plan["query_plan"]
            
            # Extract analysis approach
            analysis_match = re.search(r'ANALYSIS_APPROACH:\s*(.+?)$', response_text, re.DOTALL)
            if analysis_match:
                analysis_approach = analysis_match.group(1).strip()
            
            # Build the final query plan
            result = {
                "is_multi_query": needs_multiple and num_queries > 1,
                "query_plan": query_plan,
                "analysis_approach": analysis_approach
            }
            
            print("Query plan generated successfully")
            return result
            
            print("No valid text in response, using default plan")
            return default_plan
                
        except Exception as e:
            print(f"Error planning queries: {e}")
            return default_plan
    
    async def generate_sql_for_subquery(self, question: str, query_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate SQL for a subquery in a multi-query plan
        
        Args:
            question: The original question
            query_info: Information about this specific query
            
        Returns:
            Dictionary with SQL generation details
        """
        # Create a modified question that focuses on this specific subquery
        query_name = query_info.get("query_name", "")
        description = query_info.get("description", "")
        time_period = query_info.get("time_period", "")
        
        # Construct a more specific question for this subquery
        specific_question = f"For {query_name}: {description}"
        if time_period:
            specific_question += f" for {time_period}"
        
        # Add the original question for context
        specific_question += f". This is part of answering: {question}"
        
        # Generate SQL for this specific question
        return await self.generate_sql(specific_question)
    
    async def execute_multi_query_analysis(self, question: str, query_plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute multiple queries for complex analysis and provide a consolidated response
        
        Args:
            question: The natural language question
            query_plan: The query plan with multiple queries
            
        Returns:
            Dictionary with execution results
        """
        start_time = time.time()
        query_results = []
        tables_info = []
        
        print(f"Starting multi-query analysis for: {question}")
        print(f"Query plan has {len(query_plan.get('query_plan', []))} queries planned")
        
        # Step 1: Generate and execute all SQL queries
        for i, query_info in enumerate(query_plan.get("query_plan", [])):
            print(f"Processing query {i+1}: {query_info.get('query_name', f'Query {i+1}')}")
            
            # Generate SQL for this specific query
            sql_generation = await self.generate_sql_for_subquery(question, query_info)
            
            if not sql_generation["success"] or not sql_generation.get("sql"):
                print(f"Failed to generate SQL for query {i+1}")
                continue
                
            sql = sql_generation["sql"]
            print(f"Generated SQL for query {i+1}: {sql[:100]}...")  # Print first 100 chars for debug
            
            # Execute the SQL
            success, results, error = self.db_analyzer.execute_query(sql)
            
            if not success:
                print(f"Error executing query {i+1}: {error}")
                continue
                
            if not results or len(results) == 0:
                print(f"Query {i+1} returned no results")
                continue
            
            # Store the successful query and results
            query_name = query_info.get("query_name", f"Query {i+1}")
            query_results.append({
                "query_name": query_name,
                "sql": sql,
                "results": results,
                "row_count": len(results),
                "description": query_info.get("description", "")
            })
            
            print(f"Successfully executed query {i+1}, returned {len(results)} rows")
        
        # If no successful queries, return error
        if not query_results:
            print("No successful queries executed")
            return {
                "success": False,
                "question": question,
                "error": "Failed to execute any queries in the analysis plan",
                "execution_time": time.time() - start_time
            }
        
        # Step 2: Process and format each result table for the LLM, handling token limits
        print("Preparing query results for analysis")
        for qr in query_results:
            results = qr["results"]
            query_name = qr["query_name"]
            
            # Format the table with appropriate sampling if needed
            if len(results) > 100:  # For large result sets
                # Include statistics about the results
                import pandas as pd
                import numpy as np
                
                # Convert to DataFrame for easier analysis
                df = pd.DataFrame(results)
                
                # Get basic statistics
                stats = {}
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    stats[col] = {
                        "min": df[col].min() if not pd.isna(df[col].min()) else "N/A",
                        "max": df[col].max() if not pd.isna(df[col].max()) else "N/A",
                        "mean": df[col].mean() if not pd.isna(df[col].mean()) else "N/A",
                        "median": df[col].median() if not pd.isna(df[col].median()) else "N/A"
                    }
                
                # Sample rows for better representation
                sampled_rows = []
                
                # First few rows
                sampled_rows.extend(results[:20])
                
                # Last few rows
                if len(results) > 40:
                    sampled_rows.append({"note": "... skipping middle rows ..."})
                    sampled_rows.extend(results[-20:])
                
                # Format for the LLM
                table_formatted = (
                    f"LARGE RESULT SET: {len(results)} rows total, showing first 20 and last 20 rows.\n\n"
                    f"STATISTICS FOR NUMERIC COLUMNS:\n{json.dumps(stats, indent=2)}\n\n"
                    f"SAMPLED ROWS:\n{self._format_results_for_display(sampled_rows)}"
                )
            else:
                # For smaller result sets, include all rows
                table_formatted = self._format_results_for_display(results)
            
            # Add formatted table to tables_info
            tables_info.append(
                f"### {query_name} ###\n"
                f"DESCRIPTION: {qr.get('description', '')}\n"
                f"SQL:\n```sql\n{qr['sql']}\n```\n\n"
                f"RESULTS ({qr['row_count']} rows):\n{table_formatted}\n"
            )
        
        # Step 3: Generate analysis from the combined results
        print("Generating analysis from query results")
        tables_info_text = "\n\n".join(tables_info)
        
        # Prepare params for analysis
        params = {
            "schema": self.schema_context,
            "question": question,
            "tables_info": tables_info_text
        }
        
        # Add memory if enabled
        if self.use_memory:
            memory_context = self._prepare_memory_for_query(question) or ""
            params["memory"] = memory_context
        
        # Generate the analysis
        analysis_response = self.analysis_chain.invoke(params)
        
        analysis_text = self._extract_response_content(analysis_response).strip()
        print("Analysis generated successfully")
        
        # Step 4: Store in memory and session context
        if self.use_memory:
            # Store the analysis
            self._store_text_in_memory(question, analysis_text)
            
            # Also store each individual query
            for qr in query_results:
                self._store_in_memory(
                    f"Subquery for: {question} - {qr['query_name']}", 
                    qr['sql'], 
                    qr['results']
                )
        
        # Store in session context for future reference
        self.session_context["multi_query_results"] = query_results
        self.session_context["text_responses"].append({
            "question": question,
            "text": analysis_text,
            "timestamp": time.time()
        })
        
        print(f"Multi-query analysis completed in {time.time() - start_time:.2f} seconds")
        
        # Step 5: Return the full result with all tables
        return {
            "success": True,
            "question": question,
            "is_multi_query": True,
            "tables": query_results,
            "text": analysis_text,
            "execution_time": time.time() - start_time
        }
    
    @observe_function("sql_query_execution")
    async def execute_query(self, question: str, auto_fix: bool = True, max_attempts: int = 2) -> Dict[str, Any]:
        """
        Generate SQL from a natural language question and execute it
        
        Args:
            question: Natural language question
            auto_fix: Whether to automatically attempt to fix query errors
            max_attempts: Maximum number of auto-fix attempts
            
        Returns:
            Dictionary with execution results
        """
        # Create Langfuse trace for this query execution
        trace = create_langfuse_trace(
            name="sql_query_execution",
            metadata={
                "question": question,
                "auto_fix": auto_fix,
                "max_attempts": max_attempts
            }
        )
        # Check if this is a "why" question requiring specialized causal analysis
        if self._is_why_question(question):
            print("Detected 'why' question, using specialized causal analysis approach")
            return await self.handle_why_question(question)
        
        # Check if this is an analysis question requiring multiple queries
        elif self._is_analysis_question(question):
            # Plan the queries needed
            query_plan = self.plan_queries(question)
            
            if query_plan.get("is_multi_query", False):
                # Execute multi-query analysis
                return await self.execute_multi_query_analysis(question, query_plan)
        
        # Check if this is a conversational question not requiring SQL
        # Use LLM-based classification instead of keyword matching
        if self._is_conversational_question(question):
            # Generate text response without SQL
            text_result = self.generate_text_response(question)
            return {
                "success": True,
                "question": question,
                "sql": None,
                "is_conversational": True,
                "source": "ai",
                "confidence": 90,
                "results": None,
                "text": text_result.get("text", "I couldn't generate a response.")
            }

        # Generate the SQL for a standard query
        generation_result = await self.generate_sql(question)
        
        if not generation_result["success"] or not generation_result.get("sql"):
            # Generate text response for the error case
            text_result = self.generate_text_response(question)
            return {
                "success": False,
                "question": question,
                "sql": None,
                "source": generation_result.get("source", "unknown"),
                "confidence": generation_result.get("confidence", 0),
                "error": generation_result.get("error", "Failed to generate SQL"),
                "execution_time": generation_result.get("execution_time", 0),
                "results": None,
                "text": text_result.get("text", "I couldn't generate SQL for your question.")
            }
        
        # Execute the SQL
        sql = generation_result["sql"]
        success, results, error = self.db_analyzer.execute_query(sql)
        
        # If execution failed and auto-fix is enabled, try to fix it
        attempts = 0
        while not success and auto_fix and attempts < max_attempts:
            attempts += 1
            fix_result = self.fix_sql(sql, error)
            
            if fix_result["success"] and fix_result.get("fixed_sql"):
                sql = fix_result["fixed_sql"]
                success, results, error = self.db_analyzer.execute_query(sql)
                
                # If the fix worked, update the query cache
                if success and self.use_cache:
                    question_hash = self._get_question_hash(question)
                    if question_hash in self.cache:
                        self.cache[question_hash]["sql"] = sql
                        self._save_cache()
            else:
                # If fixing failed, break the loop
                break
        
        # Handle pagination for large result sets
        page_size = 10
        total_rows = len(results) if results else 0
        paginated_results = None
        table_id = None
        
        if success and results and len(results) > page_size:
            # Generate a unique table ID
            import uuid
            table_id = str(uuid.uuid4())
            
            # Store the full results for later pagination
            self.paginated_results[table_id] = {
                "question": question,
                "sql": sql,
                "results": results,
                "total_rows": total_rows,
                "timestamp": time.time()
            }
            
            # Return only the first page
            paginated_results = results[:page_size]
        else:
            paginated_results = results
        
        # Generate text response based on SQL and results
        text_result = self.generate_text_response(question, sql, results if success else None)
        
        # Generate chart recommendations if successful query with results
        visualization_recommendations = None
        if success and results and len(results) > 0:
            visualization_recommendations = self.generate_chart_recommendations(question, sql, results, database_type="general")
        
        # Store in memory if successful or even if failed (to remember errors too)
        if self.use_memory:
            self._store_in_memory(question, sql, results if success else None)
            
            # Update session context
            if success:
                self._update_session_context(question, sql, results)
            
        response = {
            "success": success,
            "question": question,
            "sql": sql,
            "is_conversational": False,
            "source": generation_result.get("source"),
            "confidence": generation_result.get("confidence"),
            "error": error,
            "auto_fixed": attempts > 0 and success,
            "fix_attempts": attempts,
            "execution_time": generation_result.get("execution_time", 0),
            "results": paginated_results,
            "text": text_result.get("text", ""),
            "visualization_recommendations": visualization_recommendations
        }
        
        # Add pagination metadata if applicable
        if table_id:
            response["pagination"] = {
                "table_id": table_id,
                "total_rows": total_rows,
                "page_size": page_size,
                "current_page": 1,
                "total_pages": (total_rows + page_size - 1) // page_size
            }
        
        return response

    def _update_session_context(self, question: str, sql: str, results: List[Dict]) -> None:
        """
        Update session context with the latest query information
        
        Args:
            question: The natural language question
            sql: The generated SQL query
            results: The results of the query
        """
        if not self.use_memory:
            return
            
        # Extract personal info
        personal_info = self._extract_personal_info(question)
        if personal_info:
            for line in personal_info.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    self.session_context["user_info"][key.strip()] = value.strip()
        
        # Store query in sequence
        query_entry = {
            "question": question,
            "sql": sql,
            "timestamp": time.time(),
            "has_results": bool(results) and len(results) > 0
        }
        
        if results and len(results) > 0:
            # Store summary of results
            row_count = len(results)
            sample = results[0]
            columns = list(sample.keys()) if isinstance(sample, dict) else []
            
            query_entry["result_summary"] = {
                "row_count": row_count,
                "columns": columns,
                "sample": results[0] if row_count > 0 else None
            }
            
            # Extract important values from the results
            important_values = self._extract_important_values(question, sql, results)
            query_entry["important_values"] = important_values
            
            # Update the overall important values
            self.session_context["important_values"].update(important_values)
            
            # Store the last query result
            self.session_context["last_query_result"] = {
                "question": question,
                "sql": sql,
                "results": results[:10]  # Store up to 10 rows
            }
        
        # Add to query sequence
        self.session_context["query_sequence"].append(query_entry)
        if len(self.session_context["query_sequence"]) > 10:
            # Keep only the last 10 queries
            self.session_context["query_sequence"] = self.session_context["query_sequence"][-10:]
    
    def _extract_important_values(self, question: str, sql: str, results: List[Dict]) -> Dict[str, Any]:
        """
        Extract important values from query results that might be referenced in future queries
        
        Args:
            question: The natural language question
            sql: The SQL query that was executed
            results: The results of the query
            
        Returns:
            Dictionary of extracted important values
        """
        important_values = {}
        
        # Extract date values
        date_columns = set()
        if results and len(results) > 0:
            for col, value in results[0].items():
                # Check if the column might contain date values
                if any(date_term in col.lower() for date_term in ['date', 'time', 'day', 'month', 'year']):
                    date_columns.add(col)
                    
                    # Store the actual date values
                    if value is not None:
                        important_values[f"last_{col.lower().replace(' ', '_')}"] = value
                        
                        # For single-value results that are dates, also store special reference names
                        if len(results) == 1 and len(results[0]) <= 2:
                            col_lower = col.lower()
                            if 'first' in col_lower or 'min' in col_lower or 'earliest' in col_lower:
                                important_values["first_date"] = value
                                # Keep track of which question produced this value
                                important_values["first_date_question"] = question
                            elif 'last' in col_lower or 'max' in col_lower or 'latest' in col_lower:
                                important_values["last_date"] = value
                                # Keep track of which question produced this value
                                important_values["last_date_question"] = question
                            else:
                                important_values["the_date"] = value
        
        # Extract min/max values that might be referenced
        aggregation_prefixes = ['min', 'max', 'first', 'last', 'top', 'bottom', 'latest', 'earliest']
        for col, value in results[0].items() if results else []:
            col_lower = col.lower()
            if any(col_lower.startswith(prefix) for prefix in aggregation_prefixes):
                important_values[col_lower.replace(' ', '_')] = value
                
        # Check if the query is finding a specific entity
        common_queries = {
            r'SELECT.*MAX\((.*?)date\)': "last_date",
            r'SELECT.*MIN\((.*?)date\)': "first_date",
            r'SELECT.*TOP\s+1\s+.*ORDER\s+BY': "specific_entity",
            r'SELECT.*LIMIT\s+1': "specific_entity"
        }
        
        for pattern, value_type in common_queries.items():
            if re.search(pattern, sql, re.IGNORECASE) and results and len(results) == 1:
                # This is likely retrieving a specific value that will be referenced later
                for col, val in results[0].items():
                    if value_type == "last_date":
                        important_values["last_date"] = val
                        important_values["last_date_question"] = question
                    elif value_type == "first_date":
                        important_values["first_date"] = val
                        important_values["first_date_question"] = question
                    else:
                        # For other specific entities, store with column name
                        important_values[f"{value_type}_{col.lower().replace(' ', '_')}"] = val
        
        # Special handling for comparison queries - if we detect a request to compare,
        # Make sure we track both values being compared
        comparison_words = ["compare", "versus", "vs", "vs.", "which", "between", "difference", "more", "less", "higher", "lower", "greater", "better", "worse", "top", "bottom"]
        
        if any(word in question.lower() for word in comparison_words):
            important_values["is_comparison_query"] = True
            
            # If this is a follow-up to previous queries, link to those important values
            if "first_date" in self.session_context["important_values"] and "first_date" not in important_values:
                important_values["first_date"] = self.session_context["important_values"]["first_date"]
                if "first_date_question" in self.session_context["important_values"]:
                    important_values["first_date_question"] = self.session_context["important_values"]["first_date_question"]
                
            if "last_date" in self.session_context["important_values"] and "last_date" not in important_values:
                important_values["last_date"] = self.session_context["important_values"]["last_date"]
                if "last_date_question" in self.session_context["important_values"]:
                    important_values["last_date_question"] = self.session_context["important_values"]["last_date_question"]
        
        return important_values

    def _extract_sql_conditions(self, sql: str) -> str:
        """Extract WHERE, HAVING, JOIN conditions, and LIMIT/ORDER BY clauses from SQL"""
        conditions = []
        
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s+(.*?)(?:GROUP BY|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            conditions.append(f"WHERE {where_match.group(1).strip()}")
            
        # Extract HAVING clause
        having_match = re.search(r'HAVING\s+(.*?)(?:ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if having_match:
            conditions.append(f"HAVING {having_match.group(1).strip()}")
            
        # Extract JOIN conditions
        join_matches = re.finditer(r'(INNER|LEFT|RIGHT|FULL|OUTER)?\s*JOIN\s+(\S+)\s+(?:AS\s+\w+\s+)?ON\s+(.*?)(?=(?:LEFT|RIGHT|INNER|FULL|OUTER)?\s*JOIN|\s+WHERE|\s+GROUP|\s+ORDER|\s+LIMIT|$)', 
                                 sql, re.IGNORECASE | re.DOTALL)
        for match in join_matches:
            join_type = match.group(1) or "JOIN"
            table = match.group(2)
            join_condition = match.group(3).strip()
            conditions.append(f"{join_type} {table} ON {join_condition}")
        
        # Extract ORDER BY clause
        order_match = re.search(r'ORDER BY\s+(.*?)(?:LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if order_match:
            conditions.append(f"ORDER BY {order_match.group(1).strip()}")
            
        # Extract LIMIT clause
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        if limit_match:
            conditions.append(f"LIMIT {limit_match.group(1).strip()}")
            
        return "; ".join(conditions)
        
    def _prepare_session_context_for_query(self, question: str) -> str:
        """
        Prepare relevant session context for the current query
        
        Args:
            question: The natural language question
            
        Returns:
            String with formatted session context
        """
        if not self.use_memory or not self.session_context:
            return ""
        
        context_parts = []
        
        # Add user information
        if self.session_context["user_info"]:
            user_info_str = "USER INFORMATION:\n"
            for key, value in self.session_context["user_info"].items():
                user_info_str += f"{key}: {value}\n"
            context_parts.append(user_info_str)
        
        # Check for references to previous queries
        reference_terms = [
            "this", "that", "these", "those", "it", "they", "them",
            "previous", "prior", "before", "last", "above", "mentioned",
            "same", "earlier"
        ]
        
        # Check for comparison terms
        comparison_terms = [
            "compare", "versus", "vs", "vs.", "which", "between", "difference", 
            "more", "less", "higher", "lower", "greater", "better", "worse",
            "top", "bottom", "each", "both", "two"
        ]
        
        # Check for superlative terms that might reference previous result set
        superlative_terms = [
            "highest", "lowest", "most", "least", "best", "worst", "largest", "smallest",
            "maximum", "minimum", "max", "min"
        ]
        
        # Check if query contains references to "those" or other terms that imply previous result set
        has_result_set_reference = any(f"{term} " in f" {question.lower()} " for term in ["those", "these", "them"])
        
        # Check for implicit references or refinement patterns
        refinement_starters = [
            "just", "only", "show", "list", "give", "select", "filter",
            "now", "then", "also", "and", "but"
        ]
        
        implicit_words = ["column", "row", "field", "value", "record", "result", "data"]
        
        # Determine if this looks like a refinement or follow-up query
        is_refinement = False
        is_reference = any(term in question.lower() for term in reference_terms)
        is_comparison = any(term in question.lower() for term in comparison_terms)
        is_superlative = any(term in question.lower() for term in superlative_terms)
        
        # Check if it starts with a refinement word
        if any(question.lower().startswith(term) for term in refinement_starters):
            is_refinement = True
            
        # Check for implicit reference patterns (like "just the ids and the date column")
        if any(word in question.lower() for word in implicit_words):
            is_refinement = True
            
        # Check for very short queries that are likely refinements
        if len(question.split()) <= 7:
            is_refinement = True
        
        # Always include the last query if it exists
        if self.session_context["last_query_result"]:
            last_result = self.session_context["last_query_result"]
            
            last_query_str = "PREVIOUS QUERY:\n"
            last_query_str += f"Question: {last_result['question']}\n"
            last_query_str += f"SQL: {last_result['sql']}\n"
            
            # Extract the conditions from the last SQL query if this looks like a refinement
            if is_refinement:
                # Extract WHERE/HAVING/JOIN conditions
                last_sql = last_result['sql']
                conditions = self._extract_sql_conditions(last_sql)
                if conditions:
                    last_query_str += f"Conditions: {conditions}\n"
                    
                # Extract the table names for context
                tables = self._extract_sql_tables(last_sql)
                if tables:
                    last_query_str += f"Tables: {', '.join(tables)}\n"
            
            # Include sample results
            if last_result["results"]:
                row_count = len(last_result["results"])
                if row_count == 1:
                    # For single row results, show the actual values clearly
                    last_query_str += "Result (single row):\n"
                    for col, val in last_result["results"][0].items():
                        last_query_str += f"- {col}: {val}\n"
                else:
                    # For multiple rows, summarize
                    last_query_str += f"Results: {row_count} rows with columns: "
                    if last_result["results"][0]:
                        last_query_str += ", ".join(last_result["results"][0].keys())
                    last_query_str += "\n"
                    
                    # If there's a reference to "those" or similar terms AND the previous query used LIMIT
                    if has_result_set_reference and "LIMIT" in last_result['sql'].upper():
                        last_query_str += "\nIMPORTANT: When referring to 'those' or 'these' results, you MUST preserve the exact same result set from the previous query.\n"
                        
                        # Extract IDs if available
                        if last_result["results"] and "salesorderid" in last_result["results"][0]:
                            ids = [str(row["salesorderid"]) for row in last_result["results"]]
                            last_query_str += f"Previous result set contains ONLY these IDs: {', '.join(ids)}\n"
                            last_query_str += "Use these exact IDs in a WHERE clause: WHERE salesorderid IN (" + ', '.join(ids) + ")\n"
            
            context_parts.append(last_query_str)
        
        # For refinements or references, add more context about previous queries
        if is_refinement or is_reference or is_superlative:
            # Add specific guidance for refinements
            guidance = "GUIDANCE FOR REFINEMENT:\n"
            
            if has_result_set_reference and is_superlative:
                guidance += "CRITICAL: This query refers to the SPECIFIC SET of results from the previous query. "
                guidance += "Do NOT run a new query against the entire table. "
                guidance += "You MUST restrict the query to ONLY the exact rows returned by the previous query.\n"
            elif is_refinement and not is_reference:
                guidance += "This appears to be a refinement of the previous query. "
                guidance += "Maintain the same filters and conditions from the previous query, "
                guidance += "but modify the output columns or presentation according to the new request.\n"
            context_parts.append(guidance)
            
            # Include important values
            if self.session_context["important_values"]:
                values_str = "IMPORTANT VALUES FROM PREVIOUS QUERIES:\n"
                for key, value in self.session_context["important_values"].items():
                    # Skip metadata keys
                    if not key.endswith('_question') and not key.startswith('is_'):
                        values_str += f"{key}: {value}\n"
                context_parts.append(values_str)
        
            # Include more context from earlier queries
            if len(self.session_context["query_sequence"]) > 1:
                earlier_queries = self.session_context["query_sequence"][:-1][-2:]  # Get up to 2 earlier queries
                
                earlier_str = "EARLIER QUERIES:\n"
                for idx, query in enumerate(earlier_queries):
                    earlier_str += f"Query {len(earlier_queries) - idx}:\n"
                    earlier_str += f"- Question: {query['question']}\n"
                    earlier_str += f"- SQL: {query['sql']}\n"
                    
                    if query.get("important_values"):
                        important_vals = {k: v for k, v in query["important_values"].items() 
                                         if not k.endswith('_question') and not k.startswith('is_')}
                        if important_vals:
                            earlier_str += "- Important values: "
                            earlier_str += ", ".join([f"{k}: {v}" for k, v in important_vals.items()])
                            earlier_str += "\n"
                
                context_parts.append(earlier_str)
                
        # Special handling for comparison queries
        if is_comparison:
            comparison_context = "COMPARISON CONTEXT:\n"
            
            # Check if we have cached first_date and last_date
            first_date = self.session_context["important_values"].get("first_date")
            last_date = self.session_context["important_values"].get("last_date")
            
            if first_date and last_date:
                comparison_context += f"You are being asked to compare values between these dates:\n"
                
                first_date_q = self.session_context["important_values"].get("first_date_question", "")
                if first_date_q:
                    comparison_context += f"- First date ({first_date}) was from query: \"{first_date_q}\"\n"
                else:
                    comparison_context += f"- First date: {first_date}\n"
                    
                last_date_q = self.session_context["important_values"].get("last_date_question", "")
                if last_date_q:
                    comparison_context += f"- Last date ({last_date}) was from query: \"{last_date_q}\"\n"
                else:
                    comparison_context += f"- Last date: {last_date}\n"
                    
                comparison_context += "Make sure to include BOTH dates in your comparison query.\n"
                context_parts.append(comparison_context)
            
        return "\n".join(context_parts)
        
    def _extract_sql_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL query"""
        tables = []
        
        # Extract FROM clause
        from_match = re.search(r'FROM\s+(.*?)(?:WHERE|GROUP BY|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if from_match:
            # Split by commas and clean up
            from_tables = from_match.group(1).strip().split(',')
            for table in from_tables:
                # Remove aliases and schema prefixes for simplicity
                table_clean = re.sub(r'(?:AS)?\s+\w+\s*$', '', table.strip(), flags=re.IGNORECASE)
                tables.append(table_clean.strip())
                
        # Extract JOIN tables
        join_matches = re.finditer(r'JOIN\s+(\S+)', sql, re.IGNORECASE)
        for match in join_matches:
            tables.append(match.group(1).strip())
            
        return tables

    def _prepare_memory_for_query(self, question: str) -> str:
        """Prepare memory context specifically for the current query"""
        memory_context = ""
        
        # First get session context (in-memory)
        session_context = self._prepare_session_context_for_query(question)
        if session_context:
            memory_context += session_context + "\n\n"
        
        # Then add vector store memory if available
        if self.memory and self.use_memory:
            try:
                # Get relevant memories
                vector_memory = self._get_memory_context(question)
                
                if vector_memory:
                    # Ensure vector_memory is a string
                    if not isinstance(vector_memory, str):
                        vector_memory = str(vector_memory)
                    
                    # Extract user information patterns
                    user_info = []
                    for line in vector_memory.split('\n'):
                        if line.startswith('User '):
                            user_info.append(line)
                    
                    # Add vector memory without duplicating user info already in session context
                    if session_context and user_info:
                        # Remove the user info from vector memory to avoid duplication
                        memory_lines = [line for line in vector_memory.split('\n') if not line.startswith('User ')]
                        filtered_memory = "\n".join(memory_lines)
                        memory_context += "PERSISTENT MEMORY:\n" + filtered_memory
                    else:
                        memory_context += vector_memory
            except Exception as e:
                print(f"Error preparing vector memory for query: {e}")
        
        return memory_context

    def get_paginated_results(self, table_id: str, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """
        Get a specific page of results for a previously executed query
        
        Args:
            table_id: The unique ID of the stored result set
            page: The page number to retrieve (1-indexed)
            page_size: Number of rows per page
            
        Returns:
            Dictionary with the requested page of results and pagination metadata
        """
        # Check if the table_id exists
        if table_id not in self.paginated_results:
            return {
                "success": False,
                "error": f"No results found for table_id: {table_id}",
                "results": []
            }
            
        # Get the stored results
        stored_data = self.paginated_results[table_id]
        results = stored_data["results"]
        total_rows = len(results)
        total_pages = (total_rows + page_size - 1) // page_size
        
        # Validate the page number
        if page < 1:
            page = 1
        elif page > total_pages:
            page = total_pages
            
        # Calculate start and end indices
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_rows)
        
        # Get the requested page of results
        page_results = results[start_idx:end_idx]
        
        return {
            "success": True,
            "question": stored_data["question"],
            "sql": stored_data["sql"],
            "results": page_results,
            "pagination": {
                "table_id": table_id,
                "total_rows": total_rows,
                "page_size": page_size,
                "current_page": page,
                "total_pages": total_pages
            }
        }

    def _is_conversational_question(self, question: str) -> bool:
        """
        Use the LLM to determine if a question is conversational rather than requiring SQL
        
        Args:
            question: The natural language question
            
        Returns:
            True if the question is conversational, False if it likely requires SQL
        """
        # Prepare schema context if not already prepared
        self._prepare_schema_context()
        
        # Define a prompt template for determining if a question is conversational
        conversation_classifier_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an expert at classifying database questions. Your task is to determine if a question requires SQL generation or if it's a conversational question that should be answered directly.

### DATABASE SCHEMA:
{schema}

### TASK:
Determine if this question requires generating SQL to query the database, or if it's a conversational question that should be answered directly.

Examples of SQL questions:
- "Show me the top 5 customers by sales"
- "List all orders from January 2023"
- "What was the total revenue last quarter?"
- "How many products are in each category?"

Examples of conversational questions:
- "What is a database index?"
- "How do I optimize SQL queries?"
- "Tell me about your capabilities"
- "What LLM are you using?"
- "How does this system work?"
- "What programming language is this built with?"
- "Can you explain what a foreign key is?"

### CLASSIFICATION:
Provide ONLY ONE of the following responses:
SQL_QUESTION - if the question requires database querying
CONVERSATIONAL - if the question is asking for information, explanation, or conversation"""),
            ("human", "{question}")
        ])
        
        # Create a temporary chain for classification
        conversation_classifier_chain = conversation_classifier_prompt | self.llm
        
        try:
            # Prepare params for classification
            params = {
                "schema": self.schema_context,
                "question": question
            }
            
            # Generate classification
            response = conversation_classifier_chain.invoke(params)
            
            classification = self._extract_response_content(response).strip().upper()
            
            # Check for conversational classification
            if "CONVERSATIONAL" in classification:
                print(f"LLM classified question as conversational: {question}")
                return True
            else:
                print(f"LLM classified question as requiring SQL: {question}")
                return False
            
            # Default to the keyword-based approach if LLM classification fails
            print("LLM classification failed, falling back to keyword-based approach")
            return not self._is_sql_question(question)
            
        except Exception as e:
            print(f"Error in conversational classification: {e}")
            # Fall back to the keyword-based approach
            return not self._is_sql_question(question)
    
    def _is_edit_operation(self, question: str) -> bool:
        """Use LLM to intelligently determine if a question requires edit operations"""
        try:
            # Prepare schema context if not already done
            if self.schema_context is None:
                self._prepare_schema_context()
            
            # Create a prompt to classify the query type
            classification_prompt = ChatPromptTemplate.from_messages([
                ("system", """You are an expert at classifying database queries. Your task is to determine if a question requires data modification (INSERT, UPDATE, DELETE) or just data retrieval (SELECT).

### DATABASE SCHEMA:
{schema}

### TASK:
Analyze the question and determine if it requires:
- EDIT operations (INSERT, UPDATE, DELETE) - modifying data in the database
- FETCH operations (SELECT) - retrieving/reading data from the database

### EXAMPLES:

EDIT OPERATIONS:
- "Add a new customer named John Doe"
- "Update the price of product X to $50"
- "Delete all orders from last year"
- "Insert a new record for employee Sarah"
- "Change the status of order 123 to completed"
- "Remove inactive users"
- "Set the discount to 10% for all premium customers"

FETCH OPERATIONS:
- "Show me all customers"
- "What are the top selling products?"
- "List orders from this month"
- "Find customers in New York"
- "Calculate total revenue"
- "Show product categories"
- "Get user details for John"

### RESPONSE:
Respond with ONLY one of these two words:
- EDIT (if the question requires data modification)
- FETCH (if the question requires data retrieval only)"""),
                ("human", "{question}")
            ])
            
            # Create classification chain
            classification_chain = classification_prompt | self.llm
            
            # Get classification
            response = classification_chain.invoke({
                "schema": self.schema_context,
                "question": question
            })
            
            classification = self._extract_response_content(response).strip().upper()
            
            # Return True if it's an edit operation
            return "EDIT" in classification
            
        except Exception as e:
            print(f"Error in LLM-based edit operation detection: {e}")
            # Fallback to pattern-based detection
            edit_patterns = [
                r'\b(insert|add|create|new)\b',
                r'\b(update|modify|change|edit|alter)\b',
                r'\b(delete|remove|drop|clear)\b',
                r'\b(set|assign|make)\b.*\b(equal|to|as)\b',
                r'\b(increase|decrease|increment|decrement)\b',
                r'\b(save|store|record)\b',
                r'\b(fix|correct|repair)\b',
            ]
            
            question_lower = question.lower()
            
            # Check against edit patterns
            for pattern in edit_patterns:
                if re.search(pattern, question_lower):
                    return True
            
            return False
    
    def generate_edit_sql(self, question: str) -> Dict[str, Any]:
        """Generate SQL for edit mode operations (INSERT, UPDATE, DELETE)"""
        start_time = time.time()
        
        try:
            # Prepare schema context if not already done
            if self.schema_context is None or self.example_patterns is None:
                self._prepare_schema_context()
            
            # Prepare memory context
            memory_context = ""
            if self.use_memory:
                memory_context = self._prepare_memory_for_query(question)
            
            # Generate SQL using edit mode prompt
            inputs = {
                "schema": self.schema_context,
                "question": question,
                "examples": self.example_patterns
            }
            
            if self.use_memory:
                inputs["memory"] = memory_context
            
            # Invoke the edit SQL chain
            response = self.edit_sql_chain.invoke(inputs)
            sql = self._extract_response_content(response).strip()
            
            # Clean up the SQL
            if sql.startswith("```sql"):
                sql = sql[6:]
            if sql.endswith("```"):
                sql = sql[:-3]
            sql = sql.strip()
            
            execution_time = time.time() - start_time
            
            return {
                "success": True,
                "question": question,
                "sql": sql,
                "execution_time": execution_time,
                "is_edit_query": True,
                "source": "edit_mode"
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "question": question,
                "sql": None,
                "error": str(e),
                "execution_time": execution_time,
                "is_edit_query": True,
                "source": "edit_mode"
            }
    
    def verify_edit_sql(self, sql: str, original_question: str) -> Dict[str, Any]:
        """Verify that an edit SQL query is safe and correct"""
        start_time = time.time()
        
        try:
            # Prepare schema context if not already done
            if self.schema_context is None:
                self._prepare_schema_context()
            
            # Generate verification report
            inputs = {
                "schema": self.schema_context,
                "sql": sql,
                "original_question": original_question
            }
            
            # Invoke the verification chain
            response = self.edit_verification_chain.invoke(inputs)
            verification_text = self._extract_response_content(response).strip()
            
            # Try to parse JSON response
            try:
                import json
                
                # Clean the response text first
                cleaned_text = verification_text.strip()
                
                # Remove markdown code blocks if present
                if cleaned_text.startswith('```json'):
                    cleaned_text = cleaned_text[7:]
                if cleaned_text.startswith('```'):
                    cleaned_text = cleaned_text[3:]
                if cleaned_text.endswith('```'):
                    cleaned_text = cleaned_text[:-3]
                
                # Try to extract JSON from text that might have additional content
                json_start = cleaned_text.find('{')
                json_end = cleaned_text.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_text = cleaned_text[json_start:json_end]
                    verification_result = json.loads(json_text)
                else:
                    # Try parsing the whole text as is
                    verification_result = json.loads(cleaned_text)
                    
            except (json.JSONDecodeError, ValueError) as e:
                print(f"JSON parsing failed: {e}")
                print(f"Raw response: {verification_text}")
                
                # If JSON parsing fails, create a basic structure
                verification_result = {
                    "is_safe": False,
                    "is_correct": False,
                    "safety_issues": ["Failed to parse verification response"],
                    "correctness_issues": ["Unable to verify"],
                    "impact_assessment": "Unknown",
                    "estimated_affected_records": "unknown",
                    "recommendations": ["Manual review required"],
                    "overall_verdict": "REQUIRES_REVIEW",
                    "explanation": f"Verification system encountered an error: JSON parsing failed. Raw response: {verification_text[:200]}..."
                }
            
            execution_time = time.time() - start_time
            
            return {
                "success": True,
                "verification_result": verification_result,
                "execution_time": execution_time,
                "raw_response": verification_text
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "error": str(e),
                "execution_time": execution_time,
                "verification_result": {
                    "is_safe": False,
                    "is_correct": False,
                    "overall_verdict": "DO_NOT_EXECUTE",
                    "explanation": f"Verification failed: {str(e)}"
                }
            }
    
    def execute_edit_query(self, sql: str) -> Dict[str, Any]:
        """Execute an edit query after verification and user confirmation"""
        start_time = time.time()
        
        try:
            # Clean the SQL first (remove literal \n characters)
            cleaned_sql = sql.replace('\\n', '\n').replace('\\t', '\t').strip()
            
            # Split SQL by separator to handle multiple statements
            sql_statements = [stmt.strip() for stmt in cleaned_sql.split('<----->') if stmt.strip()]

            # Clean the sql statements
            sql_statements = [stmt.replace('\\n', '\n').replace('\\t', '\t').strip() for stmt in sql_statements]

            # Print the sql statements
            print(f"SQL Statements: {sql_statements}")
            
            if len(sql_statements) == 1:
                # Single query execution (no transaction needed)
                return self._execute_single_query(sql_statements[0], start_time)
            else:
                # Multiple queries - execute with transaction support
                return self._execute_multiple_queries_with_transaction(sql_statements, start_time)
                
        except Exception as e:
            execution_time = time.time() - start_time
            # Clean SQL for error reporting too
            cleaned_sql = sql.replace('\\n', '\n').replace('\\t', '\t').strip()
            return {
                "success": False,
                "sql": cleaned_sql,
                "results": [],
                "error": str(e),
                "execution_time": execution_time,
                "is_edit_execution": True,
                "affected_rows": 0,
                "query_count": 0
            }

    def _execute_multiple_queries_with_transaction(self, sql_statements: List[str], start_time: float) -> Dict[str, Any]:
        """Execute multiple SQL queries with transaction support (rollback on failure)"""
        try:
            # Use the new transaction-aware method from db_analyzer
            success, results_list, error = self.db_analyzer.execute_query_with_transaction(sql_statements)
            execution_time = time.time() - start_time
            
            if success:
                # Calculate total affected rows from all queries
                total_affected_rows = sum(result.get("affected_rows", 0) for result in results_list)
                
                # Collect all result data
                all_results = []
                for result in results_list:
                    if result.get("results"):
                        all_results.extend(result["results"])
                
                return {
                    "success": True,
                    "sql": '<----->'.join(sql_statements),
                    "results": all_results,
                    "execution_time": execution_time,
                    "is_edit_execution": True,
                    "affected_rows": total_affected_rows,
                    "query_count": len(sql_statements),
                    "query_results": results_list,
                    "transaction_mode": True
                }
            else:
                # Transaction failed and was rolled back
                failed_query_num = len([r for r in results_list if r.get("success", True)]) + 1
                
                return {
                    "success": False,
                    "sql": '<----->'.join(sql_statements),
                    "results": [],
                    "error": f"{error} (Transaction rolled back)",
                    "execution_time": execution_time,
                    "is_edit_execution": True,
                    "affected_rows": 0,
                    "query_count": len(sql_statements),
                    "failed_at_query": failed_query_num,
                    "query_results": results_list,
                    "transaction_mode": True,
                    "rollback_performed": True
                }
                
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "sql": '<----->'.join(sql_statements),
                "results": [],
                "error": f"Transaction execution failed: {str(e)}",
                "execution_time": execution_time,
                "is_edit_execution": True,
                "affected_rows": 0,
                "query_count": len(sql_statements),
                "transaction_mode": True,
                "rollback_performed": True
            }
    
    def _execute_single_query(self, sql: str, start_time: float) -> Dict[str, Any]:
        """Execute a single SQL query"""
        # Execute the query - db_analyzer.execute_query returns (success, results, error)
        success, raw_results, error = self.db_analyzer.execute_query(sql)
        execution_time = time.time() - start_time
        
        # Check if execution failed
        if not success:
            return {
                "success": False,
                "sql": sql,
                "results": [],
                "error": error or "Query execution failed",
                "execution_time": execution_time,
                "is_edit_execution": True,
                "affected_rows": 0,
                "query_count": 1
            }
        
        # Handle different result types
        if isinstance(raw_results, list):
            results = raw_results
            affected_rows = len(raw_results)
        elif isinstance(raw_results, (int, str)):
            results = []
            affected_rows = int(raw_results) if str(raw_results).isdigit() else 0
        elif raw_results is None:
            results = []
            affected_rows = 0
        elif hasattr(raw_results, 'rowcount'):
            results = []
            affected_rows = raw_results.rowcount
        else:
            results = []
            affected_rows = 1 if raw_results else 0
        
        return {
            "success": True,
            "sql": sql,
            "results": results,
            "execution_time": execution_time,
            "is_edit_execution": True,
            "affected_rows": affected_rows,
            "query_count": 1
        }
    
    async def process_unified_query(self, question: str, user_role: str = "viewer", edit_mode_enabled: bool = False) -> Dict[str, Any]:
        """
        Unified query processing that automatically detects edit vs fetch operations
        and handles them appropriately based on user permissions
        """
        start_time = time.time()
        
        try:
            # Step 1: Use LLM to determine if this is an edit or fetch operation
            is_edit_operation = self._is_edit_operation(question)
            
            # Step 2: Handle based on operation type and user permissions
            if is_edit_operation:
                # Check if user has permission for edit operations
                if user_role != "admin" or not edit_mode_enabled:
                    execution_time = time.time() - start_time
                    return {
                        "success": False,
                        "question": question,
                        "error": "Edit operations require admin access with edit mode enabled",
                        "execution_time": execution_time,
                        "is_edit_operation": True,
                        "is_edit_query": True,
                        "requires_admin": True,
                        "query_type": "edit_blocked"
                    }
                
                # Generate edit SQL using edit mode prompts
                edit_result = self.generate_edit_sql(question)
                
                if not edit_result["success"]:
                    return edit_result
                
                # Verify the generated SQL
                verification_result = self.verify_edit_sql(edit_result["sql"], question)
                
                execution_time = time.time() - start_time
                
                return {
                    "success": True,
                    "question": question,
                    "sql": edit_result["sql"],
                    "execution_time": execution_time,
                    "is_edit_operation": True,
                    "is_edit_query": True,
                    "requires_confirmation": True,
                    "verification_result": verification_result.get("verification_result"),
                    "query_type": "edit_sql",
                    "text": f"I've generated an edit query for you. Please review the SQL and click execute if you're satisfied with it."
                }
            
            else:
                # Handle as regular fetch operation
                fetch_result = await self.execute_query(question, auto_fix=True, max_attempts=2)
                
                # Add flags to indicate this was processed as fetch operation
                fetch_result["is_edit_operation"] = False
                fetch_result["is_edit_query"] = False
                fetch_result["query_type"] = fetch_result.get("query_type", "fetch")
                
                return fetch_result
                
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "question": question,
                "error": str(e),
                "execution_time": execution_time,
                "is_edit_operation": None,
                "is_edit_query": False,
                "query_type": "error"
            }

    def refresh_schema_context(self) -> bool:
        """
        Refresh the schema context after potential schema changes
        
        Returns:
            True if schema was refreshed successfully, False otherwise
        """
        try:
            # Re-analyze the schema
            self.db_analyzer.analyze_schema()
            
            # Update the prepared schema context
            self._prepare_schema_context()
            
            print("Schema context refreshed successfully")
            return True
        except Exception as e:
            print(f"Error refreshing schema context: {e}")
            return False

    def check_and_refresh_schema_if_needed(self, executed_sql: str) -> bool:
        """
        Check if the executed SQL requires schema refresh and refresh if needed
        
        Args:
            executed_sql: The SQL that was executed
            
        Returns:
            True if schema was refreshed or no refresh was needed, False if refresh failed
        """
        # Check if the SQL contains schema-changing operations
        schema_changing_keywords = [
            'CREATE TABLE', 'DROP TABLE', 'ALTER TABLE', 
            'CREATE INDEX', 'DROP INDEX', 'CREATE VIEW', 'DROP VIEW',
            'CREATE SCHEMA', 'DROP SCHEMA', 'RENAME TABLE',
            'ADD COLUMN', 'DROP COLUMN', 'RENAME COLUMN',
            'CREATE SEQUENCE', 'DROP SEQUENCE', 'TRUNCATE TABLE'
        ]
        
        sql_upper = executed_sql.upper().strip()
        needs_refresh = any(keyword in sql_upper for keyword in schema_changing_keywords)
        
        if needs_refresh:
            print("Schema-changing operation detected, refreshing schema context...")
            return self.refresh_schema_context()
        
        return True  # No refresh needed

    def execute_edit_query_with_schema_update(self, sql: str) -> Dict[str, Any]:
        """
        Execute an edit query and handle schema updates if needed
        
        This is an enhanced version of execute_edit_query that automatically
        handles schema context updates for schema-changing operations
        """
        # Execute the query first
        result = self.execute_edit_query(sql)
        
        # If the query was successful, check if we need to refresh schema
        if result.get("success", False):
            schema_refreshed = self.check_and_refresh_schema_if_needed(sql)
            result["schema_refreshed"] = schema_refreshed
            
            if not schema_refreshed:
                result["warning"] = "Query executed successfully but schema context refresh failed. Future queries may not reflect schema changes."
        
        return result

    @observe_function("chart_recommendations")
    def generate_chart_recommendations(self, question: str, sql: str, results: List[Dict[str, Any]], database_type: str = None) -> Dict[str, Any]:
        """
        Generate chart recommendations based on the query results using LLM
        
        Args:
            question: Original natural language question
            sql: Generated SQL query
            results: Query results
            database_type: Type of database/domain for context
            
        Returns:
            Dictionary with visualization recommendations
        """
        if not results or len(results) == 0:
            return {
                "is_visualizable": False,
                "reason": "No data returned from query",
                "recommended_charts": [],
                "database_type": database_type,
                "data_characteristics": None
            }
        
        try:
            # Analyze data characteristics
            data_characteristics = self._analyze_data_characteristics(results)
            print(f"Chart recommendations - data characteristics: {data_characteristics}")
            
            # Check if we have visualizable data characteristics
            has_numerical = len(data_characteristics.get("numerical_columns", [])) > 0
            has_categorical = len(data_characteristics.get("categorical_columns", [])) > 0
            has_data = len(data_characteristics.get("all_columns", [])) > 0
            
            # If we have no data characteristics but have results, something went wrong - use fallback
            if not has_data and results:
                print("Data characteristics analysis failed, using fallback recommendations")
                return self._create_fallback_recommendations({}, results)
            
            # Prepare prompt for LLM
            chart_recommendation_prompt = self._create_chart_recommendation_prompt()
            
            # Create context for the LLM
            context = {
                "question": question,
                "sql": sql,
                "data_sample": results[:5] if len(results) > 5 else results,  # First 5 rows
                "total_rows": len(results),
                "columns": list(results[0].keys()) if results else [],
                "data_characteristics": data_characteristics,
                "database_type": database_type or "general"
            }
            
            # Generate recommendations using LLM
            chain = chart_recommendation_prompt | self.llm
            response = chain.invoke(context)
            
            # Parse the LLM response
            response_text = self._extract_response_content(response)
            print(f"ions: {response_text[:500]}...")  # Debug log
            
            # Parse JSON response from LLM
            import json
            try:
                recommendations_data = json.loads(response_text)
            except json.JSONDecodeError:
                # Fallback: try to extract JSON from response
                import re
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                if json_match:
                    try:
                        recommendations_data = json.loads(json_match.group())
                    except json.JSONDecodeError:
                        # If JSON parsing still fails, create basic recommendations
                        print("JSON parsing failed, using fallback recommendations")
                        recommendations_data = self._create_fallback_recommendations(data_characteristics, results)
                else:
                    # If parsing fails, create basic recommendations
                    print("No JSON found in LLM response, using fallback recommendations")
                    recommendations_data = self._create_fallback_recommendations(data_characteristics, results)
            
            # Validate and format recommendations
            formatted_recommendations = self._format_chart_recommendations(recommendations_data, data_characteristics)
            
            # Final safety check - ensure no None values in x_axis or y_axis
            if formatted_recommendations.get("recommended_charts"):
                safe_charts = []
                for chart in formatted_recommendations["recommended_charts"]:
                    if (chart.get("x_axis") and chart.get("y_axis") and 
                        chart["x_axis"] != "None" and chart["y_axis"] != "None"):
                        safe_charts.append(chart)
                formatted_recommendations["recommended_charts"] = safe_charts
                
                # Update is_visualizable if no charts remain
                if not safe_charts:
                    formatted_recommendations["is_visualizable"] = False
                    formatted_recommendations["reason"] = "Unable to determine valid chart axes from data"
            
            # Double-check: if we marked as not visualizable but have good data, override with fallback
            if (not formatted_recommendations.get("is_visualizable") and 
                (has_numerical or has_categorical) and 
                len(formatted_recommendations.get("recommended_charts", [])) == 0):
                print("LLM marked as not visualizable but data seems good, using fallback")
                fallback_recommendations = self._create_fallback_recommendations(data_characteristics, results)
                if fallback_recommendations.get("is_visualizable"):
                    return fallback_recommendations
            
            return formatted_recommendations
            
        except Exception as e:
            print(f"Error in generate_chart_recommendations: {e}")
            # Return fallback recommendations
            data_chars = data_characteristics if 'data_characteristics' in locals() else {}
            fallback = self._create_fallback_recommendations(data_chars, results)
            return fallback

    def _analyze_data_characteristics(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze the characteristics of query results to help with visualization recommendations
        
        Args:
            results: List of query result dictionaries
            
        Returns:
            Dictionary with data characteristics including column types and statistics
        """
        if not results or len(results) == 0:
            return {}
            
        try:
            characteristics = {
                "numerical_columns": [],
                "categorical_columns": [],
                "date_columns": [],
                "unique_categories": {},
                "row_count": len(results),
                "all_columns": []
            }
            
            # Get all columns from first row
            first_row = results[0]
            characteristics["all_columns"] = list(first_row.keys())
            
            for column in first_row.keys():
                try:
                    # Extract all values for this column
                    values = [row.get(column) for row in results if row.get(column) is not None]
                    if not values:
                        continue
                        
                    sample_value = values[0]
                    
                    # Check if it's a date column
                    if isinstance(sample_value, (datetime, date)):
                        characteristics["date_columns"].append(column)
                    # Check if it's numerical
                    elif isinstance(sample_value, (int, float, Decimal)) and not isinstance(sample_value, bool):
                        # Additional check: if it looks like an ID field, treat as categorical if reasonable unique values
                        if (column.lower().endswith('id') or 'id' in column.lower()) and len(set(values)) > len(results) * 0.8:
                            # This looks like an ID field with high uniqueness - treat as categorical but low priority
                            characteristics["categorical_columns"].append(column)
                        else:
                            characteristics["numerical_columns"].append(column)
                    # Check if it's a string that could be parsed as date
                    elif isinstance(sample_value, str):
                        # Try to detect common date patterns without pandas
                        is_date = False
                        try:
                            from dateutil import parser
                            parser.parse(sample_value)
                            is_date = True
                        except:
                            # Check for common date patterns manually
                            import re
                            date_patterns = [
                                r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                                r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
                                r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
                                r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
                            ]
                            for pattern in date_patterns:
                                if re.match(pattern, str(sample_value)):
                                    is_date = True
                                    break
                        
                        if is_date:
                            characteristics["date_columns"].append(column)
                        else:
                            # It's a categorical string
                            characteristics["categorical_columns"].append(column)
                    else:
                        # Default to categorical
                        characteristics["categorical_columns"].append(column)
                    
                    # Count unique values for categorical insights
                    unique_values = set(str(v) for v in values if v is not None)
                    characteristics["unique_categories"][column] = len(unique_values)
                
                except Exception as column_error:
                    print(f"Error analyzing column {column}: {column_error}")
                    # If we can't analyze the column, default to categorical
                    characteristics["categorical_columns"].append(column)
                    characteristics["unique_categories"][column] = len(set(str(row.get(column)) for row in results if row.get(column) is not None))
            
            
            return characteristics
            
        except Exception as e:
            print(f"Error in _analyze_data_characteristics: {e}")
            # Return basic characteristics based on data types
            try:
                basic_characteristics = {
                    "numerical_columns": [],
                    "categorical_columns": [],
                    "date_columns": [],
                    "unique_categories": {},
                    "row_count": len(results),
                    "all_columns": list(results[0].keys()) if results else []
                }
                
                # Basic type detection fallback
                if results:
                    for column in results[0].keys():
                        sample_value = results[0].get(column)
                        if isinstance(sample_value, (int, float, Decimal)) and not isinstance(sample_value, bool):
                            basic_characteristics["numerical_columns"].append(column)
                        else:
                            basic_characteristics["categorical_columns"].append(column)
                
                return basic_characteristics
            except:
                return {}

    def _create_chart_recommendation_prompt(self):
        """Create the prompt template for chart recommendations"""
        
        return ChatPromptTemplate.from_messages([
            ("system", """You are an expert data visualization analyst. Based on the query and data provided, recommend the most appropriate charts for business analysis.

CRITICAL INSTRUCTIONS:
1. You MUST use ONLY the exact column names from the "Available Columns" list in the user message
2. For person names, if you see columns like "firstname" and "lastname", use one of them (not "Employee Name" or "Full Name")
3. For counts, use the actual column name (e.g., "order_count" not "Order Count")
4. NEVER invent column names that don't exist in the data

Please analyze this data and provide visualization recommendations in the following JSON format:

{{
    "is_visualizable": true/false,
    "reason": "explanation if not visualizable (null if visualizable)",
    "recommended_charts": [
        {{
            "chart_type": "bar/line/pie/donut/scatter/area/bubble/composed/radial/treemap/funnel/gauge/waterfall/heatmap/pyramid",
            "title": "Descriptive chart title using actual data context",
            "description": "Why this chart is recommended for business analysis",
            "x_axis": "EXACT_COLUMN_NAME_FROM_AVAILABLE_COLUMNS",
            "y_axis": "EXACT_COLUMN_NAME_FROM_AVAILABLE_COLUMNS", 
            "secondary_y_axis": "optional_exact_column_name",
            "chart_config": {{
                "color_scheme": "suggested color scheme",
                "aggregation": "sum/count/avg/max/min if needed",
                "name_combination": "firstname_lastname" // ONLY if using name fields
            }},
            "confidence_score": 0.85
        }}
    ],
    "database_type": "{database_type}",
    "data_characteristics": {{
        "numerical_columns": ["list of exact numerical column names"],
        "categorical_columns": ["list of exact categorical column names"], 
        "date_columns": ["list of exact date column names"],
        "unique_categories": number_of_unique_values_in_categorical_data
    }}
}}

CHART SELECTION GUIDELINES:
1. Only recommend charts that make business sense for the data
2. For simple ID lists or metadata-only queries, set is_visualizable to false
3. Prioritize charts that show trends, comparisons, or distributions
4. For time series data, prefer line/area charts
5. For categorical comparisons, prefer bar/donut charts  
6. For correlation analysis, prefer scatter/bubble charts
7. Consider the database type (e-commerce, financial, HR, etc.) for context
8. Provide 1-3 chart recommendations maximum
9. Each chart should have a clear business purpose
10. Set confidence_score based on how well the chart fits the data

COLUMN MAPPING RULES:
- If you see "firstname", "lastname" separately, use "firstname" for x-axis and suggest name_combination in chart_config
- For count/quantity fields, use the actual column name (e.g., "order_count", "total_sales")
- For categorical grouping, prefer shorter descriptive names over IDs when available
- Never use spaces in column names unless they exist in the actual data

Return ONLY the JSON object with no additional text or formatting."""),
            ("human", """Business Question: {question}
SQL Query: {sql}
Database Type: {database_type}
Total Rows: {total_rows}
Available Columns: {columns}

Data Sample:
{data_sample}

Data Characteristics:
{data_characteristics}""")
        ])

    def _create_fallback_recommendations(self, data_characteristics: Dict, results: List[Dict]) -> Dict[str, Any]:
        """Create basic fallback recommendations when LLM fails"""
        if not results or len(results) == 0:
            return {
                "is_visualizable": False,
                "reason": "No data available for visualization",
                "recommended_charts": [],
                "database_type": "general",
                "data_characteristics": data_characteristics
            }
        
        numerical_cols = data_characteristics.get("numerical_columns", [])
        categorical_cols = data_characteristics.get("categorical_columns", [])
        date_cols = data_characteristics.get("date_columns", [])
        all_cols = data_characteristics.get("all_columns", [])
        
        # Simple logic for fallback recommendations
        recommendations = []
        
        # Check for name fields that can be combined
        name_fields = []
        for col in all_cols:
            if any(name_part in col.lower() for name_part in ['firstname', 'lastname', 'name']):
                name_fields.append(col)
        
        # Prefer categorical fields that aren't IDs for x-axis
        preferred_categorical = [col for col in categorical_cols 
                               if not (col.lower().endswith('id') or 'id' in col.lower())]
        
        if len(numerical_cols) >= 1 and (len(preferred_categorical) >= 1 or len(name_fields) >= 1):
            # Basic bar chart recommendation
            x_col = name_fields[0] if name_fields else preferred_categorical[0]
            y_col = numerical_cols[0]
            
            chart_config = {}
            if x_col in name_fields and len(name_fields) >= 2:
                chart_config["name_combination"] = "_".join(name_fields[:2])
            
            recommendations.append({
                "chart_type": "bar",
                "title": f"{y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}",
                "description": "Bar chart showing values across categories",
                "x_axis": x_col,
                "y_axis": y_col,
                "secondary_y_axis": None,
                "chart_config": chart_config,
                "confidence_score": 0.7
            })
        elif len(numerical_cols) >= 2:
            # If we have multiple numerical columns, suggest a scatter plot
            recommendations.append({
                "chart_type": "scatter",
                "title": f"{numerical_cols[1].replace('_', ' ').title()} vs {numerical_cols[0].replace('_', ' ').title()}",
                "description": "Scatter plot showing correlation between values",
                "x_axis": numerical_cols[0],
                "y_axis": numerical_cols[1],
                "secondary_y_axis": None,
                "chart_config": {"color_scheme": "blue"},
                "confidence_score": 0.6
            })
        elif len(categorical_cols) >= 1 and len(numerical_cols) >= 1:
            # Pie chart for categorical distribution
            x_col = preferred_categorical[0] if preferred_categorical else categorical_cols[0]
            recommendations.append({
                "chart_type": "pie",
                "title": f"Distribution of {numerical_cols[0].replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}",
                "description": "Pie chart showing distribution across categories",
                "x_axis": x_col,
                "y_axis": numerical_cols[0],
                "secondary_y_axis": None,
                "chart_config": {"color_scheme": "multi"},
                "confidence_score": 0.6
            })
        
        if len(date_cols) >= 1 and len(numerical_cols) >= 1:
            # Time series line chart
            recommendations.append({
                "chart_type": "line", 
                "title": f"{numerical_cols[0].replace('_', ' ').title()} over time",
                "description": "Line chart showing trends over time",
                "x_axis": date_cols[0],
                "y_axis": numerical_cols[0],
                "secondary_y_axis": None,
                "chart_config": {"color_scheme": "blue"},
                "confidence_score": 0.8
            })
        
        return {
            "is_visualizable": len(recommendations) > 0,
            "reason": None if len(recommendations) > 0 else "Data structure not suitable for visualization",
            "recommended_charts": recommendations,
            "database_type": "general", 
            "data_characteristics": data_characteristics
        }

    def _format_chart_recommendations(self, recommendations_data: Dict, data_characteristics: Dict) -> Dict[str, Any]:
        """Format and validate chart recommendations from LLM response"""
        try:
            # Ensure required fields exist
            formatted = {
                "is_visualizable": recommendations_data.get("is_visualizable", False),
                "reason": recommendations_data.get("reason"),
                "recommended_charts": [],
                "database_type": recommendations_data.get("database_type", "general"),
                "data_characteristics": recommendations_data.get("data_characteristics", data_characteristics)
            }
            
            # Get available columns for validation
            available_columns = data_characteristics.get("all_columns", [])
            numerical_cols = data_characteristics.get("numerical_columns", [])
            categorical_cols = data_characteristics.get("categorical_columns", [])
            date_cols = data_characteristics.get("date_columns", [])
            
            # Format recommended charts
            for chart in recommendations_data.get("recommended_charts", []):
                if isinstance(chart, dict) and chart.get("chart_type") and chart.get("title"):
                    # Validate and fix x_axis and y_axis - ensure they exist in actual data
                    x_axis = chart.get("x_axis")
                    y_axis = chart.get("y_axis")
                    
                    # Validate x_axis exists in data
                    if x_axis not in available_columns:
                        # Try to find a suitable replacement
                        if categorical_cols:
                            x_axis = categorical_cols[0]
                        elif date_cols:
                            x_axis = date_cols[0]
                        elif available_columns:
                            x_axis = available_columns[0]
                        else:
                            continue  # Skip this chart
                    
                    # Validate y_axis exists in data
                    if y_axis not in available_columns:
                        # Try to find a suitable replacement
                        if numerical_cols:
                            y_axis = numerical_cols[0]
                        elif available_columns:
                            y_axis = next((col for col in available_columns if col != x_axis), available_columns[0])
                        else:
                            continue  # Skip this chart
                    
                    # Skip if either axis is empty after conversion
                    if not x_axis or not y_axis:
                        continue
                    
                    # Handle secondary_y_axis - ensure it's either None or exists in data
                    secondary_y_axis = chart.get("secondary_y_axis")
                    if secondary_y_axis and secondary_y_axis not in available_columns:
                        secondary_y_axis = None
                    
                    formatted_chart = {
                        "chart_type": str(chart["chart_type"]),
                        "title": str(chart["title"]),
                        "description": str(chart.get("description", "")),
                        "x_axis": x_axis,
                        "y_axis": y_axis,
                        "secondary_y_axis": secondary_y_axis,
                        "chart_config": chart.get("chart_config", {}),
                        "confidence_score": float(chart.get("confidence_score", 0.5))
                    }
                    formatted["recommended_charts"].append(formatted_chart)
            
            return formatted
            
        except Exception as e:
            print(f"Error formatting chart recommendations: {e}")
            return self._create_fallback_recommendations(data_characteristics, [])


if __name__ == "__main__":
    # Example usage
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Database connection parameters
    db_name = os.getenv("DB_NAME", "postgres")
    username = os.getenv("DB_USERNAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    
    # Initialize database analyzer
    db_analyzer = DatabaseAnalyzer(db_name, username, password, host, port)
    
    # Initialize SQL generator
    sql_generator = SmartSQLGenerator(
        db_analyzer,
        use_memory=True,
        memory_persist_dir="./memory_store"
    )
    
    # Test with a sample question
    question = "Show me the top 5 customers by total order amount"
    result = sql_generator.execute_query(question)
    
    if result["success"]:
        print(f"Question: {question}")
        print(f"SQL: {result['sql']}")
        print(f"Results: {len(result['results'])} rows")
        
        # Display first few results
        if result["results"]:
            import pandas as pd
            df = pd.DataFrame(result["results"])
            print("\nSample results:")
            print(df.head()) 