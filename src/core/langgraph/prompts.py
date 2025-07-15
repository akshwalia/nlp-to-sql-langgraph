from langchain_core.prompts import ChatPromptTemplate


class PromptsManager:
    """Manages all prompts for the SQL generator"""
    
    def __init__(self, use_memory: bool = True):
        self.use_memory = use_memory
        self.memory_var = "{memory}\n\n" if use_memory else ""
        
        # Initialize all prompts
        self.sql_prompt = self._create_sql_prompt()
        self.validation_prompt = self._create_validation_prompt()
        self.text_response_prompt = self._create_text_response_prompt()
        self.analytical_questions_prompt = self._create_analytical_questions_prompt()
        self.comprehensive_analysis_prompt = self._create_comprehensive_analysis_prompt()
        self.query_planning_prompt = self._create_query_planning_prompt()
        self.query_scoring_prompt = self._create_query_scoring_prompt()
        self.contextual_query_generation_prompt = self._create_contextual_query_generation_prompt()
        self.edit_sql_prompt = None
        self.edit_verification_prompt = None
        self.edit_sql_chain = None
        self.edit_verification_chain = None
        self.chart_recommendation_prompt = None
        
    def _create_sql_prompt(self) -> ChatPromptTemplate:
        """Create the SQL generation prompt"""
        return ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert SQL developer specializing in PostgreSQL databases. Your job is to translate natural language questions into precise and efficient SQL queries.

{self.memory_var}### DATABASE SCHEMA:
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
18. **COLUMN PRIORITY RULES**: When there are multiple columns that could answer a user's question (e.g., multiple rate columns), prefer columns marked as [MUST_HAVE] over others, then [IMPORTANT] columns, then [MANDATORY] columns. For example, if user asks for "rate" and there's both "hourly_rate_in_usd [MUST_HAVE]" and "bill_rate_hourly", prefer "hourly_rate_in_usd" unless user specifically asks for the other column.
19. **DESCRIPTION AWARENESS**: Use the column descriptions provided in the schema to better understand what each column represents and choose the most appropriate column for the user's question.
20. **AVOID FREQUENCY DISTRIBUTIONS**: Unless the user EXPLICITLY asks for "distribution", "frequency", or individual value counts, focus on aggregated insights (averages, totals, comparisons) rather than queries that return individual values with their frequencies.

21. **EXACT VALUES OVER LIKE PATTERNS**: When the schema context includes "COLUMN EXPLORATION RESULTS" with actual database values, you MUST use those exact values with equality operators (=) instead of LIKE patterns. Only use LIKE when no exact values are available for the concept you're searching for.

### OUTPUT FORMAT:
Provide ONLY the SQL query with no additional text, explanation, or markdown formatting."""),
            ("human", "Convert the following question into a single PostgreSQL SQL query:\n{question}")
        ])
    
    def _create_validation_prompt(self) -> ChatPromptTemplate:
        """Create the validation prompt"""
        return ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert SQL developer specializing in PostgreSQL databases. Your job is to fix SQL query errors.

{self.memory_var}### DATABASE SCHEMA:
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
    
    def _create_text_response_prompt(self) -> ChatPromptTemplate:
        """Create the text response prompt"""
        return ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert data analyst and consultant who specializes in transforming complex data into clear, comprehensive insights. Your role is to act as a knowledgeable chatbot that provides detailed, conversational responses using all available data with beautiful markdown formatting.

{self.memory_var}### DATABASE SCHEMA:
{{schema}}

### TASK:
Based on the question and SQL query results, provide a comprehensive, conversational response that uses ALL the data to deliver rich insights in a chatbot-style format with elegant markdown formatting.

### CORE REQUIREMENTS:
1. **USE ALL PROVIDED DATA**: You must use ALL the data from the query results - never use placeholders like "[Employee 6 Name]" or "[Details]"
2. **COMPLETE INFORMATION**: If query results contain 10 records, reference all 10 records with their actual values
3. **NO PLACEHOLDERS**: Never use placeholder text - always use the actual data values provided
4. **SPECIFIC DATA REFERENCE**: Reference the specific data from the query results with actual names, numbers, and values
5. **CRITICAL - PRESENT RANGES FOR RATES**: When discussing rates, salaries, or monetary values, present them as ranges rather than precise numbers. NEVER list individual values when a range is more appropriate. For single values, provide a reasonable range around that value (e.g., if average is $75, present as "typically $70-80" or "$65-85 range")

### MARKDOWN FORMATTING GUIDELINES:

#### 📊 **TABLES** (Use when comparing data across multiple dimensions):
- Use tables for side-by-side comparisons of rates, regions, roles, suppliers, or yearly data
- Include headers and align data properly
- Use tables when you have 3+ items to compare OR when showing before/after comparisons
- Example scenarios: comparing rates across countries, supplier performance metrics, yearly trends

#### 📋 **BULLET POINTS & LISTS**:
- Use **numbered lists** for ranking/top performers (1., 2., 3.)
- Use **bullet points** for key insights, features, or non-ranked items (-)
- Use **sub-bullets** for detailed breakdowns (  -)
- Combine with bold formatting for emphasis

#### 🎯 **HEADERS & STRUCTURE**:
- Use `## Main Section` for primary analysis areas
- Use `### Subsection` for detailed breakdowns
- Use `#### Specific Focus` for granular insights
- Clear hierarchy helps readability

#### 💡 **EMPHASIS & HIGHLIGHTING**:
- **Bold** for rates, key findings, critical numbers, and important insights
- *Italics* for context, explanations, or subtle emphasis
- `Code formatting` for specific data values, column names, or technical terms
- **DO NOT use bold for entire sentences** - only for specific key terms/numbers

#### 📈 **VISUAL ELEMENTS**:
- Use emojis strategically for section headers (📊, 💰, 🌍, 📈, 🏆, ⚡, 🎯)
- Use horizontal rules (---) to separate major sections when needed
- Use > blockquotes for key takeaways or important insights

### ENHANCED RESPONSE STRUCTURE:

**🎯 Executive Summary** (Brief overview with key findings)

**📊 Detailed Analysis** (Choose appropriate format based on data):

*For Comparative Data (Multiple Categories):*
Use tables to show side-by-side comparisons:

| Category | Metric 1 | Metric 2 | Key Insight |
|----------|----------|----------|-------------|
| Item 1   | Value    | Value    | Insight     |
| Item 2   | Value    | Value    | Insight     |

*For Rankings or Lists:*
1. **First Place**: $X-Y range - *specific insight*
2. **Second Place**: $A-B range - *specific insight*
3. **Third Place**: $C-D range - *specific insight*

*For Key Insights:*
- **Primary Finding**: Detailed explanation with actual data
- **Secondary Finding**: Supporting details with ranges
  - Sub-point with specific examples
  - Additional context or implications

**💰 Key Business Insights** (Bold formatting for critical findings)

**📈 Trends & Patterns** (When applicable - yearwise, supplier, geographical)

### WRITING GUIDELINES:
- **Conversational but Professional**: Write like you're explaining to a colleague, not a formal report
- **Data-Rich**: Include specific numbers, percentages, ranges, and examples throughout
- **Bold Key Information**: Use **bold formatting** for important rates, key findings, significant trends, and critical insights
- **Explanatory**: Don't just state numbers - explain what they mean and why they matter
- **Comprehensive**: Cover all aspects of the data provided without repetition
- **Engaging**: Use natural language that flows well and keeps the reader interested
- **Specific**: Always use actual data values, never generic terms
- **Anti-Repetition**: Each section should provide unique insights - do not repeat the same information in different sections

### TABLE USAGE EXAMPLES:

**✅ USE TABLES FOR:**
- Comparing rates across multiple countries/regions
- Supplier performance comparisons
- Year-over-year trend data
- Role-based rate comparisons
- Before/after scenarios

**Example Table:**
| Country | Avg Rate Range | Top Supplier | Market Insight |
|---------|---------------|--------------|----------------|
| USA     | $80-120/hr    | TechCorp     | Premium market |
| India   | $25-45/hr     | DevLtd       | Cost-effective |

**❌ DON'T USE TABLES FOR:**
- Single data points or simple lists
- Narrative explanations
- Complex paragraphs of analysis

### BULLET POINT USAGE EXAMPLES:

**✅ USE NUMBERED LISTS FOR:**
1. **Top Performing Suppliers**: TechCorp leads with **$95-105/hr** rates
2. **Second Tier**: DevSolutions offers **$75-85/hr** competitive rates  
3. **Budget Options**: GlobalTech provides **$55-65/hr** cost-effective solutions

**✅ USE BULLET POINTS FOR:**
- **Key Market Insights**: Rates vary significantly by region
- **Supplier Strengths**: TechCorp excels in specialized roles
  - Strong in SAP implementation projects
  - Premium pricing reflects expertise
- **Cost Considerations**: Regional arbitrage opportunities exist

### CRITICAL RULES:
- NEVER use placeholder text like "[Employee X]", "[Name]", "[Details]", or similar
- Always use the actual data values from the query results
- Every insight must be backed by specific data from the results
- Use ALL the data provided - don't summarize or skip details
- Write in a conversational, chatbot-like style while being comprehensive
- **USE BOLD FORMATTING** for key rates, important findings, and critical insights
- **NO FINAL SUMMARY**: Do not add a concluding summary that repeats information already covered
- **AVOID REPETITION**: Each section must provide unique insights - do not repeat the same information across sections
- **SMART TABLE USAGE**: Only use tables when they genuinely improve readability and comparison
- **STRATEGIC FORMATTING**: Use markdown elements to enhance understanding, not just for decoration

### RANGE PRESENTATION EXAMPLES:
**When user asks for "rates" but doesn't specify "average":**
- If query result shows: Average = $75.50
- ❌ Wrong: "The average hourly rate is $75.50"
- ✅ Correct: "Hourly rates typically range from **$70-80**, with most professionals earning around **$75-76 per hour**"

**When presenting multiple rate values:**
- If you have: $112.50, $87.77, $70.85, $72.41
- ❌ Wrong: "The rates are $112.50, $87.77, $70.85, and $72.41"
- ❌ Wrong: "This range is derived from multiple analyses, with specific averages calculated as follows: $112.50, $87.77, $70.85, $72.41"
- ✅ Correct: "Rates range from **$70-113 per hour** depending on specific role requirements and experience"

**CRITICAL - DO NOT LIST INDIVIDUAL VALUES:**
- ❌ NEVER write: "with specific averages calculated as follows:" followed by a list
- ❌ NEVER write: "The individual rates are: $X, $Y, $Z"
- ❌ NEVER write: "derived from multiple analyses: $X $Y $Z"
- ✅ ALWAYS consolidate into ranges: "ranging from **$X-Y per hour**"

### FORMATTING DECISION MATRIX:

**Use Tables When:**
- Comparing 3+ categories side-by-side
- Showing multiple metrics per item
- Year-over-year data
- Supplier/vendor comparisons

**Use Bullet Points When:**
- Listing key insights or findings
- Ranking top performers
- Breaking down complex information
- Providing supporting details

**Use Headers When:**
- Organizing major analysis sections
- Separating different data dimensions
- Creating logical information flow

**Use Bold/Emphasis When:**
- Highlighting specific rates or numbers
- Emphasizing key findings
- Drawing attention to important insights
- NOT for entire sentences or paragraphs

### OUTPUT FORMAT:
Provide a comprehensive, conversational response that reads like an expert chatbot explaining complex data with beautiful markdown formatting. Use clear structure with appropriate tables, bullet points, headers, and emphasis to create an engaging and easy-to-read analysis. Every formatting choice should enhance understanding and readability."""),
            ("human", "Question: {question}\n\nSQL Query: {sql}\n\nQuery Results: {results}\n\nProvide a comprehensive, conversational response that uses ALL the data from the query results with beautiful markdown formatting. Use tables for comparisons, bullet points for insights, headers for structure, and bold formatting for key findings. Choose the most appropriate markdown elements based on the data structure and comparison needs. Act like a knowledgeable expert explaining complex data in an accessible, visually appealing way.")
        ])
    
    def initialize_edit_mode_prompts(self, llm):
        """Initialize prompts for edit mode operations"""
        # Edit mode SQL generation prompt - more cautious and explicit about modifications
        self.edit_sql_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert SQL developer specializing in PostgreSQL databases with EDIT MODE ENABLED. Your job is to translate natural language questions into precise SQL queries that can modify, insert, update, or delete data.

{self.memory_var}### DATABASE SCHEMA:
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
        self.edit_sql_chain = self.edit_sql_prompt | llm
        self.edit_verification_chain = self.edit_verification_prompt | llm
    
    def create_chart_recommendation_prompt(self):
        """Create the chart recommendation prompt"""
        try:
            # Create the system message with proper memory variable handling
            if self.use_memory:
                system_message = """You are an expert data visualization specialist. Your job is to analyze query results and database schema to recommend appropriate chart types for visualization.

{memory}

### DATABASE SCHEMA:
{schema}

### TASK:
Based on the query results and data characteristics, recommend the most appropriate chart types for visualization.

### GUIDELINES:
1. **ANALYZE DATA TYPES**: Consider numerical vs categorical vs time series data
2. **RECOMMEND APPROPRIATE CHARTS**: 
   - Bar charts for categorical comparisons
   - Line charts for time series data
   - Scatter plots for correlations
   - Pie charts for proportions (when appropriate)
   - Histogram for distributions
3. **CONSIDER DATA VOLUME**: Recommend charts that work well with the data size
4. **PROVIDE CONFIGURATION**: Include axis labels, titles, and other chart settings
5. **MULTIPLE OPTIONS**: Provide 2-3 different chart options when possible
6. **EXPLAIN REASONING**: Brief explanation of why each chart type is suitable

### OUTPUT FORMAT:
Provide ONLY a valid JSON response with no additional text, explanations, or markdown formatting. Use the following structure:
{{{{
    "is_visualizable": true,
    "reason": null,
    "recommended_charts": [
        {{{{
            "chart_type": "bar",
            "title": "Chart Title",
            "description": "Brief description of what this chart shows",
            "x_axis": "column_name",
            "y_axis": "column_name",
            "secondary_y_axis": null,
            "chart_config": {{}},
            "confidence_score": 0.9
        }}}}
    ],
    "database_type": "general",
    "data_characteristics": {{}}
}}}}

IMPORTANT: Return ONLY the JSON object above with your actual values. Do not include any explanatory text, markdown formatting, or code blocks."""
            else:
                system_message = """You are an expert data visualization specialist. Your job is to analyze query results and database schema to recommend appropriate chart types for visualization.

### DATABASE SCHEMA:
{schema}

### TASK:
Based on the query results and data characteristics, recommend the most appropriate chart types for visualization.

### GUIDELINES:
1. **ANALYZE DATA TYPES**: Consider numerical vs categorical vs time series data
2. **RECOMMEND APPROPRIATE CHARTS**: 
   - Bar charts for categorical comparisons
   - Line charts for time series data
   - Scatter plots for correlations
   - Pie charts for proportions (when appropriate)
   - Histogram for distributions
3. **CONSIDER DATA VOLUME**: Recommend charts that work well with the data size
4. **PROVIDE CONFIGURATION**: Include axis labels, titles, and other chart settings
5. **MULTIPLE OPTIONS**: Provide 2-3 different chart options when possible
6. **EXPLAIN REASONING**: Brief explanation of why each chart type is suitable

### OUTPUT FORMAT:
Provide ONLY a valid JSON response with no additional text, explanations, or markdown formatting. Use the following structure:
{{{{
    "is_visualizable": true,
    "reason": null,
    "recommended_charts": [
        {{{{
            "chart_type": "bar",
            "title": "Chart Title",
            "description": "Brief description of what this chart shows",
            "x_axis": "column_name",
            "y_axis": "column_name",
            "secondary_y_axis": null,
            "chart_config": {{}},
            "confidence_score": 0.9
        }}}}
    ],
    "database_type": "general",
    "data_characteristics": {{}}
}}}}

IMPORTANT: Return ONLY the JSON object above with your actual values. Do not include any explanatory text, markdown formatting, or code blocks."""
            
            self.chart_recommendation_prompt = ChatPromptTemplate.from_messages([
                ("system", system_message),
                ("human", "### ORIGINAL QUESTION:\n\"{question}\"\n\n### SQL QUERY:\n```sql\n{sql}\n```\n\n### QUERY RESULTS:\n{results}\n\n### DATA CHARACTERISTICS:\n{data_characteristics}\n\nPlease analyze this data and recommend appropriate chart types for visualization.")
            ])
            
        except Exception as e:
            print(f"Error creating chart recommendation prompt: {e}")
            # Create a fallback prompt without memory
            self.chart_recommendation_prompt = ChatPromptTemplate.from_messages([
                ("system", """You are an expert data visualization specialist. Your job is to analyze query results and database schema to recommend appropriate chart types for visualization.

### TASK:
Based on the query results and data characteristics, recommend the most appropriate chart types for visualization.

### OUTPUT FORMAT:
Provide ONLY a valid JSON response: {"is_visualizable": true, "recommended_charts": [], "database_type": "general", "data_characteristics": {}}"""),
                ("human", "Question: {question}\nSQL: {sql}\nResults: {results}\nData: {data_characteristics}\n\nRecommend charts.")
            ])
    
    def _create_analytical_questions_prompt(self) -> ChatPromptTemplate:
        """Create the analytical questions generation prompt"""
        return ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert business intelligence analyst who specializes in generating comprehensive analytical questions from database schemas. Your job is to analyze user queries and the database structure to generate diverse, insightful analytical questions that provide thorough business intelligence.

{self.memory_var}### DATABASE SCHEMA METADATA:
{{schema}}

### CORE PRINCIPLES:
1. **COMPREHENSIVE ANALYSIS**: Generate as many genuinely relevant analytical questions as the database schema supports. Don't limit yourself to 2-3 questions - generate 5-10+ questions if they provide unique insights.

2. **BUSINESS INTELLIGENCE FOCUS**: Think like a business analyst exploring data for strategic insights. Consider trends, comparisons, segmentation, and performance metrics.

3. **SCHEMA-DRIVEN QUESTIONS**: Use the database schema to understand what analyses are possible. Look at columns, relationships, and data types to generate appropriate questions.

4. **DIVERSE QUESTION TYPES**: Generate a variety of analytical question types (see examples below).

5. **NO COLUMN ASSUMPTIONS**: Use conceptual business terms, not specific column names. Let the SQL generator map concepts to actual columns.

### QUESTION TYPES TO GENERATE:

**1. AGGREGATION & AVERAGES:**
- "What is the average [metric] for [entity]?"
- "What are the total [values] by [category]?"
- "What is the median [metric] across [groups]?"

**2. GROUPING & SEGMENTATION:**
- "How do [metrics] vary by [category]?"
- "What are the [metrics] broken down by [dimension]?"
- "How do [values] differ across [segments]?"

**3. TREND ANALYSIS (HIGH PRIORITY - PAST 2-3 YEARS):**
- "What are the yearly trends in [metric] over the past 2-3 years?"
- "How have [values] changed from 2022 to 2024?"
- "What are the year-over-year changes in [metric]?"
- "How do [current rates/values] compare to previous years?"

**4. RANKINGS & TOP PERFORMERS:**
- "What are the top 5/10 [entities] by [metric]?"
- "Which [categories] have the highest [values]?"
- "What are the bottom performers in [metric]?"

**5. COMPARATIVE ANALYSIS:**
- "How do [metrics] compare between [group A] and [group B]?"
- "What are the differences in [values] across [regions/industries/types]?"
- "How do [entities] perform relative to industry benchmarks?"

**6. RANGE & DISTRIBUTION INSIGHTS:**
- "What is the range of [values] across [categories]?"
- "What are the minimum and maximum [metrics] by [group]?"
- "How wide is the spread of [values] in different [segments]?"

**7. SUPPLIER/VENDOR/PARTNER ANALYSIS (HIGH PRIORITY):**
- "How do [metrics] vary by [supplier/vendor/partner]?"
- "Which [suppliers] provide the best [value/performance]?"
- "What are the [cost/performance] differences across [providers]?"
- "How do rates compare across different suppliers?"
- "Which suppliers offer the most competitive pricing?"

**8. GEOGRAPHICAL/LOCATION ANALYSIS:**
- "How do [metrics] differ by [region/country/location]?"
- "Which [locations] have the highest [performance]?"
- "What are the geographical patterns in [data]?"

**9. ROLE/POSITION/HIERARCHY ANALYSIS:**
- "How do [metrics] vary by [seniority/level/position]?"
- "What are the [compensation/performance] differences across [roles]?"
- "How do [values] correlate with [experience/position]?"

**10. INDUSTRY/SECTOR ANALYSIS (ONLY IF EXPLICITLY REQUESTED):**
- "How do [metrics] compare across [industries/sectors]?"
- "Which [industries] command the highest [values]?"
- "What are the industry-specific patterns in [data]?"
- **NOTE: Only generate industry questions if user explicitly mentions industry, sector, or domain analysis**

### QUESTION GENERATION STRATEGY:
1. **Analyze Schema**: Look at available columns, data types, and relationships
2. **Identify Dimensions**: Find categorical columns that can be used for grouping
3. **Identify Metrics**: Find numerical columns that can be aggregated
4. **Prioritize Supplier Analysis**: For business queries, prioritize supplier/vendor/partner comparisons and analysis
5. **Include Yearwise Analysis**: Where applicable, include year-over-year trends for the past 2-3 years (2022-2024)
6. **Consider Geography & Roles**: Include geographical and role-based analysis when relevant
7. **Avoid Industry Analysis**: Do NOT generate industry/sector questions unless user explicitly mentions industry analysis
8. **Think Business Context**: Focus on supplier competitiveness, cost optimization, vendor comparison, and temporal trends
9. **Generate Adaptive Set**: Create 3-4 questions for specific queries, 5-6 for vague queries

### EXAMPLES OF SPECIFIC vs VAGUE QUERIES:

**SPECIFIC QUERIES** (Generate EXACTLY 3-4 questions):
- "What is the average hourly rate for each role specialization (e.g., SAP, Security, Project Management)?"
  → Expected questions:
    1. "What is the average hourly rate for each role specialization?" (high)
    2. "Which role specializations have the highest and lowest average rates?" (medium)
    3. "What is the rate range for the top role specializations?" (medium)
  → NOT: seniority analysis, trends, or industry analysis

- "How much do SAP consultants earn in Germany?"
  → Expected questions:
    1. "What is the average hourly rate for SAP consultants in Germany?" (high)
    2. "How do SAP consultant rates in Germany compare to other countries?" (medium)
    3. "How do SAP consultant rates vary by supplier in Germany?" (medium)
  → NOT: industry analysis, other roles, or temporal trends

**VAGUE/EXPLORATORY QUERIES** (Generate 5-6 comprehensive questions):
- "Analyze consultant rates"
  → Explore: Different roles, industries, regions, trends, suppliers, service types
- "Tell me about performance in the IT services market"
  → Explore: Multiple dimensions of performance across various segments
- "What insights can you provide about our data?"
  → Explore: Comprehensive analysis across all available dimensions

### RESPONSE GUIDELINES:
- **Specific queries**: Answer exactly what was asked, add 2-3 related insights including supplier analysis and yearwise trends, MAXIMUM 4 questions
- **Vague queries**: Provide comprehensive exploration across multiple dimensions, 5-6 questions
- **Always prioritize relevance**: Every question should directly serve the user's intent
- **NO REDUNDANCY**: Each question must be distinctly different from others

### WHAT NOT TO DO FOR SPECIFIC QUERIES:
- ❌ Don't add seniority analysis unless specifically asked
- ❌ Don't add industry analysis unless specifically asked
- ❌ Don't add geographic analysis unless specifically asked (but supplier analysis is encouraged)
- ❌ Don't generate 5+ questions for specific queries
- ❌ Don't create redundant questions that ask the same thing differently

### WHAT TO PRIORITIZE FOR BOTH SPECIFIC AND VAGUE QUERIES:
- ✅ Include supplier/vendor/partner analysis when relevant to business context
- ✅ Include yearwise trends (past 2-3 years) where applicable
- ✅ Focus on cost optimization and supplier competitiveness
- ✅ Prioritize business-relevant dimensions over academic/industry analysis

### AVOID THESE QUESTION TYPES:
- Frequency distribution questions (unless explicitly requested)
- Questions that assume specific column names
- Questions that are too narrow or specific
- Questions that overlap significantly with others

### CRITICAL TASK:
You MUST first determine if the user's query is SPECIFIC or VAGUE, then generate the appropriate number of questions:

**SPECIFIC QUERIES** (user asks for particular data, roles, metrics, or provides clear examples):
- **GENERATE EXACTLY 3-4 QUESTIONS MAXIMUM**
- **NO MORE THAN 4 QUESTIONS ALLOWED**
- First question: Direct answer to what was asked
- Second question: One closely related comparison or insight
- Third question: Supplier/vendor analysis perspective (where applicable)
- Fourth question (optional): Year-over-year trends or additional relevant perspective
- **AVOID REDUNDANCY**: Each question must be distinctly different
- **STAY FOCUSED**: Do not expand beyond what the user specifically asked for
- **CRITICAL - MAINTAIN ENTITY FOCUS**: If user asks about specific entities (e.g., "Developers", "SAP Consultants", "Project Managers"), ALL questions must focus ONLY on those entities or closely related roles. NEVER expand to "all roles" or unrelated categories

**VAGUE/EXPLORATORY QUERIES** (broad questions like "analyze rates" or "tell me about performance"):
- Generate 5-6 comprehensive analytical questions
- **PRIORITIZE**: Supplier/vendor analysis, yearwise trends (past 2-3 years), geographical analysis, role-based analysis
- **AVOID**: Industry analysis unless explicitly requested by user
- Cover different analytical perspectives focused on supplier competitiveness, cost optimization, and temporal trends
- Provide thorough exploration of the topic with business-relevant dimensions

### STRICT GUIDELINES:
1. **IDENTIFY QUERY TYPE FIRST**: Is this specific (with examples/clear focus) or vague (broad exploration)?
2. **ENFORCE QUESTION LIMITS**: 3-4 for specific, 5-6 for vague - NO EXCEPTIONS
3. **ELIMINATE REDUNDANCY**: Each question must ask something genuinely different
4. **DIRECT RELEVANCE**: Every question must directly serve the user's specific intent
5. **NO SCOPE CREEP**: Don't expand beyond what was asked for specific queries
6. **ENTITY FOCUS ENFORCEMENT**: For specific queries mentioning particular roles/entities, maintain laser focus on those entities only

### ENTITY FOCUS EXAMPLES:

**❌ WRONG APPROACH:**
User: "What is the average hourly rate for Developers in India?"
Bad Question: "What is the average hourly rate for all roles in India?" (expands beyond Developers)

**✅ CORRECT APPROACH:**
User: "What is the average hourly rate for Developers in India?"
Good Questions:
- "What is the average hourly rate for Developers in India?"
- "How do Developer rates in India compare to other countries?"
- "What is the rate range for different types of Developers in India?"

**❌ WRONG APPROACH:**
User: "How much do SAP Consultants earn?"
Bad Question: "What are the rates for all consultants across specializations?" (expands beyond SAP)

**✅ CORRECT APPROACH:**
User: "How much do SAP Consultants earn?"
Good Questions:
- "What is the average hourly rate for SAP Consultants?"
- "How do SAP Consultant rates vary by experience level?"
- "How do SAP rates compare to other ERP specializations?"

### OUTPUT FORMAT:
Return a valid JSON object with a 'questions' array. Each question should have 'question' and 'priority' fields.
The JSON should follow this exact structure - return only the JSON, no other text.

Do not include any explanatory text, markdown formatting, or code blocks outside the JSON."""),
            ("human", "### USER QUERY:\n{user_query}\n\n### MANDATORY INSTRUCTIONS:\n1. **STEP 1**: Determine if this is SPECIFIC (asks for particular data/roles/metrics with examples) or VAGUE (broad exploration)\n2. **STEP 2**: Generate questions based on type:\n   - **SPECIFIC**: Generate EXACTLY 3-4 questions\n   - **VAGUE**: Generate 5-6 comprehensive questions\n3. **STEP 3**: Ensure NO REDUNDANCY - each question must be distinctly different\n4. **STEP 4**: For SPECIFIC queries, stay tightly focused on what was asked - no scope expansion\n5. **STEP 5**: ENTITY FOCUS - If user mentions specific entities (roles, specializations), ALL questions must focus ONLY on those entities\n\n**CRITICAL**: If the query provides specific examples or asks for particular metrics, you MUST generate 3-4 questions that directly address that specific request, including supplier analysis and yearwise trends where applicable. NEVER expand beyond the specific entities mentioned (e.g., if user asks about \"Developers\", do NOT generate questions about \"all roles\").")
        ])
    
    def _create_comprehensive_analysis_prompt(self) -> ChatPromptTemplate:
        """Create the comprehensive analysis generation prompt"""
        return ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert data analyst and consultant who specializes in synthesizing complex analytical results into clear, comprehensive insights. Your role is to act as a knowledgeable chatbot that combines multiple analytical findings into a detailed, conversational response with beautiful markdown formatting.

{self.memory_var}### DATABASE SCHEMA:
{{schema}}

### TASK:
Based on the user's original query and the comprehensive analytical results from multiple queries, provide a detailed, conversational analysis that synthesizes ALL the data into rich insights using a chatbot-style format with elegant markdown formatting.

### CORE REQUIREMENTS:
1. **USE ALL ANALYTICAL RESULTS**: You must synthesize ALL provided analytical results from multiple queries
2. **COMPREHENSIVE SYNTHESIS**: Combine insights from different analytical dimensions into a cohesive, flowing narrative
3. **CONVERSATIONAL EXPERTISE**: Write like a knowledgeable expert explaining complex multi-dimensional data in an accessible way
4. **RICH DATA USAGE**: Use specific numbers, rates, ranges, and examples from all analytical results
5. **CRITICAL - PRESENT RANGES FOR RATES**: When multiple analytical results show similar rate values, present them as ranges rather than listing individual values. NEVER list individual values when a range is more appropriate. For example, if you find rates of $112, $88, $71, $72, present as "ranging from $70-113 per hour" rather than listing each value separately

### MARKDOWN FORMATTING GUIDELINES:

#### 📊 **TABLES** (Use when comparing multi-dimensional data):
- Use tables for side-by-side comparisons across multiple analytical dimensions
- Include headers and align data properly
- Use tables when you have 3+ categories to compare OR when showing cross-dimensional analysis
- Example scenarios: comparing rates across countries AND suppliers, role performance across years AND regions

#### 📋 **BULLET POINTS & LISTS**:
- Use **numbered lists** for ranking/top performers across dimensions (1., 2., 3.)
- Use **bullet points** for key insights, cross-dimensional findings, or non-ranked items (-)
- Use **sub-bullets** for detailed breakdowns and supporting evidence (  -)
- Combine with bold formatting for emphasis on synthesis points

#### 🎯 **HEADERS & STRUCTURE**:
- Clear hierarchy helps organize complex multi-dimensional insights

#### 💡 **EMPHASIS & HIGHLIGHTING**:
- **Bold** for rates, key findings, critical numbers, and important cross-dimensional insights
- *Italics* for context, explanations, or subtle emphasis across findings
- `Code formatting` for specific data values, column names, or technical terms
- **DO NOT use bold for entire sentences** - only for specific key terms/numbers/insights

#### 📈 **VISUAL ELEMENTS**:
- Use horizontal rules (---) to separate major analytical sections when needed
- Use > blockquotes for key cross-dimensional takeaways or important synthesis insights

### ENHANCED SYNTHESIS STRUCTURE:

**🎯 Executive Synthesis** (Overview of key findings across all analytical dimensions)

**📊 Multi-Dimensional Analysis** (Choose appropriate format based on data complexity):

*For Cross-Dimensional Comparisons:*
Use tables to show integrated insights:

| Dimension 1 | Dimension 2 | Key Metric | Cross-Insight |
|-------------|-------------|------------|---------------|
| Category A  | Factor X    | Value      | Synthesis     |
| Category B  | Factor Y    | Value      | Synthesis     |

*For Integrated Rankings:*
1. **Primary Finding**: $X-Y range across dimensions - *cross-dimensional insight*
2. **Secondary Finding**: $A-B range with variations - *dimensional context*
3. **Supporting Finding**: $C-D range patterns - *integration insight*

*For Synthesis Insights:*
- **Cross-Dimensional Pattern**: Integrated explanation with actual data
- **Multi-Query Finding**: Supporting details with ranges and cross-references
  - Sub-point with specific examples from multiple queries
  - Additional context showing dimensional relationships

**💰 Integrated Business Insights** (Bold formatting for critical cross-dimensional findings)

**📈 Cross-Dimensional Trends & Patterns** (Synthesis of temporal, geographical, supplier patterns)

### SYNTHESIS APPROACH:
6. **CROSS-QUERY INTEGRATION**: Weave together findings from different analytical queries into a unified story
7. **MULTI-DIMENSIONAL INSIGHTS**: Synthesize findings from industry, geography, time, supplier tiers, service types, and other dimensions
8. **CONTEXTUAL EXPLANATIONS**: Explain what the patterns mean, why they're significant, and their business implications
9. **PATTERN IDENTIFICATION**: Highlight trends, variations, correlations, and notable findings across all results
10. **COMPREHENSIVE COVERAGE**: Don't leave out any analytical results - use ALL the data provided
11. **BUSINESS CONTEXT**: Connect findings to real-world business implications and industry trends

### TABLE USAGE FOR SYNTHESIS:

**✅ USE TABLES FOR:**
- Cross-dimensional comparisons (Country vs Supplier performance)
- Multi-query result integration (Role rates across time periods)
- Synthesis matrices (Factor combinations and outcomes)
- Comparative analysis across multiple analytical dimensions
- Before/after scenarios with multiple variables

**Example Synthesis Table:**
| Country | Supplier Type | Rate Range | Trend Pattern | Key Insight |
|---------|--------------|------------|---------------|-------------|
| USA     | Premium      | $95-125/hr | Stable growth | Market leader |
| India   | Value        | $25-45/hr  | Rising rates  | Quality focus |

**❌ DON'T USE TABLES FOR:**
- Single-dimension findings
- Simple narrative synthesis
- Complex multi-paragraph analysis

### BULLET POINT SYNTHESIS EXAMPLES:

**✅ USE NUMBERED LISTS FOR:**
1. **Top Cross-Dimensional Pattern**: Premium suppliers in developed markets show **$95-125/hr** ranges
2. **Secondary Integration**: Value providers in emerging markets offer **$25-45/hr** competitive rates  
3. **Supporting Synthesis**: Specialized roles command **15-30% premium** across all supplier categories

**✅ USE BULLET POINTS FOR:**
- **Cross-Query Insights**: Multiple analyses confirm regional rate variations
- **Dimensional Relationships**: Supplier tier strongly correlates with geographical presence
  - Premium suppliers concentrate in North America and Europe
  - Value providers dominate Asia-Pacific markets
- **Synthesis Conclusions**: Market segmentation follows predictable patterns

### WRITING GUIDELINES:
- **Conversational but Comprehensive**: Write like you're explaining complex multi-dimensional analysis to a colleague
- **Data-Rich**: Include specific numbers, percentages, ranges, and examples from ALL analytical results
- **Bold Key Information**: Use **bold formatting** for important rates, key findings, significant trends, and critical insights
- **Explanatory**: Don't just state numbers - explain what they mean and why they matter across dimensions
- **Integrated**: Weave together findings from multiple queries into a cohesive narrative
- **Engaging**: Use natural language that flows well while being thorough
- **Specific**: Always use actual data values from ALL analytical results
- **Range-Focused**: Present monetary values and rates as meaningful ranges rather than precise individual values. NEVER list individual values when ranges are more appropriate
- **Anti-Repetition**: Each section should provide unique insights - do not repeat the same information across sections

### CRITICAL RULES:
- NEVER use placeholder text - always use actual data values from ALL analytical results
- Every insight must be backed by specific data from the results
- Use ALL the analytical results provided - don't summarize or skip any data
- Write in a conversational, chatbot-like style while being comprehensive
- Synthesize across all analytical dimensions and queries
- **USE BOLD FORMATTING** for key rates, important findings, and critical insights
- **NO FINAL SUMMARY**: Do not add a concluding summary that repeats information already covered
- **AVOID REPETITION**: Each section must provide unique insights - do not repeat the same information across sections
- **CRITICAL - NO INDIVIDUAL VALUE LISTINGS**: When multiple similar values exist, present as ranges rather than listing individual values. Avoid phrases like "specific averages calculated as follows:" followed by value lists
- **SMART TABLE USAGE**: Only use tables when they genuinely improve cross-dimensional comparison
- **STRATEGIC FORMATTING**: Use markdown elements to enhance synthesis understanding, not just for decoration

### FORMATTING DECISION MATRIX FOR SYNTHESIS:

**Use Tables When:**
- Comparing findings across 3+ analytical dimensions
- Showing multiple metrics from different queries side-by-side
- Cross-dimensional trend analysis
- Integrated supplier/geographical/temporal comparisons

**Use Bullet Points When:**
- Synthesizing key insights from multiple queries
- Ranking integrated findings
- Breaking down complex cross-dimensional patterns
- Providing supporting evidence from different analyses

**Use Headers When:**
- Organizing major synthesis sections
- Separating different analytical integration areas
- Creating logical flow for multi-dimensional insights

**Use Bold/Emphasis When:**
- Highlighting cross-dimensional rates or patterns
- Emphasizing synthesis findings
- Drawing attention to integrated insights
- NOT for entire sentences or paragraphs

### RANGE PRESENTATION EXAMPLES:
**Multiple analytical results scenario:**
- If analytical results show: $112.50, $87.77, $70.85, $72.41
- ❌ Wrong: "The average rates are $112.50, $87.77, $70.85, and $72.41"
- ❌ Wrong: "This range is derived from multiple analyses, with specific averages calculated as follows: $112.50, $87.77, $70.85, $72.41"
- ✅ Correct: "SAP Developer rates range from **$70-113 per hour** across different analyses"

**Single average scenario (user didn't ask for "average"):**
- If result shows: Average = $75.50 for "rates for Developer in IND"
- ❌ Wrong: "The average rate is $75.50"
- ✅ Correct: "Developer rates in India typically range from **$70-80 per hour**"

**CRITICAL - DO NOT LIST INDIVIDUAL VALUES:**
- ❌ NEVER write: "with specific averages calculated as follows:" followed by a list
- ❌ NEVER write: "The individual rates from different analyses are: $X, $Y, $Z"
- ❌ NEVER write: "derived from multiple analyses, with specific averages calculated as follows:"
- ✅ ALWAYS consolidate into ranges: "ranging from **$X-Y per hour** across different analyses"

### OUTPUT FORMAT:
Provide a comprehensive, conversational analysis that reads like an expert chatbot explaining complex multi-dimensional data with beautiful markdown formatting. Use clear structure with appropriate tables, bullet points, headers, and emphasis to create an engaging and easy-to-read synthesis. Every formatting choice should enhance understanding of cross-dimensional relationships and integrated insights."""),
            ("human", "### ORIGINAL USER QUERY:\n{user_query}\n\n### COMPREHENSIVE ANALYTICAL RESULTS:\n{analytical_results}\n\nSynthesize ALL analytical results into a comprehensive, conversational analysis with beautiful markdown formatting. Use tables for cross-dimensional comparisons, bullet points for integrated insights, headers for synthesis structure, and bold formatting for key findings. Choose the most appropriate markdown elements based on the complexity and relationships in the data. Act like a knowledgeable expert explaining complex multi-dimensional data in an accessible, visually appealing way that weaves together all analytical findings.")
        ]) 

    def _create_query_planning_prompt(self) -> ChatPromptTemplate:
        """Create the query planning prompt"""
        query_planning_system = """You are an intelligent query planning assistant for a database analysis system.

Your job is to analyze user questions and determine whether multiple exploratory queries would provide better insights than a single query.

GUIDELINES FOR DECISION:
- Simple questions (basic counts, lookups) → Single query
- Complex analytical questions → Multiple queries
- Questions asking for comparisons → Multiple queries
- Questions with multiple dimensions → Multiple queries
- Questions requiring deep analysis → Multiple queries

SCHEMA CONTEXT:
{schema}

Respond with JSON:
{{
  "needs_multiple_queries": true/false,
  "reasoning": "Brief explanation of decision",
  "suggested_explorations": ["column1", "column2"] // if multiple queries needed
}}"""

        query_planning_human = """Question: {question}

Analyze this question and determine if multiple exploratory queries would provide better insights than a single query."""

        return ChatPromptTemplate.from_messages([
            ("system", query_planning_system),
            ("human", query_planning_human)
        ])

    def _create_query_scoring_prompt(self) -> ChatPromptTemplate:
        """Create the query scoring prompt"""
        query_scoring_system = """You are a query result quality assessor for a database analysis system.

Your job is to score query results based on their relevance, data quality, and usefulness for answering the original question.

SCORING CRITERIA (0-100):
- Relevance to original question (40%)
- Data completeness and quality (30%) 
- Insights potential (20%)
- Statistical significance (10%)

CONSIDERATIONS:
- Results with more data points are generally better
- Results that directly answer the question score higher
- Empty results or obvious outliers score lower
- Results showing clear patterns or trends score higher

ORIGINAL QUESTION: {original_question}

Analyze each query result and provide scores. Respond with JSON:
{{
  "scores": [
    {{
      "query_description": "exact description from input",
      "score": 85,
      "reasoning": "why this score",
      "key_insights": ["insight1", "insight2"]
    }}
  ],
  "best_query_index": 0,
  "overall_assessment": "summary of result quality"
}}"""

        query_scoring_human = """Query Results to Score:
{query_results}

Score each query result for quality and relevance to the original question."""

        return ChatPromptTemplate.from_messages([
            ("system", query_scoring_system),
            ("human", query_scoring_human)
        ])
    
    def _create_contextual_query_generation_prompt(self) -> ChatPromptTemplate:
        """Create the contextual query generation prompt"""
        system_message = """You are an expert SQL query generator who specializes in creating contextually relevant database queries. Your job is to generate 3-5 specific SQL queries that will help answer the user's question using the available database schema.

### DATABASE SCHEMA:
{schema}

### CRITICAL INSTRUCTIONS:
1. CONTEXT-AWARE: Generate queries that are directly relevant to the user's question and will provide the specific information needed to answer it.

2. SCHEMA-BASED: Use the actual column names and table structure from the schema. Pay attention to enum values and column types.

3. DIVERSE APPROACHES: Generate different types of queries (averages, counts, comparisons, rankings) that together provide comprehensive insights.

4. SPECIFIC FILTERING: Use appropriate WHERE clauses based on the user's question to filter for relevant data.

5. **CRITICAL - ENTITY FOCUS**: If the user asks about specific entities (e.g., "Developers", "SAP Consultants", "Project Managers"), ALL generated queries must focus ONLY on those specific entities or closely related roles. NEVER expand to broader categories like "all roles" or unrelated job types.

**COMPOUND ENTITY FILTERING**: For compound entities like "SAP Developer" or "Java Consultant", filter by BOTH the specialization AND the role type. Do NOT filter only by specialization and return all roles within that specialization.

6. **AVOID FREQUENCY DISTRIBUTIONS**: NEVER generate queries that return individual value frequencies or distributions unless the user EXPLICITLY asks for distribution analysis. Focus on aggregated insights instead.

7. **RATE RELATED QUERIES**: If the user asks for rates, you MUST use the hourly rate column (like hourly_rate_in_usd) and not the bill rate column (like bill_rate_hourly).

### QUERY TYPES TO PREFER:
- Aggregated values (AVG, SUM, COUNT of groups)
- Comparisons between categories (GROUP BY with aggregations)
- Rankings or top/bottom N results
- **SUPPLIER/VENDOR ANALYSIS**: Comparative analysis across suppliers/vendors/partners (high priority)
- **YEARWISE TRENDS**: Year-over-year analysis for the past 2-3 years (2022-2024) where applicable
- Geographical comparisons (countries, regions)
- Role-based analysis and comparisons
- **AVOID**: Industry/sector analysis unless explicitly requested by user

### QUERY TYPES TO AVOID (unless explicitly requested):
- Individual value frequencies (value, COUNT(*) GROUP BY value)
- Distribution queries that return many individual data points
- Queries that return long lists of individual values with their counts

### ENTITY FOCUS EXAMPLES:

**✅ CORRECT ENTITY FOCUS:**
Question: "What is the average hourly rate for Developers in India?"
Good Queries (all focus on Developers only):
- SELECT AVG(hourly_rate_in_usd) FROM public."IT_Professional_Services" WHERE country_of_work = 'IND' AND normalized_role_title = 'Developer/Programmer'
- SELECT AVG(hourly_rate_in_usd) FROM public."IT_Professional_Services" WHERE country_of_work = 'IND' AND role_title_group = 'Application Design & Programming/Deployment'

**❌ WRONG ENTITY FOCUS:**
Question: "What is the average hourly rate for Developers in India?"
Bad Query (expands beyond Developers):
- SELECT normalized_role_title, AVG(hourly_rate_in_usd) as avg_rate FROM public."IT_Professional_Services" WHERE country_of_work = 'IND' GROUP BY normalized_role_title ORDER BY avg_rate DESC

**✅ CORRECT ENTITY FOCUS:**
Question: "How much do SAP Consultants earn?"
Good Queries (focus on SAP consultants only):
- SELECT AVG(hourly_rate_in_usd) FROM public."IT_Professional_Services" WHERE role_specialization = 'SAP' AND (normalized_role_title LIKE '%Consultant%' OR role_title_from_supplier LIKE '%Consultant%')
- SELECT normalized_role_title, AVG(hourly_rate_in_usd) as avg_rate FROM public."IT_Professional_Services" WHERE role_specialization = 'SAP' AND (normalized_role_title LIKE '%Consultant%' OR role_title_from_supplier LIKE '%Consultant%') GROUP BY normalized_role_title

**❌ WRONG ENTITY FOCUS:**
Question: "How much do SAP Consultants earn?"
Bad Query (returns ALL SAP roles, not just consultants):
- SELECT role_title_from_supplier, AVG(hourly_rate_in_usd) as avg_rate FROM public."IT_Professional_Services" WHERE role_specialization = 'SAP' GROUP BY role_title_from_supplier

**✅ CORRECT ENTITY FOCUS:**
Question: "Give me the rates for SAP Developer"
Good Queries (focus on SAP developers only):
- SELECT AVG(hourly_rate_in_usd) FROM public."IT_Professional_Services" WHERE role_specialization = 'SAP' AND (normalized_role_title LIKE '%Developer%' OR role_title_from_supplier LIKE '%Developer%')
- SELECT normalized_role_title, AVG(hourly_rate_in_usd) as avg_rate FROM public."IT_Professional_Services" WHERE role_specialization = 'SAP' AND (normalized_role_title LIKE '%Developer%' OR role_title_from_supplier LIKE '%Developer%') GROUP BY normalized_role_title

**❌ WRONG ENTITY FOCUS:**
Question: "Give me the rates for SAP Developer"
Bad Query (returns ALL SAP roles, not just developers):
- SELECT role_title_from_supplier, AVG(hourly_rate_in_usd) as avg_rate FROM public."IT_Professional_Services" WHERE role_specialization = 'SAP' GROUP BY role_title_from_supplier ORDER BY avg_rate DESC

### EXAMPLE SCENARIOS:

Question: What is the average hourly rate for Developers in India?
Good Queries:
- SELECT AVG(hourly_rate_in_usd) FROM public."IT_Professional_Services" WHERE country_of_work = 'IND' AND normalized_role_title = 'Developer/Programmer'
- SELECT AVG(hourly_rate_in_usd) FROM public."IT_Professional_Services" WHERE country_of_work = 'IND' AND role_title_group = 'Application Design & Programming/Deployment'

Question: How does the hourly rate for Developers in India compare to other countries?
Good Queries:
- SELECT country_of_work, AVG(hourly_rate_in_usd) as avg_rate FROM public."IT_Professional_Services" WHERE normalized_role_title = 'Developer/Programmer' GROUP BY country_of_work ORDER BY avg_rate DESC
- SELECT country_of_work, AVG(hourly_rate_in_usd) as avg_rate FROM public."IT_Professional_Services" WHERE role_title_group = 'Application Design & Programming/Deployment' GROUP BY country_of_work ORDER BY avg_rate DESC
- SELECT AVG(hourly_rate_in_usd) as india_avg FROM public."IT_Professional_Services" WHERE country_of_work = 'IND' AND normalized_role_title = 'Developer/Programmer'
- SELECT AVG(hourly_rate_in_usd) as usa_avg FROM public."IT_Professional_Services" WHERE country_of_work = 'IND' AND normalized_role_title = 'Developer/Programmer'

### QUERY GENERATION GUIDELINES:
1. Use appropriate column names from the schema (e.g., hourly_rate_in_usd, country_of_work, normalized_role_title)
2. Filter by relevant values mentioned in the question (e.g., 'IND' for India, 'Developer' for developers)
3. **CRITICAL - MAINTAIN ENTITY FOCUS**: If user mentions specific entities (roles, specializations), ALL queries must filter to include ONLY those entities. Never generate broad queries that return unrelated roles.

**COMPOUND ENTITY RULE**: For requests like "SAP Developer" or "Java Consultant", filter by BOTH the specialization (e.g., role_specialization = 'SAP') AND the role type (e.g., role_title LIKE '%Developer%'). Do NOT filter only by specialization and return all roles within that specialization.
4. Generate different query types (averages, counts, comparisons, grouping) BUT avoid frequency distributions
5. **CRITICAL - TABLE NAMING**: Always use schema-qualified and quoted table names like `public."TableName"` to avoid PostgreSQL case-sensitivity issues. NEVER use unquoted table names.
6. Use proper PostgreSQL syntax with correct table references
7. Include meaningful descriptions that explain what each query does
8. **COLUMN PRIORITY**: When there are multiple columns that could answer the question, prefer columns marked as [MUST_HAVE] over others, then [IMPORTANT] columns, then [MANDATORY] columns. For example, prefer "hourly_rate_in_usd [MUST_HAVE]" over "bill_rate_hourly" when user asks for rates.
9. **DESCRIPTION AWARENESS**: Use the column descriptions provided in the schema to better understand what each column represents and choose the most appropriate column for the user's question.
10. **AGGREGATED FOCUS**: Focus on queries that produce aggregated insights rather than individual value distributions.
11. **SUPPLIER ANALYSIS PRIORITY**: When generating queries for business analysis, prioritize supplier/vendor/partner comparisons and analysis. This provides more actionable business insights than industry analysis.
12. **YEARWISE TRENDS PRIORITY**: Include year-over-year analysis for the past 2-3 years (2022-2024) where applicable to show temporal trends and changes.
13. **AVOID INDUSTRY ANALYSIS**: Do NOT generate industry/sector analysis queries unless the user explicitly requests industry insights.
14. **EXACT VALUES FROM EXPLORATION**: If the schema contains "COLUMN EXPLORATION RESULTS" with actual database values, you MUST use those exact values without any expansion, interpretation, or modification. For example, if you see "BI Developer" in the exploration results, use exactly "BI Developer" in your WHERE clause, NOT "Business Intelligence Developer".

15. **CRITICAL - AVOID LIKE WHEN EXACT VALUES EXIST**: When "COLUMN EXPLORATION RESULTS" section provides exact values for a column, you MUST use exact equality (=) operators, NOT LIKE patterns. Only use LIKE patterns when no exact values are available in the exploration results for the relevant concept.

### EXACT MATCH PRIORITY RULES:
- ✅ **PREFERRED**: `WHERE role_specialization = 'SAP'` (when 'SAP' is found in exploration results)
- ❌ **AVOID**: `WHERE role_title_from_supplier LIKE '%Developer%'` (when exact developer titles are available)
- ✅ **CORRECT**: `WHERE normalized_role_title = 'Developer/Programmer'` (using exact value from exploration)
- ❌ **WRONG**: `WHERE role_specialization LIKE '%SAP%'` (when 'SAP' exists as exact value)

### OUTPUT FORMAT:
Return a valid JSON object with a queries array. Each query should have sql, description, and type fields.
Example:
{{"queries": [{{"sql": "SELECT AVG(hourly_rate_in_usd) FROM public.\"IT_Professional_Services\" WHERE country_of_work = 'IND'", "description": "Average hourly rate for India", "type": "average"}}]}}

Do not include any explanatory text, markdown formatting, or code blocks outside the JSON."""
        
        human_message = """USER QUESTION: {question}

INSTRUCTIONS: Generate 3-5 contextually relevant SQL queries that will help answer this question. Use the actual column names and values from the database schema. Focus on queries that directly address what the user is asking for with aggregated insights, NOT individual value frequencies or distributions.

CRITICAL: If the user mentions specific entities (roles, specializations, job types), ALL queries must filter to include ONLY those specific entities. Do NOT generate broad queries that return unrelated roles or categories.

COMPOUND ENTITY FILTERING: For compound requests like "SAP Developer", "Java Consultant", or "Senior Manager", filter by BOTH parts - the specialization AND the role type. Never filter only by specialization and return all roles within that category.

SUPPLIER ANALYSIS PRIORITY: Prioritize supplier/vendor/partner analysis over industry analysis. Generate queries that compare suppliers, vendors, or partners unless the user explicitly requests industry analysis.

YEARWISE TRENDS PRIORITY: Include year-over-year analysis for the past 2-3 years (2022-2024) where applicable to show temporal trends and changes in the data."""
        
        return ChatPromptTemplate.from_messages([
            ("system", system_message),
            ("human", human_message)
        ]) 