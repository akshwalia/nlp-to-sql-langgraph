# 🏗️ **NLP-to-SQL LangGraph Agent Architecture**

A comprehensive guide to understand and replicate our production-ready NLP-to-SQL agent architecture.

## **📊 High-Level Architecture Overview**

Our system is a **modular, production-ready NLP-to-SQL agent** built with **LangGraph** as the core orchestration engine. It follows a **microservices-inspired modular design** with clear separation of concerns.

## **🎯 System Architecture Diagram**

```mermaid
graph TB
    subgraph "Frontend Layer"
        UI[Next.js React Frontend]
        UI --> ChatBot[ChatBot Component]
        UI --> Dashboard[Dashboard Component]
        UI --> Auth[Auth Component]
        ChatBot --> SqlResult[SqlResult Component]
        ChatBot --> Visualization[Visualization Component]
        ChatBot --> InsightPanel[InsightPanel Component]
    end

    subgraph "API Layer"
        FastAPI[FastAPI Backend]
        FastAPI --> AuthAPI[Authentication Endpoints]
        FastAPI --> QueryAPI[Query Processing Endpoints]
        FastAPI --> SessionAPI[Session Management]
    end

    subgraph "Core LangGraph Engine"
        SmartSQL[SmartSQLGenerator]
        
        subgraph "LangGraph Components"
            GraphMgr[GraphManager]
            State[SQLGeneratorState]
            
            subgraph "Graph Nodes"
                RouteQuery[route_query]
                GenerateSQL[generate_sql]
                ValidateSQL[validate_sql]
                ExecuteQuery[execute_query]
                GenerateResponse[generate_response]
                AnalyticalFlow[analytical_workflow]
            end
        end
        
        subgraph "Modular Managers"
            PromptsMgr[PromptsManager]
            MemoryMgr[MemoryManager]
            CacheMgr[CacheManager]
            SQLGenMgr[SQLGenerationManager]
            ExecutionMgr[ExecutionManager]
            AnalyticalMgr[AnalyticalManager]
            TextResponseMgr[TextResponseManager]
            ChartMgr[ChartRecommendationsManager]
        end
    end

    subgraph "Database Layer"
        DBAnalyzer[DatabaseAnalyzer]
        ConnMgr[ConnectionManager]
        PoolMgr[PoolManager]
        WorkspaceMgr[WorkspaceManager]
        
        subgraph "Database Analysis"
            SchemaAnalyzer[SchemaAnalyzer]
            TableAnalyzer[TableAnalyzer]
            RelationshipAnalyzer[RelationshipAnalyzer]
        end
    end

    subgraph "Memory & Vector Store"
        ChromaDB[Chroma Vector Store]
        Embeddings[Gemini Embeddings]
        MemoryStore[Persistent Memory]
    end

    subgraph "External Services"
        AzureOpenAI[Azure OpenAI GPT-4]
        MongoDB[MongoDB Database]
        PostgreSQL[PostgreSQL Database]
        Langfuse[Langfuse Observability]
    end

    %% Connections
    UI --> FastAPI
    FastAPI --> SmartSQL
    SmartSQL --> GraphMgr
    GraphMgr --> State
    
    %% Graph flow
    RouteQuery --> GenerateSQL
    RouteQuery --> AnalyticalFlow
    GenerateSQL --> ValidateSQL
    ValidateSQL --> ExecuteQuery
    ExecuteQuery --> GenerateResponse
    
    %% Manager connections
    SmartSQL --> PromptsMgr
    SmartSQL --> MemoryMgr
    SmartSQL --> SQLGenMgr
    SmartSQL --> ExecutionMgr
    SmartSQL --> AnalyticalMgr
    
    %% Database connections
    ExecutionMgr --> DBAnalyzer
    DBAnalyzer --> SchemaAnalyzer
    DBAnalyzer --> TableAnalyzer
    DBAnalyzer --> ConnMgr
    
    %% External service connections
    PromptsMgr --> AzureOpenAI
    SQLGenMgr --> AzureOpenAI
    MemoryMgr --> ChromaDB
    ChromaDB --> Embeddings
    FastAPI --> MongoDB
    ExecutionMgr --> PostgreSQL
    SmartSQL --> Langfuse

    %% Styling
    classDef frontend fill:#e1f5fe
    classDef api fill:#f3e5f5
    classDef core fill:#e8f5e8
    classDef database fill:#fff3e0
    classDef memory fill:#fce4ec
    classDef external fill:#f0f4c3

    class UI,ChatBot,Dashboard,Auth,SqlResult,Visualization,InsightPanel frontend
    class FastAPI,AuthAPI,QueryAPI,SessionAPI api
    class SmartSQL,GraphMgr,State,RouteQuery,GenerateSQL,ValidateSQL,ExecuteQuery,GenerateResponse,AnalyticalFlow,PromptsMgr,MemoryMgr,CacheMgr,SQLGenMgr,ExecutionMgr,AnalyticalMgr,TextResponseMgr,ChartMgr core
    class DBAnalyzer,ConnMgr,PoolMgr,WorkspaceMgr,SchemaAnalyzer,TableAnalyzer,RelationshipAnalyzer database
    class ChromaDB,Embeddings,MemoryStore memory
    class AzureOpenAI,MongoDB,PostgreSQL,Langfuse external
```

## **🔄 Request Lifecycle Flow**

```mermaid
sequenceDiagram
    participant User
    participant Frontend
    participant FastAPI
    participant SmartSQL
    participant LangGraph
    participant LLM
    participant Database
    participant Memory
    participant Langfuse

    User->>Frontend: "Give me rates for SAP developers"
    Frontend->>FastAPI: POST /sessions/{id}/query
    FastAPI->>SmartSQL: process_unified_query()
    
    SmartSQL->>LangGraph: Run graph with state
    Note over LangGraph: SQLGeneratorState initialized
    
    LangGraph->>LangGraph: route_query_node()
    Note over LangGraph: Determines workflow type<br/>(conversational/analytical/standard)
    
    alt Analytical Workflow
        LangGraph->>LLM: Generate analytical questions
        LLM-->>LangGraph: 5-6 strategic questions
        
        loop For each question
            LangGraph->>SmartSQL: Column exploration
            SmartSQL->>Database: Explore column values
            Database-->>SmartSQL: Actual values
            
            LangGraph->>LLM: Generate SQL query
            LLM-->>LangGraph: Quartile-based SQL
            
            LangGraph->>Database: Execute SQL
            Database-->>LangGraph: Query results
        end
        
        LangGraph->>LLM: Synthesize comprehensive analysis
        LLM-->>LangGraph: Formatted tables + insights
    
    else Standard SQL Workflow
        LangGraph->>Memory: Get conversation context
        Memory-->>LangGraph: Previous context
        
        LangGraph->>LLM: Generate SQL query
        LLM-->>LangGraph: SQL query
        
        LangGraph->>Database: Execute SQL
        Database-->>LangGraph: Query results
        
        LangGraph->>LLM: Generate response
        LLM-->>LangGraph: Natural language response
    end
    
    LangGraph-->>SmartSQL: Final result
    SmartSQL->>Memory: Store conversation
    SmartSQL->>Langfuse: Log trace data
    SmartSQL-->>FastAPI: Return response
    FastAPI-->>Frontend: JSON response
    Frontend-->>User: Display tables + insights

    Note over User,Langfuse: All LLM calls, execution times,<br/>and errors are tracked in Langfuse
```

## **🔧 Core Components Breakdown**

### **1. Frontend Layer (Next.js/React)**

```typescript
// Key Components Structure:
frontend/
├── components/
│   ├── ChatBot.tsx           # Main chat interface (1,524 lines)
│   ├── SqlResult.tsx         # Table display with pagination (466 lines)
│   ├── Visualization.tsx     # Chart visualization (942 lines)
│   ├── InsightPanel.tsx      # AI-generated insights (420 lines)
│   ├── Dashboard.tsx         # Session management (331 lines)
│   ├── Auth.tsx              # Authentication (238 lines)
│   ├── SessionManager.tsx    # Session handling (292 lines)
│   └── SqlEditor.tsx         # Admin SQL editing (324 lines)
├── lib/
│   ├── api.ts                # API client functions
│   ├── authContext.tsx       # Authentication context
│   └── themeContext.tsx      # Theme management
└── app/
    ├── layout.tsx            # Root layout
    ├── page.tsx              # Landing page
    └── chatbot/
        └── page.tsx          # Main chat interface
```

**Key Features:**
- Real-time chat interface with message history
- Dynamic table visualization with pagination
- Interactive chart recommendations
- Session-based conversation management
- JWT authentication with role-based access
- Dark/light theme support
- Voice-to-text integration

### **2. API Layer (FastAPI)**

```python
# Main API Structure:
src/api/main.py

# Key Endpoints:
POST   /auth/login                    # User authentication
POST   /auth/register                 # User registration
GET    /sessions                      # List user sessions
POST   /sessions                      # Create new session
POST   /sessions/{session_id}/query   # Main query processing
GET    /sessions/{session_id}/messages # Get session messages
POST   /admin/users/search           # Admin user management
```

**Core Features:**
- JWT authentication with role-based access (admin/viewer)
- Session-based conversation tracking
- Query routing and processing
- Result formatting and pagination
- Error handling and logging
- CORS configuration for frontend
- MongoDB integration for user/session data

```python
# Main Query Processing Example:
@app.post("/sessions/{session_id}/query")
async def query_with_session(
    session_id: str, 
    query_req: QueryRequest,
    current_user: User = Depends(get_current_active_user)
):
    # Initialize or get SQL generator for session
    if session_id not in active_generators:
        sql_generator = SmartSQLGenerator(
            use_memory=True,
            memory_persist_dir=f"./memory_store/session_{session_id}",
            use_cache=True,
            cache_file=f"query_cache_{session_id}.json"
        )
        active_generators[session_id] = sql_generator
    
    # Process query with unified workflow
    result = await sql_generator.process_unified_query(
        question=query_req.question,
        user_role=current_user.role.value,
        edit_mode_enabled=(current_user.role == UserRole.ADMIN)
    )
    
    return result
```

### **3. Core LangGraph Engine**

This is the **heart of the system** - here's the detailed breakdown:

#### **A. State Management**

```python
# src/core/langgraph/state.py
from typing import Dict, List, Optional, Any
from typing_extensions import TypedDict

class SQLGeneratorState(TypedDict):
    """State for the SQL generator graph"""
    question: str                    # User's original question
    schema: str                      # Database schema context
    examples: str                    # Example SQL patterns
    memory: str                      # Conversation memory context
    sql: str                         # Generated SQL query
    results: List[Dict]              # Query execution results
    error: Optional[str]             # Error message if any
    response: str                    # Natural language response
    validation_attempts: int         # SQL validation retry count
    
    # Workflow classification
    is_conversational: bool          # True if no SQL needed
    requires_analysis: bool          # True if complex analysis needed
    workflow_type: str              # "conversational", "analytical", "standard", "error"
    
    # Analytical workflow data
    analytical_questions: List[Dict[str, Any]]  # Generated strategic questions
    analytical_results: List[Dict[str, Any]]    # Results from multiple queries
    comprehensive_analysis: str                 # Final synthesized analysis
```

#### **B. Graph Structure (Core Innovation)**

```python
# src/core/langgraph/graph.py
class GraphManager:
    """Manages the LangGraph for SQL generation"""
    
    def create_graph(self) -> StateGraph:
        """Create the LangGraph for SQL generation"""
        graph = StateGraph(SQLGeneratorState)
        
        # Add core nodes
        graph.add_node("route_query", self._route_query_node)
        graph.add_node("handle_conversational", self._handle_conversational_node)
        graph.add_node("generate_sql", self._generate_sql_node)
        graph.add_node("validate_sql", self._validate_sql_node)
        graph.add_node("generate_response", self._generate_response_node)
        graph.add_node("handle_error", self._handle_error_node)
        
        # Add analytical workflow nodes (our secret sauce)
        graph.add_node("generate_analytical_questions", self._generate_analytical_questions_node)
        graph.add_node("execute_analytical_workflow", self._execute_analytical_workflow_node)
        graph.add_node("generate_comprehensive_analysis", self._generate_comprehensive_analysis_node)
        
        # Define routing logic
        graph.add_edge(START, "route_query")
        graph.add_conditional_edges(
            "route_query",
            self._route_decision,
            {
                "conversational": "handle_conversational",
                "analytical": "generate_analytical_questions",
                "standard": "generate_sql",
                "error": "handle_error"
            }
        )
        
        # Standard SQL workflow
        graph.add_conditional_edges(
            "generate_sql",
            self._should_validate,
            {
                "validate": "validate_sql",
                "respond": "generate_response",
                "error": "handle_error"
            }
        )
        
        # Analytical workflow (our innovation)
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
        
        # Terminal nodes
        graph.add_edge("handle_conversational", END)
        graph.add_edge("generate_response", END)
        graph.add_edge("generate_comprehensive_analysis", END)
        graph.add_edge("handle_error", END)
        
        return graph.compile(checkpointer=self.checkpointer)
```

#### **C. Modular Manager System (Key Innovation)**

Instead of a monolithic class, we use **13 specialized managers**:

```python
# src/core/langgraph/ - Modular Architecture

1. PromptsManager (prompts.py - 1,248 lines)
   - All LLM prompts and templates
   - Context-aware prompt generation
   - Memory integration for prompts

2. MemoryManager (memory.py - 273 lines)
   - Chroma vector store integration
   - Conversation context storage/retrieval
   - Semantic similarity search

3. CacheManager (cache.py - 73 lines)
   - Query result caching
   - Performance optimization
   - Cache invalidation strategies

4. SQLGenerationManager (sql_generation.py - 1,042 lines)
   - Core SQL generation logic
   - Column exploration and value discovery
   - Context-aware SQL creation

5. ExecutionManager (execution.py - 221 lines)
   - Database query execution
   - Error handling and retry logic
   - Result formatting

6. AnalyticalManager (analytical_manager.py - 1,652 lines)
   - Multi-query analytical workflows
   - Strategic question generation
   - Comprehensive analysis synthesis

7. TextResponseManager (text_response.py - 348 lines)
   - Natural language response generation
   - Result summarization
   - Conversational handling

8. ChartRecommendationsManager (chart_recommendations.py - 418 lines)
   - Visualization suggestions
   - Chart type recommendations
   - Data-driven insights

9. SessionContextManager (session_context.py - 305 lines)
   - Session state management
   - Context tracking
   - User preference storage

10. QueryAnalyzer (query_analysis.py - 198 lines)
    - Query classification
    - Intent detection
    - Workflow routing decisions

11. EditOperationsManager (edit_operations.py - 212 lines)
    - Admin edit mode functionality
    - SQL modification capabilities
    - Change tracking

12. GraphManager (graph.py - 468 lines)
    - LangGraph orchestration
    - Node management
    - Workflow coordination

13. Multi-Query Manager (multi_query.py - 121 lines)
    - Complex query decomposition
    - Result aggregation
    - Cross-query analysis
```

### **4. Analytical Workflow (Our Secret Sauce)**

This is what makes our system unique - a sophisticated multi-query analytical engine:

```python
# src/core/langgraph/analytical_manager.py
class AnalyticalManager:
    """Manages complex analytical workflows with multiple queries"""
    
    async def process_analytical_workflow(self, user_query: str) -> Dict[str, Any]:
        """Main analytical workflow orchestrator"""
        
        # Step 1: Generate 5-6 strategic analytical questions
        questions = await self.generate_analytical_questions(user_query, schema_context)
        """
        Example questions generated:
        1. "Which suppliers offer the most competitive rates for SAP developers?"
        2. "How do SAP developer rates vary across different geographic regions?"
        3. "What are the rate ranges by supplier for different seniority levels?"
        4. "How have SAP developer rates evolved over the past 2-3 years?"
        5. "Which suppliers provide the best cost-benefit ratio for SAP expertise?"
        """
        
        analytical_results = []
        
        # Step 2: Execute each question with intelligent planning
        for i, question_data in enumerate(questions):
            question = question_data.get("question", "")
            
            # Step 2a: Proactive column exploration
            identified_columns = await self.sql_generation_manager.identify_relevant_columns(question)
            exploration_results = await self.sql_generation_manager.proactive_column_exploration(
                question, identified_columns
            )
            
            # Step 2b: Generate enhanced context with actual DB values
            enhanced_context = self._build_enhanced_context(exploration_results, question)
            
            # Step 2c: Generate SQL queries using flexible approach
            generated_queries = await self._generate_flexible_queries(question, enhanced_context)
            
            # Step 2d: Execute queries and collect results
            for query_data in generated_queries:
                result = await self.execution_manager.execute_single_query(query_data["sql"])
                if result["success"]:
                    analytical_results.append({
                        "question": question,
                        "sql": query_data["sql"],
                        "description": query_data["description"],
                        "results": result["results"],
                        "execution_time": result["execution_time"]
                    })
        
        # Step 3: Synthesize comprehensive analysis
        comprehensive_analysis = await self._generate_comprehensive_analysis(
            user_query, analytical_results
        )
        
        return {
            "success": True,
            "query_type": "analysis",
            "is_multi_query": True,
            "analysis_type": "comprehensive_market_intelligence",
            "text": comprehensive_analysis,
            "tables": self._format_results_as_tables(analytical_results),
            "analytical_questions": questions,
            "total_queries_executed": len(analytical_results),
            "total_execution_time": sum(r["execution_time"] for r in analytical_results)
        }
```

**Key Innovation - Column Exploration:**

```python
async def proactive_column_exploration(self, question: str, columns: List[str]) -> Dict[str, Any]:
    """Explore actual database values for better SQL generation"""
    
    exploration_results = {}
    
    for column in columns:
        try:
            if column in ['supplier', 'supplier_company']:
                # Get actual supplier names
                query = f'SELECT DISTINCT "{column}" FROM public."IT_Professional_Services" LIMIT 20'
                result = await self.db_analyzer.execute_query(query)
                exploration_results[column] = [row[column] for row in result]
                
            elif column in ['country_of_work', 'country']:
                # Get actual country codes/names
                query = f'SELECT DISTINCT "{column}" FROM public."IT_Professional_Services" LIMIT 50'
                result = await self.db_analyzer.execute_query(query)
                exploration_results[column] = [row[column] for row in result]
                
            elif column in ['role_specialization']:
                # Get actual specializations
                query = f'SELECT DISTINCT "{column}" FROM public."IT_Professional_Services" WHERE "{column}" IS NOT NULL LIMIT 30'
                result = await self.db_analyzer.execute_query(query)
                exploration_results[column] = [row[column] for row in result]
                
        except Exception as e:
            logger.warning(f"Could not explore column {column}: {e}")
    
    return exploration_results
```

### **5. Database Layer**

```python
# src/core/database/ - Database Management Architecture

database/
├── __init__.py                    # Database analyzer factory
├── connection/
│   ├── cleanup_manager.py         # Connection cleanup
│   ├── pool_manager.py           # Connection pooling
│   └── workspace_manager.py      # Multi-database management
├── query/
│   ├── executor.py               # Query execution engine
│   ├── schema_updater.py         # Schema refresh logic
│   └── transaction_manager.py    # Transaction handling
└── analysis/
    ├── schema_analyzer.py        # Schema discovery
    ├── table_analyzer.py         # Table metadata analysis
    ├── relationship_analyzer.py  # Foreign key relationships
    └── single_table_analyzer.py  # Individual table analysis
```

**Key Features:**
- **Connection Pooling**: Efficient database connection management
- **Schema Discovery**: Automatic table/column analysis
- **Relationship Mapping**: Foreign key relationship detection
- **Performance Optimization**: Query optimization suggestions
- **Multi-Database Support**: Connect to multiple databases

```python
# Example: Database Analyzer Usage
class DatabaseAnalyzer:
    def __init__(self):
        self.schema_analyzer = SchemaAnalyzer()
        self.table_analyzer = TableAnalyzer()
        self.relationship_analyzer = RelationshipAnalyzer()
    
    async def analyze_complete_schema(self) -> Dict[str, Any]:
        """Complete database analysis"""
        return {
            "tables": await self.schema_analyzer.get_all_tables(),
            "relationships": await self.relationship_analyzer.get_relationships(),
            "statistics": await self.table_analyzer.get_table_statistics(),
            "columns": await self.schema_analyzer.get_column_details()
        }
```

### **6. Memory & Vector Store**

```python
# src/vector_store/manager.py & src/core/langgraph/memory.py

class MemoryManager:
    """Manages conversation memory using Chroma vector store"""
    
    def __init__(self, use_memory: bool = True, memory_persist_dir: str = "./memory_store"):
        self.use_memory = use_memory
        
        if self.use_memory:
            try:
                # Initialize Chroma with Gemini embeddings
                self.vector_store = Chroma(
                    embedding_function=GoogleGenerativeAIEmbeddings(
                        model="models/embedding-001",
                        google_api_key=os.getenv("GOOGLE_API_KEY")
                    ),
                    persist_directory=memory_persist_dir
                )
                logger.info(f"Memory initialized with persist directory: {memory_persist_dir}")
            except Exception as e:
                logger.warning(f"Could not initialize memory: {e}")
                self.use_memory = False
    
    def store_conversation(self, question: str, sql: str, results: List[Dict], response: str):
        """Store conversation in vector database"""
        if not self.use_memory:
            return
            
        try:
            # Create conversation document
            conversation_text = f"Question: {question}\nSQL: {sql}\nResponse: {response}"
            metadata = {
                "question": question,
                "sql": sql,
                "timestamp": datetime.now().isoformat(),
                "result_count": len(results) if results else 0
            }
            
            # Store in vector database
            self.vector_store.add_texts(
                texts=[conversation_text],
                metadatas=[metadata],
                ids=[f"conv_{uuid.uuid4()}"]
            )
            logger.info("Conversation stored in memory")
            
        except Exception as e:
            logger.error(f"Error storing conversation: {e}")
    
    def get_relevant_context(self, question: str, k: int = 3) -> str:
        """Retrieve relevant conversation context"""
        if not self.use_memory:
            return ""
            
        try:
            # Semantic search for relevant conversations
            docs = self.vector_store.similarity_search(question, k=k)
            
            if docs:
                context_parts = []
                for doc in docs:
                    context_parts.append(f"Previous: {doc.page_content}")
                
                return "\n\n".join(context_parts)
            
        except Exception as e:
            logger.error(f"Error retrieving context: {e}")
        
        return ""
```

### **7. Observability & Monitoring**

```python
# src/observability/langfuse_config.py

class LangfuseManager:
    """Centralized Langfuse observability management"""
    
    def __init__(self):
        self.langfuse = None
        self.callback_handler = None
        self._initialize_langfuse()
    
    def _initialize_langfuse(self):
        """Initialize Langfuse with environment variables"""
        try:
            secret_key = os.getenv("LANGFUSE_SECRET_KEY")
            public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
            host = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")
            
            if secret_key and public_key:
                self.langfuse = Langfuse(
                    secret_key=secret_key,
                    public_key=public_key,
                    host=host
                )
                
                # Initialize LangChain callback handler
                self.callback_handler = LangfuseCallbackHandler(
                    secret_key=secret_key,
                    public_key=public_key,
                    host=host
                )
                
                logger.info("Langfuse initialized successfully")
            else:
                logger.warning("Langfuse credentials not found")
                
        except Exception as e:
            logger.error(f"Failed to initialize Langfuse: {e}")

# Usage throughout the system:
@observe_function("sql_generation")
async def generate_sql(self, question: str) -> Dict[str, Any]:
    """All major functions are decorated for automatic tracing"""
    # Function implementation
    pass
```

## **📋 Project Structure**

```
nlp-to-sql-langgraph/
├── 📁 frontend/                     # Next.js Frontend
│   ├── components/                  # React components
│   ├── lib/                        # API clients & utilities
│   ├── app/                        # Next.js app router
│   └── types/                      # TypeScript definitions
│
├── 📁 src/                         # Python Backend
│   ├── api/                        # FastAPI application
│   │   └── main.py                 # API endpoints
│   │
│   ├── core/                       # Core business logic
│   │   ├── langgraph/              # LangGraph implementation
│   │   │   ├── sql_generator.py    # Main SmartSQL class
│   │   │   ├── graph.py            # LangGraph orchestration
│   │   │   ├── state.py            # State management
│   │   │   ├── prompts.py          # Prompt management (1,248 lines)
│   │   │   ├── analytical_manager.py # Multi-query analysis (1,652 lines)
│   │   │   ├── sql_generation.py   # SQL generation logic (1,042 lines)
│   │   │   ├── execution.py        # Query execution
│   │   │   ├── memory.py           # Vector memory management
│   │   │   ├── cache.py            # Query caching
│   │   │   ├── text_response.py    # NL response generation
│   │   │   ├── chart_recommendations.py # Visualization
│   │   │   ├── query_analysis.py   # Query classification
│   │   │   ├── session_context.py  # Session management
│   │   │   ├── edit_operations.py  # Admin edit mode
│   │   │   └── multi_query.py      # Multi-query handling
│   │   │
│   │   └── database/               # Database layer
│   │       ├── connection/         # Connection management
│   │       ├── query/             # Query execution
│   │       └── analysis/          # Schema analysis
│   │
│   ├── models/                     # Data models
│   │   └── schemas.py             # Pydantic models
│   │
│   ├── auth/                       # Authentication
│   │   └── handlers.py            # JWT auth logic
│   │
│   ├── services/                   # Business services
│   │   └── db_service.py          # Database services
│   │
│   ├── vector_store/              # Vector database
│   │   └── manager.py             # Chroma integration
│   │
│   └── observability/             # Monitoring
│       └── langfuse_config.py     # Langfuse integration
│
├── 📁 docs/                        # Documentation
│   ├── README.md                   # System overview
│   ├── LANGFUSE_INTEGRATION_README.md
│   ├── BACKEND_README.md
│   ├── FRONTEND_README.md
│   └── CHART_FEATURES_README.md
│
├── 📁 config/                      # Configuration
│   ├── settings.py                # App settings
│   └── env.example                # Environment template
│
├── requirements.txt                # Python dependencies
├── README.md                      # Project overview
└── ARCHITECTURE.md                # This file
```

## **🛠️ How to Replicate This Architecture**

### **Step 1: Set Up Core Dependencies**

```bash
# Backend Dependencies
pip install langgraph>=0.2.0
pip install langchain-openai
pip install fastapi uvicorn
pip install sqlalchemy psycopg2-binary
pip install pymongo motor
pip install chromadb
pip install google-generativeai
pip install langfuse
pip install python-jose[cryptography]
pip install python-multipart

# Frontend Dependencies
npx create-next-app@latest your-app --typescript --tailwind --eslint
cd your-app
npm install axios
npm install @types/react
npm install recharts  # For charts
npm install lucide-react  # Icons
```

### **Step 2: Implement State Management**

```python
# 1. Start with SQLGeneratorState TypedDict
# This is your graph's memory - design it carefully
from typing_extensions import TypedDict
from typing import Dict, List, Optional, Any

class YourAgentState(TypedDict):
    # Core fields
    user_input: str
    processed_query: str
    results: List[Dict]
    error: Optional[str]
    
    # Workflow control
    workflow_type: str  # "simple", "complex", "conversational"
    requires_multi_step: bool
    
    # Multi-step analysis
    sub_questions: List[Dict]
    sub_results: List[Dict]
    final_analysis: str
```

### **Step 3: Build Modular Managers**

```python
# 2. Create separate managers for each concern
# Example structure:

class PromptManager:
    """Handles all LLM prompts and templates"""
    def __init__(self):
        self.prompts = {}
    
    def get_prompt(self, prompt_type: str, **kwargs) -> str:
        # Return formatted prompt
        pass

class QueryManager:
    """Handles query generation and execution"""
    def __init__(self, prompt_manager, db_connector):
        self.prompt_manager = prompt_manager
        self.db_connector = db_connector
    
    async def generate_query(self, user_input: str) -> str:
        # Generate query using prompts
        pass

class AnalysisManager:
    """Handles complex multi-step analysis"""
    def __init__(self, query_manager, prompt_manager):
        self.query_manager = query_manager
        self.prompt_manager = prompt_manager
    
    async def process_complex_analysis(self, user_input: str) -> Dict:
        # Multi-step analysis workflow
        pass

# Key Principles:
# - Single Responsibility: Each manager has one clear purpose
# - Dependency Injection: Pass dependencies in constructor
# - Async/Await: Use async for all I/O operations
# - Error Handling: Comprehensive error handling in each manager
```

### **Step 4: Design LangGraph Flow**

```python
# 3. Create your LangGraph structure
from langgraph.graph import StateGraph, START, END

class GraphManager:
    def __init__(self, managers_dict):
        self.managers = managers_dict
        self.graph = self.create_graph()
    
    def create_graph(self) -> StateGraph:
        graph = StateGraph(YourAgentState)
        
        # Add nodes
        graph.add_node("classify_input", self._classify_node)
        graph.add_node("simple_processing", self._simple_node)
        graph.add_node("complex_analysis", self._complex_node)
        graph.add_node("generate_response", self._response_node)
        graph.add_node("handle_error", self._error_node)
        
        # Add routing
        graph.add_edge(START, "classify_input")
        graph.add_conditional_edges(
            "classify_input",
            self._route_decision,
            {
                "simple": "simple_processing",
                "complex": "complex_analysis",
                "error": "handle_error"
            }
        )
        
        # Add end connections
        graph.add_edge("simple_processing", "generate_response")
        graph.add_edge("complex_analysis", "generate_response")
        graph.add_edge("generate_response", END)
        graph.add_edge("handle_error", END)
        
        return graph.compile()
    
    async def _classify_node(self, state: YourAgentState) -> YourAgentState:
        """Classify the input and determine workflow"""
        # Your classification logic
        workflow_type = self._determine_workflow(state["user_input"])
        return {**state, "workflow_type": workflow_type}
    
    def _route_decision(self, state: YourAgentState) -> str:
        """Decide which path to take"""
        return state["workflow_type"]

# Key Patterns:
# - Conditional Routing: Use conditional edges for dynamic decisions
# - Error Handling: Always include error handling paths
# - State Immutability: Return new state, don't mutate existing
# - Async Nodes: All node functions should be async
```

### **Step 5: Implement Multi-Step Analysis (The Secret Sauce)**

```python
# 4. This is the most complex part - multi-step analytical workflow
class AnalyticalWorkflow:
    async def process_complex_query(self, user_input: str) -> Dict:
        # Step 1: Break down the query into sub-questions
        sub_questions = await self._generate_sub_questions(user_input)
        
        # Step 2: Execute each sub-question
        sub_results = []
        for question in sub_questions:
            # 2a: Explore data context
            context = await self._explore_data_context(question)
            
            # 2b: Generate specific query
            query = await self._generate_contextual_query(question, context)
            
            # 2c: Execute and collect results
            result = await self._execute_query(query)
            sub_results.append({
                "question": question,
                "query": query,
                "results": result
            })
        
        # Step 3: Synthesize all results into comprehensive analysis
        final_analysis = await self._synthesize_analysis(user_input, sub_results)
        
        return {
            "user_input": user_input,
            "sub_questions": sub_questions,
            "sub_results": sub_results,
            "final_analysis": final_analysis,
            "total_queries": len(sub_results)
        }
    
    async def _generate_sub_questions(self, user_input: str) -> List[str]:
        """Generate 3-5 strategic sub-questions"""
        prompt = f"""
        Given this user query: "{user_input}"
        Generate 3-5 strategic sub-questions that will help provide comprehensive analysis.
        Focus on different aspects like suppliers, geography, trends, etc.
        """
        # Use your LLM to generate sub-questions
        pass
    
    async def _explore_data_context(self, question: str) -> Dict:
        """Explore actual data values for better query generation"""
        # This is key - explore actual database values
        # to inform better query generation
        pass
```

### **Step 6: Add Memory and Context**

```python
# 5. Implement conversation memory
class MemoryManager:
    def __init__(self):
        # Use Chroma or any vector database
        self.vector_store = self._initialize_vector_store()
    
    def store_conversation(self, user_input: str, result: Dict):
        """Store conversation for future context"""
        conversation_text = f"User: {user_input}\nResult: {result}"
        self.vector_store.add_texts([conversation_text])
    
    def get_relevant_context(self, user_input: str) -> str:
        """Get relevant past conversations"""
        docs = self.vector_store.similarity_search(user_input, k=3)
        return "\n".join([doc.page_content for doc in docs])
```

## **🔑 Key Architectural Decisions**

### **1. Why LangGraph?**

**Advantages:**
- **Explicit State Management**: Every step has clear state
- **Conditional Routing**: Dynamic workflow decisions based on input
- **Error Recovery**: Built-in retry mechanisms and error handling
- **Observability**: Native monitoring and tracing support
- **Scalability**: Async support and efficient execution

**Comparison to alternatives:**
```python
# LangChain Sequential Chain (Old Way)
chain = LLMChain(llm=llm, prompt=prompt1) | LLMChain(llm=llm, prompt=prompt2)
result = chain.run(input)  # No state visibility, hard to debug

# LangGraph (New Way)
graph = StateGraph(MyState)
graph.add_node("step1", step1_function)
graph.add_node("step2", step2_function)
graph.add_conditional_edges("step1", router_function, {...})
result = await graph.ainvoke({"input": input})  # Full state visibility
```

### **2. Why Modular Design?**

**Benefits:**
- **Testability**: Each component can be unit tested independently
- **Scalability**: Easy to optimize individual components
- **Maintainability**: Clear separation of concerns
- **Reusability**: Managers can be reused across different projects
- **Team Development**: Different team members can work on different managers

**Example:**
```python
# Monolithic (Bad)
class BigSQLGenerator:
    def do_everything(self, input):
        # 4000+ lines of mixed concerns
        pass

# Modular (Good)
class SmartSQLGenerator:
    def __init__(self):
        self.prompt_manager = PromptManager()
        self.memory_manager = MemoryManager()
        self.sql_manager = SQLGenerationManager()
        self.execution_manager = ExecutionManager()
        # Each manager has single responsibility
```

### **3. Why Vector Memory?**

**Advantages:**
- **Semantic Search**: Find relevant conversations by meaning, not just keywords
- **Context Awareness**: Understand user intent based on conversation history
- **Scalability**: Efficient storage and retrieval even with large conversation histories
- **Personalization**: Remember user preferences and patterns

```python
# Traditional memory (Limited)
memory = {"last_query": "SELECT * FROM users"}

# Vector memory (Powerful)
memory.store("User asked about customer data from California")
relevant = memory.search("Show me West Coast customer information")
# Returns: Previous conversation about California customers
```

## **📈 Production Considerations**

### **Performance Optimizations**

```python
# 1. Connection Pooling
class ConnectionManager:
    def __init__(self):
        self.pool = asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=5,
            max_size=20,
            command_timeout=60
        )

# 2. Query Caching
class CacheManager:
    def __init__(self):
        self.cache = {}
        self.ttl = 3600  # 1 hour
    
    def get_cached_result(self, query_hash: str):
        # Return cached result if available and not expired
        pass

# 3. Async Operations
async def process_multiple_queries(queries: List[str]):
    tasks = [execute_query(q) for q in queries]
    results = await asyncio.gather(*tasks)
    return results
```

### **Observability & Monitoring**

```python
# Langfuse Integration Example
from langfuse.decorators import observe

@observe()
async def generate_sql(self, question: str):
    """All major functions are automatically traced"""
    trace_data = {
        "input": question,
        "model": "gpt-4",
        "user_id": self.current_user_id
    }
    
    # Function implementation
    result = await self.llm.ainvoke(prompt)
    
    # Automatic cost, latency, and quality tracking
    return result

# Custom Metrics
class MetricsManager:
    def track_query_execution(self, query_time: float, result_count: int):
        # Track custom metrics
        pass
    
    def track_user_satisfaction(self, rating: int):
        # Track user feedback
        pass
```

### **Security & Authentication**

```python
# JWT Authentication
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    """Validate JWT token and return user"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Role-based Access Control
@app.post("/admin/endpoint")
async def admin_only_endpoint(
    current_user: User = Depends(get_current_admin_user)
):
    # Only admins can access this endpoint
    pass

# SQL Injection Prevention
def sanitize_sql(sql: str) -> str:
    """Basic SQL injection prevention"""
    # Use parameterized queries instead of string concatenation
    # Validate SQL syntax
    # Check for dangerous patterns
    pass
```

## **🎯 Replication Strategy**

To build a similar agent in your project, follow this step-by-step approach:

### **Phase 1: Foundation (Week 1-2)**
1. **Set up LangGraph**: Start with basic graph structure
2. **Design State**: Plan your state management carefully
3. **Build Core Managers**: Create 3-4 essential managers
4. **Implement Basic Routing**: Simple conditional logic

### **Phase 2: Core Functionality (Week 3-4)**
1. **Add Query Generation**: Basic LLM integration
2. **Database Integration**: Connection and execution
3. **Error Handling**: Comprehensive error management
4. **Basic Memory**: Simple conversation storage

### **Phase 3: Advanced Features (Week 5-6)**
1. **Multi-Step Analysis**: Complex analytical workflows
2. **Vector Memory**: Semantic conversation context
3. **Data Exploration**: Dynamic database introspection
4. **Visualization**: Chart recommendations

### **Phase 4: Production Ready (Week 7-8)**
1. **Authentication**: JWT-based security
2. **Observability**: Monitoring and logging
3. **Performance**: Caching and optimization
4. **Frontend**: User interface

### **Key Success Factors:**

1. **Start Simple**: Begin with basic functionality, add complexity gradually
2. **Design for Modularity**: Each component should have a single responsibility
3. **Plan State Carefully**: Your state design affects everything else
4. **Implement Observability Early**: Monitoring is crucial for debugging
5. **Focus on User Experience**: The interface is as important as the backend
6. **Test Incrementally**: Test each component as you build it

## **📚 Additional Resources**

### **Documentation Files:**
- `docs/LANGFUSE_INTEGRATION_README.md` - Observability setup
- `docs/BACKEND_README.md` - Backend architecture details
- `docs/FRONTEND_README.md` - Frontend implementation guide
- `docs/CHART_FEATURES_README.md` - Visualization features

### **Key Libraries:**
- **LangGraph**: Graph-based LLM workflows
- **LangChain**: LLM abstractions and utilities
- **FastAPI**: Modern Python web framework
- **Next.js**: React-based frontend framework
- **Chroma**: Vector database for embeddings
- **Langfuse**: LLM observability platform

### **Environment Setup:**
```bash
# Backend Environment Variables
AZURE_OPENAI_ENDPOINT=your_endpoint
AZURE_OPENAI_API_KEY=your_key
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4
GOOGLE_API_KEY=your_gemini_key
LANGFUSE_SECRET_KEY=your_langfuse_secret
LANGFUSE_PUBLIC_KEY=your_langfuse_public
DATABASE_URL=postgresql://user:pass@host:port/db
MONGODB_URL=mongodb://user:pass@host:port/db

# Frontend Environment Variables
NEXT_PUBLIC_API_URL=http://localhost:8000
```

This architecture provides a **production-ready, scalable foundation** for building sophisticated NLP-to-SQL agents. The key innovation is the **analytical workflow system** that can handle complex, multi-faceted queries while maintaining conversation context and providing comprehensive insights.

The modular design ensures that you can adapt and extend the system for your specific use case while maintaining code quality and performance. 