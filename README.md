# NLP to SQL - Professional LangGraph & Python Application

A professional, modular NLP-to-SQL application built with LangGraph, FastAPI, and modern Python practices.

## 🏗️ Project Structure

```
NLP_TO_SQL/
├── src/                              # 📁 Main source code
│   ├── api/                          # 🚀 API Layer
│   │   ├── __init__.py
│   │   └── main.py                   # FastAPI application
│   ├── core/                         # 🧠 Core Business Logic
│   │   ├── __init__.py
│   │   ├── langgraph/               # 🔄 LangGraph Implementation
│   │   │   ├── __init__.py
│   │   │   └── sql_generator.py     # SmartSQL Generator with LangGraph
│   │   └── database/                # 🗄️ Database Operations
│   │       ├── __init__.py
│   │       ├── analyzer.py          # Database schema analysis
│   │       └── connection_manager.py # Connection pooling
│   ├── services/                     # 🔧 Business Services
│   │   ├── __init__.py
│   │   └── db_service.py            # Database service layer
│   ├── models/                       # 📊 Data Models
│   │   ├── __init__.py
│   │   └── schemas.py               # Pydantic models and MongoDB schemas
│   ├── auth/                         # 🔐 Authentication
│   │   ├── __init__.py
│   │   └── handlers.py              # JWT authentication and authorization
│   ├── vector_store/                 # 📚 Vector Store Management
│   │   ├── __init__.py
│   │   └── manager.py               # Chroma vector store with Gemini embeddings
│   ├── observability/                # 📈 Monitoring & Observability
│   │   ├── __init__.py
│   │   └── langfuse_config.py       # Langfuse integration
│   ├── utils/                        # 🛠️ Utilities
│   │   ├── __init__.py
│   │   └── (future utility modules)
│   └── __init__.py
├── config/                           # ⚙️ Configuration
│   ├── __init__.py
│   ├── settings.py                   # Application settings
│   └── env.example                   # Environment template
├── frontend/                         # 🎨 Next.js Frontend
│   └── (React/Next.js application)
├── docs/                             # 📖 Documentation
│   ├── README.md                     # Main documentation
│   ├── BACKEND_README.md             # Backend documentation
│   ├── FRONTEND_README.md            # Frontend documentation
│   ├── LANGFUSE_INTEGRATION_README.md
│   ├── CHART_FEATURES_README.md
│   ├── TRANSACTION_AND_SCHEMA_README.md
│   ├── LANGCHAIN_TO_LANGGRAPH_MIGRATION.md
│   └── deployment_guide.md
├── vector_stores/                    # 💾 Vector store persistence
├── memory_store/                     # 🧠 Memory persistence
├── main.py                           # 🚀 Application entry point
├── requirements.txt                  # 📦 Python dependencies
└── .gitignore                        # 🚫 Git ignore rules
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- SQLite database (built into Python)
- MongoDB
- Node.js 18+ (for frontend)

### 1. Environment Setup

```bash
# Copy environment template
cp config/env.example .env

# Edit .env with your configuration
nano .env
```

### 2. Install Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install frontend dependencies (optional)
cd frontend
npm install
cd ..
```

### 3. Run the Application

```bash
# Run the backend API
python main.py

# Run the frontend (in separate terminal)
cd frontend
npm run dev
```

## 🎯 Key Features

### 🧠 LangGraph SQL Generation
- **Smart SQL Generator**: Advanced AI-powered SQL generation using LangGraph
- **State Management**: Sophisticated conversation state tracking
- **Multi-query Support**: Complex analysis with multiple SQL queries
- **Auto-correction**: Intelligent SQL error detection and fixing

### 🔐 Authentication & Authorization
- **JWT-based Authentication**: Secure token-based auth system
- **Role-based Access Control**: Admin and viewer roles
- **Edit Mode**: Admin-only edit capabilities with confirmation

### 🗄️ Database Management
- **Connection Pooling**: Efficient database connection management
- **Multi-workspace Support**: Multiple database connections
- **Schema Analysis**: Automatic database schema discovery
- **Real-time Schema Updates**: Dynamic schema refresh

### 📚 Vector Store & Memory
- **Conversation Memory**: Persistent conversation context using Chroma
- **Gemini Embeddings**: Google's state-of-the-art embeddings
- **Context-aware Queries**: Intelligent query understanding with history

### 📈 Observability
- **Langfuse Integration**: Complete AI operation monitoring
- **Performance Tracking**: Query execution and AI performance metrics
- **Error Monitoring**: Comprehensive error tracking and logging

## 🔧 Module Overview

### Core Modules

#### `src.core.langgraph.sql_generator`
- **SmartSQLGenerator**: Main LangGraph-based SQL generation engine
- **State Management**: Conversation state and memory management
- **Multi-query Processing**: Complex analytical query handling

#### `src.core.database.analyzer`
- **DatabaseAnalyzer**: Schema analysis and metadata extraction
- **Relationship Discovery**: Automatic foreign key relationship detection
- **Performance Optimization**: Smart query optimization suggestions

#### `src.core.database.connection_manager`
- **Connection Pooling**: Multi-workspace database connection management
- **Health Monitoring**: Connection health and automatic recovery
- **Schema Caching**: Efficient schema information caching

### Service Layer

#### `src.services.db_service`
- **UserService**: User management and authentication
- **WorkspaceService**: Multi-tenant workspace management
- **SessionService**: Query session management
- **MessageService**: Chat message and context management

### API Layer

#### `src.api.main`
- **FastAPI Application**: RESTful API with automatic documentation
- **CORS Configuration**: Cross-origin resource sharing setup
- **Route Organization**: Logical API endpoint grouping

## 🌟 Architecture Benefits

### 1. **Separation of Concerns**
- Clear module boundaries and responsibilities
- Easy to test and maintain individual components
- Reduced coupling between different layers

### 2. **Scalability**
- Modular design allows for easy horizontal scaling
- Independent deployment of different components
- Clear interfaces between modules

### 3. **Maintainability**
- Professional Python package structure
- Consistent import patterns
- Clear documentation and type hints

### 4. **Extensibility**
- Easy to add new features and modules
- Plugin-like architecture for vector stores and AI models
- Configurable components through settings

## 🔧 Development

### Running Tests
```bash
# Run all tests
python -m pytest

# Run with coverage
python -m pytest --cov=src
```

### Code Quality
```bash
# Format code
black src/

# Lint code
flake8 src/

# Type checking
mypy src/
```

## 📚 Documentation

Detailed documentation is available in the `docs/` directory:

- **[Backend Documentation](docs/BACKEND_README.md)**: Complete backend API documentation
- **[Frontend Documentation](docs/FRONTEND_README.md)**: Frontend setup and development
- **[Deployment Guide](docs/deployment_guide.md)**: Production deployment instructions
- **[LangGraph Migration](docs/LANGCHAIN_TO_LANGGRAPH_MIGRATION.md)**: Migration from LangChain

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes following the existing code style
4. Add tests for new functionality
5. Update documentation as needed
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.