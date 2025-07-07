"""
Application Configuration Settings
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project Root
PROJECT_ROOT = Path(__file__).parent.parent

# Database Configuration
DATABASE_CONFIG = {
    "default_host": os.getenv("DB_HOST", "localhost"),
    "default_port": int(os.getenv("DB_PORT", "5432")),
    "connection_timeout": int(os.getenv("DB_CONNECTION_TIMEOUT", "10")),
    "pool_size": int(os.getenv("DB_POOL_SIZE", "10")),
    "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "20")),
}

# MongoDB Configuration
MONGODB_CONFIG = {
    "uri": os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    "database": os.getenv("MONGO_DATABASE", "nlp_sql"),
}

# Azure OpenAI Configuration
AZURE_OPENAI_CONFIG = {
    "endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
    "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
    "api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
    "deployment_name": os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4"),
}

# Google AI Configuration
GOOGLE_AI_CONFIG = {
    "api_key": os.getenv("GOOGLE_API_KEY"),
    "embedding_model": os.getenv("GOOGLE_EMBEDDING_MODEL", "models/embedding-001"),
}

# Langfuse Configuration
LANGFUSE_CONFIG = {
    "secret_key": os.getenv("LANGFUSE_SECRET_KEY"),
    "public_key": os.getenv("LANGFUSE_PUBLIC_KEY"),
    "host": os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com"),
}

# JWT Configuration
JWT_CONFIG = {
    "secret_key": os.getenv("SECRET_KEY", "your-secret-key-for-jwt"),
    "algorithm": "HS256",
    "access_token_expire_minutes": int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440")),  # 24 hours
}

# Vector Store Configuration
VECTOR_STORE_CONFIG = {
    "base_dir": os.getenv("VECTOR_STORE_DIR", str(PROJECT_ROOT / "vector_stores")),
    "collection_prefix": "session_",
}

# Memory Store Configuration
MEMORY_STORE_CONFIG = {
    "base_dir": os.getenv("MEMORY_STORE_DIR", str(PROJECT_ROOT / "memory_store")),
    "cache_file": os.getenv("QUERY_CACHE_FILE", str(PROJECT_ROOT / "query_cache.json")),
}

# API Configuration
API_CONFIG = {
    "title": "NLP to SQL API",
    "description": "API for natural language to SQL conversion with multi-user support",
    "version": "2.0.0",
    "cors_origins": os.getenv("CORS_ORIGINS", "*").split(","),
    "host": os.getenv("API_HOST", "0.0.0.0"),
    "port": int(os.getenv("API_PORT", "8000")),
}

# Session Configuration
SESSION_CONFIG = {
    "timeout_minutes": int(os.getenv("SESSION_TIMEOUT", "60")),  # 1 hour
    "cleanup_interval_minutes": int(os.getenv("SESSION_CLEANUP_INTERVAL", "15")),  # 15 minutes
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
    "file_path": os.getenv("LOG_FILE", str(PROJECT_ROOT / "logs" / "app.log")),
}

# Environment
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DEBUG = os.getenv("DEBUG", "False").lower() == "true" 