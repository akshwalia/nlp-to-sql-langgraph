"""
LangGraph SQL Generator Package

This package contains the modular components for the SmartSQLGenerator:
- state: SQL generator state management
- prompts: Prompt management and templates
- memory: Memory and conversation context management
- cache: Query caching functionality
- session_context: Session context management
- query_analysis: Query analysis and planning
- sql_generation: SQL generation and validation
- text_response: Text response generation
- execution: SQL execution management
- edit_operations: Edit mode operations
- multi_query: Multi-query analysis
- chart_recommendations: Chart recommendation generation
- graph: LangGraph graph management
- sql_generator: Main SQL generator class
"""

from .sql_generator import SmartSQLGenerator, SQLGenerator
from .state import SQLGeneratorState

# Export the main classes
__all__ = [
    "SmartSQLGenerator",
    "SQLGenerator",
    "SQLGeneratorState"
]

# Version information
__version__ = "2.0.0"
__author__ = "SQL Generator Team"
__description__ = "Modular AI-powered SQL query generator using LangGraph" 