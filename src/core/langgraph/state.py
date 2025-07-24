from typing import Dict, List, Optional, Any
from typing_extensions import TypedDict


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

    
    # Simplified classification fields
    is_conversational: bool
    requires_analysis: bool
    workflow_type: str  # "conversational", "analytical", "standard", "error"
    
    # Analytical workflow fields
    analytical_questions: List[Dict[str, Any]]
    analytical_results: List[Dict[str, Any]]
    comprehensive_analysis: str 