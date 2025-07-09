from typing import Dict, List, Optional
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
    validation_attempts: int 