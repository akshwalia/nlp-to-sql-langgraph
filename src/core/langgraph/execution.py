import time
from typing import Dict, Any, List
from src.observability.langfuse_config import observe_function


class ExecutionManager:
    """Manages SQL execution for the SQL generator"""
    
    def __init__(self, db_analyzer, session_context_manager):
        self.db_analyzer = db_analyzer
        self.session_context_manager = session_context_manager
    
    @observe_function("sql_query_execution")
    async def execute_query(self, question: str, sql: str, auto_fix: bool = True, max_attempts: int = 2) -> Dict[str, Any]:
        """Execute SQL query with error handling and auto-fix"""
        start_time = time.time()
        
        try:
            # Execute the query
            result = self._execute_single_query(sql, start_time)
            
            if result["success"]:
                # Update session context with successful execution
                self.session_context_manager.update_session_context(
                    question, sql, result["results"]
                )
                
                return {
                    "success": True,
                    "sql": sql,
                    "results": result["results"],
                    "execution_time": result["execution_time"],
                    "question": question,
                    "row_count": len(result["results"]) if result["results"] else 0
                }
            else:
                # Query failed
                return {
                    "success": False,
                    "sql": sql,
                    "results": [],
                    "error": result["error"],
                    "execution_time": result["execution_time"],
                    "question": question,
                    "row_count": 0
                }
                
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "sql": sql,
                "results": [],
                "error": f"Execution error: {str(e)}",
                "execution_time": execution_time,
                "question": question,
                "row_count": 0
            }
    
    def _execute_single_query(self, sql: str, start_time: float) -> Dict[str, Any]:
        """Execute a single SQL query"""
        try:
            # Execute query using the database analyzer
            # DatabaseAnalyzer.execute_query returns (success, results, error)
            success, results, error = self.db_analyzer.execute_query(sql)
            
            execution_time = time.time() - start_time
            
            if success:
                # Handle different result types
                if isinstance(results, list):
                    return {
                        "success": True,
                        "results": results,
                        "execution_time": execution_time,
                        "row_count": len(results)
                    }
                elif isinstance(results, dict):
                    # Handle single result
                    return {
                        "success": True,
                        "results": [results],
                        "execution_time": execution_time,
                        "row_count": 1
                    }
                else:
                    # Handle other result types
                    return {
                        "success": True,
                        "results": [{"result": results}] if results is not None else [],
                        "execution_time": execution_time,
                        "row_count": 1 if results is not None else 0
                    }
            else:
                # Query failed
                return {
                    "success": False,
                    "results": [],
                    "error": error or "Query execution failed",
                    "execution_time": execution_time,
                    "row_count": 0
                }
                
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "results": [],
                "error": str(e),
                "execution_time": execution_time,
                "row_count": 0
            }
    
 