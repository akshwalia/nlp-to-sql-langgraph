import uuid
from typing import Dict, List, Any
from datetime import datetime


class SessionContextManager:
    """Simplified session context manager focused on pagination and basic tracking"""
    
    def __init__(self):
        # Only track essential session data
        self.session_context = {
            "query_sequence": [],
            "last_query_result": None
        }
        
        # Data store for paginated results
        self.paginated_results = {}
    
    def update_session_context(self, question: str, sql: str, results: List[Dict]) -> None:
        """Update session context with new query information"""
        try:
            # Update query sequence (keep last 5 queries only)
            self.session_context["query_sequence"].append({
                "question": question,
                "sql": sql,
                "timestamp": datetime.now().isoformat(),
                "result_count": len(results) if results else 0
            })
            
            # Keep only last 5 queries for performance
            if len(self.session_context["query_sequence"]) > 5:
                self.session_context["query_sequence"] = self.session_context["query_sequence"][-5:]
            
            # Update last query result (keep first 3 results for basic context)
            self.session_context["last_query_result"] = {
                "question": question,
                "sql": sql,
                "results": results[:3] if results else [],
                "total_count": len(results) if results else 0
            }
            
        except Exception as e:
            print(f"Error updating session context: {e}")
    
    def get_paginated_results(self, table_id: str, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """Get paginated results for a table"""
        try:
            if table_id not in self.paginated_results:
                return {
                    "success": False,
                    "error": "Table ID not found",
                    "data": [],
                    "pagination": {
                        "current_page": page,
                        "page_size": page_size,
                        "total_pages": 0,
                        "total_items": 0
                    }
                }
            
            data = self.paginated_results[table_id]
            total_items = len(data)
            total_pages = (total_items + page_size - 1) // page_size
            
            # Calculate start and end indices
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            # Get page data
            page_data = data[start_idx:end_idx]
            
            return {
                "success": True,
                "data": page_data,
                "pagination": {
                    "current_page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "total_items": total_items,
                    "has_next": page < total_pages,
                    "has_previous": page > 1
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error getting paginated results: {str(e)}",
                "data": [],
                "pagination": {
                    "current_page": page,
                    "page_size": page_size,
                    "total_pages": 0,
                    "total_items": 0
                }
            }
    
    def store_paginated_results(self, results: List[Dict], table_id: str = None) -> str:
        """Store results for pagination and return table ID"""
        if not table_id:
            table_id = str(uuid.uuid4())
        
        self.paginated_results[table_id] = results
        return table_id
    
    def clear_session_context(self) -> None:
        """Clear session context"""
        self.session_context = {
            "query_sequence": [],
            "last_query_result": None
        }
        self.paginated_results = {}
    
    def get_session_stats(self) -> Dict[str, Any]:
        """Get session statistics"""
        return {
            "total_queries": len(self.session_context["query_sequence"]),
            "paginated_tables": len(self.paginated_results),
            "last_query_time": (
                self.session_context["query_sequence"][-1]["timestamp"] 
                if self.session_context["query_sequence"] 
                else None
            )
        } 