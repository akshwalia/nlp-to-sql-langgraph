import re
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime


class SessionContextManager:
    """Manages session context for the SQL generator"""
    
    def __init__(self):
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
        
        # Data store for paginated results
        self.paginated_results = {}
    
    def update_session_context(self, question: str, sql: str, results: List[Dict]) -> None:
        """Update session context with new query information"""
        try:
            # Update query sequence
            self.session_context["query_sequence"].append({
                "question": question,
                "sql": sql,
                "timestamp": datetime.now().isoformat(),
                "result_count": len(results) if results else 0
            })
            
            # Keep only last 10 queries
            if len(self.session_context["query_sequence"]) > 10:
                self.session_context["query_sequence"] = self.session_context["query_sequence"][-10:]
            
            # Update last query result
            self.session_context["last_query_result"] = {
                "question": question,
                "sql": sql,
                "results": results[:5] if results else [],  # Keep first 5 results
                "total_count": len(results) if results else 0
            }
            
            # Extract and update important values
            important_values = self._extract_important_values(question, sql, results)
            self.session_context["important_values"].update(important_values)
            
            # Update entity mentions
            self._update_entity_mentions(question, results)
            
        except Exception as e:
            print(f"Error updating session context: {e}")
    
    def _extract_important_values(self, question: str, sql: str, results: List[Dict]) -> Dict[str, Any]:
        """Extract important values from query and results"""
        important_values = {}
        
        try:
            # Extract values from question
            # Numbers
            numbers = re.findall(r'\b\d+(?:\.\d+)?\b', question)
            if numbers:
                important_values["numbers_mentioned"] = numbers
            
            # Dates (basic patterns)
            date_patterns = [
                r'\b\d{4}-\d{2}-\d{2}\b',  # 2023-12-25
                r'\b\d{2}/\d{2}/\d{4}\b',  # 12/25/2023
                r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',  # 12/25/23
            ]
            
            for pattern in date_patterns:
                dates = re.findall(pattern, question)
                if dates:
                    important_values["dates_mentioned"] = dates
                    break
            
            # Extract IDs from results
            if results and isinstance(results, list):
                first_result = results[0] if results else {}
                if isinstance(first_result, dict):
                    # Look for ID columns
                    id_columns = [col for col in first_result.keys() if col.lower().endswith('id')]
                    for col in id_columns:
                        values = [str(row.get(col, '')) for row in results[:10]]  # First 10 values
                        important_values[f"{col}_values"] = values
                    
                    # Look for name columns
                    name_columns = [col for col in first_result.keys() if 'name' in col.lower()]
                    for col in name_columns:
                        values = [str(row.get(col, '')) for row in results[:10]]  # First 10 values
                        important_values[f"{col}_values"] = values
            
            # Extract conditions from SQL
            sql_conditions = self._extract_sql_conditions(sql)
            if sql_conditions:
                important_values["sql_conditions"] = sql_conditions
                
        except Exception as e:
            print(f"Error extracting important values: {e}")
        
        return important_values
    
    def _extract_sql_conditions(self, sql: str) -> str:
        """Extract WHERE conditions from SQL query"""
        try:
            # Simple pattern to extract WHERE clause
            where_match = re.search(r'WHERE\s+(.+?)(?:\s+(?:GROUP|ORDER|HAVING|LIMIT|;|$))', sql, re.IGNORECASE | re.DOTALL)
            if where_match:
                return where_match.group(1).strip()
            return ""
        except Exception as e:
            print(f"Error extracting SQL conditions: {e}")
            return ""
    
    def _update_entity_mentions(self, question: str, results: List[Dict]) -> None:
        """Update entity mentions from question and results"""
        try:
            # Extract entities from question (simple patterns)
            entities = {}
            
            # Extract quoted strings as potential entities
            quoted_strings = re.findall(r'"([^"]+)"', question)
            quoted_strings.extend(re.findall(r"'([^']+)'", question))
            
            if quoted_strings:
                entities["quoted_entities"] = quoted_strings
            
            # Extract capitalized words as potential entities
            capitalized_words = re.findall(r'\b[A-Z][a-z]+\b', question)
            if capitalized_words:
                entities["capitalized_entities"] = capitalized_words
            
            # Update session context
            self.session_context["entity_mentions"].update(entities)
            
        except Exception as e:
            print(f"Error updating entity mentions: {e}")
    
    def extract_sql_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL query"""
        try:
            # Simple pattern to extract table names
            # This is a basic implementation - could be improved
            table_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
            tables = re.findall(table_pattern, sql, re.IGNORECASE)
            
            # Also look for JOIN statements
            join_pattern = r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
            join_tables = re.findall(join_pattern, sql, re.IGNORECASE)
            
            all_tables = tables + join_tables
            return list(set(all_tables))  # Remove duplicates
            
        except Exception as e:
            print(f"Error extracting SQL tables: {e}")
            return []
    
    def prepare_session_context_for_query(self, question: str) -> str:
        """Prepare session context for query generation"""
        try:
            context_parts = []
            
            # Add user information
            if self.session_context["user_info"]:
                context_parts.append(f"User Info: {self.session_context['user_info']}")
            
            # Add recent query results for context
            if self.session_context["last_query_result"]:
                last_result = self.session_context["last_query_result"]
                context_parts.append(f"Previous Question: {last_result['question']}")
                context_parts.append(f"Previous SQL: {last_result['sql']}")
                
                # Add sample results
                if last_result["results"]:
                    context_parts.append(f"Previous Results (showing first 3):")
                    for i, row in enumerate(last_result["results"][:3]):
                        context_parts.append(f"  Row {i+1}: {row}")
            
            # Add important values
            if self.session_context["important_values"]:
                context_parts.append(f"Important Values: {self.session_context['important_values']}")
            
            # Add entity mentions
            if self.session_context["entity_mentions"]:
                context_parts.append(f"Entity Mentions: {self.session_context['entity_mentions']}")
            
            # Check for conversational references
            if self._is_conversational_question(question):
                context_parts.append("NOTE: This appears to be a follow-up question. Use previous context to understand references to 'this', 'that', 'these', 'those', etc.")
            
            return "\n".join(context_parts) if context_parts else ""
            
        except Exception as e:
            print(f"Error preparing session context: {e}")
            return ""
    
    def _is_conversational_question(self, question: str) -> bool:
        """Check if question is conversational (references previous context)"""
        conversational_indicators = [
            r'\bthis\b', r'\bthat\b', r'\bthese\b', r'\bthose\b',
            r'\bit\b', r'\bthey\b', r'\bthem\b',
            r'\bsame\b', r'\bsimilar\b', r'\blike that\b',
            r'\babove\b', r'\bbelow\b', r'\bprevious\b',
            r'\blast\b', r'\brecent\b', r'\bearlier\b',
            r'\bgive me more\b', r'\bshow me more\b',
            r'\bwhat about\b', r'\bhow about\b',
            r'\balso\b', r'\btoo\b', r'\bas well\b',
            r'\bother\b', r'\belse\b', r'\badditional\b'
        ]
        
        question_lower = question.lower()
        for indicator in conversational_indicators:
            if re.search(indicator, question_lower):
                return True
        return False
    
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
            "user_info": {},
            "query_sequence": [],
            "important_values": {},
            "last_query_result": None,
            "entity_mentions": {},
            "text_responses": [],
            "multi_query_results": []
        }
        self.paginated_results = {}
    
    def get_session_stats(self) -> Dict[str, Any]:
        """Get session statistics"""
        return {
            "total_queries": len(self.session_context["query_sequence"]),
            "user_info_items": len(self.session_context["user_info"]),
            "important_values_count": len(self.session_context["important_values"]),
            "entity_mentions_count": len(self.session_context["entity_mentions"]),
            "text_responses_count": len(self.session_context["text_responses"]),
            "paginated_tables": len(self.paginated_results)
        } 