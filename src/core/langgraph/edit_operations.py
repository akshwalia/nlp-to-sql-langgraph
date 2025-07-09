import json
from typing import Dict, Any


class EditOperationsManager:
    """Manages edit operations for the SQL generator"""
    
    def __init__(self, prompts_manager, sql_generation_manager, llm):
        self.prompts_manager = prompts_manager
        self.sql_generation_manager = sql_generation_manager
        self.llm = llm
    
    def is_edit_operation(self, question: str) -> bool:
        """Check if question is requesting an edit operation (INSERT, UPDATE, DELETE)"""
        import re
        
        edit_indicators = [
            # INSERT operations
            r'\badd\b', r'\binsert\b', r'\bcreate\b', r'\bnew\b',
            r'\bregister\b', r'\bsign\s+up\b', r'\benroll\b',
            
            # UPDATE operations
            r'\bupdate\b', r'\bmodify\b', r'\bchange\b', r'\bedit\b',
            r'\bfix\b', r'\bcorrect\b', r'\badjust\b', r'\bset\b',
            
            # DELETE operations
            r'\bdelete\b', r'\bremove\b', r'\bdrop\b', r'\bcancel\b',
            r'\bunregister\b', r'\bwithdraw\b', r'\bterminate\b',
            
            # General modification indicators
            r'\bmake\s+(?:a\s+)?(?:change|modification)\b',
            r'\bI\s+(?:want|need|would\s+like)\s+to\s+(?:add|insert|create|update|modify|change|delete|remove)\b'
        ]
        
        question_lower = question.lower()
        for indicator in edit_indicators:
            if re.search(indicator, question_lower):
                return True
        return False
    
    def generate_edit_sql(self, question: str) -> Dict[str, Any]:
        """Generate SQL for edit operations"""
        try:
            # Initialize edit mode prompts if not already done
            if not self.prompts_manager.edit_sql_prompt:
                self.prompts_manager.initialize_edit_mode_prompts(self.llm)
            
            # Prepare prompt values
            prompt_values = {
                "question": question,
                "schema": self.sql_generation_manager.schema_context or "Database schema not available",
                "examples": self.sql_generation_manager.example_patterns or "No examples available"
            }
            
            if self.sql_generation_manager.memory_manager.use_memory:
                prompt_values["memory"] = self.sql_generation_manager.memory_manager.get_memory_context(question)
            
            # Generate SQL using edit mode prompt
            response = self.llm.invoke(
                self.prompts_manager.edit_sql_prompt.format_messages(**prompt_values)
            )
            
            sql = self._extract_response_content(response)
            
            # Validate the generated SQL
            is_valid, error_msg = self.sql_generation_manager.validate_sql(sql)
            
            return {
                "success": is_valid,
                "sql": sql,
                "error": error_msg,
                "question": question,
                "operation_type": self._determine_operation_type(sql)
            }
            
        except Exception as e:
            return {
                "success": False,
                "sql": "",
                "error": f"Error generating edit SQL: {str(e)}",
                "question": question,
                "operation_type": "unknown"
            }
    
    def verify_edit_sql(self, sql: str, original_question: str) -> Dict[str, Any]:
        """Verify edit SQL for safety and correctness"""
        try:
            # Initialize edit mode prompts if not already done
            if not self.prompts_manager.edit_verification_prompt:
                self.prompts_manager.initialize_edit_mode_prompts(self.llm)
            
            # Prepare prompt values
            prompt_values = {
                "original_question": original_question,
                "sql": sql,
                "schema": self.sql_generation_manager.schema_context or "Database schema not available"
            }
            
            # Generate verification response
            response = self.llm.invoke(
                self.prompts_manager.edit_verification_prompt.format_messages(**prompt_values)
            )
            
            verification_text = self._extract_response_content(response)
            
            # Parse verification response
            try:
                verification_data = json.loads(verification_text)
                
                return {
                    "success": True,
                    "is_safe": verification_data.get("is_safe", False),
                    "is_correct": verification_data.get("is_correct", False),
                    "safety_issues": verification_data.get("safety_issues", []),
                    "correctness_issues": verification_data.get("correctness_issues", []),
                    "impact_assessment": verification_data.get("impact_assessment", ""),
                    "estimated_affected_records": verification_data.get("estimated_affected_records", "unknown"),
                    "recommendations": verification_data.get("recommendations", []),
                    "overall_verdict": verification_data.get("overall_verdict", "REQUIRES_REVIEW"),
                    "explanation": verification_data.get("explanation", "")
                }
                
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing verification response: {e}")
                return self._create_basic_verification(sql)
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Error verifying edit SQL: {str(e)}",
                "is_safe": False,
                "is_correct": False,
                "overall_verdict": "ERROR"
            }
    
    def _determine_operation_type(self, sql: str) -> str:
        """Determine the type of edit operation"""
        sql_upper = sql.upper().strip()
        
        if sql_upper.startswith('INSERT'):
            return "INSERT"
        elif sql_upper.startswith('UPDATE'):
            return "UPDATE"
        elif sql_upper.startswith('DELETE'):
            return "DELETE"
        elif sql_upper.startswith('CREATE'):
            return "CREATE"
        elif sql_upper.startswith('ALTER'):
            return "ALTER"
        elif sql_upper.startswith('DROP'):
            return "DROP"
        else:
            return "unknown"
    
    def _create_basic_verification(self, sql: str) -> Dict[str, Any]:
        """Create basic verification when LLM verification fails"""
        try:
            operation_type = self._determine_operation_type(sql)
            
            # Basic safety checks
            safety_issues = []
            is_safe = True
            
            if operation_type in ["UPDATE", "DELETE"]:
                if "WHERE" not in sql.upper():
                    safety_issues.append("Missing WHERE clause in UPDATE/DELETE operation")
                    is_safe = False
            
            # Basic correctness checks
            is_correct = True
            correctness_issues = []
            
            if not sql.strip():
                correctness_issues.append("Empty SQL query")
                is_correct = False
            
            return {
                "success": True,
                "is_safe": is_safe,
                "is_correct": is_correct,
                "safety_issues": safety_issues,
                "correctness_issues": correctness_issues,
                "impact_assessment": f"Basic {operation_type} operation",
                "estimated_affected_records": "unknown",
                "recommendations": ["Manual review recommended"],
                "overall_verdict": "SAFE_TO_EXECUTE" if is_safe and is_correct else "REQUIRES_REVIEW",
                "explanation": "Basic verification completed"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error in basic verification: {str(e)}",
                "is_safe": False,
                "is_correct": False,
                "overall_verdict": "ERROR"
            }
    
    def _extract_response_content(self, response) -> str:
        """Extract content from LLM response"""
        try:
            if hasattr(response, 'content'):
                return response.content.strip()
            elif hasattr(response, 'text'):
                return response.text.strip()
            elif isinstance(response, str):
                return response.strip()
            else:
                return str(response).strip()
        except Exception as e:
            print(f"Error extracting response content: {e}")
            return "" 