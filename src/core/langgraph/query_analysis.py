import re
from typing import Dict, List, Any
from datetime import datetime


class QueryAnalyzer:
    """Analyzes and processes queries for the SQL generator"""
    
    def __init__(self):
        pass
    
    def analyze_question(self, question: str) -> Dict[str, Any]:
        """Analyze the question to understand its intent and characteristics"""
        try:
            analysis = {
                "question": question,
                "is_analysis": self._is_analysis_question(question),
                "is_why_question": self._is_why_question(question),
                "is_sql_question": self._is_sql_question(question),
                "is_conversational": self._is_conversational_question(question),
                "is_edit_operation": self._is_edit_operation(question),
                "time_periods": self._extract_time_periods_from_question(question),
                "requires_multi_query": self._requires_multi_query_analysis(question),
                "complexity": self._assess_complexity(question),
                "entities": self._extract_entities(question),
                "intent": self._determine_intent(question)
            }
            
            return analysis
            
        except Exception as e:
            print(f"Error analyzing question: {e}")
            return {
                "question": question,
                "is_analysis": False,
                "is_why_question": False,
                "is_sql_question": False,
                "is_conversational": False,
                "is_edit_operation": False,
                "time_periods": [],
                "requires_multi_query": False,
                "complexity": "simple",
                "entities": [],
                "intent": "unknown"
            }
    
    def _is_analysis_question(self, question: str) -> bool:
        """Check if question requires analysis beyond simple data retrieval"""
        analysis_indicators = [
            r'\bwhy\b', r'\bhow\b', r'\bwhat causes\b', r'\bwhat leads to\b',
            r'\banalyz[e|ing]\b', r'\bcompare\b', r'\bcontrast\b',
            r'\btrend\b', r'\bpattern\b', r'\bcorrelation\b',
            r'\binsight\b', r'\bexplain\b', r'\breason\b',
            r'\bfactor\b', r'\bimpact\b', r'\beffect\b',
            r'\bperformance\b', r'\boptimiz\b', r'\bimprove\b',
            r'\brecommend\b', r'\bsuggestion\b', r'\badvice\b',
            r'\bpredict\b', r'\bforecast\b', r'\bfuture\b',
            r'\bbest\b', r'\bworst\b', r'\btop\b', r'\bbottom\b',
            r'\bhighest\b', r'\blowest\b', r'\bmost\b', r'\bleast\b'
        ]
        
        question_lower = question.lower()
        for indicator in analysis_indicators:
            if re.search(indicator, question_lower):
                return True
        return False
    
    def _is_why_question(self, question: str) -> bool:
        """Check if this is a 'why' question that needs deeper analysis"""
        why_patterns = [
            r'\bwhy\s+(?:is|are|was|were|do|does|did|has|have|had)\b',
            r'\bwhy\s+(?:not|don\'t|doesn\'t|didn\'t|haven\'t|hasn\'t|hadn\'t)\b',
            r'\bwhy\s+(?:would|could|should|might|may|can|will)\b',
            r'\bwhat\s+(?:causes|leads\s+to|results\s+in|makes)\b',
            r'\bwhat\s+(?:is\s+the\s+reason|are\s+the\s+reasons)\b',
            r'\bhow\s+(?:come|is\s+it\s+that)\b',
            r'\bwhat\s+(?:explains|accounts\s+for)\b'
        ]
        
        question_lower = question.lower()
        for pattern in why_patterns:
            if re.search(pattern, question_lower):
                return True
        return False
    
    def _is_sql_question(self, question: str) -> bool:
        """Check if the question is asking for SQL code or explanation"""
        sql_indicators = [
            r'\bsql\b', r'\bquery\b', r'\bselect\b', r'\bfrom\b',
            r'\bwhere\b', r'\bjoin\b', r'\binner\b', r'\bouter\b',
            r'\bgroup\s+by\b', r'\border\s+by\b', r'\bhaving\b',
            r'\binsert\b', r'\bupdate\b', r'\bdelete\b',
            r'\bcreate\b', r'\balter\b', r'\bdrop\b',
            r'\bshow\s+me\s+the\s+(?:query|sql)\b',
            r'\bwrite\s+(?:a\s+)?(?:query|sql)\b',
            r'\bgenerate\s+(?:a\s+)?(?:query|sql)\b',
            r'\bhow\s+do\s+I\s+(?:query|select|find)\b'
        ]
        
        question_lower = question.lower()
        for indicator in sql_indicators:
            if re.search(indicator, question_lower):
                return True
        return False
    
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
    
    def _is_edit_operation(self, question: str) -> bool:
        """Check if question is requesting an edit operation (INSERT, UPDATE, DELETE)"""
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
    
    def _extract_time_periods_from_question(self, question: str) -> List[Dict[str, str]]:
        """Extract time periods mentioned in the question"""
        time_periods = []
        
        # Define time period patterns
        time_patterns = [
            (r'\b(?:in\s+)?(?:the\s+)?(?:last|past)\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)\b', 'relative_past'),
            (r'\b(?:in\s+)?(?:the\s+)?(?:next|coming)\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)\b', 'relative_future'),
            (r'\b(?:this\s+)?(day|week|month|quarter|year)\b', 'current_period'),
            (r'\b(?:last\s+)?(day|week|month|quarter|year)\b', 'last_period'),
            (r'\b(?:next\s+)?(day|week|month|quarter|year)\b', 'next_period'),
            (r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\b', 'specific_month'),
            (r'\b(20\d{2})\b', 'specific_year'),
            (r'\b(\d{1,2}\/\d{1,2}\/\d{2,4})\b', 'specific_date'),
            (r'\b(\d{4}-\d{2}-\d{2})\b', 'specific_date_iso'),
            (r'\b(today|yesterday|tomorrow)\b', 'relative_day'),
            (r'\b(q1|q2|q3|q4|quarter\s+\d)\b', 'quarter')
        ]
        
        question_lower = question.lower()
        
        for pattern, period_type in time_patterns:
            matches = re.findall(pattern, question_lower)
            for match in matches:
                if isinstance(match, tuple):
                    time_periods.append({
                        "type": period_type,
                        "value": " ".join(match),
                        "raw_match": match
                    })
                else:
                    time_periods.append({
                        "type": period_type,
                        "value": match,
                        "raw_match": match
                    })
        
        return time_periods
    
    def _requires_multi_query_analysis(self, question: str) -> bool:
        """Check if question requires multiple queries for analysis"""
        multi_query_indicators = [
            r'\bcompare\b.*\bwith\b', r'\bversus\b', r'\bvs\b',
            r'\bdifference\s+between\b', r'\bcontrast\b',
            r'\bbefore\s+and\s+after\b', r'\bthen\s+and\s+now\b',
            r'\btrend\b', r'\bover\s+time\b', r'\bacross\s+(?:time|periods)\b',
            r'\bboth\b.*\band\b', r'\beither\b.*\bor\b',
            r'\bmultiple\b', r'\bseveral\b', r'\bvarious\b',
            r'\beach\b.*\bcompared\s+to\b', r'\brelationship\s+between\b',
            r'\bcorrelation\b', r'\bimpact\s+of\b.*\bon\b'
        ]
        
        question_lower = question.lower()
        for indicator in multi_query_indicators:
            if re.search(indicator, question_lower):
                return True
        return False
    
    def _assess_complexity(self, question: str) -> str:
        """Assess the complexity of the question"""
        complexity_indicators = {
            "simple": [
                r'\bshow\b', r'\blist\b', r'\bget\b', r'\bfind\b',
                r'\bwhat\s+is\b', r'\bwho\s+is\b', r'\bwhere\s+is\b',
                r'\bhow\s+many\b', r'\bhow\s+much\b'
            ],
            "medium": [
                r'\bsum\b', r'\bcount\b', r'\baverage\b', r'\bmax\b', r'\bmin\b',
                r'\bgroup\s+by\b', r'\border\s+by\b', r'\btop\b', r'\bbottom\b',
                r'\bfilter\b', r'\bwhere\b', r'\bbetween\b'
            ],
            "complex": [
                r'\banalyz[e|ing]\b', r'\bcompare\b', r'\btrend\b',
                r'\bcorrelation\b', r'\bperformance\b', r'\boptimiz\b',
                r'\bpredict\b', r'\bforecast\b', r'\bwhy\b',
                r'\bmultiple\b.*\bqueries\b', r'\bcomplex\b.*\banalysis\b'
            ]
        }
        
        question_lower = question.lower()
        
        # Check for complex indicators first
        for indicator in complexity_indicators["complex"]:
            if re.search(indicator, question_lower):
                return "complex"
        
        # Then check for medium indicators
        for indicator in complexity_indicators["medium"]:
            if re.search(indicator, question_lower):
                return "medium"
        
        # Default to simple
        return "simple"
    
    def _extract_entities(self, question: str) -> List[Dict[str, str]]:
        """Extract entities from the question"""
        entities = []
        
        # Extract quoted strings
        quoted_strings = re.findall(r'"([^"]+)"', question)
        quoted_strings.extend(re.findall(r"'([^']+)'", question))
        
        for entity in quoted_strings:
            entities.append({
                "type": "quoted_string",
                "value": entity
            })
        
        # Extract numbers
        numbers = re.findall(r'\b\d+(?:\.\d+)?\b', question)
        for number in numbers:
            entities.append({
                "type": "number",
                "value": number
            })
        
        # Extract dates
        dates = re.findall(r'\b\d{4}-\d{2}-\d{2}\b', question)
        dates.extend(re.findall(r'\b\d{1,2}\/\d{1,2}\/\d{2,4}\b', question))
        
        for date in dates:
            entities.append({
                "type": "date",
                "value": date
            })
        
        # Extract capitalized words (potential proper nouns)
        capitalized_words = re.findall(r'\b[A-Z][a-z]+\b', question)
        for word in capitalized_words:
            entities.append({
                "type": "capitalized_word",
                "value": word
            })
        
        return entities
    
    def _determine_intent(self, question: str) -> str:
        """Determine the intent of the question"""
        intent_patterns = {
            "retrieve": [
                r'\bshow\b', r'\bget\b', r'\bfind\b', r'\blist\b',
                r'\bdisplay\b', r'\bview\b', r'\bsee\b'
            ],
            "count": [
                r'\bhow\s+many\b', r'\bcount\b', r'\bnumber\s+of\b',
                r'\btotal\b'
            ],
            "calculate": [
                r'\bsum\b', r'\baverage\b', r'\bmean\b', r'\bmax\b',
                r'\bmin\b', r'\bcalculate\b'
            ],
            "analyze": [
                r'\banalyz[e|ing]\b', r'\bcompare\b', r'\btrend\b',
                r'\bwhy\b', r'\bhow\b', r'\bwhat\s+causes\b'
            ],
            "create": [
                r'\badd\b', r'\binsert\b', r'\bcreate\b', r'\bnew\b'
            ],
            "update": [
                r'\bupdate\b', r'\bmodify\b', r'\bchange\b', r'\bedit\b'
            ],
            "delete": [
                r'\bdelete\b', r'\bremove\b', r'\bdrop\b'
            ]
        }
        
        question_lower = question.lower()
        
        for intent, patterns in intent_patterns.items():
            for pattern in patterns:
                if re.search(pattern, question_lower):
                    return intent
        
        return "unknown"
    
    def plan_queries(self, question: str) -> Dict[str, Any]:
        """Plan multiple queries for complex analysis"""
        try:
            # Analyze the question
            analysis = self.analyze_question(question)
            
            # If it's not a multi-query question, return simple plan
            if not analysis["requires_multi_query"]:
                return {
                    "is_multi_query": False,
                    "queries": [
                        {
                            "id": "main_query",
                            "question": question,
                            "type": "main",
                            "dependencies": []
                        }
                    ]
                }
            
            # Plan multiple queries based on question type
            queries = []
            
            # Check for comparison queries
            if re.search(r'\bcompare\b.*\bwith\b|\bversus\b|\bvs\b', question.lower()):
                queries.extend(self._plan_comparison_queries(question))
            
            # Check for trend analysis
            elif re.search(r'\btrend\b|\bover\s+time\b|\bacross\s+(?:time|periods)\b', question.lower()):
                queries.extend(self._plan_trend_queries(question))
            
            # Check for before/after analysis
            elif re.search(r'\bbefore\s+and\s+after\b|\bthen\s+and\s+now\b', question.lower()):
                queries.extend(self._plan_before_after_queries(question))
            
            # Default to single query if no specific pattern found
            else:
                queries.append({
                    "id": "main_query",
                    "question": question,
                    "type": "main",
                    "dependencies": []
                })
            
            return {
                "is_multi_query": len(queries) > 1,
                "queries": queries
            }
            
        except Exception as e:
            print(f"Error planning queries: {e}")
            return {
                "is_multi_query": False,
                "queries": [
                    {
                        "id": "main_query",
                        "question": question,
                        "type": "main",
                        "dependencies": []
                    }
                ]
            }
    
    def _plan_comparison_queries(self, question: str) -> List[Dict[str, Any]]:
        """Plan queries for comparison analysis"""
        # This is a simplified implementation
        # In a real system, you'd want more sophisticated query planning
        return [
            {
                "id": "comparison_query_1",
                "question": f"First part of comparison: {question}",
                "type": "comparison_part",
                "dependencies": []
            },
            {
                "id": "comparison_query_2",
                "question": f"Second part of comparison: {question}",
                "type": "comparison_part",
                "dependencies": []
            },
            {
                "id": "comparison_analysis",
                "question": f"Analysis of comparison: {question}",
                "type": "analysis",
                "dependencies": ["comparison_query_1", "comparison_query_2"]
            }
        ]
    
    def _plan_trend_queries(self, question: str) -> List[Dict[str, Any]]:
        """Plan queries for trend analysis"""
        return [
            {
                "id": "trend_data_query",
                "question": f"Time series data for: {question}",
                "type": "trend_data",
                "dependencies": []
            },
            {
                "id": "trend_analysis",
                "question": f"Trend analysis of: {question}",
                "type": "analysis",
                "dependencies": ["trend_data_query"]
            }
        ]
    
    def _plan_before_after_queries(self, question: str) -> List[Dict[str, Any]]:
        """Plan queries for before/after analysis"""
        return [
            {
                "id": "before_query",
                "question": f"Before state: {question}",
                "type": "before",
                "dependencies": []
            },
            {
                "id": "after_query",
                "question": f"After state: {question}",
                "type": "after",
                "dependencies": []
            },
            {
                "id": "before_after_analysis",
                "question": f"Before/after analysis: {question}",
                "type": "analysis",
                "dependencies": ["before_query", "after_query"]
            }
        ] 