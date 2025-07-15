import re
from typing import Dict, List, Any
from datetime import datetime


class QueryAnalyzer:
    """Simple query analyzer that classifies questions as conversational or analysis"""
    
    def __init__(self):
        pass
    
    def analyze_question(self, question: str) -> Dict[str, Any]:
        """Analyze the question to determine if it's conversational or requires analysis"""
        try:
            # is_conversational = self._is_conversational_question(question)
            
            analysis = {
                "question": question,
                "is_conversational": False,
                "requires_analysis": True,
                "intent": self._determine_intent(question),
                "complexity": self._assess_complexity(question)
            }
            
            return analysis
            
        except Exception as e:
            print(f"Error analyzing question: {e}")
            return {
                "question": question,
                "is_conversational": False,
                "requires_analysis": True,
                "intent": "unknown",
                "complexity": "simple"
            }
    
    def _is_conversational_question(self, question: str) -> bool:
        """Check if question is conversational (greetings, context references, simple chat)"""
        question_lower = question.lower().strip()
        
        # Empty or very short questions
        if len(question_lower) < 3:
            return True
        
        # Greeting patterns
        greeting_patterns = [
            r'^(hi|hello|hey|good morning|good afternoon|good evening|thanks|thank you|bye|goodbye)$',
            r'^(hi|hello|hey)\s+(there|everyone|guys?)$',
            r'^(how are you|how\'s it going|what\'s up|how do you do)$',
            r'^(thanks|thank you)(\s+(so\s+)?much)?$',
            r'^(ok|okay|alright|got it|understood|sure)$',
            r'^(yes|yeah|yep|no|nope|maybe)$'
        ]
        
        # Check greeting patterns
        for pattern in greeting_patterns:
            if re.search(pattern, question_lower):
                return True
        
        # Context reference patterns (referencing previous conversation)
        context_patterns = [
            r'^(what about|how about|and)\s+',
            r'\b(this|that|these|those)\s+(one|ones|result|results|data|table|query|analysis)s?\b',
            r'\b(same|similar|like that|like this)\b',
            r'\b(above|below|previous|last|recent|earlier)\s+(result|query|analysis|data)\b',
            r'\b(more|other|else|additional|further)\s+(details|info|information|data)\b',
            r'\b(give me more|show me more|tell me more)\b',
            r'\b(also|too|as well)\b.*\?$',
            r'^(can you|could you|would you|will you)\s+(also|too|as well)',
            r'^(what|how|why|where|when)\s+(about|of)\s+(this|that|these|those)\b'
        ]
        
        # Check context reference patterns
        for pattern in context_patterns:
            if re.search(pattern, question_lower):
                return True
        
        # Simple acknowledgments or responses
        acknowledgment_patterns = [
            r'^(ok|okay|alright|got it|understood|sure|fine|right|correct|exactly)$',
            r'^(good|great|excellent|perfect|awesome|nice|cool)$',
            r'^(i see|i understand|makes sense|that works|sounds good)$',
            r'^(let me think|hmm|well|um|uh)$'
        ]
        
        for pattern in acknowledgment_patterns:
            if re.search(pattern, question_lower):
                return True
        
        # Questions about the system itself (not data analysis)
        system_questions = [
            r'^(what|how)\s+(can|do)\s+you\s+(do|help)',
            r'^(what|who)\s+are\s+you\s*\?$',
            r'^(how|what)\s+(does|is)\s+(this|the system|the app|the application)',
            r'^(can|could|would)\s+you\s+(help|assist|explain|tell)'
        ]
        
        for pattern in system_questions:
            if re.search(pattern, question_lower):
                return True
        
        # All other questions are considered analysis questions
        return False
    
    def _determine_intent(self, question: str) -> str:
        """Determine the basic intent of the question"""
        question_lower = question.lower()
        
        # If conversational, return conversational intent
        if self._is_conversational_question(question):
            return "conversational"
        
        # For analysis questions, determine data intent
        intent_patterns = {
            "retrieve": [
                r'\b(show|get|find|list|display|view|see)\b',
                r'\b(what\s+is|what\s+are|who\s+is|who\s+are)\b'
            ],
            "count": [
                r'\b(how\s+many|count|number\s+of|total)\b'
            ],
            "calculate": [
                r'\b(sum|average|mean|max|min|calculate|compute)\b'
            ],
            "analyze": [
                r'\b(analyz[e|ing]|compare|trend|why|how|what\s+causes)\b'
            ],
            "create": [
                r'\b(add|insert|create|new)\b'
            ],
            "update": [
                r'\b(update|modify|change|edit)\b'
            ],
            "delete": [
                r'\b(delete|remove|drop)\b'
            ]
        }
        
        for intent, patterns in intent_patterns.items():
            for pattern in patterns:
                if re.search(pattern, question_lower):
                    return intent
        
        return "retrieve"  # Default intent for analysis questions
    
    def _assess_complexity(self, question: str) -> str:
        """Assess the complexity of the question"""
        if self._is_conversational_question(question):
            return "simple"
        
        question_lower = question.lower()
        
        # Complex indicators
        complex_indicators = [
            r'\b(analyz[e|ing]|comprehensive|detailed|thorough)\b',
            r'\b(compare|contrast|versus|vs)\b',
            r'\b(trend|pattern|correlation|relationship)\b',
            r'\b(why|how|what\s+causes|what\s+drives)\b',
            r'\b(multiple|several|various|different)\b',
            r'\b(across|between|among)\b.*\band\b'
        ]
        
        # Check for complex indicators
        for indicator in complex_indicators:
            if re.search(indicator, question_lower):
                return "complex"
        
        # Medium indicators
        medium_indicators = [
            r'\b(count|sum|average|group\s+by|order\s+by|filter)\b',
            r'\b(top|bottom|highest|lowest|most|least)\b',
            r'\b(over\s+time|by\s+year|by\s+month|by\s+category)\b'
        ]
        
        for indicator in medium_indicators:
            if re.search(indicator, question_lower):
                return "medium"
        
        return "simple"
    
    def _extract_entities(self, question: str) -> List[str]:
        """Extract basic entities from the question"""
        entities = []
        
        # Time-related entities
        time_patterns = [
            r'\b(yesterday|today|tomorrow)\b',
            r'\b(last|this|next)\s+(week|month|year|quarter)\b',
            r'\b(20\d{2})\b',  # Years
            r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\b'
        ]
        
        question_lower = question.lower()
        for pattern in time_patterns:
            matches = re.findall(pattern, question_lower)
            entities.extend(matches)
        
        return entities 