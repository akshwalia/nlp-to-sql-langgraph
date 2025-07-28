import os
import re
from typing import Any, Optional
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_google_genai import GoogleGenerativeAIEmbeddings


class MemoryManager:
    """Manages memory functionality for the SQL generator"""
    
    def __init__(self, use_memory: bool = True, memory_persist_dir: str = "./memory_store"):
        self.use_memory = use_memory
        self.memory_persist_dir = memory_persist_dir
        self.memory = None
        
        if use_memory:
            self.memory = self._initialize_memory(memory_persist_dir)
    
    def _initialize_memory(self, persist_dir: str) -> Optional[Chroma]:
        """Initialize vector store memory for LangGraph with Gemini embeddings"""
        try:
            # Ensure the directory exists
            os.makedirs(persist_dir, exist_ok=True)
            
            # Initialize Gemini embeddings
            gemini_api_key = os.getenv("GOOGLE_API_KEY")
            if not gemini_api_key:
                print("Warning: GOOGLE_API_KEY not found. Memory functionality will be disabled.")
                return None
            
            # Create Gemini embeddings
            embeddings = GoogleGenerativeAIEmbeddings(
                model="models/embedding-001",
                google_api_key=gemini_api_key
            )
            
            # Create or load the vector store with Gemini embeddings
            vectorstore = Chroma(
                persist_directory=persist_dir,
                collection_name="sql_conversation_memory",
                embedding_function=embeddings
            )
            
            return vectorstore
        except Exception as e:
            print(f"Error initializing memory: {e}")
            return None
    
    def store_in_memory(self, question: str, sql: str, results: Any = None) -> None:
        """Store the question, generated SQL and results in memory"""
        if not self.memory or not self.use_memory:
            return
            
        try:
            # Create document with question and SQL
            content = f"Question: {question}\nSQL: {sql}"
            
            # Extract and store personal information
            personal_info = self._extract_personal_info(question, results)
            if personal_info:
                content = f"{personal_info}\n\n{content}"
            
            # Add result summary if available
            if results:
                try:
                    # Count rows or summarize results
                    if isinstance(results, list) and results:
                        num_rows = len(results)
                        sample = results[0] if results else {}
                        columns = list(sample.keys()) if isinstance(sample, dict) else []
                        result_summary = f"\nReturned {num_rows} rows with columns: {', '.join(columns)}"
                        
                        # Include sample results (first 3 rows at most)
                        if num_rows > 0:
                            result_summary += "\nSample results:"
                            for i, row in enumerate(results[:3]):
                                result_summary += f"\nRow {i+1}: {str(row)}"
                        
                        content += result_summary
                except Exception as e:
                    print(f"Error summarizing results: {e}")
                
            # Store in memory as a document
            doc = Document(page_content=content, metadata={"question": question})
            self.memory.add_documents([doc])
        except Exception as e:
            print(f"Error storing in memory: {e}")

    def _extract_personal_info(self, question: str, results: Any = None) -> str:
        """Extract personal information from user queries or results"""
        personal_info = []
        
        # Check for name information
        name_patterns = [
            r"my name is (?P<name>[\w\s]+)",
            r"I am (?P<name>[\w\s]+)",
            r"I'm (?P<name>[\w\s]+)",
            r"call me (?P<name>[\w\s]+)"
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                name = match.group("name").strip()
                personal_info.append(f"User name: {name}")
                break
        
        # Check for other personal identifiers in the question
        if "my" in question.lower():
            id_patterns = [
                r"my (?P<id_type>user|customer|employee|sales|account|order|client|supplier|vendor) (?P<id_value>\w+)",
                r"my (?P<id_type>user|customer|employee|sales|account|order|client|supplier|vendor) id is (?P<id_value>\w+)",
                r"my (?P<id_type>user|customer|employee|sales|account|order|client|supplier|vendor) number is (?P<id_value>\w+)",
            ]
            
            for pattern in id_patterns:
                match = re.search(pattern, question, re.IGNORECASE)
                if match:
                    id_type = match.group("id_type").strip()
                    id_value = match.group("id_value").strip()
                    personal_info.append(f"User {id_type} ID: {id_value}")
                    break
        
        # Check for personal context "I am a X"
        role_patterns = [
            r"I am a (?P<role>[\w\s]+)",
            r"I'm a (?P<role>[\w\s]+)",
            r"I work as a (?P<role>[\w\s]+)",
            r"my role is (?P<role>[\w\s]+)"
        ]
        
        for pattern in role_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                role = match.group("role").strip()
                personal_info.append(f"User role: {role}")
                break
        
        # Check for location information
        location_patterns = [
            r"I am in (?P<location>[\w\s]+)",
            r"I'm in (?P<location>[\w\s]+)",
            r"I work in (?P<location>[\w\s]+)",
            r"my location is (?P<location>[\w\s]+)"
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                location = match.group("location").strip()
                personal_info.append(f"User location: {location}")
                break
        
        return "\n".join(personal_info)
    
    def get_memory_context(self, question: str) -> str:
        """Get relevant memory context for the question"""
        if not self.memory or not self.use_memory:
            return ""
            
        try:
            # Search for relevant memories with enhanced error handling
            try:
                relevant_docs = self.memory.similarity_search(question, k=5)
                
                # Validate the structure of returned documents
                if not isinstance(relevant_docs, list):
                    print(f"Warning: similarity_search returned {type(relevant_docs)} instead of list")
                    return ""
                
            except Exception as search_error:
                print(f"Error in similarity_search: {search_error}")
                return ""
            
            # Extract and format relevant information with bounds checking
            context_parts = []
            for i, doc in enumerate(relevant_docs):
                try:
                    # Check if doc has the expected structure
                    if hasattr(doc, 'page_content'):
                        context_parts.append(doc.page_content)
                    elif isinstance(doc, (tuple, list)):
                        # Handle case where doc might be a tuple/list
                        print(f"Warning: Document {i} is {type(doc)} instead of expected Document object")
                        if len(doc) > 0:
                            context_parts.append(str(doc[0]))
                    else:
                        print(f"Warning: Document {i} has unexpected structure: {type(doc)}")
                        context_parts.append(str(doc))
                except Exception as doc_error:
                    print(f"Error processing document {i}: {doc_error}")
                    continue
            
            if context_parts:
                # Safely access the first 3 context parts
                limited_context = context_parts[:3] if len(context_parts) >= 3 else context_parts
                return "### RELEVANT MEMORY CONTEXT:\n" + "\n\n".join(limited_context)
            else:
                return ""
        except Exception as e:
            print(f"Error retrieving memory context: {e}")
            return ""
    
    def store_text_in_memory(self, question: str, text_response: str, sql: str = None, results: Any = None) -> None:
        """Store text response in memory for future reference"""
        if not self.memory or not self.use_memory:
            return
            
        try:
            # Create document for text response
            content = f"Question: {question}\nResponse: {text_response}"
            
            # Add SQL if provided
            if sql:
                content += f"\nSQL: {sql}"
            
            # Add results summary if available
            if results:
                try:
                    if isinstance(results, list) and results:
                        num_rows = len(results)
                        content += f"\nReturned {num_rows} rows"
                except Exception as e:
                    print(f"Error summarizing results for text memory: {e}")
                
            # Store in memory as a document
            doc = Document(page_content=content, metadata={"question": question, "type": "text_response"})
            self.memory.add_documents([doc])
        except Exception as e:
            print(f"Error storing text in memory: {e}")
    
 