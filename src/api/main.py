import os
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from decimal import Decimal
import json

import uvicorn
from fastapi import FastAPI, Depends, HTTPException, status, Header, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field

from src.core.langgraph import SmartSQLGenerator
from src.core.database import get_database_analyzer
from src.observability.langfuse_config import (
    langfuse_manager, 
    create_langfuse_trace, 
    cleanup_langfuse
)
from src.models.schemas import (
    UserCreate, User, Token, UserRole, UserSettingsUpdate,
    UserSearchRequest, PromoteUserRequest, UserSearchResult,
    SessionCreate, Session,
    MessageCreate, Message, QueryResult,
    EditQueryRequest, EditQueryResponse, ExecuteEditRequest, ExecuteSQLRequest,
    SavedQueryCreate, SavedQuery, SavedChart, PyObjectId
)
from src.auth.handlers import (
    authenticate_user, create_access_token,
    get_current_active_user, get_current_user,
    get_current_admin_user, get_current_admin_user_with_edit_mode,
    check_edit_permission, check_admin_permission,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from src.services.db_service import (
    UserService, SessionService, MessageService
)
from src.vector_store.manager import vector_store_manager


app = FastAPI(
    title="NLP to SQL API - Simplified",
    description="Simplified API for natural language to SQL conversion with hardcoded PBTest database",
    version="3.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update with your frontend domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Active SQL generators for each session
# Maps session_id -> SmartSQLGenerator instance
active_generators: Dict[str, SmartSQLGenerator] = {}

# Session cleanup interval (in minutes)
SESSION_TIMEOUT = 60  # 1 hour

# Global database analyzer instance
db_analyzer = None


def convert_non_serializable_objects(obj):
    """Recursively convert non-serializable objects (Decimal, timedelta, datetime) for JSON/MongoDB serialization"""
    from datetime import datetime, timedelta
    
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, timedelta):
        # Convert timedelta to a dictionary for MongoDB compatibility
        return {
            "__type__": "timedelta",
            "total_seconds": obj.total_seconds(),
            "days": obj.days,
            "seconds": obj.seconds,
            "microseconds": obj.microseconds,
            "str_representation": str(obj)
        }
    elif isinstance(obj, datetime):
        # Ensure datetime objects are ISO formatted strings
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_non_serializable_objects(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_non_serializable_objects(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_non_serializable_objects(item) for item in obj)
    else:
        return obj


def get_global_db_analyzer():
    """Get the global database analyzer instance"""
    global db_analyzer
    if db_analyzer is None:
        db_analyzer = get_database_analyzer()
    return db_analyzer


# Pydantic models for simplified API
class QueryRequest(BaseModel):
    question: str = Field(..., description="Natural language question")
    auto_fix: bool = Field(True, description="Whether to auto-fix SQL errors")
    max_attempts: int = Field(2, description="Maximum number of fix attempts")


class SimplifiedSessionCreate(BaseModel):
    name: str = Field(..., description="Session name")
    description: Optional[str] = Field(None, description="Session description")


# Startup event to initialize the database analyzer
@app.on_event("startup")
async def startup_event():
    """Initialize the application"""
    global db_analyzer
    try:
        print("Initializing simplified NLP-to-SQL system...")
        db_analyzer = get_database_analyzer()
        
        if db_analyzer.test_connection():
            print("✅ Connected to PBTest database successfully")
            schema_context = db_analyzer.get_schema_context()
            print(f"✅ Schema analysis completed ({len(schema_context)} characters)")
        else:
            print("❌ Failed to connect to PBTest database")
            
    except Exception as e:
        print(f"❌ Startup error: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    try:
        # Clean up Langfuse connections
        cleanup_langfuse()
        print("✅ Application shutdown completed")
    except Exception as e:
        print(f"❌ Shutdown error: {e}")


# Root endpoint
@app.get("/")
def read_root():
    """Root endpoint"""
    return {
        "message": "Simplified NLP to SQL API is running",
        "version": "3.0.0",
        "database": "PBTest (hardcoded)",
        "table": "IT_Professional_Services"
    }


# Health check endpoint
@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        db_analyzer = get_global_db_analyzer()
        db_connected = db_analyzer.test_connection()
        
        return {
            "status": "healthy" if db_connected else "unhealthy",
            "database_connected": db_connected,
            "database": "PBTest",
            "table": "IT_Professional_Services",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


# Database info endpoint
@app.get("/database-info")
async def get_database_info(current_user: User = Depends(get_current_active_user)):
    """Get information about the hardcoded database"""
    try:
        db_analyzer = get_global_db_analyzer()
        
        if db_analyzer.test_connection():
            table_info = db_analyzer.get_table_info()
            schema_context = db_analyzer.get_schema_context()
            
            return {
                "success": True,
                "database": "PBTest",
                "table": "IT_Professional_Services",
                "schema": "public",
                "connected": True,
                "table_info": {
                    "columns": len(table_info.get("table_structure", {}).get("columns", [])),
                    "rows": table_info.get("data_analysis", {}).get("row_count", 0),
                    "size": table_info.get("data_analysis", {}).get("table_size", "Unknown")
                },
                "schema_context_length": len(schema_context)
            }
        else:
            return {
                "success": False,
                "error": "Database connection failed",
                "database": "PBTest",
                "table": "IT_Professional_Services"
            }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting database info: {str(e)}"
        )


# Authentication endpoints (kept as-is)
@app.post("/register", response_model=User, status_code=status.HTTP_201_CREATED)
async def register(user: UserCreate):
    """Register a new user"""
    try:
        return await UserService.create_user(user)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.post("/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """User login"""
    user = authenticate_user(form_data.username, form_data.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.id}, expires_delta=access_token_expires
    )
    
    # Calculate expiration time for client
    expires_at = datetime.utcnow() + access_token_expires
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_at": expires_at
    }


@app.get("/me", response_model=User)
async def get_current_user_info(current_user: User = Depends(get_current_active_user)):
    """Get current user information"""
    return current_user


@app.put("/me/settings", response_model=User)
async def update_user_settings(
    settings: UserSettingsUpdate,
    current_user: User = Depends(get_current_active_user)
):
    """Update user settings"""
    return await UserService.update_user_settings(current_user.id, settings)


@app.get("/users", response_model=List[User])
async def get_all_users(current_user: User = Depends(get_current_admin_user)):
    """Get all users (admin only)"""
    return await UserService.get_all_users(current_user.id)


@app.post("/admin/promote-user", response_model=User)
async def promote_user(
    promote_req: PromoteUserRequest,
    current_user: User = Depends(get_current_admin_user)
):
    """Promote a user to admin (admin only)"""
    try:
        return await UserService.promote_user_to_admin(promote_req.user_email, current_user.id)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.post("/admin/search-user", response_model=Optional[UserSearchResult])
async def search_user_by_email(
    search_req: UserSearchRequest,
    current_user: User = Depends(get_current_admin_user)
):
    """Search for a user by email address (admin only)"""
    try:
        user_result = await UserService.search_user_by_email(search_req.email)
        return user_result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"User search failed: {str(e)}"
        )


# Simplified session endpoints (no workspace required)
@app.post("/sessions", response_model=Session, status_code=status.HTTP_201_CREATED)
async def create_session(
    session: SimplifiedSessionCreate,
    current_user: User = Depends(get_current_active_user)
):
    """Create a new session for the current user"""
    try:
        # Create session directly mapped to user (no workspace needed)
        session_data = SessionCreate(
            name=session.name,
            description=session.description
        )
        return await SessionService.create_session(session_data, current_user.id)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.get("/sessions", response_model=List[Session])
async def get_user_sessions(current_user: User = Depends(get_current_active_user)):
    """Get all sessions for the current user"""
    return await SessionService.get_user_sessions(current_user.id)


@app.get("/sessions/{session_id}", response_model=Session)
async def get_session(
    session_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """Get a session by ID"""
    session = await SessionService.get_session(session_id, current_user.id)
    
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session not found"
        )
    
    return session


@app.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session(
    session_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """Delete a session"""
    success = await SessionService.delete_session(session_id, current_user.id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session not found"
        )
    
    # Clean up active generator
    if session_id in active_generators:
        del active_generators[session_id]
    
    return None


# Message endpoints
@app.get("/sessions/{session_id}/messages", response_model=List[Message])
async def get_session_messages(
    session_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """Get all messages for a session"""
    return await MessageService.get_session_messages(session_id, current_user.id)


@app.post("/sessions/{session_id}/messages", response_model=Message, status_code=status.HTTP_201_CREATED)
async def create_message(
    session_id: str,
    message: MessageCreate,
    current_user: User = Depends(get_current_active_user)
):
    """Create a new message in a session"""
    return await MessageService.create_message(message, current_user.id)


# Main query endpoint - simplified
@app.post("/sessions/{session_id}/query")
async def query_with_session(
    session_id: str, 
    query_req: QueryRequest,
    current_user: User = Depends(get_current_active_user)
):
    """Process a query within a session - simplified version using hardcoded database"""
    # Create Langfuse trace for this query
    trace = create_langfuse_trace(
        name="nlp_to_sql_query",
        user_id=str(current_user.id),
        session_id=session_id,
        question=query_req.question,
        auto_fix=query_req.auto_fix,
        max_attempts=query_req.max_attempts
    )
    
    # Get the session
    session = await SessionService.get_session(session_id, current_user.id)
    
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session not found"
        )
    
    # Initialize or get the SQL generator for this session
    if session_id not in active_generators:
        try:
            # Create SQL generator with simplified configuration
            sql_generator = SmartSQLGenerator(
                use_memory=True,
                    memory_persist_dir=f"./memory_store/session_{session_id}",
                    use_cache=True,
                    cache_file=f"query_cache_{session_id}.json"
            )
            active_generators[session_id] = sql_generator
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to initialize SQL generator: {str(e)}"
            )
    
    sql_generator = active_generators[session_id]
    
    try:
        # Process the query using the simplified system
        result = await sql_generator.process_unified_query(
            question=query_req.question,
            user_role=current_user.role.value,
            edit_mode_enabled=(current_user.role == UserRole.ADMIN)
        )
        
        # Convert non-serializable objects
        result = convert_non_serializable_objects(result)
        
        # Store the message
        message_data = MessageCreate(
            content=query_req.question,
            role="user",
            session_id=PyObjectId(session_id),
            query_result=QueryResult(success=True)
        )
        
        stored_message = await MessageService.create_message(
            message_data, current_user.id
        )
        
        # Add database info to the result
        if isinstance(result, dict):
            result["database_info"] = {
                "database": "PBTest",
                "table": "IT_Professional_Services",
                "schema": "public"
            }
        
        return result
        
    except Exception as e:
        error_result = {
            "success": False,
            "question": query_req.question,
            "error": str(e),
            "text": f"I encountered an error while processing your question: {str(e)}",
            "execution_time": 0,
            "sql": "",
            "results": [],
            "query_type": "error",
            "database_info": {
                "database": "PBTest",
                "table": "IT_Professional_Services",
                "schema": "public"
            }
        }
        
        # Store the error message
        try:
            message_data = MessageCreate(
                content=query_req.question,
                role="user",
                session_id=PyObjectId(session_id),
                query_result=QueryResult(**error_result)
            )
            await MessageService.create_message(
                message_data, current_user.id
            )
        except Exception:
            pass  # Don't fail if we can't store the error message
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Direct query endpoint (no session required)
@app.post("/query")
async def direct_query(
    query_req: QueryRequest,
    current_user: User = Depends(get_current_active_user)
):
    """Process a query directly without requiring a session"""
    try:
        # Create a temporary SQL generator
        sql_generator = SmartSQLGenerator(
            use_memory=False,  # No memory for direct queries
            use_cache=False    # No cache for direct queries
        )
        
        # Process the query
        result = await sql_generator.process_unified_query(
            question=query_req.question,
            user_role=current_user.role.value,
            edit_mode_enabled=(current_user.role == UserRole.ADMIN)
        )
        
        # Convert non-serializable objects
        result = convert_non_serializable_objects(result)
        
        # Add database info to the result
        if isinstance(result, dict):
            result["database_info"] = {
                "database": "PBTest",
                "table": "IT_Professional_Services",
                "schema": "public"
            }
        
        return result
        
    except Exception as e:
        error_result = {
            "success": False,
            "question": query_req.question,
            "error": str(e),
            "text": f"I encountered an error while processing your question: {str(e)}",
            "execution_time": 0,
            "sql": "",
            "results": [],
            "query_type": "error",
            "database_info": {
                "database": "PBTest",
                "table": "IT_Professional_Services",
                "schema": "public"
            }
        }
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Cleanup endpoint for active sessions
@app.post("/admin/cleanup-sessions")
async def cleanup_inactive_sessions(current_user: User = Depends(get_current_admin_user)):
    """Cleanup inactive SQL generators (admin only)"""
    cleaned_count = 0
    
    # Remove SQL generators that haven't been used recently
    # This is a simple cleanup - in production you might want more sophisticated logic
    sessions_to_remove = []
    
    for session_id in active_generators.keys():
        # Here you could add logic to check last access time
        # For now, we'll just report the count
        pass
    
    for session_id in sessions_to_remove:
        del active_generators[session_id]
        cleaned_count += 1
    
    return {
        "cleaned_sessions": cleaned_count,
        "active_sessions": len(active_generators),
        "message": f"Cleaned up {cleaned_count} inactive sessions"
    }


# System status endpoint
@app.get("/admin/system-status")
async def get_system_status(current_user: User = Depends(get_current_admin_user)):
    """Get system status (admin only)"""
    try:
        db_analyzer = get_global_db_analyzer()
        db_connected = db_analyzer.test_connection()
        
        return {
            "database_connected": db_connected,
            "database": "PBTest",
            "table": "IT_Professional_Services",
            "active_sessions": len(active_generators),
            "memory_usage": {
                "active_generators": len(active_generators),
                "generator_ids": list(active_generators.keys())
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting system status: {str(e)}"
        )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 