import os
from datetime import datetime
from typing import Optional, List, Dict, Any
from decimal import Decimal
from bson import ObjectId
from pydantic import BaseModel, Field, EmailStr, validator, field_validator
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection
MONGODB_URL = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGODB_URL)
db = client.get_database("nlp_sql")

# Collections
users_collection = db.users
workspaces_collection = db.workspaces
sessions_collection = db.sessions
messages_collection = db.messages
saved_queries_collection = db.saved_queries


# Custom ObjectId field for Pydantic models
class PyObjectId(str):
    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type, _handler):
        from pydantic_core import core_schema
        
        def validate_objectid(value):
            if isinstance(value, ObjectId):
                return str(value)
            if not ObjectId.is_valid(value):
                raise ValueError("Invalid ObjectId")
            return str(value)
        
        return core_schema.no_info_plain_validator_function(
            function=validate_objectid,
            serialization=core_schema.to_string_ser_schema(),
        )


# Role enum for users
from enum import Enum

class UserRole(str, Enum):
    ADMIN = "admin"
    VIEWER = "viewer"


# User models
class UserCreate(BaseModel):
    email: EmailStr
    password: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class UserSettings(BaseModel):
    edit_mode_enabled: bool = False  # Only meaningful for admin users
    last_activity: Optional[datetime] = None


class User(BaseModel):
    id: PyObjectId = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    email: EmailStr
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    role: UserRole = UserRole.VIEWER
    is_active: bool = True
    is_admin: bool = False  # Keep for backward compatibility
    settings: UserSettings = Field(default_factory=UserSettings)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_schema_extra = {
            "example": {
                "_id": "123456789012345678901234",
                "email": "user@example.com",
                "first_name": "John",
                "last_name": "Doe",
                "is_active": True
            }
        }


class UserInDB(User):
    hashed_password: str


# Token models
class Token(BaseModel):
    access_token: str
    token_type: str
    expires_at: datetime


class TokenData(BaseModel):
    user_id: Optional[str] = None
    exp: Optional[datetime] = None


# Database connection model
class DatabaseConnection(BaseModel):
    db_name: str
    username: str
    password: str
    host: str = "localhost"
    port: int = 5432


# Workspace models
class WorkspaceCreate(BaseModel):
    name: str
    description: Optional[str] = None
    db_connection: DatabaseConnection


class Workspace(BaseModel):
    id: PyObjectId = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    name: str
    description: Optional[str] = None
    db_connection: DatabaseConnection
    user_id: PyObjectId
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True


class WorkspaceInDB(Workspace):
    pass


# Chart recommendation models
class ChartRecommendation(BaseModel):
    chart_type: str  # 'bar', 'line', 'pie', etc.
    title: str
    description: str
    x_axis: Optional[str] = None  # Fixed: Allow None for pie charts
    y_axis: Optional[str] = None  # Fixed: Allow None for pie charts
    secondary_y_axis: Optional[str] = None
    chart_config: Optional[Dict[str, Any]] = None  # Additional chart configuration
    confidence_score: float = 0.0  # LLM confidence in this recommendation


class VisualizationRecommendations(BaseModel):
    is_visualizable: bool
    reason: Optional[str] = None  # Reason why not visualizable, if applicable
    recommended_charts: List[ChartRecommendation] = []
    database_type: Optional[str] = None
    data_characteristics: Optional[Dict[str, Any]] = None


class SavedChart(BaseModel):
    chart_id: str = Field(default_factory=lambda: str(ObjectId()))
    chart_type: str
    title: str
    x_axis: Optional[str] = None  # Fixed: Allow None for pie charts
    y_axis: Optional[str] = None  # Fixed: Allow None for pie charts
    secondary_y_axis: Optional[str] = None
    chart_config: Optional[Dict[str, Any]] = None
    created_by: str = "user"  # "user" or "llm"
    created_at: datetime = Field(default_factory=datetime.utcnow)


# Session models
class SessionCreate(BaseModel):
    name: str
    description: Optional[str] = None
    workspace_id: PyObjectId


class Session(BaseModel):
    id: PyObjectId = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    name: str
    description: Optional[str] = None
    workspace_id: PyObjectId
    user_id: PyObjectId
    vector_store_id: str = Field(default_factory=lambda: str(ObjectId()))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True


class SessionInDB(Session):
    pass


# Query execution result models
class QueryResult(BaseModel):
    success: bool
    sql: Optional[str] = None
    results: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    is_conversational: Optional[bool] = None
    is_multi_query: Optional[bool] = None
    is_why_analysis: Optional[bool] = None
    query_type: Optional[str] = None
    analysis_type: Optional[str] = None
    source: Optional[str] = None
    confidence: Optional[int] = None
    auto_fixed: Optional[bool] = None
    fix_attempts: Optional[int] = None
    pagination: Optional[Dict[str, Any]] = None
    tables: Optional[List[Dict[str, Any]]] = None
    # Add chart recommendations
    visualization_recommendations: Optional[VisualizationRecommendations] = None
    saved_charts: List[SavedChart] = Field(default_factory=list)
    
    @field_validator('results', 'pagination', 'tables', mode='before')
    @classmethod
    def convert_non_serializable(cls, v):
        """Convert non-serializable objects (Decimal, timedelta, datetime) for JSON serialization"""
        if v is None:
            return v
        return cls._convert_non_serializable_recursive(v)
    
    @staticmethod
    def _convert_non_serializable_recursive(obj):
        """Recursively convert non-serializable objects"""
        from datetime import datetime, date, timedelta
        
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, timedelta):
            # Convert timedelta to total seconds for consistent serialization
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
        elif isinstance(obj, date):
            # Convert date objects to ISO formatted strings
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {key: QueryResult._convert_non_serializable_recursive(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [QueryResult._convert_non_serializable_recursive(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(QueryResult._convert_non_serializable_recursive(item) for item in obj)
        else:
            return obj


# Message models
class MessageCreate(BaseModel):
    content: str
    role: str  # "user" or "assistant"
    session_id: PyObjectId
    query_result: Optional[QueryResult] = None


class Message(BaseModel):
    id: PyObjectId = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    content: str
    role: str  # "user" or "assistant"
    session_id: PyObjectId
    user_id: PyObjectId
    query_result: Optional[QueryResult] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True


class MessageInDB(Message):
    pass


# Edit mode models
class EditQueryRequest(BaseModel):
    question: str
    session_id: PyObjectId


class EditQueryResponse(BaseModel):
    success: bool
    question: str
    sql: str
    verification_result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    is_edit_query: bool = True
    requires_confirmation: bool = True


class ExecuteEditRequest(BaseModel):
    sql: str
    session_id: PyObjectId
    user_confirmed: bool = True


class ExecuteSQLRequest(BaseModel):
    sql: str


class UserSettingsUpdate(BaseModel):
    edit_mode_enabled: Optional[bool] = None


# Admin management models
class UserSearchRequest(BaseModel):
    email: str


class PromoteUserRequest(BaseModel):
    user_email: str


class UserSearchResult(BaseModel):
    id: str
    email: EmailStr
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    role: UserRole
    is_active: bool
    created_at: datetime


# Saved Query models
class SavedQueryCreate(BaseModel):
    title: str
    description: Optional[str] = None
    sql: str
    data: List[Dict[str, Any]]
    table_name: Optional[str] = None


class SavedQuery(BaseModel):
    id: PyObjectId = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    title: str
    description: Optional[str] = None
    sql: str
    data: List[Dict[str, Any]]
    table_name: Optional[str] = None
    user_id: PyObjectId
    workspace_id: Optional[PyObjectId] = None
    session_id: Optional[PyObjectId] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    @field_validator('data', mode='before')
    @classmethod
    def convert_non_serializable(cls, v):
        """Convert non-serializable objects (Decimal, timedelta, datetime) for JSON serialization"""
        if v is None:
            return v
        return cls._convert_non_serializable_recursive(v)
    
    @staticmethod
    def _convert_non_serializable_recursive(obj):
        """Recursively convert non-serializable objects"""
        from datetime import datetime, date, timedelta
        
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, timedelta):
            # Convert timedelta to total seconds for consistent serialization
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
        elif isinstance(obj, date):
            # Convert date objects to ISO formatted strings
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {key: SavedQuery._convert_non_serializable_recursive(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [SavedQuery._convert_non_serializable_recursive(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(SavedQuery._convert_non_serializable_recursive(item) for item in obj)
        else:
            return obj
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True


class SavedQueryInDB(SavedQuery):
    pass


# These classes are now defined earlier in the file