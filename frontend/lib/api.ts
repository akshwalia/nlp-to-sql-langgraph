import axios from 'axios';

// Define the base URL for API requests
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

// Configure axios defaults
axios.defaults.baseURL = API_BASE_URL;

// Auth interfaces
export interface LoginRequest {
  username: string; // This is the email in the backend
  password: string;
}

export interface TokenResponse {
  access_token: string;
  token_type: string;
  expires_at: string;
}

// Set up axios interceptor for authentication
axios.interceptors.request.use((config) => {
  if (typeof window !== 'undefined') {
    const token = localStorage.getItem('auth_token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  }
  return config;
}, (error) => Promise.reject(error));

// Auth functions
export const login = async (credentials: LoginRequest): Promise<TokenResponse> => {
  try {
    // Convert to form data as required by OAuth2
    const formData = new URLSearchParams();
    formData.append('username', credentials.username);
    formData.append('password', credentials.password);

    const response = await axios.post('/login', formData.toString(), {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    });
    
    return response.data;
  } catch (error) {
    console.error('Login failed:', error);
    throw error;
  }
};

// Get current user information
export const getCurrentUser = async () => {
  try {
    const response = await axios.get('/me');
    return response.data;
  } catch (error) {
    console.error('Error getting current user:', error);
    throw error;
  }
};

export const register = async (user: { email: string; password: string; name: string }): Promise<{ success: boolean; message: string; user_id?: string }> => {
  try {
    const response = await axios.post('/register', user);
    return response.data;
  } catch (error) {
    console.error('Registration failed:', error);
    throw error;
  }
};

// Define the table interface to match the API response
interface TableResponse {
  name: string;
  description: string;
  sql: string;
  results: Record<string, unknown>[];
  row_count: number;
  table_id?: string;
  [key: string]: unknown; // Allow any additional fields
}

// Types for API requests and responses
export interface QueryRequest {
  question: string;
  auto_fix?: boolean;
  max_attempts?: number;
}

export interface SessionRequest {
  db_name?: string;
  username?: string;
  password?: string;
  host?: string;
  port?: string;
  use_memory?: boolean;
  use_cache?: boolean;
  name?: string;
  description?: string;
}

// Session management
export const createSession = async (params: SessionRequest & { workspace_id?: string }) => {
  try {
    // First, try direct session creation (old API)
    if (!params.workspace_id) {
      try {
        const directResponse = await axios.post('/sessions', params);
        return directResponse.data;
      } catch (directError) {
        console.warn('Direct session creation failed:', directError);
        throw new Error('Session creation requires a workspace_id in the new API');
      }
    }

    // Workspace-scoped session creation (new API)
    const sessionRequest = {
      name: params.name || `${params.db_name || 'Default'} Session`,
      description: params.description || `Session for ${params.host || 'localhost'} database`,
      workspace_id: params.workspace_id
    };
    
    const response = await axios.post(
      `/workspaces/${params.workspace_id}/sessions`, 
      sessionRequest
    );
    return response.data;
  } catch (error) {
    console.error('Error creating session:', error);
    throw error;
  }
};

// Update getSessionInfo to use authentication
export const getSessionInfo = async (sessionId: string) => {
  try {
    console.log(`Fetching session info for: ${sessionId}`);
    const response = await axios.get(`/sessions/${sessionId}`);
    console.log('Session info response:', response.data);
    return response.data;
  } catch (error) {
    console.error('Error getting session info:', error);
    // Create a fallback session info with minimal data 
    // to prevent UI errors but show the problem
    return {
      _id: sessionId,
      name: "Disconnected Session",
      description: "Session information could not be retrieved. The session may have expired or been deleted.",
      created_at: new Date().toISOString(),
      status: "disconnected",
      error: true
    };
  }
};

export const listSessions = async () => {
  try {
    const response = await axios.get('/sessions');
    return response.data;
  } catch (error) {
    console.error('Error listing sessions:', error);
    throw error;
  }
};

export const deleteSession = async (sessionId: string) => {
  try {
    const response = await axios.delete(`/sessions/${sessionId}`);
    return response.data;
  } catch (error) {
    console.error('Error deleting session:', error);
    throw error;
  }
};

// Get all messages for a session
export const getSessionMessages = async (sessionId: string) => {
  try {
    const response = await axios.get(`/sessions/${sessionId}/messages`);
    return response.data;
  } catch (error) {
    console.error('Error fetching session messages:', error);
    throw error;
  }
};

// Query execution
export const executeQuery = async (question: string, sessionId?: string, autoFix: boolean = true, maxAttempts: number = 2) => {
  try {
    const queryParams: QueryRequest = {
      question,
      auto_fix: autoFix,
      max_attempts: maxAttempts,
    };

    console.log(`Executing query: "${question}" ${sessionId ? `with session ${sessionId}` : 'without session'}`);
    
    let response;
    
    if (sessionId) {
      // Execute query with session
      response = await axios.post(`/sessions/${sessionId}/query`, queryParams);
    } else {
      // Execute query without session (temporary session)
      response = await axios.post('/query', queryParams);
    }

    const responseData = response.data;
    console.log('Query response:', responseData);
    
    // Handle different response types based on query_type
    switch(responseData.query_type) {
      case 'conversational':
        // For conversational responses, just return the text
        return {
          ...responseData,
          message: responseData.text,
          query_type: 'conversational'
        };
      
      case 'analysis':
        // For analysis queries (multi-query or why analysis)
        // Make sure each table has a table_id
        const tablesWithIds = responseData.tables?.map((table: TableResponse) => ({
          ...table,
          // Use the table_id from pagination if available
          table_id: table.pagination?.table_id || table.table_id || `table-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`
        })) || [];
        
        return {
          ...responseData,
          message: responseData.text,
          tables: tablesWithIds,
          query_type: 'analysis',
          analysis_type: responseData.analysis_type
        };
      
      case 'sql':
      default:
        // For SQL queries, return the SQL and results
        return {
          ...responseData,
          message: responseData.text || 'Query executed successfully',
          query_type: 'sql',
          data: responseData.results  // Add data field that maps to results for table display
        };
    }
  } catch (error) {
    console.error('Error executing query:', error);
    // Return a standardized error response
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error occurred',
      query_type: 'conversational',
      text: error instanceof Error ? `Error: ${error.message}` : 'An unknown error occurred while executing your query.'
    };
  }
};

// Pagination
export const getPaginatedResults = async (sessionId: string, tableId: string, page: number = 1, pageSize: number = 10) => {
  try {
    console.log(`Fetching page ${page} for table ${tableId} in session ${sessionId}`);
    const response = await axios.get(
      `/sessions/${sessionId}/results/${tableId}?page=${page}&page_size=${pageSize}`
    );
    return response.data;
  } catch (error) {
    console.error(`Error fetching paginated results for table ${tableId}:`, error);
    throw error;
  }
};

// Admin functions
export const getSettings = async () => {
  try {
    const response = await axios.get('/settings');
    return response.data;
  } catch (error) {
    console.error('Error fetching settings:', error);
    throw error;
  }
};

export const toggleEditMode = async () => {
  try {
    const response = await axios.post('/toggle-edit-mode');
    return response.data;
  } catch (error) {
    console.error('Error toggling edit mode:', error);
    throw error;
  }
};

export const searchUser = async (email: string) => {
  try {
    const response = await axios.post('/admin/search-user', { email });
    return response.data;
  } catch (error) {
    console.error('Error searching user:', error);
    throw error;
  }
};

export const promoteUser = async (userEmail: string) => {
  try {
    const response = await axios.post('/admin/promote-user', { user_email: userEmail });
    return response.data;
  } catch (error) {
    console.error('Error promoting user:', error);
    throw error;
  }
};

export const executeSQL = async (sessionId: string, sql: string) => {
  try {
    const response = await axios.post(`/sessions/${sessionId}/execute-sql`, {
      sql: sql
    });
    return response.data;
  } catch (error) {
    console.error('Error executing SQL:', error);
    throw error;
  }
};

// Saved Queries API functions
export interface SavedQueryCreate {
  title: string;
  description?: string;
  sql: string;
  data: Record<string, unknown>[];
  table_name?: string;
}

export interface SavedQuery {
  id: string;
  title: string;
  description?: string;
  sql: string;
  data: Record<string, unknown>[];
  table_name?: string;
  user_id: string;
  session_id?: string;
  created_at: string;
  updated_at: string;
}

export const createSavedQuery = async (
  queryData: SavedQueryCreate,
  sessionId?: string
): Promise<SavedQuery> => {
  try {
    const params: Record<string, string> = {};
    if (sessionId) {
      params.session_id = sessionId;
    }
    
    const response = await axios.post('/saved-queries', queryData, { params });
    return response.data;
  } catch (error) {
    console.error('Error creating saved query:', error);
    throw error;
  }
};

export const getSavedQueries = async (
  sessionId?: string
): Promise<SavedQuery[]> => {
  try {
    const params: Record<string, string> = {};
    if (sessionId) {
      params.session_id = sessionId;
    }
    
    const response = await axios.get('/saved-queries', { params });
    return response.data;
  } catch (error) {
    console.error('Error fetching saved queries:', error);
    throw error;
  }
};

export const getSavedQuery = async (queryId: string): Promise<SavedQuery> => {
  try {
    const response = await axios.get(`/saved-queries/${queryId}`);
    return response.data;
  } catch (error) {
    console.error('Error fetching saved query:', error);
    throw error;
  }
};

export const updateSavedQuery = async (
  queryId: string,
  queryData: SavedQueryCreate
): Promise<SavedQuery> => {
  try {
    const response = await axios.put(`/saved-queries/${queryId}`, queryData);
    return response.data;
  } catch (error) {
    console.error('Error updating saved query:', error);
    throw error;
  }
};

export const deleteSavedQuery = async (queryId: string): Promise<void> => {
  try {
    await axios.delete(`/saved-queries/${queryId}`);
  } catch (error) {
    console.error('Error deleting saved query:', error);
    throw error;
  }
};

export const deleteAllSavedQueries = async (): Promise<void> => {
  try {
    await axios.delete('/saved-queries');
  } catch (error) {
    console.error('Error deleting all saved queries:', error);
    throw error;
  }
};

// Chart management APIs
export const saveChartToMessage = async (messageId: string, chartData: Record<string, unknown>) => {
  try {
    const response = await axios.post(`/messages/${messageId}/charts`, chartData);
    return response.data;
  } catch (error) {
    console.error('Error saving chart to message:', error);
    throw error;
  }
};

export const deleteChartFromMessage = async (messageId: string, chartId: string) => {
  try {
    const response = await axios.delete(`/messages/${messageId}/charts/${chartId}`);
    return response.data;
  } catch (error) {
    console.error('Error deleting chart from message:', error);
    throw error;
  }
};

export const getMessageCharts = async (messageId: string) => {
  try {
    const response = await axios.get(`/messages/${messageId}/charts`);
    return response.data;
  } catch (error) {
    console.error('Error getting message charts:', error);
    throw error;
  }
}; 