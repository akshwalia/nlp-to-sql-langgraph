import { useState } from 'react';
import { X, Database, AlertCircle, Loader2 } from 'lucide-react';
import axios from 'axios';

interface SessionManagerProps {
  onSessionCreated: (sessionId: string) => void;
  isOpen: boolean;
  onClose: () => void;
}

// Define the session request interface
interface SessionRequest {
  db_name: string;
  db_path: string;
  db_type: 'sqlite';
  // Legacy PostgreSQL fields (optional)
  username?: string;
  password?: string;
  host?: string;
  port?: string;
  use_memory: boolean;
  use_cache: boolean;
}

export default function SessionManager({ onSessionCreated, isOpen, onClose }: SessionManagerProps) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [formData, setFormData] = useState<SessionRequest>({
    db_name: 'PBTest',
    db_path: './data/PBTest.db',
    db_type: 'sqlite',
    // Legacy PostgreSQL fields (for backward compatibility)
    username: '',
    password: '',
    host: '',
    port: '',
    use_memory: true,
    use_cache: true,
  });

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value, type, checked } = e.target;
    setFormData({
      ...formData,
      [name]: type === 'checkbox' ? checked : value
    });
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError(null);

    // API base URL
    const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

    try {
      // Get auth headers
      const token = localStorage.getItem('auth_token');
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      };
      
      if (token) {
        headers['Authorization'] = `Bearer ${token}`;
      }

      // Try to create a session directly first
      try {
        const directSessionResponse = await axios.post(
          `${API_BASE_URL}/sessions`, 
          formData,
          { headers }
        );
        
        console.log('Session created:', directSessionResponse.data);
        onSessionCreated(directSessionResponse.data.id || directSessionResponse.data._id || directSessionResponse.data.session_id);
        onClose();
        return;
      } catch (directSessionError) {
        console.log('Direct session creation failed, trying workspace flow:', directSessionError);
        // Continue with workspace flow
      }
      
      // Step 1: Create a new workspace with the database connection details
      const workspaceRequest = {
        name: `${formData.db_name} Workspace`,
        description: `Workspace for ${formData.db_type} database: ${formData.db_path || formData.host}`,
        db_connection: {
          db_name: formData.db_name,
          db_path: formData.db_path,
          db_type: formData.db_type,
          // Include legacy fields if they exist
          ...(formData.username && { username: formData.username }),
          ...(formData.password && { password: formData.password }),
          ...(formData.host && { host: formData.host }),
          ...(formData.port && { port: formData.port })
        }
      };

      const workspaceResponse = await axios.post(
        `${API_BASE_URL}/workspaces`, 
        workspaceRequest,
        { headers }
      );
      
      console.log('Workspace created:', workspaceResponse.data);
      const newWorkspaceId = workspaceResponse.data.id || workspaceResponse.data._id;
      
      // Step 2: Activate the workspace (connect to database)
      const activationResponse = await axios.post(
        `${API_BASE_URL}/workspaces/${newWorkspaceId}/activate`,
        {}, // Empty body
        { headers }
      );
      
      console.log('Workspace activated:', activationResponse.data);
      
      // Step 3: Create a session in the workspace
      const sessionRequest = {
        name: `${formData.db_name} Session`,
        description: `Session for ${formData.host} database`,
        workspace_id: newWorkspaceId
      };
      
      const sessionResponse = await axios.post(
        `${API_BASE_URL}/workspaces/${newWorkspaceId}/sessions`,
        sessionRequest,
        { headers }
      );
      
      console.log('Session created:', sessionResponse.data);
      
      // Pass the session ID back
      onSessionCreated(sessionResponse.data.id || sessionResponse.data._id);
      onClose();
    } catch (err: unknown) {
      console.error('Session creation error:', err);
      let errorMsg = 'Failed to create session';
      
      if (axios.isAxiosError(err)) {
        if (err.response) {
          // The request was made and the server responded with a non-2xx status
          errorMsg = err.response.data?.detail || `Error: ${err.response.status} ${err.response.statusText}`;
        } else if (err.request) {
          // The request was made but no response was received
          errorMsg = 'No response from server. Please check if the backend is running.';
        } else {
          // Something happened in setting up the request
          errorMsg = err.message || 'Unknown error occurred';
        }
      } else if (err instanceof Error) {
        errorMsg = err.message;
      }
      
      setError(errorMsg);
    } finally {
      setIsLoading(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 p-4 animate-in fade-in duration-300">
      <div className="bg-white rounded-2xl p-6 w-full max-w-lg shadow-2xl border border-gray-200 animate-in zoom-in-95 duration-300">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center space-x-3">
            <div className="p-2 bg-gradient-to-r from-blue-600 to-indigo-600 rounded-xl">
              <Database className="h-5 w-5 text-white" />
            </div>
            <h2 className="text-xl font-bold text-gray-800">Connect to Database</h2>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-xl transition-colors duration-200"
            disabled={isLoading}
          >
            <X className="h-5 w-5 text-gray-500" />
          </button>
        </div>
        
        {/* Error Message */}
        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-xl animate-in slide-in-from-top-2 duration-300">
            <div className="flex items-center space-x-2">
              <AlertCircle className="h-5 w-5 text-red-500 flex-shrink-0" />
              <p className="text-sm text-red-700 font-medium">{error}</p>
            </div>
          </div>
        )}
        
        {/* Connection Form */}
        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Database Type - SQLite Only */}
          <div>
            <label className="block text-sm font-medium text-gray-700">Database Type</label>
            <div className="w-full px-4 py-2 border border-gray-300 rounded-lg mt-1 bg-gray-50 text-gray-700">
              SQLite Database
            </div>
          </div>

          {/* SQLite Configuration */}
          <div className="space-y-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
            <h3 className="text-sm font-medium text-blue-800">SQLite Configuration</h3>
            <div className="grid grid-cols-1 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700">Database Name</label>
                <input
                  type="text"
                  name="db_name"
                  value={formData.db_name}
                  onChange={handleChange}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg mt-1"
                  disabled={isLoading}
                  placeholder="PBTest"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700">Database File Path</label>
                <input
                  type="text"
                  name="db_path"
                  value={formData.db_path}
                  onChange={handleChange}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg mt-1"
                  disabled={isLoading}
                  placeholder="./data/PBTest.db"
                />
              </div>
            </div>
          </div>
          
          <div className="flex items-center space-x-4">
            <div className="flex items-center">
              <input
                type="checkbox"
                name="use_memory"
                checked={formData.use_memory}
                onChange={handleChange}
                className="h-4 w-4 text-blue-600 border-gray-300 rounded"
                disabled={isLoading}
              />
              <label className="ml-2 text-sm text-gray-700">Use Memory</label>
            </div>
            
            <div className="flex items-center">
              <input
                type="checkbox"
                name="use_cache"
                checked={formData.use_cache}
                onChange={handleChange}
                className="h-4 w-4 text-blue-600 border-gray-300 rounded"
                disabled={isLoading}
              />
              <label className="ml-2 text-sm text-gray-700">Use Cache</label>
            </div>
          </div>
          
          <div className="flex justify-end">
            <button
              type="submit"
              className="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg flex items-center space-x-2"
              disabled={isLoading}
            >
              {isLoading ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span>Connecting...</span>
                </>
              ) : (
                <span>Connect</span>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}