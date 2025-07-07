import { useState, useEffect } from 'react';
import { Plus, Database, MoreVertical, Edit, Trash2, Loader2, Play, RefreshCw, MessageSquare, History } from 'lucide-react';
import { getAllWorkspaces, createWorkspaceWithDetails, updateWorkspace, deleteWorkspace, activateWorkspace, WorkspaceRequest, listWorkspaceSessions, createSession } from '../lib/api';
import WorkspaceForm from './WorkspaceForm';
import SessionsList from './SessionsList';

interface Workspace {
  _id: string;
  name: string;
  description?: string;
  db_connection: {
    db_name: string;
    username: string;
    password: string;
    host: string;
    port: string;
    db_type: string;
  };
  created_at: string;
  updated_at: string;
}

interface WorkspaceManagerProps {
  onWorkspaceConnect: (workspaceId: string, autoSessionId?: string) => void;
}

export default function WorkspaceManager({ onWorkspaceConnect }: WorkspaceManagerProps) {
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [editingWorkspace, setEditingWorkspace] = useState<Workspace | null>(null);
  const [connectingWorkspace, setConnectingWorkspace] = useState<string | null>(null);
  const [showSessions, setShowSessions] = useState<string | null>(null);
  const [dropdownOpen, setDropdownOpen] = useState<string | null>(null);

  useEffect(() => {
    fetchWorkspaces();
  }, []);

  const fetchWorkspaces = async () => {
    try {
      setLoading(true);
      const data = await getAllWorkspaces();
      setWorkspaces(data);
    } catch (error) {
      console.error('Error fetching workspaces:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleCreateWorkspace = async (workspaceData: WorkspaceRequest) => {
    try {
      await createWorkspaceWithDetails(workspaceData);
      await fetchWorkspaces();
      setShowCreateForm(false);
    } catch (error) {
      console.error('Error creating workspace:', error);
      throw error;
    }
  };

  const handleUpdateWorkspace = async (workspaceId: string, workspaceData: Partial<WorkspaceRequest>) => {
    try {
      await updateWorkspace(workspaceId, workspaceData);
      await fetchWorkspaces();
      setEditingWorkspace(null);
    } catch (error) {
      console.error('Error updating workspace:', error);
      throw error;
    }
  };

  const handleDeleteWorkspace = async (workspaceId: string) => {
    if (!confirm('Are you sure you want to delete this workspace?')) return;
    
    try {
      await deleteWorkspace(workspaceId);
      await fetchWorkspaces();
    } catch (error) {
      console.error('Error deleting workspace:', error);
    }
  };

  const handleConnectWorkspace = async (workspaceId: string) => {
    try {
      setConnectingWorkspace(workspaceId);
      await activateWorkspace(workspaceId);
      
      // Fetch all sessions for this workspace and find the most recent one
      try {
        const sessions = await listWorkspaceSessions(workspaceId);
        if (sessions && sessions.length > 0) {
          // Sort by updated_at to find the most recent session
          const sortedSessions = sessions.sort((a: any, b: any) => 
            new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime()
          );
          const mostRecentSession = sortedSessions[0];
          onWorkspaceConnect(workspaceId, mostRecentSession._id);
        } else {
          // No sessions exist, create a new one automatically
          try {
            const newSession = await createSession({
              workspace_id: workspaceId,
              name: `Chat Session ${new Date().toLocaleString()}`,
              description: 'Auto-created session for new workspace connection'
            });
            console.log('Auto-created new session:', newSession._id);
            onWorkspaceConnect(workspaceId, newSession._id);
          } catch (createSessionError) {
            console.error('Error creating auto session:', createSessionError);
            // Fall back to connecting without a session
            onWorkspaceConnect(workspaceId);
          }
        }
      } catch (sessionError) {
        console.warn('Could not fetch sessions, connecting without auto-loading:', sessionError);
        onWorkspaceConnect(workspaceId);
      }
    } catch (error) {
      console.error('Error connecting to workspace:', error);
      alert('Failed to connect to database. Please check your connection settings.');
    } finally {
      setConnectingWorkspace(null);
    }
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="h-12 w-12 text-blue-400 animate-spin mx-auto mb-4" />
          <p className="text-gray-800 dark:text-gray-300">Loading workspaces...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 transition-colors duration-300">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-gray-900 dark:text-white mb-4">
            Welcome to NLP to SQL Assistant
          </h1>
          <p className="text-xl text-gray-800 dark:text-gray-300 max-w-3xl mx-auto">
            Connect to your databases and start querying with natural language
          </p>
        </div>

        {workspaces.length === 0 ? (
          <div className="text-center py-16">
            <div className="bg-white dark:bg-gray-800 p-12 rounded-2xl shadow-xl max-w-2xl mx-auto border border-gray-200 dark:border-gray-700 transition-colors duration-300">
              <Database className="h-16 w-16 text-blue-400 mx-auto mb-6" />
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">
                No Workspaces Found
              </h2>
              <p className="text-gray-800 dark:text-gray-300 mb-8 text-lg">
                Create your first workspace to connect to a database and start querying with natural language.
              </p>
              <button
                onClick={() => setShowCreateForm(true)}
                className="inline-flex items-center px-8 py-4 bg-gradient-to-r from-blue-500 to-purple-600 text-white font-semibold rounded-xl hover:from-blue-600 hover:to-purple-700 transition-all duration-200 shadow-lg hover:shadow-xl transform hover:-translate-y-1"
              >
                <Plus className="h-5 w-5 mr-2" />
                Create Your First Workspace
              </button>
            </div>
          </div>
        ) : (
          <div>
            <div className="flex justify-between items-center mb-8">
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white">Your Workspaces</h2>
              <button
                onClick={() => setShowCreateForm(true)}
                className="inline-flex items-center px-6 py-3 bg-gradient-to-r from-blue-500 to-purple-600 text-white font-semibold rounded-xl hover:from-blue-600 hover:to-purple-700 transition-all duration-200 shadow-lg hover:shadow-xl"
              >
                <Plus className="h-5 w-5 mr-2" />
                New Workspace
              </button>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {workspaces.map((workspace) => (
                <div
                  key={workspace._id}
                  className="bg-white dark:bg-gray-800 rounded-xl shadow-lg hover:shadow-xl transition-all duration-200 overflow-hidden border border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600"
                >
                  <div className="p-6">
                    <div className="flex justify-between items-start mb-4">
                      <div className="flex items-center space-x-3">
                        <div className="p-2 bg-gradient-to-r from-blue-500 to-purple-600 rounded-lg">
                          <Database className="h-5 w-5 text-white" />
                        </div>
                        <div>
                          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">{workspace.name}</h3>
                          <p className="text-sm text-gray-600 dark:text-gray-400">{workspace.db_connection.db_name}</p>
                        </div>
                      </div>
                      <div className="relative">
                        <button
                          onClick={() => setDropdownOpen(dropdownOpen === workspace._id ? null : workspace._id)}
                          className="p-2 text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
                        >
                          <MoreVertical className="h-4 w-4" />
                        </button>
                        
                        {dropdownOpen === workspace._id && (
                          <>
                            <div 
                              className="fixed inset-0 z-10" 
                              onClick={() => setDropdownOpen(null)}
                            />
                            <div className="absolute right-0 mt-1 w-48 bg-white dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded-lg shadow-xl z-20 transition-colors duration-300">
                              <button
                                onClick={() => {
                                  setEditingWorkspace(workspace);
                                  setDropdownOpen(null);
                                }}
                                className="flex items-center w-full px-4 py-2 text-sm text-gray-800 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600 hover:text-gray-900 dark:hover:text-white transition-colors"
                              >
                                <Edit className="h-4 w-4 mr-2" />
                                Edit
                              </button>
                              <button
                                onClick={() => {
                                  setShowSessions(workspace._id);
                                  setDropdownOpen(null);
                                }}
                                className="flex items-center w-full px-4 py-2 text-sm text-gray-800 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600 hover:text-gray-900 dark:hover:text-white transition-colors"
                              >
                                <History className="h-4 w-4 mr-2" />
                                View Sessions
                              </button>
                              <button
                                onClick={() => {
                                  handleDeleteWorkspace(workspace._id);
                                  setDropdownOpen(null);
                                }}
                                className="flex items-center w-full px-4 py-2 text-sm text-red-400 hover:bg-gray-600 hover:text-red-300 transition-colors rounded-b-lg"
                              >
                                <Trash2 className="h-4 w-4 mr-2" />
                                Delete
                              </button>
                            </div>
                          </>
                        )}
                      </div>
                    </div>

                    {workspace.description && (
                      <p className="text-gray-600 dark:text-gray-400 text-sm mb-4 line-clamp-2">
                        {workspace.description}
                      </p>
                    )}

                    <div className="space-y-2 mb-6">
                      <div className="flex items-center justify-between text-sm">
                        <span className="text-gray-600 dark:text-gray-400">Database:</span>
                        <span className="text-gray-800 dark:text-gray-300 font-medium">{workspace.db_connection.db_type}</span>
                      </div>
                      <div className="flex items-center justify-between text-sm">
                        <span className="text-gray-600 dark:text-gray-400">Host:</span>
                        <span className="text-gray-800 dark:text-gray-300 font-mono text-xs">{workspace.db_connection.host}:{workspace.db_connection.port}</span>
                      </div>
                      <div className="flex items-center justify-between text-sm">
                        <span className="text-gray-600 dark:text-gray-400">Updated:</span>
                        <span className="text-gray-800 dark:text-gray-300 text-xs">{formatDate(workspace.updated_at)}</span>
                      </div>
                    </div>

                    <div className="space-y-3">
                      <button
                        onClick={() => handleConnectWorkspace(workspace._id)}
                        disabled={connectingWorkspace === workspace._id}
                        className="w-full flex items-center justify-center px-4 py-3 bg-gradient-to-r from-blue-500 to-purple-600 text-white font-semibold rounded-lg hover:from-blue-600 hover:to-purple-700 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed transform hover:-translate-y-0.5"
                      >
                        {connectingWorkspace === workspace._id ? (
                          <>
                            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                            Connecting...
                          </>
                        ) : (
                          <>
                            <MessageSquare className="h-4 w-4 mr-2" />
                            Connect & Chat
                          </>
                        )}
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Workspace Form Modal */}
      {(showCreateForm || editingWorkspace) && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto transition-colors duration-300">
            <WorkspaceForm
              workspace={editingWorkspace}
              onSubmit={editingWorkspace ? 
                (data: WorkspaceRequest) => handleUpdateWorkspace(editingWorkspace._id, data) : 
                handleCreateWorkspace
              }
              onClose={() => {
                setShowCreateForm(false);
                setEditingWorkspace(null);
              }}
            />
          </div>
        </div>
      )}

      {/* Sessions List Modal */}
      {showSessions && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 shadow-2xl max-w-4xl w-full max-h-[90vh] transition-colors duration-300">
            <SessionsList
              workspaceId={showSessions}
              onClose={() => setShowSessions(null)}
              onSessionSelect={(sessionId) => {
                onWorkspaceConnect(showSessions, sessionId);
                setShowSessions(null);
              }}
            />
          </div>
        </div>
      )}
    </div>
  );
} 