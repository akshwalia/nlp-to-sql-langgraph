import { useState, useEffect } from 'react';
import { X, MessageSquare, Plus, Trash2, Calendar, Clock, Loader2 } from 'lucide-react';
import { listSessions, deleteSession, createSession } from '../lib/api';

interface Session {
  _id: string;
  name: string;
  description?: string;
  created_at: string;
  updated_at: string;
  message_count?: number;
}

interface SessionsListProps {
  onClose: () => void;
  onSessionSelect: (sessionId: string) => void;
}

export default function SessionsList({ onClose, onSessionSelect }: SessionsListProps) {
  const [sessions, setSessions] = useState<Session[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newSessionName, setNewSessionName] = useState('');
  const [newSessionDescription, setNewSessionDescription] = useState('');

  useEffect(() => {
    fetchSessions();
  }, []);

  const fetchSessions = async () => {
    try {
      setLoading(true);
      const data = await listSessions(); // Changed from listWorkspaceSessions
      setSessions(data);
    } catch (error) {
      console.error('Error fetching sessions:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleCreateSession = async () => {
    if (!newSessionName.trim()) return;

    try {
      setCreating(true);
      await createSession({
        name: newSessionName,
        description: newSessionDescription
      });
      await fetchSessions();
      setShowCreateForm(false);
      setNewSessionName('');
      setNewSessionDescription('');
    } catch (error) {
      console.error('Error creating session:', error);
    } finally {
      setCreating(false);
    }
  };

  const handleDeleteSession = async (sessionId: string) => {
    if (!confirm('Are you sure you want to delete this session?')) return;

    try {
      await deleteSession(sessionId);
      await fetchSessions();
    } catch (error) {
      console.error('Error deleting session:', error);
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

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-gray-800 rounded-2xl shadow-2xl w-full max-w-5xl max-h-[90vh] overflow-hidden border border-gray-700">
        <div className="flex items-center justify-between p-6 border-b border-gray-700">
          <div className="flex items-center space-x-3">
            <div className="p-2 bg-gradient-to-r from-blue-500 to-purple-600 rounded-lg">
              <MessageSquare className="h-5 w-5 text-white" />
            </div>
            <h2 className="text-xl font-bold text-white">Chat Sessions</h2>
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={() => setShowCreateForm(true)}
              className="flex items-center px-4 py-2 bg-gradient-to-r from-blue-500 to-purple-600 text-white font-semibold rounded-lg hover:from-blue-600 hover:to-purple-700 transition-all duration-200 shadow-lg"
            >
              <Plus className="h-4 w-4 mr-2" />
              New Session
            </button>
            <button
              onClick={onClose}
              className="p-2 text-gray-400 hover:text-white hover:bg-gray-700 rounded-lg transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>

        <div className="p-6 overflow-y-auto max-h-[calc(90vh-140px)]">
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-8 w-8 text-blue-400 animate-spin" />
            </div>
          ) : sessions.length === 0 ? (
            <div className="text-center py-16">
              <MessageSquare className="h-16 w-16 text-gray-400 mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-white mb-2">No Sessions Found</h3>
              <p className="text-gray-300 mb-6">Create your first chat session to start querying the database.</p>
              <button
                onClick={() => setShowCreateForm(true)}
                className="inline-flex items-center px-6 py-3 bg-gradient-to-r from-blue-500 to-purple-600 text-white font-semibold rounded-lg hover:from-blue-600 hover:to-purple-700 transition-all duration-200 shadow-lg"
              >
                <Plus className="h-5 w-5 mr-2" />
                Create First Session
              </button>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
              {sessions.map((session) => (
                <div
                  key={session._id}
                  className="bg-gray-700/50 border border-gray-600 rounded-xl p-5 hover:shadow-xl hover:bg-gray-700/70 transition-all duration-200 cursor-pointer group"
                  onClick={() => onSessionSelect(session._id)}
                >
                  <div className="flex justify-between items-start mb-4">
                    <div className="flex items-start space-x-3 min-w-0 flex-1">
                      <div className="p-2 bg-gradient-to-r from-blue-500/20 to-purple-500/20 rounded-lg flex-shrink-0">
                        <MessageSquare className="h-4 w-4 text-blue-400" />
                      </div>
                      <div className="min-w-0 flex-1">
                        <h3 className="font-semibold text-white text-sm leading-tight line-clamp-2 group-hover:text-blue-400 transition-colors">
                          {session.name}
                        </h3>
                      </div>
                    </div>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDeleteSession(session._id);
                      }}
                      className="p-1.5 text-gray-400 hover:text-red-400 hover:bg-red-500/10 rounded-lg transition-colors flex-shrink-0 ml-2"
                      title="Delete session"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </div>
                  
                  {session.description && (
                    <p className="text-gray-300 text-sm mb-4 line-clamp-3 leading-relaxed">
                      {session.description}
                    </p>
                  )}
                  
                  <div className="space-y-2">
                    <div className="flex items-center text-xs text-gray-400">
                      <Calendar className="h-3.5 w-3.5 mr-2 flex-shrink-0" />
                      <span className="truncate">{formatDate(session.created_at)}</span>
                    </div>
                    
                    {session.message_count !== undefined && (
                      <div className="flex items-center text-xs text-gray-400">
                        <MessageSquare className="h-3.5 w-3.5 mr-2 flex-shrink-0" />
                        <span>{session.message_count} {session.message_count === 1 ? 'message' : 'messages'}</span>
                      </div>
                    )}
                    
                    <div className="flex items-center text-xs text-gray-400">
                      <Clock className="h-3.5 w-3.5 mr-2 flex-shrink-0" />
                      <span className="truncate">Updated {formatDate(session.updated_at)}</span>
                    </div>
                  </div>
                  
                  <div className="mt-4 pt-3 border-t border-gray-600">
                    <div className="flex items-center justify-between">
                      <span className="text-xs text-gray-400">Click to open session</span>
                      <div className="h-2 w-2 rounded-full bg-emerald-400 group-hover:bg-emerald-300 transition-colors"></div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Create Session Form */}
        {showCreateForm && (
          <div className="absolute inset-0 bg-gray-800 rounded-2xl">
            <div className="flex items-center justify-between p-6 border-b border-gray-700">
              <h3 className="text-lg font-semibold text-white">Create New Session</h3>
              <button
                onClick={() => setShowCreateForm(false)}
                className="p-2 text-gray-400 hover:text-white hover:bg-gray-700 rounded-lg transition-colors"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            
            <div className="p-6 space-y-6">
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Session Name *
                </label>
                <input
                  type="text"
                  value={newSessionName}
                  onChange={(e) => setNewSessionName(e.target.value)}
                  className="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="Enter session name"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Description
                </label>
                <textarea
                  value={newSessionDescription}
                  onChange={(e) => setNewSessionDescription(e.target.value)}
                  rows={4}
                  className="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none"
                  placeholder="Enter session description (optional)"
                />
              </div>
              
              <div className="flex justify-end space-x-4 pt-4 border-t border-gray-700">
                <button
                  onClick={() => setShowCreateForm(false)}
                  className="px-6 py-2 text-gray-300 border border-gray-600 rounded-lg hover:bg-gray-700 hover:text-white transition-colors"
                >
                  Cancel
                </button>
                <button
                  onClick={handleCreateSession}
                  disabled={creating || !newSessionName.trim()}
                  className="flex items-center px-6 py-2 bg-gradient-to-r from-blue-500 to-purple-600 text-white font-semibold rounded-lg hover:from-blue-600 hover:to-purple-700 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed shadow-lg"
                >
                  {creating ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Creating...
                    </>
                  ) : (
                    <>
                      <Plus className="h-4 w-4 mr-2" />
                      Create Session
                    </>
                  )}
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
} 