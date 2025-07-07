'use client';

import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';
import { useRouter } from 'next/navigation';
import { useAuth } from '../../lib/authContext';
import UserProfile from '../../components/UserProfile';
import WorkspaceManager from '../../components/WorkspaceManager';

// Dynamically import the ChatBot component with no SSR to avoid hydration issues
const ChatBot = dynamic(() => import('../../components/ChatBot'), { ssr: false });

export default function Dashboard() {
  const { isAuthenticated, loading } = useAuth();
  const router = useRouter();
  const [connectedWorkspace, setConnectedWorkspace] = useState<string | null>(null);
  const [autoSessionId, setAutoSessionId] = useState<string | null>(null);
  const [showWorkspaceManager, setShowWorkspaceManager] = useState(true);

  useEffect(() => {
    if (!loading && !isAuthenticated) {
      router.push('/');
    }
  }, [isAuthenticated, loading, router]);

  const handleWorkspaceConnect = (workspaceId: string, sessionId?: string) => {
    setConnectedWorkspace(workspaceId);
    setAutoSessionId(sessionId || null);
    setShowWorkspaceManager(false);
    
    // If an auto session ID is provided, it will be handled by the ChatBot component
    if (sessionId) {
      console.log('Auto-loading session:', sessionId);
    }
  };

  const handleBackToWorkspaces = () => {
    setConnectedWorkspace(null);
    setAutoSessionId(null);
    setShowWorkspaceManager(true);
  };

  // Show loading state
  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="flex items-center space-x-3">
          <div className="animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-blue-500"></div>
          <span className="text-gray-300">Loading...</span>
        </div>
      </div>
    );
  }

  // Redirect if not authenticated (handled by useEffect, but just in case)
  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen flex flex-col bg-gray-900">
      {/* Header only shown for workspace manager */}
      {showWorkspaceManager && (
        <header className="bg-gray-800 border-b border-gray-700 shadow-sm">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex justify-between items-center">
            <div className="flex items-center space-x-4">
              <h1 className="text-xl font-semibold text-white">NLP to SQL Assistant</h1>
            </div>
            <UserProfile />
          </div>
        </header>
      )}

      {/* Main content */}
      <main className="flex-grow bg-gray-900">
        {showWorkspaceManager ? (
          <WorkspaceManager onWorkspaceConnect={handleWorkspaceConnect} />
        ) : (
          <ChatBot 
            workspaceId={connectedWorkspace} 
            autoSessionId={autoSessionId}
            onBackToWorkspaces={handleBackToWorkspaces}
          />
        )}
      </main>
    </div>
  );
}