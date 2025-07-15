'use client';

import { useAuth } from '../../lib/authContext';
import { useRouter } from 'next/navigation';
import { useEffect } from 'react';

export default function ChatBotLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const { isAuthenticated, loading } = useAuth();
  const router = useRouter();

  // Redirect to login if not authenticated
  useEffect(() => {
    if (!loading && !isAuthenticated) {
      router.push('/');
    }
  }, [isAuthenticated, loading, router]);

  // Show loading state
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-indigo-500"></div>
      </div>
    );
  }

  // Don't render anything if not authenticated (handled by useEffect)
  if (!isAuthenticated) {
    return null;
  }

  // Render children (chatbot content) if authenticated
  return <>{children}</>;
} 