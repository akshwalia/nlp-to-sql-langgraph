'use client';

import { useEffect, useState } from 'react';
import dynamic from 'next/dynamic';
import { useRouter } from 'next/navigation';
import { useAuth } from '../lib/authContext';
import Auth from '../components/Auth';
import LandingPage from '../components/LandingPage';

// Dynamically import the ChatBot component with no SSR to avoid hydration issues
const ChatBot = dynamic(() => import('../components/ChatBot'), { ssr: false });

type PageState = 'landing' | 'login' | 'register';

export default function Home() {
  const { isAuthenticated, loading } = useAuth();
  const router = useRouter();
  const [currentPage, setCurrentPage] = useState<PageState>('landing');

  useEffect(() => {
    if (!loading && isAuthenticated) {
      router.push('/dashboard');
    }
  }, [isAuthenticated, loading, router]);

  // Show loading state
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-900">
        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
      </div>
    );
  }

  // Show different pages based on state if not authenticated
  if (!isAuthenticated) {
    if (currentPage === 'login') {
      return (
        <Auth 
          initialMode="login" 
          onBack={() => setCurrentPage('landing')} 
        />
      );
    }
    
    if (currentPage === 'register') {
      return (
        <Auth 
          initialMode="register" 
          onBack={() => setCurrentPage('landing')} 
        />
      );
    }
    
    // Default to landing page
    return (
      <LandingPage
        onLoginClick={() => setCurrentPage('login')}
        onRegisterClick={() => setCurrentPage('register')}
      />
    );
  }

  // This should not render due to the redirect in useEffect, but just in case
  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-900">
      <p className="text-white">Redirecting to dashboard...</p>
    </div>
  );
} 