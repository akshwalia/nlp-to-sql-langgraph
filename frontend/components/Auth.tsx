'use client';

import React, { useState } from 'react';
import { useAuth } from '../lib/authContext';
import { AlertCircle, EyeOff, Eye, Loader2, ArrowLeft, Sparkles, User, Mail, Lock } from 'lucide-react';
import ThemeToggle from './ThemeToggle';

type AuthMode = 'login' | 'register';

interface AuthProps {
  initialMode?: AuthMode;
  onBack?: () => void;
}

export default function Auth({ initialMode = 'login', onBack }: AuthProps) {
  const [mode, setMode] = useState<AuthMode>(initialMode);
  const [showPassword, setShowPassword] = useState(false);
  const { login, register, loading, error } = useAuth();
  
  const [formData, setFormData] = useState({
    email: '',
    password: '',
    firstName: '',
    lastName: '',
  });

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value,
    });
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (mode === 'login') {
      await login(formData.email, formData.password);
    } else {
      await register(formData.email, formData.password, formData.firstName, formData.lastName);
    }
  };

  const toggleMode = () => {
    setMode(mode === 'login' ? 'register' : 'login');
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-900 py-12 px-4 sm:px-6 lg:px-8 relative overflow-hidden transition-colors duration-300">
      {/* Animated background elements */}
      <div className="absolute top-10 left-10 w-32 h-32 bg-blue-500/5 rounded-full blur-2xl animate-float"></div>
      <div className="absolute bottom-10 right-10 w-24 h-24 bg-purple-500/5 rounded-full blur-2xl animate-float-delayed"></div>
      <div className="absolute top-1/3 right-1/4 w-16 h-16 bg-cyan-500/5 rounded-full blur-xl animate-bounce-slow"></div>
      
      <div className="max-w-md w-full space-y-8 bg-white/50 dark:bg-gray-800/50 backdrop-blur-sm p-8 rounded-2xl shadow-2xl border border-gray-200 dark:border-gray-700 animate-fade-in-scale transition-colors duration-300">
        {/* Header with logo */}
        <div className="text-center">
          <div className="flex items-center justify-between mb-6">
            <div className="flex-1" />
            <div className="p-3 bg-gradient-to-r from-blue-500 to-purple-600 rounded-2xl shadow-lg animate-pulse-subtle">
              <Sparkles className="h-8 w-8 text-white" />
            </div>
            <div className="flex-1 flex justify-end">
              <ThemeToggle size="sm" />
            </div>
          </div>
          <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent mb-2">
            AnalytIQ.AI
          </h1>
        </div>

        {/* Back button if provided */}
        {onBack && (
          <button
            onClick={onBack}
            className="flex items-center space-x-2 text-gray-700 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white transition-all duration-200 mb-4 hover:scale-105"
          >
            <ArrowLeft className="h-4 w-4" />
            <span>Back to home</span>
          </button>
        )}
        
        <div>
          <h2 className="text-center text-3xl font-extrabold text-gray-900 dark:text-white mb-2">
            {mode === 'login' ? 'Welcome back' : 'Join AnalytIQ.AI'}
          </h2>
          <p className="text-center text-gray-800 dark:text-gray-400 mb-2">
            {mode === 'login' ? 'Sign in to your account' : 'Create your account to get started'}
          </p>
          <p className="text-center text-sm text-gray-800 dark:text-gray-400">
            {mode === 'login' ? "Don't have an account? " : "Already have an account? "}
            <button
              onClick={toggleMode}
              className="font-medium text-blue-500 dark:text-blue-400 hover:text-blue-600 dark:hover:text-blue-300 focus:outline-none transition-colors duration-200 hover:scale-105 inline-block"
            >
              {mode === 'login' ? 'Sign up here' : 'Sign in here'}
            </button>
          </p>
        </div>
        
        {error && (
          <div className="bg-red-50 dark:bg-red-900/50 border border-red-400 dark:border-red-500/50 rounded-lg p-4 animate-slide-up-fade transition-colors duration-300">
            <div className="flex items-start">
              <AlertCircle className="h-5 w-5 text-red-600 dark:text-red-400 mt-0.5 mr-3 flex-shrink-0" />
              <p className="text-sm text-red-700 dark:text-red-300">{error}</p>
            </div>
          </div>
        )}
        
        <form className="mt-8 space-y-6" onSubmit={handleSubmit}>
          <div className="space-y-5">
            <div className="space-y-2">
              <label htmlFor="email" className="block text-sm font-medium text-gray-800 dark:text-gray-300">
                <div className="flex items-center space-x-2">
                  <Mail className="h-4 w-4 text-blue-500 dark:text-blue-400" />
                  <span>Email address</span>
                </div>
              </label>
              <input
                id="email"
                name="email"
                type="email"
                autoComplete="email"
                required
                value={formData.email}
                onChange={handleChange}
                placeholder="Enter your email"
                className="block w-full px-4 py-3 bg-gray-100 dark:bg-gray-700/50 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-900 dark:text-white placeholder-gray-500 dark:placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200 hover:bg-gray-200 dark:hover:bg-gray-700/70"
              />
            </div>
            
            <div className="space-y-2">
              <label htmlFor="password" className="block text-sm font-medium text-gray-800 dark:text-gray-300">
                <div className="flex items-center space-x-2">
                  <Lock className="h-4 w-4 text-blue-500 dark:text-blue-400" />
                  <span>Password</span>
                </div>
              </label>
              <div className="relative">
                <input
                  id="password"
                  name="password"
                  type={showPassword ? "text" : "password"}
                  autoComplete={mode === 'login' ? "current-password" : "new-password"}
                  required
                  value={formData.password}
                  onChange={handleChange}
                  placeholder="Enter your password"
                  className="block w-full px-4 py-3 bg-gray-100 dark:bg-gray-700/50 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-900 dark:text-white placeholder-gray-500 dark:placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200 hover:bg-gray-200 dark:hover:bg-gray-700/70 pr-12"
                />
                <button
                  type="button"
                  className="absolute inset-y-0 right-0 flex items-center pr-4 text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white transition-colors duration-200"
                  onClick={() => setShowPassword(!showPassword)}
                >
                  {showPassword ? (
                    <EyeOff className="h-5 w-5" />
                  ) : (
                    <Eye className="h-5 w-5" />
                  )}
                </button>
              </div>
            </div>
            
            {mode === 'register' && (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 animate-slide-up-fade">
                <div className="space-y-2">
                  <label htmlFor="firstName" className="block text-sm font-medium text-gray-800 dark:text-gray-300">
                    <div className="flex items-center space-x-2">
                      <User className="h-4 w-4 text-blue-500 dark:text-blue-400" />
                      <span>First name</span>
                    </div>
                  </label>
                  <input
                    id="firstName"
                    name="firstName"
                    type="text"
                    autoComplete="given-name"
                    required
                    value={formData.firstName}
                    onChange={handleChange}
                    placeholder="First name"
                    className="block w-full px-4 py-3 bg-gray-100 dark:bg-gray-700/50 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-900 dark:text-white placeholder-gray-500 dark:placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200 hover:bg-gray-200 dark:hover:bg-gray-700/70"
                  />
                </div>
                
                <div className="space-y-2">
                  <label htmlFor="lastName" className="block text-sm font-medium text-gray-800 dark:text-gray-300">
                    <div className="flex items-center space-x-2">
                      <User className="h-4 w-4 text-blue-500 dark:text-blue-400" />
                      <span>Last name</span>
                    </div>
                  </label>
                  <input
                    id="lastName"
                    name="lastName"
                    type="text"
                    autoComplete="family-name"
                    value={formData.lastName}
                    onChange={handleChange}
                    placeholder="Last name"
                    className="block w-full px-4 py-3 bg-gray-100 dark:bg-gray-700/50 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-900 dark:text-white placeholder-gray-500 dark:placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200 hover:bg-gray-200 dark:hover:bg-gray-700/70"
                  />
                </div>
              </div>
            )}
          </div>

          <div>
            <button
              type="submit"
              disabled={loading}
              className="group relative w-full flex justify-center py-3 px-4 border border-transparent text-sm font-semibold rounded-lg text-white bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed hover:scale-105 animate-pulse-glow shadow-lg hover:shadow-xl"
            >
              {loading ? (
                <div className="flex items-center space-x-2">
                  <Loader2 className="h-5 w-5 animate-spin text-white" />
                  <span>Please wait...</span>
                </div>
              ) : (
                <span>{mode === 'login' ? 'Sign in' : 'Create account'}</span>
              )}
            </button>
          </div>
        </form>

        {/* Additional footer */}
        <div className="text-center">
          <p className="text-xs text-gray-700 dark:text-gray-500">
            By {mode === 'login' ? 'signing in' : 'creating an account'}, you agree to our Terms of Service
          </p>
        </div>
      </div>
    </div>
  );
} 