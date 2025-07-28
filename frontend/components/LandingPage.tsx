'use client';

import React from 'react';
import { ArrowRight, Database, Brain, BarChart3, MessageSquare, TrendingUp, Sparkles, Zap } from 'lucide-react';
import ThemeToggle from './ThemeToggle';

interface LandingPageProps {
  onLoginClick: () => void;
  onRegisterClick: () => void;
}

const features = [
  {
    icon: Brain,
    title: "Natural Language Queries",
    description: "Type questions in plain English and get instant SQL queries. No complex syntax required.",
    gradient: "from-blue-500 to-cyan-500",
    delay: "0.1s"
  },
  {
    icon: BarChart3,
    title: "Smart Visualizations",
    description: "Transform data into beautiful charts and interactive visualizations automatically.",
    gradient: "from-purple-500 to-pink-500",
    delay: "0.2s"
  },
  {
    icon: Database,
    title: "Multi-Database Support",
    description: "Works with PostgreSQL, MySQL, SQLite, and other popular database systems.",
    gradient: "from-green-500 to-emerald-500",
    delay: "0.3s"
  },
  {
    icon: MessageSquare,
    title: "Conversation Memory",
    description: "Build upon previous queries with intelligent context and chat history.",
    gradient: "from-orange-500 to-red-500",
    delay: "0.4s"
  },
  {
    icon: TrendingUp,
    title: "Automated Insights",
    description: "Get statistical summaries and discover hidden patterns in your data.",
    gradient: "from-indigo-500 to-blue-500",
    delay: "0.5s"
  },
  {
    icon: Zap,
    title: "Lightning Fast",
    description: "Real-time query execution with intelligent suggestions and optimization.",
    gradient: "from-yellow-500 to-orange-500",
    delay: "0.6s"
  }
];

export default function LandingPage({ onLoginClick, onRegisterClick }: LandingPageProps) {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900 text-gray-900 dark:text-white overflow-hidden relative transition-colors duration-300">
      {/* Animated Background Bubbles */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="bubble bubble-1"></div>
        <div className="bubble bubble-2"></div>
        <div className="bubble bubble-3"></div>
        <div className="bubble bubble-4"></div>
        <div className="bubble bubble-5"></div>
        <div className="bubble bubble-6"></div>
        <div className="bubble bubble-7"></div>
        <div className="bubble bubble-8"></div>
      </div>

      {/* Navigation */}
      <nav className="bg-white/30 dark:bg-gray-800/30 backdrop-blur-md border-b border-gray-200/50 dark:border-gray-700/50 sticky top-0 z-50 animate-slide-down transition-colors duration-300">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center space-x-3">
              <div className="p-2 bg-gradient-to-r from-blue-500 to-purple-600 rounded-xl shadow-lg animate-pulse-subtle">
                <Sparkles className="h-6 w-6 text-white" />
              </div>
              <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
                AnalytIQ.AI
              </h1>
            </div>
            <div className="flex items-center space-x-4">
              <ThemeToggle size="sm" />
              <button
                onClick={onLoginClick}
                className="px-4 py-2 text-gray-800 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white transition-all duration-200 hover:scale-105"
              >
                Login
              </button>
              <button
                onClick={onRegisterClick}
                className="px-6 py-2 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 rounded-lg font-medium transition-all duration-200 shadow-lg hover:shadow-xl transform hover:scale-105 text-white"
              >
                Register
              </button>
            </div>
          </div>
        </div>
      </nav>

      {/* Hero Section */}
      <section className="relative">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-24">
          <div className="text-center">
            <h1 className="text-5xl md:text-7xl font-extrabold mb-6 leading-tight animate-fade-in-up">
              <span className="bg-gradient-to-r from-blue-400 via-purple-400 to-pink-400 bg-clip-text text-transparent">
                AnalytIQ.AI
              </span>
            </h1>
            <p className="text-2xl md:text-3xl font-light text-gray-800 dark:text-gray-300 mb-4 animate-fade-in-up-delayed">
              Intelligent SQL Made Simple
            </p>
            <p className="text-lg md:text-xl text-gray-700 dark:text-gray-400 mb-8 max-w-3xl mx-auto leading-relaxed animate-fade-in-up-delayed">
              Transform natural language into powerful SQL queries with AI-driven analytics and visualization.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center items-center animate-fade-in-up-delayed-2">
              <button
                onClick={onRegisterClick}
                className="group px-8 py-4 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 rounded-xl font-semibold text-lg transition-all duration-300 shadow-2xl hover:shadow-blue-500/25 transform hover:scale-105 flex items-center space-x-2"
              >
                <span>Get Started</span>
                <ArrowRight className="h-5 w-5 group-hover:translate-x-1 transition-transform" />
              </button>
              <button
                onClick={onLoginClick}
                className="px-8 py-4 border-2 border-gray-300 dark:border-gray-600 hover:border-gray-400 dark:hover:border-gray-500 rounded-xl font-semibold text-lg transition-all duration-300 hover:bg-gray-100 dark:hover:bg-gray-800/50 hover:scale-105"
              >
                Login
              </button>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section className="py-24 relative">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h2 className="text-4xl md:text-5xl font-bold mb-6 animate-fade-in-up">
              <span className="bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
                Core Features
              </span>
            </h2>
            <p className="text-xl text-gray-800 dark:text-gray-300 max-w-3xl mx-auto animate-fade-in-up-delayed">
              Everything you need to unlock the power of your data with intelligent SQL generation
            </p>
          </div>
          
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
            {features.map((feature, index) => (
              <div
                key={index}
                className="group bg-white/30 dark:bg-gray-800/30 backdrop-blur-sm border border-gray-200/50 dark:border-gray-700/50 rounded-2xl p-8 hover:border-gray-300/50 dark:hover:border-gray-600/50 transition-all duration-500 hover:transform hover:scale-105 hover:shadow-2xl hover:shadow-blue-500/10 animate-slide-up hover:bg-gray-50/50 dark:hover:bg-gray-800/50"
                style={{ animationDelay: feature.delay }}
              >
                <div className={`inline-flex p-4 rounded-2xl bg-gradient-to-r ${feature.gradient} mb-6 shadow-lg group-hover:shadow-xl transition-all duration-300 group-hover:scale-110`}>
                  <feature.icon className="h-8 w-8 text-white" />
                </div>
                <h3 className="text-xl font-bold mb-4 text-gray-900 dark:text-white group-hover:text-blue-400 transition-colors duration-300">
                  {feature.title}
                </h3>
                <p className="text-gray-800 dark:text-gray-300 leading-relaxed group-hover:text-gray-900 dark:group-hover:text-gray-200 transition-colors duration-300">
                  {feature.description}
                </p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Call to Action Section */}
      <section className="py-24 relative">
        <div className="max-w-4xl mx-auto text-center px-4 sm:px-6 lg:px-8">
          <h2 className="text-4xl md:text-5xl font-bold mb-6 animate-fade-in-up">
            Ready to Transform
            <span className="bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent"> Your Data?</span>
          </h2>
          <p className="text-xl text-gray-300 mb-8 max-w-2xl mx-auto animate-fade-in-up-delayed">
            Experience intelligent database interaction with AI-powered insights and natural language processing.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center animate-fade-in-up-delayed-2">
            <button
              onClick={onRegisterClick}
              className="group px-8 py-4 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 rounded-xl font-semibold text-lg transition-all duration-300 shadow-2xl hover:shadow-blue-500/25 transform hover:scale-105 flex items-center justify-center space-x-2"
            >
              <span>Start Free</span>
              <TrendingUp className="h-5 w-5 group-hover:translate-x-1 transition-transform" />
            </button>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="bg-white/30 dark:bg-gray-800/30 border-t border-gray-200/50 dark:border-gray-700/50 backdrop-blur-sm transition-colors duration-300">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="flex flex-col md:flex-row justify-between items-center">
            <div className="flex items-center space-x-3 mb-4 md:mb-0">
              <div className="p-2 bg-gradient-to-r from-blue-500 to-purple-600 rounded-xl animate-pulse-subtle">
                <Sparkles className="h-5 w-5 text-white" />
              </div>
              <span className="text-lg font-bold bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
                AnalytIQ.AI
              </span>
            </div>
                          <p className="text-gray-700 dark:text-gray-400 text-sm text-center md:text-right">
                © 2024 AnalytIQ.AI. Intelligent data transformation.
              </p>
          </div>
        </div>
      </footer>
    </div>
  );
}