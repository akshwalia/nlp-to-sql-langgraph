'use client';

import React, { useState } from 'react';
import { useAuth } from '../lib/authContext';
import { LogOut, ChevronDown, Settings } from 'lucide-react';

export default function UserProfile() {
  const { user, logout, isAuthenticated } = useAuth();
  const [dropdownOpen, setDropdownOpen] = useState(false);

  if (!isAuthenticated || !user) {
    return null;
  }

  // Format the user's name or use email if no name available
  const displayName = user.first_name 
    ? `${user.first_name} ${user.last_name || ''}`.trim()
    : user.email.split('@')[0];

  // Format the first letter of name for avatar
  const avatarInitial = displayName[0].toUpperCase();

  const toggleDropdown = () => {
    setDropdownOpen(!dropdownOpen);
  };

  return (
    <div className="relative">
      <button 
        onClick={toggleDropdown}
        className="flex items-center space-x-2 p-2 rounded-lg hover:bg-gray-700 transition-colors duration-200"
      >
        <div className="bg-gradient-to-r from-blue-500 to-purple-600 text-white rounded-full h-8 w-8 flex items-center justify-center font-medium">
          {avatarInitial}
        </div>
        <span className="font-medium text-gray-300 hidden sm:block">{displayName}</span>
        <ChevronDown className="h-4 w-4 text-gray-400" />
      </button>

      {dropdownOpen && (
        <div 
          className="absolute right-0 mt-2 w-48 py-2 bg-gray-800 border border-gray-700 rounded-lg shadow-xl z-20"
          onClick={() => setDropdownOpen(false)}
        >
          <div className="px-4 py-2 border-b border-gray-700">
            <p className="text-sm font-medium text-white">{displayName}</p>
            <p className="text-xs text-gray-400 truncate">{user.email}</p>
          </div>
          
          <a 
            href="#" 
            className="flex items-center px-4 py-2 text-sm text-gray-300 hover:bg-gray-700 hover:text-white transition-colors"
          >
            <Settings className="h-4 w-4 mr-2" />
            Account settings
          </a>
          
          <button 
            onClick={logout}
            className="flex items-center w-full text-left px-4 py-2 text-sm text-red-400 hover:bg-gray-700 hover:text-red-300 transition-colors"
          >
            <LogOut className="h-4 w-4 mr-2" />
            Sign out
          </button>
        </div>
      )}
      
      {/* Backdrop to close dropdown when clicking outside */}
      {dropdownOpen && (
        <div 
          className="fixed inset-0 z-10" 
          onClick={() => setDropdownOpen(false)}
        />
      )}
    </div>
  );
} 