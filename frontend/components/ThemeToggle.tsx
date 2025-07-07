'use client';

import { Sun, Moon } from 'lucide-react';
import { useTheme } from '../lib/themeContext';

interface ThemeToggleProps {
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

export default function ThemeToggle({ size = 'md', className = '' }: ThemeToggleProps) {
  const { theme, toggleTheme } = useTheme();

  const sizeClasses = {
    sm: 'p-1.5 h-8 w-8',
    md: 'p-2 h-9 w-9',
    lg: 'p-2.5 h-10 w-10',
  };

  const iconSizes = {
    sm: 'h-4 w-4',
    md: 'h-5 w-5',
    lg: 'h-6 w-6',
  };

  return (
    <button
      onClick={toggleTheme}
      className={`
        ${sizeClasses[size]}
        ${className}
        relative overflow-hidden rounded-lg
        bg-gray-200 hover:bg-gray-300 dark:bg-gray-700 dark:hover:bg-gray-600
        border border-gray-300 dark:border-gray-600
        text-gray-600 hover:text-gray-800 dark:text-gray-300 dark:hover:text-white
        transition-all duration-300 ease-in-out
        transform hover:scale-105 active:scale-95
        focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 dark:focus:ring-offset-gray-800
        shadow-sm hover:shadow-md
      `}
      title={theme === 'light' ? 'Switch to dark mode' : 'Switch to light mode'}
      aria-label={theme === 'light' ? 'Switch to dark mode' : 'Switch to light mode'}
    >
      <div className="relative flex items-center justify-center">
        <Sun 
          className={`
            ${iconSizes[size]} 
            absolute transition-all duration-300 ease-in-out
            ${theme === 'light' 
              ? 'opacity-100 rotate-0 scale-100' 
              : 'opacity-0 -rotate-90 scale-75'
            }
          `} 
        />
        <Moon 
          className={`
            ${iconSizes[size]} 
            absolute transition-all duration-300 ease-in-out
            ${theme === 'dark' 
              ? 'opacity-100 rotate-0 scale-100' 
              : 'opacity-0 rotate-90 scale-75'
            }
          `} 
        />
      </div>
    </button>
  );
} 