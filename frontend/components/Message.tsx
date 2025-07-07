import { User, Bot } from 'lucide-react';
import { formatRelative } from 'date-fns';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface MessageProps {
  isUser: boolean;
  content: string;
  timestamp: Date;
  isConversational?: boolean;
}

export default function Message({ isUser, content, timestamp, isConversational = false }: MessageProps) {
  const formattedTime = formatRelative(new Date(timestamp), new Date());
  
  // Fix table formatting issues by ensuring proper markdown syntax
  let processedContent = content;
  
  // Fix common table formatting issues
  if (content.includes('|')) {
    // Remove extra spaces between table rows that break markdown tables
    processedContent = content
      .replace(/\|\s*\n\s*\|/g, '|\n|')
      .replace(/\|\s*\n\n\s*\|/g, '|\n|');
    
    // Fix malformed table headers/separators
    const lines = processedContent.split('\n');
    for (let i = 0; i < lines.length; i++) {
      // If this looks like a table separator row with errors
      if (lines[i].trim().startsWith('|') && lines[i].includes('--') && !lines[i].includes('|--')) {
        // Fix the separator line format
        lines[i] = lines[i].replace(/\s*\|\s*/g, ' | ').replace(/\s*-+\s*/g, ' --- ');
      }
    }
    processedContent = lines.join('\n');
  }

  return (
    <div className={`flex gap-4 ${isUser ? 'justify-end' : ''} group`}>
      {!isUser && (
        <div className="flex-shrink-0">
          <div className="h-10 w-10 rounded-full shadow-sm bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center transition-all duration-300 group-hover:shadow-lg group-hover:scale-105">
            <Bot className="h-5 w-5 text-white" />
          </div>
        </div>
      )}
      
      <div className={`max-w-[75%] sm:max-w-[65%] ${isUser ? 'order-1' : 'order-2'}`}>
        <div 
          className={`px-5 py-4 rounded-2xl shadow-sm transition-all duration-300 group-hover:shadow-lg transform group-hover:-translate-y-0.5 ${
            isUser
              ? 'bg-gradient-to-r from-blue-500 to-purple-600 text-white shadow-lg hover:from-blue-600 hover:to-purple-700'
              : isConversational
                ? 'bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-750 hover:border-gray-300 dark:hover:border-gray-600'
                : 'bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-750 hover:border-gray-300 dark:hover:border-gray-600'
          }`}
        >
          <div 
            className={`prose prose-sm max-w-none ${
              isUser 
                ? 'prose-invert [&_*]:text-white' 
                : 'prose dark:prose-invert [&_*]:text-gray-900 dark:[&_*]:text-gray-100'
            } prose-table:table-auto prose-table:w-full prose-td:p-2 prose-th:p-2 prose-thead:bg-gray-200 dark:prose-thead:bg-gray-700 prose-tr:border-b prose-tr:border-gray-300 dark:prose-tr:border-gray-600`}
          >
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{processedContent}</ReactMarkdown>
          </div>
        </div>
        <div 
          className={`text-xs text-gray-800 dark:text-gray-400 mt-2 transition-all duration-300 group-hover:text-gray-900 dark:group-hover:text-gray-300 ${
            isUser ? 'text-right' : 'text-left'
          }`}
        >
          {formattedTime}
        </div>
      </div>
      
      {isUser && (
        <div className="flex-shrink-0 order-3">
          <div className="h-10 w-10 rounded-full shadow-sm bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center transition-all duration-300 group-hover:shadow-lg group-hover:scale-105">
            <User className="h-5 w-5 text-white" />
          </div>
        </div>
      )}
    </div>
  );
}