import { AlertTriangle, CheckCircle, Database, Copy, Download, ChevronLeft, ChevronRight, BarChart, Lightbulb, BookmarkPlus } from 'lucide-react';
import { useState } from 'react';
import Visualization from './Visualization';
import InsightPanel from './InsightPanel';

// Utility function to format numerical values to 2 decimal places
const formatNumber = (value: any): string => {
  if (value === null || value === undefined) return '';
  
  // Check if it's a number
  const num = Number(value);
  if (isNaN(num)) return String(value);
  
  // If it's a whole number, don't show decimals
  if (num % 1 === 0) return num.toString();
  
  // Format to 2 decimal places and remove trailing zeros
  return parseFloat(num.toFixed(2)).toString();
};

interface PaginationInfo {
  table_id: string;
  current_page: number;
  total_pages: number;
  total_rows: number;
  page_size: number;
  has_next?: boolean;
  has_prev?: boolean;
}

interface SqlResultProps {
  sql: string;
  data?: any[];
  error?: string;
  title?: string;
  description?: string;
  pagination?: PaginationInfo;
  onPageChange?: (page: number) => void;
  sessionId?: string;
  tableId?: string;
  onSaveToAnalytics?: (query: any) => void;
  databaseType?: string;
  tableSchema?: any;
  messageId?: string; // Add message ID for chart saving
  visualizationRecommendations?: any; // Add LLM recommendations
  savedCharts?: any[]; // Add saved charts
}

export default function SqlResult({ 
  sql, 
  data, 
  error, 
  title, 
  description,
  pagination,
  onPageChange,
  sessionId,
  tableId,
  onSaveToAnalytics,
  databaseType,
  tableSchema,
  messageId,
  visualizationRecommendations,
  savedCharts
}: SqlResultProps) {
  const [copied, setCopied] = useState(false);
  const [showVisualization, setShowVisualization] = useState(false);
  const [showInsights, setShowInsights] = useState(false);
  const [isSaved, setIsSaved] = useState(false);

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };

  const handlePageChange = (newPage: number) => {
    if (onPageChange && pagination) {
      // Validate page bounds
      if (newPage < 1 || newPage > pagination.total_pages) {
        console.warn(`Invalid page number: ${newPage}. Must be between 1 and ${pagination.total_pages}`);
        return;
      }
      
      onPageChange(newPage);
    } else {
      console.error('Cannot change page: missing onPageChange callback or pagination info');
    }
  };

  // CSV Export functionality
  const exportToCSV = () => {
    if (!data || data.length === 0) return;
    
    const headers = Object.keys(data[0]);
    const csvContent = [
      // Header row
      headers.map(header => `"${header}"`).join(','),
      // Data rows
      ...data.map(row => 
        headers.map(header => {
          const value = row[header];
          // Handle different data types
          if (value === null || value === undefined) return '""';
          if (typeof value === 'object') return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
          return `"${formatNumber(value).replace(/"/g, '""')}"`;
        }).join(',')
      )
    ].join('\n');
    
    // Create and download file
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `query_results_${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const handleSaveToAnalytics = () => {
    if (onSaveToAnalytics && data) {
      const savedQuery = {
        id: `query-${Date.now()}`,
        title: title || "Saved Query",
        description: description || "",
        sql,
        data,
        timestamp: new Date().toISOString(),
        tableName: tableId
      };
      
      onSaveToAnalytics(savedQuery);
      setIsSaved(true);
      
      // Reset saved status after a while
      setTimeout(() => setIsSaved(false), 3000);
    }
  };

  if (error) {
    return (
      <div className="border border-red-500/20 rounded-2xl p-6 bg-red-500/10 shadow-lg animate-in slide-in-from-bottom-2 duration-500">
        <div className="flex items-center space-x-3 mb-4">
          <div className="p-2 bg-red-500/20 rounded-xl">
            <AlertTriangle className="h-5 w-5 text-red-400" />
          </div>
          <h3 className="text-lg font-bold text-red-400">{title || "Query Error"}</h3>
        </div>
        
        <div className="bg-red-500/20 rounded-xl p-4 mb-4">
          <pre className="text-sm text-red-300 whitespace-pre-wrap font-mono leading-relaxed">{error}</pre>
        </div>
        
        <div className="pt-4 border-t border-red-500/20">
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-sm font-semibold text-red-400 flex items-center space-x-2">
              <Database className="h-4 w-4" />
              <span>SQL Query</span>
            </h4>
            <button
              onClick={() => copyToClipboard(sql)}
              className="flex items-center space-x-1 text-xs text-red-400 hover:text-red-300 bg-red-500/20 hover:bg-red-500/30 px-3 py-1 rounded-lg transition-all duration-200"
            >
              <Copy className="h-3 w-3" />
              <span>{copied ? 'Copied!' : 'Copy'}</span>
            </button>
          </div>
          <div className="bg-red-500/20 rounded-xl p-3 overflow-x-auto">
            <pre className="text-sm text-red-300 font-mono">{sql}</pre>
          </div>
        </div>
      </div>
    );
  }

  if (!data || !data.length) {
    return (
      <div className="border border-amber-400 dark:border-amber-500/20 rounded-2xl p-6 bg-amber-50 dark:bg-amber-500/10 shadow-lg animate-in slide-in-from-bottom-2 duration-500 transition-colors duration-300">
        <div className="flex items-center space-x-3 mb-4">
          <div className="p-2 bg-amber-100 dark:bg-amber-500/20 rounded-xl">
            <CheckCircle className="h-5 w-5 text-amber-600 dark:text-amber-400" />
          </div>
          <h3 className="text-lg font-bold text-amber-800 dark:text-amber-400">{title || "Query Executed"}</h3>
        </div>
        
        {description && (
          <p className="text-sm text-amber-700 dark:text-amber-300 mb-4">{description}</p>
        )}
        
        <p className="text-sm text-amber-700 dark:text-amber-300 mb-4 bg-amber-100 dark:bg-amber-500/20 p-3 rounded-xl">
          The query executed successfully but returned no data.
        </p>
        
        <div className="pt-4 border-t border-amber-200 dark:border-amber-500/20">
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-sm font-semibold text-amber-800 dark:text-amber-400 flex items-center space-x-2">
              <Database className="h-4 w-4" />
              <span>SQL Query</span>
            </h4>
            <button
              onClick={() => copyToClipboard(sql)}
              className="flex items-center space-x-1 text-xs text-amber-700 dark:text-amber-400 hover:text-amber-800 dark:hover:text-amber-300 bg-amber-100 dark:bg-amber-500/20 hover:bg-amber-200 dark:hover:bg-amber-500/30 px-3 py-1 rounded-lg transition-all duration-200"
            >
              <Copy className="h-3 w-3" />
              <span>{copied ? 'Copied!' : 'Copy'}</span>
            </button>
          </div>
          <div className="bg-amber-100 dark:bg-amber-500/20 rounded-xl p-3 overflow-x-auto">
            <pre className="text-sm text-amber-800 dark:text-amber-300 font-mono">{sql}</pre>
          </div>
        </div>
      </div>
    );
  }

  // Safely get headers - make sure data[0] exists
  const headers = data[0] ? Object.keys(data[0]) : [];

  return (
    <div className="border border-emerald-400 dark:border-emerald-500/20 rounded-2xl p-6 bg-emerald-50 dark:bg-emerald-500/10 shadow-lg animate-in slide-in-from-bottom-2 duration-500 transition-colors duration-300">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-emerald-100 dark:bg-emerald-500/20 rounded-xl">
            <CheckCircle className="h-5 w-5 text-emerald-600 dark:text-emerald-400" />
          </div>
          <h3 className="text-lg font-bold text-emerald-800 dark:text-emerald-400">{title || "Query Results"}</h3>
        </div>
        
        <div className="flex items-center space-x-2">
          <span className="text-xs bg-emerald-100 dark:bg-emerald-500/20 text-emerald-800 dark:text-emerald-400 px-3 py-1 rounded-full font-semibold">
            {pagination ? `${data?.length || 0} of ${pagination.total_rows}` : `${data?.length || 0}`} {data?.length === 1 ? 'row' : 'rows'}
          </span>
          <div className="relative group">
            <button
              onClick={exportToCSV}
              className="flex items-center space-x-1 text-xs text-emerald-700 dark:text-emerald-400 hover:text-emerald-800 dark:hover:text-emerald-300 bg-emerald-100 dark:bg-emerald-500/20 hover:bg-emerald-200 dark:hover:bg-emerald-500/30 px-3 py-1 rounded-lg transition-all duration-200"
            >
              <Download className="h-3 w-3" />
              <span>CSV</span>
            </button>
          </div>
          <button
            onClick={() => copyToClipboard(JSON.stringify(data, null, 2))}
            className="flex items-center space-x-1 text-xs text-emerald-700 dark:text-emerald-400 hover:text-emerald-800 dark:hover:text-emerald-300 bg-emerald-100 dark:bg-emerald-500/20 hover:bg-emerald-200 dark:hover:bg-emerald-500/30 px-3 py-1 rounded-lg transition-all duration-200"
          >
            <Copy className="h-3 w-3" />
            <span>JSON</span>
          </button>
          {/* Only show Visualize button if data is visualizable */}
          {(!visualizationRecommendations || visualizationRecommendations.is_visualizable !== false) && (
            <button
              onClick={() => setShowVisualization(true)}
              className="flex items-center space-x-1 text-xs text-blue-400 hover:text-blue-300 bg-blue-500/20 hover:bg-blue-500/30 px-3 py-1 rounded-lg transition-all duration-200"
            >
              <BarChart className="h-3 w-3" />
              <span>Visualize</span>
            </button>
          )}
          <button
            onClick={() => setShowInsights(!showInsights)}
            className={`flex items-center space-x-1 text-xs ${
              showInsights ? 'text-purple-400 hover:text-purple-300 bg-purple-500/20 hover:bg-purple-500/30' : 'text-gray-400 hover:text-gray-300 bg-gray-600/20 hover:bg-gray-600/30'
            } px-3 py-1 rounded-lg transition-all duration-200`}
          >
            <Lightbulb className="h-3 w-3" />
            <span>Insights</span>
          </button>
          {onSaveToAnalytics && (
            <button
              onClick={handleSaveToAnalytics}
              disabled={isSaved}
              className={`flex items-center space-x-1 text-xs transition-all duration-200 px-3 py-1 rounded-lg ${
                isSaved 
                  ? 'text-green-400 bg-green-500/20' 
                  : 'text-orange-400 hover:text-orange-300 bg-orange-500/20 hover:bg-orange-500/30'
              }`}
            >
              <BookmarkPlus className="h-3 w-3" />
              <span>{isSaved ? 'Saved!' : 'Save'}</span>
            </button>
          )}
        </div>
      </div>
      
      {description && (
        <p className="text-sm text-emerald-700 dark:text-emerald-300 mb-4">{description}</p>
      )}
      
      {/* Data Table */}
      <div className="bg-gray-100 dark:bg-gray-800/50 rounded-xl overflow-hidden mb-6 transition-colors duration-300">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-200 dark:bg-gray-700/50">
              <tr>
                {headers.map((header, index) => (
                  <th key={index} className="px-4 py-3 text-left font-semibold text-gray-900 dark:text-gray-300 border-b border-gray-300 dark:border-gray-600">
                    {header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((row, rowIndex) => (
                <tr key={rowIndex} className="hover:bg-gray-200 dark:hover:bg-gray-700/30 transition-colors">
                  {headers.map((header, colIndex) => (
                    <td key={colIndex} className="px-4 py-3 text-gray-900 dark:text-gray-300 border-b border-gray-200 dark:border-gray-700/50">
                      {typeof row[header] === 'object' && row[header] !== null 
                        ? JSON.stringify(row[header]) 
                        : formatNumber(row[header])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      
      {/* Pagination */}
      {pagination && pagination.total_pages > 1 && (
        <div className="flex items-center justify-between py-4 border-t border-emerald-200 dark:border-emerald-500/20">
          <div className="flex items-center space-x-3">
            <span className="text-sm text-gray-700 dark:text-gray-400">
              Page {pagination.current_page} of {pagination.total_pages}
            </span>
            <span className="text-xs text-gray-600 dark:text-gray-500">
              ({pagination.total_rows} total rows)
            </span>
          </div>
          <div className="flex items-center space-x-2">
            <button
              onClick={() => handlePageChange(pagination.current_page - 1)}
              disabled={pagination.current_page <= 1 || !onPageChange}
              className="flex items-center space-x-1 px-3 py-2 text-sm text-gray-700 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              <ChevronLeft className="h-4 w-4" />
              <span>Previous</span>
            </button>
            
            {/* Page numbers for better navigation */}
            {pagination.total_pages <= 7 ? (
              // Show all pages if 7 or fewer
              Array.from({ length: pagination.total_pages }, (_, i) => i + 1).map(pageNum => (
                <button
                  key={pageNum}
                  onClick={() => handlePageChange(pageNum)}
                  disabled={!onPageChange}
                  className={`px-3 py-2 text-sm rounded-lg transition-colors ${
                    pageNum === pagination.current_page
                      ? 'bg-emerald-500 text-white'
                      : 'text-gray-700 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600'
                  }`}
                >
                  {pageNum}
                </button>
              ))
            ) : (
              // Show condensed pagination for many pages
              <>
                {pagination.current_page > 3 && (
                  <>
                    <button
                      onClick={() => handlePageChange(1)}
                      disabled={!onPageChange}
                      className="px-3 py-2 text-sm text-gray-700 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg transition-colors"
                    >
                      1
                    </button>
                    {pagination.current_page > 4 && <span className="text-gray-600 dark:text-gray-500">...</span>}
                  </>
                )}
                
                {[pagination.current_page - 1, pagination.current_page, pagination.current_page + 1]
                  .filter(page => page >= 1 && page <= pagination.total_pages)
                  .map(pageNum => (
                    <button
                      key={pageNum}
                      onClick={() => handlePageChange(pageNum)}
                      disabled={!onPageChange}
                      className={`px-3 py-2 text-sm rounded-lg transition-colors ${
                        pageNum === pagination.current_page
                          ? 'bg-emerald-500 text-white'
                          : 'text-gray-700 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600'
                      }`}
                    >
                      {pageNum}
                    </button>
                  ))}
                
                {pagination.current_page < pagination.total_pages - 2 && (
                  <>
                    {pagination.current_page < pagination.total_pages - 3 && <span className="text-gray-600 dark:text-gray-500">...</span>}
                    <button
                      onClick={() => handlePageChange(pagination.total_pages)}
                      disabled={!onPageChange}
                      className="px-3 py-2 text-sm text-gray-700 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg transition-colors"
                    >
                      {pagination.total_pages}
                    </button>
                  </>
                )}
              </>
            )}
            
            <button
              onClick={() => handlePageChange(pagination.current_page + 1)}
              disabled={pagination.current_page >= pagination.total_pages || !onPageChange}
              className="flex items-center space-x-1 px-3 py-2 text-sm text-gray-700 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              <span>Next</span>
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}
      
      {/* Insights Panel */}
      {showInsights && (
        <div className="mt-6">
          <InsightPanel data={data} />
        </div>
      )}
      
      {/* SQL Query Display */}
      <div className="pt-6 border-t border-emerald-200 dark:border-emerald-500/20">
        <div className="flex items-center justify-between mb-2">
          <h4 className="text-sm font-semibold text-emerald-800 dark:text-emerald-400 flex items-center space-x-2">
            <Database className="h-4 w-4" />
            <span>SQL Query</span>
          </h4>
          <button
            onClick={() => copyToClipboard(sql)}
            className="flex items-center space-x-1 text-xs text-emerald-700 dark:text-emerald-400 hover:text-emerald-800 dark:hover:text-emerald-300 bg-emerald-100 dark:bg-emerald-500/20 hover:bg-emerald-200 dark:hover:bg-emerald-500/30 px-3 py-1 rounded-lg transition-all duration-200"
          >
            <Copy className="h-3 w-3" />
            <span>{copied ? 'Copied!' : 'Copy'}</span>
          </button>
        </div>
        <div className="bg-gray-100 dark:bg-gray-800/50 rounded-xl p-3 overflow-x-auto">
          <pre className="text-sm text-gray-800 dark:text-gray-300 font-mono">{sql}</pre>
        </div>
      </div>
      
      {/* Visualization Modal */}
      {showVisualization && (
        <Visualization 
          data={data} 
          onClose={() => setShowVisualization(false)}
          databaseType={databaseType}
          tableSchema={tableSchema}
          messageId={messageId}
          visualizationRecommendations={visualizationRecommendations}
          savedCharts={savedCharts}
        />
      )}
    </div>
  );
}