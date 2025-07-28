import { useState, useEffect } from 'react';
import { Play, Plus, Trash2, PlayCircle, Edit3, Save } from 'lucide-react';
import { executeSQL } from '../lib/api';

interface SqlCell {
  id: string;
  sql: string;
  isEditing: boolean;
  result?: Record<string, unknown>;
  error?: string;
  isExecuting?: boolean;
}

interface SqlEditorProps {
  sessionId: string;
  initialQueries: string[];
  onResults: (results: Record<string, unknown>[]) => void;
  className?: string;
  requiresConfirmation?: boolean;
}

export default function SqlEditor({ sessionId, initialQueries, onResults, className = '', requiresConfirmation = false }: SqlEditorProps) {
  const [cells, setCells] = useState<SqlCell[]>([]);
  const [isBatchExecuting, setIsBatchExecuting] = useState(false);

  useEffect(() => {
    // Initialize cells from initial queries
    const initialCells: SqlCell[] = initialQueries.map((sql, index) => ({
      id: `cell-${index}-${Date.now()}`,
      sql: sql.trim(),
      isEditing: false,
    }));
    
    // Add empty cell if no initial queries
    if (initialCells.length === 0) {
      initialCells.push({
        id: `cell-0-${Date.now()}`,
        sql: '',
        isEditing: true,
      });
    }
    
    setCells(initialCells);
  }, [initialQueries]);

  const addCell = () => {
    const newCell: SqlCell = {
      id: `cell-${cells.length}-${Date.now()}`,
      sql: '',
      isEditing: true,
    };
    setCells([...cells, newCell]);
  };

  const removeCell = (cellId: string) => {
    if (cells.length > 1) {
      setCells(cells.filter(cell => cell.id !== cellId));
    }
  };

  const updateCellSql = (cellId: string, sql: string) => {
    setCells(cells.map(cell => 
      cell.id === cellId ? { ...cell, sql } : cell
    ));
  };

  const toggleCellEdit = (cellId: string) => {
    setCells(cells.map(cell => 
      cell.id === cellId ? { ...cell, isEditing: !cell.isEditing } : cell
    ));
  };

  const executeCell = async (cellId: string) => {
    const cell = cells.find(c => c.id === cellId);
    if (!cell || !cell.sql.trim()) return;

    // Update cell to show executing state
    setCells(cells.map(c => 
      c.id === cellId ? { ...c, isExecuting: true, result: undefined, error: undefined } : c
    ));

    try {
      const result = await executeSQL(sessionId, cell.sql);
      setCells(cells.map(c => 
        c.id === cellId ? { 
          ...c, 
          isExecuting: false, 
          result: result.success ? result : null,
          error: result.success ? null : result.error || 'Execution failed'
        } : c
      ));
    } catch (error) {
      setCells(cells.map(c => 
        c.id === cellId ? { 
          ...c, 
          isExecuting: false, 
          error: error instanceof Error ? error.message : 'Unknown error'
        } : c
      ));
    }
  };

  const executeBatch = async () => {
    const validCells = cells.filter(cell => cell.sql.trim());
    if (validCells.length === 0) return;

    setIsBatchExecuting(true);
    
    // Combine all SQL queries with separator
    const combinedSql = validCells.map(cell => cell.sql.trim()).join('\n<----->\n');
    
    try {
      const result = await executeSQL(sessionId, combinedSql);
      
      // Handle the new transaction response format
      if (result.success) {
        // Success case
        const results = result.results || [result];
        onResults(results);
        
        // Update all cells with success
        setCells(cells.map(cell => ({
          ...cell,
          result: cell.sql.trim() ? {
            success: true,
            affected_rows: result.affected_rows || 0,
            transaction_mode: result.transaction_mode,
            message: `Successfully executed${result.transaction_mode ? ' (transaction)' : ''}`
          } : undefined,
          error: undefined
        })));
      } else {
        // Failure case - handle transaction failure
        const errorMsg = result.error || 'Execution failed';
        
        // If we have query_results, show individual query status
        if (result.query_results && Array.isArray(result.query_results)) {
          setCells(cells.map((cell, index) => {
            if (!cell.sql.trim()) return cell;
            
            const queryResult = result.query_results[index];
            if (queryResult) {
                             return {
                 ...cell,
                 result: queryResult.success ? {
                   success: true,
                   affected_rows: queryResult.affected_rows || 0,
                   message: 'Query executed successfully'
                 } : undefined,
                 error: queryResult.success ? undefined : queryResult.error || 'Query failed'
               };
             } else {
               // Query was not executed due to earlier failure
               return {
                 ...cell,
                 result: undefined,
                 error: index < (result.failed_at_query - 1) ? undefined : 'Not executed (transaction failed)'
               };
            }
          }));
        } else {
                     // Update all cells with the general error
           setCells(cells.map(cell => ({
             ...cell,
             result: undefined,
             error: cell.sql.trim() ? errorMsg : undefined
           })));
        }
        
        // Pass the error result to parent
        onResults([{
          success: false,
          error: errorMsg,
          transaction_mode: result.transaction_mode,
          rollback_performed: result.rollback_performed
        }]);
      }
      
    } catch (error) {
      // Network or other error
      const errorMsg = error instanceof Error ? error.message : 'Batch execution failed';
      setCells(cells.map(cell => ({
        ...cell,
        error: cell.sql.trim() ? errorMsg : undefined,
        result: undefined
      })));
      
      onResults([{
        success: false,
        error: errorMsg
      }]);
    } finally {
      setIsBatchExecuting(false);
    }
  };

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-lg border border-gray-300 dark:border-gray-600 p-4 transition-colors duration-300 ${className}`}>
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 flex items-center gap-2">
          <Edit3 size={20} className="text-orange-500 dark:text-orange-400" />
          SQL Editor - Review & Execute
        </h3>
        <div className="flex gap-2">
          <button
            onClick={addCell}
            className="flex items-center gap-1 px-3 py-1 text-sm bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
            title="Add SQL Cell"
          >
            <Plus size={16} />
            Add Cell
          </button>
          <button
            onClick={executeBatch}
            disabled={isBatchExecuting || cells.every(cell => !cell.sql.trim())}
            className="flex items-center gap-1 px-3 py-1 text-sm bg-green-600 text-white rounded hover:bg-green-700 disabled:bg-gray-500 disabled:cursor-not-allowed transition-colors"
            title="Execute All Cells"
          >
            <PlayCircle size={16} />
            {isBatchExecuting ? 'Executing...' : 'Execute All'}
          </button>
        </div>
      </div>

      {requiresConfirmation && (
        <div className="mb-4 p-3 bg-amber-50 dark:bg-amber-900/50 border border-amber-400 dark:border-amber-600 rounded-lg transition-colors duration-300">
          <div className="flex items-center gap-2 text-amber-800 dark:text-amber-200">
            ⚠️ <strong>Confirmation Required</strong>
          </div>
          <p className="text-sm text-amber-700 dark:text-amber-300 mt-1">
            This query modifies your database. Please review the SQL carefully before executing.
          </p>
        </div>
      )}

      <div className="space-y-4">
        {cells.map((cell, index) => (
          <div key={cell.id} className="border border-gray-300 dark:border-gray-600 rounded-lg p-3 bg-gray-50 dark:bg-gray-700 transition-colors duration-300">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Cell {index + 1}</span>
              <div className="flex gap-1">
                <button
                  onClick={() => executeCell(cell.id)}
                  disabled={!cell.sql.trim() || cell.isExecuting}
                  className="p-1 text-green-400 hover:bg-green-800/50 rounded disabled:text-gray-500 disabled:cursor-not-allowed transition-colors"
                  title="Execute This Cell"
                >
                  <Play size={16} />
                </button>
                <button
                  onClick={() => toggleCellEdit(cell.id)}
                  className="p-1 text-blue-400 hover:bg-blue-800/50 rounded transition-colors"
                  title={cell.isEditing ? "Save" : "Edit"}
                >
                  {cell.isEditing ? <Save size={16} /> : <Edit3 size={16} />}
                </button>
                {cells.length > 1 && (
                  <button
                    onClick={() => removeCell(cell.id)}
                    className="p-1 text-red-400 hover:bg-red-800/50 rounded transition-colors"
                    title="Remove Cell"
                  >
                    <Trash2 size={16} />
                  </button>
                )}
              </div>
            </div>

            {cell.isEditing ? (
              <textarea
                value={cell.sql}
                onChange={(e) => updateCellSql(cell.id, e.target.value)}
                placeholder="Enter your SQL query here..."
                className="w-full h-32 p-2 border border-gray-500 rounded font-mono text-sm resize-vertical focus:outline-none focus:ring-2 focus:ring-blue-500 bg-gray-800 text-gray-100 placeholder-gray-400"
              />
            ) : (
              <div className="bg-gray-100 dark:bg-gray-900 text-green-600 dark:text-green-400 p-2 rounded font-mono text-sm min-h-[4rem] whitespace-pre-wrap transition-colors duration-300">
                {cell.sql || 'Empty cell - click edit to add SQL'}
              </div>
            )}

            {cell.isExecuting && (
              <div className="mt-2 text-sm text-blue-400">
                Executing...
              </div>
            )}

            {cell.error && (
              <div className="mt-2 p-2 bg-red-50 dark:bg-red-900/50 border border-red-400 dark:border-red-600 rounded text-sm text-red-700 dark:text-red-300 transition-colors duration-300">
                <strong>Error:</strong> {cell.error}
              </div>
            )}

            {cell.result && (
              <div className="mt-2 p-2 bg-green-50 dark:bg-green-900/50 border border-green-400 dark:border-green-600 rounded text-sm text-green-700 dark:text-green-300 transition-colors duration-300">
                <strong>Success:</strong> {cell.result.message || 'Query executed successfully'}
                {cell.result.affected_rows !== undefined && (
                  <span className="ml-2">({cell.result.affected_rows} rows affected)</span>
                )}
                {cell.result.transaction_mode && (
                  <div className="text-xs text-green-600 dark:text-green-400 mt-1">
                    ✓ Executed in transaction mode
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
      </div>

      {cells.length === 0 && (
        <div className="text-center py-8 text-gray-400">
          <p>No SQL cells available.</p>
          <button
            onClick={addCell}
            className="mt-2 text-blue-400 hover:text-blue-300"
          >
            Add your first SQL cell
          </button>
        </div>
      )}
    </div>
  );
} 