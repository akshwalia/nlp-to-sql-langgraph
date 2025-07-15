import { useState, useRef, useEffect } from 'react';
import { Database, Wifi, WifiOff, BarChart2, Plus, History, RefreshCw, Menu, X, MessageCircle, Shield, Activity, ArrowLeft, User, ChevronLeft, ChevronRight, Mic, MicOff } from 'lucide-react';
import Message from './Message';
import SqlResult from './SqlResult';
import SessionManager from './SessionManager';
import Dashboard from './Dashboard';
import SessionsList from './SessionsList';
import SqlEditor from './SqlEditor';
import AdminPanel from './AdminPanel';
import VerificationResult from './VerificationResult';
import UserProfile from './UserProfile';
import { executeQuery, getSessionInfo, getPaginatedResults, createSession, getSessionMessages, createSavedQuery, getSavedQueries, deleteSavedQuery, deleteAllSavedQueries, SavedQuery, SavedQueryCreate } from '../lib/api';
import { useAuth } from '../lib/authContext';
import { useTheme } from '../lib/themeContext';
import ThemeToggle from './ThemeToggle';

// PaginationInfo interface to match the API response
interface PaginationInfo {
  table_id: string;
  current_page: number;
  total_pages: number;
  total_rows: number;
  page_size: number;
  has_next?: boolean;
  has_prev?: boolean;
}

interface TableInfo {
  name: string;
  description: string;
  sql: string;
  results: any[];
  row_count: number;
  table_id?: string;
  pagination?: PaginationInfo;
}

interface VerificationResult {
  is_safe: boolean;
  is_correct: boolean;
  safety_issues: string[];
  correctness_issues: string[];
  impact_assessment: string;
  estimated_affected_records: string;
  recommendations: string[];
  overall_verdict: 'SAFE_TO_EXECUTE' | 'REQUIRES_REVIEW' | 'DO_NOT_EXECUTE';
  explanation: string;
}

interface ChatMessage {
  id: string;
  isUser: boolean;
  text: string;
  timestamp: Date;
  query_type?: 'conversational' | 'sql' | 'edit_sql' | 'analysis' | 'edit_execution';
  sqlResult?: {
    sql: string;
    data?: any[];
    error?: string;
    pagination?: PaginationInfo;
    table_id?: string;
    visualization_recommendations?: any; // Add LLM chart recommendations
    saved_charts?: any[]; // Add saved charts
  };
  analysisResult?: {
    tables: TableInfo[];
    analysis_type: 'causal' | 'comparative';
  };
  sqlEditor?: {
    queries: string[];
    requiresConfirmation?: boolean;
  };
  verificationResult?: VerificationResult;
}

// Add a new interface for tracking pagination state
interface PaginationState {
  messageId: string;
  tableId: string;
  currentPage: number;
}

interface ChatBotProps {
  autoSessionId?: string | null;
}

export default function ChatBot({ autoSessionId }: ChatBotProps) {
  const { user, settings } = useAuth();
  const { theme } = useTheme();
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      id: 'welcome',
      isUser: false,
      text: 'Hello! I can help you query your database using natural language. How can I help you today?',
      timestamp: new Date(),
    },
  ]);
  const [input, setInput] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [sessionInfo, setSessionInfo] = useState<any>(null);
  const [showSessionManager, setShowSessionManager] = useState(false);
  const [showSessionsList, setShowSessionsList] = useState(false);
  const [paginationState, setPaginationState] = useState<PaginationState | null>(null);
  const [showDashboard, setShowDashboard] = useState(false);
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>([]);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [showAdminPanel, setShowAdminPanel] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [showUserProfile, setShowUserProfile] = useState(false);
  const [isLoadingSavedQueries, setIsLoadingSavedQueries] = useState(false);
  
  // Voice-to-text states
  const [isListening, setIsListening] = useState(false);
  const [speechError, setSpeechError] = useState<string | null>(null);
  const [speechSupported, setSpeechSupported] = useState(false);
  
  // Animation states
  const [visibleMessages, setVisibleMessages] = useState<Set<string>>(new Set());
  const [scrollDirection, setScrollDirection] = useState<'up' | 'down'>('down');
  const [lastScrollY, setLastScrollY] = useState(0);
  
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const recognitionRef = useRef<SpeechRecognition | null>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const isPaginatingRef = useRef(false);
  const scrollPositionRef = useRef<number>(0);

  // Check for speech recognition support
  useEffect(() => {
    if (typeof window !== 'undefined') {
      const SpeechRecognition = window.webkitSpeechRecognition || window.SpeechRecognition;
      setSpeechSupported(!!SpeechRecognition);
    }
  }, []);

  // Enhanced scroll direction detection
  useEffect(() => {
    const handleScroll = () => {
      const currentScrollY = messagesContainerRef.current?.scrollTop || 0;
      const direction = currentScrollY > lastScrollY ? 'down' : 'up';
      setScrollDirection(direction);
      setLastScrollY(currentScrollY);
    };

    const container = messagesContainerRef.current;
    if (container) {
      container.addEventListener('scroll', handleScroll, { passive: true });
      return () => container.removeEventListener('scroll', handleScroll);
    }
  }, [lastScrollY]);

  // Enhanced Intersection Observer for message animations
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          const messageId = entry.target.getAttribute('data-message-id');
          if (messageId) {
            if (entry.isIntersecting) {
              // Add message to visible set when it enters viewport
              setVisibleMessages(prev => new Set([...prev, messageId]));
            } else {
              // Keep message visible even when out of viewport to prevent re-animations
              // Only remove if scrolling up significantly
              const ratio = entry.intersectionRatio;
              if (ratio === 0 && scrollDirection === 'up') {
                // Optional: Remove from visible set when scrolling up and completely out of view
                // setVisibleMessages(prev => {
                //   const newSet = new Set(prev);
                //   newSet.delete(messageId);
                //   return newSet;
                // });
              }
            }
          }
        });
      },
      {
        threshold: [0, 0.1, 0.5, 1],
        rootMargin: '50px 0px 100px 0px' // Extended margins for smoother animations
      }
    );

    // Observe all message elements
    const messageElements = document.querySelectorAll('[data-message-id]');
    messageElements.forEach(el => observer.observe(el));

    // Make welcome message immediately visible
    if (messages.length > 0) {
      setVisibleMessages(prev => new Set([...prev, messages[0].id]));
    }

    return () => observer.disconnect();
  }, [messages, scrollDirection]);

  // Auto-show new messages with enhanced timing
  useEffect(() => {
    const latestMessage = messages[messages.length - 1];
    if (latestMessage && !visibleMessages.has(latestMessage.id)) {
      const timer = setTimeout(() => {
        setVisibleMessages(prev => new Set([...prev, latestMessage.id]));
      }, 50); // Faster for new messages
      return () => clearTimeout(timer);
    }
  }, [messages, visibleMessages]);

  useEffect(() => {
    // Only auto-scroll when not paginating and if messages array actually grew (new message added)
    if (!isPaginatingRef.current) {
      scrollToBottom();
    } else {
      // If paginating, restore the previous scroll position after DOM updates
      requestAnimationFrame(() => {
        if (messagesContainerRef.current && scrollPositionRef.current !== undefined) {
          messagesContainerRef.current.scrollTop = scrollPositionRef.current;
        }
      });
    }
  }, [messages]);
  
  useEffect(() => {
    inputRef.current?.focus();
    
    if (sessionId) {
      fetchSessionInfo();
    }
  }, [sessionId]);

  // Handle auto session loading when workspace connects with a session ID
  useEffect(() => {
    if (autoSessionId && autoSessionId !== sessionId) {
      handleSessionSelect(autoSessionId);
    }
  }, [autoSessionId]);

  // Load saved queries from database when component mounts or session changes
  useEffect(() => {
    loadSavedQueries();
  }, [sessionId]);

  const fetchSessionInfo = async () => {
    if (!sessionId) return;
    
    try {
      const info = await getSessionInfo(sessionId);
      setSessionInfo(info);
      
      // Check if the info returned is the fallback error object
      if (info.error) {
        // Add a message to the chat about the disconnected session
        const errorMessage: ChatMessage = {
          id: `session-error-${Date.now()}`,
          isUser: false,
          text: `⚠️ ${info.description}`,
          timestamp: new Date(),
        };
        
        setMessages(prev => [...prev, errorMessage]);
      }
    } catch (error) {
      console.error('Error fetching session info:', error);
      setSessionId(null);
      setSessionInfo(null);
    }
  };

  const scrollToBottom = () => {
    // Use instant scroll if we're paginating to avoid conflicts
    const behavior = isPaginatingRef.current ? 'auto' : 'smooth';
    messagesEndRef.current?.scrollIntoView({ behavior });
  };

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!input.trim()) return;
    
    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      isUser: true,
      text: input,
      timestamp: new Date(),
    };
    
    setMessages((prev) => [...prev, userMessage]);
    const currentInput = input;
    setInput('');
    setIsProcessing(true);
    
    try {
      let currentSessionId = sessionId;
      
      // Auto-create session if none exists
      if (!currentSessionId) {
        try {
          const sessionData = await createSession({
            name: `Chat Session ${new Date().toLocaleString()}`,
            description: 'Auto-created session'
          });
          
          currentSessionId = sessionData._id;
          setSessionId(currentSessionId);
          
          const systemMessage: ChatMessage = {
            id: `system-${Date.now()}`,
            isUser: false,
            text: `✅ Created new session: ${sessionData.name}`,
            timestamp: new Date(),
          };
          
          setMessages((prev) => [...prev, systemMessage]);
        } catch (error) {
          console.error('Error auto-creating session:', error);
          const errorMessage: ChatMessage = {
            id: `error-${Date.now()}`,
            isUser: false,
            text: '❌ Failed to create session. Please try connecting to the workspace again.',
            timestamp: new Date(),
          };
          
          setMessages((prev) => [...prev, errorMessage]);
          setIsProcessing(false);
          return;
        }
      }
      
      const result = await executeQuery(currentInput, currentSessionId || undefined);
      
      let responseMessage = result.text || result.message || 'Query executed successfully.';
      const botMessage: ChatMessage = {
        id: result.assistant_message?._id || `response-${Date.now()}`, // Use real message ID from API
        isUser: false,
        text: responseMessage,
        timestamp: new Date(),
        query_type: result.query_type,
      };
      
      // Handle different response types
      if (result.query_type === 'sql' || result.query_type === 'edit_sql') {
        botMessage.sqlResult = {
          sql: result.sql || '',
          data: result.data,
          error: result.error,
          pagination: result.pagination,
          table_id: result.table_id || result.pagination?.table_id,
          visualization_recommendations: result.visualization_recommendations,
          saved_charts: result.saved_charts,
        };
        
        // Add verification result if available (only for new messages, not loaded ones)
        if (result.verification_result) {
          botMessage.verificationResult = result.verification_result;
        }
        
        // Check if this requires SQL editor (edit queries or multiple queries in edit mode)
        if (user?.role === 'admin' && result.sql) {
          let shouldShowEditor = false;
          let queries: string[] = [];
          
          // Case 1: Edit SQL query that requires confirmation
          if (result.query_type === 'edit_sql' || result.is_edit_query || result.requires_confirmation) {
            shouldShowEditor = true;
            // Split by <-----> separator for edit queries too
            queries = result.sql.split('<----->').map((q: string) => q.trim()).filter((q: string) => q);
            
            // Add SQL editor to the message
            botMessage.sqlEditor = {
              queries: queries,
              requiresConfirmation: result.requires_confirmation
            };
          }
          // Case 2: Multiple queries in edit mode
          else if (settings?.settings?.edit_mode_enabled) {
            const splitQueries = result.sql.split('<----->').map((q: string) => q.trim()).filter((q: string) => q);
            if (splitQueries.length > 1) {
              shouldShowEditor = true;
              queries = splitQueries;
              
              // Add SQL editor to the message
              botMessage.sqlEditor = {
                queries: queries,
                requiresConfirmation: false
              };
            }
          }
        }
        
        // Log for debugging
        console.log('SQL Result with pagination:', {
          tableId: result.table_id,
          pagination: result.pagination,
          queryType: result.query_type,
          isEditQuery: result.is_edit_query,
          requiresConfirmation: result.requires_confirmation
        });
      } else if (result.query_type === 'analysis') {
        // Each table should have its own table_id for pagination
        const tablesWithIds = result.tables.map((table: any) => {
          console.log('Analysis table:', table);
          return {
            ...table,
            table_id: table.table_id || `table-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
            pagination: table.pagination
          };
        });
        
        botMessage.analysisResult = {
          tables: tablesWithIds,
          analysis_type: result.analysis_type
        };
      }
      // For conversational queries, we just use the text
      
      setMessages((prev) => [...prev, botMessage]);
    } catch (error: any) {
      const errorMessage: ChatMessage = {
        id: `error-${Date.now()}`,
        isUser: false,
        text: `Error: ${error.message || 'Failed to execute query'}`,
        timestamp: new Date(),
      };
      
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsProcessing(false);
      inputRef.current?.focus();
    }
  };

  const handleSessionCreated = (newSessionId: string) => {
    setSessionId(newSessionId);
    
    const systemMessage: ChatMessage = {
      id: `system-${Date.now()}`,
      isUser: false,
      text: `✅ Connected to database successfully. Session ID: ${newSessionId}`,
      timestamp: new Date(),
    };
    
    setMessages((prev) => [...prev, systemMessage]);
  };

  const handlePageChange = async (messageId: string, tableId: string, newPage: number) => {
    if (!sessionId || !tableId) {
      console.error('Missing session ID or table ID for pagination', { sessionId, tableId });
      return;
    }
    
    console.log(`Fetching page ${newPage} for table ${tableId} in session ${sessionId}`);
    
    // Capture current scroll position before pagination
    if (messagesContainerRef.current) {
      scrollPositionRef.current = messagesContainerRef.current.scrollTop;
    }
    
    setIsProcessing(true);
    isPaginatingRef.current = true;
    
    try {
      const result = await getPaginatedResults(sessionId, tableId, newPage);
      console.log('Paginated results:', result);
      
      // Make sure we have the current table_id (it might have changed in the response)
      const currentTableId = result.pagination?.table_id || tableId;
      
      // Update the message with the new data
      setMessages((prevMessages) => 
        prevMessages.map((msg) => {
          if (msg.id === messageId && msg.sqlResult) {
            return {
              ...msg,
              sqlResult: {
                ...msg.sqlResult,
                data: result.results || result.data, // Use results field from pagination response
                pagination: result.pagination,
                table_id: currentTableId,
                // Explicitly preserve chart-related fields during pagination
                visualization_recommendations: msg.sqlResult.visualization_recommendations,
                saved_charts: msg.sqlResult.saved_charts,
              }
            };
          } else if (msg.id === messageId && msg.analysisResult) {
            // For analysis results, find and update the specific table
            const updatedTables = msg.analysisResult.tables.map(table => {
              if (table.table_id === tableId) {
                return {
                  ...table,
                  results: result.results || result.data, // Use results field from pagination response
                  pagination: result.pagination,
                  table_id: currentTableId
                };
              }
              return table;
            });
            
            return {
              ...msg,
              analysisResult: {
                ...msg.analysisResult,
                tables: updatedTables
              }
            };
          }
          return msg;
        })
      );
      
      // Update the pagination state
      setPaginationState({
        messageId,
        tableId: currentTableId,
        currentPage: newPage
      });
    } catch (error) {
      console.error('Error fetching paginated results:', error);
      const errorMessage: ChatMessage = {
        id: `pagination-error-${Date.now()}`,
        isUser: false,
        text: `Error loading page ${newPage}: ${error instanceof Error ? error.message : 'Unknown error'}`,
        timestamp: new Date(),
      };
      
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsProcessing(false);
      
      // Restore scroll position after a short delay to ensure DOM is updated
      setTimeout(() => {
        if (messagesContainerRef.current && scrollPositionRef.current !== undefined) {
          messagesContainerRef.current.scrollTop = scrollPositionRef.current;
        }
        isPaginatingRef.current = false;
      }, 50);
    }
  };

  // New function to save a query to the dashboard
  const handleSaveQuery = async (query: any) => {
    if (!query.sql || !query.data) return;
    
    try {
      const queryData: SavedQueryCreate = {
        title: query.title || `Query ${new Date().toLocaleString()}`,
        description: query.description || '',
        sql: query.sql,
        data: query.data,
        table_name: query.table_name || 'results'
      };
      
      const savedQuery = await createSavedQuery(
        queryData,
        undefined, // No workspace context
        sessionId || undefined
      );
      
      // Add to local state
      setSavedQueries(prev => [savedQuery, ...prev]);
      
      // Show success message
      const successMessage: ChatMessage = {
        id: `save-success-${Date.now()}`,
        isUser: false,
        text: `✅ Query saved successfully: "${savedQuery.title}"`,
        timestamp: new Date(),
      };
      
      setMessages((prev) => [...prev, successMessage]);
    } catch (error) {
      console.error('Error saving query:', error);
      const errorMessage: ChatMessage = {
        id: `save-error-${Date.now()}`,
        isUser: false,
        text: '❌ Failed to save query. Please try again.',
        timestamp: new Date(),
      };
      
      setMessages((prev) => [...prev, errorMessage]);
    }
  };

  // Function to delete a saved query
  const handleDeleteQuery = async (queryId: string) => {
    try {
      await deleteSavedQuery(queryId);
      setSavedQueries(prev => prev.filter(q => q.id !== queryId));
    } catch (error) {
      console.error('Error deleting saved query:', error);
    }
  };

  // Function to clear all saved queries
  const handleClearAllQueries = async () => {
    try {
      await deleteAllSavedQueries(undefined); // No workspace context
      setSavedQueries([]);
    } catch (error) {
      console.error('Error clearing all queries:', error);
    }
  };

  const handleNewChat = async () => {
    try {
      const sessionData = await createSession({
        name: `Chat Session ${new Date().toLocaleString()}`,
        description: 'New chat session'
      });
      
      setSessionId(sessionData._id);
      setMessages([
        {
          id: 'welcome',
          isUser: false,
          text: 'Hello! I can help you query your database using natural language. How can I help you today?',
          timestamp: new Date(),
        },
      ]);
      
      const systemMessage: ChatMessage = {
        id: `system-${Date.now()}`,
        isUser: false,
        text: `✅ Started new chat session: ${sessionData.name}`,
        timestamp: new Date(),
      };
      
      setMessages((prev) => [...prev, systemMessage]);
      setSidebarOpen(false);
    } catch (error) {
      console.error('Error creating new chat:', error);
    }
  };

  const handleRefreshConnection = async () => {
    if (!sessionId) return;
    
    try {
      setIsRefreshing(true);
      // Refresh session info instead of activating workspace
      await fetchSessionInfo();
      
      const systemMessage: ChatMessage = {
        id: `system-${Date.now()}`,
        isUser: false,
        text: '✅ Session refreshed successfully!',
        timestamp: new Date(),
      };
      
      setMessages((prev) => [...prev, systemMessage]);
    } catch (error) {
      console.error('Error refreshing session:', error);
      const errorMessage: ChatMessage = {
        id: `error-${Date.now()}`,
        isUser: false,
        text: '❌ Failed to refresh session. Please check your connection settings.',
        timestamp: new Date(),
      };
      
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsRefreshing(false);
    }
  };

  const loadSessionMessages = async (sessionId: string) => {
    try {
      const sessionMessages = await getSessionMessages(sessionId);
      
      // Convert session messages to ChatMessage format
      const convertedMessages: ChatMessage[] = sessionMessages.map((msg: any) => {
        const queryResult = msg.query_result;
        
        // Determine query type from the query_result
        let queryType: 'conversational' | 'sql' | 'analysis' | 'edit_sql' | 'edit_execution' | 'fetch' | undefined;
        if (queryResult?.query_type) {
          // Prioritize the stored query_type from the query_result
          queryType = queryResult.query_type;
        } else if (queryResult?.is_conversational) {
          queryType = 'conversational';
        } else if (queryResult?.is_multi_query || queryResult?.is_why_analysis) {
          queryType = 'analysis';
        } else if (queryResult?.sql) {
          queryType = 'sql';
        }

        if(queryType === 'fetch'){
          queryType = 'sql';
        }
        
        const chatMessage: ChatMessage = {
          id: msg._id || `msg-${Date.now()}-${Math.random()}`,
          isUser: msg.role === 'user',
          text: msg.content,
          timestamp: new Date(msg.created_at || Date.now()),
          query_type: queryType,
          sqlResult: queryResult && queryResult.sql ? {
            sql: queryResult.sql || '',
            data: queryResult.results,
            error: queryResult.error,
            pagination: queryResult.pagination,
            table_id: queryResult.pagination?.table_id,
            visualization_recommendations: queryResult.visualization_recommendations,
            saved_charts: queryResult.saved_charts,
          } : undefined,
          analysisResult: queryResult && queryResult.tables ? {
            tables: queryResult.tables || [],
            analysis_type: queryResult.analysis_type
          } : undefined
        };

        // For edit queries, create sqlEditor property instead of showing sqlResult
        if (queryType === 'edit_sql' && queryResult && queryResult.sql && user?.role === 'admin') {
          const queries = queryResult.sql.split('<----->').map((q: string) => q.trim()).filter((q: string) => q);
          chatMessage.sqlEditor = {
            queries: queries,
            requiresConfirmation: true // Historical edit queries should require confirmation
          };
        }

        return chatMessage;
      });

      if (convertedMessages.length > 0) {
        setMessages(convertedMessages);
        
        const systemMessage: ChatMessage = {
          id: `system-${Date.now()}`,
          isUser: false,
          text: `✅ Loaded ${convertedMessages.length} messages from session`,
          timestamp: new Date(),
        };
        
        setMessages(prev => [...prev, systemMessage]);
      } else {
        // If no messages, start with welcome message
        setMessages([
          {
            id: 'welcome',
            isUser: false,
            text: 'Hello! I can help you query your database using natural language. How can I help you today?',
            timestamp: new Date(),
          },
        ]);
        
        const systemMessage: ChatMessage = {
          id: `system-${Date.now()}`,
          isUser: false,
          text: '✅ Session loaded (no previous messages)',
          timestamp: new Date(),
        };
        
        setMessages(prev => [...prev, systemMessage]);
      }
    } catch (error) {
      console.error('Error loading session messages:', error);
      setMessages([
        {
          id: 'error-loading',
          isUser: false,
          text: '❌ Failed to load session messages. Starting with a fresh chat.',
          timestamp: new Date(),
        },
      ]);
    }
  };

  const handleSessionSelect = async (selectedSessionId: string) => {
    setSessionId(selectedSessionId);
    setShowSessionsList(false);
    setSidebarOpen(false);
    
    // Clear current messages and show loading
    setMessages([
      {
        id: 'loading',
        isUser: false,
        text: 'Loading session messages...',
        timestamp: new Date(),
      },
    ]);
    
    // Load session messages
    await loadSessionMessages(selectedSessionId);
    
    // Fetch session info
    fetchSessionInfo();
  };
  
  const handleSqlEditorResults = (messageId: string, results: any[]) => {
    // Handle the new transaction response format
    results.forEach((result, index) => {
      let text = '';
      let query_type: 'edit_execution' | 'sql' = 'edit_execution';
      
      if (result.success) {
        // Success case
        text = result.transaction_mode 
          ? `✅ Transaction executed successfully (${result.affected_rows || 0} rows affected)`
          : `✅ SQL executed successfully (${result.affected_rows || 0} rows affected)`;
      } else {
        // Failure case
        if (result.transaction_mode && result.rollback_performed) {
          text = `❌ Transaction failed and was rolled back: ${result.error || 'Unknown error'}`;
        } else {
          text = `❌ SQL execution failed: ${result.error || 'Unknown error'}`;
        }
      }
      
      const resultMessage: ChatMessage = {
        id: `sql-editor-result-${messageId}-${Date.now()}-${index}`,
        isUser: false,
        text: text,
        timestamp: new Date(),
        query_type: query_type,
        sqlResult: {
          sql: result.sql || '',
          data: result.data || result.results || [],
          error: result.success ? undefined : (result.error || 'Execution failed'),
          visualization_recommendations: result.visualization_recommendations,
          saved_charts: result.saved_charts,
        }
      };
      
      setMessages(prev => [...prev, resultMessage]);
    });
  };

  const loadSavedQueries = async () => {
    try {
      setIsLoadingSavedQueries(true);
      const queries = await getSavedQueries(undefined); // No workspace context
      setSavedQueries(queries);
    } catch (error) {
      console.error('Error loading saved queries:', error);
    } finally {
      setIsLoadingSavedQueries(false);
    }
  };

  // Voice-to-text functions
  const initializeSpeechRecognition = () => {
    if (typeof window === 'undefined' || !speechSupported) {
      return null;
    }

    const SpeechRecognition = window.webkitSpeechRecognition || window.SpeechRecognition;
    const recognition = new SpeechRecognition();
    
    recognition.continuous = false;
    recognition.interimResults = true; // Enable interim results for better UX
    recognition.lang = 'en-US'; // Use English
    recognition.maxAlternatives = 1;

    recognition.onstart = () => {
      setIsListening(true);
      setSpeechError(null);
      console.log('Speech recognition started');
    };

    recognition.onresult = (event: SpeechRecognitionEvent) => {
      let finalTranscript = '';
      let interimTranscript = '';

      for (let i = event.resultIndex; i < event.results.length; i++) {
        const transcript = event.results[i][0].transcript;
        if (event.results[i].isFinal) {
          finalTranscript += transcript;
        } else {
          interimTranscript += transcript;
        }
      }

      // Update input with final transcript or interim if no final yet
      const displayTranscript = finalTranscript || interimTranscript;
      if (displayTranscript.trim()) {
        setInput(displayTranscript.trim());
      }

      // If we have a final result, stop listening
      if (finalTranscript.trim()) {
        setIsListening(false);
        console.log('Speech recognition final result:', finalTranscript);
      }
    };

    recognition.onerror = (event: SpeechRecognitionErrorEvent) => {
      console.error('Speech recognition error:', event.error);
      setIsListening(false);
      
      let errorMessage = 'Speech recognition failed. ';
      switch (event.error) {
        case 'no-speech':
          errorMessage += 'No speech was detected. Please try again.';
          break;
        case 'audio-capture':
          errorMessage += 'No microphone was found. Please check your microphone.';
          break;
        case 'not-allowed':
          errorMessage += 'Microphone permission denied. Please allow microphone access.';
          break;
        case 'network':
          errorMessage += 'Network error occurred. Please check your connection.';
          break;
        case 'language-not-supported':
          errorMessage += 'Selected language is not supported. Please try a different language.';
          break;
        case 'aborted':
          // Don't show error for aborted (user stopped intentionally)
          return;
        default:
          errorMessage += 'Please try again.';
      }
      setSpeechError(errorMessage);
      
      // Clear error after 5 seconds
      setTimeout(() => setSpeechError(null), 5000);
    };

    recognition.onend = () => {
      setIsListening(false);
      console.log('Speech recognition ended');
    };

    return recognition;
  };

  const startListening = () => {
    if (!speechSupported) {
      setSpeechError('Speech recognition is not supported in your browser.');
      setTimeout(() => setSpeechError(null), 3000);
      return;
    }

    if (isListening) {
      stopListening();
      return;
    }

    const recognition = initializeSpeechRecognition();
    if (recognition) {
      recognitionRef.current = recognition;
      try {
        recognition.start();
      } catch (error) {
        console.error('Error starting speech recognition:', error);
        setSpeechError('Failed to start speech recognition. Please try again.');
        setTimeout(() => setSpeechError(null), 3000);
      }
    }
  };

  const stopListening = () => {
    if (recognitionRef.current) {
      recognitionRef.current.stop();
      recognitionRef.current = null;
    }
    setIsListening(false);
  };

  // Cleanup speech recognition on unmount
  useEffect(() => {
    return () => {
      if (recognitionRef.current) {
        recognitionRef.current.stop();
      }
    };
  }, []);

  // Keyboard shortcut for voice input (Ctrl+Shift+V or Cmd+Shift+V)
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.shiftKey && event.key === 'V') {
        event.preventDefault();
        if (speechSupported && !isProcessing) {
          startListening();
        }
      }
      // ESC to stop listening
      if (event.key === 'Escape' && isListening) {
        event.preventDefault();
        stopListening();
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [speechSupported, isProcessing, isListening]);

  const navigationItems = [
    {
      id: 'new-chat',
      icon: Plus,
      label: 'New Chat',
      onClick: handleNewChat,
      disabled: false,
      color: 'text-emerald-400 group-hover:text-emerald-300'
    },
    {
      id: 'chat-history',
      icon: History,
      label: 'Chat History',
      onClick: () => { setShowSessionsList(true); setSidebarOpen(false); },
      disabled: false,
      color: 'text-blue-400 group-hover:text-blue-300'
    },
    {
      id: 'refresh',
      icon: RefreshCw,
      label: 'Refresh Session',
      onClick: handleRefreshConnection,
      disabled: !sessionId || isRefreshing,
      color: 'text-orange-400 group-hover:text-orange-300',
      spinning: isRefreshing
    },
    {
      id: 'dashboard',
      icon: BarChart2,
      label: 'Analytics Dashboard',
      onClick: () => { setShowDashboard(true); setSidebarOpen(false); },
      disabled: false,
      color: 'text-purple-400 group-hover:text-purple-300'
    }
  ];

  if (user?.role === 'admin') {
    navigationItems.push({
      id: 'admin',
      icon: Shield,
      label: 'Admin Panel',
      onClick: () => { setShowAdminPanel(true); setSidebarOpen(false); },
      disabled: false,
      color: 'text-red-400 group-hover:text-red-300'
    });
  }

  const sidebarWidth = sidebarCollapsed ? 'w-20' : 'w-80';
  const sidebarClasses = `${sidebarOpen ? 'translate-x-0' : '-translate-x-full'} ${sidebarWidth} fixed inset-y-0 left-0 z-50 bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 transition-all duration-300 ease-in-out lg:translate-x-0 lg:static lg:inset-0 pt-3 flex flex-col max-h-screen`;

  return (
    <div className="flex h-full min-h-screen bg-gray-50 dark:bg-gray-900 transition-colors duration-300">
      {/* Sidebar */}
      <div className={sidebarClasses}>
        <div className="flex items-center justify-between h-16 px-4 border-b border-gray-200 dark:border-gray-700">
          {!sidebarCollapsed && (
            <div className="flex items-center space-x-3 ">
              <div className="p-2 bg-gradient-to-r from-blue-500 to-purple-600 rounded-lg">
                <Database className="h-5 w-5 text-white" />
              </div>
              <div>
                <h1 className="text-sm font-semibold text-gray-900 dark:text-white">SQL Assistant</h1>
                <p className="text-xs text-gray-500 dark:text-gray-400">AI-Powered Queries</p>
              </div>
            </div>
          )}
          
          <div className="flex items-center space-x-2">
            {/* Desktop collapse toggle */}
            <button
              onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
              className="hidden lg:flex p-2 text-gray-500 hover:text-gray-900 dark:text-gray-400 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
            >
              {sidebarCollapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
            </button>
            
            {/* Mobile close button */}
            <button
              onClick={() => setSidebarOpen(false)}
              className="lg:hidden p-1 text-gray-500 hover:text-gray-900 dark:text-gray-400 dark:hover:text-white transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>

        {/* Connection Status */}
        <div className="p-4 border-b border-gray-200 dark:border-gray-700 py-8 flex-shrink-0">
          {sessionId ? (
            <div className={`flex items-center ${sidebarCollapsed ? 'justify-center' : 'space-x-3'} bg-emerald-50 dark:bg-emerald-500/10 border border-emerald-400 dark:border-emerald-500/20 rounded-lg p-3 transition-colors duration-300`}>
              <div className="flex-shrink-0">
                <Wifi className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
              </div>
              {!sidebarCollapsed && (
                <>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-emerald-800 dark:text-emerald-400">Connected</p>
                    <p className="text-xs text-emerald-700 dark:text-emerald-300 truncate">
                      {sessionInfo?.db_info?.db_name || 'Active Session'}
                    </p>
                  </div>
                  <div className="h-2 w-2 rounded-full bg-emerald-600 dark:bg-emerald-400 animate-pulse"></div>
                </>
              )}
            </div>
          ) : (
            <div className={`flex items-center ${sidebarCollapsed ? 'justify-center' : 'space-x-3'} bg-gray-100 dark:bg-gray-700/50 border border-gray-300 dark:border-gray-600 rounded-lg p-3`}>
              <div className="flex-shrink-0">
                <WifiOff className="h-4 w-4 text-gray-500 dark:text-gray-400" />
              </div>
              {!sidebarCollapsed && (
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Disconnected</p>
                  <p className="text-xs text-gray-500 dark:text-gray-500">No active session</p>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Navigation */}
        <nav className="flex-1 px-4 py-8 space-y-8 overflow-y-auto min-h-0">
          {navigationItems.map((item) => {
            const IconComponent = item.icon;
            return (
              <button
                key={item.id}
                onClick={item.onClick}
                disabled={item.disabled}
                className={`group w-full flex items-center ${sidebarCollapsed ? 'justify-center' : 'px-4'} py-3 text-sm font-medium rounded-xl transition-all duration-200 ${
                  item.disabled 
                    ? 'text-gray-400 dark:text-gray-500 cursor-not-allowed' 
                    : 'text-gray-700 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-gray-700/60 hover:shadow-lg hover:scale-105'
                }`}
                title={sidebarCollapsed ? item.label : undefined}
              >
                <IconComponent 
                  className={`${sidebarCollapsed ? '' : 'mr-4'} h-5 w-5 transition-colors ${
                    item.disabled 
                      ? 'text-gray-400 dark:text-gray-600' 
                      : item.color
                  } ${item.spinning ? 'animate-spin' : ''}`} 
                />
                {!sidebarCollapsed && item.label}
              </button>
            );
          })}
        </nav>

        {/* User Info */}
        <div className="p-4 flex-shrink-0">
          <button
            onClick={() => setShowUserProfile(true)}
            className={`w-full flex items-center ${sidebarCollapsed ? 'justify-center' : 'space-x-3'} p-3 rounded-xl hover:bg-gray-100 dark:hover:bg-gray-700/60 transition-all duration-200 hover:shadow-lg`}
            title={sidebarCollapsed ? `${user?.first_name || user?.email || 'User'} (${user?.role || 'member'})` : undefined}
          >
            <div className="h-9 w-9 rounded-full bg-gradient-to-r from-blue-500 to-purple-600 flex items-center justify-center flex-shrink-0">
              <span className="text-sm font-medium text-white">
                {(user?.first_name?.charAt(0) || user?.email?.charAt(0) || 'U').toUpperCase()}
              </span>
            </div>
            {!sidebarCollapsed && (
              <>
                <div className="flex-1 min-w-0 text-left">
                  <p className="text-sm font-medium text-gray-900 dark:text-white truncate">
                    {user?.first_name || user?.email?.split('@')[0] || 'User'}
                  </p>
                  <p className="text-xs text-gray-500 dark:text-gray-400 capitalize">
                    {user?.role || 'member'}
                  </p>
                </div>
                <User className="h-4 w-4 text-gray-500 dark:text-gray-400" />
              </>
            )}
          </button>
        </div>
      </div>

      {/* Sidebar Overlay for mobile */}
      {sidebarOpen && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 z-40 lg:hidden" 
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Main Content */}
      <div className="flex-1 flex flex-col min-w-0 min-h-0">
        {/* Header */}
        <header className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-4 lg:px-6 py-4 flex-shrink-0">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
                              <button
                  onClick={() => setSidebarOpen(true)}
                  className="lg:hidden p-2 text-gray-500 hover:text-gray-900 dark:text-gray-400 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
                >
                  <Menu className="h-5 w-5" />
                </button>
                <div className="flex items-center space-x-3">
                  <MessageCircle className="h-6 w-6 text-blue-400" />
                  <div>
                    <h1 className="text-lg font-semibold text-gray-900 dark:text-white">
                      Natural Language Query
                    </h1>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      Ask questions about your data in plain English
                    </p>
                  </div>
                </div>
            </div>
            
            <div className="flex items-center space-x-2">
              <div className="hidden sm:flex items-center space-x-2 text-xs text-gray-500 dark:text-gray-400">
                <Activity className="h-4 w-4" />
                <span>Live Session</span>
              </div>
              <ThemeToggle size="sm" />
            </div>
          </div>
        </header>
        
        {/* Chat Messages */}
        <div ref={messagesContainerRef} className="flex-1 overflow-y-auto bg-gray-50 dark:bg-gray-900 chat-scroll min-h-0 max-h-[70vh] transition-colors duration-300">
          <div className="max-w-6xl mx-auto px-4 lg:px-8 py-6">
            <div className="space-y-6">
              {messages.map((message, index) => {
                const isVisible = visibleMessages.has(message.id);
                const isLatest = index === messages.length - 1;
                const animationDelay = isLatest ? 50 : Math.min(index * 80, 600);
                
                return (
                  <div 
                    key={message.id} 
                    data-message-id={message.id}
                    className={`transform transition-all duration-500 ease-out ${
                      isVisible 
                        ? 'translate-y-0 opacity-100 scale-100' 
                        : 'translate-y-6 opacity-0 scale-98'
                    } ${scrollDirection === 'up' && isVisible ? 'animate-pulse-subtle' : ''}`}
                    style={{ 
                      transitionDelay: `${animationDelay}ms`,
                      animationFillMode: 'both',
                      willChange: 'transform, opacity'
                    }}
                  >
                    <div className="message-hover transition-all duration-300 ease-out">
                      <Message
                        isUser={message.isUser}
                        content={message.text}
                        timestamp={message.timestamp}
                        isConversational={!message.isUser && message.query_type === 'conversational'}
                      />
                    </div>
                    
                    {/* Render verification result for edit queries if available */}
                    {message.verificationResult && (
                      <div className={`ml-4 mt-4 transform transition-all duration-400 ease-out ${
                        isVisible 
                          ? 'translate-x-0 opacity-100 scale-100' 
                          : 'translate-x-3 opacity-0 scale-98'
                      }`} style={{ 
                        transitionDelay: `${animationDelay + 100}ms`,
                        willChange: 'transform, opacity'
                      }}>
                        <VerificationResult 
                          verificationResult={message.verificationResult}
                          className="border-l-4 border-l-blue-400"
                        />
                      </div>
                    )}
                    
                    {/* Render different types of results based on query_type */}
                    {((message.query_type === 'sql' && !message.sqlEditor) || (message.query_type === 'edit_execution')) && message.sqlResult && (
                      <div className={`ml-4 mt-4 transform transition-all duration-400 ease-out ${
                        isVisible 
                          ? 'translate-x-0 opacity-100 scale-100' 
                          : 'translate-x-3 opacity-0 scale-98'
                      }`} style={{ 
                        transitionDelay: `${animationDelay + 150}ms`,
                        willChange: 'transform, opacity'
                      }}>
                        {/* <SqlResult
                          sql={message.sqlResult.sql}
                          data={message.sqlResult.data}
                          error={message.sqlResult.error}
                          pagination={message.sqlResult.pagination}
                          onPageChange={(page) => handlePageChange(message.id, message.sqlResult?.table_id || '', page)}
                          sessionId={sessionId || undefined}
                          tableId={message.sqlResult.table_id}
                          onSaveToAnalytics={handleSaveQuery}
                          messageId={message.id}
                          visualizationRecommendations={message.sqlResult.visualization_recommendations}
                          savedCharts={message.sqlResult.saved_charts}
                        /> */}
                      </div>
                    )}

                    {message.query_type === 'analysis' && message.analysisResult && (
                      <div className={`ml-4 mt-4 transform transition-all duration-400 ease-out ${
                        isVisible 
                          ? 'translate-x-0 opacity-100 scale-100' 
                          : 'translate-x-3 opacity-0 scale-98'
                      }`} style={{ 
                        transitionDelay: `${animationDelay + 200}ms`,
                        willChange: 'transform, opacity'
                      }}>
                                                  {message.analysisResult.tables.map((table, tableIndex) => (
                            <div key={tableIndex} className={`mb-8 last:mb-0 transform transition-all duration-500 ease-out ${
                              isVisible 
                                ? 'translate-y-0 opacity-100 scale-100' 
                                : 'translate-y-3 opacity-0 scale-98'
                            }`} style={{ 
                              transitionDelay: `${animationDelay + 250 + tableIndex * 100}ms`,
                              willChange: 'transform, opacity'
                            }}>
                            {/* <SqlResult
                              sql={table.sql}
                              data={table.results}
                              title={table.name}
                              description={table.description}
                              pagination={table.pagination}
                              onPageChange={(page) => handlePageChange(message.id, table.table_id || '', page)}
                              sessionId={sessionId || undefined}
                              tableId={table.table_id}
                              onSaveToAnalytics={handleSaveQuery}
                              messageId={message.id}
                              visualizationRecommendations={undefined}
                              savedCharts={undefined}
                            /> */}
                          </div>
                        ))}
                      </div>
                    )}
                    {/* Render SQL Editor for edit queries */}
                    {message.sqlEditor && sessionId && user?.role === 'admin' && (
                      <div className={`ml-4 mt-4 transform transition-all duration-400 ease-out ${
                        isVisible 
                          ? 'translate-x-0 opacity-100 scale-100' 
                          : 'translate-x-3 opacity-0 scale-98'
                      }`} style={{ 
                        transitionDelay: `${animationDelay + 200}ms`,
                        willChange: 'transform, opacity'
                      }}>
                        <SqlEditor
                          sessionId={sessionId}
                          initialQueries={message.sqlEditor.queries}
                          onResults={(results) => handleSqlEditorResults(message.id, results)}
                          className="border-l-4 border-l-orange-400"
                          requiresConfirmation={message.sqlEditor.requiresConfirmation}
                        />
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
            <div ref={messagesEndRef} />
            
            {isProcessing && (
              <div className="flex items-center justify-center py-8 animate-in fade-in duration-300">
                <div className="flex items-center space-x-3 bg-white/80 dark:bg-gray-800/80 backdrop-blur-sm px-6 py-3 rounded-xl border border-gray-200 dark:border-gray-700">
                  <div className="flex space-x-1">
                    <div className="h-2 w-2 rounded-full bg-blue-400 animate-bounce"></div>
                    <div className="h-2 w-2 rounded-full bg-blue-400 animate-bounce" style={{ animationDelay: '0.1s' }}></div>
                    <div className="h-2 w-2 rounded-full bg-blue-400 animate-bounce" style={{ animationDelay: '0.2s' }}></div>
                  </div>
                  <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Processing your query...</span>
                </div>
              </div>
            )}
          </div>
        </div>
        
        {/* Input Area */}
        <div className="bg-white dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700 p-4 lg:p-6 flex-shrink-0 transition-colors duration-300">
          <div className="max-w-6xl mx-auto">
            {/* Speech Error Display */}
            {speechError && (
              <div className="mb-4 p-3 bg-red-500/20 border border-red-500/30 rounded-lg text-red-300 text-sm animate-in fade-in duration-300">
                {speechError}
              </div>
            )}
            
            <form onSubmit={handleSendMessage} className="relative">
              <div className="flex items-center bg-gray-100 dark:bg-gray-700 rounded-xl border border-gray-300 dark:border-gray-600 overflow-hidden transition-all duration-300 hover:border-gray-400 dark:hover:border-gray-500 focus-within:border-blue-500 focus-within:ring-1 focus-within:ring-blue-500">
                <input
                  ref={inputRef}
                  type="text"
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  placeholder={isListening ? "Listening... Speak now" : "Ask me anything about your data..."}
                  className="flex-1 py-4 px-6 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400 bg-transparent focus:outline-none"
                  disabled={isProcessing || isListening}
                />
                
                

                {/* Voice Input Button */}
                {speechSupported && (
                  <button
                    type="button"
                    onClick={startListening}
                    className={`mx-2 p-3 rounded-lg transition-all duration-300 transform hover:scale-105 ${
                      isListening 
                        ? 'bg-red-500 hover:bg-red-600 text-white animate-pulse' 
                        : 'bg-green-500 hover:bg-green-600 text-white hover:shadow-lg'
                    } ${isProcessing ? 'opacity-50 cursor-not-allowed scale-100' : ''}`}
                    disabled={isProcessing}
                    title={isListening ? 'Stop listening (ESC)' : 'Start voice input (Ctrl+Shift+V)'}
                    aria-label={isListening ? 'Stop voice recording' : 'Start voice recording'}
                    aria-pressed={isListening}
                  >
                    {isListening ? (
                      <MicOff className="h-5 w-5" aria-hidden="true" />
                    ) : (
                      <Mic className="h-5 w-5" aria-hidden="true" />
                    )}
                  </button>
                )}
                
                <button
                  type="submit"
                  className={`m-2 bg-gradient-to-r from-blue-500 to-purple-600 hover:from-blue-600 hover:to-purple-700 text-white p-3 rounded-lg transition-all duration-300 transform hover:scale-105 ${
                    isProcessing ? 'opacity-50 cursor-not-allowed scale-100' : 'hover:shadow-lg'
                  }`}
                  disabled={isProcessing || !input.trim()}
                >
                  <svg className="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
                  </svg>
                </button>
              </div>
            </form>
            
            {/* Voice Recognition Status */}
            {isListening && (
              <div className="flex items-center justify-center mt-4 text-sm text-green-400 animate-in fade-in duration-300">
                <div className="flex items-center space-x-2">
                  <div className="w-2 h-2 bg-green-400 rounded-full animate-bounce"></div>
                  <div className="w-2 h-2 bg-green-400 rounded-full animate-bounce" style={{ animationDelay: '0.1s' }}></div>
                  <div className="w-2 h-2 bg-green-400 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
                  <span className="ml-2">Listening for your voice...</span>
                </div>
              </div>
            )}
            
            {!sessionId && !isListening && (
              <p className="text-center text-sm text-gray-600 dark:text-gray-500 mt-3">
                💡 Connect to a database for persistent context and better results
              </p>
            )}
            
            {/* Voice Recognition Info */}
            {speechSupported && !isListening && !speechError && (
              <p className="text-center text-sm text-gray-600 dark:text-gray-500 mt-2">
                🎤 Click the microphone button or press <span className="text-gray-500 dark:text-gray-400 bg-gray-200 dark:bg-gray-700 px-1 py-0.5 rounded text-xs font-mono">Ctrl+Shift+V</span> to use voice input
              </p>
            )}
            
            {!speechSupported && (
              <p className="text-center text-sm text-orange-500 dark:text-orange-400 mt-2">
                ⚠️ Voice input not supported in your browser
              </p>
            )}
          </div>
        </div>
      </div>
      
      {/* Session Manager Dialog */}
      <SessionManager
        isOpen={showSessionManager}
        onClose={() => setShowSessionManager(false)}
        onSessionCreated={handleSessionCreated}
      />

      {/* Dashboard */}
      <Dashboard 
        isOpen={showDashboard}
        onClose={() => setShowDashboard(false)}
        onDeleteQuery={handleDeleteQuery}
        onClearAllQueries={handleClearAllQueries}
        savedQueries={savedQueries}
        isLoading={isLoadingSavedQueries}
      />

      {/* Sessions List */}
      {showSessionsList && (
        <SessionsList 
          onClose={() => setShowSessionsList(false)}
          onSessionSelect={handleSessionSelect}
        />
      )}

      {/* User Profile Modal */}
      {showUserProfile && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-gray-800 rounded-xl border border-gray-700 shadow-2xl max-w-md w-full">
            <div className="flex items-center justify-between p-6 border-b border-gray-700">
              <h2 className="text-xl font-semibold text-white">User Profile</h2>
              <button
                onClick={() => setShowUserProfile(false)}
                className="text-gray-400 hover:text-white transition-colors p-1 hover:bg-gray-700 rounded-lg"
              >
                <X className="h-6 w-6" />
              </button>
            </div>
            <div className="p-6">
              <UserProfile />
            </div>
          </div>
        </div>
      )}

      {/* Admin Panel Modal */}
      {showAdminPanel && user?.role === 'admin' && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-gray-800 rounded-xl border border-gray-700 shadow-2xl max-w-4xl w-full max-h-[90vh] overflow-hidden">
            <div className="flex items-center justify-between p-6 border-b border-gray-700">
              <h2 className="text-xl font-semibold text-white">Admin Panel</h2>
              <button
                onClick={() => setShowAdminPanel(false)}
                className="text-gray-400 hover:text-white transition-colors p-1 hover:bg-gray-700 rounded-lg"
              >
                <X className="h-6 w-6" />
              </button>
            </div>
            <div className="p-6 max-h-[calc(90vh-100px)] overflow-y-auto">
              <AdminPanel />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}