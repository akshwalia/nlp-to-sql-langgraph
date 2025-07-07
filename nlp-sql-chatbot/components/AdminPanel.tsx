import { useState, useEffect } from 'react';
import { Settings, UserPlus, ToggleLeft, ToggleRight, Search, Power, PowerOff } from 'lucide-react';
import { useAuth } from '../lib/authContext';
import { searchUser, promoteUser } from '../lib/api';

export default function AdminPanel({ className = '' }: { className?: string }) {
  const { user, settings, toggleEditMode, refreshSettings } = useAuth();
  const [searchEmail, setSearchEmail] = useState('');
  const [searchResult, setSearchResult] = useState<any>(null);
  const [isSearching, setIsSearching] = useState(false);
  const [isPromoting, setIsPromoting] = useState(false);
  const [isTogglingEditMode, setIsTogglingEditMode] = useState(false);
  const [localEditMode, setLocalEditMode] = useState<boolean | null>(null);

  // Update local state when settings change
  useEffect(() => {
    if (settings !== null) {
      // Use the correct field name from API response
      const editModeEnabled = settings.settings?.edit_mode_enabled || settings.edit_mode_enabled || false;
      setLocalEditMode(editModeEnabled);
    }
  }, [settings]);

  if (user?.role !== 'admin') {
    return null;
  }

  const handleSearchUser = async () => {
    if (!searchEmail.trim()) return;
    
    setIsSearching(true);
    try {
      const result = await searchUser(searchEmail);
      setSearchResult(result);
    } catch (error) {
      console.error('Search failed:', error);
      setSearchResult({ error: 'User not found or search failed' });
    } finally {
      setIsSearching(false);
    }
  };

  const handlePromoteUser = async (userEmail: string) => {
    setIsPromoting(true);
    try {
      await promoteUser(userEmail);
      setSearchResult(null);
      setSearchEmail('');
      // Optionally show success message
    } catch (error) {
      console.error('Promotion failed:', error);
    } finally {
      setIsPromoting(false);
    }
  };

  const handleToggleEditMode = async () => {
    setIsTogglingEditMode(true);
    // Optimistically update local state for immediate UI feedback
    const newState = !localEditMode;
    setLocalEditMode(newState);
    
    try {
      await toggleEditMode();
      // Refresh settings to get the actual server state
      await refreshSettings();
    } catch (error) {
      console.error('Toggle edit mode failed:', error);
      // Revert local state on error
      setLocalEditMode(!newState);
    } finally {
      setIsTogglingEditMode(false);
    }
  };

  // Use local state if available, fallback to settings with correct field names
  const currentEditMode = localEditMode !== null ? localEditMode : 
    (settings?.settings?.edit_mode_enabled || settings?.edit_mode_enabled || false);
  
  // Check if user can edit (from API response)
  const canEdit = settings?.can_edit || false;

  return (
    <div className={`bg-gray-800 rounded-lg border border-gray-700 p-6 ${className}`}>
      <div className="flex items-center gap-2 mb-6">
        <Settings className="text-blue-400" size={24} />
        <h2 className="text-xl font-semibold text-white">Admin Panel</h2>
      </div>

      {/* Edit Mode Toggle */}
      <div className="mb-6 p-4 border border-gray-600 rounded-lg bg-gray-700/50">
        <h3 className="text-lg font-medium text-white mb-3">Edit Mode Settings</h3>
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm text-gray-300">
              Edit mode allows generation and editing of SQL queries in multiple cells
            </p>
            <div className="flex items-center gap-2 mt-2">
              <div className={`inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium ${
                currentEditMode 
                  ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' 
                  : 'bg-red-500/20 text-red-400 border border-red-500/30'
              }`}>
                {currentEditMode ? (
                  <>
                    <Power className="h-3 w-3" />
                    <span>ENABLED</span>
                  </>
                ) : (
                  <>
                    <PowerOff className="h-3 w-3" />
                    <span>DISABLED</span>
                  </>
                )}
              </div>
              {isTogglingEditMode && (
                <div className="text-xs text-yellow-400 animate-pulse">
                  Updating...
                </div>
              )}
              {/* Show can_edit status */}
              <div className={`inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium ${
                canEdit 
                  ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30' 
                  : 'bg-gray-500/20 text-gray-400 border border-gray-500/30'
              }`}>
                <span>CAN_EDIT: {canEdit ? 'YES' : 'NO'}</span>
              </div>
            </div>
          </div>
          <button
            onClick={handleToggleEditMode}
            disabled={isTogglingEditMode}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-all duration-200 ${
              isTogglingEditMode
                ? 'bg-gray-600 text-gray-400 cursor-not-allowed'
                : currentEditMode
                  ? 'bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 text-white shadow-lg'
                  : 'bg-gradient-to-r from-emerald-500 to-emerald-600 hover:from-emerald-600 hover:to-emerald-700 text-white shadow-lg'
            }`}
          >
            {isTogglingEditMode ? (
              <>
                <div className="animate-spin h-4 w-4 border-2 border-gray-300 border-t-transparent rounded-full"></div>
                <span>Updating...</span>
              </>
            ) : currentEditMode ? (
              <>
                <ToggleRight className="h-5 w-5" />
                <span>Turn Off</span>
              </>
            ) : (
              <>
                <ToggleLeft className="h-5 w-5" />
                <span>Turn On</span>
              </>
            )}
          </button>
        </div>
      </div>

      {/* User Management */}
      <div className="mb-6 p-4 border border-gray-600 rounded-lg bg-gray-700/50">
        <h3 className="text-lg font-medium text-white mb-3">User Management</h3>
        
        {/* Search User */}
        <div className="mb-4">
          <label htmlFor="searchEmail" className="block text-sm font-medium text-gray-300 mb-2">
            Search User by Email
          </label>
          <div className="flex gap-2">
            <input
              id="searchEmail"
              type="email"
              value={searchEmail}
              onChange={(e) => setSearchEmail(e.target.value)}
              placeholder="Enter user email..."
              className="flex-1 px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              onKeyPress={(e) => e.key === 'Enter' && handleSearchUser()}
            />
            <button
              onClick={handleSearchUser}
              disabled={isSearching || !searchEmail.trim()}
              className="flex items-center gap-1 px-4 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 disabled:bg-gray-600 disabled:cursor-not-allowed transition-colors"
            >
              <Search size={16} />
              {isSearching ? 'Searching...' : 'Search'}
            </button>
          </div>
        </div>

        {/* Search Result */}
        {searchResult && (
          <div className="p-3 border border-gray-600 rounded-lg bg-gray-700/30">
            {searchResult.error ? (
              <div className="text-red-400">
                <strong>Error:</strong> {searchResult.error}
              </div>
            ) : (
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-medium text-white">{searchResult.full_name || 'N/A'}</p>
                    <p className="text-sm text-gray-300">{searchResult.email}</p>
                    <p className="text-xs text-gray-400">
                      Status: {searchResult.is_active ? 'Active' : 'Inactive'} • 
                      Role: {searchResult.role === 'admin' ? 'Admin' : 'User'}
                    </p>
                  </div>
                  {searchResult.role !== 'admin' && (
                    <button
                      onClick={() => handlePromoteUser(searchResult.email)}
                      disabled={isPromoting}
                      className="flex items-center gap-1 px-3 py-1 text-sm bg-emerald-500 text-white rounded-lg hover:bg-emerald-600 disabled:bg-gray-600 disabled:cursor-not-allowed transition-colors"
                    >
                      <UserPlus size={14} />
                      {isPromoting ? 'Promoting...' : 'Promote to Admin'}
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Current Settings Display */}
      <div className="p-4 border border-gray-600 rounded-lg bg-gray-700/50">
        <h3 className="text-lg font-medium text-white mb-3">Current Settings</h3>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between items-center">
            <span className="text-gray-300">Edit Mode:</span>
            <div className="flex items-center gap-2">
              <span className={`font-medium ${currentEditMode ? 'text-emerald-400' : 'text-red-400'}`}>
                {currentEditMode ? 'Enabled' : 'Disabled'}
              </span>
              {currentEditMode ? (
                <Power className="h-4 w-4 text-emerald-400" />
              ) : (
                <PowerOff className="h-4 w-4 text-red-400" />
              )}
            </div>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-gray-300">Can Edit:</span>
            <div className="flex items-center gap-2">
              <span className={`font-medium ${canEdit ? 'text-emerald-400' : 'text-red-400'}`}>
                {canEdit ? 'Yes' : 'No'}
              </span>
            </div>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-300">Admin User:</span>
            <span className="font-medium text-white">{user.email}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-300">User ID:</span>
            <span className="font-medium text-gray-400 text-xs">{settings?.user_id || 'N/A'}</span>
          </div>
        </div>
      </div>
    </div>
  );
} 