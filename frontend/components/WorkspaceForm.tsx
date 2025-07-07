import { useState } from 'react';
import { X, Database, Save, Loader2 } from 'lucide-react';
import { WorkspaceRequest } from '../lib/api';

interface Workspace {
  _id: string;
  name: string;
  description?: string;
  db_connection: {
    db_name: string;
    username: string;
    password: string;
    host: string;
    port: string;
    db_type: string;
  };
  created_at: string;
  updated_at: string;
}

interface WorkspaceFormProps {
  workspace?: Workspace | null;
  onSubmit: (data: WorkspaceRequest) => Promise<void>;
  onClose: () => void;
}

const DB_TYPES = [
  { value: 'postgresql', label: 'PostgreSQL' },
  { value: 'mysql', label: 'MySQL' },
  { value: 'sqlite', label: 'SQLite' },
  { value: 'mssql', label: 'SQL Server' },
];

export default function WorkspaceForm({ workspace, onSubmit, onClose }: WorkspaceFormProps) {
  const [formData, setFormData] = useState<WorkspaceRequest>({
    name: workspace?.name || '',
    description: workspace?.description || '',
    db_connection: {
      db_name: workspace?.db_connection.db_name || '',
      username: workspace?.db_connection.username || '',
      password: workspace?.db_connection.password || '',
      host: workspace?.db_connection.host || 'localhost',
      port: String(workspace?.db_connection.port || '5432'),
      db_type: workspace?.db_connection.db_type || 'postgresql'
    }
  });
  
  const [loading, setLoading] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});

  const validateForm = () => {
    const newErrors: Record<string, string> = {};

    if (!formData.name.trim()) {
      newErrors.name = 'Workspace name is required';
    }

    if (!formData.db_connection.db_name.trim()) {
      newErrors.db_name = 'Database name is required';
    }

    if (!formData.db_connection.username.trim()) {
      newErrors.username = 'Username is required';
    }

    if (!formData.db_connection.host.trim()) {
      newErrors.host = 'Host is required';
    }

    const portStr = String(formData.db_connection.port).trim();
    if (!portStr) {
      newErrors.port = 'Port is required';
    } else if (!/^\d+$/.test(portStr)) {
      newErrors.port = 'Port must be a number';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!validateForm()) return;

    try {
      setLoading(true);
      await onSubmit(formData);
    } catch (error: any) {
      console.error('Error submitting form:', error);
      setErrors({ submit: error.message || 'Failed to save workspace' });
    } finally {
      setLoading(false);
    }
  };

  const handleInputChange = (field: string, value: string) => {
    if (field.startsWith('db_connection.')) {
      const dbField = field.split('.')[1];
      setFormData(prev => ({
        ...prev,
        db_connection: {
          ...prev.db_connection,
          [dbField]: value
        }
      }));
    } else {
      setFormData(prev => ({
        ...prev,
        [field]: value
      }));
    }
    
    // Clear error when user starts typing
    if (errors[field]) {
      setErrors(prev => ({ ...prev, [field]: '' }));
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-gray-800 rounded-2xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto border border-gray-700">
        <div className="flex items-center justify-between p-6 border-b border-gray-700">
          <div className="flex items-center space-x-3">
            <div className="p-2 bg-gradient-to-r from-blue-500 to-purple-600 rounded-lg">
              <Database className="h-5 w-5 text-white" />
            </div>
            <h2 className="text-xl font-bold text-white">
              {workspace ? 'Edit Workspace' : 'Create New Workspace'}
            </h2>
          </div>
          <button
            onClick={onClose}
            className="p-2 text-gray-400 hover:text-gray-300 hover:bg-gray-700 rounded-lg transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-6">
          {errors.submit && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4">
              <p className="text-red-400 text-sm">{errors.submit}</p>
            </div>
          )}

          {/* Workspace Details */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold text-white">Workspace Details</h3>
            
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">
                Workspace Name *
              </label>
              <input
                type="text"
                value={formData.name}
                onChange={(e) => handleInputChange('name', e.target.value)}
                className={`w-full px-4 py-2 bg-gray-700 border rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                  errors.name ? 'border-red-500' : 'border-gray-600'
                }`}
                placeholder="Enter workspace name"
              />
              {errors.name && <p className="text-red-400 text-sm mt-1">{errors.name}</p>}
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">
                Description
              </label>
              <textarea
                value={formData.description}
                onChange={(e) => handleInputChange('description', e.target.value)}
                rows={3}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="Enter workspace description (optional)"
              />
            </div>
          </div>

          {/* Database Connection */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold text-white">Database Connection</h3>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Database Type *
                </label>
                <select
                  value={formData.db_connection.db_type}
                  onChange={(e) => handleInputChange('db_connection.db_type', e.target.value)}
                  className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  {DB_TYPES.map((type) => (
                    <option key={type.value} value={type.value}>
                      {type.label}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Database Name *
                </label>
                <input
                  type="text"
                  value={formData.db_connection.db_name}
                  onChange={(e) => handleInputChange('db_connection.db_name', e.target.value)}
                  className={`w-full px-4 py-2 bg-gray-700 border rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                    errors.db_name ? 'border-red-500' : 'border-gray-600'
                  }`}
                  placeholder="Enter database name"
                />
                {errors.db_name && <p className="text-red-400 text-sm mt-1">{errors.db_name}</p>}
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Host *
                </label>
                <input
                  type="text"
                  value={formData.db_connection.host}
                  onChange={(e) => handleInputChange('db_connection.host', e.target.value)}
                  className={`w-full px-4 py-2 bg-gray-700 border rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                    errors.host ? 'border-red-500' : 'border-gray-600'
                  }`}
                  placeholder="Enter host address"
                />
                {errors.host && <p className="text-red-400 text-sm mt-1">{errors.host}</p>}
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Port *
                </label>
                <input
                  type="text"
                  value={formData.db_connection.port}
                  onChange={(e) => handleInputChange('db_connection.port', e.target.value)}
                  className={`w-full px-4 py-2 bg-gray-700 border rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                    errors.port ? 'border-red-500' : 'border-gray-600'
                  }`}
                  placeholder="Enter port number"
                />
                {errors.port && <p className="text-red-400 text-sm mt-1">{errors.port}</p>}
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Username *
                </label>
                <input
                  type="text"
                  value={formData.db_connection.username}
                  onChange={(e) => handleInputChange('db_connection.username', e.target.value)}
                  className={`w-full px-4 py-2 bg-gray-700 border rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                    errors.username ? 'border-red-500' : 'border-gray-600'
                  }`}
                  placeholder="Enter username"
                />
                {errors.username && <p className="text-red-400 text-sm mt-1">{errors.username}</p>}
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Password
                </label>
                <input
                  type="password"
                  value={formData.db_connection.password}
                  onChange={(e) => handleInputChange('db_connection.password', e.target.value)}
                  className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="Enter password"
                />
              </div>
            </div>
          </div>

          {/* Form Actions */}
          <div className="flex items-center justify-end space-x-4 pt-6 border-t border-gray-700">
            <button
              type="button"
              onClick={onClose}
              className="px-6 py-2 text-gray-300 border border-gray-600 rounded-lg hover:bg-gray-700 hover:text-white transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading}
              className="flex items-center space-x-2 px-6 py-2 bg-gradient-to-r from-blue-500 to-purple-600 text-white rounded-lg hover:from-blue-600 hover:to-purple-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200"
            >
              {loading ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span>Saving...</span>
                </>
              ) : (
                <>
                  <Save className="h-4 w-4" />
                  <span>{workspace ? 'Update' : 'Create'} Workspace</span>
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
} 