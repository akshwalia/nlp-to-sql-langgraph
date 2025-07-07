import { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { 
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, 
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  ScatterChart, Scatter, AreaChart, Area, ComposedChart, 
  RadialBarChart, RadialBar, Treemap, FunnelChart, Funnel, LabelList,
  ReferenceLine, ReferenceArea
} from 'recharts';
import { 
  BarChart2, LineChart as LineChartIcon, PieChart as PieChartIcon, 
  ScatterChart as ScatterIcon, AreaChart as AreaIcon, Activity,
  CircleUser as RadialIcon, Triangle, X, Check, Settings, ChevronDown,
  Target, Filter, Layers, TrendingUp, Gauge, Zap, Eye, ArrowLeft, Download,
  Grid3X3, TrendingDown, Plus, Save, Trash2, Star, Lightbulb
} from 'lucide-react';
import { saveChartToMessage, deleteChartFromMessage, getMessageCharts } from '../lib/api';

// Define chart types
export type ChartType = 'bar' | 'line' | 'pie' | 'donut' | 'scatter' | 'area' | 'composed' | 'radial' | 'treemap' | 'funnel' | 'gauge' | 'waterfall' | 'heatmap' | 'pyramid' | 'bubble';

interface VisualizationProps {
  data: any[];
  onClose: () => void;
  embedded?: boolean;
  messageId?: string; // Add message ID for saving charts
  visualizationRecommendations?: any; // LLM recommendations
  savedCharts?: any[]; // Previously saved charts
  databaseType?: string; // Database type for context
  tableSchema?: any; // Table schema information
}

interface ChartDataItem {
  name: string;
  value: number;
}

interface ChartOption {
  type: ChartType;
  label: string;
  icon: React.ReactNode;
  recommended?: boolean;
  compatible: boolean;
  description?: string;
}

interface RecommendedChart {
  chart_type: string;
  title: string;
  description: string;
  x_axis: string;
  y_axis: string;
  secondary_y_axis?: string;
  chart_config?: any;
  confidence_score: number;
}

interface SavedChart {
  chart_id: string;
  chart_type: string;
  title: string;
  x_axis: string;
  y_axis: string;
  secondary_y_axis?: string;
  chart_config?: any;
  created_by: string;
  created_at: string;
}

// Helper function to format numbers
const formatNumber = (value: any): number => {
  if (typeof value === 'number') return value;
  if (typeof value === 'string') {
    const parsed = parseFloat(value);
    return isNaN(parsed) ? 0 : parsed;
  }
  return 0;
};

// Helper function to format tooltip values
const formatTooltipValue = (value: any, name: string) => {
  if (typeof value === 'number') {
    return [value.toLocaleString(), name];
  }
  return [value, name];
};

// Helper function to get display name for a row based on column mapping
const getDisplayName = (row: any, column: string, chart?: RecommendedChart | SavedChart): string => {
  // Check if we need to combine name fields
  const chartConfig = chart?.chart_config;
  
  // If chart config suggests name combination
  if (chartConfig?.name_combination) {
    const nameFields = chartConfig.name_combination.split('_');
    if (nameFields.length >= 2) {
      const combinedName = nameFields
        .map(field => row[field] || '')
        .filter(part => part.trim())
        .join(' ')
        .trim();
      if (combinedName) return combinedName;
    }
  }
  
  // Check for common name combinations in the data
  if (column.toLowerCase().includes('name') || column.toLowerCase().includes('employee')) {
    // Look for firstname, lastname combinations
    if (row.firstname && row.lastname) {
      return `${row.firstname} ${row.lastname}`.trim();
    }
    // Look for first_name, last_name combinations
    if (row.first_name && row.last_name) {
      return `${row.first_name} ${row.last_name}`.trim();
    }
    // Look for name field
    if (row.name) {
      return row.name;
    }
  }
  
  // Fall back to the actual column value
  const value = row[column];
  if (value !== null && value !== undefined) {
    return String(value);
  }
  
  // Last resort: try to find a suitable column
  const availableKeys = Object.keys(row);
  const nameRelatedKeys = availableKeys.filter(key => 
    key.toLowerCase().includes('name') || 
    key.toLowerCase().includes('firstname') || 
    key.toLowerCase().includes('lastname')
  );
  
  if (nameRelatedKeys.length > 0) {
    return String(row[nameRelatedKeys[0]] || 'Unknown');
  }
  
  return 'Unknown';
};

// Helper function to map recommended columns to actual data columns
const mapColumnToData = (recommendedColumn: string, availableColumns: string[]): string => {
  // If the recommended column exists in data, use it
  if (availableColumns.includes(recommendedColumn)) {
    return recommendedColumn;
  }
  
  const lowerRecommended = recommendedColumn.toLowerCase();
  
  // Handle common mappings
  const columnMappings: { [key: string]: string[] } = {
    'employee name': ['firstname', 'lastname', 'name', 'employee_name', 'full_name'],
    'full name': ['firstname', 'lastname', 'name', 'full_name'],
    'name': ['firstname', 'lastname', 'name', 'employee_name'],
    'order count': ['order_count', 'ordercount', 'orders', 'count'],
    'total amount': ['total_amount', 'amount', 'total', 'value'],
    'total sales': ['total_sales', 'sales', 'total_amount', 'amount'],
    'revenue': ['revenue', 'total_sales', 'sales', 'amount'],
    'quantity': ['quantity', 'qty', 'count', 'amount'],
    'price': ['price', 'unit_price', 'amount', 'cost'],
    'date': ['date', 'created_at', 'order_date', 'date_created'],
    'age': ['age', 'years', 'employee_age']
  };
  
  // Check for direct mapping
  if (columnMappings[lowerRecommended]) {
    for (const candidate of columnMappings[lowerRecommended]) {
      if (availableColumns.includes(candidate)) {
        return candidate;
      }
    }
  }
  
  // Try partial matches
  for (const available of availableColumns) {
    const lowerAvailable = available.toLowerCase();
    
    // Check if available column contains the recommended word
    if (lowerRecommended.includes(lowerAvailable) || lowerAvailable.includes(lowerRecommended)) {
      return available;
    }
    
    // Handle common patterns
    if (lowerRecommended.includes('count') && lowerAvailable.includes('count')) {
      return available;
    }
    if (lowerRecommended.includes('amount') && lowerAvailable.includes('amount')) {
      return available;
    }
    if (lowerRecommended.includes('name') && lowerAvailable.includes('name')) {
      return available;
    }
  }
  
  // Fallback to first available column
  return availableColumns[0] || recommendedColumn;
};

export default function Visualization({ 
  data, 
  onClose, 
  embedded = false, 
  messageId,
  visualizationRecommendations,
  savedCharts: initialSavedCharts = [],
  databaseType,
  tableSchema
}: VisualizationProps) {
  // State management
  const [currentView, setCurrentView] = useState<'recommendations' | 'create' | 'saved'>('recommendations');
  const [savedCharts, setSavedCharts] = useState<SavedChart[]>(initialSavedCharts);
  const [recommendedCharts, setRecommendedCharts] = useState<RecommendedChart[]>([]);
  const [isVisualizable, setIsVisualizable] = useState(true);
  const [notVisualizableReason, setNotVisualizableReason] = useState<string>('');

  // Chart creation states
  const [chartType, setChartType] = useState<ChartType>('bar');
  const [chartTitle, setChartTitle] = useState('');
  const [xAxis, setXAxis] = useState<string>('');
  const [yAxis, setYAxis] = useState<string>('');
  const [secondaryYAxis, setSecondaryYAxis] = useState<string>('');
  const [availableColumns, setAvailableColumns] = useState<string[]>([]);
  const [selectedRecommendationIndex, setSelectedRecommendationIndex] = useState<number | null>(null);
  const [isMounted, setIsMounted] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  // Ensure we're on the client side
  useEffect(() => {
    setIsMounted(true);
  }, []);
  
  // Load chart data and recommendations
  useEffect(() => {
    if (!data || data.length === 0) return;
    
    const columns = Object.keys(data[0]);
    setAvailableColumns(columns);
    
    // Set default axes
    setXAxis(columns[0] || '');
    setYAxis(columns[1] || columns[0] || '');

    // Process visualization recommendations
    if (visualizationRecommendations) {
      setIsVisualizable(visualizationRecommendations.is_visualizable);
      setNotVisualizableReason(visualizationRecommendations.reason || '');
      setRecommendedCharts(visualizationRecommendations.recommended_charts || []);
    }

    // Load saved charts if messageId is provided
    if (messageId) {
      loadSavedCharts();
    }
  }, [data, visualizationRecommendations, messageId]);

  const loadSavedCharts = async () => {
    if (!messageId) return;
    
    try {
      const response = await getMessageCharts(messageId);
      if (response.success) {
        setSavedCharts(response.saved_charts || []);
      }
    } catch (error) {
      console.error('Error loading saved charts:', error);
    }
  };

  // COLORS for charts - Updated for dark theme with more variety
  const COLORS = [
    '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#06B6D4', '#84CC16', '#F97316', 
    '#EC4899', '#6366F1', '#14B8A6', '#F472B6', '#A855F7', '#22D3EE', '#FCD34D', '#FB7185'
  ];

  // Prepare data for visualization
  const prepareChartData = (chart?: RecommendedChart | SavedChart): any[] => {
    if (!data || data.length === 0) return [];

    // Map recommended columns to actual data columns
    const xCol = mapColumnToData(chart?.x_axis || xAxis, availableColumns);
    const yCol = mapColumnToData(chart?.y_axis || yAxis, availableColumns);
    const chartTypeToUse = chart?.chart_type || chartType;

    if (!xCol || !yCol) return [];

    // For pie, donut, radial, funnel, and treemap charts, we need to aggregate data
    if (['pie', 'donut', 'radial', 'funnel', 'treemap'].includes(chartTypeToUse)) {
      const aggregated: { [key: string]: number } = {};
      data.forEach(row => {
        const key = getDisplayName(row, xCol, chart);
        const value = formatNumber(row[yCol] || 0);
        
        if (aggregated[key]) {
          aggregated[key] = formatNumber(aggregated[key] + value);
        } else {
          aggregated[key] = value;
        }
      });
      
      return Object.entries(aggregated).map(([name, value]) => ({ 
        name: name.length > 15 ? name.substring(0, 12) + '...' : name,
        fullName: name,
        value 
      }));
    }
    
    // For scatter and bubble plots
    if (chartTypeToUse === 'scatter' || chartTypeToUse === 'bubble') {
      return data.map((row, index) => {
        // For scatter plots, we need numerical values for both axes
        // But we can use a display name for the point label
        const displayName = getDisplayName(row, xCol, chart);
        const xValue = formatNumber(row[xCol] || 0);
        const yValue = formatNumber(row[yCol] || 0);
        
        return {
          name: displayName,
          [xCol]: xValue,  // Use actual column name for axis
          [yCol]: yValue,  // Use actual column name for axis
          z: chartTypeToUse === 'bubble' ? formatNumber(row[secondaryYAxis] || row[yCol] || 10) : 10,
          fill: COLORS[index % COLORS.length]
        };
      });
    }
    
    // For bar, line, and area charts
    return data.map(row => {
      const displayName = getDisplayName(row, xCol, chart);
      const value = formatNumber(row[yCol] || 0);
      
      return {
        name: displayName,
        value: value
      };
    });
  };
  
  const renderChart = (chart?: RecommendedChart | SavedChart) => {
    const chartData = prepareChartData(chart);
    const chartTypeToUse = chart?.chart_type || chartType;
    
    if (chartData.length === 0) {
    return (
        <div className="text-gray-400 text-center h-full flex items-center justify-center">
          <p>No data available to visualize</p>
        </div>
      );
    }

    switch (chartTypeToUse) {
      case 'bar':
        return (
          <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 30 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="name" tick={{ fill: '#D1D5DB' }} />
            <YAxis tick={{ fill: '#D1D5DB' }} />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '8px',
                color: '#F9FAFB'
              }}
              formatter={formatTooltipValue}
            />
            <Legend />
            <Bar dataKey="value" name={chart?.y_axis || yAxis} fill="#3B82F6" />
          </BarChart>
        );
      
      case 'line':
        return (
          <LineChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 30 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="name" tick={{ fill: '#D1D5DB' }} />
            <YAxis tick={{ fill: '#D1D5DB' }} />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '8px',
                color: '#F9FAFB'
              }}
              formatter={formatTooltipValue}
            />
            <Legend />
            <Line type="monotone" dataKey="value" name={chart?.y_axis || yAxis} stroke="#3B82F6" activeDot={{ r: 8 }} />
          </LineChart>
        );
      
      case 'pie':
        return (
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              labelLine={false}
              outerRadius={100}
              fill="#3B82F6"
              dataKey="value"
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '8px',
                color: '#F9FAFB'
              }} 
              formatter={(value: number, name: string, props: any) => [formatNumber(value), props.payload.fullName || name]} 
            />
            <Legend />
          </PieChart>
        );

      case 'donut':
        return (
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              labelLine={false}
              outerRadius={100}
              innerRadius={40}
              fill="#3B82F6"
              dataKey="value"
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '8px',
                color: '#F9FAFB'
              }} 
              formatter={(value: number, name: string, props: any) => [formatNumber(value), props.payload.fullName || name]} 
            />
            <Legend />
          </PieChart>
        );
      
      case 'area':
        return (
          <AreaChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 30 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="name" tick={{ fill: '#D1D5DB' }} />
            <YAxis tick={{ fill: '#D1D5DB' }} />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '8px',
                color: '#F9FAFB'
              }}
              formatter={formatTooltipValue}
            />
            <Legend />
            <Area type="monotone" dataKey="value" name={chart?.y_axis || yAxis} stroke="#3B82F6" fill="#3B82F6" fillOpacity={0.6} />
          </AreaChart>
        );

      case 'scatter':
        const xColMapped = mapColumnToData(chart?.x_axis || xAxis, availableColumns);
        const yColMapped = mapColumnToData(chart?.y_axis || yAxis, availableColumns);
        return (
          <ScatterChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 30 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis 
              dataKey={xColMapped} 
              type="number" 
              tick={{ fill: '#D1D5DB' }} 
              name={chart?.x_axis || xAxis}
            />
            <YAxis 
              dataKey={yColMapped} 
              type="number" 
              tick={{ fill: '#D1D5DB' }} 
              name={chart?.y_axis || yAxis}
            />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '8px',
                color: '#F9FAFB'
              }}
              formatter={formatTooltipValue}
            />
            <Legend />
            <Scatter name={chart?.y_axis || yAxis} fill="#3B82F6" />
          </ScatterChart>
        );
      
      case 'composed':
        return (
          <ComposedChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 30 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="name" tick={{ fill: '#D1D5DB' }} />
            <YAxis tick={{ fill: '#D1D5DB' }} />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '8px',
                color: '#F9FAFB'
              }}
              formatter={formatTooltipValue}
            />
            <Legend />
            <Bar dataKey="value" name={chart?.y_axis || yAxis} fill="#3B82F6" />
            <Line type="monotone" dataKey="value" name={`${chart?.y_axis || yAxis} (Line)`} stroke="#10B981" />
          </ComposedChart>
        );
      
      case 'radial':
        return (
          <RadialBarChart data={chartData} cx="50%" cy="50%" innerRadius="10%" outerRadius="80%">
            <RadialBar 
              label={{ position: 'insideStart', fill: '#fff' }} 
              background 
              dataKey="value" 
            />
            <Legend iconSize={18} layout="vertical" verticalAlign="middle" wrapperStyle={{ color: '#D1D5DB' }} />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '8px',
                color: '#F9FAFB'
              }} 
              formatter={formatTooltipValue}
            />
          </RadialBarChart>
        );
      
      case 'treemap':
        return (
          <Treemap
            data={chartData}
            dataKey="value"
            stroke="#374151"
            fill="#3B82F6"
          />
        );

      case 'funnel':
        return (
          <FunnelChart>
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '8px',
                color: '#F9FAFB'
              }} 
              formatter={formatTooltipValue}
            />
            <Funnel
              dataKey="value"
              data={chartData}
              isAnimationActive
            >
              <LabelList position="center" fill="#fff" stroke="none" />
            </Funnel>
          </FunnelChart>
        );

      default:
        // Create a proper chart object with required fields
        const defaultChart: RecommendedChart = {
          chart_type: 'bar',
          title: chart?.title || 'Default Chart',
          description: 'Default bar chart',
          x_axis: chart?.x_axis || xAxis,
          y_axis: chart?.y_axis || yAxis,
          confidence_score: 0.5
        };
        return renderChart(defaultChart);
    }
  };

  const handleSaveChart = async () => {
    if (!messageId || !chartTitle.trim()) return;

    setIsLoading(true);
    try {
      const chartData = {
        chart_type: chartType,
        title: chartTitle.trim(),
        x_axis: xAxis,
        y_axis: yAxis,
        secondary_y_axis: secondaryYAxis || null,
        chart_config: {},
      };

      const response = await saveChartToMessage(messageId, chartData);
      if (response.success) {
        await loadSavedCharts(); // Reload saved charts
        setCurrentView('recommendations'); // Switch back to recommendations view
        setChartTitle(''); // Reset form
      }
    } catch (error) {
      console.error('Error saving chart:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleDeleteChart = async (chartId: string) => {
    if (!messageId) return;

    try {
      const response = await deleteChartFromMessage(messageId, chartId);
      if (response.success) {
        await loadSavedCharts(); // Reload saved charts
      }
    } catch (error) {
      console.error('Error deleting chart:', error);
    }
  };

  const handleSelectRecommendation = (index: number) => {
    const recommendation = recommendedCharts[index];
    setSelectedRecommendationIndex(index);
    setChartType(recommendation.chart_type as ChartType);
    setChartTitle(recommendation.title);
    
    // Map the recommended column names to actual data columns
    const mappedXAxis = mapColumnToData(recommendation.x_axis, availableColumns);
    const mappedYAxis = mapColumnToData(recommendation.y_axis, availableColumns);
    const mappedSecondaryYAxis = recommendation.secondary_y_axis 
      ? mapColumnToData(recommendation.secondary_y_axis, availableColumns) 
      : '';
    
    setXAxis(mappedXAxis);
    setYAxis(mappedYAxis);
    setSecondaryYAxis(mappedSecondaryYAxis);
    setCurrentView('create');
  };

  // CSV Export functionality
  const exportChartDataToCSV = () => {
    if (!data || data.length === 0) return;
    
    const headers = Object.keys(data[0]);
    const csvContent = [
      headers.map(header => `"${header}"`).join(','),
      ...data.map(row => 
        headers.map(header => {
          const value = row[header];
          if (value === null || value === undefined) return '""';
          if (typeof value === 'object') return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
          return `"${String(value).replace(/"/g, '""')}"`;
        }).join(',')
      )
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `chart_data_${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };
  
  // If embedded in dashboard, render a simplified version
  if (embedded) {
    return (
      <div className="h-full w-full">
        {data && data.length > 0 ? (
          <ResponsiveContainer width="100%" height="100%">
            {renderChart()}
          </ResponsiveContainer>
        ) : (
          <div className="text-gray-400 text-center h-full flex items-center justify-center">
            <p>No data available to visualize</p>
          </div>
        )}
      </div>
    );
  }
  
  // Render main views
  const renderRecommendationsView = () => (
    <div className="space-y-6">
      {!isVisualizable ? (
        <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-xl p-6 text-center">
          <Lightbulb className="h-12 w-12 text-yellow-500 mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-yellow-300 mb-2">Data Not Suitable for Visualization</h3>
          <p className="text-yellow-200">{notVisualizableReason}</p>
        </div>
      ) : (
        <>
          {recommendedCharts.length > 0 && (
            <div className="bg-blue-500/10 border border-blue-500/30 rounded-xl p-6">
              <div className="flex items-center gap-3 mb-4">
                <Star className="h-6 w-6 text-blue-400" />
                <h3 className="text-lg font-semibold text-blue-300">AI Recommended Charts</h3>
              </div>
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                {recommendedCharts.map((chart, index) => (
                  <div key={index} className="bg-gray-700/50 rounded-lg p-4 border border-gray-600">
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="font-medium text-white">{chart.title}</h4>
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-blue-400 bg-blue-400/20 px-2 py-1 rounded">
                          {Math.round(chart.confidence_score * 100)}% confidence
                        </span>
                    <button
                          onClick={() => handleSelectRecommendation(index)}
                          className="bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-sm transition-colors"
                    >
                          Use This
                    </button>
                  </div>
                </div>
                    <p className="text-gray-300 text-sm mb-3">{chart.description}</p>
                    <div className="h-48">
                      <ResponsiveContainer width="100%" height="100%">
                        {renderChart(chart)}
                      </ResponsiveContainer>
            </div>
              </div>
                ))}
              </div>
            </div>
          )}
        </>
      )}

      {savedCharts.length > 0 && (
        <div className="bg-gray-700/30 border border-gray-600 rounded-xl p-6">
          <h3 className="text-lg font-semibold text-white mb-4">Saved Charts</h3>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {savedCharts.map((chart) => (
              <div key={chart.chart_id} className="bg-gray-700/50 rounded-lg p-4 border border-gray-600">
                <div className="flex items-center justify-between mb-3">
                  <h4 className="font-medium text-white">{chart.title}</h4>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-400 bg-gray-600/50 px-2 py-1 rounded">
                      {chart.created_by === 'user' ? 'Custom' : 'AI'}
                    </span>
              <button 
                      onClick={() => handleDeleteChart(chart.chart_id)}
                      className="text-red-400 hover:text-red-300 p-1 rounded transition-colors"
                    >
                      <Trash2 className="h-4 w-4" />
              </button>
            </div>
          </div>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    {renderChart(chart)}
                  </ResponsiveContainer>
                </div>
                </div>
            ))}
              </div>
                 </div>
      )}
               </div>
  );

  const renderCreateView = () => (
    <div className="space-y-6">
      <div className="bg-gray-700/50 p-6 rounded-xl border border-gray-600">
        <h3 className="text-lg font-semibold text-white mb-4">Create Custom Chart</h3>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">Chart Title</label>
            <input
              type="text"
              value={chartTitle}
              onChange={(e) => setChartTitle(e.target.value)}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="Enter chart title"
            />
            </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">Chart Type</label>
            <select
              value={chartType}
              onChange={(e) => setChartType(e.target.value as ChartType)}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="bar">Bar Chart</option>
              <option value="line">Line Chart</option>
              <option value="area">Area Chart</option>
              <option value="scatter">Scatter Plot</option>
              <option value="pie">Pie Chart</option>
              <option value="donut">Donut Chart</option>
              <option value="composed">Composed Chart</option>
              <option value="radial">Radial Bar Chart</option>
              <option value="treemap">Treemap</option>
              <option value="funnel">Funnel Chart</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">X-Axis</label>
                <select
                  value={xAxis}
                  onChange={(e) => setXAxis(e.target.value)}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  {availableColumns.map(col => (
                    <option key={col} value={col}>{col}</option>
                  ))}
                </select>
              </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">Y-Axis</label>
                <select
                  value={yAxis}
                  onChange={(e) => setYAxis(e.target.value)}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  {availableColumns.map(col => (
                    <option key={col} value={col}>{col}</option>
                  ))}
                </select>
            </div>
          </div>
          
        <div className="flex items-center gap-3 mb-6">
                  <button
            onClick={handleSaveChart}
            disabled={!chartTitle.trim() || isLoading}
            className="bg-green-600 hover:bg-green-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white px-4 py-2 rounded-lg flex items-center gap-2 transition-colors"
                  >
            <Save className="h-4 w-4" />
            {isLoading ? 'Saving...' : 'Save Chart'}
                  </button>
          
                    <button
            onClick={exportChartDataToCSV}
            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg flex items-center gap-2 transition-colors"
          >
            <Download className="h-4 w-4" />
            Export CSV
                    </button>
                </div>

        <div className="bg-gray-700/30 rounded-lg h-96">
          <ResponsiveContainer width="100%" height="100%">
            {renderChart()}
          </ResponsiveContainer>
                </div>
              </div>
            </div>
  );

  // Full visualization modal
  const modalContent = (
    <div 
      className="fixed inset-0 z-[9999] bg-black/60 backdrop-blur-sm flex items-center justify-center p-2 sm:p-4"
      style={{ zIndex: 9999 }}
      onClick={(e) => {
        if (e.target === e.currentTarget) {
          onClose();
        }
      }}
    >
      <div className="bg-gray-800 rounded-xl sm:rounded-2xl shadow-2xl w-full max-w-7xl max-h-[98vh] border border-gray-700 animate-in zoom-in-95 duration-300 overflow-hidden flex flex-col relative z-[10000]" style={{ zIndex: 10000 }}>
        <div className="p-3 sm:p-6 overflow-y-auto flex-1">
          {/* Header */}
          <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-4 sm:mb-6 gap-3">
            <div className="flex items-center gap-2 sm:gap-3">
              <h2 className="text-lg sm:text-xl font-bold text-white">Data Visualization</h2>
              </div>
            <div className="flex items-center space-x-2 sm:space-x-3">
              <button 
                onClick={(e) => {
                  e.stopPropagation();
                  onClose();
                }}
                className="p-1.5 sm:p-2 rounded-full hover:bg-gray-700 transition-colors text-gray-400 hover:text-white"
              >
                <X className="h-4 w-4 sm:h-5 sm:w-5" />
              </button>
            </div>
          </div>

          {/* Navigation Tabs */}
          <div className="flex flex-wrap gap-2 mb-6">
            <button
              onClick={() => setCurrentView('recommendations')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                currentView === 'recommendations' 
                  ? 'bg-blue-600 text-white' 
                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
              }`}
            >
              <Star className="h-4 w-4 inline mr-2" />
              AI Recommendations
            </button>
            
            <button
              onClick={() => setCurrentView('create')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                currentView === 'create' 
                  ? 'bg-blue-600 text-white' 
                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
              }`}
            >
              <Plus className="h-4 w-4 inline mr-2" />
              Create Chart
            </button>
          </div>

          {/* Content */}
          {currentView === 'recommendations' && renderRecommendationsView()}
          {currentView === 'create' && renderCreateView()}
        </div>
      </div>
    </div>
  );

  // Use Portal to render modal at document body level
  if (!isMounted) return null;
  
  return typeof document !== 'undefined' 
    ? createPortal(modalContent, document.body)
    : null;
} 