import { useState } from 'react';
import { Shield, AlertTriangle, CheckCircle, XCircle, ChevronDown, ChevronRight, AlertCircle, Info } from 'lucide-react';

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

interface VerificationResultProps {
  verificationResult: VerificationResult;
  className?: string;
}

export default function VerificationResult({ verificationResult, className = '' }: VerificationResultProps) {
  const [isRecommendationsOpen, setIsRecommendationsOpen] = useState(false);

  const getVerdictIcon = () => {
    switch (verificationResult.overall_verdict) {
      case 'SAFE_TO_EXECUTE':
        return <CheckCircle className="h-5 w-5 text-green-500" />;
      case 'REQUIRES_REVIEW':
        return <AlertTriangle className="h-5 w-5 text-yellow-500" />;
      case 'DO_NOT_EXECUTE':
        return <XCircle className="h-5 w-5 text-red-500" />;
      default:
        return <AlertCircle className="h-5 w-5 text-gray-500" />;
    }
  };

  const getVerdictColor = () => {
    switch (verificationResult.overall_verdict) {
      case 'SAFE_TO_EXECUTE':
        return 'border-green-500/20 bg-green-500/10';
      case 'REQUIRES_REVIEW':
        return 'border-yellow-500/20 bg-yellow-500/10';
      case 'DO_NOT_EXECUTE':
        return 'border-red-500/20 bg-red-500/10';
      default:
        return 'border-gray-500/20 bg-gray-500/10';
    }
  };

  const getVerdictTextColor = () => {
    switch (verificationResult.overall_verdict) {
      case 'SAFE_TO_EXECUTE':
        return 'text-green-400';
      case 'REQUIRES_REVIEW':
        return 'text-yellow-400';
      case 'DO_NOT_EXECUTE':
        return 'text-red-400';
      default:
        return 'text-gray-400';
    }
  };

  return (
    <div className={`bg-gray-800 rounded-lg border border-gray-600 ${className}`}>
      {/* Header */}
      <div className={`px-4 py-3 border-b border-gray-600 ${getVerdictColor()}`}>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Shield className="h-5 w-5 text-blue-400" />
            <h3 className="font-semibold text-white">Query Verification</h3>
          </div>
          <div className={`flex items-center gap-2 ${getVerdictTextColor()}`}>
            {getVerdictIcon()}
            <span className="font-medium text-sm">
              {verificationResult.overall_verdict.replace(/_/g, ' ')}
            </span>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="p-4 space-y-4">
        {/* Safety and Correctness Status */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="flex items-center gap-2">
            {verificationResult.is_safe ? (
              <CheckCircle className="h-4 w-4 text-green-500" />
            ) : (
              <XCircle className="h-4 w-4 text-red-500" />
            )}
            <span className={`text-sm font-medium ${verificationResult.is_safe ? 'text-green-400' : 'text-red-400'}`}>
              {verificationResult.is_safe ? 'Safe' : 'Safety Concerns'}
            </span>
          </div>
          <div className="flex items-center gap-2">
            {verificationResult.is_correct ? (
              <CheckCircle className="h-4 w-4 text-green-500" />
            ) : (
              <XCircle className="h-4 w-4 text-red-500" />
            )}
            <span className={`text-sm font-medium ${verificationResult.is_correct ? 'text-green-400' : 'text-red-400'}`}>
              {verificationResult.is_correct ? 'Syntactically Correct' : 'Syntax Issues'}
            </span>
          </div>
        </div>

        {/* Impact Assessment */}
        <div className="bg-blue-500/10 border border-blue-500/20 rounded-lg p-3">
          <div className="flex items-start gap-2">
            <Info className="h-4 w-4 text-blue-400 mt-0.5" />
            <div>
              <h4 className="font-medium text-blue-300 text-sm">Impact Assessment</h4>
              <p className="text-blue-200 text-sm mt-1">{verificationResult.impact_assessment}</p>
              <p className="text-blue-300 text-xs mt-2">
                <strong>Estimated affected records:</strong> {verificationResult.estimated_affected_records}
              </p>
            </div>
          </div>
        </div>

        {/* Safety Issues */}
        {verificationResult.safety_issues.length > 0 && (
          <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3">
            <div className="flex items-start gap-2">
              <AlertTriangle className="h-4 w-4 text-red-400 mt-0.5" />
              <div>
                <h4 className="font-medium text-red-300 text-sm">Safety Issues</h4>
                <ul className="text-red-200 text-sm mt-1 space-y-1">
                  {verificationResult.safety_issues.map((issue, index) => (
                    <li key={index} className="flex items-start gap-1">
                      <span className="text-red-400">•</span>
                      <span>{issue}</span>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          </div>
        )}

        {/* Correctness Issues */}
        {verificationResult.correctness_issues.length > 0 && (
          <div className="bg-orange-500/10 border border-orange-500/20 rounded-lg p-3">
            <div className="flex items-start gap-2">
              <AlertCircle className="h-4 w-4 text-orange-400 mt-0.5" />
              <div>
                <h4 className="font-medium text-orange-300 text-sm">Correctness Issues</h4>
                <ul className="text-orange-200 text-sm mt-1 space-y-1">
                  {verificationResult.correctness_issues.map((issue, index) => (
                    <li key={index} className="flex items-start gap-1">
                      <span className="text-orange-400">•</span>
                      <span>{issue}</span>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          </div>
        )}

        {/* Recommendations - Collapsible */}
        {verificationResult.recommendations.length > 0 && (
          <div className="border border-gray-600 rounded-lg">
            <button
              onClick={() => setIsRecommendationsOpen(!isRecommendationsOpen)}
              className="w-full px-3 py-2 flex items-center justify-between bg-gray-700 hover:bg-gray-600 transition-colors rounded-t-lg"
            >
              <div className="flex items-center gap-2">
                <Info className="h-4 w-4 text-gray-400" />
                <span className="font-medium text-white text-sm">
                  Recommendations ({verificationResult.recommendations.length})
                </span>
              </div>
              {isRecommendationsOpen ? (
                <ChevronDown className="h-4 w-4 text-gray-400" />
              ) : (
                <ChevronRight className="h-4 w-4 text-gray-400" />
              )}
            </button>
            {isRecommendationsOpen && (
              <div className="p-3 border-t border-gray-600">
                <ul className="text-gray-300 text-sm space-y-2">
                  {verificationResult.recommendations.map((recommendation, index) => (
                    <li key={index} className="flex items-start gap-2">
                      <span className="text-blue-400 mt-1">•</span>
                      <span>{recommendation}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        {/* Explanation */}
        {verificationResult.explanation && (
          <div className="bg-gray-700 border border-gray-600 rounded-lg p-3">
            <h4 className="font-medium text-white text-sm mb-2">Explanation</h4>
            <p className="text-gray-300 text-sm">{verificationResult.explanation}</p>
          </div>
        )}
      </div>
    </div>
  );
} 