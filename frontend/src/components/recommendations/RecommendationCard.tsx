// src/components/recommendations/RecommendationCard.tsx
import React from 'react';
import { NormalizedRecommendation, RecommendationDetail } from "@/types/recommendations";
import { AlertCircle, ArrowDownCircle, CheckCircle, Clock } from 'lucide-react';

interface RecommendationCardProps {
    recommendation: NormalizedRecommendation;
}

const getSeverityStyles = (severity: 'High' | 'Medium' | 'Low') => {
    switch (severity) {
        // ... (Styles logic remains the same)
        case 'High':
            return {
                border: 'border-red-500',
                text: 'text-red-700',
                icon: <AlertCircle className="w-5 h-5 text-red-500" />
            };
        case 'Medium':
            return {
                border: 'border-yellow-500',
                text: 'text-yellow-700',
                icon: <Clock className="w-5 h-5 text-yellow-500" />
            };
        case 'Low':
            return {
                border: 'border-green-500',
                text: 'text-green-700',
                icon: <CheckCircle className="w-5 h-5 text-green-500" />
            };
        default:
            return {
                border: 'border-gray-300',
                text: 'text-gray-700',
                icon: <ArrowDownCircle className="w-5 h-5 text-gray-500" />
            };
    }
};

const RecommendationCard: React.FC<RecommendationCardProps> = ({ recommendation: rec }) => {
    const { border, text, icon } = getSeverityStyles(rec.severity);
    const estSaving = rec.monthlyForecast.toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 0, maximumFractionDigits: 0 });

    return (
        <div className={`p-6 border-l-4 ${border} rounded-lg shadow-md bg-white transition-shadow hover:shadow-lg`}>
            {/* ... (Header content remains the same) */}
            <div className="flex justify-between items-start">
                <div className="flex-1 min-w-0">
                    <div className="flex items-center space-x-2">
                        {icon}
                        <h3 className="text-xl font-semibold text-gray-800 truncate">{rec.title}</h3>
                    </div>
                    <p className={`text-sm font-medium mt-2 ${text}`}>
                        <span className="font-semibold">Severity:</span> {rec.severity} | <span className="font-semibold">Anomaly:</span> {rec.anomalyTimestamp.split('T')[0]}
                    </p>
                    <p className="text-xs text-gray-500 mt-1" title={rec.resourceId}>
                         <span className="font-semibold">Resource ID:</span> <span className="truncate inline-block max-w-full">{rec.resourceId}</span>
                    </p>
                </div>
                <div className="text-right ml-6 flex-shrink-0">
                    <p className="text-3xl font-bold text-green-600">
                        {rec.totalSavingPercent}%
                    </p>
                    <p className="text-sm text-gray-500">Total Est. Savings</p>
                    <p className="text-md font-semibold text-gray-600 mt-1">
                        {estSaving}/Month
                    </p>
                </div>
            </div>

            {/* Details Section - FIX APPLIED HERE */}
            <div className="mt-4 pt-4 border-t border-gray-100">
                <h4 className="text-sm font-semibold text-gray-700 mb-2">Detailed Recommendations:</h4>
                <ul className="space-y-2">
                    {/* FIX: Use optional chaining or check if rec.details exists and is an array 
                      before mapping. We also check if detail.text and detail.saving_pct exist.
                    */}
                    {Array.isArray(rec.details) && rec.details.map((detail: RecommendationDetail, index) => {
                        // Ensure both text and saving_pct are present before rendering the list item
                        if (!detail.text || detail.saving_pct === undefined) return null; 

                        return (
                            <li key={index} className="flex justify-between items-start text-sm text-gray-600">
                                <span className="flex-1 mr-4">
                                    • {detail.text}
                                </span>
                                <span className="font-mono text-xs bg-green-50 text-green-800 px-2 py-0.5 rounded flex-shrink-0">
                                    {detail.saving_pct}% Saving
                                </span>
                            </li>
                        );
                    })}
                    {/* Fallback for empty details array */}
                    {Array.isArray(rec.details) && rec.details.length === 0 && (
                        <li className="text-sm text-gray-500 italic">No specific steps provided.</li>
                    )}
                </ul>
            </div>
        </div>
    );
};

export default RecommendationCard;