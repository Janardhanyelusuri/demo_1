// src/components/recommendations/AnomaliesCard.tsx
import React, { useState } from 'react';
import { AlertTriangle, ChevronDown, ChevronUp } from 'lucide-react';
import { NormalizedRecommendation } from "@/types/recommendations";

interface AnomaliesCardProps {
    recommendation: NormalizedRecommendation;
    isExpanded?: boolean;
}

const AnomaliesCard: React.FC<AnomaliesCardProps> = ({ recommendation: rec, isExpanded = false }) => {
    const [expanded, setExpanded] = useState(isExpanded);
    const anomalies = rec.anomalies || [];

    return (
        <div className="bg-white rounded-lg border border-red-200 shadow-sm hover:shadow-md transition-shadow overflow-hidden">
            {/* Header Button */}
            <button
                onClick={() => setExpanded(!expanded)}
                className="w-full p-4 flex items-center justify-between hover:bg-red-50 transition-colors"
            >
                <div className="flex items-center space-x-3">
                    <div className="bg-red-100 p-2 rounded-lg">
                        <AlertTriangle className="w-5 h-5 text-red-600" />
                    </div>
                    <div className="text-left">
                        <h3 className="text-sm font-semibold text-gray-800">Anomalies Detected</h3>
                        <p className="text-xs text-gray-500 mt-0.5">{anomalies.length} anomalies found</p>
                    </div>
                </div>
                <div className="flex items-center space-x-2">
                    <span className="text-xs font-semibold text-red-600 bg-red-50 px-2 py-1 rounded">
                        {anomalies.length}
                    </span>
                    {expanded ? (
                        <ChevronUp className="w-5 h-5 text-gray-600" />
                    ) : (
                        <ChevronDown className="w-5 h-5 text-gray-600" />
                    )}
                </div>
            </button>

            {/* Content - Collapsible */}
            {expanded && (
                <div className="border-t border-red-200 p-4">
                    {anomalies.length > 0 ? (
                        <div className="space-y-4">
                            {anomalies.map((anomaly, index) => (
                                <div key={index} className="p-4 bg-red-50 border border-red-200 rounded-lg">
                                    <div className="grid grid-cols-2 gap-4 mb-3">
                                        <div>
                                            <p className="text-xs text-gray-600 font-semibold mb-1">Metric</p>
                                            <p className="text-sm font-mono text-gray-800">{anomaly.metric_name}</p>
                                        </div>
                                        <div>
                                            <p className="text-xs text-gray-600 font-semibold mb-1">Date</p>
                                            <p className="text-sm font-mono text-gray-800">
                                                {anomaly.timestamp.split('T')[0] || anomaly.timestamp}
                                            </p>
                                        </div>
                                    </div>

                                    <div className="mb-3">
                                        <p className="text-xs text-gray-600 font-semibold mb-1">Value</p>
                                        <p className="text-2xl font-bold text-red-600">{anomaly.value}</p>
                                    </div>

                                    <div className="bg-red-100 rounded p-3">
                                        <p className="text-xs text-gray-600 font-semibold mb-1">Reason</p>
                                        <p className="text-sm text-gray-700">{anomaly.reason_short}</p>
                                    </div>
                                </div>
                            ))}
                        </div>
                    ) : (
                        <p className="text-sm text-gray-500 italic text-center py-6">No anomalies detected.</p>
                    )}
                </div>
            )}
        </div>
    );
};

export default AnomaliesCard;
