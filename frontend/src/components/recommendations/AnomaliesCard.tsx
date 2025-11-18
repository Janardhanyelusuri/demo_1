// src/components/recommendations/AnomaliesCard.tsx
import React from 'react';
import { AlertTriangle } from 'lucide-react';
import { NormalizedRecommendation } from "@/types/recommendations";

interface AnomaliesCardProps {
    recommendation: NormalizedRecommendation;
}

const AnomaliesCard: React.FC<AnomaliesCardProps> = ({ recommendation: rec }) => {
    const anomalies = rec.anomalies || [];

    return (
        <div className="p-6 border-l-4 border-red-500 rounded-lg shadow-md bg-white transition-shadow hover:shadow-lg">
            <div className="flex items-center space-x-2 mb-4">
                <AlertTriangle className="w-5 h-5 text-red-600" />
                <h3 className="text-lg font-semibold text-gray-800">Anomalies Detected</h3>
            </div>

            {/* Anomalies List */}
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
                <p className="text-sm text-gray-500 italic text-center py-4">No anomalies detected.</p>
            )}
        </div>
    );
};

export default AnomaliesCard;
