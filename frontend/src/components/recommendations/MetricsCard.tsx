// src/components/recommendations/MetricsCard.tsx
import React from 'react';
import { TrendingUp } from 'lucide-react';
import { NormalizedRecommendation } from "@/types/recommendations";

interface MetricsCardProps {
    recommendation: NormalizedRecommendation;
}

const MetricsCard: React.FC<MetricsCardProps> = ({ recommendation: rec }) => {
    const baseMetrics = rec.baseOfRecommendations || [];
    const monthly = rec.costForecasting.monthly;
    const annually = rec.costForecasting.annually;

    return (
        <div className="p-6 border-l-4 border-blue-500 rounded-lg shadow-md bg-white transition-shadow hover:shadow-lg">
            <div className="flex items-center space-x-2 mb-4">
                <TrendingUp className="w-5 h-5 text-blue-600" />
                <h3 className="text-lg font-semibold text-gray-800">Metrics & Forecasting</h3>
            </div>

            {/* Metrics Basis Section */}
            <div className="mb-6">
                <h4 className="text-sm font-semibold text-gray-700 mb-3">Base of Recommendations:</h4>
                <div className="space-y-2">
                    {baseMetrics.length > 0 ? (
                        baseMetrics.map((metric, index) => (
                            <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded">
                                <span className="text-sm text-gray-700">{metric}</span>
                                <span className="text-xs font-mono bg-blue-100 text-blue-800 px-2 py-1 rounded">
                                    {/* Extract unit if present (e.g., "(GiB)" from "UsedCapacity (GiB)_Avg") */}
                                    {metric.match(/\(.*?\)/)?.[0] || 'N/A'}
                                </span>
                            </div>
                        ))
                    ) : (
                        <p className="text-sm text-gray-500 italic">No metrics information available.</p>
                    )}
                </div>
            </div>

            {/* Cost Forecasting Section */}
            <div className="border-t pt-4">
                <h4 className="text-sm font-semibold text-gray-700 mb-3">Cost Forecast:</h4>
                <div className="grid grid-cols-2 gap-4">
                    <div className="p-3 bg-green-50 rounded">
                        <p className="text-xs text-gray-600 mb-1">Monthly Forecast</p>
                        <p className="text-2xl font-bold text-green-600">
                            ${monthly.toFixed(2)}
                        </p>
                    </div>
                    <div className="p-3 bg-green-50 rounded">
                        <p className="text-xs text-gray-600 mb-1">Annual Forecast</p>
                        <p className="text-2xl font-bold text-green-600">
                            ${annually.toFixed(2)}
                        </p>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default MetricsCard;
