// src/components/recommendations/KPICard.tsx
import React from 'react';
import { NormalizedRecommendation } from "@/types/recommendations";
import { DollarSign, TrendingDown, AlertCircle, CheckCircle } from 'lucide-react';

interface KPICardProps {
    recommendation: NormalizedRecommendation;
}

const KPICard: React.FC<KPICardProps> = ({ recommendation: rec }) => {
    const anomalyCount = rec.anomalies?.length || 0;
    const additionalRecsCount = rec.additionalRecommendations?.length || 0;
    const hasContractDeal = rec.contractDeal ? true : false;

    return (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* KPI: Total Monthly Forecast */}
            <div className="bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md transition-shadow">
                <div className="flex items-center justify-between">
                    <div>
                        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Monthly Forecast</p>
                        <p className="text-2xl font-bold text-blue-600 mt-2">
                            ${rec.costForecasting.monthly.toFixed(2)}
                        </p>
                    </div>
                    <div className="bg-blue-100 p-3 rounded-lg">
                        <DollarSign className="w-6 h-6 text-blue-600" />
                    </div>
                </div>
            </div>

            {/* KPI: Annual Forecast */}
            <div className="bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md transition-shadow">
                <div className="flex items-center justify-between">
                    <div>
                        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Annual Forecast</p>
                        <p className="text-2xl font-bold text-indigo-600 mt-2">
                            ${rec.costForecasting.annually.toFixed(2)}
                        </p>
                    </div>
                    <div className="bg-indigo-100 p-3 rounded-lg">
                        <TrendingDown className="w-6 h-6 text-indigo-600" />
                    </div>
                </div>
            </div>

            {/* KPI: Total Saving Percentage */}
            <div className="bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md transition-shadow">
                <div className="flex items-center justify-between">
                    <div>
                        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Total Savings</p>
                        <p className="text-2xl font-bold text-green-600 mt-2">
                            {rec.totalSavingPercent}%
                        </p>
                    </div>
                    <div className="bg-green-100 p-3 rounded-lg">
                        <CheckCircle className="w-6 h-6 text-green-600" />
                    </div>
                </div>
            </div>

            {/* KPI: Anomalies Count */}
            <div className="bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md transition-shadow">
                <div className="flex items-center justify-between">
                    <div>
                        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Anomalies</p>
                        <p className="text-2xl font-bold text-red-600 mt-2">
                            {anomalyCount}
                        </p>
                    </div>
                    <div className="bg-red-100 p-3 rounded-lg">
                        <AlertCircle className="w-6 h-6 text-red-600" />
                    </div>
                </div>
            </div>
        </div>
    );
};

export default KPICard;
