// src/components/recommendations/RecommendationCard.tsx
import React from 'react';
import { NormalizedRecommendation } from "@/types/recommendations";
import { AlertCircle, Clock, CheckCircle } from 'lucide-react';

// Import the specialized card components
import MetricsCard from './MetricsCard';
import EffectiveRecommendationCard from './EffectiveRecommendationCard';
import AdditionalRecommendationsCard from './AdditionalRecommendationsCard';
import AnomaliesCard from './AnomaliesCard';
import ContractDealCard from './ContractDealCard';

interface RecommendationCardProps {
    recommendation: NormalizedRecommendation;
}

const getSeverityStyles = (severity: 'High' | 'Medium' | 'Low') => {
    switch (severity) {
        case 'High':
            return {
                icon: <AlertCircle className="w-6 h-6 text-red-500" />,
                color: 'text-red-700',
                label: 'High Impact'
            };
        case 'Medium':
            return {
                icon: <Clock className="w-6 h-6 text-yellow-500" />,
                color: 'text-yellow-700',
                label: 'Medium Impact'
            };
        case 'Low':
            return {
                icon: <CheckCircle className="w-6 h-6 text-green-500" />,
                color: 'text-green-700',
                label: 'Low Impact'
            };
        default:
            return {
                icon: <AlertCircle className="w-6 h-6 text-gray-500" />,
                color: 'text-gray-700',
                label: 'Unknown'
            };
    }
};

const RecommendationCard: React.FC<RecommendationCardProps> = ({ recommendation: rec }) => {
    const { icon, color, label } = getSeverityStyles(rec.severity);
    const monthlyForcastDisplay = rec.costForecasting.monthly.toLocaleString('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });

    return (
        <div className="space-y-6">
            {/* Header Section */}
            <div className="bg-gradient-to-r from-blue-50 to-indigo-50 p-6 rounded-lg border border-blue-200">
                <div className="flex justify-between items-start mb-4">
                    <div className="flex items-start space-x-4">
                        <div>{icon}</div>
                        <div>
                            <h2 className="text-2xl font-bold text-gray-800">Resource Analysis</h2>
                            <p className="text-sm text-gray-600 mt-1">
                                Resource ID: <span className="font-mono text-xs">{rec.resourceId}</span>
                            </p>
                        </div>
                    </div>
                    <div className="text-right">
                        <p className={`text-sm font-semibold ${color}`}>{label}</p>
                        <p className="text-3xl font-bold text-green-600 mt-2">{rec.totalSavingPercent}%</p>
                        <p className="text-xs text-gray-600">Total Savings</p>
                    </div>
                </div>

                {/* Monthly Forecast Preview */}
                <div className="border-t border-blue-200 pt-4 mt-4">
                    <p className="text-xs text-gray-600 mb-2">Monthly Forecast Preview</p>
                    <p className="text-xl font-bold text-blue-600">{monthlyForcastDisplay}</p>
                </div>
            </div>

            {/* Main Content Grid - 5 Cards */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Row 1: Metrics Card spans 2 columns */}
                <div className="lg:col-span-2">
                    <MetricsCard recommendation={rec} />
                </div>

                {/* Row 2: Effective & Additional Recommendations */}
                <div>
                    <EffectiveRecommendationCard recommendation={rec} />
                </div>
                <div>
                    <AdditionalRecommendationsCard recommendation={rec} />
                </div>

                {/* Row 3: Anomalies & Contract Deal */}
                <div>
                    <AnomaliesCard recommendation={rec} />
                </div>
                <div>
                    <ContractDealCard recommendation={rec} />
                </div>
            </div>
        </div>
    );
};

export default RecommendationCard;