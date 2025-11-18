// src/components/recommendations/RecommendationCard.tsx
import React from 'react';
import { NormalizedRecommendation } from "@/types/recommendations";
import { AlertCircle, Clock, CheckCircle, Info } from 'lucide-react';

// Import the specialized card components
import KPICard from './KPICard';
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
                bg: 'from-red-50 to-orange-50',
                border: 'border-red-300',
                badge: 'bg-red-100 text-red-800',
                color: 'text-red-700'
            };
        case 'Medium':
            return {
                icon: <Clock className="w-6 h-6 text-yellow-500" />,
                bg: 'from-yellow-50 to-amber-50',
                border: 'border-yellow-300',
                badge: 'bg-yellow-100 text-yellow-800',
                color: 'text-yellow-700'
            };
        case 'Low':
            return {
                icon: <CheckCircle className="w-6 h-6 text-green-500" />,
                bg: 'from-green-50 to-emerald-50',
                border: 'border-green-300',
                badge: 'bg-green-100 text-green-800',
                color: 'text-green-700'
            };
        default:
            return {
                icon: <Info className="w-6 h-6 text-gray-500" />,
                bg: 'from-gray-50 to-slate-50',
                border: 'border-gray-300',
                badge: 'bg-gray-100 text-gray-800',
                color: 'text-gray-700'
            };
    }
};

const RecommendationCard: React.FC<RecommendationCardProps> = ({ recommendation: rec }) => {
    const styles = getSeverityStyles(rec.severity);
    const resourceIdShort = rec.resourceId.split('/').pop() || rec.resourceId;

    return (
        <div className="space-y-6">
            {/* Header Section */}
            <div className={`bg-gradient-to-r ${styles.bg} p-6 rounded-xl border-2 ${styles.border} shadow-sm`}>
                <div className="flex justify-between items-start gap-4">
                    <div className="flex items-start gap-4 flex-1">
                        <div className="flex-shrink-0">{styles.icon}</div>
                        <div className="min-w-0">
                            <h1 className="text-3xl font-bold text-gray-900">Resource Analysis</h1>
                            <p className="text-sm text-gray-600 mt-2">
                                <span className="font-semibold">Resource:</span> {resourceIdShort}
                            </p>
                        </div>
                    </div>
                    <div className="flex-shrink-0 text-right">
                        <span className={`inline-block ${styles.badge} px-4 py-2 rounded-full text-sm font-bold mb-3`}>
                            {rec.severity} Severity
                        </span>
                        <div className="mt-3">
                            <p className="text-sm text-gray-600">Total Savings</p>
                            <p className="text-4xl font-bold text-green-600 mt-1">{rec.totalSavingPercent}%</p>
                        </div>
                    </div>
                </div>
            </div>

            {/* KPI Cards Section */}
            <div>
                <h2 className="text-lg font-bold text-gray-900 mb-4">Key Performance Indicators</h2>
                <KPICard recommendation={rec} />
            </div>

            {/* Main Content Grid */}
            <div className="space-y-6">
                {/* Metrics Card - Full Width */}
                <MetricsCard recommendation={rec} />

                {/* Recommendations Row */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <EffectiveRecommendationCard recommendation={rec} />
                    <AdditionalRecommendationsCard recommendation={rec} />
                </div>

                {/* Collapsible Sections Row */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <AnomaliesCard recommendation={rec} />
                    <ContractDealCard recommendation={rec} />
                </div>
            </div>
        </div>
    );
};

export default RecommendationCard;