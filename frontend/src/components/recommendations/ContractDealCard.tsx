// src/components/recommendations/ContractDealCard.tsx
import React from 'react';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import { NormalizedRecommendation } from "@/types/recommendations";

interface ContractDealCardProps {
    recommendation: NormalizedRecommendation;
}

const ContractDealCard: React.FC<ContractDealCardProps> = ({ recommendation: rec }) => {
    const deal = rec.contractDeal;

    // Determine assessment styling
    const getAssessmentStyles = (assessment: string) => {
        switch (assessment.toLowerCase()) {
            case 'good':
                return {
                    bg: 'bg-green-50',
                    border: 'border-green-200',
                    badge: 'bg-green-100 text-green-800',
                    text: 'text-green-700',
                    icon: <TrendingUp className="w-5 h-5 text-green-600" />
                };
            case 'bad':
                return {
                    bg: 'bg-red-50',
                    border: 'border-red-200',
                    badge: 'bg-red-100 text-red-800',
                    text: 'text-red-700',
                    icon: <TrendingDown className="w-5 h-5 text-red-600" />
                };
            default:
                return {
                    bg: 'bg-gray-50',
                    border: 'border-gray-200',
                    badge: 'bg-gray-100 text-gray-800',
                    text: 'text-gray-700',
                    icon: <Minus className="w-5 h-5 text-gray-600" />
                };
        }
    };

    const styles = getAssessmentStyles(deal.assessment);

    return (
        <div className="p-6 border-l-4 border-cyan-500 rounded-lg shadow-md bg-white transition-shadow hover:shadow-lg">
            <div className="flex items-center space-x-2 mb-4">
                <div>{styles.icon}</div>
                <h3 className="text-lg font-semibold text-gray-800">Contract Deal Assessment</h3>
            </div>

            {/* Assessment Header */}
            <div className={`p-4 rounded-lg ${styles.bg} border ${styles.border} mb-4`}>
                <div className="flex justify-between items-start">
                    <div>
                        <p className="text-xs text-gray-600 font-semibold mb-1">Assessment</p>
                        <p className={`text-lg font-bold capitalize ${styles.text}`}>
                            {deal.assessment}
                        </p>
                    </div>
                    <span className={`${styles.badge} px-3 py-1 rounded-full text-sm font-semibold`}>
                        {deal['for sku']}
                    </span>
                </div>
            </div>

            {/* Reason */}
            <div className="mb-4 p-4 bg-gray-50 rounded-lg">
                <p className="text-xs text-gray-600 font-semibold mb-2">Reason</p>
                <p className="text-sm text-gray-700 leading-relaxed">{deal.reason}</p>
            </div>

            {/* Savings/Loss Metrics */}
            <div className="border-t pt-4">
                <p className="text-xs text-gray-600 font-semibold mb-3">Potential Monthly & Annual Impact</p>
                <div className="grid grid-cols-2 gap-3">
                    <div className={`p-3 rounded-lg ${deal.monthly_saving_pct > 0 ? 'bg-green-50' : 'bg-red-50'}`}>
                        <p className="text-xs text-gray-600 mb-1">Monthly</p>
                        <p className={`text-2xl font-bold ${deal.monthly_saving_pct > 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {deal.monthly_saving_pct > 0 ? '+' : ''}{deal.monthly_saving_pct}%
                        </p>
                    </div>
                    <div className={`p-3 rounded-lg ${deal.annual_saving_pct > 0 ? 'bg-green-50' : 'bg-red-50'}`}>
                        <p className="text-xs text-gray-600 mb-1">Annual</p>
                        <p className={`text-2xl font-bold ${deal.annual_saving_pct > 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {deal.annual_saving_pct > 0 ? '+' : ''}{deal.annual_saving_pct}%
                        </p>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default ContractDealCard;
