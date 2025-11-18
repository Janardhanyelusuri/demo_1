// src/components/recommendations/ContractDealCard.tsx
import React, { useState } from 'react';
import { TrendingUp, TrendingDown, Minus, ChevronDown, ChevronUp } from 'lucide-react';
import { NormalizedRecommendation } from "@/types/recommendations";

interface ContractDealCardProps {
    recommendation: NormalizedRecommendation;
    isExpanded?: boolean;
}

const ContractDealCard: React.FC<ContractDealCardProps> = ({ recommendation: rec, isExpanded = false }) => {
    const [expanded, setExpanded] = useState(isExpanded);
    const deal = rec.contractDeal;

    // Determine assessment styling
    const getAssessmentStyles = (assessment: string) => {
        switch (assessment.toLowerCase()) {
            case 'good':
                return {
                    bgLight: 'bg-green-50',
                    border: 'border-green-200',
                    badge: 'bg-green-100 text-green-800',
                    text: 'text-green-700',
                    headerBg: 'bg-green-100',
                    icon: <TrendingUp className="w-5 h-5 text-green-600" />
                };
            case 'bad':
                return {
                    bgLight: 'bg-red-50',
                    border: 'border-red-200',
                    badge: 'bg-red-100 text-red-800',
                    text: 'text-red-700',
                    headerBg: 'bg-red-100',
                    icon: <TrendingDown className="w-5 h-5 text-red-600" />
                };
            default:
                return {
                    bgLight: 'bg-gray-50',
                    border: 'border-gray-200',
                    badge: 'bg-gray-100 text-gray-800',
                    text: 'text-gray-700',
                    headerBg: 'bg-gray-100',
                    icon: <Minus className="w-5 h-5 text-gray-600" />
                };
        }
    };

    const styles = getAssessmentStyles(deal.assessment);

    return (
        <div className="bg-white rounded-lg border border-cyan-200 shadow-sm hover:shadow-md transition-shadow overflow-hidden">
            {/* Header Button */}
            <button
                onClick={() => setExpanded(!expanded)}
                className="w-full p-4 flex items-center justify-between hover:bg-cyan-50 transition-colors"
            >
                <div className="flex items-center space-x-3">
                    <div className={`${styles.headerBg} p-2 rounded-lg`}>
                        {styles.icon}
                    </div>
                    <div className="text-left">
                        <h3 className="text-sm font-semibold text-gray-800">Contract Deal Assessment</h3>
                        <p className={`text-xs font-semibold mt-0.5 ${styles.text}`}>
                            {deal.assessment.charAt(0).toUpperCase() + deal.assessment.slice(1)} · {deal['for sku']}
                        </p>
                    </div>
                </div>
                <div className="flex items-center space-x-2">
                    <span className={`text-xs font-semibold ${styles.badge} px-2 py-1 rounded`}>
                        {deal.annual_saving_pct > 0 ? '+' : ''}{deal.annual_saving_pct}% annual
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
                <div className="border-t border-cyan-200 p-4 space-y-4">
                    {/* Assessment Details */}
                    <div className={`p-4 rounded-lg ${styles.bgLight} border ${styles.border}`}>
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
                    <div className="p-4 bg-gray-50 rounded-lg">
                        <p className="text-xs text-gray-600 font-semibold mb-2">Reason</p>
                        <p className="text-sm text-gray-700 leading-relaxed">{deal.reason}</p>
                    </div>

                    {/* Savings/Loss Metrics */}
                    <div>
                        <p className="text-xs text-gray-600 font-semibold mb-3">Monthly & Annual Impact</p>
                        <div className="grid grid-cols-2 gap-3">
                            <div className={`p-4 rounded-lg border ${deal.monthly_saving_pct > 0 ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}`}>
                                <p className="text-xs text-gray-600 font-semibold mb-2">Monthly</p>
                                <p className={`text-2xl font-bold ${deal.monthly_saving_pct > 0 ? 'text-green-600' : 'text-red-600'}`}>
                                    {deal.monthly_saving_pct > 0 ? '+' : ''}{deal.monthly_saving_pct}%
                                </p>
                            </div>
                            <div className={`p-4 rounded-lg border ${deal.annual_saving_pct > 0 ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}`}>
                                <p className="text-xs text-gray-600 font-semibold mb-2">Annual</p>
                                <p className={`text-2xl font-bold ${deal.annual_saving_pct > 0 ? 'text-green-600' : 'text-red-600'}`}>
                                    {deal.annual_saving_pct > 0 ? '+' : ''}{deal.annual_saving_pct}%
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default ContractDealCard;
