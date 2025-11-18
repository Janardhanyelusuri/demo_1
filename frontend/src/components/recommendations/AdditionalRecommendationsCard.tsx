// src/components/recommendations/AdditionalRecommendationsCard.tsx
import React from 'react';
import { CheckCircle2 } from 'lucide-react';
import { NormalizedRecommendation } from "@/types/recommendations";

interface AdditionalRecommendationsCardProps {
    recommendation: NormalizedRecommendation;
}

const AdditionalRecommendationsCard: React.FC<AdditionalRecommendationsCardProps> = ({ recommendation: rec }) => {
    const additionalRecs = rec.additionalRecommendations || [];

    return (
        <div className="p-6 border-l-4 border-amber-500 rounded-lg shadow-md bg-white transition-shadow hover:shadow-lg">
            <div className="flex items-center space-x-2 mb-4">
                <CheckCircle2 className="w-5 h-5 text-amber-600" />
                <h3 className="text-lg font-semibold text-gray-800">Additional Recommendations</h3>
            </div>

            {/* Additional Recommendations List */}
            {additionalRecs.length > 0 ? (
                <div className="space-y-3">
                    {additionalRecs.map((rec, index) => (
                        <div key={index} className="p-4 bg-amber-50 border border-amber-200 rounded-lg hover:bg-amber-100 transition-colors">
                            <div className="flex justify-between items-start gap-3">
                                <div className="flex-1">
                                    <p className="text-sm text-gray-700 leading-relaxed">
                                        {rec.text}
                                    </p>
                                </div>
                                <div className="flex-shrink-0">
                                    <span className="inline-block bg-amber-200 text-amber-900 font-semibold px-3 py-1 rounded-full text-sm whitespace-nowrap">
                                        {rec.saving_pct}%
                                    </span>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            ) : (
                <p className="text-sm text-gray-500 italic text-center py-4">No additional recommendations available.</p>
            )}
        </div>
    );
};

export default AdditionalRecommendationsCard;
