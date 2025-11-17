// src/app/(main)/(projects)/connections/[projectName]/[cloudPlatform]/dashboards/azuredashboard/recommendations/page.tsx

"use client";

import React, { useState } from "react";
import { useParams } from "next/navigation";
import { NormalizedRecommendation, RecommendationFilters, AZURE_RESOURCES } from "@/types/recommendations";
import { fetchRecommendationsWithFilters } from "@/lib/recommendations"; 

// NEW SHARED COMPONENT IMPORTS
import RecommendationFilterBar from "@/components/recommendations/RecommendationFilterBar";
import RecommendationCard from "@/components/recommendations/RecommendationCard"; 

// Note: You must ensure you have a utility file for 'cn' or remove/replace it if not using Shadcn structure.

// FIX: Renamed component for correct spelling
const AzureRecommendationsPage: React.FC = () => {
  const params = useParams();
  const projectId = params.projectName as string;
  const cloudPlatform = 'azure' as const; 
  
  const [recommendations, setRecommendations] = useState<NormalizedRecommendation[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  const resourceOptions = AZURE_RESOURCES;

  // Initialize filters with the first resource type
  const [filters, setFilters] = useState<RecommendationFilters>({
    resourceType: resourceOptions[0]?.displayName || '', 
    resourceId: '',
    startDate: undefined,
    endDate: undefined,
  });

  const handleFetch = async () => {
    // Validation ensures analysis only runs if a resource type is selected.
    if (!filters.resourceType) {
        setError("Please select a Resource Type to analyze.");
        setRecommendations([]);
        return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const normalizedData = await fetchRecommendationsWithFilters(
        projectId, 
        cloudPlatform, 
        filters
      );
      setRecommendations(normalizedData);
    } catch (err) {
      // Robust error handling
      if (err instanceof Error) {
        setError(err.message);
      } else {
        setError("An unknown error occurred while fetching recommendations.");
      }
    } finally {
      setIsLoading(false);
    }
  };

  // FIX: Removed the useEffect hook that caused the default load on mount.
  // The user must now click "Run Analysis" to fetch data.

  return (
    <div className="p-8">
      <h1 className="text-cp-title-2xl font-cp-semibold mb-6 text-cp-blue">
        Azure Cost Optimization Recommendations
      </h1>
      
      {/* FILTER BAR UI (Uses shared component) */}
      <RecommendationFilterBar
        filters={filters}
        setFilters={setFilters}
        resourceOptions={resourceOptions}
        isLoading={isLoading}
        onRunAnalysis={handleFetch}
      />

      {/* RESULTS DISPLAY */}
      {isLoading ? (
        <div className="p-8 text-center text-lg">Analyzing {filters.resourceType} data...</div>
      ) : error ? (
        <div className="p-8 text-center text-red-600 font-medium">Error: {error}</div>
      ) : recommendations.length === 0 ? (
        <div className="p-8 text-center bg-gray-50 border rounded-lg shadow-sm">
          <p className="text-cp-body text-gray-700">No optimization opportunities found for the selected filters.</p>
        </div>
      ) : (
        <div className="space-y-6">
          <p className="text-sm text-gray-600">
            Found **{recommendations.length}** resource recommendations for **{filters.resourceType}**.
          </p>
          
          {/* Card Mapping Logic (Uses shared component) */}
          {recommendations.map((rec) => (
             <RecommendationCard key={rec.resourceId} recommendation={rec} />
          ))}
        </div>
      )}
    </div>
  );
};

export default AzureRecommendationsPage;