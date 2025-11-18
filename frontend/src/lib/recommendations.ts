// src/lib/recommendations.ts

import axiosInstance, { BACKEND } from "@/lib/api"; 
import { 
    RawRecommendation, 
    NormalizedRecommendation, 
    RecommendationFilters, 
    AZURE_RESOURCES, 
    AWS_RESOURCES, 
    GCP_RESOURCES,
    CloudResourceMap // REQUIRED for explicit typing
} from "@/types/recommendations";
import { format } from "date-fns"; 

// Helper function to normalize the data (from previous steps)
const normalizeRecommendations = (data: RawRecommendation[]): NormalizedRecommendation[] => {
  return data.map((item): NormalizedRecommendation => {
    // Defensive checks for nested properties
    const recommendations = item.recommendations || {
      effective_recommendation: { text: 'No recommendation available', saving_pct: 0 } as RecommendationDetail,
      additional_recommendation: [] as RecommendationDetail[]
    };

    const effectiveRec: RecommendationDetail = recommendations.effective_recommendation || {
      text: 'No recommendation available',
      saving_pct: 0
    };
    const additionalRecs: RecommendationDetail[] = recommendations.additional_recommendation || [];

    let totalSavingPct: number = effectiveRec.saving_pct || 0;
    const allDetails: RecommendationDetail[] = [effectiveRec];

    additionalRecs.forEach((detail: RecommendationDetail) => {
        if (detail && typeof detail.saving_pct === 'number') {
          totalSavingPct += detail.saving_pct;
          allDetails.push(detail);
        }
    });

    const getSeverity = (saving: number): 'High' | 'Medium' | 'Low' => {
      if (saving >= 20) return 'High';
      if (saving >= 5) return 'Medium';
      return 'Low';
    };

    // Defensive access to cost_forecasting
    const costForecasting = item.cost_forecasting || { monthly: 0, annually: 0 };
    const anomalies = item.anomalies || [];

    return {
      resourceId: item.resource_id || 'Unknown',
      title: effectiveRec.text || 'No recommendation available',
      totalSavingPercent: parseFloat(totalSavingPct.toFixed(2)),
      monthlyForecast: costForecasting.monthly || 0,
      anomalyTimestamp: anomalies[0]?.timestamp || "N/A",
      severity: getSeverity(totalSavingPct),
      details: allDetails,
    };
  });
};

/**
 * Helper to map frontend display name to the backend resource key.
 */
const getBackendKey = (cloud: string, displayName: string): string | undefined => {
    // ⭐ FIX APPLIED: Explicitly type the resources array to resolve the visual error
    let resources: CloudResourceMap[] = []; 
    
    if (cloud === 'azure') resources = AZURE_RESOURCES;
    else if (cloud === 'aws') resources = AWS_RESOURCES;
    else if (cloud === 'gcp') resources = GCP_RESOURCES;

    const map = resources.find(r => r.displayName === displayName);
    return map?.backendKey;
};


/**
 * Public function: Fetches recommendations based on user-selected filters.
 */
export const fetchRecommendationsWithFilters = async (
    projectId: string, 
    cloudPlatform: 'azure' | 'aws' | 'gcp',
    filters: RecommendationFilters
): Promise<NormalizedRecommendation[]> => {
    
    // 1. Get the internal backend key from the selected display name
    const backendKey = getBackendKey(cloudPlatform, filters.resourceType);

    if (!backendKey) {
        throw new Error("Invalid resource type selected.");
    }

    const url = `${BACKEND}/llm/${cloudPlatform}/${projectId}`; 
    
    // 2. Prepare the payload, formatting dates to ISO string if they exist
    const body = {
        resource_type: backendKey,
        resource_id: filters.resourceId || undefined,
        // Format dates as ISO strings (FastAPI standard)
        start_date: filters.startDate ? format(filters.startDate, "yyyy-MM-dd'T'HH:mm:ss") : undefined,
        end_date: filters.endDate ? format(filters.endDate, "yyyy-MM-dd'T'HH:mm:ss") : undefined,
    };

    try {
        const response = await axiosInstance.post(url, body, {
            headers: { "Content-Type": "application/json" }
        });

        // 3. Parse the JSON string from the 'recommendations' field
        const rawJsonString = response.data.recommendations;

        console.log('📦 Raw API Response:', response.data);
        console.log('📦 Raw JSON String:', rawJsonString);

        if (rawJsonString) {
             const rawData = JSON.parse(rawJsonString) as RawRecommendation[];
             console.log('✅ Parsed Raw Data:', rawData);

             // 4. Normalize and return
             const normalized = normalizeRecommendations(rawData);
             console.log('✅ Normalized Data:', normalized);

             return normalized;
        }

        console.warn('⚠️ No recommendations in response');
        return [];

    } catch (err) {
        console.error(`❌ API Error fetching ${cloudPlatform} ${backendKey}:`, err);
        // Throw a user-friendly error after logging the technical details
        throw new Error(`Failed to load ${filters.resourceType} analysis. Please check the backend service.`);
    }
};