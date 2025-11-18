// src/components/recommendations/RecommendationFilterBar.tsx

import React, { useEffect, useState } from 'react';
import { RecommendationFilters, CloudResourceMap } from "@/types/recommendations";
import { fetchResourceIds } from "@/lib/recommendations";
import { format } from "date-fns";
import { cn } from "@/lib/utils"; // Assuming you have this utility function
import { CalendarIcon, Loader2 } from "lucide-react";

// --- UI Imports (Shadcn/Radix components) ---
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Calendar } from "@/components/ui/calendar";

interface RecommendationFilterBarProps {
    filters: RecommendationFilters;
    setFilters: React.Dispatch<React.SetStateAction<RecommendationFilters>>;
    resourceOptions: CloudResourceMap[];
    isLoading: boolean;
    onRunAnalysis: () => void;
    projectId: string;
    cloudPlatform: 'azure' | 'aws' | 'gcp';
}

const RecommendationFilterBar: React.FC<RecommendationFilterBarProps> = ({
    filters,
    setFilters,
    resourceOptions,
    isLoading,
    onRunAnalysis,
    projectId,
    cloudPlatform
}) => {
    // Define the date boundaries to prevent future date selection
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    // State for resource IDs dropdown
    const [resourceIds, setResourceIds] = useState<Array<{ resource_id: string; resource_name: string }>>([]);
    const [loadingResourceIds, setLoadingResourceIds] = useState(false);

    // Fetch resource IDs when resource type changes
    useEffect(() => {
        const fetchIds = async () => {
            if (!filters.resourceType) {
                setResourceIds([]);
                return;
            }

            setLoadingResourceIds(true);
            try {
                const ids = await fetchResourceIds(projectId, cloudPlatform, filters.resourceType);
                setResourceIds(ids);
            } catch (error) {
                console.error('Failed to fetch resource IDs:', error);
                setResourceIds([]);
            } finally {
                setLoadingResourceIds(false);
            }
        };

        fetchIds();
    }, [filters.resourceType, projectId, cloudPlatform]);

    return (
        <div className="flex items-center gap-4 p-4 mb-8 bg-gray-50 border rounded-lg shadow-sm flex-wrap">

            {/* 1. Resource Type Dropdown */}
            <Select
                value={filters.resourceType}
                onValueChange={(value) => setFilters(prev => ({ ...prev, resourceType: value, resourceId: '' }))}
            >
                <SelectTrigger className="w-[180px]">
                    <SelectValue placeholder="Resource Type" />
                </SelectTrigger>
                <SelectContent>
                    {resourceOptions.map((r) => (
                        <SelectItem key={r.backendKey} value={r.displayName}>
                            {r.displayName}
                        </SelectItem>
                    ))}
                </SelectContent>
            </Select>

            {/* 2. Resource ID Dropdown */}
            <Select
                value={filters.resourceId || ""}
                onValueChange={(value) => setFilters(prev => ({ ...prev, resourceId: value === "all" ? "" : value }))}
                disabled={!filters.resourceType || loadingResourceIds}
            >
                <SelectTrigger className="w-[300px]">
                    {loadingResourceIds ? (
                        <div className="flex items-center gap-2">
                            <Loader2 className="h-4 w-4 animate-spin" />
                            <span>Loading resources...</span>
                        </div>
                    ) : (
                        <SelectValue placeholder="Select Resource ID (Optional)" />
                    )}
                </SelectTrigger>
                <SelectContent>
                    <SelectItem value="all">All Resources</SelectItem>
                    {resourceIds.length > 0 ? (
                        resourceIds.map((resource) => (
                            <SelectItem
                                key={resource.resource_id}
                                value={resource.resource_id}
                                title={resource.resource_id}
                            >
                                <div className="flex flex-col">
                                    <span className="font-medium">{resource.resource_name}</span>
                                    <span className="text-xs text-gray-500 truncate max-w-[250px]">
                                        {resource.resource_id}
                                    </span>
                                </div>
                            </SelectItem>
                        ))
                    ) : (
                        <SelectItem value="no-resources" disabled>
                            No resources found
                        </SelectItem>
                    )}
                </SelectContent>
            </Select>

            {/* 3. Start Date Picker (Popover + Calendar) */}
            <Popover>
                <PopoverTrigger asChild>
                    <Button
                        variant={"outline"}
                        className={cn(
                            "w-[200px] justify-start text-left font-normal",
                            !filters.startDate && "text-muted-foreground"
                        )}
                    >
                        <CalendarIcon className="mr-2 h-4 w-4" />
                        {filters.startDate ? format(filters.startDate, "PPP") : <span>Start Date</span>}
                    </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                    <Calendar
                        mode="single"
                        selected={filters.startDate}
                        onSelect={(date) => setFilters(prev => ({ ...prev, startDate: date }))}
                        initialFocus
                        // FIX: Disable future dates
                        disabled={(date) => date > today}
                    />
                </PopoverContent>
            </Popover>

            {/* 4. End Date Picker (Popover + Calendar) */}
            <Popover>
                <PopoverTrigger asChild>
                    <Button
                        variant={"outline"}
                        className={cn(
                            "w-[200px] justify-start text-left font-normal",
                            !filters.endDate && "text-muted-foreground"
                        )}
                    >
                        <CalendarIcon className="mr-2 h-4 w-4" />
                        {filters.endDate ? format(filters.endDate, "PPP") : <span>End Date</span>}
                    </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                    <Calendar
                        mode="single"
                        selected={filters.endDate}
                        onSelect={(date) => setFilters(prev => ({ ...prev, endDate: date }))}
                        initialFocus
                        // FIX: Disable future dates AND dates before start date
                        disabled={(date) =>
                           date > today ||
                           date < (filters.startDate || new Date(0))
                        }
                    />
                </PopoverContent>
            </Popover>

            {/* 5. Run Analysis Button */}
            <Button
                onClick={onRunAnalysis}
                disabled={isLoading || !filters.resourceType}
                className="ml-auto"
            >
                {isLoading ? 'Analyzing...' : 'Run Analysis'}
            </Button>
        </div>
    );
};

export default RecommendationFilterBar;