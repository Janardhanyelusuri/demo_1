// src/components/recommendations/RecommendationFilterBar.tsx

import React from 'react';
import { RecommendationFilters, CloudResourceMap } from "@/types/recommendations";
import { format } from "date-fns";
import { cn } from "@/lib/utils"; // Assuming you have this utility function
import { CalendarIcon } from "lucide-react";

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
}

const RecommendationFilterBar: React.FC<RecommendationFilterBarProps> = ({
    filters,
    setFilters,
    resourceOptions,
    isLoading,
    onRunAnalysis
}) => {
    // Define the date boundaries to prevent future date selection
    const today = new Date();
    today.setHours(0, 0, 0, 0); 

    return (
        <div className="flex space-x-4 p-4 mb-8 bg-gray-50 border rounded-lg shadow-sm flex-wrap">
            
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
            
            {/* 2. Resource ID Input */}
            <Input
                type="text"
                placeholder="Resource ID (Optional)"
                value={filters.resourceId}
                onChange={(e) => setFilters(prev => ({ ...prev, resourceId: e.target.value }))}
                className="w-[250px]"
            />
            
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
                // FIX: Disabled unless a Resource Type is selected
                disabled={isLoading || !filters.resourceType} 
            >
                {isLoading ? 'Analyzing...' : 'Run Analysis'}
            </Button>
        </div>
    );
};

export default RecommendationFilterBar;