# app/ingestion/azure/llm_analysis.py

import json
from typing import Optional, List, Dict, Any
import sys
import os

# Assuming app.core.genai and app.ingestion.azure.llm_json_extractor are available
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from app.core.genai import llm_call
from app.ingestion.azure.llm_json_extractor import extract_json_str # Only need extract_json_str

# --- Utility Functions (Only keeping _extrapolate_costs here) ---

def _extrapolate_costs(billed_cost: float, duration_days: int) -> Dict[str, float]:
    """Helper to calculate monthly/annual forecasts."""
    if duration_days == 0:
        return {"monthly": 0.0, "annually": 0.0}
        
    avg_daily_cost = billed_cost / duration_days
    monthly = avg_daily_cost * 30.4375 
    annually = avg_daily_cost * 365 
    return {
        "monthly": round(monthly, 2),
        "annually": round(annually, 2)
    }


# --- PROMPT GENERATION FUNCTIONS (Updated to include Max and MaxDate) ---

def _generate_storage_prompt(resource_data: dict, start_date: str, end_date: str, monthly_forecast: float, annual_forecast: float) -> str:
    """Generates the structured prompt for Storage LLM analysis with AVG, MAX, and MAX Date."""
    resource_id = resource_data.get("resource_id", "N/A")
    sku = resource_data.get("sku", "N/A")
    access_tier = resource_data.get("access_tier", "N/A")
    billed_cost = resource_data.get("billed_cost", 0.0)
    duration_days = resource_data.get("duration_days", 30)
    
    # Updated metrics display to show all three derived values
    metrics_display = "\n".join([
        f"- {k.replace('metric_', '')}: {v}" 
        for k, v in resource_data.items() 
        if k.startswith("metric_") and v is not None and ('_Avg' in k or '_Max' in k or '_MaxDate' in k)
    ])
    
    # Get Max values for anomaly context
    max_capacity = resource_data.get('metric_UsedCapacity (GiB)_Max', 0)
    max_capacity_date = resource_data.get('metric_UsedCapacity (GiB)_MaxDate', end_date)

    return """
  You are an Azure FinOps & Cost Optimization Expert.  
  Analyze the following Storage Account data and produce ONLY a valid JSON object according to the schema provided below.  
  Do not output any natural language commentary outside the JSON.  
  Do not include markdown or code fencing.  
  Use the values exactly as provided.

  ANALYSIS CONTEXT:
  - Resource ID: {resource_id}
  - SKU/Tier: {sku} ({access_tier})
  - Analysis Period: {start_date} to {end_date} ({duration_days} days)
  - Total Billed Cost for Period: ${billed_cost:.2f}

  UTILIZATION METRICS (AVG / MAX / MAX DATE):
  {metrics_display}

  INSTRUCTIONS FOR ANALYSIS:
  1. Use {resource_data.get('metric_UsedCapacity (GiB)_Max', 0)} and its date {resource_data.get('metric_UsedCapacity (GiB)_MaxDate', end_date)} to determine saturation or anomaly.
  2. If access tier is Hot, evaluate whether a portion of UsedCapacity (GiB)_Avg can safely be moved to Cool/Archive.  
    Estimate saving_pct based on typical ratios (Cool ≈ 30% cost of Hot, Archive ≈ 5%).
  3. If Transactions (count)_Avg is high but capacity is small, prioritize transaction optimization instead of tiering.
  4. Perfectly follow the cost_forecasting values passed into the template:  
    monthly = {monthly_forecast:.2f}, annually = {annual_forecast:.2f}.
  5. Contract evaluation: Compare contracted_unit_price ({resource_data.get('contracted_unit_price', 'N/A')}) vs general SKU {sku}.  
    Return assessment as: "good", "bad", or "unknown".
  6. You MUST compute a realistic saving_pct (0–100 range).  
    Use the most impactful optimization as the effective_recommendation.
  7. Output MUST strictly follow the schema below. No extra fields. No missing fields.

  STRICT JSON OUTPUT SCHEMA (do not modify keys, types, or structure):

  {
    "recommendations": {
      "effective_recommendation": { "text": "...", "saving_pct": 12.3 },
      "additional_recommendation": [
        {"text": "...", "saving_pct": 3.4},
        {"text": "...", "saving_pct": 5.0}
      ],
      "base_of_recommendations": ["UsedCapacity (GiB)_Avg", "Transactions (count)_Max"]
    },
    "cost_forecasting": {
      "monthly": {monthly_forecast:.2f},
      "annually": {annual_forecast:.2f}
    },
    "anomalies": [
      {
        "metric_name": "UsedCapacity (GiB)",
        "timestamp": "{max_capacity_date}",
        "value": {max_capacity},
        "reason_short": "Max capacity reached on this date"
      }
    ],
    "contract_deal": {
      "assessment": "good" | "bad" | "unknown",
      "for sku": "{sku}",
      "reason": "...",
      "monthly_saving_pct": 1.2,
      "annual_saving_pct": 14.4
    }
  }
    """

def _generate_compute_prompt(resource_data: dict, start_date: str, end_date: str, monthly_forecast: float, annual_forecast: float) -> str:
    """Generates the structured prompt for Compute/VM LLM analysis with AVG, MAX, and MAX Date."""
    resource_id = resource_data.get("resource_id", "N/A")
    resource_name = resource_data.get("resource_name", "N/A")
    billed_cost = resource_data.get("billed_cost", 0.0)
    duration_days = resource_data.get("duration_days", 30)
    
    cpu_avg = resource_data.get("metric_Percentage CPU_Avg", 0.0)
    cpu_max = resource_data.get("metric_Percentage CPU_Max", 0.0)
    cpu_max_date = resource_data.get("metric_Percentage CPU_MaxDate", end_date)
    sku = resource_data.get("sku", "N/A")
    
    # Updated metrics display to show all three derived values
    metrics_display = "\n".join([
        f"- {k.replace('metric_', '')}: {v}" 
        for k, v in resource_data.items() 
        if k.startswith("metric_") and v is not None and ('_Avg' in k or '_Max' in k or '_MaxDate' in k)
    ])

    return """
   You are an Azure FinOps & VM Optimization Expert.  
      Analyze the following Virtual Machine metrics and produce ONLY a valid JSON object based strictly on the schema shown below.  
      Never output text outside JSON.  
      Never use markdown.

      ANALYSIS CONTEXT:
      - Resource ID: {resource_id}
      - VM Name: {resource_name}
      - Analysis Period: {start_date} to {end_date} ({duration_days} days)
      - Total Billed Cost: ${billed_cost:.2f}

      UTILIZATION METRICS (AVG / MAX / MAX DATE):
      {metrics_display}

      INSTRUCTIONS:
      1. Rightsizing Logic:  
        Recommend downsizing when:
        - Percentage CPU_Avg < 20  
        - Percentage CPU_Max < 75  
        Use these exact inputs:  
        CPU_Avg = {resource_data.get('metric_Percentage CPU_Avg', 0.0)},  
        CPU_Max = {resource_data.get('metric_Percentage CPU_Max', 0.0)}  
        CPU_MaxDate = {cpu_max_date}
      2. If CPU_Max > 90, include a high-risk note and do NOT recommend downsizing.
      3. Cost forecasting must use the precomputed values:  
        monthly = {monthly_forecast:.2f}, annually = {annual_forecast:.2f}.
      4. Calculate saving_pct realistically based on SKU reduction (e.g., 30–50% vCPU reduction).  
        Assume Estimated Monthly Savings = (current_hourly_cost * % reduction * 24 * 30.4375).  
        If current_hourly_cost is missing, set savings to 0 and continue.
      5. Reserved Instance Recommendation:  
        If billed_cost is high and metrics stable, add an RI suggestion with reasonable saving_pct (typically 10–40%).
      6. Anomalies: Always include the CPU_Max spike event.
      7. Use EXACT schema below. Do NOT change any field names or structure.

      STRICT JSON OUTPUT SCHEMA:
    
      {
        "recommendations": {
          "effective_recommendation": { "text": "...", "saving_pct": 12.3 },
          "additional_recommendation": [
            {"text": "...", "saving_pct": 3.4},
            {"text": "...", "saving_pct": 5.0}
          ],
          "base_of_recommendations": ["Percentage CPU_Avg", "Percentage CPU_Max"]
        },
        "cost_forecasting": {
          "monthly": {monthly_forecast:.2f},
          "annually": {annual_forecast:.2f}
        },
        "anomalies": [
          {
            "metric_name": "Percentage CPU",
            "timestamp": "{cpu_max_date}",
            "value": {cpu_max:.1f},
            "reason_short": "CPU spike occurred on this date"
          }
        ],
        "contract_deal": {
          "assessment": "good" | "bad" | "unknown",
          "for sku": "{sku}",
          "reason": "...",
          "monthly_saving_pct": 1.2,
          "annual_saving_pct": 14.4
        }
      }
    """

# --- EXPORTED LLM CALL FUNCTIONS (single-resource variants) ---

def get_storage_recommendation_single(resource_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Generates cost recommendations for a single Azure Storage Account:
    returns a dict (or None) with parsed JSON from LLM.
    """
    if not resource_data:
        return None

    billed_cost = resource_data.get("billed_cost", 0.0)
    duration_days = int(resource_data.get("duration_days", 30) or 30)
    start_date = resource_data.get("start_date", "N/A")
    end_date = resource_data.get("end_date", "N/A")
    
    forecast = _extrapolate_costs(billed_cost, duration_days)
    
    prompt = _generate_storage_prompt(resource_data, start_date, end_date, forecast['monthly'], forecast['annually'])
    
    raw = llm_call(prompt)
    if not raw:
        print(f"Empty LLM response for storage resource {resource_data.get('resource_id')}")
        return None

    # Use the extractor to get JSON text
    json_str = extract_json_str(raw)
    if not json_str:
        print(f"Could not extract JSON from LLM output for storage resource {resource_data.get('resource_id')}. Raw output:\n{raw}")
        return None

    try:
        parsed = json.loads(json_str)
        if not isinstance(parsed, dict):
            print(f"LLM storage response parsed to non-dict: {type(parsed)}")
            return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON (after extraction) for storage resource {resource_data.get('resource_id')}. Extracted string:\n{json_str}")
        return None

    parsed['resource_id'] = resource_data.get('resource_id')
    parsed['_forecast_monthly'] = forecast['monthly']
    parsed['_forecast_annual'] = forecast['annually']
    return parsed


def get_compute_recommendation_single(resource_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Generates cost recommendations for a single VM resource:
    returns a dict (or None) with parsed JSON from LLM.
    """
    if not resource_data:
        return None

    billed_cost = resource_data.get("billed_cost", 0.0)
    duration_days = int(resource_data.get("duration_days", 30) or 30)
    start_date = resource_data.get("start_date", "N/A")
    end_date = resource_data.get("end_date", "N/A")

    forecast = _extrapolate_costs(billed_cost, duration_days)
    
    prompt = _generate_compute_prompt(resource_data, start_date, end_date, forecast['monthly'], forecast['annually'])
    
    raw = llm_call(prompt)
    if not raw:
        print(f"Empty LLM response for compute resource {resource_data.get('resource_id')}")
        return None

    # Use the extractor to get JSON text
    json_str = extract_json_str(raw)
    if not json_str:
        print(f"Could not extract JSON from LLM output for compute resource {resource_data.get('resource_id')}. Raw output:\n{raw}")
        return None

    try:
        parsed = json.loads(json_str)
        if not isinstance(parsed, dict):
            print(f"LLM compute response parsed to non-dict: {type(parsed)}")
            return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON (after extraction) for compute resource {resource_data.get('resource_id')}. Extracted string:\n{json_str}")
        return None

    parsed['resource_id'] = resource_data.get('resource_id')
    parsed['_forecast_monthly'] = forecast['monthly']
    parsed['_forecast_annual'] = forecast['annually']
    return parsed


# Backwards-compatible wrappers (process lists but only the first element)
def get_storage_recommendation(data: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """Wrapper for backward compatibility, processes only the first resource."""
    if not data:
        return None
    # Only process first resource (single-resource flow)
    single = get_storage_recommendation_single(data[0])
    return [single] if single else None

def get_compute_recommendation(data: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """Wrapper for backward compatibility, processes only the first resource."""
    if not data:
        return None
    single = get_compute_recommendation_single(data[0])
    return [single] if single else None
