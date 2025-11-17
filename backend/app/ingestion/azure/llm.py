# app/ingestion/azure/llm.py
import psycopg2
import pandas as pd
import sys
import os
import json
import hashlib
from typing import Optional, List, Dict, Any
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from datetime import datetime, timedelta
from app.core.genai import llm_call
from app.ingestion.azure.postgres_operation import connection, dump_to_postgresql, create_hash_key, fetch_existing_hash_keys
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus
# JSON extraction util (expects this file in app/ingestion/azure/)
from app.ingestion.azure.llm_json_extractor import extract_json, extract_json_str

load_dotenv()

# --- Utility Functions ---

def _create_local_engine_from_env():
    """
    Create a SQLAlchemy engine using DB env vars.
    Uses quote_plus to safely escape the password.
    """
    user = os.getenv("DB_USER_NAME")
    password = os.getenv("DB_PASSWORD") or ""
    host = os.getenv("DB_HOST_NAME")
    port = os.getenv("DB_PORT") or "5432"
    db = os.getenv("DB_NAME")

    if not all([user, password, host, db]):
        raise RuntimeError("Missing DB env vars. Ensure DB_USER_NAME/DB_PASSWORD/DB_HOST_NAME/DB_NAME are set")

    pwd_esc = quote_plus(password)
    engine_url = f"postgresql+psycopg2://{user}:{pwd_esc}@{host}:{port}/{db}"
    print(f"[DEBUG] Creating local SQLAlchemy engine for {host}:{port}/{db} user={user}")
    engine = create_engine(engine_url, pool_pre_ping=True)
    return engine

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


# --- Resource-type guard (Approach 1) ---
def _is_resource_for_type(resource_type: str, resource_id: Optional[str]) -> bool:
    """
    Quick heuristic to confirm the supplied resource_id looks like the requested resource_type.
    Returns True if unknown or resource_id is None.
    """
    if not resource_id:
        return True
    rid = resource_id.lower()
    t = resource_type.strip().lower()
    if t in ("vm", "virtualmachine", "virtual_machine"):
        # ARM path fragment check for VMs
        return ("/virtualmachines/" in rid) or ("/compute/virtualmachines" in rid)
    if t in ("storage", "storageaccount", "storage_account"):
        return ("/storageaccounts/" in rid) or ("/storage/" in rid)
    # default: accept if unknown type
    return True


# --- VM: Dynamic Metrics + Spike Date ---

def fetch_vm_utilization_data(conn, schema_name, start_date, end_date, resource_id=None):
    """
    Fetch VM metrics including AVG, MAX value, and the MAX timestamp.
    Parameterizes resource_id to avoid SQL injection, and returns at most one row
    when resource_id is provided.
    """
    resource_filter_sql = ""
    resource_dim_filter_sql = ""
    params = {"start_date": start_date, "end_date": end_date}
    if resource_id:
        resource_filter_sql = "AND LOWER(resource_id) = LOWER(%(resource_id)s)"
        resource_dim_filter_sql = "WHERE LOWER(resource_id) = LOWER(%(resource_id)s)"
        params["resource_id"] = resource_id

    query = f"""
       WITH metric_pivot AS (
            SELECT
                LOWER(resource_id) AS resource_id,
                metric_name,
                value::FLOAT AS metric_value,
                "timestamp"
            FROM {schema_name}.gold_azure_fact_vm_metrics 
            WHERE resource_id IS NOT NULL
              AND "timestamp" >= %(start_date)s::timestamp
              AND "timestamp" <= (%(end_date)s::timestamp + INTERVAL '1 day' - INTERVAL '1 second')
              AND metric_name IS NOT NULL
              {resource_filter_sql}
        ),

        -- NEW CTE 1: Calculate AVG and MAX metric values
        metric_avg_max AS (
            SELECT
                resource_id,
                metric_name,
                AVG(metric_value) AS avg_value,
                MAX(metric_value) AS max_value
            FROM metric_pivot
            GROUP BY resource_id, metric_name
        ),
        
        -- NEW CTE 2: Find the exact timestamp corresponding to the maximum value
        metric_max_timestamp AS (
            SELECT DISTINCT ON (resource_id, metric_name)
                resource_id,
                metric_name,
                "timestamp" AS max_timestamp
            FROM metric_pivot
            -- Order by value DESC to get the highest value, then timestamp DESC for tie-breaking
            ORDER BY resource_id, metric_name, metric_value DESC, "timestamp" DESC
        ),

        -- NEW CTE 3: Combine AVG/MAX values with the MAX timestamp
        metric_agg AS (
            SELECT
                amm.resource_id,
                amm.metric_name,
                amm.avg_value,
                amm.max_value,
                amt.max_timestamp
            FROM metric_avg_max amm
            JOIN metric_max_timestamp amt 
                ON amm.resource_id = amt.resource_id 
                AND amm.metric_name = amt.metric_name
        ),

        metric_map AS (
            SELECT
                resource_id,
                -- Combine AVG, MAX value, and MAX timestamp into a single JSON object
                (
                    json_object_agg(metric_name || '_Avg', ROUND(avg_value::NUMERIC, 6))::jsonb ||
                    json_object_agg(metric_name || '_Max', ROUND(max_value::NUMERIC, 6))::jsonb ||
                    json_object_agg(metric_name || '_MaxDate', TO_CHAR(max_timestamp, 'YYYY-MM-DD HH24:MI'))::jsonb
                )::json AS metrics_json
            FROM metric_agg
            GROUP BY resource_id
        ),
        
        cost_agg AS (
            SELECT
                LOWER(f.resource_id) AS resource_id,
                MAX(f.contracted_unit_price) AS contracted_unit_price,
                SUM(COALESCE(f.pricing_quantity,0)) AS pricing_quantity,
                SUM(COALESCE(f.billed_cost,0)) AS billed_cost,
                SUM(COALESCE(f.consumed_quantity,0)) AS consumed_quantity,
                MAX(COALESCE(f.consumed_unit, '')) AS consumed_unit,
                MAX(COALESCE(f.pricing_unit, '')) AS pricing_unit
            FROM {schema_name}.gold_azure_fact_cost f
            WHERE f.charge_period_start::date BETWEEN %(start_date)s::date AND %(end_date)s::date
              {resource_filter_sql}
            GROUP BY LOWER(f.resource_id)
        ),

        resource_dim AS (
            SELECT
                LOWER(resource_id) AS resource_id,
                resource_name,
                region_id,
                region_name,
                service_category,
                service_name
            FROM {schema_name}.gold_azure_resource_dim
            {resource_dim_filter_sql} -- FIX APPLIED: Filters resource_dim when resource_id is provided
        )

        SELECT
            rd.resource_id,
            rd.resource_name,
            rd.region_id,
            rd.region_name,
            rd.service_category,
            rd.service_name,
            COALESCE(c.contracted_unit_price, NULL) AS contracted_unit_price,
            COALESCE(c.pricing_quantity, 0) AS pricing_quantity,
            COALESCE(c.billed_cost, 0) AS billed_cost,
            COALESCE(c.consumed_quantity, 0) AS consumed_quantity,
            COALESCE(c.consumed_unit, '') AS consumed_unit,
            COALESCE(c.pricing_unit, '') AS pricing_unit,
            m.metrics_json
        FROM resource_dim rd
        LEFT JOIN metric_map m ON rd.resource_id = m.resource_id
        LEFT JOIN cost_agg c ON rd.resource_id = c.resource_id
        ORDER BY COALESCE(c.billed_cost, 0) DESC;
    """

    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        print(f"Error executing VM utilization query: {e}")
        return pd.DataFrame()  # return empty df for upstream handling

    # If resource_id provided, guarantee at most one row
    if resource_id and not df.empty:
        df = df.head(1).reset_index(drop=True)

    # Expand the metrics_json into separate columns (flatten)
    if not df.empty and "metrics_json" in df.columns:
        try:
            # Ensure JSON strings become dicts
            def _to_dict(x):
                if x is None:
                    return {}
                if isinstance(x, str):
                    try:
                        return json.loads(x)
                    except Exception:
                        return {}
                if isinstance(x, dict):
                    return x
                return {}

            metrics_series = df["metrics_json"].apply(_to_dict)
            metrics_expanded = pd.json_normalize(metrics_series).add_prefix("metric_")
            metrics_expanded.index = df.index
            df = pd.concat([df.drop(columns=["metrics_json"]), metrics_expanded], axis=1)
        except Exception as ex:
            print(f"Warning: failed to expand metrics_json: {ex}")
            # keep original df without expansion

    return df


@connection
def run_llm_vm(conn, schema_name, start_date=None, end_date=None, resource_id=None) -> Optional[Dict[str, Any]]:
    """
    Run LLM analysis for a single VM and return a single recommendation dict (or None).
    """
    if end_date is None:
        end_dt = datetime.utcnow().date()
    else:
        end_dt = pd.to_datetime(end_date).date()

    if start_date is None:
        start_dt = end_dt - timedelta(days=30)
    else:
        start_dt = pd.to_datetime(start_date).date()

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    print(f"🔎 Running VM LLM for {schema_name} from {start_str} to {end_str} "
          f"{'(resource_id filter applied)' if resource_id else ''}")

    df = fetch_vm_utilization_data(conn, schema_name, start_str, end_str, resource_id=resource_id)
    if df is None or df.empty:
        print("⚠️ No VM data found for the requested date range / resource.")
        return None

    # annotate with date info for LLM context
    df["start_date"] = start_str
    df["end_date"] = end_str
    df["duration_days"] = (pd.to_datetime(end_str) - pd.to_datetime(start_str)).days or 1

    # convert to single record (we always process only one resource)
    if resource_id and df.shape[0] > 1:
        print(f"⚠️ WARNING: Resource ID was provided, but {df.shape[0]} records were fetched. Restricting to the first record for LLM analysis.")
    resource_row = df.head(1).to_dict(orient="records")[0]

    recommendation = get_compute_recommendation_single(resource_row)
    
    if recommendation:
        print("✅ LLM analysis complete! Returning recommendation.")
        print(f"vm recommendation generated by LLM: {recommendation}")
        return recommendation
    else:
        print("⚠️ No recommendation generated by LLM.")
        return None


# --- Storage: Dynamic Metrics + Spike Date ---
def fetch_storage_account_utilization_data(
    conn,
    schema_name: str,
    start_date: str,
    end_date: str,
    resource_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch storage account metrics including AVG, MAX value, and the date of MAX (spike).
    Parameterized resource id and returns at most one row when resource_id provided.
    """
    params = {
        "start_date": start_date,
        "end_date": end_date,
    }

    resource_filter_metric = ""
    resource_filter_cost = ""
    resource_filter_dim = ""
    if resource_id:
        params["resource_id"] = resource_id
        resource_filter_metric = "AND LOWER(dsa.resource_id) = LOWER(%(resource_id)s)"
        resource_filter_cost = "AND LOWER(f.resource_id) = LOWER(%(resource_id)s)"
        resource_filter_dim = "WHERE LOWER(resource_id) = LOWER(%(resource_id)s)"

    query = f"""
    WITH metric_agg AS (
        -- Base metric selection
        SELECT
            LOWER(dsa.resource_id) AS resource_id,
            dsa.metric_name,
            dsa.value::FLOAT AS metric_value,
            dsa.date_key
        FROM {schema_name}.gold_azure_fact_storage_metrics dsa
        WHERE dsa.resource_id IS NOT NULL
          AND dsa.date_key BETWEEN %(start_date)s AND %(end_date)s
          AND dsa.metric_name IS NOT NULL
          {resource_filter_metric}
    ),

    -- NEW CTE 1: Calculate AVG and MAX metric values
    metric_avg_max AS (
        SELECT
            resource_id,
            metric_name,
            AVG(metric_value) AS avg_value,
            MAX(metric_value) AS max_value
        FROM metric_agg
        GROUP BY resource_id, metric_name
    ),

    -- NEW CTE 2: Find the date_key corresponding to the maximum value (Spike Date)
    metric_max_date AS (
        SELECT DISTINCT ON (resource_id, metric_name)
            resource_id,
            metric_name,
            date_key AS max_date_key
        FROM metric_agg
        ORDER BY resource_id, metric_name, metric_value DESC, date_key DESC
    ),
    
    -- NEW CTE 3 (REPLACES metric_final): Combine AVG/MAX values with the MAX date key
    metric_final AS (
        SELECT
            amm.resource_id,
            amm.metric_name,
            amm.avg_value,
            amm.max_value,
            amd.max_date_key
        FROM metric_avg_max amm
        JOIN metric_max_date amd 
            ON amm.resource_id = amd.resource_id 
            AND amm.metric_name = amd.metric_name
    ),

    metric_map AS (
        SELECT
            resource_id,
            (
                json_object_agg(metric_name || '_Avg', ROUND(avg_value::NUMERIC, 6))::jsonb ||
                json_object_agg(metric_name || '_Max', ROUND(max_value::NUMERIC, 6))::jsonb ||
                json_object_agg(metric_name || '_MaxDate', max_date_key::TEXT)::jsonb
            )::json AS metrics_json
        FROM metric_final
        GROUP BY resource_id
    ),

    cost_agg AS (
        SELECT
            LOWER(f.resource_id) AS resource_id,
            MAX(f.contracted_unit_price) AS contracted_unit_price,
            SUM(COALESCE(f.pricing_quantity,0)) AS pricing_quantity,
            SUM(COALESCE(f.billed_cost,0)) AS billed_cost,
            SUM(COALESCE(f.consumed_quantity,0)) AS consumed_quantity,
            MAX(COALESCE(f.consumed_unit, '')) AS consumed_unit,
            MAX(COALESCE(f.pricing_unit, '')) AS pricing_unit
        FROM {schema_name}.gold_azure_fact_cost f
        WHERE f.charge_period_start::date BETWEEN %(start_date)s::date AND %(end_date)s::date
          {resource_filter_cost}
        GROUP BY LOWER(f.resource_id)
    ),

    resource_dim AS (
        SELECT
            LOWER(resource_id) AS resource_id,
            storage_account_name,
            location,
            kind,
            sku,
            access_tier
        FROM {schema_name}.dim_storage_account
        {resource_filter_dim}
    )

    SELECT
        rd.resource_id,
        rd.storage_account_name,
        rd.location,
        rd.kind,
        rd.sku,
        rd.access_tier,
        COALESCE(c.contracted_unit_price, NULL) AS contracted_unit_price,
        COALESCE(c.pricing_quantity, 0) AS pricing_quantity,
        COALESCE(c.billed_cost, 0) AS billed_cost,
        COALESCE(c.consumed_quantity, 0) AS consumed_quantity,
        COALESCE(c.consumed_unit, '') AS consumed_unit,
        COALESCE(c.pricing_unit, '') AS pricing_unit,
        m.metrics_json
    FROM resource_dim rd
    LEFT JOIN metric_map m ON rd.resource_id = m.resource_id
    LEFT JOIN cost_agg c ON rd.resource_id = c.resource_id
    ORDER BY COALESCE(c.billed_cost, 0) DESC;
    """

    df = pd.read_sql_query(query, conn, params=params)

    # If resource_id provided, guarantee at most one row
    if resource_id and not df.empty:
        df = df.head(1).reset_index(drop=True)

    # Expand metrics_json into columns
    if not df.empty and "metrics_json" in df.columns:
        try:
            metrics_expanded = pd.json_normalize(df["metrics_json"].fillna({})).add_prefix("metric_")
            metrics_expanded.index = df.index
            df = pd.concat([df.drop(columns=["metrics_json"]), metrics_expanded], axis=1)
        except Exception as ex:
            print(f"Warning: failed to expand storage metrics_json: {ex}")

    return df


@connection
def run_llm_storage(conn, schema_name, start_date=None, end_date=None, resource_id=None) -> Optional[Dict[str, Any]]:
    """
    Run LLM analysis for a single Storage Account and return a single recommendation dict (or None).
    """
    if end_date is None:
        end_dt = datetime.utcnow().date()
    else:
        end_dt = pd.to_datetime(end_date).date()

    if start_date is None:
        start_dt = end_dt - timedelta(days=90)
    else:
        start_dt = pd.to_datetime(start_date).date()

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    print(f"🔎 Running Storage LLM for {schema_name} from {start_str} to {end_str} "
          f"{'(resource_id filter applied)' if resource_id else ''}")

    df = fetch_storage_account_utilization_data(conn, schema_name, start_str, end_str, resource_id=resource_id)
    if df is None or df.empty:
        print("⚠️ No storage account data found for the requested date range / resource.")
        return None

    df["start_date"] = start_str
    df["end_date"] = end_str
    df["duration_days"] = (pd.to_datetime(end_str) - pd.to_datetime(start_str)).days or 1

    if resource_id and df.shape[0] > 1:
        print(f"⚠️ WARNING: Resource ID was provided, but {df.shape[0]} records were fetched. Restricting to the first record for LLM analysis.")

    resource_row = df.head(1).to_dict(orient="records")[0]

    recommendation = get_storage_recommendation_single(resource_row)

    if recommendation:
        print("✅ LLM analysis complete! Returning recommendation.")
        print(f"storage recommendation generated by LLM: {recommendation}")
        return recommendation
    else:
        print("⚠️ No recommendation generated by LLM.")
        return None


def run_llm_analysis(resource_type, schema_name, start_date=None, end_date=None,resource_id=None):
    """
    Unified entry point for running LLM cost optimization analyses.
    This version implements a strict guard: it will raise ValueError if the provided
    resource_id does not look like the requested resource_type.
    """
    # Input normalization
    rtype = resource_type.strip().lower()
    start_date = start_date or (datetime.utcnow().date().replace(day=1).strftime("%Y-%m-%d"))
    end_date = end_date or datetime.utcnow().date().strftime("%Y-%m-%d")

    # STRICT GUARD: ensure resource_id matches resource_type
    if resource_id and not _is_resource_for_type(rtype, resource_id):
        raise ValueError(f"Resource id '{resource_id}' does not appear to be a '{resource_type}' resource. Aborting to avoid misrouting.")

    print(f"🚀 Starting LLM analysis for '{rtype}' resources "
          f"from {start_date} to {end_date} in schema '{schema_name}'")

    if rtype in ["vm", "virtualmachine", "virtual_machine"]:
        return run_llm_vm(schema_name, start_date=start_date, end_date=end_date, resource_id=resource_id)
    elif rtype in ["storage", "storageaccount", "storage_account"]:
        return run_llm_storage(schema_name, start_date=start_date, end_date=end_date, resource_id=resource_id)
    else:
        raise ValueError(f"Unsupported resource_type: {resource_type}")


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

    return f"""
    You are an expert Azure Cost Optimization Analyst. Analyze the provided data for the Storage Account and generate cost optimization recommendations in the strict JSON format below.

    **ANALYSIS CONTEXT:**
    - Resource ID: {resource_id}
    - SKU/Tier: {sku} ({access_tier})
    - Analysis Period: {start_date} to {end_date} ({duration_days} days)
    - Total Billed Cost for Period: ${billed_cost:.2f} USD
    
    **UTILIZATION METRICS (AVG / MAX / MAX DATE):**
    {metrics_display}

    **INSTRUCTIONS:**
    1.  **Effective Recommendation:** Use the **MAX Capacity** and **MAX Date** to check for saturation trends. Prioritize moving high **UsedCapacity (GiB)_Avg** from **Hot** to **Cool/Archive** tiers. If capacity is small, prioritize reducing high **Transactions (count)_Avg**. Calculate a plausible **saving_pct**.
    2.  **Contract Deal:** Compare the contracted unit price ({resource_data.get('contracted_unit_price', 'N/A')}) against the market rate for `{sku}`.

    **REQUIRED JSON OUTPUT FORMAT (STRICTLY ADHERE TO THIS SCHEMA):**
    ```json
    {{
      "recommendations": {{
        "effective_recommendation": {{ "text": "...", "saving_pct": 12.3 }},
        "additional_recommendation": [
           {{"text":"...", "saving_pct": 3.4}}, 
           {{"text":"...", "saving_pct": 5.0}}
        ],
        "base_of_recommendations": ["UsedCapacity (GiB)_Avg", "Transactions (count)_Max"]
      }},
      "cost_forecasting": {{
        "monthly": {monthly_forecast:.2f},
        "annually": {annual_forecast:.2f}
      }},
      "anomalies": [ 
        {{ 
          "metric_name": "UsedCapacity (GiB)", 
          "timestamp":"{max_capacity_date}", 
          "value": {max_capacity},
          "reason_short":"Max capacity reached on this date, indicating usage trend"
        }}
      ],
      "contract_deal": {{
        "assessment": "good" | "bad" | "unknown",
        "for sku": "{sku}",
        "reason": "...",
        "monthly_saving_pct": 1.2,
        "annual_saving_pct": 14.4
      }}
    }}
    ```
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

    return f"""
    You are an expert Azure Cost Optimization Analyst. Analyze the provided data for the Virtual Machine and generate cost optimization recommendations in the strict JSON format below.

    **ANALYSIS CONTEXT:**
    - Resource ID: {resource_id}
    - VM Name: {resource_name}
    - Analysis Period: {start_date} to {end_date} ({duration_days} days)
    - Total Billed Cost for Period: ${billed_cost:.2f} USD
    
    **UTILIZATION METRICS (AVG / MAX / MAX DATE):**
    {metrics_display}

    **INSTRUCTIONS:**
    1.  **Effective Recommendation:** Use the **Percentage CPU_Avg** for general right-sizing. Use **Percentage CPU_Max** and **Percentage CPU_MaxDate** to identify spikes that might prevent downsizing. Prioritize **right-sizing** if AVG CPU is below 20% AND MAX CPU is below 75%. If cost is high, recommend **Reserved Instance (RI)** purchase. Calculate a plausible **saving_pct**.
    2.  **Contract Deal:** Compare the contracted unit price ({resource_data.get('contracted_unit_price', 'N/A')}) against the market rate for VM SKU `{sku}`.

    **REQUIRED JSON OUTPUT FORMAT (STRICTLY ADHERE TO THIS SCHEMA):**
    ```json
    {{
      "recommendations": {{
        "effective_recommendation": {{ "text": "...", "saving_pct": 12.3 }},
        "additional_recommendation": [
           {{"text":"...", "saving_pct": 3.4}},
           {{"text":"...", "saving_pct": 5.0}}
        ],
        "base_of_recommendations": ["Percentage CPU_Avg", "Percentage CPU_Max"]
      }},
      "cost_forecasting": {{
        "monthly": {monthly_forecast:.2f},
        "annually": {annual_forecast:.2f}
      }},
      "anomalies": [
        {{
          "metric_name": "Percentage CPU",
          "timestamp": "{cpu_max_date}",
          "value": {cpu_max:.1f},
          "reason_short": "CPU spike occurred on this date"
        }}
      ],
      "contract_deal": {{
        "assessment": "good" | "bad" | "unknown",
        "for sku": "{sku}",
        "reason": "...",
        "monthly_saving_pct": 1.2,
        "annual_saving_pct": 14.4
      }}
    }}
    ```
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
    if not data:
        return None
    # Only process first resource (single-resource flow)
    single = get_storage_recommendation_single(data[0])
    return [single] if single else None

def get_compute_recommendation(data: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    if not data:
        return None
    single = get_compute_recommendation_single(data[0])
    return [single] if single else None
