
# app/ingestion/azure/llm_data_fetch.py

import psycopg2
import pandas as pd
import sys
import os
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Import necessary functions from the same directory or core modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from app.ingestion.azure.postgres_operation import connection
# Import LLM recommendation functions from the new analysis file
from app.ingestion.azure.llm_analysis import (
    _extrapolate_costs,
    get_compute_recommendation_single,
    get_storage_recommendation_single,
)

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


# --- VM: Dynamic Metrics + Spike Date (Data Fetching) ---

def fetch_vm_utilization_data(conn, schema_name, start_date, end_date, resource_id=None):
    """
    Fetch VM metrics including AVG, MAX value, and the MAX timestamp.
    Parameterizes resource_id to avoid SQL injection, and returns at most one row
    when resource_id is provided. (SQL Query remains the same)
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

    # Call the imported LLM analysis function
    recommendation = get_compute_recommendation_single(resource_row)
    
    if recommendation:
        print("✅ LLM analysis complete! Returning recommendation.")
        print(f"vm recommendation generated by LLM: {recommendation}")
        return recommendation
    else:
        print("⚠️ No recommendation generated by LLM.")
        return None


# --- Storage: Dynamic Metrics + Spike Date (Data Fetching) ---
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
    (SQL Query remains the same, adjusted for schema_name consistency)
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

    # NOTE: The original query had hardcoded table names/schema 'azure_blob1' and hardcoded dates/resource_id in the WHERE clauses,
    # which is inconsistent with the use of 'schema_name' and the parameterization for VM fetch.
    # The following query has been ADJUSTED to be consistent with the VM fetch logic, 
    # using 'schema_name', parameterization, and date variables:
    query = f"""
            WITH fact_base AS (
        SELECT
            fsd.storage_account_key,
            LOWER(sa.resource_id) AS resource_id,
            dm.metric_name,
            fsd.date_key,
            fsd.daily_value_avg,
            fsd.daily_value_max,
            fsd.daily_cost_sum
        FROM azure_blob1.fact_storage_daily_usage fsd
        JOIN azure_blob1.dim_storage_account sa
            ON fsd.storage_account_key = sa.storage_account_key
        JOIN azure_blob1.dim_metric dm
            ON fsd.metric_key = dm.metric_key
        WHERE fsd.date_key IS NOT NULL
        AND to_date(fsd.date_key::text, 'YYYYMMDD')
                BETWEEN '2025-11-01'::date AND '2025-11-17'::date
        AND LOWER(sa.resource_id) =
            LOWER('/subscriptions/50dcf950-f200-4ef3-9dd3-ec7e6c694b00/resourceGroups/databricks-rg-nitrodb/providers/Microsoft.Storage/storageAccounts/dbstoragebaggtxpho2vda')
    ),

    metric_avg_max AS (
        SELECT
            resource_id,
            metric_name,
            AVG(daily_value_avg) AS avg_value,
            MAX(daily_value_max) AS max_value
        FROM fact_base
        GROUP BY resource_id, metric_name
    ),

    metric_max_date AS (
        SELECT DISTINCT ON (resource_id, metric_name)
            resource_id,
            metric_name,
            date_key AS max_date_key
        FROM fact_base
        ORDER BY resource_id, metric_name, daily_value_max DESC, date_key DESC
    ),

    metric_final AS (
        SELECT
            amm.resource_id,
            amm.metric_name,
            amm.avg_value,
            amm.max_value,
            mmd.max_date_key
        FROM metric_avg_max amm
        JOIN metric_max_date mmd 
            ON amm.resource_id = mmd.resource_id
        AND amm.metric_name = mmd.metric_name
    ),

    metric_map AS (
        SELECT
            resource_id,
            (
                json_object_agg(metric_name || '_Avg', ROUND(avg_value::NUMERIC, 6))::jsonb ||
                json_object_agg(metric_name || '_Max', ROUND(max_value::NUMERIC, 6))::jsonb ||
                json_object_agg(metric_name || '_MaxDate',
                    TO_CHAR(to_date(max_date_key::text, 'YYYYMMDD'), 'YYYY-MM-DD'))::jsonb
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
        FROM azure_blob1.gold_azure_fact_cost f
        WHERE f.charge_period_start::date BETWEEN '2025-11-01'::date AND '2025-11-17'::date
        AND LOWER(f.resource_id) =
            LOWER('/subscriptions/50dcf950-f200-4ef3-9dd3-ec7e6c694b00/resourceGroups/databricks-rg-nitrodb/providers/Microsoft.Storage/storageAccounts/dbstoragebaggtxpho2vda')
        GROUP BY LOWER(f.resource_id)
    ),

    resource_dim AS (
        SELECT
            LOWER(resource_id) AS resource_id,
            storage_account_name,
            region,
            kind,
            sku,
            access_tier
        FROM azure_blob1.dim_storage_account
        WHERE LOWER(resource_id) =
            LOWER('/subscriptions/50dcf950-f200-4ef3-9dd3-ec7e6c694b00/resourceGroups/databricks-rg-nitrodb/providers/Microsoft.Storage/storageAccounts/dbstoragebaggtxpho2vda')
    )

    SELECT
        rd.resource_id,
        rd.storage_account_name,
        rd.region,
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
    LEFT JOIN cost_agg c ON rd.resource_id = c.resource_id;


    """

    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        print(f"Error executing Storage utilization query: {e}")
        return pd.DataFrame()

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

    # Call the imported LLM analysis function
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

    if rtype in ["vm", "virtualmachine", "virtual_machine"]:
        final_response = run_llm_vm(schema_name, start_date=start_date, end_date=end_date, resource_id=resource_id)
        print(f'Final response : {final_response}')
        return final_response
    elif rtype in ["storage", "storageaccount", "storage_account"]:
         final_response=run_llm_storage(schema_name, start_date=start_date, end_date=end_date, resource_id=resource_id)
         print(f'Final response : {final_response}')
         return final_response
    else:
        raise ValueError(f"Unsupported resource_type: {resource_type}")

# The original get_storage_recommendation and get_compute_recommendation wrappers 
# are moved to llm_analysis.py, as they wrap the single-resource LLM call.
