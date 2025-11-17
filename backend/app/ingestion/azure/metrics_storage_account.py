import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os 
import time
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from app.ingestion.azure.postgres_operation import dump_to_postgresql
# ==================== CONFIGURATION ====================

# Metric aggregation methods (match Azure Monitor aggregation types)
# In metrics_storage_account.py

AGGREGATION_METHODS = {
    # FIX: Capacity and Count metrics require 'Total' aggregation over a daily interval
    "UsedCapacity": "Total",
    "BlobCapacity": "Total",
    "BlobCount": "Total",
    
    # Transaction metrics (Already correct)
    "Transactions": "Total",
    "Egress": "Total",
    "Ingress": "Total",
    
    # Latency/Availability metrics (Already correct)
    "SuccessE2ELatency": "Average",
    "SuccessServerLatency": "Average",
    "Availability": "Average",
    
    # FIX: Other capacity metrics require 'Total'
    "FileCapacity": "Total",
    "TableCapacity": "Total",
    "QueueCapacity": "Total",
}
# Storage account tier mapping (for reference in analysis)
SKU_TIER_MAP = {
    "Standard": 1,
    "Premium": 2,
}

ACCESS_TIER_MAP = {
    "Hot": "hot",
    "Cool": "cool",
    "Archive": "archive",
}

REPLICATION_TYPES = {
    "LRS": "Locally Redundant Storage",
    "GRS": "Geo-Redundant Storage",
    "RAGRS": "Read-Access Geo-Redundant Storage",
    "ZRS": "Zone-Redundant Storage",
    "GZRS": "Geo-Zone-Redundant Storage",
    "RAGZRS": "Read-Access Geo-Zone-Redundant Storage",
}

# Configuration
interval = "P1D"  # Daily aggregation
days_back = 90    # Example: 7 days history

def create_hash_key(df, columns):
    # Placeholder for hash key generation logic
    df['hash_key'] = df[columns].astype(str).sum(axis=1).apply(lambda x: hash(x) % (10**8))
    return df

def fetch_existing_hash_keys(schema_name, table_name):
    # Placeholder for fetching existing keys for deduplication
    return set() # Return an empty set for this example


# ==================== AZURE API FUNCTIONS ====================

def get_access_token(tenant_id, client_id, client_secret):
    """Gets OAuth2 token for Azure API access"""
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://management.azure.com/.default",
        "grant_type": "client_credentials"
    }
    response = requests.post(token_url, data=token_data)
    access_token = response.json().get("access_token")
    if not access_token:
        raise Exception("❌ Failed to retrieve access token")
    return access_token


def list_storage_accounts(subscription_id, headers):
    """Lists all storage accounts in the subscription"""
    url = (
        f"https://management.azure.com/subscriptions/{subscription_id}/"
        f"providers/Microsoft.Storage/storageAccounts"
        f"?api-version=2023-01-01"
    )
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to list storage accounts: {response.status_code}")
    return response.json().get("value", [])


def get_available_metrics(resource_id, headers):
    """Discovers available metrics for a storage account"""
    url = (
        f"https://management.azure.com{resource_id}/providers/microsoft.insights/"
        f"metricDefinitions?api-version=2023-10-01"
    )
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"⚠️ Failed to fetch available metrics for {resource_id}: {response.status_code}")
        return []

    definitions = response.json().get("value", [])
    return [metric["name"]["value"] for metric in definitions if "name" in metric]


# In metrics_storage_account.py

def fetch_storage_metrics(storage_account, headers, timespan, interval, metric_name, aggregation):
    """Fetches metric data for a specific metric, adding filters for capacity metrics."""
    resource_id = storage_account["id"]
    
    # 🌟 CRUCIAL FIX: Check if the metric requires a dimension filter
    filter_param = ""
    # List of metrics that typically require the Service dimension filter
    CAPACITY_AND_COUNT_METRICS = [
        "UsedCapacity", "BlobCapacity", "BlobCount", "FileCapacity", 
        "TableCapacity", "QueueCapacity"
    ]
    
    if metric_name in CAPACITY_AND_COUNT_METRICS:
        # Most capacity/count metrics are reported under the 'blob' service dimension.
        # This resolves the 400 Bad Request error.
        filter_param = "&$filter=Service eq 'blob'" 
        
    metrics_url = (
        f"https://management.azure.com{resource_id}/providers/microsoft.insights/metrics"
        f"?api-version=2023-10-01"
        f"&metricnames={metric_name.replace(' ', '%20')}"
        f"&timespan={timespan}"
        f"&interval={interval}"
        f"&aggregation={aggregation}"
        f"{filter_param}" 
    )
    return requests.get(metrics_url, headers=headers)


def get_storage_account_details(resource_id, subscription_id, headers):
    """Fetches storage account properties (SKU, access tier, replication, location)"""
    url = (
        f"https://management.azure.com{resource_id}"
        f"?api-version=2023-01-01"
    )
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"⚠️ Failed to fetch storage account details for {resource_id}: {response.status_code}")
        return {}

    data = response.json()
    properties = data.get("properties", {})
    sku = data.get("sku", {})
    
    # Extract replication type from SKU name (e.g., Standard_RAGRS -> RAGRS)
    replication_type = sku.get("name", "").split("_")[-1] if "_" in sku.get("name", "") else "unknown"

    return {
        "sku": sku.get("name", "unknown"),
        "access_tier": properties.get("accessTier", "unknown"),
        "replication": replication_type,
        "location": data.get("location", "unknown"),
        "kind": data.get("kind", "unknown"),
        "creation_time": properties.get("creationTime", ""),
        "status": properties.get("statusOfPrimary", "unknown"),
    }


def collect_all_storage_metrics(storage_accounts, headers, timespan, interval, metric_names, subscription_id):
    """
    Aggregates all storage account metrics into a dataframe.
    Includes robust error handling for individual accounts.
    """
    rows = []
    
    for storage_account in storage_accounts:
        storage_account_name = storage_account["name"]
        
        # 🌟 Robust try/except block to handle account-level failures 🌟
        try:
            storage_account_id = storage_account["id"]
            resource_group = storage_account_id.split("/")[4]
            
            # Get storage account specific details
            details = get_storage_account_details(storage_account_id, subscription_id, headers)
            
            print(f"📦 Processing storage account: {storage_account_name}")
            
            for metric_name in metric_names:
                aggregation = AGGREGATION_METHODS.get(metric_name, "Average")
                response = fetch_storage_metrics(
                    storage_account, headers, timespan, interval, metric_name, aggregation
                )
                time.sleep(0.5)  # Rate limiting
                
                # Handle API status errors (e.g., 400 Bad Request for unavailable metrics)
                if response.status_code != 200:
                    print(f"⚠️ Failed for storage '{storage_account_name}' on metric '{metric_name}': {response.status_code}")
                    continue

                data = response.json()
                namespace = data.get("namespace", "")
                resourceregion = data.get("resourceregion", "")

                for metric in data.get("value", []):
                    full_id = metric.get("id", "")
                    resource_id_clean = (
                        full_id.split("/providers/Microsoft.Insights/metrics")[0] 
                        if "/providers/Microsoft.Insights/metrics" in full_id 
                        else full_id
                    )
                    metric_unit = metric.get("unit", "")
                    metric_name_actual = metric["name"]["value"]
                    display_desc = metric.get("displayDescription", "")

                    for series in metric.get("timeseries", []):
                        for point in series.get("data", []):
                            # Get the appropriate value based on metric type
                            if aggregation == "Total":
                                value = point.get("total", 0.0)
                            else:
                                value = point.get("average", 0.0)

                            row = {
                                "storage_account_name": storage_account_name,
                                "resource_group": resource_group,
                                "subscription_id": subscription_id,
                                "timestamp": point.get("timeStamp"),
                                "value": value,
                                "metric_name": metric_name_actual,
                                "unit": metric_unit,
                                "displaydescription": display_desc,
                                "namespace": namespace,
                                "resourceregion": resourceregion,
                                "resource_id": resource_id_clean,
                                "sku": details.get("sku", "unknown"),
                                "access_tier": details.get("access_tier", "unknown"),
                                "replication": details.get("replication", "unknown"),
                                "location": details.get("location", "unknown"),
                                "kind": details.get("kind", "unknown"),
                                "storage_account_status": details.get("status", "unknown"),
                                "cost": "", # Placeholder for cost data
                            }
                            rows.append(row)
        
        except requests.exceptions.RequestException as e:
            # Catch network errors (ConnectionError, Timeout, etc.)
            print(f"❌ FATAL REQUEST ERROR for account '{storage_account_name}'. Skipping to next account. Error: {e}")
            continue 
        
        except Exception as e:
            # Catch any other unexpected error (like JSON parsing failure)
            print(f"❌ UNEXPECTED ERROR during processing of '{storage_account_name}'. Skipping to next account. Error: {e}")
            continue

    return pd.DataFrame(rows)


def metrics_dump(tenant_id, client_id, client_secret, subscription_id, schema_name, table_name):
    """Main entry point for storage account metrics ingestion"""
    print(f"🔄 Starting Storage Account metrics dump...")
    print(f"📊 Schema: {schema_name}, Table: {table_name}")
    
    # Step 1: Get access token
    print("🔐 Authenticating with Azure...")
    try:
        access_token = get_access_token(tenant_id, client_id, client_secret)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        print("✅ Authentication successful")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return

    # Step 2: Define time range
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days_back)
    timespan = f"{start_time.isoformat()}Z/{end_time.isoformat()}Z"
    print(f"📅 Time range: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')} ({days_back} days)")

    # Step 3: List storage accounts and determine metrics
    print(f"📋 Fetching storage accounts from subscription: {subscription_id}...")
    try:
        storage_accounts = list_storage_accounts(subscription_id, headers)
        print(f"📦 Found {len(storage_accounts)} storage account(s)")
    except Exception as e:
        print(f"❌ Failed to list storage accounts: {e}")
        return

    if not storage_accounts:
        print("No storage accounts found. Exiting.")
        return

    metric_names_to_collect = list(AGGREGATION_METHODS.keys())
    print(f"   📊 Metrics to collect: {len(metric_names_to_collect)}")
    print("   📈 Collecting metrics... (this may take a moment)")

    # Step 4: Collect all metrics
    df = collect_all_storage_metrics(
        storage_accounts, headers, timespan, interval, metric_names_to_collect, subscription_id
    )
    
    # Step 5: Final processing and dump
    if df.empty:
        print("No metrics collected. Exiting.")
        return

    print("============================================================")
    print(f"📊 Total records collected: {len(df)}")
    
    # Create hash key for deduplication
    df = create_hash_key(df, [
        "storage_account_name", "timestamp", "metric_name", "resource_id", "subscription_id"
    ])

    # Deduplication check against PostgreSQL
    existing_hash_keys = fetch_existing_hash_keys(schema_name, table_name)
    new_df = df[~df['hash_key'].isin(existing_hash_keys)].copy()
    
    print(f"🔎 New unique records to insert (after dedupe): {len(new_df)}")

    if not new_df.empty:
        # Step 6: Dump to PostgreSQL
        print(f"💾 Dumping unique storage metrics to PostgreSQL...")
        column_order = [
            "storage_account_name", "resource_group", "subscription_id", "timestamp", "value",
            "metric_name", "unit", "displaydescription", "namespace", "resourceregion", 
            "resource_id", "sku", "access_tier", "replication", "kind", 
            "storage_account_status", "cost", "hash_key"
        ]
        
        # Ensure the columns in the DataFrame match the target table columns precisely
        new_df = new_df[column_order]

        dump_to_postgresql(new_df, schema_name, table_name)
        print(f"✅ Storage account metrics dumped successfully to {schema_name}.{table_name}!")
    else:
        print("No new unique records to insert.")

    print("============================================================")

