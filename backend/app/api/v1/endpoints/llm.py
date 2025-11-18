import sys
import os
import json
from typing import Optional, Union
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from tortoise.exceptions import DoesNotExist
from app.models.project import Project
from datetime import datetime

# =========================================================
# DIAGNOSTIC BLOCK: Catch silent import errors that cause 404
# =========================================================
try:
    # Standardized Imports (assuming 'app' is the project root)
    from app.ingestion.aws.llm_s3_integration import run_llm_analysis_s3
    from app.ingestion.azure.llm import run_llm_analysis
    # from app.ingestion.gcp.llm import run_llm_analysis_gcp # <-- Keeping this commented to resolve the 404
except ImportError as e:
    # If this prints, it means one of the ingestion modules has an internal error.
    print("FATAL IMPORT ERROR DURING LLM ROUTER LOAD:")
    print(f"Error: {e}")

# Router definition
router = APIRouter(tags=["llm"])

# ---------------------------------------------------------
# REQUEST MODEL
# ---------------------------------------------------------
class LLMRequest(BaseModel):
    resource_type: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    resource_id: Optional[str] = None
    schema_name: Optional[str] = None


# ---------------------------------------------------------
# RESPONSE MODEL
# ---------------------------------------------------------
class LLMResponse(BaseModel):
    status: str
    cloud: str
    schema_name: str
    resource_type: str
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    resource_id: Optional[str]
    recommendations: Optional[str] = None
    details: Optional[dict] = None
    timestamp: datetime


# ---------------------------------------------------------
# Helper: Resolve Schema - NEW ROBUST LOGIC APPLIED HERE
# ---------------------------------------------------------
async def _resolve_schema_name(project_id: Optional[Union[int, str]], schema_name: Optional[str]) -> str:
    if schema_name:
        return schema_name.lower()

    if project_id is None:
        raise HTTPException(status_code=400, detail="Either project_id or schema_name must be provided.")

    project_id_str = str(project_id)
    project = None

    # --- ROBUST LOOKUP LOGIC: Try ID first, then fall back to Name ---

    # 1. Check if the input is numeric (could be an int or a numeric string like '5')
    if project_id_str.isdigit():
        try:
            # Try to look up by Primary Key (id)
            pk = int(project_id_str)
            project = await Project.get(id=pk)
        except DoesNotExist:
            # If the ID is numeric but doesn't exist, we fall through to try by name, 
            # in case a project was named '5', for example.
            pass
    
    # 2. If no project was found yet (either input was non-numeric, or numeric ID was missing)
    if not project:
        try:
            # Try to look up by Name (case-insensitive)
            project = await Project.get(name__iexact=project_id_str)
        except DoesNotExist:
            # If both ID and Name lookups failed, raise the final 404.
            raise HTTPException(status_code=404, detail=f"Project ID/Name '{project_id_str}' not found")

    # If we reached here, 'project' is guaranteed to be a valid Project instance.
    return project.name.lower() 


# ---------------------------------------------------------
# AWS Endpoint
# ---------------------------------------------------------
@router.post("/aws/{project_id}", response_model=LLMResponse, status_code=200)
async def llm_aws(
    project_id: str, 
    payload: LLMRequest,
):
    schema = await _resolve_schema_name(project_id, payload.schema_name)

    result = run_llm_analysis_s3(
        resource_type=payload.resource_type,
        schema_name=schema,
        start_date=payload.start_date,
        end_date=payload.end_date,
        resource_id=payload.resource_id
    )

    return LLMResponse(
        status="success",
        cloud="aws",
        schema_name= schema,
        resource_type=payload.resource_type,
        start_date=payload.start_date,
        end_date=payload.end_date,
        resource_id=payload.resource_id,
        recommendations=json.dumps(result) if isinstance(result, list) else None, 
        details=None,
        timestamp=datetime.utcnow()
    )


# ---------------------------------------------------------
# AZURE Endpoint
# ---------------------------------------------------------
@router.post("/azure/{project_id}", response_model=LLMResponse, status_code=200)
async def llm_azure(
    project_id: str, 
    payload: LLMRequest,
):
    schema = await _resolve_schema_name(project_id, payload.schema_name)

    result = run_llm_analysis(
        payload.resource_type,
        schema,
        payload.start_date,
        payload.end_date,
        payload.resource_id,
    )

    return LLMResponse(
        status="success",
        cloud="azure",
        schema_name=schema,
        resource_type=payload.resource_type,
        start_date=payload.start_date,
        end_date=payload.end_date,
        resource_id=payload.resource_id,
        recommendations=json.dumps(result) if isinstance(result, list) else None,
        details=None,
        timestamp=datetime.utcnow()
    )


# ---------------------------------------------------------
# GCP Endpoint (when ready)
# ---------------------------------------------------------
@router.post("/gcp/{project_id}", response_model=LLMResponse, status_code=200)
async def llm_gcp(
    project_id: str,
    payload: LLMRequest,
):
    schema = await _resolve_schema_name(project_id, payload.schema_name)

    # result = run_llm_analysis_gcp(...)
    result = {"message": "GCP LLM not implemented yet"}

    return LLMResponse(
        status="success",
        cloud="gcp",
        schema_name=schema,
        resource_type=payload.resource_type,
        start_date=payload.start_date,
        end_date=payload.end_date,
        resource_id=payload.resource_id,
        details=result,
        recommendations=None,
        timestamp=datetime.utcnow()
    )


# ---------------------------------------------------------
# Resource IDs Endpoint - Get available resource IDs by type
# ---------------------------------------------------------
@router.get("/{cloud_platform}/{project_id}/resources/{resource_type}")
async def get_resource_ids(
    cloud_platform: str,
    project_id: str,
    resource_type: str,
):
    """
    Fetch available resource IDs for a given resource type and schema.
    Supports Azure (VM, Storage), AWS (EC2, S3), and GCP (future).
    """
    from app.ingestion.azure.postgres_operation import connection
    import pandas as pd

    # Resolve schema name from project_id
    schema = await _resolve_schema_name(project_id, None)

    # Normalize inputs
    cloud_platform = cloud_platform.lower()
    resource_type = resource_type.lower()

    @connection
    def fetch_resource_ids(conn, schema_name: str, res_type: str, cloud: str):
        """Fetch resource IDs from the database based on cloud and resource type."""
        query = None

        if cloud == "azure":
            if res_type in ["vm", "virtualmachine", "virtual_machine"]:
                # Fetch VM resource IDs from Azure
                query = f"""
                    SELECT DISTINCT LOWER(resource_id) as resource_id, resource_name
                    FROM {schema_name}.gold_azure_resource_dim
                    WHERE service_category = 'Compute'
                      AND (LOWER(resource_id) LIKE '%/virtualmachines/%'
                           OR LOWER(resource_id) LIKE '%/compute/virtualmachines%')
                    ORDER BY resource_name
                    LIMIT 100;
                """
            elif res_type in ["storage", "storageaccount", "storage_account"]:
                # Fetch Storage Account resource IDs from Azure
                query = f"""
                    SELECT DISTINCT LOWER(resource_id) as resource_id, storage_account_name as resource_name
                    FROM {schema_name}.dim_storage_account
                    WHERE resource_id IS NOT NULL
                    ORDER BY storage_account_name
                    LIMIT 100;
                """
        elif cloud == "aws":
            if res_type in ["ec2", "instance"]:
                # Fetch EC2 instance IDs from AWS
                query = f"""
                    SELECT DISTINCT resource_id, resource_id as resource_name
                    FROM {schema_name}.gold_aws_resource_dim
                    WHERE service_code = 'AmazonEC2'
                      AND resource_id IS NOT NULL
                    ORDER BY resource_id
                    LIMIT 100;
                """
            elif res_type in ["s3", "bucket"]:
                # Fetch S3 bucket resource IDs from AWS
                query = f"""
                    SELECT DISTINCT resource_id, resource_id as resource_name
                    FROM {schema_name}.gold_aws_resource_dim
                    WHERE service_code = 'AmazonS3'
                      AND resource_id IS NOT NULL
                    ORDER BY resource_id
                    LIMIT 100;
                """

        if not query:
            return []

        try:
            df = pd.read_sql_query(query, conn)
            if df.empty:
                return []
            return df.to_dict('records')
        except Exception as e:
            print(f"Error fetching resource IDs: {e}")
            return []

    # Fetch the resource IDs
    resource_ids = fetch_resource_ids(schema, resource_type, cloud_platform)

    return {
        "status": "success",
        "cloud_platform": cloud_platform,
        "resource_type": resource_type,
        "schema_name": schema,
        "resource_ids": resource_ids,
        "count": len(resource_ids)
    }