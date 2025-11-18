import sys
import os
import json
from typing import Optional, Union, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from tortoise.exceptions import DoesNotExist
from app.models.project import Project
from datetime import datetime
from app.core.db import get_db_connection

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
# Get Resource IDs by Type (for dropdown population)
# ---------------------------------------------------------
@router.get("/{cloud_platform}/{project_id}/resources/{resource_type}", status_code=200)
async def get_resource_ids(
    cloud_platform: str,
    project_id: str,
    resource_type: str,
    schema_name: Optional[str] = None
) -> List[dict]:
    """
    Fetch resource IDs for a specific resource type to populate the dropdown.
    Returns a list of {resource_id, resource_name} objects.
    """
    schema = await _resolve_schema_name(project_id, schema_name)

    # Map resource types to table names and columns
    resource_mapping = {
        "azure": {
            "vm": {
                "table": "dim_virtual_machine",
                "id_column": "resource_id",
                "name_column": "vm_name"
            },
            "storage": {
                "table": "dim_storage_account",
                "id_column": "resource_id",
                "name_column": "storage_account_name"
            }
        },
        "aws": {
            "s3": {
                "table": "dim_s3_bucket",
                "id_column": "bucket_arn",
                "name_column": "bucket_name"
            },
            "ec2": {
                "table": "dim_ec2_instance",
                "id_column": "instance_id",
                "name_column": "instance_name"
            }
        }
    }

    # Get the table info for this resource type
    if cloud_platform not in resource_mapping:
        raise HTTPException(status_code=400, detail=f"Unsupported cloud platform: {cloud_platform}")

    if resource_type not in resource_mapping[cloud_platform]:
        raise HTTPException(status_code=400, detail=f"Unsupported resource type: {resource_type}")

    config = resource_mapping[cloud_platform][resource_type]
    table_name = f"{schema}.{config['table']}"
    id_col = config['id_column']
    name_col = config['name_column']

    # Query the database
    conn = await get_db_connection()
    try:
        query = f"""
            SELECT DISTINCT
                {id_col} as resource_id,
                {name_col} as resource_name
            FROM {table_name}
            WHERE {id_col} IS NOT NULL
            ORDER BY {name_col}
            LIMIT 1000
        """

        rows = await conn.fetch(query)

        result = [
            {
                "resource_id": row['resource_id'],
                "resource_name": row['resource_name'] or row['resource_id']
            }
            for row in rows
        ]

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch resources: {str(e)}")
    finally:
        await conn.close()