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
    from app.ingestion.aws.llm_ec2_vpc_integration import run_llm_analysis as run_llm_analysis_ec2_vpc
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

    # Route based on resource type
    resource_type_lower = payload.resource_type.lower().strip()

    if resource_type_lower == 's3':
        result = run_llm_analysis_s3(
            resource_type=payload.resource_type,
            schema_name=schema,
            start_date=payload.start_date,
            end_date=payload.end_date,
            resource_id=payload.resource_id
        )
    elif resource_type_lower in ['ec2', 'vpc']:
        result = run_llm_analysis_ec2_vpc(
            resource_type=payload.resource_type,
            schema_name=schema,
            start_date=payload.start_date,
            end_date=payload.end_date,
            resource_id=payload.resource_id
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported AWS resource type: {payload.resource_type}. Supported types: s3, ec2, vpc"
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