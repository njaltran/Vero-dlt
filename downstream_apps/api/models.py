"""Pydantic models for the FastAPI semantic layer API."""

from pydantic import BaseModel, Field
from typing import Optional, List, Any


class FilterCondition(BaseModel):
    field: str = Field(..., description="Dimension name to filter on")
    operator: str = Field("=", description="Operator: =, !=, <, <=, >, >=, contains")
    value: str = Field(..., description="Value to compare against")


class QueryRequest(BaseModel):
    measures: List[str] = Field(default_factory=list, description="Measure names")
    dimensions: List[str] = Field(default_factory=list, description="Dimension names")
    filters: List[FilterCondition] = Field(
        default_factory=list, description="Filter conditions"
    )
    limit: Optional[int] = Field(500, description="Max rows to return")
    offset: Optional[int] = Field(0, description="Rows to skip")


class JsonDataResponse(BaseModel):
    data: Any
    row_count: int
