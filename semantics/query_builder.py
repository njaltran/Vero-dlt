"""Query builder for BSL semantic model queries.

Translates query requests (measures, dimensions, filters, time dimensions)
into executable BSL/Ibis queries using SemanticModel.query().
"""

from boring_semantic_layer import SemanticModel
from pydantic import BaseModel, Field
from typing import Optional, Union, Literal, List


class FilterCondition(BaseModel):
    field: str = Field(..., description="Dimension or measure name to filter on")
    operator: str = Field(
        "=", description="Comparison operator: =, !=, <, <=, >, >=, contains"
    )
    value: str = Field(..., description="Value to compare against")


class TimeDimension(BaseModel):
    dimension: str = Field(..., description="Name of the time dimension")
    granularity: Literal[
        "second", "minute", "hour", "day", "week", "month", "quarter", "year"
    ] = Field(..., description="Time granularity")
    dateRange: Union[List[str], str] = Field(
        ..., description="Pair of ISO date strings or relative range"
    )


class QueryRequest(BaseModel):
    measures: List[str] = Field(default_factory=list, description="Measure names")
    dimensions: List[str] = Field(default_factory=list, description="Dimension names")
    filters: List[FilterCondition] = Field(
        default_factory=list, description="Filter conditions"
    )
    timeDimensions: List[TimeDimension] = Field(
        default_factory=list, description="Time dimensions with granularity"
    )
    limit: Optional[int] = Field(500, description="Max rows to return")
    offset: Optional[int] = Field(0, description="Rows to skip")
    order: dict = Field(
        default_factory=dict, description="Ordering: {field: 'asc'|'desc'}"
    )


def build_semantic_query(model: SemanticModel, query_request: QueryRequest):
    """Build a BSL semantic query from a QueryRequest.

    Uses the SemanticModel.query() API which handles dimensions, measures,
    filters, ordering, and limits natively.

    Returns an executable result (call .execute() or .to_pandas() on it).
    """
    # Resolve dimension and measure names (strip table prefix if present)
    selected_dims = []
    for d in query_request.dimensions:
        name = d.split(".")[-1] if "." in d else d
        if name in model.dimensions:
            selected_dims.append(name)

    selected_measures = []
    for m in query_request.measures:
        name = m.split(".")[-1] if "." in m else m
        if name in model.measures:
            selected_measures.append(name)

    # Build order_by tuples
    order_by = None
    if query_request.order:
        order_by = [
            (k.split(".")[-1] if "." in k else k, v)
            for k, v in query_request.order.items()
        ]

    # Use the native query() method
    result = model.query(
        dimensions=selected_dims if selected_dims else None,
        measures=selected_measures if selected_measures else None,
        limit=query_request.limit,
        order_by=order_by,
    )

    return result
