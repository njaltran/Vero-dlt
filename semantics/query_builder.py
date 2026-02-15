"""Query builder for BSL semantic model queries.

Translates query requests (measures, dimensions, filters, time dimensions)
into executable BSL/Ibis queries.
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


def build_single_filter(dim_obj, op: str, value: str):
    """Build an Ibis filter predicate from a dimension object, operator, and value."""
    if op == "=":
        return lambda t, d=dim_obj, v=value: d(t) == v
    elif op == "!=":
        return lambda t, d=dim_obj, v=value: d(t) != v
    elif op == ">":
        return lambda t, d=dim_obj, v=value: d(t) > v
    elif op == ">=":
        return lambda t, d=dim_obj, v=value: d(t) >= v
    elif op == "<":
        return lambda t, d=dim_obj, v=value: d(t) < v
    elif op == "<=":
        return lambda t, d=dim_obj, v=value: d(t) <= v
    elif op == "contains":
        return lambda t, d=dim_obj, v=value: d(t).contains(v)
    else:
        raise ValueError(f"Unsupported operator: {op}")


def build_semantic_query(model: SemanticModel, query_request: QueryRequest):
    """Build a BSL semantic query from a QueryRequest.

    Returns an unexecuted query object that can be .execute()'d to get a DataFrame.
    """
    query = model

    # Apply filters
    for f in query_request.filters:
        dim_name = f.field.split(".")[-1] if "." in f.field else f.field
        if dim_name in model.dimensions:
            dim_obj = model.dimensions[dim_name]
            filter_fn = build_single_filter(dim_obj, f.operator, f.value)
            query = query.filter(filter_fn)

    # Apply time dimension filters as date range filters
    for td in query_request.timeDimensions:
        dim_name = td.dimension.split(".")[-1] if "." in td.dimension else td.dimension
        if dim_name in model.dimensions and isinstance(td.dateRange, list) and len(td.dateRange) == 2:
            dim_obj = model.dimensions[dim_name]
            query = query.filter(lambda t, d=dim_obj, dr=td.dateRange: d(t) >= dr[0])
            query = query.filter(lambda t, d=dim_obj, dr=td.dateRange: d(t) <= dr[1])

    # Resolve dimension and measure names
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

    # Build aggregation query
    if selected_measures and selected_dims:
        query = query.select(
            dimensions=selected_dims,
            measures=selected_measures,
        )
    elif selected_measures:
        query = query.select(measures=selected_measures)
    elif selected_dims:
        query = query.select(dimensions=selected_dims)

    return query
