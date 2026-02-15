"""FastAPI server exposing the BSL semantic model as a REST API."""

import sys
import os

# Add repo root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from constants import PIPELINE_NAME
from semantics.model import create_semantic_model
from semantics.query_builder import (
    QueryRequest as SemanticQueryRequest,
    FilterCondition as SemanticFilterCondition,
    build_semantic_query,
)
from downstream_apps.api.models import QueryRequest, JsonDataResponse

from fastapi import FastAPI
import dlt
import uvicorn


app = FastAPI(title="Vero Semantic Layer API", version="0.1.0")

# Initialize semantic model on startup
pipeline = dlt.attach(pipeline_name=PIPELINE_NAME)
semantic_model = create_semantic_model(pipeline)


@app.get("/dimensions")
def get_dimensions():
    """List all available dimensions."""
    return {
        "dimensions": [
            {"name": name, "title": name.replace("_", " ").title()}
            for name in semantic_model.dimensions
        ]
    }


@app.get("/measures")
def get_measures():
    """List all available measures."""
    return {
        "measures": [
            {"name": name, "title": name.replace("_", " ").title()}
            for name in semantic_model.measures
        ]
    }


@app.post("/query", response_model=JsonDataResponse)
def execute_query(query: QueryRequest):
    """Execute a semantic query and return results as JSON."""
    semantic_query = SemanticQueryRequest(
        measures=query.measures,
        dimensions=query.dimensions,
        filters=[
            SemanticFilterCondition(
                field=f.field, operator=f.operator, value=f.value
            )
            for f in query.filters
        ],
        limit=query.limit,
        offset=query.offset,
    )

    result = build_semantic_query(semantic_model, semantic_query)
    df = result.execute()

    if query.offset and query.offset > 0:
        df = df.iloc[query.offset:]
    if query.limit and query.limit > 0:
        df = df.head(query.limit)

    data = df.to_dict(orient="records")
    return JsonDataResponse(data=data, row_count=len(data))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
