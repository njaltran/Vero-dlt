"""BSL MCP Server — replaces the Cube.js REST API with direct BSL semantic queries."""

from __future__ import annotations
from typing import Literal, Optional, Union
from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent, EmbeddedResource, TextResourceContents
from pydantic import BaseModel, Field
import json
import logging
import uuid
import yaml

from constants import PIPELINE_NAME
from semantics.model import create_semantic_model
from semantics.query_builder import (
    QueryRequest,
    TimeDimension,
    FilterCondition,
    build_semantic_query,
)
import dlt


def data_to_yaml(data) -> str:
    return yaml.dump(data, indent=2, sort_keys=False)


class Query(BaseModel):
    """Query model matching the interface the Agno agent already uses."""

    measures: list[str] = Field([], description="Names of measures to query")
    dimensions: list[str] = Field([], description="Names of dimensions to group by")
    timeDimensions: list[TimeDimension] = Field(
        [], description="Time dimensions to group by"
    )
    filters: list[FilterCondition] = Field([], description="Filters to apply")
    limit: Optional[int] = Field(500, description="Maximum number of rows to return")
    offset: Optional[int] = Field(0, description="Number of rows to skip")
    order: dict[str, Literal["asc", "desc"]] = Field(
        {}, description="Ordering of results"
    )


def main(pipeline_name: str, logger: logging.Logger):
    logger.info("Starting BSL MCP server initialization")
    mcp = FastMCP("Vero")
    logger.info("FastMCP instance created")

    # Attach to the existing dlt pipeline and build semantic model
    logger.info("Attaching to dlt pipeline: %s", pipeline_name)
    pipeline = dlt.attach(pipeline_name=pipeline_name)
    semantic_model = create_semantic_model(pipeline)
    logger.info("Semantic model created successfully")

    # Extract metadata for describe_data
    dim_names = list(semantic_model.dimensions) if hasattr(semantic_model, "dimensions") else []
    measure_names = list(semantic_model.measures) if hasattr(semantic_model, "measures") else []
    logger.info("Model has %d dimensions and %d measures", len(dim_names), len(measure_names))

    @mcp.resource("context://data_description")
    def data_description() -> str:
        """Describe the data available in the semantic model."""
        logger.info("Resource 'context://data_description' called")

        description = [
            {
                "name": "contoso_sales",
                "title": "Contoso Sales Analytics",
                "description": "Unified semantic model for Contoso retail sales data with customers, products, stores, and dates.",
                "dimensions": [
                    {"name": name, "title": name.replace("_", " ").title()}
                    for name in dim_names
                ],
                "measures": [
                    {"name": name, "title": name.replace("_", " ").title()}
                    for name in measure_names
                ],
            }
        ]

        yaml_desc = yaml.dump(description, indent=2, sort_keys=True)
        logger.info("Data description generated")
        return (
            "Here is a description of the data available via the read_data tool:\n\n"
            + yaml_desc
        )

    @mcp.tool("describe_data")
    def describe_data() -> str:
        """Describe the data available in the semantic model."""
        logger.info("Tool 'describe_data' invoked")
        description_text = data_description()
        return {"type": "text", "text": description_text}

    @mcp.tool("read_data")
    def read_data(query: Query) -> str:
        """Read data from the semantic model."""
        try:
            logger.info("Tool 'read_data' invoked with query: %s", query)

            # Convert Query to QueryRequest for the query builder
            query_request = QueryRequest(
                measures=query.measures,
                dimensions=query.dimensions,
                filters=query.filters,
                timeDimensions=query.timeDimensions,
                limit=query.limit,
                offset=query.offset,
                order=query.order,
            )

            result = build_semantic_query(semantic_model, query_request)

            # Execute the query to get a pandas DataFrame
            df = result.execute()
            logger.info("Query returned %d rows", len(df))

            # Apply limit/offset
            if query.offset and query.offset > 0:
                df = df.iloc[query.offset :]
            if query.limit and query.limit > 0:
                df = df.head(query.limit)

            data = df.to_dict(orient="records")

            data_id = str(uuid.uuid4())

            @mcp.resource(f"data://{data_id}")
            def data_resource() -> str:
                return json.dumps(data, default=str)

            output = {
                "type": "data",
                "data_id": data_id,
                "data": data,
            }
            yaml_output = data_to_yaml(output)
            json_output = json.dumps(output, default=str)
            logger.info("Tool 'read_data' completed successfully")
            return [
                TextContent(type="text", text=yaml_output),
                EmbeddedResource(
                    type="resource",
                    resource=TextResourceContents(
                        uri=f"data://{data_id}",
                        text=json_output,
                        mimeType="application/json",
                    ),
                ),
            ]

        except Exception as e:
            logger.exception("Error in read_data: %s", str(e))
            return f"Error: {str(e)}"

    exposed_services = [
        "Resource: context://data_description",
        "Tool: describe_data",
        "Tool: read_data",
    ]
    logger.info("Exposing the following service endpoints:")
    for service in exposed_services:
        logger.info("  - %s", service)

    logger.info("Starting BSL MCP server")

    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = 9000

    mcp.run(transport="sse")
