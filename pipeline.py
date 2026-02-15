"""Main dlt pipeline for loading Contoso retail data into DuckDB."""

from constants import PIPELINE_NAME, DATASET_NAME, DESTINATION
from sources import get_sources
import dlt


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=DESTINATION,
        dataset_name=DATASET_NAME,
    )

    sources = get_sources()

    info = pipeline.run(
        sources,
        write_disposition="replace",
        refresh="drop_sources",
    )

    print(info)

    # Print loaded tables
    tables_loaded = [
        table
        for table in pipeline.default_schema.tables.keys()
        if not table.startswith("_dlt")
    ]
    print(f"\nTables loaded: {', '.join(tables_loaded)}")
