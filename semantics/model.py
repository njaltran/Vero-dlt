"""BSL semantic model for the Contoso retail dataset.

Translates the Cube.js semantic definitions (sales, customers, products, stores, dates)
into BSL (Boring Semantic Layer) using Ibis expressions.

Note: dlt's snake_case normalizer converts PascalCase CSV column names:
  OrderKey → order_key, UnitPrice → unit_price, CountryName → country_name, etc.
All column references below use the normalized snake_case names.
"""

from constants import PIPELINE_NAME
from semantics.table_references import get_semantic_table_references
from boring_semantic_layer import to_semantic_table, SemanticModel
from typing import Dict, List
import copy
import argparse
import dlt


def _prefix_columns(table, table_name: str):
    """Prefix all non-dlt columns with table name to avoid naming conflicts in joins."""
    rename_map = {
        f"{table_name}__{col}": col
        for col in table.columns
        if col not in ("_dlt_id", "_dlt_load_id")
    }
    return table.rename(**rename_map)


def _build_join_lambda(left_table_name, right_table_name, left_cols, right_cols):
    """Create a join predicate lambda for BSL."""
    def _join(left, right):
        cond = None
        for lc, rc in zip(left_cols, right_cols):
            left_col = f"{left_table_name}__{lc}"
            right_col = f"{right_table_name}__{rc}"
            c = getattr(left, left_col) == getattr(right, right_col)
            cond = c if cond is None else (cond & c)
        return cond

    return _join


def _recursive_semantic_join(
    semantic_table_references_copy,
    semantic_model,
    semantic_model_base,
    referencing_table,
) -> SemanticModel:
    references = semantic_table_references_copy.get(referencing_table, [])

    if not references:
        return semantic_model

    del semantic_table_references_copy[referencing_table]

    for reference in references:
        referenced_table = reference["referenced_table"]
        if referenced_table not in semantic_model_base:
            raise RuntimeError(f"Referenced table '{referenced_table}' was not loaded")

        on_clause = _build_join_lambda(
            referencing_table,
            referenced_table,
            reference["columns"],
            reference["referenced_columns"],
        )

        semantic_model = semantic_model.join_one(
            semantic_model_base[referenced_table],
            on=on_clause,
            how="left",
        )

        semantic_model = _recursive_semantic_join(
            semantic_table_references_copy,
            semantic_model,
            semantic_model_base,
            referenced_table,
        )

    return semantic_model


# -- Dimension and measure definitions per table --
# Column names use dlt's snake_case normalization of the PascalCase CSV headers.
# Prefixed with {table_name}__ for join uniqueness.

SEMANTIC_DEFINITIONS = {
    "fact_sales": {
        "dimensions": {
            "orderdate": lambda t: t.fact_sales__order_date,
            "deliverydate": lambda t: t.fact_sales__delivery_date,
            "currencycode": lambda t: t.fact_sales__currency_code,
            "exchangerate": lambda t: t.fact_sales__exchange_rate,
        },
        "measures": {
            "totalRevenue": lambda t: (t.fact_sales__unit_price * t.fact_sales__quantity).sum(),
            "netRevenue": lambda t: t.fact_sales__net_price.sum(),
            "totalUnitsSold": lambda t: t.fact_sales__quantity.sum(),
            "totalCost": lambda t: (t.fact_sales__unit_cost * t.fact_sales__quantity).sum(),
            "orderCount": lambda t: t.fact_sales__order_key.nunique(),
            "averageOrderValue": lambda t: t.fact_sales__net_price.mean(),
            "profit": lambda t: (t.fact_sales__net_price - t.fact_sales__unit_cost * t.fact_sales__quantity).sum(),
        },
    },
    "dim_customer": {
        "dimensions": {
            "surname": lambda t: t.dim_customer__surname,
            "gender": lambda t: t.dim_customer__gender,
            "continent": lambda t: t.dim_customer__continent,
            "country": lambda t: t.dim_customer__country_full,
            "city": lambda t: t.dim_customer__city,
            "state": lambda t: t.dim_customer__state,
            "company": lambda t: t.dim_customer__company,
            "vehicle": lambda t: t.dim_customer__vehicle,
            "age": lambda t: t.dim_customer__age,
            "birthday": lambda t: t.dim_customer__birthday,
            "occupation": lambda t: t.dim_customer__occupation,
        },
        "measures": {
            "customerCount": lambda t: t.dim_customer__customer_key.nunique(),
            "avgCustomerAge": lambda t: t.dim_customer__age.mean(),
        },
    },
    "dim_product": {
        "dimensions": {
            "productname": lambda t: t.dim_product__product_name,
            "manufacturer": lambda t: t.dim_product__manufacturer,
            "brand": lambda t: t.dim_product__brand,
            "color": lambda t: t.dim_product__color,
            "weight": lambda t: t.dim_product__weight,
            "cost": lambda t: t.dim_product__cost,
            "price": lambda t: t.dim_product__price,
            "categoryname": lambda t: t.dim_product__category_name,
            "subcategoryname": lambda t: t.dim_product__sub_category_name,
        },
        "measures": {
            "productCount": lambda t: t.dim_product__product_key.nunique(),
        },
    },
    "dim_store": {
        "dimensions": {
            "storecode": lambda t: t.dim_store__store_code,
            "countrycode": lambda t: t.dim_store__country_code,
            "countryname": lambda t: t.dim_store__country_name,
            "store_state": lambda t: t.dim_store__state,
            "opendate": lambda t: t.dim_store__open_date,
            "closedate": lambda t: t.dim_store__close_date,
            "squaremeters": lambda t: t.dim_store__square_meters,
            "status": lambda t: t.dim_store__status,
        },
        "measures": {
            "storeCount": lambda t: t.dim_store__store_key.nunique(),
            "averageStoreSize": lambda t: t.dim_store__square_meters.mean(),
        },
    },
    "dim_date": {
        "dimensions": {
            "date": lambda t: t.dim_date__date,
            "year": lambda t: t.dim_date__year,
            "quarter": lambda t: t.dim_date__quarter,
            "yearmonth": lambda t: t.dim_date__year_month,
            "month": lambda t: t.dim_date__month,
            "dayofweek": lambda t: t.dim_date__dayof_week,
            "workingday": lambda t: t.dim_date__working_day,
        },
        "measures": {},
    },
}


def create_semantic_model(pipeline: dlt.Pipeline) -> SemanticModel:
    """Build the full BSL semantic model from the dlt pipeline's loaded data."""
    semantic_table_references = get_semantic_table_references()

    semantic_model_base: Dict[str, SemanticModel] = {}

    for table_name in semantic_table_references.keys():
        defn = SEMANTIC_DEFINITIONS.get(table_name)
        if defn is None:
            continue

        # Get the Ibis table from the dlt pipeline dataset
        table = (
            pipeline.dataset()
            .table(table_name)
            .to_ibis()
        )

        # Drop internal dlt columns if present
        dlt_cols = [c for c in table.columns if c.startswith("_dlt_")]
        if dlt_cols:
            table = table.drop(*dlt_cols)

        # Prefix columns with table name
        table = _prefix_columns(table, table_name)

        st = (
            to_semantic_table(table)
            .with_dimensions(**defn["dimensions"])
            .with_measures(**defn["measures"])
        )

        semantic_model_base[table_name] = st

    # Build relationships via recursive joins starting from fact table
    root = "fact_sales"
    semantic_model = semantic_model_base[root]
    refs_copy = copy.deepcopy(semantic_table_references)

    semantic_model = _recursive_semantic_join(
        refs_copy,
        semantic_model,
        semantic_model_base,
        root,
    )

    return semantic_model


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--pipeline", required=False, type=str)
    args = parser.parse_args()

    pipeline = dlt.attach(
        pipeline_name=args.pipeline if args.pipeline else PIPELINE_NAME,
    )

    semantic_model = create_semantic_model(pipeline)

    print("Dimensions:", semantic_model.dimensions)
    print("Measures:", semantic_model.measures)
