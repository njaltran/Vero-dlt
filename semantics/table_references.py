"""Star schema relationship definitions for the Contoso retail dataset.

Column names use dlt's snake_case normalization:
  CustomerKey → customer_key, StoreKey → store_key, etc.

Schema:
    fact_sales (central fact table)
    ├── dim_customer (via customer_key)
    ├── dim_store (via store_key)
    ├── dim_product (via product_key)
    └── dim_date (via order_date → date)
"""


def get_semantic_table_references():
    return {
        "fact_sales": [
            {
                "referenced_table": "dim_customer",
                "columns": ["customer_key"],
                "referenced_columns": ["customer_key"],
            },
            {
                "referenced_table": "dim_store",
                "columns": ["store_key"],
                "referenced_columns": ["store_key"],
            },
            {
                "referenced_table": "dim_product",
                "columns": ["product_key"],
                "referenced_columns": ["product_key"],
            },
            {
                "referenced_table": "dim_date",
                "columns": ["order_date"],
                "referenced_columns": ["date"],
            },
        ],
        "dim_customer": [],
        "dim_store": [],
        "dim_product": [],
        "dim_date": [],
    }
