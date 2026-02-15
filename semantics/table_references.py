"""Star schema relationship definitions for the Contoso retail dataset.

Schema:
    fact_sales (central fact table)
    ├── dim_customer (via customerkey)
    ├── dim_store (via storekey)
    ├── dim_product (via productkey)
    └── dim_date (via orderdate → date)
"""


def get_semantic_table_references():
    return {
        "fact_sales": [
            {
                "referenced_table": "dim_customer",
                "columns": ["customerkey"],
                "referenced_columns": ["customerkey"],
            },
            {
                "referenced_table": "dim_store",
                "columns": ["storekey"],
                "referenced_columns": ["storekey"],
            },
            {
                "referenced_table": "dim_product",
                "columns": ["productkey"],
                "referenced_columns": ["productkey"],
            },
            {
                "referenced_table": "dim_date",
                "columns": ["orderdate"],
                "referenced_columns": ["date"],
            },
        ],
        "dim_customer": [],
        "dim_store": [],
        "dim_product": [],
        "dim_date": [],
    }
