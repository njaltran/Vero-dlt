"""dlt source definitions for the Contoso retail dataset (CSV files).

Note: dlt's default snake_case normalizer converts PascalCase CSV headers:
  OrderKey → order_key, CustomerKey → customer_key, etc.
Primary keys must match the normalized names.
"""

import os
import dlt
import pandas as pd

CSV_DIR = os.path.join(os.path.dirname(__file__), "db", "init", "data")


def _csv_resource(filename: str, name: str, primary_key):
    @dlt.resource(name=name, write_disposition="replace", primary_key=primary_key)
    def _load():
        df = pd.read_csv(os.path.join(CSV_DIR, filename), low_memory=False)
        yield df.to_dict(orient="records")

    return _load()


def get_sources():
    # dlt normalizes PascalCase → snake_case, so primary keys use snake_case
    dim_date = _csv_resource("date.csv", "dim_date", "date")
    currencyexchange = _csv_resource(
        "currencyexchange.csv",
        "currencyexchange",
        ["date", "from_currency", "to_currency"],
    )
    customer = _csv_resource("customer.csv", "dim_customer", "customer_key")
    store = _csv_resource("store.csv", "dim_store", "store_key")
    product = _csv_resource("product.csv", "dim_product", "product_key")
    orders = _csv_resource("orders.csv", "orders", "order_key")
    orderrows = _csv_resource(
        "orderrows.csv", "orderrows", ["order_key", "line_number"]
    )
    sales = _csv_resource(
        "sales.csv", "fact_sales", ["order_key", "line_number"]
    )

    return [
        dim_date,
        currencyexchange,
        customer,
        store,
        product,
        orders,
        orderrows,
        sales,
    ]
