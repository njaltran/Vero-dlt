"""dlt source definitions for the Contoso retail dataset (CSV files)."""

import os
import dlt
import pandas as pd

CSV_DIR = os.path.join(os.path.dirname(__file__), "db", "init", "data")


def _csv_resource(filename: str, name: str, primary_key):
    @dlt.resource(name=name, write_disposition="replace", primary_key=primary_key)
    def _load():
        df = pd.read_csv(os.path.join(CSV_DIR, filename))
        yield df.to_dict(orient="records")

    return _load()


def get_sources():
    dim_date = _csv_resource("date.csv", "dim_date", "date")
    currencyexchange = _csv_resource(
        "currencyexchange.csv",
        "currencyexchange",
        ["date", "from_currency", "to_currency"],
    )
    customer = _csv_resource("customer.csv", "dim_customer", "customerkey")
    store = _csv_resource("store.csv", "dim_store", "storekey")
    product = _csv_resource("product.csv", "dim_product", "productkey")
    orders = _csv_resource("orders.csv", "orders", "orderkey")
    orderrows = _csv_resource(
        "orderrows.csv", "orderrows", ["orderkey", "linenumber"]
    )
    sales = _csv_resource("sales.csv", "fact_sales", ["orderkey", "linenumber"])

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
