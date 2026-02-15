# Semantic Modeling with BSL

Vero uses [BSL](https://github.com/dlt-hub/boring-semantic-layer) (Boring Semantic Layer) to define a semantic model over the loaded data.

## Overview

BSL provides a Python-native semantic layer built on [Ibis](https://ibis-project.org/). It allows you to define:

- **Dimensions**: Descriptive attributes for grouping and filtering (e.g., country, product category, year)
- **Measures**: Aggregation expressions (e.g., total revenue, average order value, profit)
- **Joins**: Star-schema relationships between fact and dimension tables

## Star Schema

```
fact_sales (central fact table)
├── dim_customer (via customerkey)
├── dim_store (via storekey)
├── dim_product (via productkey)
└── dim_date (via orderdate → date)
```

## Measures

| Measure | Expression | Description |
|---------|-----------|-------------|
| `totalRevenue` | SUM(unitprice * quantity) | Gross revenue |
| `netRevenue` | SUM(netprice) | Net revenue after discounts |
| `totalUnitsSold` | SUM(quantity) | Total units sold |
| `totalCost` | SUM(unitcost * quantity) | Total cost of goods |
| `orderCount` | COUNT DISTINCT(orderkey) | Number of unique orders |
| `averageOrderValue` | AVG(netprice) | Average order value |
| `profit` | SUM(netprice - unitcost * quantity) | Net profit |
| `customerCount` | COUNT DISTINCT(customerkey) | Number of unique customers |
| `avgCustomerAge` | AVG(age) | Average customer age |
| `productCount` | COUNT DISTINCT(productkey) | Number of products |
| `storeCount` | COUNT DISTINCT(storekey) | Number of stores |
| `averageStoreSize` | AVG(squaremeters) | Average store size |

## Dimensions

### Sales Dimensions
- `orderdate`, `deliverydate`, `currencycode`, `exchangerate`

### Customer Dimensions
- `surname`, `gender`, `continent`, `country`, `city`, `state`, `company`, `vehicle`, `age`, `birthday`, `occupation`

### Product Dimensions
- `productname`, `manufacturer`, `brand`, `color`, `weight`, `cost`, `price`, `categoryname`, `subcategoryname`

### Store Dimensions
- `storecode`, `countrycode`, `countryname`, `store_state`, `opendate`, `closedate`, `squaremeters`, `status`

### Date Dimensions
- `date`, `year`, `quarter`, `yearmonth`, `month`, `dayofweek`, `workingday`

## Usage

```python
from semantics.model import create_semantic_model
from semantics.query_builder import QueryRequest, build_semantic_query
import dlt

pipeline = dlt.attach(pipeline_name="contoso")
model = create_semantic_model(pipeline)

query = QueryRequest(
    measures=["totalRevenue", "profit"],
    dimensions=["country", "categoryname"],
    limit=10,
)

result = build_semantic_query(model, query)
df = result.execute()
print(df)
```

## Files

- `semantics/model.py` — Builds the full semantic model with dimensions, measures, and joins
- `semantics/table_references.py` — Defines star-schema relationships
- `semantics/query_builder.py` — Query construction with filters, aggregations, and time dimensions
