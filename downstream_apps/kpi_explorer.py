"""Streamlit KPI Explorer for the BSL semantic model."""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from constants import PIPELINE_NAME
from semantics.model import create_semantic_model
from semantics.query_builder import (
    QueryRequest,
    FilterCondition,
    build_semantic_query,
)

import streamlit as st
import dlt


st.set_page_config(page_title="Vero KPI Explorer", layout="wide")
st.title("Vero KPI Explorer")


@st.cache_resource
def get_semantic_model():
    pipeline = dlt.attach(pipeline_name=PIPELINE_NAME)
    return create_semantic_model(pipeline)


semantic_model = get_semantic_model()

dim_names = list(semantic_model.dimensions)
measure_names = list(semantic_model.measures)

# Sidebar: dimension and measure selection
st.sidebar.header("Query Builder")

selected_dims = st.sidebar.multiselect(
    "Dimensions",
    options=dim_names,
    default=[],
    help="Select dimensions to group by",
)

selected_measures = st.sidebar.multiselect(
    "Measures",
    options=measure_names,
    default=measure_names[:1] if measure_names else [],
    help="Select measures to aggregate",
)

# Filters
st.sidebar.header("Filters")
num_filters = st.sidebar.number_input("Number of filters", min_value=0, max_value=10, value=0)

filters = []
for i in range(int(num_filters)):
    col1, col2, col3 = st.sidebar.columns(3)
    with col1:
        field = st.selectbox(f"Field {i+1}", options=dim_names, key=f"filter_field_{i}")
    with col2:
        op = st.selectbox(
            f"Op {i+1}",
            options=["=", "!=", ">", ">=", "<", "<=", "contains"],
            key=f"filter_op_{i}",
        )
    with col3:
        value = st.text_input(f"Value {i+1}", key=f"filter_value_{i}")
    if field and value:
        filters.append(FilterCondition(field=field, operator=op, value=value))

limit = st.sidebar.number_input("Limit", min_value=1, max_value=10000, value=500)

# Execute query
if st.sidebar.button("Run Query", type="primary"):
    if not selected_measures and not selected_dims:
        st.warning("Please select at least one dimension or measure.")
    else:
        query_request = QueryRequest(
            measures=selected_measures,
            dimensions=selected_dims,
            filters=filters,
            limit=limit,
        )

        with st.spinner("Executing query..."):
            result = build_semantic_query(semantic_model, query_request)
            df = result.execute()

            if limit:
                df = df.head(limit)

        st.success(f"Query returned {len(df)} rows")
        st.dataframe(df, use_container_width=True)

        # Download button
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="query_results.csv",
            mime="text/csv",
        )
else:
    st.info("Configure your query in the sidebar and click 'Run Query'.")

# Show model metadata
with st.expander("Available Dimensions"):
    for name in dim_names:
        st.text(f"  - {name}")

with st.expander("Available Measures"):
    for name in measure_names:
        st.text(f"  - {name}")
