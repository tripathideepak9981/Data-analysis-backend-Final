# app/routes/chart.py

from fastapi import APIRouter, HTTPException, Query

from pydantic import BaseModel

from app.state import state  # state is a dict

from app.utils.sql_helpers import enhance_user_query, generate_sql_query, execute_sql_query

from app.utils.llm_helpers import GoogleGenerativeAI

from app.config import MODEL_NAME, GOOGLE_API_KEY

import sqlalchemy

import pandas as pd

import logging

import duckdb
 
router = APIRouter()

logger = logging.getLogger("chart")

logger.setLevel(logging.INFO)
 
class ChartQuery(BaseModel):

    query: str

    chart_type: str = "bar"  # Frontend will decide final chart style (bar, line, pie, etc.)
 
# Initialize the LLM instance for query processing

llm = GoogleGenerativeAI(model=MODEL_NAME, api_key=GOOGLE_API_KEY)
 
@router.post("/chart")

def generate_chart(

    chart_query: ChartQuery,

    page: int = Query(1, ge=1),

    page_size: int = Query(100, ge=1, le=1000)

):

    # Ensure that table data is available

    if not state["table_names"]:

        raise HTTPException(status_code=400, detail="No tables available for charting.")
 
    # Determine the data source; if not explicitly set, default to "file"

    source = state.get("source", "file")
 
    # For personal DB, expect state["personal_engine"] to be set.

    if source == "personal":

        connection = state.get("personal_engine")

        if connection is None:

            raise HTTPException(status_code=500, detail="Database connection is not available. Please load data via DB connection.")

    else:

        # For file-based data, we'll use duckdb.

        connection = None
 
    # Build a schema info string from the loaded tables (only the uploaded/selected ones)

    schema_info = "\n".join(

        [f"Table: {name}, Columns: {', '.join(df.columns)}" for name, df in state["table_names"]]

    )
 
    # Enhance the user query (map friendly names to actual table/column names)

    enhanced_query = enhance_user_query(chart_query.query, state["table_names"])

    dialect = None  # Optionally set dialect if needed
 
    try:

        # Generate SQL query using the LLM helper

        sql_query, _ = generate_sql_query(enhanced_query, schema_info, [], llm, state["table_names"], dialect=dialect)

        logger.info(f"Generated SQL for chart: {sql_query}")
 
        # Execute the SQL query based on data source

        if source == "personal":

            result_df = execute_sql_query(sql_query, chart_query.query, connection)

        else:

            # Use duckdb to execute the SQL on the uploaded DataFrames

            con = duckdb.connect(database=':memory:')

            for table_name, df in state["table_names"]:

                con.register(table_name, df)

            result_df = con.execute(sql_query).df()

    except Exception as e:

        logger.error(f"Error executing SQL query for chart: {e}")

        raise HTTPException(status_code=500, detail=f"Error executing SQL for chart: {e}")
 
    if result_df.empty:

        raise HTTPException(status_code=400, detail="Query returned no data for charting.")
 
    # Auto-infer chart data: assume first column is the dimension (labels) and the rest are measures.

    cols = list(result_df.columns)

    if len(cols) < 2:

        raise HTTPException(status_code=400, detail="Query returned insufficient columns for charting.")

    labels = result_df.iloc[:, 0].tolist()

    measure_cols = cols[1:]

    if len(measure_cols) == 1:

        data = result_df[measure_cols[0]].tolist()

        multi_value = False

    else:

        data = {col: result_df[col].tolist() for col in measure_cols}

        multi_value = True
 
    # Return only the essential data for chart rendering.

    response = {

        "chart_type": chart_query.chart_type,  # Frontend decides the final chart type

        "labels": labels,

        "data": data,

        "multi_value": multi_value  # True if multiple measures exist (enable multi-bar charts)

    }

    return response

 