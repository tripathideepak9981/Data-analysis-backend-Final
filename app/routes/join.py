# app/routes/join.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.state import state
from app.utils.sql_helpers import execute_sql_query
import sqlalchemy
import pandas as pd

router = APIRouter()

class JoinRequest(BaseModel):
    table1: str
    table2: str
    join_column1: str
    join_column2: str
    join_type: str  # e.g., "INNER JOIN", "LEFT JOIN", etc.

@router.post("/join_tables")
def join_tables(request: JoinRequest):
    tables = dict(state.get("table_names", []))
    if request.table1 not in tables or request.table2 not in tables:
        raise HTTPException(status_code=400, detail="Selected tables not available.")
    sql_query = f"""
    SELECT * FROM `{request.table1}`
    {request.join_type} `{request.table2}`
    ON `{request.table1}`.`{request.join_column1}` = `{request.table2}`.`{request.join_column2}`;
    """
    connection = state.get("personal_engine")
    try:
        result_df = execute_sql_query(sql_query, "", connection)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error performing join: {e}")
    result = result_df.to_dict(orient="records") if not result_df.empty else []
    return {"join_sql": sql_query, "result": result}
