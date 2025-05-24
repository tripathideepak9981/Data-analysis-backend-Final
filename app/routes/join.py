from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from app.state import state
import pandas as pd
import uuid
 
router = APIRouter()
 
class JoinRequest(BaseModel):
    table1: str
    table2: str
    join_column1: str
    join_column2: str
    join_type: str  # "INNER", "LEFT", "RIGHT", "FULL OUTER"
    new_table_name: Optional[str] = None
    select_columns: Optional[List[str]] = Field(default=None, description="Optional: Columns to include in result (use exact joined column names)")
    limit: Optional[int] = Field(default=10, description="Number of rows in preview (default 10)")
 
@router.post("/join_tables")
def join_tables(request: JoinRequest):
    # Get loaded tables as a dictionary
    tables = dict(state.get("table_names", []))
 
    # Validate presence of tables
    if request.table1 not in tables or request.table2 not in tables:
        raise HTTPException(status_code=400, detail="Selected tables not available.")
 
    df1, df2 = tables[request.table1], tables[request.table2]
 
    # Validate join columns
    if request.join_column1 not in df1.columns or request.join_column2 not in df2.columns:
        raise HTTPException(
            status_code=400,
            detail=f"Join columns not found: {request.join_column1} in {request.table1} or {request.join_column2} in {request.table2}"
        )
 
    # Map join type to pandas
    join_type_map = {
        "INNER": "inner",
        "LEFT": "left",
        "RIGHT": "right",
        "FULL OUTER": "outer"
    }
    jt = request.join_type.strip().upper()
    if jt not in join_type_map:
        raise HTTPException(status_code=400, detail=f"Invalid join type. Allowed: {', '.join(join_type_map.keys())}")
    pandas_join_type = join_type_map[jt]
 
    # Add suffixes to overlapping columns (except join keys)
    suffixes = (f'_{request.table1}', f'_{request.table2}')
 
    try:
        joined_df = pd.merge(
            df1, df2,
            left_on=request.join_column1, right_on=request.join_column2,
            how=pandas_join_type,
            suffixes=suffixes
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error joining tables: {e}")
 
    # Show ALL columns if select_columns is None or empty
    if request.select_columns is not None and len(request.select_columns) > 0:
        valid_cols = []
        for col in request.select_columns:
            if col in joined_df.columns:
                valid_cols.append(col)
            else:
                alt = col.replace('.', '_')
                if alt in joined_df.columns:
                    valid_cols.append(alt)
        if not valid_cols:
            raise HTTPException(status_code=400, detail="No valid select_columns found in joined table.")
        result_df = joined_df[valid_cols]
    else:
        result_df = joined_df  # Show all columns
 
    # Preview: top N rows (default 10)
    preview_df = result_df.head(request.limit or 10)
    columns = list(preview_df.columns)
    preview = preview_df.to_dict(orient="records")
 
    # Unique name for the joined table
    if request.new_table_name:
        joined_name = request.new_table_name
    else:
        joined_name = f"joined_{request.table1}_{request.table2}_{str(uuid.uuid4())[:8]}"
 
    # Remove any old table with this name, then add new one
    state["table_names"] = [(n, df) for n, df in state["table_names"] if n != joined_name]
    state["table_names"].append((joined_name, joined_df))
 
    # ************** THIS IS THE ONLY NEW BLOCK YOU NEED TO ADD *************
    # Also save to the user's database if using DB mode (so query endpoint works!)
    engine = state.get("personal_engine")
    if engine:
        try:
            joined_df.to_sql(joined_name, engine, index=False, if_exists="replace")
        except Exception as e:
            print(f"Warning: Failed to save joined table in DB: {e}")
    # ***********************************************************************
 
    return {
        "joined_table_name": joined_name,
        "columns": columns,
        "preview": preview,
        "row_count": len(joined_df),
        "message": f"Join successful! You can now query this joined table as '{joined_name}'."
    }
 
# Endpoint to list all loaded tables and their columns for frontend dropdowns
@router.get("/available_tables")
def available_tables():
    return [
        {
            "table_name": t,
            "columns": list(df.columns),
            "row_count": len(df)
        } for t, df in state["table_names"]
    ]
 
 