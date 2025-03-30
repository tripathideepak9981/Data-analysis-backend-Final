# app/routes/db.py
from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel
from typing import List
from app.utils.db_helpers import connect_personal_db, list_tables, disconnect_database
from app.state import state
from fastapi.encoders import jsonable_encoder
import logging
import pandas as pd
import math
 
router = APIRouter()
logger = logging.getLogger("db")
logger.setLevel(logging.DEBUG)
 
class DBConnectionParams(BaseModel):
    db_type: str
    host: str
    port: int
    user: str
    password: str
    database: str
 
def clean_nan(obj):
    """
    Recursively traverse lists and dictionaries, replacing any float('nan') with None.
    """
    if isinstance(obj, list):
        return [clean_nan(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: clean_nan(value) for key, value in obj.items()}
    elif isinstance(obj, float):
        if math.isnan(obj):
            return None
        else:
            return obj
    else:
        return obj
 
@router.post("/connect_db")
def connect_db(params: DBConnectionParams):
    engine = connect_personal_db(
        params.db_type,
        params.host,
        params.user,
        params.password,
        params.database,
        params.port
    )
    if engine is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")
    state["personal_engine"] = engine
    tables = list_tables(engine)
    logger.info(f"Connected. Available tables: {tables}")
    return jsonable_encoder({"status": "connected", "tables": tables})
 
@router.post("/load_tables")
def load_tables(table_names: List[str] = Body(...)):
    if not state.get("personal_engine"):
        raise HTTPException(status_code=400, detail="No personal database connected.")
   
    engine = state["personal_engine"]
    previews = {}
    loaded_tables = []  # We keep track of fetched data (for potential later use)
 
    for table in table_names:
        try:
            query = f"SELECT * FROM `{table}`;"
            df = pd.read_sql_query(query, engine)
            loaded_tables.append((table, df))
            logger.info(f"Fetched table '{table}' with shape: {df.shape}")
           
            # Generate preview from the raw data: first 10 rows.
            if df.empty:
                logger.warning(f"Table {table} is empty.")
                previews[table] = "No data available (table is empty)."
            else:
                # Convert preview to list of dictionaries and clean NaN values.
                preview_data = df.head(10).to_dict(orient="records")
                preview_data = clean_nan(preview_data)
                previews[table] = preview_data if preview_data else "No preview data available."
            logger.info(f"Preview for '{table}': {previews[table]}")
        except Exception as e:
            logger.error(f"Error fetching data for table '{table}': {e}")
            previews[table] = f"Error fetching data: {e}"
   
    # Optionally store loaded_tables in state
    state["table_names"] = loaded_tables
 
    response = {
        "status": "tables loaded",
        "tables": table_names,
        "previews": previews
    }
    logger.info(f"Final Response: {response}")
    return jsonable_encoder(response)
 
@router.post("/disconnect")
def disconnect():
    disconnect_database()
    return jsonable_encoder({"status": "disconnected"})
 
 