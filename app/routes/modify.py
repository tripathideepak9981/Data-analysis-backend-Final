# app/routes/modify.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.state import state
from app.utils.llm_helpers import translate_natural_language_to_sql, GoogleGenerativeAI
from app.utils.sql_helpers import execute_sql_query
from app.utils.db_helpers import refresh_tables
import sqlalchemy

router = APIRouter()

class ModificationRequest(BaseModel):
    command: str

# Initialize LLM instance
from app.config import GOOGLE_API_KEY
llm = GoogleGenerativeAI(model="gemini-pro", api_key=GOOGLE_API_KEY)


@router.post("/modify_data")
def modify_data(request: ModificationRequest):
    if not state.get("table_names"):
        raise HTTPException(status_code=400, detail="No tables available.")
    schema_info = "\n".join([f"Table: {name}, Columns: {', '.join(df.columns)}" for name, df in state["table_names"]])
    sql_query = translate_natural_language_to_sql(request.command, schema_info, llm)
    connection = state.get("personal_engine")
    try:
        if hasattr(connection, "cursor"):
            cursor = connection.cursor(buffered=True)
            try:
                cursor.execute(sql_query)
                connection.commit()
            finally:
                cursor.close()
        else:
            with connection.begin():
                connection.execute(sqlalchemy.text(sql_query))
        refresh_tables(connection, state["table_names"], state["original_table_names"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing modification: {e}")
    return {"status": "modification executed", "sql_query": sql_query}
