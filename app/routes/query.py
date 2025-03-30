# app/routes/query.py
import re
import sqlalchemy
from sqlalchemy import text
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app.routes.auth import get_current_user, create_dynamic_database_for_user
from app.models import User
from app.utils.llm_helpers import (
    classify_user_query_llm,
    get_special_prompt,
    GoogleGenerativeAI
)
# Locally define generate_dynamic_response since it's not imported.
def generate_dynamic_response(user_query: str, column_name: str, value) -> str:
    prompt = f"""You are an expert data analysis assistant.
The user asked: "{user_query}".
The result computed from the data for the column "{column_name}" is {value}.
Generate a friendly and natural language response that answers the user's query,
making sure the response reflects the full context of the query.
For example, if the query was "Total admission of Bhopal district", your answer could be "Total admission of Bhopal district is {value}."
"""
    dynamic_response = llm(prompt)
    return dynamic_response.strip()
 
from app.utils.sql_helpers import enhance_user_query, generate_sql_query, execute_sql_query
from app.utils.data_processing import generate_detailed_overview_in_memory
from app.config import MODEL_NAME, GOOGLE_API_KEY, DATABASE_URI, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST
from app.state import state
from app.database import get_db  # Dependency to get a DB session
 
router = APIRouter()
 
class UserQuery(BaseModel):
    query: str
 
# Initialize the LLM instance.
llm = GoogleGenerativeAI(model=MODEL_NAME, api_key=GOOGLE_API_KEY)
 
def is_advanced_sql_query(query: str) -> bool:
    """
    Dynamically detect advanced SQL query indicators.
    Checks for keywords such as "top", "group by", "order by", "limit",
    aggregate functions, joins, CTEs, window functions, and ranking functions.
    """
    advanced_keywords = [
        r'\btop\s+\d+',        
        r'\bgroup\s+by\b',      
        r'\border\s+by\b',      
        r'\blimit\b',          
        r'\bsum\s*\(',        
        r'\bavg\s*\(',        
        r'\bcount\s*\(',      
        r'\bmax\s*\(',        
        r'\bmin\s*\(',        
        r'\bjoin\b',          
        r'\bwith\b',          
        r'\bover\s*\(',        
        r'\brow_number\s*\(',  
        r'\brank\s*\(',        
        r'\bdense_rank\s*\('  
    ]
    for pattern in advanced_keywords:
        if re.search(pattern, query, flags=re.IGNORECASE):
            return True
    return False
 
 
def dynamic_classify_query(user_query: str, llm: GoogleGenerativeAI) -> str:
    """
    Dynamically classify the user's query by asking the LLM to decide if the query
    should be executed as SQL (direct data retrieval) or treated as a summary/analysis.
    The LLM is instructed to respond with one word: SQL, SUMMARY, or ANALYSIS.
    """
    prompt = f"""
You are an expert query classifier. Given the following user query:
"{user_query}"
Decide if this query is intended for direct data retrieval using SQL or if it is meant for summary or analysis.
Respond with one of these words only: SQL, SUMMARY, or ANALYSIS.
Consider that queries requesting aggregates or metrics (such as totals, averages, counts, etc.) should be classified as SQL.
"""
    response = llm(prompt)
    classification = response.strip().upper()
    if classification not in ["SQL", "SUMMARY", "ANALYSIS"]:
        # Fallback to the existing classifier if the dynamic response is unclear.
        classification = classify_user_query_llm(user_query, llm)
    return classification
 
 
@router.post("/execute_query")
def execute_user_query(
    user_query: UserQuery,
    current_user: User = Depends(get_current_user),
    db: sqlalchemy.orm.Session = Depends(get_db)
):
    if not state.get("table_names"):
        raise HTTPException(status_code=400, detail="No tables available. Please upload and save your data first.")
 
    # Determine which connection to use.
    if state.get("personal_engine"):
        user_engine = state["personal_engine"]
        source = "personal"
    else:
        if not current_user.dynamic_db:
            dynamic_db_name = create_dynamic_database_for_user(current_user)
            current_user.dynamic_db = dynamic_db_name
            db.commit()
        try:
            user_engine = sqlalchemy.create_engine(
                f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{current_user.dynamic_db}"
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error creating dynamic database connection: {e}")
        source = "dynamic"
 
    # For dynamic DBs, verify that expected tables exist.
    if source == "dynamic":
        try:
            with user_engine.connect() as connection:
                result = connection.execute(text("SHOW TABLES;"))
                available_tables = [t[0] for t in result.fetchall()]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error checking available tables: {e}")
 
        expected_tables = [name for name, _ in state["table_names"]]
        missing = [tbl for tbl in expected_tables if tbl not in available_tables]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Tables {missing} are not found in your dynamic database. Please confirm cleaning or cancel cleaning to save your data."
            )
 
    # --- EARLY METRIC CHECK (OPTIONAL) ---
    user_query_lower = user_query.query.lower()
    expected_metrics = []
    if "sales" in user_query_lower:
        expected_metrics.append("sales")
    if "admission" in user_query_lower:
        expected_metrics.append("admission")
    if expected_metrics:
        available_columns = set()
        for _, df in state["table_names"]:
            available_columns.update(col.lower() for col in df.columns)
        for metric in expected_metrics:
            if not any(metric in col for col in available_columns):
                return {
                    "result": f"Requested metric '{metric}' not found in available columns. Please check your query or available data."
                }
    # -----------------------------------------------------
 
    # Use advanced SQL detection first; if not detected, use the dynamic classifier.
    if is_advanced_sql_query(user_query.query):
        classification = "SQL"
    else:
        classification = dynamic_classify_query(user_query.query, llm)
        if classification not in ["SQL", "SUMMARY", "ANALYSIS"]:
            classification = "SQL"
 
    if classification == "SQL":
        schema_info = "\n".join(
            [f"Table: {name}, Columns: {', '.join(df.columns)}" for name, df in state["table_names"]]
        )
        enhanced_query = enhance_user_query(user_query.query, state["table_names"])
        dialect = None  # Optionally detect dialect.
        sql_query, optimizations = generate_sql_query(
            enhanced_query, schema_info, [], llm, state["table_names"], dialect=dialect
        )
       
        # For ranking queries: if no ORDER BY or LIMIT is present, re-generate with additional instruction.
        if re.search(r'\btop\s+\d+', user_query.query.lower()):
            sql_lower = sql_query.lower()
            if "order by" not in sql_lower and "limit" not in sql_lower:
                additional_instruction = "Ensure the query returns only the top results using ORDER BY and LIMIT."
                sql_query, optimizations = generate_sql_query(
                    enhanced_query + " " + additional_instruction,
                    schema_info, [], llm, state["table_names"], dialect=dialect
                )
        try:
            result_df = execute_sql_query(sql_query, user_query.query, user_engine)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error executing SQL: {e}")
       
        if result_df.empty:
            return {
                "sql_query": sql_query,
                "optimizations": optimizations,
                "result": "No matching data found for your query. Please adjust your filters or try a different query."
            }
       
        if result_df.shape == (1, 1):
            column_name = list(result_df.columns)[0]
            value = result_df.iloc[0, 0]
            result_response = generate_dynamic_response(user_query.query, column_name, value)
        else:
            result_response = result_df.to_dict(orient="records")
       
        return {"sql_query": sql_query, "optimizations": optimizations, "result": result_response}
   
    elif classification == "SUMMARY":
        overview = generate_detailed_overview_in_memory(state["table_names"])
        special_instructions = get_special_prompt("SUMMARY")
        prompt = f"""
User asked for a summary: "{user_query.query}"
 
Data Overview:
{overview}
 
Follow these instructions when summarizing:
{special_instructions}
"""
        summary_response = llm(prompt)
        return {"summary": summary_response}
   
    else:  # ANALYSIS
        overview = generate_detailed_overview_in_memory(state["table_names"])
        prompt = f"""
You are an AI data analyst. The user asked: "{user_query.query}"
 
Data Overview:
{overview}
 
Provide insights, trends, and actionable recommendations.
"""
        analysis_response = llm(prompt)
        return {"analysis": analysis_response}
 
 