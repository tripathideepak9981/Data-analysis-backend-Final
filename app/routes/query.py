# app/routes/query.py
 
import re
import sqlalchemy
from sqlalchemy import text
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app.routes.auth import get_current_user, create_dynamic_database_for_user
from app.models import User
import pandas as pd
from difflib import get_close_matches
 
from app.utils.llm_helpers import (
    classify_user_query_llm,
    get_special_prompt,
    GoogleGenerativeAI
)
from app.utils.sql_helpers import enhance_user_query, generate_sql_query, execute_sql_query
from app.utils.data_processing import generate_detailed_overview_in_memory
from app.config import MODEL_NAME, GOOGLE_API_KEY, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST
from app.state import state, get_duckdb_connection  # get_duckdb_connection returns a persistent DuckDB connection
from app.database import get_db  # Dependency to get a DB session
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
 
router = APIRouter()
 
# Initialize the LLM instance.
llm = GoogleGenerativeAI(model=MODEL_NAME, api_key=GOOGLE_API_KEY)
 
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
 
class UserQuery(BaseModel):
    query: str
 
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
 
 
 
import pandas as pd
import re
from difflib import get_close_matches
from app.state import state  # Assuming you're using app.state
 
def normalize(text: str) -> str:
    """Normalize by lowering, removing spaces, underscores, and special characters"""
    return re.sub(r'[^a-z0-9]', '', text.lower())
 
def handle_statistical_query(user_query: str) -> dict:
    query_lower = user_query.lower()
    combined_df = pd.concat([df for _, df in state["table_names"]], axis=1)
 
    # Build normalized column map
    column_map = {normalize(col): col for col in combined_df.columns}
    all_normalized_columns = list(column_map.keys())
 
    # Detect metrics
    metrics = []
    if "mean" in query_lower or "average" in query_lower:
        metrics.append("mean")
    if "median" in query_lower:
        metrics.append("median")
    if "mode" in query_lower:
        metrics.append("mode")
    if "standard deviation" in query_lower or "std" in query_lower:
        metrics.append("std")
 
    # Step: Extract sliding phrases (3-word max window)
    words = user_query.split()
    candidate_phrases = []
    for i in range(len(words)):
        for j in range(i+1, min(i+4, len(words)+1)):
            phrase = normalize(" ".join(words[i:j]))
            if len(phrase) >= 4:
                candidate_phrases.append(phrase)
 
    # ✅ IMPROVED COLUMN MATCHING
    matched_cols = set()
    used_columns = set()
 
    for phrase in candidate_phrases:
        matches = get_close_matches(phrase, all_normalized_columns, n=3, cutoff=0.85)
        for m in matches:
            actual_col = column_map[m]
            if actual_col not in used_columns:
                matched_cols.add(actual_col)
                used_columns.add(actual_col)
                break  # Only one column per phrase
 
    if not matched_cols or not metrics:
        return {
            "response": "Sorry, I couldn't identify valid metrics or columns. Please rephrase using known statistical terms and column-like keywords."
        }
 
    response_parts = []
    skipped_cols = []
 
    for metric in metrics:
        for col in matched_cols:
            series = combined_df[col].dropna()
            n = len(series)
 
            if n == 0:
                skipped_cols.append((col, "no data"))
                continue
            if not pd.api.types.is_numeric_dtype(series):
                skipped_cols.append((col, "non-numeric"))
                continue
 
            friendly_name = col.replace("_", " ").title()
 
            if metric == "mean":
                total = series.sum()
                mean_val = total / n
                response_parts.append(
                    f"The mean of '{friendly_name}' is {mean_val:.2f} (Formula: sum({friendly_name}) / count({friendly_name}) = {total:.0f} / {n} = {mean_val:.2f})."
                )
 
            elif metric == "median":
                sorted_vals = sorted(series)
                if n % 2 == 1:
                    mid_val = sorted_vals[n // 2]
                    formula = f"Middle value at position {(n // 2) + 1}"
                else:
                    mid_val = (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
                    formula = f"Middle average = ({sorted_vals[n // 2 - 1]} + {sorted_vals[n // 2]}) / 2"
                response_parts.append(
                    f"The median of '{friendly_name}' is {mid_val:.2f} (Formula: {formula})."
                )
 
            elif metric == "mode":
                try:
                    mode_val = series.mode().iloc[0]
                    response_parts.append(
                        f"The mode of '{friendly_name}' is {mode_val} (Formula: Most frequent value in {friendly_name})."
                    )
                except Exception:
                    skipped_cols.append((col, "mode error"))
                    continue
 
            elif metric == "std":
                std_val = series.std()
                mean_val = series.mean()
                squared_diffs = sum((x - mean_val) ** 2 for x in series)
                response_parts.append(
                    f"The standard deviation of '{friendly_name}' is {std_val:.2f} (Formula: sqrt(sum(({friendly_name} - mean({friendly_name}))²) / count({friendly_name})) = sqrt({squared_diffs:.2f} / {n}) = {std_val:.2f})."
                )
 
    for col, reason in skipped_cols:
        friendly_name = col.replace("_", " ").title()
        if reason == "non-numeric":
            response_parts.append(f"Note: The column '{friendly_name}' exists but is non-numeric.")
        elif reason == "no data":
            response_parts.append(f"Note: The column '{friendly_name}' has no usable data.")
 
    if not response_parts:
        return {"response": "No valid numeric columns found for statistical calculations."}
 
    return {"response": " ".join(response_parts)}
 
def dynamic_classify_query(user_query: str, llm: GoogleGenerativeAI) -> str:
    prompt = f"""
You are an expert query classifier. Given the following user query:
"{user_query}"
Decide if this query is intended for direct data retrieval using SQL, or if it is meant for summary, analysis, or statistical computation.
Respond with one of these words only: SQL, SUMMARY, ANALYSIS, or STATISTICAL.
 
Use "STATISTICAL" if the query involves average, mean, median, mode, correlation, standard deviation, or similar terms.
"""
    try:
        response = llm(prompt)
        classification = response.strip().upper()
        if classification not in ["SQL", "SUMMARY", "ANALYSIS", "STATISTICAL"]:
            classification = classify_user_query_llm(user_query, llm)
        return classification
    except Exception:
        return classify_user_query_llm(user_query, llm)
 
 
# --- Caching Functions for LLM Calls ---
@lru_cache(maxsize=100)
def cached_classification(query: str) -> str:
    """
    Cache the classification (SQL, SUMMARY, ANALYSIS) for the user query.
    This avoids repeating expensive LLM calls for the same query.
    """
    return dynamic_classify_query(query, llm)
 
@lru_cache(maxsize=100)
def cached_sql_generation(query: str, schema_info: str) -> tuple:
    """
    Cache the SQL generation (including optimizations) based on the query
    and the current schema (derived from the table names). If the same query
    arrives with the same schema_info, reuse the generated SQL.
    """
    enhanced_query = enhance_user_query(query, state["table_names"])
    return generate_sql_query(enhanced_query, schema_info, [], llm, state["table_names"], dialect=None)
 
# --- Use Thread Pool Executor to Parallelize LLM Calls (Optional) ---
executor = ThreadPoolExecutor(max_workers=4)
 
def parallel_execute_task(fn, *args):
    """Execute long-running tasks in parallel to avoid blocking the main thread."""
    future = executor.submit(fn, *args)
    return future.result()
 
# Optimized execute_user_query function
@router.post("/execute_query")
def execute_user_query(
    user_query: UserQuery,
    current_user: User = Depends(get_current_user),
    db: sqlalchemy.orm.Session = Depends(get_db)
):
    if not state.get("table_names"):
        raise HTTPException(status_code=400, detail="No tables available. Please upload and save your data first.")
   
    # Setup DB engine
    user_engine = state.get("personal_engine") or sqlalchemy.create_engine(
        f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{current_user.dynamic_db}",
        pool_size=10, max_overflow=20, pool_recycle=1800
    )
 
    # Ensure table presence
    if state.get("personal_engine"):
        try:
            with user_engine.connect() as connection:
                connection.execute(text(f"USE {current_user.dynamic_db};"))
                result = connection.execute(text("SHOW TABLES;"))
                available_tables = [t[0] for t in result.fetchall()]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error checking available tables: {e}")
    else:
        try:
            con = get_duckdb_connection()
            available_tables = [table_name for table_name, _ in state["table_names"]]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error accessing DuckDB: {e}")
 
    expected_tables = [name for name, _ in state["table_names"]]
    missing = [tbl for tbl in expected_tables if tbl not in available_tables]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Tables {missing} are not found in your database. Please confirm cleaning or cancel cleaning to save your data."
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
 
    # --- STATISTICAL LOGIC ---
    stat_terms = ["mean", "average", "median", "mode", "standard deviation", "std", "correlation"]
    is_statistical = any(term in user_query.query.lower() for term in stat_terms)
 
    if is_statistical:
        classification = "STATISTICAL"
        return handle_statistical_query(user_query.query)
 
    # --- CLASSIFICATION ---
    if is_advanced_sql_query(user_query.query):
        classification = "SQL"
    else:
        classification = cached_classification(user_query.query)
        if classification not in ["SQL", "SUMMARY", "ANALYSIS", "STATISTICAL"]:
            classification = "SQL"
 
    # --- ROUTING ---
    if classification == "SQL":
        schema_info = "\n".join(
            [f"Table: {name}, Columns: {', '.join(df.columns)}" for name, df in state["table_names"]]
        )
        enhanced_query = enhance_user_query(user_query.query, state["table_names"])
        dialect = None
 
        try:
            sql_query, optimizations = generate_sql_query(
                enhanced_query, schema_info, [], llm, state["table_names"], dialect=dialect
            )
 
            # Handle 'top' queries with ORDER BY + LIMIT
            if re.search(r'\btop\s+\d+', user_query.query.lower()):
                sql_lower = sql_query.lower()
                if "order by" not in sql_lower and "limit" not in sql_lower:
                    additional_instruction = "Ensure the query returns only the top results using ORDER BY and LIMIT."
                    sql_query, optimizations = generate_sql_query(
                        enhanced_query + " " + additional_instruction,
                        schema_info, [], llm, state["table_names"], dialect=dialect
                    )
 
            # --- ✅ TRY SQL EXECUTION ---
            try:
                if state.get("personal_engine"):
                    result_df = execute_sql_query(sql_query, user_query.query, user_engine)
                else:
                    con = get_duckdb_connection()
                    for table_name, df in state["table_names"]:
                        con.register(table_name, df)
                    result_df = con.execute(sql_query).df()
 
            except Exception as e:
                from app.utils.llm_helpers import explain_sql_failure_simple
                error_msg = str(e)
                user_friendly_msg = explain_sql_failure_simple(user_query.query, sql_query, error_msg, llm)
                return {
                    "classification": "SQL",
                    "result": user_friendly_msg,
                    "error_code": "SQL_EXECUTION_FAILED"
                }
 
            # Empty result case
            if result_df.empty:
                return {
                    "sql_query": sql_query,
                    "optimizations": optimizations,
                    "result": "No matching data found for your query. Please adjust your filters or try a different query."
                }
 
            # Single value (e.g. total count/sum)
            if result_df.shape == (1, 1):
                column_name = result_df.columns[0]
                value = result_df.iloc[0, 0]
                result_response = generate_dynamic_response(user_query.query, column_name, value)
            else:
                result_response = result_df.to_dict(orient="records")
 
            return {
                "classification": "SQL",
                "sql_query": sql_query,
                "optimizations": optimizations,
                "result": result_response
            }
 
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected failure during query processing: {e}")
 
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
 
    elif classification == "STATISTICAL":
        return handle_statistical_query(user_query.query)
 
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
   
@router.get("/initial_suggestions")
def get_initial_suggestions():
    return {
        "suggested_questions": state.get("initial_suggestions", [])
    }
 