# app/utils/sql_helpers.py
import re
import pandas as pd
import sqlalchemy
from sqlalchemy import text
 
def clean_sql_query(raw_query: str, dialect: str = None) -> str:
    cleaned_query = raw_query.strip()
    if cleaned_query.startswith("```sql") and cleaned_query.endswith("```"):
        cleaned_query = cleaned_query[6:-3].strip()
    elif cleaned_query.startswith("```") and cleaned_query.endswith("```"):
        cleaned_query = cleaned_query[3:-3].strip()
    cleaned_query = cleaned_query.replace("```", "").strip()
    cleaned_query = cleaned_query.rstrip(";").strip()
    if dialect and dialect.lower() == "vertica":
        cleaned_query = cleaned_query.replace("`", "")
        cleaned_query = re.sub(r'"([^"]+)"', lambda m: m.group(1).lower(), cleaned_query)
        def lower_comparison(match):
            column = match.group(1)
            literal = match.group(2)
            return f"lower({column}) = lower('{literal}')"
        cleaned_query = re.sub(r"(\b\w+\b)\s*=\s*'([^']+)'", lower_comparison, cleaned_query)
    return cleaned_query + ";"
 
def enhance_user_query(user_query: str, table_names: list) -> str:
    column_mapping = {}
    for table_tuple in table_names:
        if isinstance(table_tuple, tuple) and len(table_tuple) >= 2:
            table_name, df = table_tuple[:2]
            for col in df.columns:
                column_mapping[col.replace("_", " ")] = col
    enhanced_query = user_query
    for user_word, actual_column in column_mapping.items():
        pattern = r'\b' + re.escape(user_word) + r'\b'
        if re.search(pattern, enhanced_query, flags=re.IGNORECASE):
            enhanced_query = re.sub(pattern, actual_column, enhanced_query, flags=re.IGNORECASE)
    return enhanced_query
 
def suggest_query_optimizations(sql_query: str, user_query: str, schema_info: str, nlp_model) -> tuple:
    optimizations = []
    doc = nlp_model(user_query)
    if any(token.text.lower() in ["average", "sum", "count", "max", "min"] for token in doc):
        optimizations.append("Consider using aggregation functions like AVG, SUM, COUNT, MAX, MIN for summary statistics.")
    if any(token.text.lower() in ["join", "combine", "merge"] for token in doc):
        optimizations.append("Consider using JOIN operations to combine data from multiple tables.")
    if any(token.text.lower() in ["date", "time", "period"] for token in doc):
        optimizations.append("Consider filtering data by date or time periods using WHERE clauses.")
    if any(token.text.lower() in ["sort", "order", "arrange"] for token in doc):
        optimizations.append("Consider ordering the results using ORDER BY clauses.")
    if "SELECT *" in sql_query.upper():
        optimizations.append("Select only the necessary columns instead of using SELECT * for efficiency.")
    return (sql_query, optimizations)
 
def generate_sql_query(user_query: str, schema_info: str, chat_history: list, llm, table_names: list, dialect: str = None) -> tuple:
    column_mapping = {}
    for tname, df in table_names:
        for col in df.columns:
            column_mapping[col.replace("_", " ")] = col
    for user_word, actual_column in column_mapping.items():
        if user_word in user_query:
            user_query = user_query.replace(user_word, actual_column)
    special_instructions = ""
    if dialect is not None and dialect.lower() == "vertica":
        special_instructions = "Ensure that the generated query is valid for Vertica database. Do not use MySQL-specific syntax such as backticks."
   
    # Updated template with additional instructions for aggregate queries.
    template = f"""\
You are an expert SQL generator with strong reasoning abilities.
Follow these steps:
1. Analyze the user's query to identify the intended data retrieval.
2. Map the user request to the provided schema and choose the correct table and column names.
3. If the user's query requests aggregated data (for example, phrases like "total", "sum", "average", or "count"), generate a query that uses the appropriate aggregate functions (e.g. SUM, AVG, COUNT) for the referenced column(s) instead of returning row-wise values.
4. Generate a syntactically correct SQL query.
5. Finally, on a new line, output "Final SQL Query:" followed by the final query.
{special_instructions}
**Available Tables and Schema**:
{schema_info}
**User Query**: {user_query}
Chain-of-thought explanation:
"""
    response = llm(template)
    if "Final SQL Query:" in response:
        sql_query = response.split("Final SQL Query:")[-1].strip()
    else:
        sql_query = clean_sql_query(response, dialect=dialect)
    sql_query = clean_sql_query(sql_query, dialect=dialect)
    max_attempts = 2
    attempts = 0
    while sql_query.strip() == ";" and attempts < max_attempts:
        fallback_instruction = "Your previous response did not produce a valid SQL query. Please generate a valid SQL query for the user's request. Ensure that the query retrieves the required data."
        fallback_prompt = template + "\n" + fallback_instruction
        fallback_response = llm(fallback_prompt)
        if "Final SQL Query:" in fallback_response:
            sql_query = fallback_response.split("Final SQL Query:")[-1].strip()
        else:
            sql_query = clean_sql_query(fallback_response, dialect=dialect)
        sql_query = clean_sql_query(sql_query, dialect=dialect)
        attempts += 1
    from app.utils.cleaning import NLP_MODEL
    optimized_query, optimizations = suggest_query_optimizations(sql_query, user_query, schema_info, NLP_MODEL)
    return optimized_query, optimizations
 
 
def execute_sql_query(sql_query: str, user_query: str, connection) -> pd.DataFrame:
    sql_query = sql_query.strip().rstrip(';') + ';'
    dialect = ""
    if hasattr(connection, "engine"):
        dialect = connection.engine.dialect.name.lower()
    elif hasattr(connection, "dialect"):
        dialect = connection.dialect.name.lower()
    if dialect == "vertica":
        sql_query = sql_query.replace("`", "")
    try:
        if isinstance(connection, sqlalchemy.engine.Engine):
            with connection.connect() as conn:
                if sql_query.strip().upper().startswith("SELECT"):
                    result = pd.read_sql_query(sql_query, conn)
                else:
                    conn.execute(text(sql_query))
                    result = pd.DataFrame()
        elif hasattr(connection, "cursor"):
            cursor = connection.cursor(buffered=True)
            try:
                cursor.execute(sql_query)
                if sql_query.strip().upper().startswith("SELECT"):
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    result = pd.DataFrame(rows, columns=columns)
                else:
                    connection.commit()
                    result = pd.DataFrame()
            finally:
                cursor.close()
        else:
            if sql_query.strip().upper().startswith("SELECT"):
                result = pd.read_sql_query(sql_query, connection)
            else:
                connection.execute(text(sql_query))
                result = pd.DataFrame()
        return result
    except Exception as e:
        raise e
 