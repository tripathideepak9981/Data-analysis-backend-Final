# app/utils/llm_helpers.py
 
import logging
from langchain_google_genai import GoogleGenerativeAI
from app.config import GOOGLE_API_KEY, MODEL_NAME
from app.utils.data_processing import summarize_schema_for_llm
from app.utils.sql_helpers import generate_sql_query, execute_sql_query
from app.state import state
import sqlalchemy
import time
from app.utils.sql_helpers import enhance_user_query
from app.state import get_duckdb_connection, state
 
import pandas as pd
 
 
 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
 
# Initialize global LLM instance
llm = GoogleGenerativeAI(model=MODEL_NAME, api_key=GOOGLE_API_KEY)
import time
 
def call_llm_with_retry(prompt, llm, retries=2, delay=2):
    for attempt in range(retries):
        try:
            return llm(prompt)
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(delay)

def icon_for_issue(issue: str) -> str:
    s = issue.lower()
    if "missing value" in s or "missing values" in s:
        return "❗"
    if "duplicate column" in s or "duplicates" in s:
        return "♻️"
    if "completely empty" in s:
        return "🚫"
    if "invalid email" in s or "valid email" in s:
        return "📧"
    if "invalid phone" in s or "valid phone" in s:
        return "📱"
    if "special characters" in s:
        return "🌀"
    if "mixed data types" in s:
        return "🔀"
    if "constant" in s or "same value" in s:
        return "⏹️"
    # Add other mappings as needed
    return "❗"
 
def issues_with_icons_and_numbers(issues: list) -> str:
    """
    Takes a list of issue sentences, adds icons and numbers for display.
    """
    if not issues:
        return "✅ No issues found. Everything looks good!"
 
    output = []
    for idx, issue in enumerate(issues, 1):
        icon = icon_for_issue(issue)
        # Clean up double numbering if LLM already puts "1.", "2." etc.
        cleaned = issue
        if cleaned[:2].isdigit() and cleaned[2] in [".", ")", "-", " "]:
            cleaned = cleaned[3:].strip()
        output.append(f"{idx}. {icon} {cleaned}")
    return "\n".join(output)
 
 
def generate_data_issue_summary(issues: list, file_name: str, llm_instance) -> str:
    """
    Summarizes detected data issues for business users.
    Returns at most 7 lines, only real detected issues.
    Avoids headings and extra numbering.
    """
    if not issues or all(("no data issues" in i.lower() or "no issues" in i.lower()) for i in issues):
        return f"✅ No issues found in '{file_name}'. Everything looks good!"
 
    prompt = f"""
You are a data quality assistant for business users.
 
Here is a list of real, grouped, counted data issues found in the file '{file_name}':
{chr(10).join(issues)}
 
Instructions:
- For each issue, write a short, clear, business-friendly bullet.
- Do not use technical jargon (never say "null", "NaN", "dtype", "IQR", etc).
- Show numbers (counts of columns, missing values, etc) clearly where present.
- Never invent or summarize issues, never cluster different issues into one.
- Output at most 7 bullets (pick the most important).
- Each line should be clear on what the issue is, with column names and numbers where possible.
- Do not mention outliers.
- Do not include any summary or heading lines, just direct numbered issues.
- Example: "The column 'Email' has 3 values that do not look like valid email addresses."
Just output the rephrased issues, one per line.
"""
 
    def is_heading_line(line: str):
        HEADINGS = [
            "here's a summary",
            "summary of data quality issues",
            "summary",
            "below is a list",
            "here are the issues",
            "data issues",
            "the following issues",
            "cleaning summary"
        ]
        l = line.lower()
        return any(h in l for h in HEADINGS)
 
    try:
        llm_response = llm_instance(prompt)
        # Split by real lines, remove headings, deduplicate, limit to 7
        clean_lines = []
        seen = set()
        for l in llm_response.splitlines():
            line = l.strip("•- \t")
            if not line or is_heading_line(line):
                continue
            # Remove leading numbering if present
            if line[:2].isdigit() and line[2] in [".", ")", "-", " "]:
                line = line[3:].strip()
            if line in seen:
                continue
            seen.add(line)
            clean_lines.append(line)
            if len(clean_lines) == 7:
                break
        issues_list = issues_with_icons_and_numbers(clean_lines)
    except Exception:
        # fallback: use the plain issues
        issues_list = issues_with_icons_and_numbers(issues[:7])
    return issues_list
 
 
def translate_natural_language_to_sql(user_query: str, schema_info: str, llm_instance: GoogleGenerativeAI) -> str:
    prompt = f"""
Translate the user's instruction to a valid SQL query for INSERT/UPDATE/DELETE.
 
User Query: {user_query}
Schema Info:
{schema_info}
 
Respond with: Final SQL Query:
"""
    response = llm_instance(prompt)
    if "Final SQL Query:" in response:
        return response.split("Final SQL Query:")[-1].strip()
    from app.utils.sql_helpers import clean_sql_query
    return clean_sql_query(response)
 
 
def classify_user_query_llm(user_query: str, llm_instance: GoogleGenerativeAI) -> str:
    prompt = f"""
You are a query classifier.
 
User Query: "{user_query}"
 
Classify as one of: SQL, SUMMARY, ANALYSIS, STATISTICAL
Respond with: Final Answer: <type>
"""
    try:
        response = llm_instance(prompt)
        for line in response.splitlines():
            if "Final Answer:" in line:
                return line.split("Final Answer:")[-1].strip().upper()
    except Exception:
        pass
    return "SQL"  # Fallback
 
 
def get_special_prompt(prompt_type: str) -> str:
    prompts = {
        "SUMMARY": """
- Strictly follow user intent and data.
- Show regional/category/time variations.
- Format clearly: Metrics, Trends, Takeaways, Recommendations.
"""
    }
    return prompts.get(prompt_type.upper(), "")
 
 
def explain_sql_failure_simple(user_query: str, sql_query: str, error_message: str, llm_instance: GoogleGenerativeAI) -> str:
    prompt = f"""
The query "{user_query}" failed.
 
Generated SQL: "{sql_query}"
Error: "{error_message}"
 
Explain this simply for a non-technical user. No SQL jargon.
"""
    try:
        return llm_instance(prompt).strip()
    except Exception:
        return "There was a problem understanding your query. Please check table or column names."
 
 
def generate_dynamic_response(user_query: str, column_name: str, value) -> str:
    prompt = f"""
User asked: "{user_query}"
Result for column "{column_name}": {value}
 
Generate a clear and friendly response. Avoid repeating the raw column name.
"""
    return llm(prompt).strip()
 
 
def generate_initial_suggestions_from_state(llm, state) -> list[str]:
    schema_info = "\n".join([
        f"Table: {name}, Columns: {', '.join(df.columns)}"
        for name, df in state.get("table_names", [])
    ])
 
    prompt = f"""
A user uploaded data with this schema:
 
{schema_info}
 
Generate 5 simple, short, natural language questions to analyze the data.
Only output 1 question per line. Keep under 20 words. No technical words.
"""
 
    try:
        raw_response = call_llm_with_retry(prompt, llm)
        candidates = [q.strip("-• ").strip() for q in raw_response.splitlines() if q.strip()]
        valid_questions = []
 
        # ✅ Use direct SQL execution, not TestClient
        for q in candidates:
            try:
                enhanced_query = enhance_user_query(q, state["table_names"])
                sql_query, _ = generate_sql_query(enhanced_query, schema_info, [], llm, state["table_names"])
                con = get_duckdb_connection()
                for table_name, df in state["table_names"]:
                    con.register(table_name, df)
                result_df = con.execute(sql_query).df()
                if not result_df.empty:
                    valid_questions.append(q)
            except Exception:
                continue
 
        return valid_questions[:4]
 
    except Exception as e:
        print(f"Suggestion generation failed: {e}")
        return []
 
 
 
# def generate_followup_suggestions(query_text: str, result_df: pd.DataFrame, llm: GoogleGenerativeAI) -> list[str]:
#     # Reduce preview size
#     preview_df = result_df.iloc[:5, :4]  # Top 5 rows, first 4 columns
#     preview_md = preview_df.to_markdown(index=False)
 
#     prompt = f"""
# You are a friendly AI data analyst helping users explore their data step by step.
 
# The user asked: "{query_text}"
 
# They received this preview table:
 
# {preview_md}
 
# Your task is to suggest 3 insightful, human-friendly follow-up questions based on this result.
 
# Guidelines:
# - Keep each question under 15 words
# - Make the questions helpful for deeper analysis or discovery
# - Use simple, natural language — avoid technical terms
# - Make them sound like something a non-technical user might genuinely ask
# - Use real column names shown above when helpful
# - Prefer questions like: comparisons, trends, breakdowns, insights, changes over time, filters, or top values
 
# Examples of good questions:
# - Which region had the highest total?
# - How did this value change over time?
# - Can you break this down by district?
# - What are the top 5 categories by total?
 
# Write 3 such questions. One per line. No numbering, no extra text.
# """
 
#     try:
#         response = call_llm_with_retry(prompt, llm)
#         candidates = [q.strip("-•* ").strip() for q in response.splitlines() if q.strip()]
#     except Exception as e:
#         return ["Could not generate follow-up suggestions."]
 
#     # Validation using SQL generation
#     schema_info = "\n".join([
#         f"Table: {name}, Columns: {', '.join(df.columns)}"
#         for name, df in state.get("table_names", [])
#     ])
 
#     def validate_suggestion(question):
#         try:
#             enhanced_query = enhance_user_query(question, state["table_names"])
#             sql_query, _ = generate_sql_query(enhanced_query, schema_info, [], llm, state["table_names"])
#             con = get_duckdb_connection()
#             for table_name, df in state["table_names"]:
#                 con.register(table_name, df)
#             temp_df = con.execute(sql_query).df()
#             if not temp_df.empty:
#                 return question
#         except:
#             return None
 
#     with ThreadPoolExecutor(max_workers=4) as executor:
#         futures = [executor.submit(validate_suggestion, q) for q in candidates]
#         validated = [f.result() for f in futures]
 
#     valid_questions = [q for q in validated if q]
#     return valid_questions[:3]
 
 