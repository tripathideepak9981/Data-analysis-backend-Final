# app/utils/llm_helpers.py
from langchain_google_genai import GoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
import logging
 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
 
 
def generate_data_issue_summary(errors: list, file_name: str, llm: GoogleGenerativeAI) -> str:
    prompt = f"""
You are a data quality expert reviewing a file named "{file_name}". The analysis has detected several issues in the data, which are summarized below:
{chr(10).join(errors)}
 
Using the above details, please generate a summary report organized into the following sections:
 
1. Missing Values:
   - List each column that has missing values, specify the number of missing values, and explain in simple terms why this is an issue.
   
2. Data Types:
   - List each column with its current data type. For columns where the type is not ideal, indicate what conversion is needed and why.
   
3. Inconsistencies:
   - Identify columns with inconsistent formatting. Clearly state these variations and explain how they can affect data grouping.
   
4. Duplicate Columns:
   - Identify any duplicate columns and explain the potential impact on data analysis.
 
Now, generate the complete summary using the structure above.
    """
    response = llm.invoke(prompt)
    return response
 
def translate_natural_language_to_sql(user_query: str, schema_info: str, llm: GoogleGenerativeAI) -> str:
    template = f"""\
You are an expert data assistant. Translate the user's natural language command into a valid SQL query for data modification (INSERT, UPDATE, DELETE).
 
Follow these steps:
1. Interpret the user's request and determine the intended modification.
2. Validate the available schema and select the correct table and columns.
3. Generate a syntactically correct SQL command.
4. Finally, output "Final SQL Query:" on a new line followed by your final query.
 
**Available Tables and Schema**:
{schema_info}
 
**User Query**: {user_query}
 
Chain-of-thought explanation:
"""
    response = llm(template)
    if "Final SQL Query:" in response:
        sql_query = response.split("Final SQL Query:")[-1].strip()
    else:
        from app.utils.sql_helpers import clean_sql_query
        sql_query = clean_sql_query(response)
    sql_query = sql_query.strip()
    return sql_query
 
def classify_user_query_llm(user_query: str, llm: GoogleGenerativeAI) -> str:
    """
    Classify the user's query using an LLM to determine if it is for SQL, SUMMARY, or ANALYSIS.
   
    Args:
        user_query (str): The original user query.
        llm (GoogleGenerativeAI): The language model client.
   
    Returns:
        str: The final classification ("SQL", "SUMMARY", or "ANALYSIS").
    """
    prompt = f"""
You are an expert query classifier. Read the following user query and decide if the user intends:
- "SQL": for data retrieval tasks (filtering, slicing, etc.),
- "SUMMARY": for explicit summarization of the data,
- "ANALYSIS": for broader insights, trends, or analysis.
 
Please think step-by-step and then output the final classification on a separate line in the format:
Final Answer: <SQL or SUMMARY or ANALYSIS>
 
User Query: "{user_query}"
 
Chain-of-thought:
"""
    response = llm(prompt)
    classification = "SQL"
    for line in response.splitlines():
        if line.strip().startswith("Final Answer:"):
            classification = line.split("Final Answer:")[-1].strip().upper()
            break
    if classification not in ["SQL", "SUMMARY", "ANALYSIS"]:
        classification = "SQL"
    return classification
 
def get_special_prompt(prompt_type: str) -> str:
    prompts = {
        "SUMMARY": """
- **Strictly follow user intent** and focus on relevant columns.
- **DO NOT make assumptions**—all insights must be based on actual data values and use correct calculations.
- **Highlight variations by region, category, time period, etc.** where applicable.
- **Format response for clarity:**
  - **Key Metrics:** Top statistics (e.g., highest, lowest, average values).
  - **Notable Trends:** Increasing/decreasing trends with percentages.
  - **Key Takeaways:** Provide variations with numbers.
  - **Business Actions:** Give data-backed recommendations.
"""
    }
    return prompts.get(prompt_type.upper(), "")
 
 