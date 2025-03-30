# app/routes/upload.py
import os
import time
import logging
from io import BytesIO
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends
from fastapi.encoders import jsonable_encoder
from typing import List, Dict
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from app.database import get_db  # Import get_db dependency
 
 
from app.utils.data_processing import load_data, generate_table_name, get_data_preview
from app.utils.cleaning import validate_data, clean_data, rename_case_conflict_columns
from app.utils.llm_helpers import generate_data_issue_summary, GoogleGenerativeAI
from app.config import GOOGLE_API_KEY, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE, MODEL_NAME
from app.state import state
from app.routes.auth import get_current_user  # Dependency to retrieve the current user
from app.models import User
 
from app.utils.cleaning import clean_data, rename_case_conflict_columns
 
# At the top of app/routes/upload.py, add:
from fastapi import Depends
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.routes.auth import get_current_user  # Already imported in your file, if not, add it.
# Make sure you import create_dynamic_database_for_user from auth.py if you wish to reuse it.
from app.routes.auth import create_dynamic_database_for_user
 
router = APIRouter()
logger = logging.getLogger("upload")
logger.setLevel(logging.INFO)
 
# Allowed MIME types for CSV and Excel files.
ALLOWED_MIME_TYPES = [
    "text/csv",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel"
]
 
# Initialize the LLM instance using your API key.
llm = GoogleGenerativeAI(model=MODEL_NAME, api_key=GOOGLE_API_KEY)
 
# Create a SQLAlchemy engine with connection pooling for the main database.
# (This engine is used only for file processing previews; final saving will use user-specific engines.)
engine = sqlalchemy.create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}",
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800
)
 
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
 
def has_duplicate_columns(df: pd.DataFrame) -> bool:
    """Return True if df has duplicate column names (case-insensitive)."""
    normalized_cols = [col.strip().lower() for col in df.columns if col.strip()]
    return len(normalized_cols) != len(set(normalized_cols))
 
def get_common_attributes(sheets: Dict[str, pd.DataFrame]) -> set:
    """
    Dynamically returns the set of column names common to all sheets.
    All column names are normalized (lowercased and stripped) for case-insensitive comparison.
    """
    common = None
    for sheet_name, df in sheets.items():
        cols = set(col.strip().lower() for col in df.columns if col.strip())
        if common is None:
            common = cols
        else:
            common = common.intersection(cols)
    return common if common is not None else set()
 
def are_sheets_related(sheets: Dict[str, pd.DataFrame], threshold: float = 0.5) -> bool:
    """
    Checks whether the sheets are related by comparing common columns' data values.
   
    For each common column (normalized), compute the overlap ratio of distinct values
    (also normalized) between sheets. If the average overlap ratio for any common column
    meets or exceeds the threshold, the sheets are considered related.
    """
    common_cols = get_common_attributes(sheets)
    if not common_cols:
        return False
 
    for col in common_cols:
        value_sets = []
        for df in sheets.values():
            actual_col = next((c for c in df.columns if c.strip().lower() == col), None)
            if actual_col:
                vals = set(df[actual_col].dropna().astype(str).str.lower().str.strip())
                if vals:
                    value_sets.append(vals)
        if len(value_sets) < 2:
            continue
        ratios = []
        ref = value_sets[0]
        for other in value_sets[1:]:
            union = ref.union(other)
            if not union:
                ratios.append(0)
            else:
                ratio = len(ref.intersection(other)) / len(union)
                ratios.append(ratio)
        if ratios and (sum(ratios) / len(ratios)) >= threshold:
            logger.info(f"Common column '{col}' has sufficient overlap: {sum(ratios)/len(ratios):.2f}")
            return True
    return False
 
async def process_file(file: UploadFile) -> List[dict]:
    """
    Process an uploaded file and return a list of table information dictionaries.
   
    For CSV files, the file is processed as a single table.
    For Excel files:
      - All sheets are read.
      - If multiple sheets exist and they share at least one common column with similar data
        (determined dynamically), the sheets are combined into one table (with an extra "sheet_name" column).
      - Otherwise, each sheet is processed separately.
   
    **Important:** No data is saved to the SQL database in this function.
    The raw data is stored in the global state, and a cleaning summary and preview are returned.
    The response always includes "requires_cleaning": True so that the frontend
    must explicitly confirm cleaning (via /clean_file) or cancel (via /cancel_clean) before any data is saved.
    """
    # Validate file extension.
    if not (file.filename.endswith(".csv") or file.filename.endswith(".xlsx")):
        raise HTTPException(status_code=400, detail=f"Unsupported file format: {file.filename}")
 
    # Validate MIME type.
    if file.content_type not in ALLOWED_MIME_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported file MIME type: {file.content_type}")
 
    # Validate file size.
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail=f"File {file.filename} is too large (max 10MB).")
    file.file.seek(0)
   
    results = []
   
    # Process CSV files.
    if file.filename.endswith(".csv"):
        try:
            df = load_data(file)
        except Exception as e:
            logger.error(f"Error loading file {file.filename}: {e}")
            raise HTTPException(status_code=400, detail=f"Error loading file {file.filename}: {e}")
        if df is None or df.empty:
            raise HTTPException(status_code=400, detail=f"File {file.filename} is empty or invalid.")
 
        try:
            errors = validate_data(df, file.filename)
            cleaning_summary = generate_data_issue_summary(errors, file.filename, llm)
        except Exception as e:
            logger.error(f"Error generating cleaning summary for {file.filename}: {e}")
            cleaning_summary = f"Failed to generate cleaning summary: {e}"
 
        tbl_name = generate_table_name(file.filename)
        duplicate_issue = has_duplicate_columns(df)
        # Store the raw data in state so that it can be saved later upon user confirmation.
        state["table_names"].append((tbl_name, df))
        state["original_table_names"].append((tbl_name, df.copy()))
       
        try:
            preview = get_data_preview(df)
            preview = jsonable_encoder(preview)
        except Exception as e:
            logger.error(f"Error generating data preview for table {tbl_name}: {e}")
            preview = {}
 
        logger.info(f"File {file.filename} processed for preview (no DB save yet) into table {tbl_name}.")
        results.append({
            "file_name": file.filename,
            "table_name": tbl_name,
            "cleaning_summary": cleaning_summary,
            "is_cleaned": False,
            "preview": preview,
            "requires_cleaning": True,
            "duplicates": duplicate_issue,
            "message": "Data not saved yet. Please confirm cleaning to save data for analysis, or cancel to save raw data."
        })
   
    # Process Excel files.
    elif file.filename.endswith(".xlsx"):
        try:
            file.file.seek(0)
            excel_file = pd.ExcelFile(file.file)
            sheet_names = excel_file.sheet_names
            if not sheet_names:
                raise HTTPException(status_code=400, detail=f"No sheets found in file {file.filename}.")
            sheets = {}
            for sheet in sheet_names:
                try:
                    df_sheet = pd.read_excel(excel_file, sheet_name=sheet)
                    if not df_sheet.empty:
                        sheets[sheet] = df_sheet
                except Exception as e:
                    logger.error(f"Error reading sheet {sheet} in file {file.filename}: {e}")
            if not sheets:
                raise HTTPException(status_code=400, detail=f"All sheets in file {file.filename} are empty or invalid.")
        except Exception as e:
            logger.error(f"Error processing Excel file {file.filename}: {e}")
            raise HTTPException(status_code=400, detail=f"Error processing Excel file {file.filename}: {e}")
 
        # Check if sheets are related using dynamic attribute detection.
        if len(sheets) > 1 and are_sheets_related(sheets, threshold=0.5):
            combined_list = []
            for sheet_name, df_sheet in sheets.items():
                df_sheet = df_sheet.copy()
                df_sheet["sheet_name"] = sheet_name  # Preserve sheet identity.
                combined_list.append(df_sheet)
            combined_df = pd.concat(combined_list, ignore_index=True)
            tbl_name = generate_table_name(file.filename) + "_combined"
            try:
                errors = validate_data(combined_df, file.filename + " (combined)")
                cleaning_summary = generate_data_issue_summary(errors, file.filename + " (combined)", llm)
            except Exception as e:
                logger.error(f"Error generating cleaning summary for combined data in {file.filename}: {e}")
                cleaning_summary = f"Failed to generate cleaning summary: {e}"
           
            duplicate_issue = has_duplicate_columns(combined_df)
            state["table_names"].append((tbl_name, combined_df))
            state["original_table_names"].append((tbl_name, combined_df.copy()))
           
            try:
                preview = get_data_preview(combined_df)
                preview = jsonable_encoder(preview)
            except Exception as e:
                logger.error(f"Error generating preview for combined table {tbl_name}: {e}")
                preview = {}
            logger.info(f"File {file.filename} processed for preview into combined table {tbl_name} (no DB save yet).")
            results.append({
                "file_name": file.filename,
                "table_name": tbl_name,
                "cleaning_summary": cleaning_summary,
                "is_cleaned": False,
                "preview": preview,
                "requires_cleaning": True,
                "duplicates": duplicate_issue,
                "message": "Data not saved yet. Please confirm cleaning to save data for analysis, or cancel to save raw data."
            })
        else:
            # Process each sheet separately.
            for sheet_name, df_sheet in sheets.items():
                current_filename = f"{file.filename} ({sheet_name})"
                try:
                    errors = validate_data(df_sheet, current_filename)
                    cleaning_summary = generate_data_issue_summary(errors, current_filename, llm)
                except Exception as e:
                    logger.error(f"Error generating cleaning summary for sheet {sheet_name} in {file.filename}: {e}")
                    cleaning_summary = f"Failed to generate cleaning summary: {e}"
               
                tbl_name = generate_table_name(file.filename) + "_" + sheet_name.lower().replace(" ", "_")
                duplicate_issue = has_duplicate_columns(df_sheet)
                state["table_names"].append((tbl_name, df_sheet))
                state["original_table_names"].append((tbl_name, df_sheet.copy()))
               
                try:
                    preview = get_data_preview(df_sheet)
                    preview = jsonable_encoder(preview)
                except Exception as e:
                    logger.error(f"Error generating preview for table {tbl_name}: {e}")
                    preview = {}
                logger.info(f"Sheet {sheet_name} from file {file.filename} processed for preview into table {tbl_name} (no DB save yet).")
                results.append({
                    "file_name": file.filename,
                    "sheet": sheet_name,
                    "table_name": tbl_name,
                    "cleaning_summary": cleaning_summary,
                    "is_cleaned": False,
                    "preview": preview,
                    "requires_cleaning": True,
                    "duplicates": duplicate_issue,
                    "message": "Data not saved yet. Please confirm cleaning to save data for analysis, or cancel to save raw data."
                })
   
    return results
 
@router.post("/upload")
async def upload_files(files: List[UploadFile] = File(...), background_tasks: BackgroundTasks = None):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded.")
    uploaded_info = []
    # Clear previous state.
    state["table_names"].clear()
    state["original_table_names"].clear()
    state["personal_engine"] = None
    state["mysql_connection"] = None
    state["chat_history"].clear()
   
    for file in files:
        try:
            file_results = await process_file(file)
            uploaded_info.extend(file_results)
        except HTTPException as he:
            raise he
        except Exception as e:
            logger.error(f"Unexpected error processing file {file.filename}: {e}")
            raise HTTPException(status_code=500, detail=f"Unexpected error processing file {file.filename}: {e}")
    return {"status": "success", "files": uploaded_info}
 
# Then, update your clean_file endpoint:
from fastapi import Query
 
@router.post("/clean_file")
async def clean_file(
    table_name: str = Query(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)  # Use get_db instead of SessionLocal directly
):
    ...
    # If the user hasn't confirmed saving a file before, dynamic_db will be empty.
    if not current_user.dynamic_db:
        dynamic_db_name = create_dynamic_database_for_user(current_user)
        current_user.dynamic_db = dynamic_db_name
        db.commit()  # Update the user record in the central DB.
 
 
    user_engine = sqlalchemy.create_engine(
        f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{current_user.dynamic_db}",
        pool_size=10,
        max_overflow=20,
        pool_recycle=1800
    )
    # ... proceed as before to clean data and save table into user_engine ...
    # (Your existing code for saving the cleaned data remains unchanged)
    for idx, (name, df) in enumerate(state["original_table_names"]):
        if name == table_name:
            cleaned_df = clean_data(df.copy())
            cleaned_df = rename_case_conflict_columns(cleaned_df)
            state["table_names"][idx] = (table_name, cleaned_df)
            try:
                cleaned_df.to_sql(table_name, user_engine, index=False, if_exists="replace")
            except Exception as e:
                logger.error(f"Error saving cleaned table {table_name}: {e}")
                raise HTTPException(status_code=500, detail=f"Error saving cleaned table {table_name}: {e}")
            preview = get_data_preview(cleaned_df)
            return {
                "status": "cleaned",
                "table_name": table_name,
                "preview": preview
            }
    raise HTTPException(status_code=404, detail="Table not found")
 
 
# Similarly update the cancel_clean endpoint:
@router.post("/cancel_clean")
async def cancel_clean(
    table_name: str = Query(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)  # Use get_db instead of SessionLocal
):
 
    if not current_user.dynamic_db:
        dynamic_db_name = create_dynamic_database_for_user(current_user)
        current_user.dynamic_db = dynamic_db_name
        db.commit()
 
    user_engine = sqlalchemy.create_engine(
        f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{current_user.dynamic_db}",
        pool_size=10,
        max_overflow=20,
        pool_recycle=1800
    )
    for idx, (name, df) in enumerate(state["original_table_names"]):
        if name == table_name:
            if has_duplicate_columns(df):
                df = rename_case_conflict_columns(df)
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    df.to_sql(table_name, user_engine, index=False, if_exists="replace")
                    break
                except Exception as e:
                    logger.error(f"Attempt {attempt+1}: Error saving raw table {table_name}: {e}")
                    if attempt == max_retries - 1:
                        raise HTTPException(status_code=500, detail=f"Error saving raw table {table_name}: {e}")
                    time.sleep(1)
            try:
                preview = get_data_preview(df)
                preview = jsonable_encoder(preview)
            except Exception as e:
                logger.error(f"Error generating preview for raw table {table_name}: {e}")
                preview = {}
            logger.info(f"Raw data for table {table_name} saved successfully (cancel cleaning).")
            return {
                "status": "saved raw",
                "table_name": table_name,
                "preview": preview
            }
    raise HTTPException(status_code=404, detail="Table not found")
 
 