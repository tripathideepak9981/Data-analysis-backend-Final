# app/utils/cleaning.py
import re
import pandas as pd
import spacy
 
# Load spaCy model globally for NLP tasks
NLP_MODEL = spacy.load("en_core_web_sm")
 
import re
import unicodedata
import pandas as pd
import numpy as np
 
 
def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans up column names for any possible scenario: blank, duplicate, special chars, etc.
    - Ensures every column has a unique, readable name.
    - Normalizes spaces/special chars to underscores.
    """
    new_columns = []
    seen = set()
    for idx, col in enumerate(df.columns):
        orig_col = str(col)
        col_clean = orig_col.strip()
        col_clean = unicodedata.normalize("NFKD", col_clean)
        col_clean = col_clean.replace('\xa0', ' ')
        # Empty/unnamed/untitled columns
        if not col_clean or re.fullmatch(r"\s*|unnamed[:\-\s\d]*|untitled[:\-\s\d]*", col_clean, re.I):
            col_clean = f"column_{idx+1}"
        # Replace all spaces and special chars with underscore
        col_clean = re.sub(r"[^\w]", "_", col_clean)
        col_clean = col_clean.lower()
        col_clean = re.sub(r"_+", "_", col_clean).strip("_")
        # If col_clean is empty or only numbers, give it a placeholder
        if not col_clean or col_clean.isdigit():
            col_clean = f"column_{idx+1}"
        base_col = col_clean
        n = 2
        while col_clean in seen:
            col_clean = f"{base_col}_{n}"
            n += 1
        seen.add(col_clean)
        new_columns.append(col_clean)
    df.columns = new_columns
    return df
 
import re
import pandas as pd
import spacy
import unicodedata
import numpy as np
 
# Load spaCy model globally for NLP tasks
NLP_MODEL = spacy.load("en_core_web_sm")
 
 
def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans up column names for any possible scenario: blank, duplicate, special chars, etc.
    - Ensures every column has a unique, readable name.
    - Normalizes spaces/special chars to underscores.
    """
    new_columns = []
    seen = set()
    for idx, col in enumerate(df.columns):
        orig_col = str(col)
        col_clean = orig_col.strip()
        col_clean = unicodedata.normalize("NFKD", col_clean)
        col_clean = col_clean.replace('\xa0', ' ')
        # Empty/unnamed/untitled columns
        if not col_clean or re.fullmatch(r"\s*|unnamed[:\-\s\d]*|untitled[:\-\s\d]*", col_clean, re.I):
            col_clean = f"column_{idx+1}"
        # Replace all spaces and special chars with underscore
        col_clean = re.sub(r"[^\w]", "_", col_clean)
        col_clean = col_clean.lower()
        col_clean = re.sub(r"_+", "_", col_clean).strip("_")
        # If col_clean is empty or only numbers, give it a placeholder
        if not col_clean or col_clean.isdigit():
            col_clean = f"column_{idx+1}"
        base_col = col_clean
        n = 2
        while col_clean in seen:
            col_clean = f"{base_col}_{n}"
            n += 1
        seen.add(col_clean)
        new_columns.append(col_clean)
    df.columns = new_columns
    return df
 
 
def validate_data(df: pd.DataFrame, file_name: str, original_columns=None) -> list:
    """
    Returns a grouped, user-friendly list of data issues with counts.
    Groups repetitive issues (e.g. missing values, invalid emails, special chars, etc)
    Shows counts and affected columns per bullet
    No technical jargon, no outlier reporting
    Does NOT warn if all values are unique (not a data issue)
    """
    issues = []
 
    # Collect for grouping
    cols_blank_name = []
    cols_special_name = []
    cols_number_name = []
    cols_whitespace_name = []
    cols_duplicate = []
    cols_empty = []
    cols_constant = []
    cols_with_missing = []
    cols_few_unique = []
    cols_mixed_type = []
    cols_special_chars = []
    cols_large_unique_cat = []
    cols_inconsistent_country = []
    cols_invalid_email = []
    cols_invalid_phone = []
    cols_inconsistent_date = []
    cols_num_text = []
    cols_text_num = []
    cols_extreme = []
    cols_name_special = []
 
    # For counts
    missing_counts = {}
    few_unique_counts = {}
    invalid_email_counts = {}
    invalid_phone_counts = {}
 
    # -------- Column name checks
    if original_columns is not None:
        for idx, col in enumerate(original_columns):
            col_str = str(col).strip()
            if not col_str or re.fullmatch(r"\s*|unnamed[:\-\s\d]*|untitled[:\-\s\d]*", col_str, re.I):
                cols_blank_name.append(f"Column {idx+1}")
            if re.fullmatch(r"\d+", col_str):
                cols_number_name.append(f"Column {idx+1} ('{col_str}')")
            if re.search(r"[^\w\s]", col_str):
                cols_special_name.append(f"Column {idx+1} ('{col_str}')")
            if col_str and col_str != col_str.strip():
                cols_whitespace_name.append(f"Column {idx+1} ('{col_str}')")
 
    # Duplicate columns
    col_lower = [c.lower().strip() for c in df.columns]
    dups = set([c for c in col_lower if col_lower.count(c) > 1])
    if dups:
        for d in dups:
            real_names = [c for c in df.columns if c.lower().strip() == d]
            cols_duplicate.append(", ".join(real_names))
 
    # Empty columns
    empty_cols = df.isnull().all(axis=0)
    if empty_cols.any():
        cols_empty.extend(df.columns[empty_cols])
 
    # Completely empty rows
    empty_rows = df.isnull().all(axis=1).sum()
    if empty_rows:
        issues.append(f"{empty_rows} completely empty row(s) found.")
 
    # Duplicate rows (counted)
    dup_rows = df.duplicated().sum()
    if dup_rows > 0:
        issues.append(f"{dup_rows} duplicate row(s) found.")
 
    # Constant columns
    for col in df.columns:
        non_null = df[col].dropna().unique()
        if len(non_null) == 1 and len(df[col].dropna()) > 0:
            cols_constant.append(col)
 
    # Columns with missing values (all grouped)
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            cols_with_missing.append(col)
            missing_counts[col] = null_count
 
    # Columns with very low unique values (except IDs/codes)
    for col in df.columns:
        nunique = df[col].nunique(dropna=True)
        if len(df) > 20 and 1 < nunique <= 3 and not re.search(r"id|code|type", col, re.I):
            cols_few_unique.append(col)
            few_unique_counts[col] = nunique
 
    # Numeric stored as text, or text stored as numeric
    for col in df.columns:
        if df[col].dtype == object:
            num_like = pd.to_numeric(df[col], errors="coerce").notnull().sum()
            if num_like > 0 and num_like / len(df[col]) > 0.8:
                cols_num_text.append(col)
        if pd.api.types.is_numeric_dtype(df[col]):
            text_like = df[col].astype(str).str.contains(r"[a-zA-Z]").sum()
            if text_like / len(df[col]) > 0.8:
                cols_text_num.append(col)
 
    # Format checks (email, phone, date, country)
    for col in df.columns:
        col_lower = col.lower()
        # Email
        if "email" in col_lower:
            pattern = r"[^@]+@[^@]+\.[^@]+"
            invalid = ~df[col].astype(str).str.strip().str.fullmatch(pattern)
            count = invalid.sum()
            if count > 0:
                cols_invalid_email.append(col)
                invalid_email_counts[col] = count
        # Phone
        if "phone" in col_lower or "mobile" in col_lower:
            phone_pattern = r"^\+?\d[\d\s\-]{7,}\d$"
            invalid = ~df[col].astype(str).str.strip().str.match(phone_pattern)
            count = invalid.sum()
            if count > 0:
                cols_invalid_phone.append(col)
                invalid_phone_counts[col] = count
        # Date
        if "date" in col_lower:
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notnull().sum() / len(df[col]) < 0.8:
                cols_inconsistent_date.append(col)
        # Country
        if "country" in col_lower:
            vals = df[col].dropna().astype(str).str.strip().str.lower()
            if len(set(vals)) > 5:
                cols_inconsistent_country.append(col)
 
    # Special characters in string columns
    for col in df.select_dtypes(include="object").columns:
        suspicious = df[col].astype(str).str.contains(r"[!@#$%^&*()_=+\[\]{};:'\",<>/?\\|]")
        if suspicious.mean() > 0.3:
            cols_special_chars.append(col)
 
    # Extreme numeric values
    for col in df.select_dtypes(include=[np.number]).columns:
        if df[col].max() > 1e12 or df[col].min() < -1e12:
            cols_extreme.append(col)
 
    # Column names with spaces or special chars
    for col in df.columns:
        if re.search(r"[\s\-\.]", col):
            cols_name_special.append(col)
 
    # Categorical with too many unique values
    for col in df.select_dtypes(include="object").columns:
        nunique = df[col].nunique(dropna=True)
        if nunique > 100:
            cols_large_unique_cat.append(col)
 
    # All values missing
    if df.isnull().all(axis=None):
        issues.append("The file has no usable data (all values are missing).")
 
    # Mixed types in columns
    for col in df.columns:
        dtypes = df[col].dropna().map(type).unique()
        if len(dtypes) > 1:
            cols_mixed_type.append(col)
 
    # --------- Now build grouped, concise issues:
 
    if cols_blank_name:
        issues.append(f"{len(cols_blank_name)} column(s) had no name or were marked 'Unnamed'/'Untitled'. Auto-renamed.")
    if cols_number_name:
        issues.append(f"{len(cols_number_name)} column(s) had names that were only numbers.")
    if cols_special_name:
        issues.append(f"{len(cols_special_name)} column(s) had special characters in their name.")
    if cols_whitespace_name:
        issues.append(f"{len(cols_whitespace_name)} column(s) had leading or trailing spaces in their name.")
 
    if cols_duplicate:
        issues.append(f"Duplicate columns detected: {', '.join(cols_duplicate)}.")
 
    if cols_empty:
        issues.append(f"{len(cols_empty)} column(s) are completely empty: {', '.join(cols_empty)}.")
 
    if cols_constant:
        issues.append(
            f"{len(cols_constant)} column(s) have the same value in every row (not useful for analysis): {', '.join(cols_constant)}."
        )
 
    # Grouped missing value reporting
    if cols_with_missing:
        col_list = [f"{col} ({missing_counts[col]} missing)" for col in cols_with_missing]
        issues.append(f"{len(cols_with_missing)} column(s) have missing values: {', '.join(col_list)}.")
 
    # Grouped low unique reporting
    if cols_few_unique:
        col_list = [f"{col} ({few_unique_counts[col]} unique)" for col in cols_few_unique]
        issues.append(f"{len(cols_few_unique)} column(s) have very few unique values: {', '.join(col_list)}.")
 
    if cols_num_text:
        issues.append(f"{len(cols_num_text)} column(s) are mostly numbers but stored as text: {', '.join(cols_num_text)}.")
    if cols_text_num:
        issues.append(f"{len(cols_text_num)} column(s) are mostly text but stored as numbers: {', '.join(cols_text_num)}.")
 
    # Grouped invalid emails
    if cols_invalid_email:
        col_list = [f"{col} ({invalid_email_counts[col]} invalid)" for col in cols_invalid_email]
        issues.append(f"{len(cols_invalid_email)} column(s) have values that may not be valid email addresses: {', '.join(col_list)}.")
 
    # Grouped invalid phones
    if cols_invalid_phone:
        col_list = [f"{col} ({invalid_phone_counts[col]} invalid)" for col in cols_invalid_phone]
        issues.append(f"{len(cols_invalid_phone)} column(s) have values that may not be valid phone numbers: {', '.join(col_list)}.")
 
    if cols_inconsistent_date:
        issues.append(f"{len(cols_inconsistent_date)} column(s) have inconsistent date formats: {', '.join(cols_inconsistent_date)}.")
    if cols_inconsistent_country:
        issues.append(f"{len(cols_inconsistent_country)} column(s) may have inconsistent country names: {', '.join(cols_inconsistent_country)}.")
 
    if cols_special_chars:
        issues.append(f"{len(cols_special_chars)} column(s) have many values containing special characters: {', '.join(cols_special_chars)}.")
 
    if cols_extreme:
        issues.append(f"{len(cols_extreme)} column(s) have extremely large or small values: {', '.join(cols_extreme)}.")
 
    if cols_name_special:
        issues.append(f"{len(cols_name_special)} column(s) have spaces or special characters in their name: {', '.join(cols_name_special)}.")
 
    if cols_large_unique_cat:
        issues.append(f"{len(cols_large_unique_cat)} column(s) have over 100 unique values: {', '.join(cols_large_unique_cat)}.")
 
    if cols_mixed_type:
        issues.append(f"{len(cols_mixed_type)} column(s) contain mixed data types: {', '.join(cols_mixed_type)}.")
 
    return issues
 
 
 
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows that are completely empty
    df = df.dropna(how='all')
    new_df = df.copy()
   
    for col in new_df.columns:
        col_lower = col.lower()
        if pd.api.types.is_numeric_dtype(new_df[col]):
            continue
        elif "date" in col_lower:
            # Use vectorized conversion for dates
            new_df[col] = pd.to_datetime(new_df[col], errors='coerce')
        else:
            # Convert column to string and use vectorized string methods
            new_df[col] = new_df[col].astype(str).str.strip()
            # Replace 'none' or 'null' (case-insensitive) with pd.NA
            new_df[col] = new_df[col].replace(to_replace=r'^(none|null)$', value=pd.NA, regex=True)
           
            if "email" in col_lower:
                new_df[col] = new_df[col].str.strip()
            elif "phone" in col_lower:
                new_df[col] = new_df[col].str.replace(r'\D', '', regex=True)
                # For phone numbers, format if length exactly 10
                new_df[col] = new_df[col].apply(lambda x: f"{x[:3]}-{x[3:6]}-{x[6:]}" if pd.notna(x) and len(x)==10 else x)
            elif "country" in col_lower:
                new_df[col] = new_df[col].str.replace(r'[^\w\s]', '', regex=True).str.strip().str.upper()
            else:
                new_df[col] = new_df[col].str.lower().str.strip()
   
    # Remove rows that are mostly empty (at least 2 non-empty cells)
    row_mask = new_df.apply(lambda row: row.astype(str).str.strip().replace({"": None, "none": None, "nan": None}).notna().sum(), axis=1) >= 2
    new_df = new_df[row_mask]
   
    # Remove columns that have fewer than 2 non-empty cells
    col_mask = new_df.apply(lambda col: col.astype(str).str.strip().replace({"": None, "none": None, "nan": None}).notna().sum()) >= 2
    new_df = new_df.loc[:, col_mask]
   
    new_df = new_df.drop_duplicates()
    return new_df
 
def rename_case_conflict_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = {}
    new_columns = []
    for col in df.columns:
        norm_col = col.lower()
        if norm_col in normalized:
            i = 2
            new_col = f"{col}_{i}"
            while new_col.lower() in [c.lower() for c in new_columns]:
                i += 1
                new_col = f"{col}_{i}"
            new_columns.append(new_col)
        else:
            normalized[norm_col] = True
            new_columns.append(col)
    df.columns = new_columns
    return df
 
def comprehensive_data_cleaning(df: pd.DataFrame, file_name: str, llm) -> tuple[pd.DataFrame, str]:
    # Rename columns to avoid case conflicts.
    df = rename_case_conflict_columns(df)
    errors = validate_data(df, file_name)
    if errors:
        from app.utils.llm_helpers import generate_data_issue_summary
        summary = generate_data_issue_summary(errors, file_name, llm)
        df_cleaned = clean_data(df.copy())
        return df_cleaned, summary
    else:
        return df, "No issues found."
   
 
   
 
 
 
 