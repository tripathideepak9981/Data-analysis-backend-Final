# app/utils/cleaning.py
import re
import pandas as pd
import spacy

# Load spaCy model globally for NLP tasks
NLP_MODEL = spacy.load("en_core_web_sm")

def validate_data(df: pd.DataFrame, file_name: str) -> list:
    messages = []
    messages.append(f"Issues found in '{file_name}':\n")
    normalized_columns = [str(col).strip().lower() for col in df.columns]
    dup_cols = [col for col in set(normalized_columns) if normalized_columns.count(col) > 1]
    if dup_cols:
        messages.append(f"• Duplicate columns found: {', '.join(dup_cols)}.")
    if df.isnull().all(axis=1).any():
        messages.append("• Some rows are completely empty.")
    dup_rows = df.duplicated(keep=False).sum()
    if dup_rows > 0:
        messages.append(f"• Duplicate rows: {dup_rows} row(s) are identical.")
    for col in df.columns:
        col_errors = []
        null_count = df[col].isnull().sum()
        if null_count > 0:
            col_errors.append(f"{null_count} missing value{'s' if null_count > 1 else ''}")
        detected_type = None
        if pd.api.types.is_numeric_dtype(df[col]):
            detected_type = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            detected_type = "date"
        else:
            non_missing = df[col].dropna()
            if len(non_missing) > 0:
                date_converted = pd.to_datetime(non_missing, errors='coerce',format='%Y-%m-%d')
                ratio_date = date_converted.notna().sum() / len(non_missing)
                if ratio_date >= 0.8:
                    detected_type = "date"
                else:
                    numeric_converted = pd.to_numeric(non_missing, errors='coerce')
                    ratio_numeric = numeric_converted.notna().sum() / len(non_missing)
                    if ratio_numeric >= 0.8:
                        detected_type = "numeric"
                        if ratio_numeric < 1.0:
                            col_errors.append("some non-numeric entries present")
                    else:
                        detected_type = "varchar"
            else:
                detected_type = "varchar"
        col_lower = col.lower()
        if detected_type == "date" or "date" in col_lower:
            try:
                pd.to_datetime(df[col], errors='raise')
            except Exception:
                col_errors.append("inconsistent date formats")
        if "phone" in col_lower:
            phone_pattern = re.compile(r"^\+?\d[\d\s\-]{7,}\d$")
            invalid = df[col].astype(str).apply(lambda x: not bool(phone_pattern.match(x.strip())))
            if invalid.sum() > 0:
                col_errors.append("inconsistent phone number format")
        if "email" in col_lower:
            email_pattern = re.compile(r"[^@]+@[^@]+\.[^@]+")
            invalid = df[col].astype(str).apply(lambda x: not bool(email_pattern.fullmatch(x.strip())))
            if invalid.sum() > 0:
                col_errors.append("possible invalid email addresses")
        if "country" in col_lower:
            unique_vals = df[col].dropna().unique()
            normalized_vals = [re.sub(r'[\W_]+', '', str(val).lower()) for val in unique_vals]
            if len(set(normalized_vals)) > 1:
                col_errors.append("inconsistent country name formats")
        if col_errors:
            message = (
                f"\nColumn: '{col}'\n"
                f"  - Detected type: {detected_type}\n"
                f"  - Issues: \n    - " + "\n    - ".join(col_errors)
            )
            messages.append(message)
    return messages

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how='all')
    new_df = df.copy()
    for col in new_df.columns:
        col_lower = col.lower()
        if pd.api.types.is_numeric_dtype(new_df[col]):
            continue
        elif "date" in col_lower:
            new_df[col] = pd.to_datetime(new_df[col], errors='coerce')
        else:
            new_df[col] = new_df[col].fillna(pd.NA)
            def convert_val(x):
                if pd.isna(x):
                    return x
                val = str(x).strip()
                if val.lower() in ["none", "null"]:
                    return pd.NA
                return val
            new_df[col] = new_df[col].apply(convert_val)
            if "email" in col_lower:
                new_df[col] = new_df[col].apply(lambda x: x.strip() if pd.notna(x) else x)
            elif "phone" in col_lower:
                new_df[col] = new_df[col].apply(lambda x: re.sub(r'\D', '', x) if pd.notna(x) else x)
                new_df[col] = new_df[col].apply(lambda x: f"{x[:3]}-{x[3:6]}-{x[6:]}" if pd.notna(x) and len(x)==10 else x)
            elif "country" in col_lower:
                new_df[col] = new_df[col].apply(lambda x: re.sub(r'[^\w\s]', '', x).strip().upper() if pd.notna(x) else x)
            else:
                new_df[col] = new_df[col].apply(lambda x: x.strip().lower() if pd.notna(x) else x)
    def row_is_missing(row):
        non_empty_count = sum(1 for cell in row if pd.notna(cell) and str(cell).strip().lower() not in ["", "none", "nan"])
        return non_empty_count < 2
    new_df = new_df[~new_df.apply(row_is_missing, axis=1)]
    def col_is_missing(col):
        non_empty_count = sum(1 for cell in col if pd.notna(cell) and str(cell).strip().lower() not in ["", "none", "nan"])
        return non_empty_count < 2
    new_df = new_df.loc[:, ~new_df.apply(col_is_missing)]
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
        # Use LLM to generate a detailed issue summary.
        from app.utils.llm_helpers import generate_data_issue_summary
        summary = generate_data_issue_summary(errors, file_name, llm)
        df_cleaned = clean_data(df.copy())
        return df_cleaned, summary
    else:
        return df, "No issues found."
