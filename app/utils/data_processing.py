# app/utils/data_processing.py
import pandas as pd
import re
import math

def load_data(file) -> pd.DataFrame:
    try:
        if file.filename.endswith(".csv"):
            return pd.read_csv(file.file)
        elif file.filename.endswith(".xlsx"):
            return pd.read_excel(file.file)
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"Error loading file {file.filename}: {e}")
        return pd.DataFrame()

def generate_table_name(file_name: str) -> str:
    return file_name.split('.')[0].replace(" ", "_").lower()

def clean_nan(obj):
    """
    Recursively traverse lists/dictionaries and replace any NaN float values with None.
    """
    if isinstance(obj, list):
        return [clean_nan(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        if math.isnan(obj):
            return None
        return obj
    else:
        return obj

def get_data_preview(df: pd.DataFrame, max_rows=10, max_columns=10) -> list:
    if df.shape[1] > max_columns:
        preview_df = df.iloc[:max_rows, :max_columns]
    else:
        preview_df = df.head(max_rows)
    # Convert the DataFrame to a dict and clean any NaN values recursively.
    preview = preview_df.to_dict(orient="records")
    preview = clean_nan(preview)
    return preview

def generate_detailed_overview_in_memory(table_names: list) -> str:
    overview_text_parts = []
    for tname, df in table_names:
        row_count = len(df)
        numeric_cols = df.select_dtypes(include=["int", "float"])
        if not numeric_cols.empty:
            desc = numeric_cols.describe().T
            stats_info = []
            for col, row_data in desc.iterrows():
                stats_info.append(
                    f"- {col}: min={row_data['min']:.2f}, max={row_data['max']:.2f}, mean={row_data['mean']:.2f}, std={row_data['std']:.2f}"
                )
            numeric_stats_text = "Numeric columns summary:\n" + "\n".join(stats_info)
        else:
            numeric_stats_text = "(No numeric columns found.)"
        categorical_cols = df.select_dtypes(include=["object"])
        if not categorical_cols.empty:
            cat_info = []
            for col in categorical_cols.columns:
                value_counts = df[col].value_counts(dropna=False).head(3)
                top_vals = ", ".join([f"{idx} ({count})" for idx, count in value_counts.items()])
                cat_info.append(f"- {col}: top values → {top_vals}")
            categorical_stats_text = "Categorical columns summary:\n" + "\n".join(cat_info)
        else:
            categorical_stats_text = "(No categorical columns found.)"
        block = (
            f"Table: {tname}\n"
            f"Row Count: {row_count}\n"
            f"{numeric_stats_text}\n"
            f"{categorical_stats_text}\n"
            "----\n"
        )
        overview_text_parts.append(block)
    return "\n".join(overview_text_parts)
