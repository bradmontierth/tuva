"""
Runs inference using a prospective model bundle and evaluates its performance
on the inference dataset.

This script performs the following steps:
1.  Loads a model bundle from a specified Snowflake stage.
2.  Fetches the data for the year to be scored, including feature data AND
    the actual outcome values (targets) for evaluation.
3.  Applies the exact same feature engineering transformations as the training script.
4.  For each model in the bundle, it generates predictions and applies the
    stored calibration factor.
5.  Calculates evaluation metrics (MAE, MAE Percent, R2, P/A Ratio) by comparing
    the calibrated predictions to the actual outcomes.
6.  Logs these evaluation metrics to a wide-format table, flagging them
    as an 'INFERENCE' set evaluation.
7.  Writes the individual predictions to a separate predictions table, preserving
    the `BENCHMARK_KEY`.

Requirements:
  pip install pandas numpy scikit-learn xgboost snowflake-snowpark-python
"""

from __future__ import annotations

import os
import pickle
import tempfile
from datetime import datetime
from typing import List, Dict, Tuple, Any

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, r2_score
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkClientException

# ---- CONFIG ----
# Input Data and Model Location
INPUT_DATABASE  = "MEDICARE_LDS_FIVE_MULTI_YEAR"
INPUT_SCHEMA    = "BENCHMARKS"
INPUT_TABLE     = f"{INPUT_DATABASE}.{INPUT_SCHEMA}.PERSON_YEAR_PROSPECTIVE"
STAGE_DATABASE  = "MEDICARE_LDS_FIVE_PERCENT"
STAGE_SCHEMA    = "BENCHMARKS"
SNOWFLAKE_STAGE_NAME = f"@{STAGE_DATABASE}.{STAGE_SCHEMA}.MODEL_STAGE"

# --- Model to Use ---
MODEL_YEAR_TAG = 2023
MODEL_BUNDLE_FILENAME = f"{MODEL_YEAR_TAG}_prospective_models_bundle.pkl"

# --- Inference and Output Config ---
SCORING_YEAR = 2023
ROW_LIMIT = 10000

OUTPUT_DATABASE = "MEDICARE_LDS_FIVE_MULTI_YEAR"
OUTPUT_SCHEMA   = "BENCHMARKS"
PREDICTIONS_TABLE_NAME = f"{OUTPUT_DATABASE}.{OUTPUT_SCHEMA}.pmpm_predictions_prospective"
METRICS_TABLE_NAME = f"{OUTPUT_DATABASE}.{OUTPUT_SCHEMA}.PROSP_MODEL_EVAL_METRICS"


# ------------------------- Snowflake Table Helpers -------------------------

def create_predictions_table_if_not_exists(session: Session, table_name: str, model_keys: List[str]):
    """Dynamically creates the predictions table based on the models in the bundle."""
    columns_sql = [
        "BENCHMARK_KEY STRING", "MODEL_RUN_ID STRING"
    ]
    for key in model_keys:
        columns_sql.append(f"PRED_{key.upper()} FLOAT")
    create_table_ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_sql)});"
    session.sql(create_table_ddl).collect()
    print(f"Ensured predictions table '{table_name}' exists.")

def create_metrics_table_if_not_exists(session: Session, table_name: str):
    """Creates the evaluation metrics table in a wide format if it does not already exist."""
    create_table_ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        MODEL_RUN_ID STRING,
        MODEL_SOURCE_TAG STRING,
        TARGET_TYPE STRING,
        TARGET_NAME STRING,
        EVAL_TS TIMESTAMP_NTZ,
        N_ROWS FLOAT,
        MAE_ANNUAL FLOAT,
        MAE_PERCENT_ANNUAL FLOAT,
        R2_ANNUAL FLOAT,
        PREDICTION_RATIO FLOAT
    );
    """
    session.sql(create_table_ddl).collect()
    print(f"Ensured metrics table '{table_name}' exists.")


# ------------------------- Data & Modeling Helpers -------------------------

def _standardize_columns_case(df: pd.DataFrame, to: str = "lower") -> pd.DataFrame:
    df.columns = [str(c).lower() if to == "lower" else str(c).upper() for c in df.columns]
    return df

def fetch_data_for_scoring_and_eval(session: Session, model_keys: List[str]) -> pd.DataFrame:
    """Fetches feature data, member months, and actual target values for the specified scoring year."""
    base_cols = [
        "BENCHMARK_KEY", "PREDICTION_YEAR_MEMBER_MONTHS", "PREDICTION_YEAR_SEX",
        "PREDICTION_YEAR_RACE", "PREDICTION_YEAR_STATE",
        "PREDICTION_YEAR_AGE_AT_YEAR_START", "COLD_START",
    ]
    lag_cols_sql = f"""
        SELECT LISTAGG(column_name, ', ')
        FROM {INPUT_DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{INPUT_SCHEMA}' AND table_name = 'PERSON_YEAR_PROSPECTIVE'
        AND (column_name ILIKE 'lag_cond_%' OR column_name ILIKE 'lag_cms_%' OR column_name ILIKE 'lag_hcc%')
    """
    lag_cols_str = session.sql(lag_cols_sql).collect()[0][0]

    target_cols = []
    for key in model_keys:
        t_type, t_name = key.split('_', 1)
        if t_type == 'PMPM':
            col = "PREDICTION_YEAR_PMPM_PAID_AMOUNT" if t_name == 'OVERALL' else f"PREDICTION_YEAR_PMPM_{t_name}_PAID_AMOUNT"
        else: # PMPC
            col = f"PREDICTION_YEAR_PMPC_{t_name}_COUNT"
        target_cols.append(col)

    select_list = ",".join(list(dict.fromkeys(base_cols + target_cols))) + f", {lag_cols_str}"
    limit_clause = f" LIMIT {ROW_LIMIT}" if ROW_LIMIT is not None else ""

    sql = f"""
        SELECT {select_list} FROM {INPUT_TABLE}
        WHERE PREDICTION_YEAR = {SCORING_YEAR} {limit_clause}
    """
    df = session.sql(sql).to_pandas()
    return _standardize_columns_case(df, to="lower")

def prep_data_for_inference(df: pd.DataFrame, feature_list: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """Prepares the raw dataframe for inference and separates features, targets, and keys."""
    df = df.copy()
    benchmark_keys = df["benchmark_key"]
    member_months = pd.to_numeric(df["prediction_year_member_months"], errors="coerce").fillna(0)
    
    cat_dummies = pd.get_dummies(
        df[["prediction_year_sex", "prediction_year_race", "prediction_year_state"]].astype("category"),
        drop_first=True, prefix=["sex", "race", "state"]
    )
    df["prediction_year_age_at_year_start"] = pd.to_numeric(df["prediction_year_age_at_year_start"], errors="coerce")
    df["cold_start"] = pd.to_numeric(df["cold_start"], errors="coerce").fillna(0).clip(0, 1)
    
    lag_cols_present = [col for col in df.columns if col.startswith('lag_')]
    lag_block = df[lag_cols_present].apply(pd.to_numeric, errors="coerce").fillna(0)

    X = pd.concat([cat_dummies, df[["prediction_year_age_at_year_start", "cold_start"]], lag_block], axis=1)
    X["prediction_year_age_at_year_start"] = X["prediction_year_age_at_year_start"].fillna(X["prediction_year_age_at_year_start"].median())
    X_aligned = X.reindex(columns=feature_list, fill_value=0)
    
    # Extract target columns into a separate DataFrame
    target_cols = [col for col in df.columns if 'pmpm' in col or 'pmpc' in col]
    Y_targets = df[target_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    
    return X_aligned, Y_targets, member_months, benchmark_keys

def calculate_inference_metrics(
    y_actual_metric: pd.Series, y_pred_metric: pd.Series, member_months: pd.Series
) -> Dict[str, float]:
    """Calculates evaluation metrics based on annualized values."""
    pred_annual, actual_annual = y_pred_metric * member_months, y_actual_metric * member_months
    mae_annual = mean_absolute_error(actual_annual, pred_annual)
    mean_actual_annual = actual_annual.mean()

    if actual_annual.sum() == 0:
        # Avoid division by zero if there are no actual costs/counts
        pa_ratio = np.nan
        r2 = np.nan
    else:
        pa_ratio = pred_annual.sum() / actual_annual.sum()
        r2 = r2_score(actual_annual, pred_annual)

    if mean_actual_annual == 0:
        mae_percent = np.nan
    else:
        mae_percent = mae_annual / mean_actual_annual

    return {
        "n_rows": float(len(y_actual_metric)),
        "mae_annual": float(mae_annual),
        "mae_percent": float(mae_percent),
        "r2_annual": float(r2),
        "prediction_ratio": float(pa_ratio),
    }

def load_model_bundle(session: Session, stage_name: str, file_name: str) -> Dict[str, Any]:
    """Downloads and unpickles the model bundle from a Snowflake stage."""
    with tempfile.TemporaryDirectory() as temp_dir:
        local_file_path = os.path.join(temp_dir, file_name)
        print(f"Downloading '{file_name}' from stage '{stage_name}'...")
        session.file.get(f"{stage_name}/{file_name}", temp_dir)
        with open(local_file_path, 'rb') as f:
            bundle = pickle.load(f)
            print("Model bundle loaded successfully.")
            return bundle

# ------------------------- Main Inference Logic -------------------------

def main(session: Session):
    print(f"--- INFERENCE SCRIPT START: {datetime.utcnow()} UTC ---")
    print(f"Scoring data for year: {SCORING_YEAR}")

    # 1. Load Model Bundle
    model_bundle = load_model_bundle(session, SNOWFLAKE_STAGE_NAME, MODEL_BUNDLE_FILENAME)
    
    models = model_bundle['models']
    cal_factors = model_bundle['calibration_factors']
    f_list = model_bundle['features']
    run_id = model_bundle['model_run_id']
    
    print(f"Loaded bundle from MODEL_RUN_ID: {run_id}")

    # 2. Fetch and Prepare Data
    print("\nFetching data for scoring and evaluation...")
    df_raw = fetch_data_for_scoring_and_eval(session, list(models.keys()))
    if df_raw.empty: raise RuntimeError(f"No rows found in {INPUT_TABLE} for PREDICTION_YEAR = {SCORING_YEAR}.")
    X_inf, Y_inf, mm_inf, keys_inf = prep_data_for_inference(df_raw, f_list)
    print(f"Prepared {len(X_inf)} rows for inference.")

    # 3. Run Inference
    print("\nRunning predictions...")
    predictions_df = pd.DataFrame({'BENCHMARK_KEY': keys_inf})
    for model_key, model in models.items():
        raw_preds = model.predict(X_inf)
        cal_factor = cal_factors.get(model_key, 1.0) or 1.0
        calibrated_preds = np.maximum(0, raw_preds / cal_factor)
        predictions_df[f"PRED_{model_key.upper()}"] = calibrated_preds
    
    # 4. Evaluate Predictions and Log Metrics
    print("\nCalculating and logging evaluation metrics for inference set...")
    eval_results = []
    model_source_tag_from_training = "prospective_encounters_2023"
    
    for model_key in models.keys():
        t_type, t_name = model_key.split('_', 1)
        
        if t_type.lower() == 'pmpm':
            actual_col_name = "prediction_year_pmpm_paid_amount" if t_name.lower() == 'overall' else f"prediction_year_pmpm_{t_name.lower()}_paid_amount"
        else:
            actual_col_name = f"prediction_year_pmpc_{t_name.lower()}_count"

        y_actual = Y_inf[actual_col_name]
        y_pred = predictions_df[f"PRED_{model_key.upper()}"]

        # Calculate metrics, which returns a dictionary with the desired metric columns
        metrics = calculate_inference_metrics(y_actual, y_pred, mm_inf)
        
        # Create a single record (dictionary) for this model target to be a row in the wide table
        record = {
            'MODEL_RUN_ID': run_id,
            'MODEL_SOURCE_TAG': model_source_tag_from_training,
            'TARGET_TYPE': t_type,
            'TARGET_NAME': t_name,
            'EVAL_TS': datetime.utcnow(),
            **metrics  # Unpack the metrics dictionary into this record
        }
        eval_results.append(record)

    if eval_results:
        # Convert the list of dictionaries directly into a wide DataFrame
        final_metrics_df = pd.DataFrame(eval_results)

        # Rename columns to uppercase to match Snowflake table DDL
        final_metrics_df = final_metrics_df.rename(columns={
            "n_rows": "N_ROWS",
            "mae_annual": "MAE_ANNUAL",
            "mae_percent": "MAE_PERCENT_ANNUAL",
            "r2_annual": "R2_ANNUAL",
            "prediction_ratio": "PREDICTION_RATIO"
        })
        
        # Ensure the target table exists with the new wide schema
        create_metrics_table_if_not_exists(session, METRICS_TABLE_NAME)

        # Define the column order to match the table DDL to ensure correct insertion
        metrics_table_cols_ordered = [
            'MODEL_RUN_ID', 'MODEL_SOURCE_TAG', 'TARGET_TYPE', 'TARGET_NAME',
            'EVAL_TS', 'N_ROWS', 'MAE_ANNUAL', 'MAE_PERCENT_ANNUAL', 'R2_ANNUAL', 'PREDICTION_RATIO'
        ]
        final_metrics_df_ordered = final_metrics_df[metrics_table_cols_ordered]
        
        session.create_dataframe(final_metrics_df_ordered).write.mode("append").save_as_table(METRICS_TABLE_NAME)
        print(f"Logged {len(final_metrics_df)} metric records (wide format) to {METRICS_TABLE_NAME}")

    # 5. Write Predictions to Snowflake
    predictions_df['MODEL_RUN_ID'] = run_id
    final_cols = ['BENCHMARK_KEY', 'MODEL_RUN_ID'] + [c for c in predictions_df if c.startswith('PRED_')]
    
    print(f"\nWriting {len(predictions_df)} predictions to {PREDICTIONS_TABLE_NAME}...")
    create_predictions_table_if_not_exists(session, PREDICTIONS_TABLE_NAME, list(models.keys()))
    # Enable auto-create so Snowflake creates the table if missing.
    session.write_pandas(
        predictions_df[final_cols],
        table_name=PREDICTIONS_TABLE_NAME.split('.')[-1],
        schema=OUTPUT_SCHEMA,
        database=OUTPUT_DATABASE,
        auto_create_table=True,
        overwrite=False,
        table_type=""
    )
    print("Successfully wrote predictions to Snowflake.")
    print(f"\n--- INFERENCE SCRIPT END: {datetime.utcnow()} UTC ---")

if __name__ == "__main__":
    try:
        snowpark_session = get_active_session()
        print("Using active Snowpark session.")
    except SnowparkClientException:
        print("No active session. Creating a new Snowpark session from local credentials...")
        snowpark_session = Session.builder.create()
    main(snowpark_session)
