import os
import shutil
import pickle
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import requests

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col as spark_col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, LongType, FloatType
)
from sklearn.metrics import (
    r2_score,
    mean_absolute_error,
)

# ==========================================================================
# 0. SCRIPT CONFIGURATION & PARAMETERS
# ==========================================================================
# --- Notebook/Execution Parameters ---
DEBUG_MODE = False
ROW_LIMIT = 10000 if DEBUG_MODE else None 
HF_TOKEN = "xxx" 

# --- Model & Data Configuration ---
MODEL_YEAR_TAG = 2023
MODEL_NAME = f"{MODEL_YEAR_TAG}_prospective_models_bundle.pkl"
HF_REPO_BASE_URL = "https://huggingface.co/tuva-ml-models/medicare-cost-encounter-model-prospective/resolve/main"
MODEL_FILE_URL = f"{HF_REPO_BASE_URL}/{MODEL_NAME}"
FORCE_DOWNLOAD = True
MODEL_ALIAS = f"prospective_{MODEL_YEAR_TAG}"

# --- Medicare Price Adjustment Factors ---
# Factors to scale 2023-trained paid amount predictions into target-year dollars.
# Counts/utilization predictions (PMPC) are NOT adjusted.
ADJUSTMENT_FACTORS_BY_YEAR = {
    2023: 1.0,   # Baseline year of the model
    2024: 1.072,  # 7% increase from 2023 to 2024
    2025: 1.126, # 12% increase from 2023 to 2025
    2026: 1.174, # 17.4% increase from 2023 to 2026
}

def get_adjustment_factor(year: int) -> float:
    """Return Medicare price adjustment factor for a given year.

    - 2023 baseline is 1.0.
    - For years after 2026, apply an additional 5% per year: 1.174 * 1.05^(year-2026).
    - For unknown/past years, default to 1.0.
    """
    try:
        y = int(year)
    except Exception:
        return 1.0
    if y in ADJUSTMENT_FACTORS_BY_YEAR:
        return ADJUSTMENT_FACTORS_BY_YEAR[y]
    if y > 2026:
        return ADJUSTMENT_FACTORS_BY_YEAR[2026] * (1.05 ** (y - 2026))
    return 1.0

# --- Fabric/Lakehouse Configuration ---
FABRIC_WORKSPACE_NAME = "phds_tuva_test"
LAKEHOUSE_NAME = "test_lakehouse_mirror"
LAKEHOUSE_SCHEMA_FOLDER = "benchmarks"
OUTPUT_SCHEMA_FOLDER = "benchmark_output"
spark = SparkSession.builder.appName("ProspectiveModelPrediction").getOrCreate()

# --- Paths ---
LAKEHOUSE_BASE_DIR = "/lakehouse/default/Files/"
MODEL_DESTINATION_DIR = os.path.join(LAKEHOUSE_BASE_DIR, "tuva_prospective_models")
MODEL_PATH = os.path.join(MODEL_DESTINATION_DIR, MODEL_NAME)

INPUT_TABLE_NAME_BASE = "person_year_prospective"
INPUT_TABLE_ABFSS_PATH = f"abfss://{FABRIC_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}.Lakehouse/Tables/{LAKEHOUSE_SCHEMA_FOLDER}/{INPUT_TABLE_NAME_BASE}"
OUTPUT_TABLES_ABFSS_BASE_DIR = f"abfss://{FABRIC_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}.Lakehouse/Tables/{OUTPUT_SCHEMA_FOLDER}"

PREDICTIONS_TABLE_BASE_NAME = "pmpm_predictions_prospective"
METRICS_TABLE_BASE_NAME = "pmpm_predictions_prospective_eval_metrics"
PREDICTIONS_TABLE_ABFSS_PATH = f"{OUTPUT_TABLES_ABFSS_BASE_DIR}/{PREDICTIONS_TABLE_BASE_NAME}"
METRICS_TABLE_ABFSS_PATH = f"{OUTPUT_TABLES_ABFSS_BASE_DIR}/{METRICS_TABLE_BASE_NAME}"
FEATURE_FREQ_TABLE_BASE_NAME = "prosp_feature_frequency"
FEATURE_FREQ_TABLE_ABFSS_PATH = f"{OUTPUT_TABLES_ABFSS_BASE_DIR}/{FEATURE_FREQ_TABLE_BASE_NAME}"

# --- Column Names ---
BENCHMARK_KEY_COL = "benchmark_key"
DATA_SOURCE_COL = "data_source"
PREDICTION_YEAR_COL = "prediction_year"

# ==========================================================================
# 1. UTILITY & HELPER FUNCTIONS
# ==========================================================================

def download_file_from_hf(file_url: str, token: str, destination_dir: str, destination_filename: str, force_download: bool = False):
    print("--- Starting File Download Process ---")
    # Basic token sanity: allow anonymous or an hf_ token, otherwise fail fast
    if not token or token.startswith("hf_"):
        pass
    else:
        raise ValueError("CRITICAL: Aborting download due to invalid or missing Hugging Face token.")

    os.makedirs(destination_dir, exist_ok=True)
    full_destination_path = os.path.join(destination_dir, destination_filename)

    exists_already = os.path.exists(full_destination_path)
    print(f"force_download={force_download}, exists={exists_already}, dest={full_destination_path}")

    # If not forcing and file exists, return early
    if not force_download and exists_already:
        file_size_mb = os.path.getsize(full_destination_path) / (1024 * 1024)
        print(f"✅ File already exists in Lakehouse: {full_destination_path} ({file_size_mb:.2f} MB)")
        return full_destination_path

    # If forcing and a file exists, remove it first to ensure a clean write
    if force_download and exists_already:
        try:
            os.remove(full_destination_path)
            print(f"🔁 FORCE: Removed existing file: {full_destination_path}")
        except Exception as e:
            print(f"⚠️ WARNING: Could not remove existing file before download: {e}")

    print(f"Attempting to download file from: {file_url}")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/octet-stream"}
    # Write temp file in the destination directory to avoid cross-filesystem issues
    temp_download_path = os.path.join(destination_dir, destination_filename + ".tmp")
    try:
        with requests.get(file_url, headers=headers, stream=True, allow_redirects=True, timeout=(10, 300)) as r:
            r.raise_for_status()
            total_bytes = 0
            with open(temp_download_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192 * 16):
                    if not chunk:
                        continue
                    f.write(chunk)
                    total_bytes += len(chunk)
            # Ensure data is flushed to disk before moving
        # Move into place atomically where possible
        shutil.move(temp_download_path, full_destination_path)
        file_size_mb = os.path.getsize(full_destination_path) / (1024 * 1024)
        print(f"✅ File successfully downloaded ({file_size_mb:.2f} MB) to: {full_destination_path}")
        return full_destination_path
    except Exception as e:
        print(f"❌ ERROR downloading file: {e}")
        raise
    finally:
        try:
            if os.path.exists(temp_download_path):
                os.remove(temp_download_path)
        except Exception:
            pass

def load_model_bundle_from_lakehouse(model_path: str):
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model bundle not found at path: {model_path}.")
    file_size_mb = os.path.getsize(model_path) / (1024 * 1024)
    mtime = datetime.fromtimestamp(os.path.getmtime(model_path), tz=timezone.utc).isoformat()
    with open(model_path, "rb") as f:
        bundle = pickle.load(f)
    print(f"✅ Loaded model bundle from {model_path} ({file_size_mb:.2f} MB, mtime: {mtime})")
    return bundle

def save_spark_df_to_delta_abfss(spark_df: DataFrame, table_abfss_path: str, mode: str = "overwrite", partition_by: list = None):
    print(f"Saving Spark DataFrame to Delta path: {table_abfss_path} (mode: {mode})")
    try:
        df_to_save = spark_df.toDF(*[c.lower() for c in spark_df.columns])
        
        writer = (df_to_save.write
                  .format("delta")
                  .mode(mode)
                  # Add this line to solve the problem
                  .option("overwriteSchema", "true"))
        
        if partition_by:
            print(f"Partitioning by: {partition_by}")
            writer = writer.partitionBy(*partition_by)
            # Note: When using mode("overwrite") on the whole table, 
            # partitionOverwriteMode is redundant, but leaving it doesn't hurt.
            # writer = writer.option("partitionOverwriteMode", "dynamic")
        
        writer.save(table_abfss_path)
        print(f"✅ Successfully saved to {table_abfss_path}")
    except Exception as e:
        print(f"❌ ERROR saving Spark DataFrame to {table_abfss_path}: {e}")
        raise

# REFACTORED: This function now loads all data at once, not year-by-year.
def load_and_prepare_all_data(spark_session: SparkSession, table_path: str, feature_list: list, row_limit: int = None) -> dict:
    print(f"Loading all data from Delta path: {table_path}")
    spark_df_full = spark_session.read.format("delta").load(table_path)
    
    spark_df = spark_df_full.limit(row_limit) if row_limit else spark_df_full
    
    loaded_rows = spark_df.count()
    print(f"Loaded {loaded_rows} total rows for processing.")
    if loaded_rows == 0:
        print(f"WARNING: No rows found in {table_path}. Aborting.")
        return None

    # Note: .toPandas() can cause memory issues on the driver node for very large datasets.
    # For massive scale, a distributed approach (e.g., pandas_udf) would be required.
    df = spark_df.toPandas()
    df.columns = [c.lower() for c in df.columns]

    # --- Feature Engineering and Metadata Extraction ---
    print("Starting feature preparation for the entire dataset...")
    # Extract metadata columns needed for output and evaluation
    metadata_cols = [BENCHMARK_KEY_COL, PREDICTION_YEAR_COL, DATA_SOURCE_COL]
    for col in metadata_cols:
        if col not in df.columns:
            raise KeyError(f"Required metadata column '{col}' not found in the input data.")
    
    metadata = df[metadata_cols]
    member_months = pd.to_numeric(df["prediction_year_member_months"], errors="coerce").fillna(0)
    
    # One-Hot Encoding
    cat_cols = ["prediction_year_sex", "prediction_year_race", "prediction_year_state"]
    cat_dummies = pd.get_dummies(df[cat_cols].astype("category"), drop_first=True, prefix=["sex", "race", "state"])
    
    # Numeric and Binary Features
    df["prediction_year_age_at_year_start"] = pd.to_numeric(df["prediction_year_age_at_year_start"], errors="coerce")
    df["cold_start"] = pd.to_numeric(df["cold_start"], errors="coerce").fillna(0).clip(0, 1)
    
    # Lag Features
    lag_cols_present = [col for col in df.columns if col.startswith('lag_')]
    lag_block = df[lag_cols_present].apply(pd.to_numeric, errors="coerce").fillna(0)

    # Combine all feature blocks
    X = pd.concat([cat_dummies, df[["prediction_year_age_at_year_start", "cold_start"]], lag_block], axis=1)
    
    X["prediction_year_age_at_year_start"] = X["prediction_year_age_at_year_start"].fillna(X["prediction_year_age_at_year_start"].median())
    
    # Align feature columns with the model's expected input
    X_aligned = X.reindex(columns=feature_list, fill_value=0)
    
    target_cols = [col for col in df.columns if 'pmpm' in col or 'pmpc' in col]
    Y_targets = df[target_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    
    print(f"Data preparation complete. Feature matrix shape: {X_aligned.shape}")

    return {
        "features": X_aligned,
        "targets": Y_targets,
        "member_months": member_months,
        "metadata": metadata
    }

def run_inference(X_features: pd.DataFrame, bundle: dict) -> pd.DataFrame:
    print("Generating predictions with calibration for the entire dataset...")
    models = bundle['models']
    cal_factors = bundle['calibration_factors']
    predictions_df = pd.DataFrame(index=X_features.index)
    
    for model_key, model in models.items():
        raw_preds = model.predict(X_features)
        cal_factor = cal_factors.get(model_key, 1.0) or 1.0
        calibrated_preds = np.maximum(0, raw_preds / cal_factor)
        predictions_df[f"pred_{model_key.upper()}"] = calibrated_preds
        
    print(f"Inference complete. Generated predictions for {len(predictions_df)} rows.")
    return predictions_df

def compute_and_save_feature_frequency(
    X_features: pd.DataFrame,
    metadata: pd.DataFrame,
    run_id: str,
    eval_ts: datetime,
    spark_session: SparkSession,
    output_table_path: str,
    mode: str = "append",
):
    """Computes feature frequency (share of rows with value > 0) overall and by group, then saves to Delta.

    - Mirrors Snowflake training definition: values > 0 are considered present.
    - Computes overall stats and grouped stats by prediction_year and data_source.
    - Writes a unified table including optional grouping columns to avoid schema drift.
    """
    print("Calculating feature frequency (fill rates) for input features...")
    if X_features is None or X_features.empty:
        print("INFO: No features provided for frequency analysis; skipping save.")
        return

    if metadata is None or metadata.empty or len(metadata) != len(X_features):
        print("WARNING: Metadata missing or mismatched with features; saving overall frequencies only.")
        groups = [(None, None, X_features.index)]
    else:
        # Ensure required grouping columns exist
        required_cols = [PREDICTION_YEAR_COL, DATA_SOURCE_COL]
        for c in required_cols:
            if c not in metadata.columns:
                print(f"WARNING: Required metadata column '{c}' missing; saving overall frequencies only.")
                groups = [(None, None, X_features.index)]
                break
        else:
            # Build group index tuples
            md = metadata[[PREDICTION_YEAR_COL, DATA_SOURCE_COL]].copy()
            md = md.reset_index(drop=True)
            groups = []
            for (year, data_source), idx in md.groupby([PREDICTION_YEAR_COL, DATA_SOURCE_COL]).groups.items():
                groups.append((year, data_source, idx))
            # Also add overall group as (None, None)
            groups.insert(0, (None, None, X_features.index))

    records = []
    for year, data_source, idx in groups:
        if idx is None or len(idx) == 0:
            continue
        sub = X_features.iloc[idx]
        total_rows = int(len(sub))
        if total_rows == 0:
            continue
        positive_counts = (sub > 0).sum()
        positive_rates = (positive_counts / total_rows) * 100.0

        rec_df = pd.DataFrame({
            "feature_name": sub.columns,
            "positive_count": positive_counts.astype(float).values,
            "total_rows": float(total_rows),
            "positive_rate_percent": positive_rates.values,
        })
        rec_df["prediction_year"] = int(year) if year is not None and pd.notna(year) else None
        rec_df["data_source"] = str(data_source) if data_source is not None and pd.notna(data_source) else None
        records.append(rec_df)

    if not records:
        print("INFO: No frequency records computed; skipping save.")
        return

    freq_pd = pd.concat(records, ignore_index=True)
    freq_pd["model_run_id"] = run_id
    freq_pd["eval_ts"] = eval_ts

    # Order columns for readability
    col_order = [
        "model_run_id", "prediction_year", "data_source", "feature_name",
        "positive_count", "total_rows", "positive_rate_percent", "eval_ts",
    ]
    freq_pd = freq_pd[col_order]

    freq_spark_df = spark_session.createDataFrame(freq_pd)
    save_spark_df_to_delta_abfss(freq_spark_df, output_table_path, mode=mode)
    print(f"Saved {len(freq_pd)} feature frequency rows to {output_table_path}")

# ==========================================================================
# 2. CORE LOGIC FUNCTIONS
# ==========================================================================

# REFACTORED: This function now computes metrics for all groups at once.
def compute_and_save_metrics_in_batch(
    Y_actuals: pd.DataFrame,
    Y_preds: pd.DataFrame,
    member_months: pd.Series,
    metadata: pd.DataFrame, # Contains prediction_year, data_source, etc.
    bundle: dict,
    spark_session: SparkSession,
    metrics_table_path: str,
    run_timestamp: datetime,
    mode: str = "overwrite"
):
    print(f"Calculating and logging evaluation metrics by '{PREDICTION_YEAR_COL}' and '{DATA_SOURCE_COL}'...")
    eval_results = []
    run_id = bundle['model_run_id']
    model_source_tag = bundle.get('model_source_tag', MODEL_ALIAS)

    # --- Combine all data for easy grouping ---
    combined_df = pd.concat([
        metadata,
        Y_actuals,
        Y_preds,
        member_months.rename('member_months')
    ], axis=1)

    # --- Group by prediction year and data source to calculate metrics for each segment ---
    grouped = combined_df.groupby([PREDICTION_YEAR_COL, DATA_SOURCE_COL])

    for (year, data_source), group_df in grouped:
        print(f"  - Processing metrics for Year: {year}, Data Source: {data_source}")
        
        for pred_col in Y_preds.columns:
            model_key = pred_col.replace('pred_', '')
            t_type, t_name = model_key.split('_', 1)
            
            # Construct the corresponding actual column name
            actual_col_base = f"prediction_year_{t_type.lower()}"
            actual_col_name = f"{actual_col_base}_paid_amount" if t_name.lower() == 'overall' else f"{actual_col_base}_{t_name.lower()}_paid_amount"
            if t_type.lower() == 'pmpc':
                actual_col_name = f"prediction_year_pmpc_{t_name.lower()}_count"

            if actual_col_name not in group_df.columns:
                print(f"WARNING: Actual column '{actual_col_name}' not found for prediction '{pred_col}'. Skipping metrics for this target.")
                continue

            y_pred_metric = group_df[pred_col]
            y_actual_metric = group_df[actual_col_name]
            mm_subset = group_df['member_months']

            pred_annual, actual_annual = y_pred_metric * mm_subset, y_actual_metric * mm_subset
            mae_annual = mean_absolute_error(actual_annual, pred_annual)
            mean_actual_annual = actual_annual.mean()

            # Handle division by zero for ratios and percentages
            pa_ratio = np.nan if actual_annual.sum() == 0 else pred_annual.sum() / actual_annual.sum()
            r2 = np.nan if actual_annual.var() == 0 else r2_score(actual_annual, pred_annual)
            mae_percent = np.nan if mean_actual_annual == 0 else mae_annual / mean_actual_annual
            
            record = {
                'MODEL_RUN_ID': run_id,
                'MODEL_SOURCE_TAG': model_source_tag,
                'PREDICTION_YEAR': year,
                'DATA_SOURCE': data_source,
                'TARGET_TYPE': t_type,
                'TARGET_NAME': t_name,
                'EVAL_TS': run_timestamp,
                'N_ROWS': float(len(y_actual_metric)),
                'MAE_ANNUAL': float(mae_annual),
                'MAE_PERCENT_ANNUAL': float(mae_percent) * 100,
                'R2_ANNUAL': float(r2),
                'PREDICTION_RATIO': float(pa_ratio),
            }
            eval_results.append(record)

    if not eval_results:
        print("WARNING: No metric records were generated.")
        return

    metrics_df = pd.DataFrame(eval_results)
    
    metrics_schema = StructType([
        StructField("model_run_id", StringType(), True),
        StructField("model_source_tag", StringType(), True),
        StructField("prediction_year", IntegerType(), True),
        StructField("data_source", StringType(), True),
        StructField("target_type", StringType(), True),
        StructField("target_name", StringType(), True),
        StructField("eval_ts", TimestampType(), True),
        StructField("n_rows", FloatType(), True),
        StructField("mae_annual", FloatType(), True),
        StructField("mae_percent_annual", FloatType(), True),
        StructField("r2_annual", FloatType(), True),
        StructField("prediction_ratio", FloatType(), True),
    ])

    metrics_spark_df = spark_session.createDataFrame(metrics_df, schema=metrics_schema)
    # The mode is now 'overwrite' since we are calculating and saving all metrics at once.
    save_spark_df_to_delta_abfss(metrics_spark_df, metrics_table_path, mode=mode)
    print(f"Logged {len(metrics_df)} metric records to {metrics_table_path}")

# ==========================================================================
# 3. MAIN EXECUTION PIPELINE
# ==========================================================================
# REFACTORED: The main pipeline now runs in a single pass without a loop over years.
def run_prediction_pipeline():
    print("--- Starting Prospective Model Inference Pipeline (Batch Mode) ---")
    pipeline_start_time = datetime.now(timezone.utc)
    print(f"Pipeline started at: {pipeline_start_time.isoformat()}")

    print("\n--- Stage 1: Downloading and Loading Model Assets ---")
    try:
        model_path = download_file_from_hf(MODEL_FILE_URL, HF_TOKEN, MODEL_DESTINATION_DIR, MODEL_NAME, FORCE_DOWNLOAD)
        model_bundle = load_model_bundle_from_lakehouse(model_path)
        feature_list = model_bundle['features']
        run_id = model_bundle['model_run_id']
        print(f"Loaded bundle from MODEL_RUN_ID: {run_id}")
    except Exception as e:
        print(f"HALT: Model asset download or load failed: {e}")
        return

    print("\n--- Stage 2: Loading and Preparing All Input Data ---")
    try:
        data = load_and_prepare_all_data(spark, INPUT_TABLE_ABFSS_PATH, feature_list, ROW_LIMIT)
        if data is None:
            return
    except Exception as e:
        print(f"HALT: Data load/preparation failed: {e}")
        return

    print("\n--- Stage 3: Generating Predictions ---")
    try:
        preds_pd = run_inference(data["features"], model_bundle)
        # Apply Medicare price adjustment factors to paid-amount predictions (PMPM) by year
        try:
            if PREDICTION_YEAR_COL in data["metadata"].columns and len(preds_pd) == len(data["metadata"]):
                years_series = pd.to_numeric(data["metadata"][PREDICTION_YEAR_COL], errors="coerce").fillna(0).astype(int)
                # Compute factor per row using the helper (supports future years)
                factors = years_series.map(get_adjustment_factor).values
                # Identify PMPM prediction columns (paid amounts) and scale them; leave PMPC (counts) unchanged
                pmpm_cols = [c for c in preds_pd.columns if c.startswith("pred_PMPM")]
                if pmpm_cols:
                    preds_pd.loc[:, pmpm_cols] = preds_pd[pmpm_cols].multiply(factors, axis=0)
                    print(f"Applied Medicare adjustment factors to {len(pmpm_cols)} PMPM prediction columns by year.")
            else:
                print("WARNING: Could not align prediction_year for adjustment; skipping PMPM scaling.")
        except Exception as adj_e:
            print(f"WARNING: Failed to apply adjustment factors to predictions: {adj_e}")
    except Exception as e:
        print(f"HALT: Prediction generation failed: {e}")
        return

    print("\n--- Stage 3.5: Computing and Saving Feature Frequency ---")
    try:
        compute_and_save_feature_frequency(
            X_features=data["features"],
            metadata=data["metadata"],
            run_id=run_id,
            eval_ts=pipeline_start_time,
            spark_session=spark,
            output_table_path=FEATURE_FREQ_TABLE_ABFSS_PATH,
            mode="append",
        )
    except Exception as e:
        print(f"ERROR computing/saving feature frequency: {e}")

    print("\n--- Stage 4: Saving All Predictions ---")
    # Combine metadata (keys, year) with predictions for the final output table
    final_preds_pd = pd.concat([data["metadata"], preds_pd], axis=1)
    final_preds_pd['model_run_id'] = run_id
    final_preds_pd['last_ran'] = pipeline_start_time
    
    predictions_spark_df = spark.createDataFrame(final_preds_pd)
    
    save_spark_df_to_delta_abfss(
        spark_df=predictions_spark_df,
        table_abfss_path=PREDICTIONS_TABLE_ABFSS_PATH,
        mode="overwrite", # Overwrite the table with the full new set of predictions
        partition_by=[PREDICTION_YEAR_COL] # Partitioning by year is still a good practice
    )

    print("\n--- Stage 5: Computing and Saving All Metrics ---")
    try:
        compute_and_save_metrics_in_batch(
            Y_actuals=data["targets"],
            Y_preds=preds_pd,
            member_months=data["member_months"],
            metadata=data["metadata"],
            bundle=model_bundle,
            spark_session=spark,
            metrics_table_path=METRICS_TABLE_ABFSS_PATH,
            run_timestamp=pipeline_start_time,
            mode="overwrite" # Overwrite with the fresh set of metrics for this run
        )
    except Exception as e:
        print(f"ERROR computing/saving metrics: {e}")

    print("\n--- Prospective Model Inference Pipeline Finished ---")

# ==========================================================================
# 4. EXECUTE THE PIPELINE
# ==========================================================================
if __name__ == "__main__":
    run_prediction_pipeline()
