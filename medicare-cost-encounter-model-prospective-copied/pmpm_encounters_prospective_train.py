from __future__ import annotations

import os
import pickle
import json
import uuid
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkClientException

# ---- CONFIG ----
INPUT_DATABASE  = "MEDICARE_LDS_FIVE_MULTI_YEAR"
INPUT_SCHEMA    = "BENCHMARKS"
INPUT_TABLE     = f"{INPUT_DATABASE}.{INPUT_SCHEMA}.PERSON_YEAR_PROSPECTIVE"

MODEL_YEAR = 2023
ROW_LIMIT = None  # set to None to pull all rows

# --- Model Training Flags ---
TUNE_TWEEDIE_POWER = False      # Set to True to run the tuning loop for PMPM models
DEFAULT_TWEEDIE_POWER = 1.5     # Used when TUNE_TWEEDIE_POWER is False

TRAIN_PMPM_ENCOUNTER_GROUPS = True  # Train models for OUTPATIENT, INPATIENT PMPMs, etc.
TRAIN_PMPM_ENCOUNTER_TYPES = True  # Train models for all detailed encounter type PMPMs.
TRAIN_PMPC_ENCOUNTER_GROUPS = True  # Train models for OUTPATIENT, INPATIENT PMPCs, etc.
TRAIN_PMPC_ENCOUNTER_TYPES = True  # Train models for all detailed encounter type PMPCs.

# --- Hyperparameter Tuning Control ---
# Toggle which categories to tune. Start with overall PMPM only.
# Keys: 'overall', 'pmpm_group', 'pmpm_type', 'pmpc_group', 'pmpc_type'
HYPERPARAM_TUNING_SCOPE: Dict[str, bool] = {
    'overall': True,
    'pmpm_group': False,
    'pmpm_type': False,
    'pmpc_group': False,
    'pmpc_type': False,
}

# Use previously saved tuned params when available; set to False to always tune when scope says so
USE_SAVED_TUNED_PARAMS = True
# Force re-tune even if saved params exist (applies only when scope says to tune)
FORCE_RETUNE = False

# ---- IO CONFIG ----
OUTPUT_DATABASE = "MEDICARE_LDS_FIVE_MULTI_YEAR"
OUTPUT_SCHEMA   = "BENCHMARKS"
STAGE_DATABASE  = "MEDICARE_LDS_FIVE_PERCENT"
STAGE_SCHEMA    = "BENCHMARKS"

# Outputs (write)
METRICS_TABLE_NAME = f"{OUTPUT_DATABASE}.{OUTPUT_SCHEMA}.PROSP_MODEL_EVAL_METRICS_TRAIN"
FEATURE_FREQ_TABLE_NAME = f"{OUTPUT_DATABASE}.{OUTPUT_SCHEMA}.PROSP_FEATURE_FREQUENCY"
FEATURE_IMPORTANCE_TABLE_NAME = f"{OUTPUT_DATABASE}.{OUTPUT_SCHEMA}.PROSP_MODEL_FEATURE_IMPORTANCE"
SNOWFLAKE_STAGE_NAME = f"@{STAGE_DATABASE}.{STAGE_SCHEMA}.MODEL_STAGE"
TUNED_PARAMS_TABLE_NAME = f"{OUTPUT_DATABASE}.{OUTPUT_SCHEMA}.PROSP_MODEL_TUNED_PARAMS"

# --- Metadata for tracking model runs ---
MODEL_RUN_ID = str(uuid.uuid4())
MODEL_SOURCE_TAG = "prospective_encounters_2023"

# --- Target Definitions ---
PMPM_ENCOUNTER_GROUPS = ['OVERALL', 'OUTPATIENT', 'INPATIENT', 'OTHER', 'OFFICE_BASED']
PMPM_ENCOUNTER_TYPES = [
    'OUTPATIENT_INJECTIONS', 'EMERGENCY_DEPARTMENT', 'OUTPATIENT_RADIOLOGY',
    'OUTPATIENT_PT_OT_ST', 'OUTPATIENT_HOSPICE', 'URGENT_CARE',
    'OUTPATIENT_HOSPITAL_OR_CLINIC', 'HOME_HEALTH', 'DIALYSIS',
    'OUTPATIENT_REHABILITATION', 'OUTPATIENT_SURGERY', 'AMBULATORY_SURGERY_CENTER',
    'OUTPATIENT_PSYCH', 'DME_ORPHANED', 'ORPHANED_CLAIM', 'AMBULANCE_ORPHANED',
    'LAB_ORPHANED', 'OFFICE_VISIT_RADIOLOGY', 'OFFICE_VISIT', 'OFFICE_VISIT_SURGERY',
    'OFFICE_VISIT_OTHER', 'TELEHEALTH', 'OFFICE_VISIT_PT_OT_ST',
    'OFFICE_VISIT_INJECTIONS', 'ACUTE_INPATIENT', 'INPATIENT_HOSPICE',
    'INPATIENT_PSYCH', 'INPATIENT_REHABILITATION', 'INPATIENT_SKILLED_NURSING',
]
PMPC_ENCOUNTER_GROUPS = ['OUTPATIENT', 'OTHER', 'OFFICE_BASED', 'INPATIENT']
PMPC_ENCOUNTER_TYPES = [
    'EMERGENCY_DEPARTMENT', 'OUTPATIENT_RADIOLOGY', 'OUTPATIENT_PT_OT_ST',
    'OUTPATIENT_HOSPICE', 'URGENT_CARE', 'OUTPATIENT_HOSPITAL_OR_CLINIC',
    'HOME_HEALTH', 'DIALYSIS', 'OUTPATIENT_REHABILITATION', 'OUTPATIENT_SURGERY',
    'AMBULATORY_SURGERY_CENTER', 'OUTPATIENT_PSYCH', 'DME_ORPHANED', 'ORPHANED_CLAIM',
    'AMBULANCE_ORPHANED', 'LAB_ORPHANED', 'OFFICE_VISIT_RADIOLOGY', 'OFFICE_VISIT',
    'OFFICE_VISIT_SURGERY', 'OFFICE_VISIT_OTHER', 'TELEHEALTH',
    'OFFICE_VISIT_PT_OT_ST', 'OFFICE_VISIT_INJECTIONS', 'ACUTE_INPATIENT',
    'INPATIENT_HOSPICE', 'INPATIENT_PSYCH', 'INPATIENT_REHABILITATION',
    'INPATIENT_SKILLED_NURSING',
]

# --- Build the final list of targets to process based on config flags ---
ALL_TARGETS_INFO: List[Dict[str, str]] = []
ALL_TARGETS_INFO.append({'name': 'OVERALL', 'type': 'PMPM'})  # Always include overall PMPM

if TRAIN_PMPM_ENCOUNTER_GROUPS:
    other_groups = [g for g in PMPM_ENCOUNTER_GROUPS if g != 'OVERALL']
    for group in other_groups:
        ALL_TARGETS_INFO.append({'name': group, 'type': 'PMPM'})

if TRAIN_PMPM_ENCOUNTER_TYPES:
    for enc_type in PMPM_ENCOUNTER_TYPES:
        ALL_TARGETS_INFO.append({'name': enc_type, 'type': 'PMPM'})

if TRAIN_PMPC_ENCOUNTER_GROUPS:
    for count_target in PMPC_ENCOUNTER_GROUPS:
        ALL_TARGETS_INFO.append({'name': count_target, 'type': 'PMPC'})

if TRAIN_PMPC_ENCOUNTER_TYPES:
    for count_target in PMPC_ENCOUNTER_TYPES:
        ALL_TARGETS_INFO.append({'name': count_target, 'type': 'PMPC'})


# ------------------------- Snowflake Table Helpers -------------------------

def create_metrics_table_if_not_exists(session: Session, table_name: str):
    session.sql(f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
        MODEL_RUN_ID          STRING,
        MODEL_SOURCE_TAG      STRING,
        TARGET_TYPE           STRING,
        TARGET_NAME           STRING,
        METRIC_NAME           STRING,
        METRIC_VALUE          FLOAT,
        EVAL_TS               TIMESTAMP_NTZ
      );
    """).collect()
    print(f"Ensured metrics table '{table_name}' exists.")

def create_feature_frequency_table_if_not_exists(session: Session, table_name: str):
    session.sql(f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
        MODEL_RUN_ID          STRING,
        FEATURE_NAME          STRING,
        POSITIVE_COUNT        NUMBER,
        TOTAL_ROWS            NUMBER,
        POSITIVE_RATE_PERCENT FLOAT,
        EVAL_TS               TIMESTAMP_NTZ
      );
    """).collect()
    print(f"Ensured feature frequency table '{table_name}' exists.")

def create_feature_importance_table_if_not_exists(session: Session, table_name: str):
    session.sql(f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
        MODEL_RUN_ID          STRING,
        TARGET_TYPE           STRING,
        TARGET_NAME           STRING,
        FEATURE_NAME          STRING,
        IMPORTANCE_TYPE       STRING,
        IMPORTANCE_VALUE      FLOAT,
        EVAL_TS               TIMESTAMP_NTZ
      );
    """).collect()
    print(f"Ensured feature importance table '{table_name}' exists.")

def create_stage_if_not_exists(session: Session, stage_name: str):
    session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name.replace('@','')}").collect()
    print(f"Ensured stage '{stage_name}' exists.")

def create_tuned_params_table_if_not_exists(session: Session, table_name: str):
    session.sql(f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
        MODEL_SOURCE_TAG      STRING,
        TARGET_TYPE           STRING,
        TARGET_NAME           STRING,
        PARAMS_JSON           STRING,
        N_ESTIMATORS          NUMBER,
        EVAL_TS               TIMESTAMP_NTZ
      );
    """).collect()
    print(f"Ensured tuned params table '{table_name}' exists.")

def load_tuned_params(session: Session, t_type: str, t_name: str) -> Tuple[Dict[str, Any], int] | Tuple[None, None]:
    sql = f"""
        SELECT PARAMS_JSON, N_ESTIMATORS
        FROM {TUNED_PARAMS_TABLE_NAME}
        WHERE MODEL_SOURCE_TAG = '{MODEL_SOURCE_TAG}'
          AND TARGET_TYPE = '{t_type}'
          AND TARGET_NAME = '{t_name}'
        ORDER BY EVAL_TS DESC
        LIMIT 1
    """
    rows = session.sql(sql).collect()
    if not rows:
        return None, None
    try:
        params = json.loads(rows[0][0])
    except Exception:
        params = None
    n_estimators = int(rows[0][1]) if rows[0][1] is not None else None
    return params, n_estimators

def save_tuned_params(session: Session, t_type: str, t_name: str, params: Dict[str, Any], n_estimators: int):
    try:
        params_json = json.dumps(params)
    except Exception:
        params_json = "{}"
    df = pd.DataFrame([
        {
            'MODEL_SOURCE_TAG': MODEL_SOURCE_TAG,
            'TARGET_TYPE': t_type,
            'TARGET_NAME': t_name,
            'PARAMS_JSON': params_json,
            'N_ESTIMATORS': int(n_estimators),
            'EVAL_TS': datetime.utcnow(),
        }
    ])
    session.create_dataframe(df).write.mode("append").save_as_table(TUNED_PARAMS_TABLE_NAME)
    print(f"Saved tuned params for {t_type}:{t_name} (n_estimators={n_estimators}).")


# ------------------------- Data & Modeling Helpers -------------------------

def _standardize_columns_case(df: pd.DataFrame, to: str = "lower") -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower() if to == "lower" else str(c).upper() for c in df.columns]
    return df

def fetch_lag_feature_columns(session: Session) -> List[str]:
    sql = f"""
    SELECT column_name
    FROM {INPUT_DATABASE}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = '{INPUT_SCHEMA}'
      AND table_name = 'PERSON_YEAR_PROSPECTIVE'
      AND (
            column_name ILIKE 'lag_cond_%' OR column_name ILIKE 'lag_cms_%' OR column_name ILIKE 'lag_HCC%'
      )
    ORDER BY column_name
    """
    return [r[0] for r in session.sql(sql).collect()]

def fetch_2023_df(session: Session, lag_cols: List[str]) -> pd.DataFrame:
    base_cols = [
        "PREDICTION_YEAR", "PREDICTION_YEAR_MEMBER_MONTHS", "PREDICTION_YEAR_SEX",
        "PREDICTION_YEAR_RACE", "PREDICTION_YEAR_STATE",
        "PREDICTION_YEAR_AGE_AT_YEAR_START", "COLD_START",
    ]

    target_cols = []
    for t_info in ALL_TARGETS_INFO:
        t_name, t_type = t_info['name'], t_info['type']
        if t_type == 'PMPM':
            col = "PREDICTION_YEAR_PMPM_PAID_AMOUNT" if t_name == 'OVERALL' else f"PREDICTION_YEAR_PMPM_{t_name.upper()}_PAID_AMOUNT"
        elif t_type == 'PMPC':
            col = f"PREDICTION_YEAR_PMPC_{t_name.upper()}_COUNT"
        target_cols.append(col)

    select_cols = base_cols + target_cols + lag_cols
    select_list = ",".join(list(dict.fromkeys(select_cols)))
    limit_clause = f" LIMIT {ROW_LIMIT}" if ROW_LIMIT is not None else ""
    order_clause = " ORDER BY RANDOM()" if ROW_LIMIT is not None else ""

    sql = f"""
        SELECT {select_list}
        FROM {INPUT_TABLE}
        WHERE PREDICTION_YEAR = {MODEL_YEAR}
        {order_clause}
        {limit_clause}
    """
    df = session.sql(sql).to_pandas()
    return _standardize_columns_case(df, to="lower")

def prep_features(df: pd.DataFrame, lag_cols: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """
    Prepare raw features WITHOUT any train/test-derived statistics.
    - Leave categoricals as raw columns (no one-hot here).
    - Convert numeric/lag columns to numeric; fill known-constant imputations only.
    - DO NOT compute medians or dummy columns here to avoid leakage.
    """
    df = df.copy()

    # Keep categoricals raw
    cat_cols = ["prediction_year_sex", "prediction_year_race", "prediction_year_state"]
    for c in cat_cols:
        df[c] = df[c].astype("category")

    # Numeric columns
    df["prediction_year_age_at_year_start"] = pd.to_numeric(df["prediction_year_age_at_year_start"], errors="coerce")
    df["cold_start"] = pd.to_numeric(df["cold_start"], errors="coerce").fillna(0).clip(0, 1)  # constant fill OK (no stats)

    # Lag block numeric
    lag_block = df[lag_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    # Assemble raw X
    X = pd.concat([df[cat_cols], df[["prediction_year_age_at_year_start", "cold_start"]], lag_block], axis=1)

    # Targets
    Y = pd.DataFrame(index=df.index)
    for t_info in ALL_TARGETS_INFO:
        t_name_lower, t_type_lower = t_info['name'].lower(), t_info['type'].lower()
        if t_type_lower == 'pmpm':
            source_col = "prediction_year_pmpm_paid_amount" if t_name_lower == 'overall' else f"prediction_year_pmpm_{t_name_lower}_paid_amount"
        else:  # pmpc
            source_col = f"prediction_year_pmpc_{t_name_lower}_count"
        target_col_name = f"{t_type_lower}_{t_name_lower}"
        Y[target_col_name] = pd.to_numeric(df[source_col], errors="coerce").fillna(0)

    member_months = pd.to_numeric(df["prediction_year_member_months"], errors="coerce").fillna(0)
    return X, Y, member_months

def align_columns(X_train: pd.DataFrame, X_test: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cols = X_train.columns
    X_test_aligned = X_test.reindex(columns=cols, fill_value=0)
    return X_train, X_test_aligned

def _evaluate_raw(model: XGBRegressor, X_eval: pd.DataFrame, y_eval_metric: pd.Series, mm_eval: pd.Series) -> Dict[str, float]:
    pred_metric = pd.Series(model.predict(X_eval), index=X_eval.index)
    pred_annual = pred_metric * mm_eval
    actual_annual = y_eval_metric * mm_eval
    mae = mean_absolute_error(actual_annual, pred_annual)
    r2 = r2_score(actual_annual, pred_annual)
    denom = actual_annual.sum()
    pa_ratio = pred_annual.sum() / denom if denom != 0 else np.nan
    return {"mae": mae, "r2": r2, "pa_ratio": pa_ratio}

def _should_tune_target(t_type: str, t_name: str) -> bool:
    """
    Determine if the current target should undergo hyperparameter tuning based on HYPERPARAM_TUNING_SCOPE.
    Categories: 'overall' (PMPM OVERALL), 'pmpm_group', 'pmpm_type', 'pmpc_group', 'pmpc_type'.
    """
    t_type_u = t_type.upper()
    t_name_u = t_name.upper()
    if t_type_u == 'PMPM':
        if t_name_u == 'OVERALL':
            return HYPERPARAM_TUNING_SCOPE.get('overall', False)
        # Decide group vs type for PMPM
        if t_name_u in PMPM_ENCOUNTER_GROUPS:
            return HYPERPARAM_TUNING_SCOPE.get('pmpm_group', False)
        if t_name_u in PMPM_ENCOUNTER_TYPES:
            return HYPERPARAM_TUNING_SCOPE.get('pmpm_type', False)
    elif t_type_u == 'PMPC':
        if t_name_u in PMPC_ENCOUNTER_GROUPS:
            return HYPERPARAM_TUNING_SCOPE.get('pmpc_group', False)
        if t_name_u in PMPC_ENCOUNTER_TYPES:
            return HYPERPARAM_TUNING_SCOPE.get('pmpc_type', False)
    return False

def _pmpm_candidate_params() -> List[Dict[str, Any]]:
    """A small curated list of candidate parameter sets for PMPM Tweedie models."""
    base = dict(
        objective="reg:tweedie",
        tweedie_variance_power=DEFAULT_TWEEDIE_POWER,
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
        base_score=None,  # filled per-target
    )
    cands = []
    # Curated combinations to keep search small and fast
    combos = [
        dict(max_depth=6, min_child_weight=20, subsample=0.8, colsample_bytree=0.6, reg_lambda=5, reg_alpha=1, gamma=1, learning_rate=0.05),
        dict(max_depth=6, min_child_weight=20, subsample=0.8, colsample_bytree=0.8, reg_lambda=5, reg_alpha=1, gamma=1, learning_rate=0.05),
        dict(max_depth=6, min_child_weight=30, subsample=0.8, colsample_bytree=0.6, reg_lambda=10, reg_alpha=0, gamma=0, learning_rate=0.05),
        dict(max_depth=4, min_child_weight=20, subsample=0.9, colsample_bytree=0.8, reg_lambda=5, reg_alpha=0, gamma=0, learning_rate=0.05),
        dict(max_depth=8, min_child_weight=30, subsample=0.7, colsample_bytree=0.6, reg_lambda=10, reg_alpha=1, gamma=1, learning_rate=0.05),
        dict(max_depth=5, min_child_weight=10, subsample=0.8, colsample_bytree=0.8, reg_lambda=1, reg_alpha=0, gamma=0, learning_rate=0.05),
        dict(max_depth=7, min_child_weight=20, subsample=0.7, colsample_bytree=0.6, reg_lambda=5, reg_alpha=1, gamma=0, learning_rate=0.05),
        dict(max_depth=5, min_child_weight=30, subsample=0.9, colsample_bytree=0.8, reg_lambda=5, reg_alpha=1, gamma=0, learning_rate=0.05),
    ]
    for d in combos:
        p = base.copy()
        p.update(d)
        cands.append(p)
    return cands

def tune_pmpm_params(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    mm_train: pd.Series,
) -> Tuple[XGBRegressor, Dict[str, Any]]:
    """
    Tune a small set of XGBoost params for PMPM (Tweedie) using a train/validation split.
    Selection metric: validation R² on annualized values using TRAIN-split calibration.
    Returns the final model retrained on full train with the best params, and the best param dict.
    """
    X_tr, X_val, y_tr, y_val, mm_tr, mm_val = train_test_split(
        X_train, y_train, mm_train, test_size=0.2, random_state=42
    )

    candidates = _pmpm_candidate_params()
    best_score = -float("inf")
    best_model = None
    best_params: Optional[Dict[str, Any]] = None
    best_n_estimators: Optional[int] = None

    base_score = float(y_tr.mean()) if len(y_tr) else 0.0

    print("\n--- Hyperparam tuning (PMPM) on train/val ---")
    for i, params in enumerate(candidates, 1):
        params = params.copy()
        params['base_score'] = base_score
        # Pass early_stopping_rounds and eval_metric via constructor for compatibility with older xgboost wrappers
        model = XGBRegressor(**params, n_estimators=2000, early_stopping_rounds=100, eval_metric='rmse', verbosity=0)
        model.fit(
            X_tr, y_tr,
            sample_weight=mm_tr,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )

        # Calibration from the TRAIN portion (X_tr)
        pred_tr = pd.Series(model.predict(X_tr), index=X_tr.index)
        cal_num = float((pred_tr * mm_tr).sum())
        cal_den = float((y_tr * mm_tr).sum())
        cal = cal_num / cal_den if cal_den != 0 else 1.0
        if not np.isfinite(cal) or cal <= 0:
            cal = 1.0

        # Validate R² annual with calibration applied
        pred_val_raw = pd.Series(model.predict(X_val), index=X_val.index)
        pred_val_ann = (pred_val_raw * mm_val) / cal
        actual_val_ann = y_val * mm_val
        r2_val = r2_score(actual_val_ann, pred_val_ann)

        # Derive number of trees used during early stopping across xgboost versions
        used_trees = getattr(model, 'best_ntree_limit', None)
        if used_trees is None:
            bi = getattr(model, 'best_iteration', None)
            used_trees = (int(bi) + 1) if bi is not None else None
        print(f"  [{i}/{len(candidates)}] R2_val={r2_val:.6f}, best_trees={used_trees}")

        if r2_val > best_score:
            best_score = r2_val
            best_params = params
            if used_trees is not None:
                best_n_estimators = int(used_trees)
            else:
                best_n_estimators = int(params.get('n_estimators', 500))

    # Retrain on full training set with best params
    assert best_params is not None
    if best_n_estimators is None or best_n_estimators <= 0:
        best_n_estimators = 500
    final = XGBRegressor(**best_params, n_estimators=best_n_estimators, verbosity=0)
    final.fit(X_train, y_train, sample_weight=mm_train)
    return final, best_params

def tune_tweedie_power(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    mm_train: pd.Series,
    powers: List[float]
) -> Tuple[XGBRegressor, float]:
    """
    Hyperparameter selection using ONLY a validation split carved out of the TRAINING data.
    Returns a model retrained on the full X_train with the best power and the best power value.
    """
    # Train/validation split inside training set (no test leakage)
    X_tr, X_val, y_tr, y_val, mm_tr, mm_val = train_test_split(
        X_train, y_train, mm_train, test_size=0.2, random_state=42
    )

    results, best_r2, best_power = [], -float("inf"), None

    print("\n--- Tuning Tweedie Variance Power (train/val only) ---")
    for power in powers:
        print(f"Training with tweedie_variance_power = {power}...")
        model = XGBRegressor(
            n_estimators=500, max_depth=5, learning_rate=0.1, subsample=0.8, colsample_bytree=0.8,
            reg_lambda=1.0, objective="reg:tweedie", tweedie_variance_power=power, tree_method="hist",
            random_state=42, n_jobs=-1,
        )
        model.fit(X_tr, y_tr)
        raw_metrics = _evaluate_raw(model, X_val, y_val, mm_val)
        results.append({"power": power, **raw_metrics})
        if raw_metrics["r2"] > best_r2:
            best_r2, best_power = raw_metrics["r2"], power

    print("\n--- Tweedie Tuning Results (Validation, Uncalibrated) ---")
    print(pd.DataFrame(results).set_index("power").to_string(formatters={'mae': '{:,.2f}'.format, 'r2': '{:.6f}'.format, 'pa_ratio': '{:.6f}'.format}))
    print(f"\n==> Best Power Found: {best_power} (R2 = {best_r2:.6f})")

    # Retrain best model on the FULL training set
    final_model = XGBRegressor(
        n_estimators=500, max_depth=5, learning_rate=0.1, subsample=0.8, colsample_bytree=0.8,
        reg_lambda=1.0, objective="reg:tweedie", tweedie_variance_power=best_power, tree_method="hist",
        random_state=42, n_jobs=-1,
    )
    final_model.fit(X_train, y_train)
    return final_model, best_power

def train_and_calibrate(
    model: XGBRegressor,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    mm_train: pd.Series,
    use_sample_weight: bool = True
) -> Tuple[XGBRegressor, float]:
    """
    Trains the model and calculates the calibration factor on the training set to prevent data leakage.
    """
    print("\nFitting model...")
    if use_sample_weight:
        model.fit(X_train, y_train, sample_weight=mm_train)
    else:
        model.fit(X_train, y_train)

    print("Calculating calibration factor on the TRAINING set...")
    pred_metric_train_raw = pd.Series(model.predict(X_train), index=X_train.index)
    pred_annual_train_raw = pred_metric_train_raw * mm_train
    actual_annual_train = y_train * mm_train

    if actual_annual_train.sum() != 0:
        calibration_factor = float(pred_annual_train_raw.sum() / actual_annual_train.sum())
    else:
        calibration_factor = 1.0

    if (not np.isfinite(calibration_factor)) or (calibration_factor <= 0):
        print("Warning: Non-finite or non-positive calibration factor on training. Using 1.0")
        calibration_factor = 1.0

    print(f"Calibration factor (train-derived): {calibration_factor:.6f}")
    return model, calibration_factor

def evaluate_on_annual_amounts(
    model: XGBRegressor,
    X_eval: pd.DataFrame,
    y_eval_metric: pd.Series,
    mm_eval: pd.Series,
    calibration_factor: float
) -> Dict[str, Any]:
    """
    Evaluates the model using a pre-calculated calibration factor (from TRAIN).
    """
    pred_metric_raw = pd.Series(model.predict(X_eval), index=X_eval.index)
    pred_annual_raw = pred_metric_raw * mm_eval
    actual_annual = y_eval_metric * mm_eval

    if (not np.isfinite(calibration_factor)) or (calibration_factor <= 0):
        print("Warning: Non-finite or non-positive calibration factor passed to evaluation. Using 1.0")
        calibration_factor = 1.0

    pred_annual_calibrated = pred_annual_raw / calibration_factor
    print(f"\n[INFO] Applying pre-calculated calibration factor: {calibration_factor:.6f}")

    pa_ratio = pred_annual_calibrated.sum() / actual_annual.sum() if actual_annual.sum() != 0 else np.nan
    mae_annual = mean_absolute_error(actual_annual, pred_annual_calibrated)

    mean_actual_annual = actual_annual.mean()
    mae_percent_annual = (mae_annual / mean_actual_annual) * 100.0 if mean_actual_annual != 0 else np.nan

    return {
        "n_test_rows": len(X_eval),
        "mae_annual": float(mae_annual),
        "mae_percent_annual": float(mae_percent_annual),
        "r2_annual": float(r2_score(actual_annual, pred_annual_calibrated)),
        "pa_ratio_overall": float(pa_ratio),
        "sum_actual_annual": float(actual_annual.sum()),
        "sum_pred_annual": float(pred_annual_calibrated.sum()),
        "calibration_factor": float(calibration_factor),
    }


# ------------------------- Main -------------------------

def main(session: Session):
    print(f"--- SCRIPT START: {datetime.utcnow()} UTC ---")
    print(f"MODEL_RUN_ID: {MODEL_RUN_ID}")
    print(f"Running prospective models for {MODEL_YEAR} with 80/20 split...")
    if ROW_LIMIT is not None:
        print(f"Row limit applied: {ROW_LIMIT:,}")
    print(f"Targets to be modeled: {[t['type'] + ':' + t['name'] for t in ALL_TARGETS_INFO]}")

    # 1. Ensure Snowflake objects exist
    create_metrics_table_if_not_exists(session, METRICS_TABLE_NAME)
    create_feature_frequency_table_if_not_exists(session, FEATURE_FREQ_TABLE_NAME)
    create_feature_importance_table_if_not_exists(session, FEATURE_IMPORTANCE_TABLE_NAME)
    create_stage_if_not_exists(session, SNOWFLAKE_STAGE_NAME)
    create_tuned_params_table_if_not_exists(session, TUNED_PARAMS_TABLE_NAME)

    # 2. Fetch data and prep features (RAW; no dummies/medians)
    lag_cols = fetch_lag_feature_columns(session)
    df_2023 = fetch_2023_df(session, lag_cols)
    if df_2023.empty:
        raise RuntimeError(f"No rows found for prediction_year = {MODEL_YEAR}.")
    X_all_raw, Y_all, mm_all = prep_features(df_2023, [c.lower() for c in lag_cols])

    all_models = {}
    all_calibration_factors = {}
    feature_list = None
    feature_freq_logged = False  # Log feature frequency once per run

    # 3. Loop through each target to train, evaluate, and save artifacts
    for target_info in ALL_TARGETS_INFO:
        t_name, t_type = target_info['name'], target_info['type']
        print(f"\n{'='*25} PROCESSING TARGET: {t_type} - {t_name} {'='*25}")

        target_col = f"{t_type.lower()}_{t_name.lower()}"
        y_target_all = Y_all[target_col]

        if y_target_all.sum() == 0:
            print(f"[INFO] Skipping target '{t_name}' as it has zero value in the dataset.")
            continue

        # --- Split (capture mm_train for calibration) ---
        X_train_raw, X_test_raw, y_train, y_test, mm_train, mm_test = train_test_split(
            X_all_raw, y_target_all, mm_all, test_size=0.2, random_state=42
        )

        # --- Build encodings AFTER split to avoid leakage ---
        cat_cols = ["prediction_year_sex", "prediction_year_race", "prediction_year_state"]
        num_cols = ["prediction_year_age_at_year_start", "cold_start"]

        # One-hot on TRAIN only; align TEST to TRAIN's columns
        train_cats = pd.get_dummies(
            X_train_raw[cat_cols].astype("category"),
            drop_first=True, prefix=["sex", "race", "state"]
        )
        test_cats = pd.get_dummies(
            X_test_raw[cat_cols].astype("category"),
            drop_first=True, prefix=["sex", "race", "state"]
        )
        train_cats, test_cats = align_columns(train_cats, test_cats)

        # Numeric / lag columns
        lag_cols_used = [c for c in X_train_raw.columns if c not in (cat_cols + num_cols)]

        # Median imputation for age computed on TRAIN only
        age_med = X_train_raw["prediction_year_age_at_year_start"].median()
        X_train_num = X_train_raw[num_cols + lag_cols_used].copy()
        X_test_num  = X_test_raw[num_cols + lag_cols_used].copy()
        X_train_num["prediction_year_age_at_year_start"] = X_train_num["prediction_year_age_at_year_start"].fillna(age_med)
        X_test_num["prediction_year_age_at_year_start"]  = X_test_num["prediction_year_age_at_year_start"].fillna(age_med)

        # Final encoded matrices
        X_train = pd.concat([train_cats, X_train_num], axis=1)
        X_test  = pd.concat([test_cats,  X_test_num],  axis=1)
        X_train, X_test = align_columns(X_train, X_test)

        # Capture feature list once (for bundle + feature importance names)
        if feature_list is None:
            feature_list = list(X_train.columns)

        # 3a. Calculate and save feature frequency once (on TRAIN ONLY to avoid leakage)
        if not feature_freq_logged:
            print("\n--- Calculating and saving feature frequency (train only, once per run) ---")
            total_rows = len(X_train)
            positive_counts = (X_train > 0).sum()
            positive_rates = (positive_counts / total_rows) * 100
            freq_df = pd.DataFrame({
                'FEATURE_NAME': X_train.columns,
                'POSITIVE_COUNT': positive_counts.astype(int),
                'TOTAL_ROWS': total_rows,
                'POSITIVE_RATE_PERCENT': positive_rates
            }).sort_values(by='POSITIVE_RATE_PERCENT', ascending=False).reset_index(drop=True)
            freq_df['MODEL_RUN_ID'] = MODEL_RUN_ID
            freq_df['EVAL_TS'] = datetime.utcnow()

            final_freq_cols = [
                'MODEL_RUN_ID', 'FEATURE_NAME', 'POSITIVE_COUNT',
                'TOTAL_ROWS', 'POSITIVE_RATE_PERCENT', 'EVAL_TS'
            ]
            session.create_dataframe(freq_df[final_freq_cols]).write.mode("append").save_as_table(FEATURE_FREQ_TABLE_NAME)
            print(f"Saved {len(freq_df)} feature frequency stats to {FEATURE_FREQ_TABLE_NAME}")
            feature_freq_logged = True

        # 3b. Instantiate, (optionally tune), train, calibrate
        if t_type == 'PMPM':
            if TUNE_TWEEDIE_POWER:
                model, best_power = tune_tweedie_power(X_train, y_train, mm_train, powers=[1.1, 1.5, 1.9])
                model, calibration_factor = train_and_calibrate(model, X_train, y_train, mm_train, use_sample_weight=True)
            elif _should_tune_target(t_type, t_name):
                # Attempt to load saved tuned params (unless forced to retune)
                model = None
                calibration_factor = None
                used_saved = False
                if USE_SAVED_TUNED_PARAMS and not FORCE_RETUNE:
                    saved_params, saved_n = load_tuned_params(session, t_type, t_name)
                    if saved_params and saved_n:
                        print("\n--- Using saved tuned params for PMPM target ---")
                        model = XGBRegressor(**saved_params, n_estimators=int(saved_n), verbosity=0)
                        used_saved = True
                if not used_saved:
                    print("\n--- Tuning hyperparameters for PMPM target ---")
                    model, best_params = tune_pmpm_params(X_train, y_train, mm_train)
                    # Determine best n_estimators by inspecting model attributes
                    best_trees = getattr(model, 'best_ntree_limit', None)
                    if best_trees is None:
                        bi = getattr(model, 'best_iteration', None)
                        best_trees = (int(bi) + 1) if bi is not None else int(getattr(model, 'n_estimators', 500))
                    save_tuned_params(session, t_type, t_name, best_params, int(best_trees))
                # Calibration factor on full train
                model, calibration_factor = train_and_calibrate(model, X_train, y_train, mm_train, use_sample_weight=True)
            else:
                print(f"\n--- Instantiating PMPM model with fixed tweedie_variance_power = {DEFAULT_TWEEDIE_POWER} ---")
                model_unfitted = XGBRegressor(
                    objective="reg:tweedie", tweedie_variance_power=DEFAULT_TWEEDIE_POWER,
                    n_estimators=500, max_depth=5, learning_rate=0.1, subsample=0.8,
                    colsample_bytree=0.8, reg_lambda=1.0, tree_method="hist", random_state=42, n_jobs=-1,
                    verbosity=0
                )
                model, calibration_factor = train_and_calibrate(model_unfitted, X_train, y_train, mm_train, use_sample_weight=True)
        else:  # PMPC
            print("\n--- Instantiating PMPC model with count:poisson objective ---")
            model_unfitted = XGBRegressor(
                objective="count:poisson", n_estimators=500, max_depth=5, learning_rate=0.1,
                subsample=0.8, colsample_bytree=0.8, reg_lambda=1.0, tree_method="hist",
                random_state=42, n_jobs=-1, verbosity=0
            )
            model, calibration_factor = train_and_calibrate(model_unfitted, X_train, y_train, mm_train, use_sample_weight=True)

        # 3c. Save Feature Importance (uses column names from DataFrame)
        print("\n--- Saving feature importance ---")
        booster = model.get_booster()
        importance_dfs = []
        for imp_type in ['weight', 'gain', 'cover']:
            try:
                scores = booster.get_score(importance_type=imp_type)
                if scores:
                    imp_df = pd.DataFrame(list(scores.items()), columns=['FEATURE_NAME', 'IMPORTANCE_VALUE'])
                    imp_df['IMPORTANCE_TYPE'] = imp_type
                    importance_dfs.append(imp_df)
            except Exception as e:
                print(f"Could not get importance type '{imp_type}': {e}")

        if importance_dfs:
            final_imp_df = pd.concat(importance_dfs, ignore_index=True)
            final_imp_df['MODEL_RUN_ID'] = MODEL_RUN_ID
            final_imp_df['TARGET_TYPE'] = t_type
            final_imp_df['TARGET_NAME'] = t_name
            final_imp_df['EVAL_TS'] = datetime.utcnow()

            final_imp_cols = [
                'MODEL_RUN_ID', 'TARGET_TYPE', 'TARGET_NAME', 'FEATURE_NAME',
                'IMPORTANCE_TYPE', 'IMPORTANCE_VALUE', 'EVAL_TS'
            ]
            session.create_dataframe(final_imp_df[final_imp_cols]).write.mode("append").save_as_table(FEATURE_IMPORTANCE_TABLE_NAME)
            print(f"Saved {len(final_imp_df)} feature importance records for target '{t_name}'.")

        # 3d. Evaluate on TEST using TRAIN-derived calibration
        annual_metrics = evaluate_on_annual_amounts(model, X_test, y_test, mm_test, calibration_factor)

        print("\n--- Saving evaluation metrics ---")
        metrics_df = pd.DataFrame(annual_metrics.items(), columns=['METRIC_NAME', 'METRIC_VALUE'])
        metrics_df['MODEL_RUN_ID'] = MODEL_RUN_ID
        metrics_df['MODEL_SOURCE_TAG'] = MODEL_SOURCE_TAG
        metrics_df['TARGET_TYPE'] = t_type
        metrics_df['TARGET_NAME'] = t_name
        metrics_df['EVAL_TS'] = datetime.utcnow()

        final_metrics_cols = [
            'MODEL_RUN_ID', 'MODEL_SOURCE_TAG', 'TARGET_TYPE', 'TARGET_NAME',
            'METRIC_NAME', 'METRIC_VALUE', 'EVAL_TS'
        ]
        session.create_dataframe(metrics_df[final_metrics_cols]).write.mode("append").save_as_table(METRICS_TABLE_NAME)
        print(f"Saved {len(metrics_df)} evaluation metrics for target '{t_name}'.")

        print("\n--- Final Evaluation on Annualized Values (Calibrated) ---")
        print(
            f"EVAL|ANNUAL|{t_type}|{t_name}|"
            f"rows_train={len(X_train)},rows_test={annual_metrics['n_test_rows']},"
            f"pa_ratio={annual_metrics['pa_ratio_overall']:.6f},mae={annual_metrics['mae_annual']:.2f},"
            f"mae_percent={annual_metrics.get('mae_percent_annual', float('nan')):.2f}%,"
            f"r2={annual_metrics['r2_annual']:.6f},sum_actual={annual_metrics['sum_actual_annual']:.2f},"
            f"sum_pred={annual_metrics['sum_pred_annual']:.2f}"
        )

        # 3e. Add artifacts to bundle
        model_key = f"{t_type.upper()}_{t_name.upper()}"
        all_models[model_key] = model
        all_calibration_factors[model_key] = calibration_factor
        print(f"Added model for '{model_key}' to the final bundle.")

    # 4. Bundle and upload
    if all_models:
        print("\n--- Bundling and uploading all model artifacts into a single file ---")
        final_bundle = {
            'models': all_models,
            'calibration_factors': all_calibration_factors,
            'features': feature_list,
            'model_run_id': MODEL_RUN_ID,
            'training_timestamp_utc': datetime.utcnow().isoformat()
        }

        bundle_file_name = f"{MODEL_YEAR}_prospective_models_bundle.pkl"

        try:
            with open(bundle_file_name, 'wb') as f:
                pickle.dump(final_bundle, f)
            print(f"All models bundled and saved locally to '{bundle_file_name}'")

            put_res = session.file.put(bundle_file_name, SNOWFLAKE_STAGE_NAME, auto_compress=False, overwrite=True)
            print(f"Successfully uploaded single bundle to stage {SNOWFLAKE_STAGE_NAME}: {put_res[0].status}")

        except Exception as e:
            print(f"ERROR: Could not create or upload the final model bundle: {e}")
        finally:
            if os.path.exists(bundle_file_name):
                os.remove(bundle_file_name)
                print(f"Cleaned up local bundle file: '{bundle_file_name}'")
    else:
        print("\nNo models were trained. Skipping bundling and upload.")

    print(f"\n--- SCRIPT END: {datetime.utcnow()} UTC ---")


if __name__ == "__main__":
    try:
        snowpark_session = get_active_session()
        print("Using active Snowpark session.")
    except SnowparkClientException:
        print("No active session. Creating a new Snowpark session from local credentials...")
        snowpark_session = Session.builder.create()
    main(snowpark_session)
