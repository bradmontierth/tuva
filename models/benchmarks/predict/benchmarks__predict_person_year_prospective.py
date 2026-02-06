import os
import pickle
import tempfile
from typing import Any

import numpy as np
import pandas as pd


def _standardize_columns_case(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).lower() for col in out.columns]
    return out


def _load_model_bundle(session: Any, stage_name: str, file_name: str) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as temp_dir:
        stage_prefix = stage_name.rstrip("/")
        session.file.get(f"{stage_prefix}/{file_name}", temp_dir)

        local_file = os.path.join(temp_dir, os.path.basename(file_name))
        with open(local_file, "rb") as fh:
            return pickle.load(fh)


def _prep_features_for_inference(df: pd.DataFrame, feature_list: list[str]) -> tuple[pd.Series, pd.DataFrame]:
    working = _standardize_columns_case(df)

    benchmark_key = working["benchmark_key"]

    cat_dummies = pd.get_dummies(
        working[["prediction_year_sex", "prediction_year_race", "prediction_year_state"]].astype("category"),
        drop_first=True,
        prefix=["sex", "race", "state"],
    )

    working["prediction_year_age_at_year_start"] = pd.to_numeric(
        working["prediction_year_age_at_year_start"], errors="coerce"
    )
    age_median = working["prediction_year_age_at_year_start"].median()
    if pd.isna(age_median):
        age_median = 0
    working["prediction_year_age_at_year_start"] = working["prediction_year_age_at_year_start"].fillna(age_median)

    working["cold_start"] = (
        pd.to_numeric(working["cold_start"], errors="coerce")
        .fillna(0)
        .clip(0, 1)
    )

    lag_cols = [col for col in working.columns if col.startswith("lag_")]
    lag_block = working[lag_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    x = pd.concat(
        [
            cat_dummies,
            working[["prediction_year_age_at_year_start", "cold_start"]],
            lag_block,
        ],
        axis=1,
    )

    x_aligned = x.reindex(columns=feature_list, fill_value=0)
    return benchmark_key, x_aligned


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["numpy", "pandas", "scikit-learn", "xgboost"],
        python_version="3.11",
    )

    scoring_year = int(dbt.config.get("scoring_year") or 2023)
    row_limit_cfg = dbt.config.get("row_limit")
    stage_name = dbt.config.get("model_stage")
    bundle_file_name = dbt.config.get("bundle_file_name")

    row_limit = None
    if row_limit_cfg not in (None, "", "none", "None", 0, "0"):
        row_limit = int(row_limit_cfg)

    if not stage_name or not bundle_file_name:
        raise ValueError(
            "Missing required config values `model_stage` or `bundle_file_name` for prospective predictions."
        )

    source_df = dbt.ref("benchmarks__person_year_prospective").filter(f"PREDICTION_YEAR = {scoring_year}")
    if row_limit is not None:
        source_df = source_df.limit(row_limit)

    df = _standardize_columns_case(source_df.to_pandas())

    if df.empty:
        return pd.DataFrame(columns=["benchmark_key"])

    bundle = _load_model_bundle(session, stage_name=stage_name, file_name=bundle_file_name)
    models = bundle["models"]
    calibration_factors = bundle.get("calibration_factors", {})
    feature_list = bundle["features"]
    model_run_id = bundle.get("model_run_id")

    benchmark_key, x_features = _prep_features_for_inference(df, feature_list)

    predictions = pd.DataFrame({"benchmark_key": benchmark_key})
    for model_key, predictor in models.items():
        raw_pred = predictor.predict(x_features)
        calibration_factor = calibration_factors.get(model_key, 1.0) or 1.0
        predictions[f"pred_{model_key.lower()}"] = np.maximum(0, raw_pred / calibration_factor)

    predictions["model_run_id"] = model_run_id
    return predictions
