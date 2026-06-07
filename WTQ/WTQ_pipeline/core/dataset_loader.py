from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from utils.normalize import convert_dataset_types
from utils.type_check import analyze_dataset_parallel
from utils.utils import serialize_table_to_tapex_format


def context_to_tsv_path(context: str) -> str:
    return context.replace(".csv", ".tsv").replace("/csv/", "/tsv/")


def load_training(training_path: str | Path) -> pd.DataFrame:
    return pd.read_csv(training_path, sep="\t")


def load_table(context: str, dataset_root: str | Path) -> pd.DataFrame:
    full_path = Path(dataset_root) / context_to_tsv_path(context)
    return pd.read_csv(full_path, sep="\t")


def prepare_table_payload(df: pd.DataFrame) -> tuple[pd.DataFrame, str, dict[str, Any]]:
    try:
        norm_df = convert_dataset_types(df)
    except Exception:
        norm_df = df.copy()

    try:
        serialized = serialize_table_to_tapex_format(norm_df)
    except Exception:
        serialized = str(norm_df.head())

    try:
        df_types = analyze_dataset_parallel(df)
        column_types = {col: types[0] for col, types in df_types.items()}
    except Exception:
        column_types = {}

    return norm_df, serialized, column_types


def load_sql_dict(squall_path: str | Path) -> dict[str, str]:
    with open(squall_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    sql_dict: dict[str, str] = {}
    for item in data:
        nt = item.get("nt")
        tokens = item.get("sql") or []
        sql_code = " ".join([t[1] for t in tokens if len(t) > 1])
        if nt:
            sql_dict[nt] = sql_code
    return sql_dict
