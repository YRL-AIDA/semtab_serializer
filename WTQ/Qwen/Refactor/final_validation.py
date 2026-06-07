# core/final_validation.py

import json
import pandas as pd
import numpy as np
from pathlib import Path

from utils.normalize import (
    convert_dataset_types,
    convert_type,
    find_word
)

from WTQ.Qwen.check_result import extract_code_from_response


def execute_code(code_str: str, df: pd.DataFrame):
    """
    Выполняет pandas-код и возвращает:
    result, error
    """

    try:
        df_norm = convert_dataset_types(df)
    except Exception as e:
        return None, f"convert_dataset_types error: {e}"

    try:
        clean_code = extract_code_from_response(code_str)

        result = eval(
            clean_code,
            {
                "df": df_norm,
                "pd": pd,
                "np": np,
                "convert_type": convert_type,
                "find_word": find_word,
            }
        )

        return result, None

    except Exception as e:
        return None, str(e)


def normalize_result(result):
    """
    Приведение результата к сериализуемому виду.
    Повторяет текущую логику пайплайна.
    """

    if result is None:
        return None

    if isinstance(result, list):
        return "|".join(str(x) for x in result)

    if isinstance(result, pd.Series):

        if len(result) == 1:
            return str(result.iloc[0])

        return "|".join(str(x) for x in result.tolist())

    return str(result)


def validate_single_record(record, dataset_root):
    """
    Повторно выполняет код для одной записи.
    """

    table_path = record["table"]

    tsv_path = (
        table_path
        .replace(".csv", ".tsv")
        .replace("/csv/", "/tsv/")
    )

    full_path = Path(dataset_root) / tsv_path

    try:
        df = pd.read_csv(full_path, sep="\t")
    except Exception as e:
        record["final_execution_ok"] = False
        record["final_execution_error"] = f"table read error: {e}"
        return record

    result, error = execute_code(record.get("code"), df)

    record["final_execution_ok"] = error is None
    record["final_execution_error"] = error

    if error is None:
        record["final_answer"] = normalize_result(result)
        record["final_result_type"] = type(result).__name__
    else:
        record["final_answer"] = None
        record["final_result_type"] = None

    return record


def validate_results(
    input_json,
    output_json,
    dataset_root
):
    """
    Проверяет весь итоговый results.json
    """

    with open(input_json, "r", encoding="utf-8") as f:
        results = json.load(f)

    validated = []

    ok_count = 0
    fail_count = 0

    for item in results:

        item = validate_single_record(
            item,
            dataset_root
        )

        if item["final_execution_ok"]:
            ok_count += 1
        else:
            fail_count += 1

        validated.append(item)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(
            validated,
            f,
            ensure_ascii=False,
            indent=2,
            default=str
        )

    print("=" * 50)
    print("FINAL VALIDATION")
    print(f"Total: {len(validated)}")
    print(f"Executable: {ok_count}")
    print(f"Failed: {fail_count}")
    print("=" * 50)

    return validated