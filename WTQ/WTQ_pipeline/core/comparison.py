from __future__ import annotations

from typing import Any, Optional
import math

import numpy as np
import pandas as pd

from utils.normalize import convert_type


def safe_convert_type(value: Any) -> Any:
    try:
        return convert_type(value)
    except Exception:
        return value


def normalize_result_for_storage(result: Any) -> Optional[str]:
    """
    Повторяет текущую save-логику: результат сохраняется строкой,
    списки и Series длиной > 1 объединяются через |.
    """
    if result is None:
        return None

    if isinstance(result, pd.DataFrame):
        return result.to_json(force_ascii=False, default_handler=str)

    if isinstance(result, pd.Series):
        if len(result) == 0:
            return ""
        if len(result) == 1:
            return str(result.iloc[0])
        return "|".join(str(x) for x in result.tolist())

    if isinstance(result, (list, tuple, set)):
        return "|".join(str(x) for x in list(result))

    if isinstance(result, np.generic):
        return str(result.item())

    return str(result)


def normalize_for_compare(value: Any) -> Any:
    """
    Приведение к типу для строгого сравнения после нормализации.
    Это не fuzzy matching: строка '2004' и число 2004 становятся одним int.
    """
    if isinstance(value, pd.Series):
        if len(value) == 0:
            return ""
        if len(value) == 1:
            return normalize_for_compare(value.iloc[0])
        return "|".join(str(normalize_for_compare(x)) for x in value.tolist())

    if isinstance(value, pd.DataFrame):
        return value.to_json(force_ascii=False, default_handler=str)

    if isinstance(value, (list, tuple, set)):
        return "|".join(str(normalize_for_compare(x)) for x in list(value))

    if isinstance(value, np.generic):
        value = value.item()

    if isinstance(value, float) and math.isnan(value):
        return pd.NA

    return safe_convert_type(value)


def compare_answers(result: Any, target_value: Any) -> bool:
    left = normalize_for_compare(result)
    right = normalize_for_compare(target_value)

    try:
        if pd.isna(left) and pd.isna(right):
            return True
    except Exception:
        pass

    # Timestamp/date comparison.
    if isinstance(left, pd.Timestamp) or isinstance(right, pd.Timestamp):
        try:
            ldt = pd.to_datetime(left, errors="coerce")
            rdt = pd.to_datetime(right, errors="coerce")
            if pd.notna(ldt) and pd.notna(rdt):
                return ldt == rdt
        except Exception:
            pass

    return left == right
