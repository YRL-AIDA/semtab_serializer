from utils.normalize import convert_dataset_types, convert_type
import pandas as pd
import numpy as np
import re
import json

def extract_code_from_response(response):
    if not response:
        return response

    # Самое простое решение: ищем между "PANDA": " и следующей кавычкой перед }
    pattern = r'"PANDA"\s*:\s*"([^"]+)"'
    match = re.search(pattern, response)

    if match:
        code = match.group(1)
        # Заменяем экранированные кавычки на обычные
        code = code.replace('\\"', '"')
        code = code.replace("\\'", "'")
        return code

    return response



def normalize_value(value):
    return convert_type(value)

def evaluate_code(code_str, df, expected):
    # Нормализуем таблицу
    df_norm = convert_dataset_types(df)

    # Извлекаем чистый код из ответа модели
    clean_code = extract_code_from_response(code_str)

    # Выполняем код на нормализованной таблице
    try:
        result = eval(clean_code, {'df': df_norm, 'pd': pd, 'np': np})
    except Exception as e:
        print(f"Execution error: {e}")
        return False

    # Приводим результат к единому формату (строка, разделитель для списков)
    if isinstance(result, list):
        norm_result = '|'.join(str(x) for x in result)
    elif isinstance(result, pd.Series):
        if len(result) == 1:
            norm_result = normalize_value(result.iloc[0])
        else:
            norm_result = '|'.join(str(x) for x in result.tolist())
    else:
        norm_result = normalize_value(result)

    # Нормализуем ожидаемый ответ
    try:
        norm_expected = normalize_value(expected)
    except Exception:
        norm_expected = expected

    return norm_result == norm_expected