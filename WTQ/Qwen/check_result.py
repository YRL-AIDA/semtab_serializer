from utils.normalize import convert_dataset_types
import pandas as pd
import numpy as np
import re
import json


import re
import json

def extract_code_from_response(response):
    if not response:
        return response

    # 1. Поиск JSON: от первой { до последней }
    start = response.find('{')
    end = response.rfind('}')
    if start != -1 and end != -1:
        # Пробуем отрезать от 0 до 5 лишних символов в конце
        for cut in range(0, 6):
            candidate = response[start:end+1-cut]
            try:
                data = json.loads(candidate)
                if 'PANDA' in data:
                    code = data['PANDA']
                    # Очистка кода от мусора в конце
                    code = re.sub(r'[\]\}]+$', '', code)   # удалить ] } в конце
                    # Если код обёрнут в квадратные скобки, снимаем их
                    if code.startswith('[') and code.endswith(']'):
                        code = code[1:-1].strip()
                    return code
            except:
                continue

    # 2. Fallback: регулярное выражение, захватывающее всё до последней кавычки перед } или ]
    match = re.search(r'"PANDA"\s*:\s*"([^"]*)"\s*[\}\]\]]', response, re.DOTALL)
    if match:
        code = match.group(1).strip()
        code = re.sub(r'[\]\}]+$', '', code)
        return code

    return response


def normalize_value(value):
    df_temp = pd.DataFrame({'col': [value]})
    df_norm = convert_dataset_types(df_temp)
    return df_norm['col'].iloc[0]


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