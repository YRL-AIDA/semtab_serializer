import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


def check_type_comprehensive(series):
    if series is None or series.empty:
        return 'unknown', 0

    # Исправленные регулярные выражения
    exp_pattern = re.compile(r'^[-+]?(?:\d+\.?\d*|\.\d+)[eE][-+]?\d+$')
    float_pattern = re.compile(r'^[+-]?(?:\d+\.\d+|\.\d+)$')  # ТОЛЬКО если есть дробная часть
    int_pattern = re.compile(r'^[+-]?\d+$')

    # Булевы значения - только строковые представления
    bool_true_pattern = re.compile(r'^(true|yes|да|истина)$', re.IGNORECASE)
    bool_false_pattern = re.compile(r'^(false|no|нет|ложь)$', re.IGNORECASE)

    # Паттерны для дат и времени (оставляем как есть)
    date_patterns = {
        'date_iso': re.compile(r'^\d{4}-\d{1,2}-\d{1,2}$'),
        'date_us': re.compile(r'^\d{1,2}-\d{1,2}-\d{4}$'),
        'date_eu': re.compile(r'^\d{1,2}\.\d{1,2}\.\d{4}$'),
        'date_slash': re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$'),
        'date_year_last': re.compile(r'^\d{1,2}-\d{1,2}-\d{2}$'),
    }

    time_patterns = {
        'time_basic': re.compile(r'^\d{1,2}:\d{2}$'),
        'time_seconds': re.compile(r'^\d{1,2}:\d{2}:\d{2}$'),
        'time_milliseconds': re.compile(r'^\d{1,2}:\d{2}:\d{2}\.\d+$'),
        'time_12h': re.compile(r'^\d{1,2}:\d{2}\s?[APap][Mm]$'),
    }

    datetime_patterns = {
        'datetime_iso': re.compile(r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'),
        'datetime_common': re.compile(r'^\d{1,2}-\d{1,2}-\d{4}\s+\d{1,2}:\d{2}'),
    }

    type_counts = {}
    nan_count = 0

    for value in series:
        if pd.isna(value):
            nan_count += 1
            continue

        str_value = str(value).strip()
        if not str_value:
            type_counts['empty'] = type_counts.get('empty', 0) + 1
            continue

        found_type = False

        # 1. Проверка на ДАТУ-ВРЕМЯ (самый специфичный тип)
        if not found_type:
            for pattern in datetime_patterns.values():
                if pattern.match(str_value):
                    try:
                        pd.to_datetime(str_value)
                        type_counts['datetime'] = type_counts.get('datetime', 0) + 1
                        found_type = True
                        break
                    except:
                        continue

        # 2. Проверка на ДАТУ
        if not found_type:
            for pattern in date_patterns.values():
                if pattern.match(str_value):
                    try:
                        pd.to_datetime(str_value)
                        type_counts['date'] = type_counts.get('date', 0) + 1
                        found_type = True
                        break
                    except:
                        continue

        # 3. Проверка на ВРЕМЯ
        if not found_type:
            for pattern in time_patterns.values():
                if pattern.match(str_value):
                    type_counts['time'] = type_counts.get('time', 0) + 1
                    found_type = True
                    break

        # 4. Проверка на INT (перед float!)
        if not found_type and int_pattern.match(str_value):
            type_counts['int'] = type_counts.get('int', 0) + 1
            found_type = True

        # 5. Проверка на НАУЧНУЮ НОТАЦИЮ
        if not found_type and exp_pattern.match(str_value):
            type_counts['float'] = type_counts.get('float', 0) + 1
            found_type = True

        # 6. Проверка на FLOAT
        if not found_type and float_pattern.match(str_value):
            type_counts['float'] = type_counts.get('float', 0) + 1
            found_type = True

        # 7. Проверка на BOOL (после чисел!)
        if not found_type:
            if bool_true_pattern.match(str_value) or bool_false_pattern.match(str_value):
                type_counts['bool'] = type_counts.get('bool', 0) + 1
                found_type = True

        # 8. Проверка на булевы значения 0/1 (только если не распознаны как числа)
        if not found_type and str_value in ('0', '1'):
            type_counts['bool'] = type_counts.get('bool', 0) + 1
            found_type = True

        # Все остальное - строка
        if not found_type:
            type_counts['str'] = type_counts.get('str', 0) + 1

    # Обработка случая, когда все значения - пропуски
    if not type_counts and nan_count > 0:
        return 'unknown', nan_count

    # Находим наиболее частый тип (исключая 'empty')
    valid_types = {k: v for k, v in type_counts.items() if k != 'empty'}

    if not valid_types:
        return 'unknown', nan_count

    max_type = max(valid_types, key=valid_types.get)

    return max_type, nan_count


def process_column_parallel(column_name, dataset):
    """Обрабатывает одну колонку параллельно"""
    series = pd.Series(dataset[column_name])
    return column_name, check_type_comprehensive(series)


def analyze_dataset_parallel(dataset, max_workers=None):
    """
    Параллельно анализирует все колонки датасета
    """
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_column_parallel, column, dataset)
            for column in dataset.column_names
        ]

        for future in futures:
            column_name, column_result = future.result()
            results[column_name] = column_result

    return results