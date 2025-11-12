import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor



def check_type_comprehensive(series):
    if series is None or series.empty:
        return None

    # Регулярные выражения для разных типов
    exp_pattern = re.compile(r'^[-+]?(?:\d+\.?\d*|\.\d+)[eE][-+]?\d+$')
    float_pattern = re.compile(r'^[+-]?(?:\d+\.\d*|\.\d+)$')
    int_pattern = re.compile(r'^[+-]?\d+$')

    # Расширенные паттерны для дат
    date_patterns = {
        'date_iso': re.compile(r'^\d{4}-\d{1,2}-\d{1,2}$'),
        'date_us': re.compile(r'^\d{1,2}-\d{1,2}-\d{4}$'),
        'date_eu': re.compile(r'^\d{1,2}\.\d{1,2}\.\d{4}$'),
        'date_slash': re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$'),
        'date_slash_eu': re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$'),
        'date_year_last': re.compile(r'^\d{1,2}-\d{1,2}-\d{2}$'),
    }

    # Паттерны для времени
    time_patterns = {
        'time_basic': re.compile(r'^\d{1,2}:\d{2}$'),
        'time_seconds': re.compile(r'^\d{1,2}:\d{2}:\d{2}$'),
        'time_milliseconds': re.compile(r'^\d{1,2}:\d{2}:\d{2}\.\d+$'),
        'time_12h': re.compile(r'^\d{1,2}:\d{2}\s?[APap][Mm]$'),
    }

    # Паттерны для даты-времени
    datetime_patterns = {
        'datetime_iso': re.compile(r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'),
        'datetime_common': re.compile(r'^\d{1,2}-\d{1,2}-\d{4}\s+\d{1,2}:\d{2}'),
    }

    types_of_text = []
    nan_count = 0

    for text in series:
        if pd.isna(text):
            types_of_text.append({'nan': 1})
            nan_count += 1
            continue

        array_types = {}
        words = re.split(r'[\s,;]+', str(text))

        for word in words:
            clean_word = word.strip('.,!?;:"\'()[]{}')

            if not clean_word:
                continue

            found_type = False

            # Проверка на ДАТУ
            if not found_type:
                for date_type, pattern in date_patterns.items():
                    if pattern.match(clean_word):
                        try:
                            pd.to_datetime(clean_word)
                            array_types['date'] = array_types.get('date', 0) + 1
                            found_type = True
                            break
                        except:
                            continue

            # Проверка на ВРЕМЯ
            if not found_type:
                for time_type, pattern in time_patterns.items():
                    if pattern.match(clean_word):
                        array_types['time'] = array_types.get('time', 0) + 1
                        found_type = True
                        break

            # Проверка на ДАТУ-ВРЕМЯ
            if not found_type:
                for datetime_type, pattern in datetime_patterns.items():
                    if pattern.match(clean_word):
                        try:
                            pd.to_datetime(clean_word)
                            array_types['datetime'] = array_types.get('datetime', 0) + 1
                            found_type = True
                            break
                        except:
                            continue

            # Проверка на НАУЧНУЮ НОТАЦИЮ
            if not found_type and exp_pattern.match(clean_word):
                array_types['exp'] = array_types.get('exp', 0) + 1
                found_type = True

            # Проверка на FLOAT
            if not found_type and float_pattern.match(clean_word):
                array_types['float'] = array_types.get('float', 0) + 1
                found_type = True

            # Проверка на INT
            if not found_type and int_pattern.match(clean_word):
                array_types['int'] = array_types.get('int', 0) + 1
                found_type = True

            # Все остальное - строка
            if not found_type:
                array_types['str'] = array_types.get('str', 0) + 1

        types_of_text.append(array_types)

    # Добавляем общую статистику по NaN в конец результатов
    if nan_count > 0:
        types_of_text.append({'_summary': {'nan_count': nan_count, 'total_count': len(series)}})

    return types_of_text


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
