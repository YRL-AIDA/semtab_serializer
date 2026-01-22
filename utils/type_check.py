import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Any, Optional, Dict, Tuple


def clean_value(value: Any) -> str:
    """Базовая очистка значения (удаление спецсимволов)"""
    if value is None:
        return ""

    if not isinstance(value, str):
        value = str(value)

    # Заменяем неразрывные пробелы и
    # другие специальные символы
    value = value.replace('\xa0', ' ')
    value = value.replace('\ufeff', '')
    value = value.replace('\u200b', '')

    return value.strip()


def extract_number_string(s: str) -> str:
    """Подготовка строки для проверки на число (удаление валют, скобок, текста)"""
    # Удаляем символы валют в начале/конце
    s = re.sub(r'^[$€£¥₽\s]*', '', s)
    s = re.sub(r'[$€£¥₽\s]*$', '', s)

    # Обработка скобок (финансовый формат)
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1].strip()

    # Удаляем текст после числа
    s = re.sub(r'\s+[a-zA-Zа-яА-Я].*$', '', s)

    # Удаляем текст перед числом
    s = re.sub(r'^[a-zA-Zа-яА-Я]+\s+', '', s)

    return s.strip()

def check_type_comprehensive(data: Union[pd.Series, list, Any]) -> Tuple[str, int]:
    """
    Определяет тип данных. Все значения проверяются через регулярные выражения.
    """
    # 1. Обработка None
    if data is None:
        return 'None', 0

    # 2. Если это одиночное значение - создаем список из одного элемента
    if not isinstance(data, (pd.Series, list)):
        # Преобразуем одиночное значение в список для единообразной обработки
        values = [data]
        is_single_value = True
    else:
        # Если это Series или список
        if isinstance(data, pd.Series):
            values = data.tolist()
        else:
            values = data
        is_single_value = False

    # РЕГУЛЯРНЫЕ ВЫРАЖЕНИЯ

    # Булевы значения - расширенный набор
    bool_true_pattern = re.compile(r'^(true|yes|да|истина|1|\+)$', re.IGNORECASE)
    bool_false_pattern = re.compile(r'^(false|no|нет|ложь|0|-|\[ \])$', re.IGNORECASE)

    # Базовые числовые паттерны (после очистки)
    exp_pattern = re.compile(r'^[-+]?(?:\d+(?:[.,]\d*)?|[.,]\d+)[eE][-+]?\d+$')

    # Паттерны для float с поддержкой разных разделителей
    float_pattern = re.compile(r'^[+-]?(?:\d{1,3}(?:[ ,.]\d{3})*[.,]\d+|\d+[.,]\d+|[.,]\d+|\d+[.,])$')

    # Паттерн для int с разделителями тысяч (точка, запятая или пробел)
    int_with_separators_pattern = re.compile(r'^[+-]?\d{1,3}(?:[ ,.]\d{3})*$')
    int_pattern = re.compile(r'^[+-]?\d+$')

    # Паттерны для дат и времени с улучшениями
    date_patterns = {
        'date_iso': re.compile(r'^\d{4}-\d{1,2}-\d{1,2}$'),
        'date_us': re.compile(r'^\d{1,2}-\d{1,2}-\d{4}$'),
        'date_eu': re.compile(r'^\d{1,2}\.\d{1,2}\.\d{4}$'),
        'date_slash': re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$'),
        'date_year_last': re.compile(r'^\d{1,2}-\d{1,2}-\d{2}$'),
        # Новые паттерны для дат с буквенными месяцами
        'date_month_short': re.compile(r'^\d{1,2}-[A-Za-z]{3,9}-\d{4}$'),
        'date_month_short_dot': re.compile(r'^\d{1,2}\.[A-Za-z]{3,9}\.\d{4}$'),
        'date_month_long': re.compile(r'^\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}$'),
        'date_month_short_comma': re.compile(r'^[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}$'),
    }

    # ИСПРАВЛЕНИЕ 3: Паттерны для времени с необязательными ведущими нулями
    time_patterns = {
        'time_basic': re.compile(r'^\d{1,2}:\d{1,2}$'),
        'time_seconds': re.compile(r'^\d{1,2}:\d{1,2}:\d{1,2}$'),
        'time_milliseconds': re.compile(r'^\d{1,2}:\d{1,2}:\d{1,2}\.\d+$'),
        'time_12h': re.compile(r'^\d{1,2}:\d{1,2}\s*[APap][Mm]$'),
        'time_12h_seconds': re.compile(r'^\d{1,2}:\d{1,2}:\d{1,2}\s*[APap][Mm]$'),
    }

    datetime_patterns = {
        'datetime_iso': re.compile(r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'),
        'datetime_common': re.compile(r'^\d{1,2}[-./]\d{1,2}[-./]\d{4}\s+\d{1,2}:\d{1,2}(?::\d{1,2})?'),
        # Datetime с буквенными месяцами
        'datetime_month_name': re.compile(r'^\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\s+\d{1,2}:\d{1,2}(?::\d{1,2})?'),
    }

    type_counts = {}
    nan_count = 0

    for value in values:
        if (pd.isna(value) or
            (isinstance(value, str) and value.lower() in ['nan', 'na', 'n/a','nill'])):
            nan_count += 1
            continue

        # ПРОВЕРКА НА ЧИСЛОВЫЕ ТИПЫ СРАЗУ
        if isinstance(value, (int, float)):
            if isinstance(value, int):
                type_counts['int'] = type_counts.get('int', 0) + 1
                continue
            elif isinstance(value, float):
                # Проверяем, является ли float целым числом
                if value.is_integer():
                    type_counts['int'] = type_counts.get('int', 0) + 1
                else:
                    type_counts['float'] = type_counts.get('float', 0) + 1
                continue

        # Очищаем значение перед анализом
        str_value = clean_value(value)
        if not str_value:
            type_counts['empty'] = type_counts.get('empty', 0) + 1
            continue

        found_type = False

        # 1. Проверка на BOOL (кроме одиночных символов)
        if not found_type and len(str_value) > 1:
            if bool_true_pattern.match(str_value) or bool_false_pattern.match(str_value):
                type_counts['bool'] = type_counts.get('bool', 0) + 1
                found_type = True

        # 2. Проверка на ДАТУ-ВРЕМЯ
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

        # 3. Проверка на ДАТУ
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

        # 4. Проверка на ВРЕМЯ
        if not found_type:
            for pattern in time_patterns.values():
                if pattern.match(str_value):
                    type_counts['time'] = type_counts.get('time', 0) + 1
                    found_type = True
                    break

        # 5. Проверка на НАУЧНУЮ НОТАЦИЮ
        if not found_type:
            cleaned_num = extract_number_string(str_value)
            if exp_pattern.match(cleaned_num):
                type_counts['float'] = type_counts.get('float', 0) + 1
                found_type = True

        # 6. Проверка на ЧИСЛА
        if not found_type:
            cleaned_num = extract_number_string(str_value)

            # Нормализация: удаляем .0 для целых чисел
            if re.match(r'^\d+\.0+$', cleaned_num):
                cleaned_num = cleaned_num.split('.')[0]

            # Удаляем все пробелы для чисел с пробелами в качестве разделителей
            # но сохраняем десятичный разделитель
            if ' ' in cleaned_num and (',' in cleaned_num or '.' in cleaned_num):
                # Для чисел с десятичными разделителями сохраняем последний разделитель
                if ',' in cleaned_num:
                    parts = cleaned_num.split(',')
                    cleaned_num = parts[0].replace(' ', '') + ',' + parts[1]
                elif '.' in cleaned_num:
                    parts = cleaned_num.split('.')
                    cleaned_num = parts[0].replace(' ', '') + '.' + parts[1]

            # Сначала проверяем int с разделителями тысяч
            if int_with_separators_pattern.match(cleaned_num.replace(' ', '')):
                type_counts['int'] = type_counts.get('int', 0) + 1
                found_type = True
            # Проверяем обычный int
            elif int_pattern.match(cleaned_num):
                type_counts['int'] = type_counts.get('int', 0) + 1
                found_type = True
            # Проверяем float (уже включает разные форматы)
            elif float_pattern.match(cleaned_num):
                type_counts['float'] = type_counts.get('float', 0) + 1
                found_type = True

        # Все остальное - строка
        if not found_type:
            # Одиночные символы обрабатываем отдельно
            if len(str_value) == 1:
                if str_value in ['+', '-', '1', '0']:
                    type_counts['bool'] = type_counts.get('bool', 0) + 1
                else:
                    type_counts['str'] = type_counts.get('str', 0) + 1
            else:
                type_counts['str'] = type_counts.get('str', 0) + 1

    # Обработка случая, когда все значения - пропуски
    if not type_counts and nan_count > 0:
        return 'None', nan_count

    # Находим наиболее частый тип (исключая 'empty')
    valid_types = {k: v for k, v in type_counts.items() if k != 'empty'}

    if not valid_types:
        return 'None', nan_count

    # Для одиночного значения возвращаем его тип
    if is_single_value and len(valid_types) == 1:
        result_type = list(valid_types.keys())[0]
    else:
        # Для списка/Series находим наиболее частый тип
        result_type = max(valid_types, key=valid_types.get)

    # Если есть и int и float - считаем float
    if result_type == 'int' and 'float' in valid_types:
        result_type = 'float'

    return result_type, nan_count


def process_column_parallel(column_name: str, dataset: Any) -> Tuple[str, Tuple[str, int]]:
    """Обрабатывает одну колонку параллельно"""
    series = pd.Series(dataset[column_name])
    return column_name, check_type_comprehensive(series)


def analyze_dataset_parallel(dataset: pd.DataFrame, max_workers: Optional[int] = None) -> Dict[str, Tuple[str, int]]:
    """Параллельно анализирует все колонки датасета"""
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_column_parallel, column, dataset)
            for column in dataset.columns
        ]

        for future in futures:
            column_name, column_result = future.result()
            results[column_name] = column_result

    return results