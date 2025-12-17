from typing import Dict, List, Any, Optional, Tuple
from type_check import check_type_comprehensive
import requests
import pandas as pd
import json


def parse_text(text: Any, locale: str = 'en_US') -> List[Dict[str, Any]]:
    """
    Парсит текст через Duckling.

    :param text: Текст для парсинга. Может быть любым типом, который может быть преобразован в str.
    :param locale: Локаль для парсинга (по умолчанию 'en_US').
    :return: Список словарей, представляющих результаты парсинга Duckling.
    """
    text_str = str(text) if pd.notna(text) else ""

    try:
        response = requests.post(
            'http://localhost:8000/parse',
            data={'locale': locale, 'text': text_str}
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при обращении к Duckling: {e}")
        return []


def get_type(value: Any) -> str:
    """
    Определяет тип значения, используя Duckling и fallback.

    :param value: Значение для определения типа.
    :return: Строка с типом значения.
    """
    if pd.notna(value):
        parsed: List[Dict[str, Any]] = parse_text(str(value))
        if parsed:
            types: List[str] = [item['dim'] for item in parsed]
            return types[0] if types else check_type_comprehensive(value)[0]
        else:
            return check_type_comprehensive(value)[0]
    else:
        return check_type_comprehensive(value)[0]


def df_to_duckling(df: pd.DataFrame, lines: int = 5) -> Dict[str, Tuple[str, int]]:
    """
    Анализирует типы данных в первых 'lines' строк (используя Duckling и fallback),
    затем агрегирует их, чтобы определить преобладающий тип и общее количество пропусков.

    :param df: Исходный DataFrame.
    :param lines: Количество первых строк для анализа типов и пропусков.
    :return: Словарь: {Имя_столбца: (Преобладающий_тип: str, Общее_количество_пропусков: int)}
    """
    aggregated_results: Dict[str, Tuple[str, int]] = {}

    for col in df.columns:
        # Берем только первые 'lines' строк для анализа
        first_rows = df[col].head(lines)
        first_values: List[Any] = first_rows.tolist()

        type_list: List[str] = [get_type(v) for v in first_values]

        type_counts = {}
        for t in type_list:
            if t not in ['None', 'empty']:
                type_counts[t] = type_counts.get(t, 0) + 1

        if type_counts:
            most_common_type = max(type_counts.items(), key=lambda x: x[1])[0]
        else:
            most_common_type = 'None'

        total_nan_count: int = int(first_rows.isnull().sum())
        aggregated_results[col] = (most_common_type, total_nan_count)

    return aggregated_results


df = pd.read_csv('C:/Users/PC/semtab_serializer/tests/Amazon Sale Report.csv')
result = df_to_duckling(df, 3)

for col, types_list in result.items():
    print(col, types_list)



print(df.head(3))