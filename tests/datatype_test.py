
# test_type_check.py
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock
from utils.type_check import check_type_comprehensive, analyze_dataset_parallel, process_column_parallel

# --- Тесты для check_type_comprehensive ---

class TestCheckTypeComprehensive:

    # Тест: Обработка None
    def test_none_input(self):
        result_type, nan_count = check_type_comprehensive(None)
        assert result_type == 'None'
        assert nan_count == 0

    # Тест: Обработка одиночного значения - int
    def test_single_int(self):
        result_type, nan_count = check_type_comprehensive(42)
        assert result_type == 'int'
        assert nan_count == 0

    # Тест: Обработка одиночного значения - float
    def test_single_float(self):
        result_type, nan_count = check_type_comprehensive(3.14)
        assert result_type == 'float'
        assert nan_count == 0

    # Тест: Обработка одиночного значения - bool
    def test_single_bool(self):
        result_type, nan_count = check_type_comprehensive(True)
        assert result_type == 'bool'
        assert nan_count == 0

    # Тест: Обработка одиночного значения - str
    def test_single_str(self):
        result_type, nan_count = check_type_comprehensive("hello")
        assert result_type == 'str'
        assert nan_count == 0

    # Тест: Обработка одиночного значения - pd.NA
    def test_single_na(self):
        result_type, nan_count = check_type_comprehensive(pd.NA)
        assert result_type == 'None'
        assert nan_count == 1

    # Тест: Пустая серия
    def test_empty_series(self):
        series = pd.Series([], dtype=object)
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == 'None' # Согласно логике функции
        assert nan_count == 0

    # Тест: Серия только с NaN
    def test_all_nan_series(self):
        series = pd.Series([np.nan, pd.NA, None])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == 'None'
        assert nan_count == 3

    # Тест: Серия только с пустыми строками
    def test_all_empty_str_series(self):
        series = pd.Series(['', '  ', ''])
        result_type, nan_count = check_type_comprehensive(series)
        # empty идет в type_counts, но max берется из valid_types (без empty), остается пусто
        assert result_type == 'None'
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - int
    @pytest.mark.parametrize("data, expected_type", [
        ([1, 2, 3], 'int'),
        (['1', '2', '3'], 'int'),
        ([1, 2, '3'], 'int'), # 3 int, 1 str -> int
    ])
    def test_dominant_int(self, data, expected_type):
        series = pd.Series(data)
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == expected_type
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - float
    @pytest.mark.parametrize("data, expected_type", [
        ([1.1, 2.2, 3.3], 'float'),
        (['1.1', '2.2', '3.3'], 'float'),
        ([1.1, 2, '3.3'], 'float'), # 2 float (1.1, 3.3), 1 int (2), 1 str -> float
        ([1, 2, 3.0], 'float'), # Смешение int и float -> float
        (['1e10', '2.5', '3'], 'float'), # Научная нотация
    ])
    def test_dominant_float(self, data, expected_type):
        series = pd.Series(data)
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == expected_type
        assert nan_count == 0

    # Тест: Приоритет float над int
    def test_float_priority_over_int(self):
        series = pd.Series([1, 2, 3.0]) # int и float
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == 'float'
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - bool
    @pytest.mark.parametrize("data, expected_type", [
        ([True, False, True], 'bool'),
        (['true', 'false', 'True'], 'bool'),
        (['yes', 'no', 'YES'], 'bool'),
    ])
    def test_dominant_bool(self, data, expected_type):
        series = pd.Series(data)
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == expected_type
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - str
    def test_dominant_str(self):
        series = pd.Series(['hello', 'world', '123abc'])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == 'str'
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - date
    def test_dominant_date(self):
        series = pd.Series(['2023-01-01', '2024-12-31', '2022-06-15'])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == 'date'
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - time
    def test_dominant_time(self):
        series = pd.Series(['12:00', '14:30:45', '23:59:59.999'])
        result_type, nan_count = check_type_comprehensive(series)
        # Проверим, что хотя бы один из них распознан как время
        # В текущей логике time_milliseconds будет первым, кто сработает на '23:59:59.999'
        # Но time_patterns не проверяют pd.to_datetime. Они просто match.
        # time_basic ('12:00') match, time_seconds ('14:30:45') match, time_milliseconds ('23:59:59.999') match
        # Все три распознаются как 'time'.
        assert result_type == 'time'
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - datetime
    def test_dominant_datetime(self):
        series = pd.Series(['2023-01-01 12:00:00', '2024-12-31T14:30:45'])
        result_type, nan_count = check_type_comprehensive(series)
        # datetime_iso и datetime_common проверяют pd.to_datetime.
        # datetime более специфичен, проверяется первым.
        assert result_type == 'datetime'
        assert nan_count == 0

    # Тест: Смешанные типы - проверка выбора наиболее частого
    def test_mixed_types_most_frequent(self):
        # 4 int, 3 str
        series = pd.Series([1, 2, 3, 4, 'text', 'text', 'text'])
        result_type, nan_count = check_type_comprehensive(series)
        # Числа 1,2,3,4 -> 4 'int'. Слова 'text' -> 3 'str'.
        # 'int' встречается чаще.
        assert result_type == 'int'
        assert nan_count == 0

    # Тест: Смешанные типы с NaN
    def test_mixed_types_with_nan(self):
        series = pd.Series([1, 2, 'text', 'text', np.nan, pd.NA])
        result_type, nan_count = check_type_comprehensive(series)
        # 2 int, 2 str, 2 nan
        # 'int' и 'str' равны. Берется первый по порядку, который чаще - int.
        assert result_type == 'int'
        assert nan_count == 2


# --- Тесты для analyze_dataset_parallel и вспомогательных функций ---

class TestAnalyzeDatasetParallel:

    # Тест: analyze_dataset_parallel с простым DataFrame
    def test_analyze_dataset_parallel_basic(self):
        # Создаем DataFrame как если бы у него был атрибут column_names
        df_data = {
            'col_int': [1, 2, 3],
            'col_str': ['a', 'b', 'c'],
            'col_float': [1.1, 2.2, 3.3],
            'col_bool': [True, False, True]
        }
        df = pd.DataFrame(df_data)
        # Вручную добавляем атрибут, если функция его ожидает
        df.column_names = df.columns.tolist()

        results = analyze_dataset_parallel(df, max_workers=2)

        expected_results = {
            'col_int': ('int', 0),
            'col_str': ('str', 0),
            'col_float': ('float', 0),
            'col_bool': ('bool', 0)
        }
        assert results == expected_results

    # Тест: analyze_dataset_parallel с NaN
    def test_analyze_dataset_parallel_with_nan(self):
        df_data = {
            'col_with_nan': [1, 2, None],  # -> pandas создаст float64 Series: [1.0, 2.0, nan]
            'col_all_nan': [None, None, None]  # -> [nan, nan, nan]
        }
        df = pd.DataFrame(df_data)
        df.column_names = df.columns.tolist()

        results = analyze_dataset_parallel(df)

        # Исправленные ожидания:
        # col_with_nan: pandas.Series([1, 2, None]) -> dtype=float64 -> значения [1.0, 2.0, nan] -> ('float', 1)
        # col_all_nan: pandas.Series([None, None, None]) -> dtype=object -> значения [nan, nan, nan] -> ('None', 3)
        expected_results = {
            'col_with_nan': ('float', 1),  # 2 float (1.0, 2.0), 1 nan (из-за None)
            'col_all_nan': ('None', 3)  # 3 nan
        }
        assert results == expected_results

    # Тест: process_column_parallel (внутренняя функция)
    def test_process_column_parallel(self):
        df_data = {
            'col_test': [1, 2, 3.0]
        }
        df = pd.DataFrame(df_data)

        column_name, result = process_column_parallel('col_test', df)

        assert column_name == 'col_test'
        assert result == ('float', 0) # int и float -> float

    # Тест: analyze_dataset_parallel с пустым DataFrame
    def test_analyze_dataset_parallel_empty(self):
        df = pd.DataFrame()
        df.column_names = df.columns.tolist() # []

        results = analyze_dataset_parallel(df)

        assert results == {}

    # Тест: analyze_dataset_parallel с max_workers=None
    def test_analyze_dataset_parallel_default_workers(self):
        df_data = {
            'col1': [1, 2],
            'col2': ['a', 'b']
        }
        df = pd.DataFrame(df_data)
        df.column_names = df.columns.tolist()

        results = analyze_dataset_parallel(df, max_workers=None) # Должно работать

        expected_results = {
            'col1': ('int', 0),
            'col2': ('str', 0)
        }
        assert results == expected_results

# --- Тесты, имитирующие реальные датасеты ---

class TestRealisticDatasets:

    def test_analyze_pl_march_2021_like(self):

        df = pd.read_csv('P  L March 2021.csv')
        df.column_names = df.columns.tolist()

        results = analyze_dataset_parallel(df)

        # Проверяем ожидаемые типы
        assert results['Category'][0] == 'str'
        assert results['Sku'][0] == 'str'
        assert results['Catalog'][0] == 'str'
        assert results['Weight'][0] == 'float'
        assert results['TP 1'][0] == 'float'
        assert results['TP 2'][0] == 'float'
        assert results['MRP Old'][0] == 'float'
        assert results['Final MRP Old'][0] == 'int'
        assert results['Ajio MRP'][0] == 'int'
        assert results['Amazon MRP'][0] == 'int'
        assert results['Amazon FBA MRP'][0] == 'int'
        assert results['Flipkart MRP'][0] == 'int'
        assert results['Limeroad MRP'][0] == 'int'
        assert results['Myntra MRP'][0] == 'int'
        assert results['Paytm MRP'][0] == 'int'
        assert results['Snapdeal MRP'][0] == 'int'

    def test_analyze_may_2022_like(self):
        """Тест, имитирующий May-2022.csv"""
        # Создаем данные, похожие на описанные в тесте
        df = pd.read_csv('May-2022.csv')
        df.column_names = df.columns.tolist()

        results = analyze_dataset_parallel(df)

        # Проверяем ожидаемые типы
        assert results['Sku'][0] == 'str'
        assert results['Catalog'][0] == 'str'
        assert results['Category'][0] == 'str'
        assert results['Weight'][0] == 'float'
        assert results['MRP Old'][0] == 'int'
        assert results['Final MRP Old'][0] == 'int'
        assert results['Ajio MRP'][0] == 'float'
        assert results['Amazon MRP'][0] == 'int'
        assert results['Amazon FBA MRP'][0] == 'int'
        assert results['Flipkart MRP'][0] == 'int'
        assert results['Limeroad MRP'][0] == 'int'
        assert results['Myntra MRP'][0] == 'int'
        assert results['Paytm MRP'][0] == 'int'
        assert results['Snapdeal MRP'][0] == 'int'
        assert results['TP 1 & TP 2 MRP Old'][0] == 'int'

    def test_analyze_amazon_sale_report_like(self):
        """Тест, имитирующий Amazon Sale Report.csv"""
        # Создаем данные, похожие на описанные в тесте
        df = pd.read_csv('Amazon Sale Report.csv')
        df.column_names = df.columns.tolist()

        results = analyze_dataset_parallel(df)

        # Проверяем ожидаемые типы
        assert results['Category'][0] == 'str'
        assert results['Size'][0] == 'str'
        assert results['Date'][0] == 'date'
        assert results['Status'][0] == 'str'
        assert results['Fulfilment'][0] == 'str'
        assert results['Style'][0] == 'str'
        assert results['SKU'][0] == 'str'
        assert results['ASIN'][0] == 'str'
        assert results['Courier Status'][0] == 'str'
        assert results['Qty'][0] == 'float'
        assert results['Amount'][0] == 'float'
        assert results['B2B'][0] == 'bool'
        assert results['currency'][0] == 'str'

    def test_realistic_dataset_with_nans(self):
        """Тест, имитирующий датасет с пропусками, как в реальных данных"""
        df_data = {
            'Sku': ['SKU010', 'SKU011', np.nan], # str с NaN
            'Weight': [1.0, np.nan, 2.5],       # float с NaN
            'Qty': [1, 0, np.nan],              # int с NaN
            'B2B': [True, np.nan, False],       # bool с NaN
        }
        df = pd.DataFrame(df_data)
        df.column_names = df.columns.tolist()

        results = analyze_dataset_parallel(df)

        # Проверяем типы и количество NaN
        assert results['Sku'] == ('str', 1) # 2 str, 1 nan
        assert results['Weight'] == ('float', 1) # 2 float, 1 nan
        assert results['Qty'] == ('int', 1) # 2 int, 1 nan
        assert results['B2B'] == ('bool', 1) # 2 bool, 1 nan

