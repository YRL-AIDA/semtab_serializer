# test_type_check.py
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock
from utils.type_check import check_type_comprehensive, analyze_dataset_parallel, process_column_parallel


# Общие константы для всех тестовых классов
class TestConstants:
    const_None = 'None'
    const_int = 'int'
    const_float = 'float'
    const_date = 'date'
    const_time = 'time'
    const_bool = 'bool'
    const_str = 'str'
    const_datetime = 'datetime'


# --- Тесты для check_type_comprehensive ---
class TestCheckTypeComprehensive(TestConstants):
    # Тест: Обработка None
    def test_none_input(self):
        result_type, nan_count = check_type_comprehensive(None)
        assert result_type == self.const_None
        assert nan_count == 0

    # Тест: Обработка одиночного значения - int
    def test_single_int(self):
        result_type, nan_count = check_type_comprehensive(42)
        assert result_type == self.const_int
        assert nan_count == 0

    # Тест: Обработка одиночного значения - float
    def test_single_float(self):
        result_type, nan_count = check_type_comprehensive(3.14)
        assert result_type == self.const_float
        assert nan_count == 0

    # Тест: Обработка одиночного значения - bool
    def test_single_bool(self):
        result_type, nan_count = check_type_comprehensive(True)
        assert result_type == self.const_bool
        assert nan_count == 0

    # Тест: Обработка одиночного значения - str
    def test_single_str(self):
        result_type, nan_count = check_type_comprehensive("hello")
        assert result_type == self.const_str
        assert nan_count == 0

    # Тест: Обработка одиночного значения - pd.NA
    def test_single_na(self):
        result_type, nan_count = check_type_comprehensive(pd.NA)
        assert result_type == self.const_None
        assert nan_count == 1

    # Тест: Пустая серия
    def test_empty_series(self):
        series = pd.Series([], dtype=object)
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_None
        assert nan_count == 0

    # Тест: Серия только с NaN
    def test_all_nan_series(self):
        series = pd.Series([np.nan, pd.NA, None])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_None
        assert nan_count == 3

    # Тест: Серия только с пустыми строками
    def test_all_empty_str_series(self):
        series = pd.Series(['', '  ', ''])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_None
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - int
    @pytest.mark.parametrize("data, expected_type", [
        ([1, 2, 3], 'int'),
        (['1', '2', '3'], 'int'),
        ([1, 2, '3'], 'int'),
    ])
    def test_dominant_int(self, data, expected_type):
        series = pd.Series(data)
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == getattr(self, f"const_{expected_type}")
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - float
    @pytest.mark.parametrize("data, expected_type", [
        ([1.1, 2.2, 3.3], 'float'),
        (['1.1', '2.2', '3.3'], 'float'),
        ([1.1, 2, '3.3'], 'float'),
        ([1, 2, 3.0], 'float'),
        (['1e10', '2.5', '3'], 'float'),
    ])
    def test_dominant_float(self, data, expected_type):
        series = pd.Series(data)
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == getattr(self, f"const_{expected_type}")
        assert nan_count == 0

    # Тест: Приоритет float над int
    def test_float_priority_over_int(self):
        series = pd.Series([1, 2, 3.0])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_float
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
        assert result_type == getattr(self, f"const_{expected_type}")
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - str
    def test_dominant_str(self):
        series = pd.Series(['hello', 'world', '123abc'])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_str
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - date
    def test_dominant_date(self):
        series = pd.Series(['2023-01-01', '2024-12-31', '2022-06-15'])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_date
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - time
    def test_dominant_time(self):
        series = pd.Series(['12:00', '14:30:45', '23:59:59.999'])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_time
        assert nan_count == 0

    # Тест: Серия с преобладающим типом - datetime
    def test_dominant_datetime(self):
        series = pd.Series(['2023-01-01 12:00:00', '2024-12-31T14:30:45'])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_datetime
        assert nan_count == 0

    # Тест: Смешанные типы - проверка выбора наиболее частого
    def test_mixed_types_most_frequent(self):
        series = pd.Series([1, 2, 3, 4, 'text', 'text', 'text'])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_int
        assert nan_count == 0

    # Тест: Смешанные типы с NaN
    def test_mixed_types_with_nan(self):
        series = pd.Series([1, 2, 3, 'text', 'text', np.nan, pd.NA])
        result_type, nan_count = check_type_comprehensive(series)
        assert result_type == self.const_int
        assert nan_count == 2


# --- Тесты для новых форматов данных ---
class TestNewFormats(TestConstants):
    # Тест: Пробелы вокруг значений
    # Тесты для пробелов вокруг значений
    def test_spaces_around_int(self):
        """Тест пробелов вокруг целых чисел"""
        test_cases = [
            ("  -5  ", self.const_int),
            (" +42 ", self.const_int),
            ("  100  ", self.const_int),
            ("  -100  ", self.const_int),
            ("  +100  ", self.const_int),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    def test_spaces_around_float(self):
        """Тест пробелов вокруг чисел с плавающей точкой"""
        test_cases = [
            ("  3.14  ", self.const_float),
            ("  .5  ", self.const_float),
            ("  -3.14  ", self.const_float),
            ("  +3.14  ", self.const_float),
            ("  0.0  ", self.const_float),
            ("  42.  ", self.const_float),  # Неполный float
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    def test_spaces_around_bool(self):
        """Тест пробелов вокруг булевых значений"""
        test_cases = [
            ("  true  ", self.const_bool),
            ("  FALSE  ", self.const_bool),
            ("  yes  ", self.const_bool),
            ("  NO  ", self.const_bool),
            ("  да  ", self.const_bool),
            ("  нет  ", self.const_bool),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    def test_spaces_around_date(self):
        """Тест пробелов вокруг дат"""
        test_cases = [
            ("  2023-12-31  ", self.const_date),
            ("  31.12.2023  ", self.const_date),
            ("  12/31/2023  ", self.const_date),
            ("  2024-01-01  ", self.const_date),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    def test_spaces_around_time(self):
        """Тест пробелов вокруг времени"""
        test_cases = [
            ("  23:59  ", self.const_time),
            ("  12:30:45  ", self.const_time),
            ("  09:00  ", self.const_time),
            ("  23:59:59.999  ", self.const_time),
            ("  11:30 AM  ", self.const_time),
            ("  9:5:1  ", self.const_time),  # Без ведущих нулей
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    # Тест: Европейский формат с запятыми
    def test_european_decimal_format(self):
        test_cases = [
            ("3,14", self.const_float),
            ("123,45", self.const_float),
            ("0,5", self.const_float),
            ("1.000,50", self.const_float),
            ("1 000,50", self.const_float),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    # Тест: Символы валюты и текст
    def test_currency_and_text(self):
        test_cases = [
            ("$500", self.const_int),
            ("€250", self.const_int),
            ("500 руб.", self.const_int),
            ("42 шт.", self.const_int),
            ("$19.99", self.const_float),
            ("100.50 $", self.const_float),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    # Тест: Неполные float значения
    def test_incomplete_floats(self):
        test_cases = [
            ("42.", self.const_float),
            (".5", self.const_float),
            ("-42.", self.const_float),
            ("+100.", self.const_float),
            ("0.", self.const_float),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    # Тест: Даты с буквенными месяцами
    def test_dates_with_month_names(self):
        test_cases = [
            ("31-Dec-2023", self.const_date),
            ("15-Jan-2024", self.const_date),
            ("01-December-2023", self.const_date),
            ("Dec 31, 2023", self.const_date),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    # Тест: Время без ведущих нулей
    def test_time_without_leading_zeros(self):
        test_cases = [
            ("9:5", self.const_time),
            ("9:5:1", self.const_time),
            ("23:9:5", self.const_time),
            ("9:5 AM", self.const_time),
            ("9:05:1 PM", self.const_time),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    # Тест: Смешанные серии с новыми форматами
    def test_mixed_series_with_new_formats(self):
        # Серия с разными форматами чисел
        series1 = pd.Series(["1,000", "2 000", "3,000.50", "4 000,50"])
        result_type, nan_count = check_type_comprehensive(series1)
        assert result_type == self.const_float
        assert nan_count == 0

        # Серия с датами разных форматов
        series2 = pd.Series(["2023-12-31", "31-Dec-2023", "12/31/2023"])
        result_type, nan_count = check_type_comprehensive(series2)
        assert result_type == self.const_date
        assert nan_count == 0

        # Серия с булевыми значениями разных форматов
        series3 = pd.Series(["true", "yes", "1", "[x]", "false", "no", "0", "[ ]"])
        result_type, nan_count = check_type_comprehensive(series3)
        assert result_type == self.const_bool
        assert nan_count == 0

    # Тест: Отрицательные числа в скобках (финансовый формат)
    def test_parentheses_for_negative(self):
        test_cases = [
            ("(125)", self.const_int),
            ("(1,000)", self.const_int),
            ("(1.000,50)", self.const_float),
            ("(500.00)", self.const_float),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    # Тест: Научная нотация с дополнительными символами
    def test_scientific_notation_variants(self):
        test_cases = [
            ("1.23e-4", self.const_float),
            ("5E+10", self.const_float),
            ("1e3", self.const_float),
            ("-2.5e-2", self.const_float),
            ("+3.14E5", self.const_float),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"
            assert nan_count == 0

    # Тест: Граничные случаи и edge cases
    def test_edge_cases(self):
        test_cases = [
            ("", self.const_None),
            ("   ", self.const_None),
            ("NaN", self.const_str),
            ("null", self.const_str),
            ("N/A", self.const_str),
            ("-", self.const_bool),
            ("+", self.const_bool),
        ]

        for value, expected_type in test_cases:
            result_type, nan_count = check_type_comprehensive(value)
            assert result_type == expected_type, f"Failed for '{value}': got {result_type}, expected {expected_type}"


# --- Тесты для analyze_dataset_parallel и вспомогательных функций ---
class TestAnalyzeDatasetParallel(TestConstants):
    # Тест: analyze_dataset_parallel с простым DataFrame
    def test_analyze_dataset_parallel_basic(self):
        df_data = {
            'col_int': [1, 2, 3],
            'col_str': ['a', 'b', 'c'],
            'col_float': [1.1, 2.2, 3.3],
            'col_bool': [True, False, True]
        }
        df = pd.DataFrame(df_data)

        results = analyze_dataset_parallel(df, max_workers=2)

        expected_results = {
            'col_int': (self.const_int, 0),
            'col_str': (self.const_str, 0),
            'col_float': (self.const_float, 0),
            'col_bool': (self.const_bool, 0)
        }
        assert results == expected_results

    # Тест: analyze_dataset_parallel с NaN
    def test_analyze_dataset_parallel_with_nan(self):
        df_data = {
            'col_with_nan': [1, 2, None],
            'col_all_nan': [None, None, None]
        }
        df = pd.DataFrame(df_data)

        results = analyze_dataset_parallel(df)

        expected_results = {
            'col_with_nan': (self.const_float, 1),
            'col_all_nan': (self.const_None, 3)
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
        assert result == (self.const_float, 0)

    # Тест: analyze_dataset_parallel с пустым DataFrame
    def test_analyze_dataset_parallel_empty(self):
        df = pd.DataFrame()
        results = analyze_dataset_parallel(df)
        assert results == {}

    # Тест: analyze_dataset_parallel с max_workers=None
    def test_analyze_dataset_parallel_default_workers(self):
        df_data = {
            'col1': [1, 2],
            'col2': ['a', 'b']
        }
        df = pd.DataFrame(df_data)

        results = analyze_dataset_parallel(df, max_workers=None)
        expected_results = {
            'col1': (self.const_int, 0),
            'col2': (self.const_str, 0)
        }
        assert results == expected_results

    # Тест: Комплексный DataFrame с новыми форматами
    def test_dataframe_with_new_formats(self):
        df_data = {
            'int_with_spaces': ["  5  ", " -10 ", " +42 "],
            'float_european': ["3,14", "1.000,50", "0,5"],
            'currency': ["$500", "€250.75", "100 руб."],
            'dates_varied': ["2023-12-31", "31-Dec-2023", "12/31/2023"],
            'time_varied': ["23:59", "9:5:1", "11:30 AM"],
        }

        df = pd.DataFrame(df_data)
        results = analyze_dataset_parallel(df, max_workers=2)

        expected_types = {
            'int_with_spaces': (self.const_int, 0),
            'float_european': (self.const_float, 0),
            'currency': (self.const_float, 0),
            'dates_varied': (self.const_date, 0),
            'time_varied': (self.const_time, 0),
        }

        for col, expected in expected_types.items():
            assert col in results, f"Column {col} not in results"
            result_type, nan_count = results[col]
            expected_type, expected_nan = expected
            assert result_type == expected_type, f"Column {col}: got {result_type}, expected {expected_type}"
            assert nan_count == expected_nan


# --- Тесты для реалистичных датасетов ---
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
        assert results['Final MRP Old'][0] == 'float'
        assert results['Ajio MRP'][0] == 'float'
        assert results['Amazon MRP'][0] == 'float'
        assert results['Amazon FBA MRP'][0] == 'float'
        assert results['Flipkart MRP'][0] == 'float'
        assert results['Limeroad MRP'][0] == 'float'
        assert results['Myntra MRP'][0] == 'float'
        assert results['Paytm MRP'][0] == 'float'
        assert results['Snapdeal MRP'][0] == 'float'

    def test_analyze_may_2022_like(self):
        """Тест, имитирующий May-2022.csv"""
        # Создаем данные, похожие на описанные в тесте
        df = pd.read_csv('May-2022.csv')
        df.column_names = df.columns.tolist()

        results = analyze_dataset_parallel(df)

        assert results['Sku'][0] == 'str'
        assert results['Catalog'][0] == 'str'
        assert results['Category'][0] == 'str'
        assert results['Weight'][0] == 'float'
        assert results['MRP Old'][0] == 'float'
        assert results['Final MRP Old'][0] == 'float'
        assert results['Ajio MRP'][0] == 'float'
        assert results['Amazon MRP'][0] == 'float'
        assert results['Amazon FBA MRP'][0] == 'float'
        assert results['Flipkart MRP'][0] == 'float'
        assert results['Limeroad MRP'][0] == 'float'
        assert results['Myntra MRP'][0] == 'float'
        assert results['Paytm MRP'][0] == 'float'
        assert results['Snapdeal MRP'][0] == 'float'
        assert results['MRP Old'][0] == 'float'  # Предполагая, что 'TP 1 & TP 2 MRP Old' соответствует типу 'MRP Old' из нового словаря.

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
        df = pd.DataFrame(df_data, dtype=object)
        df.column_names = df.columns.tolist()

        results = analyze_dataset_parallel(df)

        # Проверяем типы и количество NaN
        assert results['Sku'] == ('str', 1) # 2 str, 1 nan
        assert results['Weight'] == ('float', 1) # 2 float, 1 nan
        assert results['Qty'] == ('int', 1) # 2 int, 1 nan
        assert results['B2B'] == ('bool', 1) # 2 bool, 1 nan