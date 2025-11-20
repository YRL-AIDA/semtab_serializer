import pandas as pd
import numpy as np
from type_check import check_type_comprehensive


def test_check_type_comprehensive_pnl_march_2021():
    """Тест для P & L March 2021.csv"""
    print("\n=== Тестирование P & L March 2021.csv ===")

    df = pd.read_csv('data_frames/P  L March 2021.csv')

    # Ожидаемые типы данных на основе описания
    expected_types = {
        'Category': 'str',
        'Sku': 'str',
        'Catalog': 'str',
        'Weight': 'float',
        'TP 1': 'int',
        'TP 2': 'float',
        'MRP Old': 'int',
        'Final MRP Old': 'int',
        'Ajio MRP': 'int',
        'Amazon MRP': 'int',
        'Amazon FBA MRP': 'int',
        'Flipkart MRP': 'int',
        'Limeroad MRP': 'int',
        'Myntra MRP': 'int',
        'Paytm MRP': 'int',
        'Snapdeal MRP': 'int'
    }

    errors = []

    for column, expected_type in expected_types.items():
        if column in df.columns:
            series = df[column]
            result_type, nan_count = check_type_comprehensive(series)

            print(f"Колонка '{column}':")
            print(f"  Ожидаемый тип: {expected_type}")
            print(f"  Полученный тип: {result_type}")
            print(f"  Пропуски: {nan_count}")
            print(f"  Всего записей: {len(series)}")

            # Проверяем, что полученный тип соответствует ожидаемому
            if result_type != expected_type:
                errors.append(f"Колонка '{column}': ожидался {expected_type}, получен {result_type}")

            # Проверяем, что nan_count не отрицательный и не больше общего количества
            if not (0 <= nan_count <= len(series)):
                errors.append(f"Колонка '{column}': некорректное количество пропусков: {nan_count}")

    # Выводим все ошибки сразу
    if errors:
        print("\n❌ ОШИБКИ В КОЛОНКАХ:")
        for error in errors:
            print(f"  - {error}")
        raise AssertionError(
            f"Найдено {len(errors)} ошибок в колонках: {', '.join([e.split(':')[0].replace('Колонка ', '') for e in errors])}")
    else:
        print("✅ Все колонки прошли проверку успешно!")


def test_check_type_comprehensive_may_2022():
    """Тест для May-2022.csv"""
    print("\n=== Тестирование May-2022.csv ===")

    df = pd.read_csv('data_frames/May-2022.csv')

    expected_types = {
        'Sku': 'str',
        'Catalog': 'str',
        'Category': 'str',
        'Weight': 'float',
        'MRP Old': 'int',
        'Final MRP Old': 'int',
        'Ajio MRP': 'int',
        'Amazon MRP': 'int',
        'Amazon FBA MRP': 'int',
        'Flipkart MRP': 'int',
        'Limeroad MRP': 'int',
        'Myntra MRP': 'int',
        'Paytm MRP': 'int',
        'Snapdeal MRP': 'int',
        'TP 1 & TP 2 MRP Old': 'int'
    }

    errors = []

    for column, expected_type in expected_types.items():
        if column in df.columns:
            series = df[column]
            result_type, nan_count = check_type_comprehensive(series)

            print(f"Колонка '{column}':")
            print(f"  Ожидаемый тип: {expected_type}")
            print(f"  Полученный тип: {result_type}")
            print(f"  Пропуски: {nan_count}")
            print(f"  Всего записей: {len(series)}")

            if result_type != expected_type:
                errors.append(f"Колонка '{column}': ожидался {expected_type}, получен {result_type}")

            if not (0 <= nan_count <= len(series)):
                errors.append(f"Колонка '{column}': некорректное количество пропусков: {nan_count}")

    if errors:
        print("\n❌ ОШИБКИ В КОЛОНКАХ:")
        for error in errors:
            print(f"  - {error}")
        raise AssertionError(
            f"Найдено {len(errors)} ошибок в колонках: {', '.join([e.split(':')[0].replace('Колонка ', '') for e in errors])}")
    else:
        print("✅ Все колонки прошли проверку успешно!")


def test_check_type_comprehensive_amazon_sale_report():
    """Тест для Amazon Sale Report.csv"""
    print("\n=== Тестирование Amazon Sale Report.csv ===")

    df = pd.read_csv('data_frames/Amazon Sale Report.csv')

    expected_types = {
        'Category': 'str',
        'Size': 'str',
        'Date': 'date',
        'Status': 'str',
        'Fulfilment': 'str',
        'Style': 'str',
        'SKU': 'str',
        'ASIN': 'str',
        'Courier Status': 'str',
        'Qty': 'int',
        'Amount': 'float',
        'B2B': 'bool',
        'currency': 'str'
    }

    errors = []

    for column, expected_type in expected_types.items():
        if column in df.columns:
            series = df[column]
            result_type, nan_count = check_type_comprehensive(series)

            print(f"Колонка '{column}':")
            print(f"  Ожидаемый тип: {expected_type}")
            print(f"  Полученный тип: {result_type}")
            print(f"  Пропуски: {nan_count}")
            print(f"  Всего записей: {len(series)}")

            if result_type != expected_type:
                errors.append(f"Колонка '{column}': ожидался {expected_type}, получен {result_type}")

            if not (0 <= nan_count <= len(series)):
                errors.append(f"Колонка '{column}': некорректное количество пропусков: {nan_count}")

    if errors:
        print("\n❌ ОШИБКИ В КОЛОНКАХ:")
        for error in errors:
            print(f"  - {error}")
        raise AssertionError(
            f"Найдено {len(errors)} ошибок в колонках: {', '.join([e.split(':')[0].replace('Колонка ', '') for e in errors])}")
    else:
        print("✅ Все колонки прошли проверку успешно!")


def test_check_type_comprehensive_edge_cases():
    """Тест граничных случаев"""
    print("\n=== Тестирование граничных случаев ===")

    errors = []

    # Тест 1: Пустая серия
    empty_series = pd.Series([], dtype=object)
    result_type, nan_count = check_type_comprehensive(empty_series)
    print(f"Пустая серия: тип={result_type}, пропуски={nan_count}")
    if result_type != 'None':
        errors.append(f"Пустая серия: ожидался 'unknown', получен '{result_type}'")

    # Тест 2: Только пропуски
    nan_series = pd.Series([np.nan, np.nan, np.nan])
    result_type, nan_count = check_type_comprehensive(nan_series)
    print(f"Только пропуски: тип={result_type}, пропуски={nan_count}")
    if result_type != 'None':
        errors.append(f"Только пропуски: ожидался 'None', получен '{result_type}'")

    # Тест 3: Смешанные типы (должен вернуть наиболее частый)
    mixed_series = pd.Series([1, 2, 3, 'text', 'text', 'text', 4])
    result_type, nan_count = check_type_comprehensive(mixed_series)
    print(f"Смешанные типы: тип={result_type}, пропуски={nan_count}")
    if result_type != 'int':
        errors.append(f"Смешанные типы: ожидался 'str', получен '{result_type}'")

    if errors:
        print("\n❌ ОШИБКИ В ГРАНИЧНЫХ СЛУЧАЯХ:")
        for error in errors:
            print(f"  - {error}")
        raise AssertionError(f"Найдено {len(errors)} ошибок в граничных случаях")
    else:
        print("✅ Все граничные случаи прошли проверку успешно!")


def print_detailed_analysis():
    """Детальный анализ проблемных колонок"""
    print("\n" + "=" * 60)
    print("ДЕТАЛЬНЫЙ АНАЛИЗ ПРОБЛЕМНЫХ КОЛОНОК")
    print("=" * 60)

    datasets = [
        ('P & L March 2021.csv', 'data_frames/P  L March 2021.csv', {
            'Weight': 'int',
            'TP 1': 'int',
            'TP 2': 'int',
            'MRP Old': 'int'
        }),
        ('May-2022.csv', 'data_frames/May-2022.csv', {
            'Weight': 'int',
            'MRP Old': 'int',
            'Final MRP Old': 'int'
        }),
        ('Amazon Sale Report.csv', 'data_frames/Amazon Sale Report.csv', {
            'Qty': 'int',
            'Amount': 'float',
            'B2B': 'bool'
        })
    ]

    for name, filepath, problem_columns in datasets:
        try:
            df = pd.read_csv(filepath)
            print(f"\n🔍 {name}:")

            for column, expected_type in problem_columns.items():
                if column in df.columns:
                    series = df[column]
                    result_type, nan_count = check_type_comprehensive(series)

                    status = "✅" if result_type == expected_type else "❌"
                    print(f"  {status} {column}: ожидалось {expected_type}, получено {result_type}")

                    # Дополнительная диагностика для проблемных колонок
                    if result_type != expected_type:
                        print(f"     Первые 5 значений: {series.head().tolist()}")
                        print(f"     Уникальные значения: {series.unique()[:10]}...")
                        print(f"     Тип данных pandas: {series.dtype}")

        except Exception as e:
            print(f"  ❌ Ошибка загрузки: {e}")


# Запуск всех тестов
if __name__ == "__main__":
    all_passed = True

    try:
        test_check_type_comprehensive_pnl_march_2021()
    except AssertionError as e:
        print(f"❌ Тест не пройден: {e}")
        all_passed = False

    try:
        test_check_type_comprehensive_may_2022()
    except AssertionError as e:
        print(f"❌ Тест не пройден: {e}")
        all_passed = False

    try:
        test_check_type_comprehensive_amazon_sale_report()
    except AssertionError as e:
        print(f"❌ Тест не пройден: {e}")
        all_passed = False

    try:
        test_check_type_comprehensive_edge_cases()
    except AssertionError as e:
        print(f"❌ Тест не пройден: {e}")
        all_passed = False

    # Печатаем детальный анализ проблемных колонок
    print_detailed_analysis()

    if all_passed:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    else:
        print("\n💥 НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОЙДЕНЫ!")
