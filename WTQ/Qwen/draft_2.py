import pandas as pd
import json
import sys
from utils.normalize import convert_dataset_types, convert_type, find_word
from utils.type_check import analyze_dataset_parallel  # не обязательно, но может пригодиться
from check_result import extract_code_from_response  # если код обёрнут в {"PANDA": ...}

# ========== НАСТРОЙКИ ==========
# Путь к файлу с результатами (тот, где is_correct == true)
JSON_PATH = 'train_results/results_20260602_151233_corrected.json'   # укажите свой
# Идентификатор примера, который хотим проверить (можно nt_id или числовой id)
EXAMPLE_NT_ID = 'nt-12'    # или 'nt-3', 'nt-14' и т.д.
# Путь к training.tsv (нужен для получения пути к таблице)
  # при необходимости исправьте

# ========== ЗАГРУЗКА ДАННЫХ ==========
train = pd.read_csv('training.tsv', sep='\t')

with open(JSON_PATH, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Ищем запись по nt_id
target_item = None
for item in data:
    if item.get('nt_id') == EXAMPLE_NT_ID or str(item.get('id')) == EXAMPLE_NT_ID:
        target_item = item
        break

if target_item is None:
    print(f"Пример {EXAMPLE_NT_ID} не найден в {JSON_PATH}")
    sys.exit(1)

print(f"Найден пример: id={target_item.get('id')}, nt_id={target_item.get('nt_id')}")
print(f"Ожидаемый ответ: {target_item['targetValue']}")
print(f"Сохранённый is_correct: {target_item.get('is_correct')}")

# ========== ИЗВЛЕЧЕНИЕ PANDAS-КОДА ==========
code_raw = target_item['code']
if isinstance(code_raw, str):
    try:
        code_dict = json.loads(code_raw)
    except:
        code_dict = {}
else:
    code_dict = code_raw

pandas_code = code_dict.get('PANDA') or code_dict.get('pandas')
if pandas_code is None:
    print("Не удалось извлечь pandas-код из поля 'code'")
    sys.exit(1)

print(f"\nPandas-код:\n{pandas_code}\n")

# ========== ЗАГРУЗКА ТАБЛИЦЫ ==========
# Определяем путь к таблице через training.tsv
row = train[train['id'] == target_item['nt_id']].iloc[0]  # ищем по nt_id
file_path = row['context']
tsv_path = file_path.replace('.csv', '.tsv').replace('/csv/', '/tsv/')
full_path = tsv_path

print(f"Загружаем таблицу: {full_path}")
try:
    df_raw = pd.read_csv(full_path, sep='\t')
except Exception as e:
    print(f"Ошибка чтения: {e}")
    sys.exit(1)

# ========== НОРМАЛИЗАЦИЯ ТАБЛИЦЫ (как в запуске 2) ==========
try:
    df_norm = convert_dataset_types(df_raw)
except Exception as e:
    print(f"Ошибка convert_dataset_types: {e}")
    df_norm = df_raw.copy()

print(f"Размер таблицы: {df_norm.shape}")
print(f"Колонки: {list(df_norm.columns)}")
print("\nПервые 3 строки:")
print(df_norm.head(3).to_string())

# ========== ВЫПОЛНЕНИЕ КОДА ==========
# Подготавливаем окружение (как в run_code_and_get_result)
# Обратите внимание: может потребоваться предварительная чистка кода через extract_code_from_response,
# но в нашем случае код уже внутри {"PANDA": ...} – он чистый.
# Если код напрямую не выполняется, попробуйте извлечь через extract_code_from_response.

clean_code = pandas_code  # если уже чистый
# Альтернативно: clean_code = extract_code_from_response(pandas_code) или extract_code_from_response(code_raw)

try:
    result = eval(clean_code, {
        'df': df_norm,
        'pd': pd,
        'np': __import__('numpy'),
        'convert_type': convert_type,
        'find_word': find_word
    })
    print(f"\nРезультат выполнения: {result} (тип: {type(result)})")
except Exception as e:
    print(f"\nОшибка выполнения: {e}")
    result = None

# ========== ПРИВЕДЕНИЕ К СТРОКЕ (как при генерации) ==========
if result is not None:
    if isinstance(result, list):
        result_str = '|'.join(str(x) for x in result)
    elif isinstance(result, pd.Series):
        if len(result) == 1:
            result_str = str(result.iloc[0])
        else:
            result_str = '|'.join(str(x) for x in result.tolist())
    else:
        result_str = str(result)
else:
    result_str = 'None'

print(f"Результат после приведения к строке: '{result_str}'")
print(f"Ожидаемое значение: '{target_item['targetValue']}'")

# ========== СРАВНЕНИЕ ==========
# Используем тот же способ, что и при сохранении (safe_convert_type)
try:
    norm_target = convert_type(target_item['target_value'])  # или safe_convert_type
    is_correct_now = (result == norm_target) if result is not None else False
except:
    is_correct_now = False

print(f"\nСравнение через convert_type: {is_correct_now}")

# Дополнительное сравнение через строки
print(f"Сравнение через строки: {result_str == str(target_item['targetValue'])}")

# Если результат не совпадает – выводим диагностику
if not is_correct_now and result is not None:
    print("\n=== ДИАГНОСТИКА РАСХОЖДЕНИЯ ===")
    # Проверим, работает ли find_word для этого кода (если он используется)
    if 'find_word' in pandas_code:
        # Попробуем выделить аргумент find_word
        import re
        match = re.search(r"find_word\(['\"]([^'\"]+)['\"]\)", pandas_code)
        if match:
            word = match.group(1)
            found = find_word(word)
            print(f"find_word('{word}') вернул: '{found}' (тип {type(found)})")
            # Проверим, есть ли такое значение в колонке, с которой сравнивается
            # (можно дополнительно вывести уникальные значения)
    # Выведем типы данных в таблице
    print("Типы колонок после нормализации:")
    print(df_norm.dtypes)