import json
import pandas as pd
# Загружаем train и результаты
train = pd.read_csv('../../datasets/WikiTableQuestions/training.tsv', sep='\t')

pd.set_option('display.max_columns', None)      # все колонки
pd.set_option('display.max_colwidth', None)     # полный текст (без ограничений)
pd.set_option('display.width', None)            # ширина вывода без переносов
pd.set_option('display.max_rows', None)

with open('train_results/final_merged_results.json', 'r') as f:
    df = pd.json_normalize(json.load(f))

with open('../../datasets/wtq_sql/squall.json') as f:
    sql = pd.json_normalize(json.load(f))

with open('train_results/sql_to_pandas.json') as f:
    sql_to_pandas = pd.json_normalize(json.load(f))

with open('train_results/logic_train/sql_to_pandas_logic.json') as f:
    sql_to_pandas_logic = pd.json_normalize(json.load(f))

with open('train_results/corrected_train/sql_to_pandas_corrected.json') as f:
    sql_to_pandas_corrected = pd.json_normalize(json.load(f))


from datasets import Dataset
import json

# Загружаем JSON файл
with open('train_results/final_result.json', 'r') as f:
    data = json.load(f)

# Преобразуем все значения в строки
for item in data:
    for key, value in item.items():
        if value is None:
            item[key] = ''
        else:
            item[key] = str(value)

# Теперь создаём Dataset
dataset = Dataset.from_list(data)
dataset.save_to_disk('wtq-sql-pandas')
print(f"Dataset сохранён в папку 'my_dataset'")
print(f"Всего записей: {len(dataset)}")
print()

# Выводим несколько примеров данных
print("=" * 80)
print("ПРИМЕРЫ ДАННЫХ (первые 5 записей):")
print("=" * 80)

for i in range(min(5, len(dataset))):
    print(f"\n--- Пример {i+1} ---")
    for key, value in dataset[i].items():
        print(f"{key}: {value}")
    print("-" * 40)
