import json


def normalize_and_compare(result, target):
    # Проверка на пустые значения
    if result is None or target is None:
        return False

    # Преобразуем result в список
    if isinstance(result, list):
        result_list = [str(x).strip() for x in result]
    elif isinstance(result, str):
        # Убираем скобки и кавычки, если они есть
        if result.startswith('[') and result.endswith(']'):
            # Убираем [ и ]
            result = result[1:-1]
            # Разделяем по запятым
            result_list = [x.strip().strip("'").strip('"') for x in result.split(',')]
        else:
            result_list = [result.strip()]
    else:
        result_list = [str(result).strip()]

    # Преобразуем target в список
    target_list = [x.strip() for x in target.split('|')]

    # Сортируем и сравниваем
    return sorted(result_list) == sorted(target_list)


# Загрузка данных
with open('train_results/sql_to_pandas.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Пересчитываем is_correct и заменяем значение
correct_count = 0
for item in data:
    if item.get('code') is None:
        continue

    result_val = item.get('result')
    target_val = item.get('targetValue')

    if result_val is None or target_val is None:
        continue

    # Заменяем is_correct на пересчитанное значение
    item['is_correct'] = normalize_and_compare(result_val, target_val)

    if item['is_correct']:
        correct_count += 1

# Сохраняем в новый файл с припиской corrected
output_path = 'train_results/results_20260602_151233_corrected.json'
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"✅ Создан файл: {output_path}")
print(f"✅ Пересчитано правильных: {correct_count} из {len(data)}")