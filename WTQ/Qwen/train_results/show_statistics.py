import pandas as pd
import json
import matplotlib.pyplot
import json
#
# with open('results_20260524_154618.json') as f:
#     results = json.load(f)
#
# unique_error = {}
#
# for i in results:
#     if i['error'] not in unique_error:
#         unique_error[i['error']] = 1
#     else:
#         unique_error[i['error']] += 1
#
# # Сохраняем в JSON файл
# with open('unique_errors.json', 'w', encoding='utf-8') as f:
#     json.dump(unique_error, f, ensure_ascii=False, indent=4)
#
# print("Сохранено в unique_errors.json")


import json

# Загрузка данных
with open('unique_errors.json', 'r') as f:
    errors = json.load(f)

# Сортировка по убыванию (по количеству)
sorted_errors = dict(sorted(errors.items(), key=lambda x: x[1], reverse=True))

# Вывод топ-20
for i, (error, count) in enumerate(list(sorted_errors.items())[:20], 1):
    print(f"{i:2}. {count:5} x {error[:80]}{'...' if len(error) > 80 else ''}")

# Сохранение в файл
with open('sorted_errors.json', 'w', encoding='utf-8') as f:
    json.dump(sorted_errors, f, ensure_ascii=False, indent=4)

print(f"\n✅ Сохранено в sorted_errors.json")
print(f"Всего типов ошибок: {len(sorted_errors)}")
print(f"null (успешно): {sorted_errors.get('null', 0)}")