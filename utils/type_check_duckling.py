from type_check import check_type_comprehensive
import requests
import pandas as pd
import json


def parse_text(text, locale='en_US'):
    """Парсит текст через Duckling"""
    text = str(text) if pd.notna(text) else ""

    response = requests.post(
        'http://localhost:8000/parse',
        data={'locale': locale, 'text': text}
    )
    return response.json()


def df_to_duckling(df, lines=5):
    """Получает типы данных через Duckling для первых lines строк"""
    df_types = {}
    for column in df.columns:
        first_values = df[column].head(lines).tolist()
        df_types[column] = []

        for value in first_values:
            if pd.notna(value):
                parsed = parse_text(str(value))
                # Извлекаем только типы из ответа Duckling
                types = [item['dim'] for item in parsed] if parsed else check_type_comprehensive(parsed)[0]
                df_types[column].append(types)
            else:
                df_types[column].append(check_type_comprehensive(value)[0])

    return df_types





# Загрузка данных
df = pd.read_csv('C:/Users/PC/semtab_serializer/tests/Amazon Sale Report.csv')

# df = pd.DataFrame({
#     'Имя': ['Анна', 'Борис', 'Мария','S'],
#     'Возраст': [25, 30, 89516236223, None],
#     'Город': ['Москва', 'Санкт-Петербург', 'Казань', True]
# })

# Анализируем
result = df_to_duckling(df, 5)

# Вывод в JSON формате
print("=== JSON результат ===")
print(json.dumps(result, indent=2, ensure_ascii=False))
for i in result:
    result[i]