import requests
import pandas as pd


def parse_text(text, locale='en_US'):  # исправил локаль на en_US
    # Преобразуем в строку на случай NaN значений
    text = str(text) if pd.notna(text) else ""

    response = requests.post(
        'http://localhost:8000/parse',
        data={'locale': locale, 'text': text}
    )
    return response.json()


def df_to_duckling(df,lines = 5):
    df_types = {}
    for column in df.columns:  # итерируем по колонкам
        # Берем первые 50 значений из каждой колонки
        first_50_values = df[column].head(lines).tolist()
        df_types[column] = []

        # Обрабатываем каждое значение
        for value in first_50_values:
            if pd.notna(value):  # проверяем на NaN
                parsed = parse_text(str(value))
                df_types[column].append(parsed)
            else:
                df_types[column].append([])  # пустой список для NaN

    return df_types


# Загрузка данных
df = pd.read_csv('Amazon Sale Report.csv')

# Анализируем
result = df_to_duckling(df,5)
print(result)