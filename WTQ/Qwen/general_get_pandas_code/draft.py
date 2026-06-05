import pandas as pd
import json
# Максимальная ширина колонок
pd.set_option('display.max_colwidth', None)

# Ширина консоли (None = авто)
pd.set_option('display.width', None)

# Отключаем перенос фрейма на несколько строк
pd.set_option('display.expand_frame_repr', False)

# Максимальное количество строк
pd.set_option('display.max_rows', None)  # Показать все строки

# Максимальное количество колонок
pd.set_option('display.max_columns', None)  # Показать все колонки

# Точность отображения float
pd.set_option('display.precision', 4)
with open('../../../datasets/wtq_sql/squall.json') as f:
    general = pd.json_normalize(json.load(f))


general['sql_query'] = general['sql'].apply(lambda tokens: ' '.join([t[1] for t in tokens]))
print(general.head())
print(general.columns)