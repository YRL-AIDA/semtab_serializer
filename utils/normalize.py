import re, pandas as pd, numpy as np
import sys

sys.path.insert(0, '/home/master/PycharmProjects/semtab_serializer')

from utils.type_check import analyze_dataset_parallel, clean_value, extract_number_string


def clean_column_name(col):
    """Заменяет спецсимволы в имени колонки на _"""
    # Заменяем переводы строк, скобки, пробелы, дефисы и другие спецсимволы на _
    col = re.sub(r'[\n\r\t\(\)\[\]\{\}\-\s]+', '_', str(col))
    # Убираем дублирующиеся подчеркивания
    col = re.sub(r'_+', '_', col)
    # Убираем подчеркивание в начале и конце
    col = col.strip('_')
    return col


def convert_value(v, t, keep_original_on_error=True):
    # Если это специальный пропуск - возвращаем pd.NA
    if pd.isna(v) or (isinstance(v, str) and v.lower() in ['nan', 'na', 'n/a', 'nill', 'none', '—', '?']):
        return pd.NA

    # Сохраняем оригинал для возможного возврата
    original = v
    s = clean_value(v)
    if not s:
        return original if keep_original_on_error else pd.NA

    if t == 'bool':
        if re.match(r'^(true|yes|да|истина|1|\+)$', s, re.I):
            return True
        if re.match(r'^(false|no|нет|ложь|0|-|\[ \])$', s, re.I):
            return False
        return original if keep_original_on_error else pd.NA

    if t == 'int':
        c = re.sub(r'[ ,]', '', extract_number_string(s))
        # Проверка на диапазон (содержит тире/минус не в начале)
        if re.search(r'\d+[–-]\d+', c):
            return original if keep_original_on_error else pd.NA
        try:
            if '.' in c:
                result = int(float(c)) if re.match(r'^\d+\.0+$', c) else None
                return result if result is not None else (original if keep_original_on_error else pd.NA)
            return int(c)
        except:
            return original if keep_original_on_error else pd.NA

    if t == 'float':
        c = extract_number_string(s)
        # Проверка на диапазон
        if re.search(r'\d+[–-]\d+', c):
            return original if keep_original_on_error else pd.NA
        if ',' in c and '.' not in c:
            c = c.replace(',', '.')
        c = re.sub(r'[ ,]', '', c)
        try:
            return float(c)
        except:
            return original if keep_original_on_error else pd.NA

    if t in ('date', 'datetime'):
        try:
            r = pd.to_datetime(s, errors='coerce')
            if pd.notna(r):
                return r.normalize() if t == 'date' else r
            return original if keep_original_on_error else pd.NA
        except:
            return original if keep_original_on_error else pd.NA

    if t == 'time':
        for p in [re.compile(x) for x in [r'^\d{1,2}:\d{1,2}$', r'^\d{1,2}:\d{1,2}:\d{1,2}$',
                                          r'^\d{1,2}:\d{1,2}:\d{1,2}\.\d+$', r'^\d{1,2}:\d{1,2}\s*[APap][Mm]$',
                                          r'^\d{1,2}:\d{1,2}:\d{1,2}\s*[APap][Mm]$']]:
            if p.match(s):
                try:
                    dt = pd.to_datetime('1970-01-01 ' + s, errors='coerce')
                    return dt.time() if pd.notna(dt) else (original if keep_original_on_error else pd.NA)
                except:
                    continue
        return original if keep_original_on_error else pd.NA

    return s


def convert_col(s, t):
    # Для колонок с типом float/int применяем специальную логику
    # которая сохранит оригинальные строки для непреобразованных значений
    if t in ('float', 'int', 'bool', 'date', 'datetime', 'time'):
        # Применяем convert_value с сохранением оригинала
        converted = s.apply(lambda x: convert_value(x, t, keep_original_on_error=True))

        # Проверяем, не получилась ли колонка со смешанными типами
        # Если да - оставляем как есть (строками где не преобразовалось)
        return converted

    # Для строк - просто приводим к типу string
    return s.astype('string')


def convert_dataset_types(df, max_workers=None):
    info = analyze_dataset_parallel(df, max_workers)
    res = df.copy()

    # Переименовываем колонки, заменяя спецсимволы на _
    res.columns = [clean_column_name(col) for col in res.columns]

    for col, (t, _) in info.items():
        # Находим новое имя колонки после переименования
        new_col = clean_column_name(col)
        if t != 'None' and new_col in res.columns:
            try:
                res[new_col] = convert_col(df[col], t)
            except:
                pass
    return res