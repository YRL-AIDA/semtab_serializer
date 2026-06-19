import re
import io
import numpy as np
import pandas as pd

def normalize_table(file_path: str) -> pd.DataFrame:
    """
    Выполняет полный цикл отказоустойчивого чтения, глобальной очистки 
    и нормализации признаков для одной "дикой" веб-таблицы.
    """
    # =====================================================================
    # ШАГ 1: Отказоустойчивое чтение файла (Защита от ошибок токенизации)
    # =====================================================================
    try:
        df = pd.read_csv(file_path)
    except Exception:
        try:
            # Если упало (разное количество колонок в строках), чистим сырой текст
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            if not lines:
                return pd.DataFrame()
            
            header_cnt = len(lines[0].split(','))
            clean_lines = []
            for line in lines:
                parts = line.split(',')
                if len(parts) > header_cnt:
                    # Склеиваем избыточные элементы (хвост строки) в одну ячейку
                    parts = parts[:header_cnt-1] + [" ".join(parts[header_cnt-1:])]
                clean_lines.append(",".join(parts))
            df = pd.read_csv(io.StringIO("".join(clean_lines)))
        except Exception:
            # Если файл абсолютно поврежден, возвращаем пустой DF, чтобы не рушить цикл
            return pd.DataFrame()

    if df.empty:
        return df

    # =====================================================================
    # ШАГ 2: Глобальная очистка структуры, пробелов и псевдо-NaN
    # =====================================================================
    # 1. Вычищаем системные переносы строк (\n) из названий колонок
    df.columns = df.columns.str.replace(r'\n', ' ', regex=True).str.strip()
    
    # 2. Приводим всё к строкам для безопасного regex-анализа, сохраняя оригинальные NaN
    df = df.astype(str).replace(r'^nan$', np.nan, regex=True)
    
    # 3. Заменяем текстовые маркеры пропусков на честный np.nan
    garbage_nas = [r'^——$', r'^none$', r'^NaN$', r'^—$', r'^\s*$']
    for pattern in garbage_nas:
        df = df.replace(pattern, np.nan, regex=True)

    # 4. Убираем \n внутри самих ячеек и срезаем пробелы по краям
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace(r'\n', ' ', regex=True).str.strip()

    # =====================================================================
    # ШАГ 3: Умная нормализация и деструктуризация колонок
    # =====================================================================
    original_columns = list(df.columns)
    
    for col in original_columns:
        col_filled = df[col].dropna()
        if col_filled.empty:
            continue
            
        total_filled = len(col_filled)

        # --- Эвристика А: Текст со скобками -> "Tommy Persson (SWE)" или "Raymond Felton (17)" ---
        has_parentheses = col_filled.str.contains(r'.+?\s*\(.+?\)', regex=True)
        if has_parentheses.sum() > total_filled * 0.5:
            extracted = df[col].str.extract(r'(.+?)\s*\((.+?)\)')
            df[f"{col}_base"] = extracted[0].str.strip()
            df[f"{col}_meta"] = extracted[1].str.strip()
            continue

        # --- Эвристика Б: Счета матчей и результаты -> "W 28–21", "17-5", "2–4 (aet)" ---
        has_scores = col_filled.str.contains(r'\d+[-–:–]\d+', regex=True)
        if has_scores.sum() > total_filled * 0.5:
            extracted = df[col].str.extract(r'(?:[A-Za-z]\s+)?(\d+)[\–\-:–](\d+)')
            df[f"{col}_score1"] = pd.to_numeric(extracted[0], errors='coerce')
            df[f"{col}_score2"] = pd.to_numeric(extracted[1], errors='coerce')
            continue

        # --- Эвристика В: Имперские длины -> 47'9" ---
        has_imperial = col_filled.str.contains(r'\d+\'\d+"', regex=True)
        if has_imperial.sum() > total_filled * 0.3:
            def _to_meters(val):
                if pd.isna(val): return np.nan
                match = re.match(r'(\d+)\'(\d+)"', str(val))
                if match:
                    feet, inches = map(int, match.groups())
                    return round((feet * 0.3048) + (inches * 0.0254), 2)
                return np.nan
            df[f"{col}_meters"] = df[col].apply(_to_meters)
            continue

        # --- Эвристика Г: Авто-приведение типов к Numeric (валюты, разделители тысяч) ---
        # Удаляем знаки доллара и запятые (например, "18,108" -> "18108")
        clean_num_attempt = df[col].str.replace(r'[\$,]', '', regex=True)
        numeric_converted = pd.to_numeric(clean_num_attempt, errors='coerce')
        if numeric_converted.notna().sum() > total_filled * 0.8:
            df[col] = numeric_converted
            continue

        # --- Эвристика Д: Даты ("June 15", "11/09/2013*", "September 13, 1987") ---
        if 'date' in col.lower() or 'year' in col.lower():
            # Очищаем маркеры сносок вроде астерисков в конце даты ("2013*" -> "2013")
            clean_date_attempt = df[col].str.replace(r'\*$', '', regex=True)
            date_converted = pd.to_datetime(clean_date_attempt, errors='coerce')
            if date_converted.notna().sum() > total_filled * 0.5:
                df[f"{col}_datetime"] = date_converted

    return df