import pandas as pd
import numpy as np
import asyncio
import random
from get_pandas_code import get_pandas
from check_result import evaluate_code
from config import system_prompt
from utils.normalize import convert_dataset_types
from check_result import extract_code_from_response


train = pd.read_csv('../../datasets/WikiTableQuestions/training.tsv', sep='\t')

# Выбираем случайные индексы
random.seed(101)
random_indices = random.sample(range(len(train)), 20)


def run_code_and_get_result(code_str, df):
    """Выполняет код на нормализованной таблице и возвращает (результат, ошибка)"""
    df_norm = convert_dataset_types(df)
    clean_code = extract_code_from_response(code_str)
    try:
        result = eval(clean_code, {'df': df_norm, 'pd': pd, 'np': np})
        return result, None
    except Exception as e:
        return None, str(e)


async def main():
    results = []
    syntax_errors = 0
    correct_answers = 0
    total = len(random_indices)

    for i, idx in enumerate(random_indices, 1):
        print(f"\n{'='*80}")
        print(f"--- Test {i}/{total} (train index: {idx}) ---")

        csv = pd.read_csv('../../datasets/WikiTableQuestions/' + train.context.iloc[idx])
        question = train.utterance.iloc[idx]
        targetValue = train.targetValue.iloc[idx]

        # Нормализуем таблицу один раз
        df_norm = convert_dataset_types(csv)

        # Передаём в get_pandas нормализованную таблицу
        code = await get_pandas(question, df_norm)
        print(f"Generated code: {code}")

        # Выполняем код на той же нормализованной таблице
        result_val, error = run_code_and_get_result(code, df_norm)
        print(f"Execution result: {result_val}")
        if error:
            print(f"Execution error: {error}")
            syntax_errors += 1

        print(f"Expected value: {targetValue}")

        # Выводим информацию о таблицах (оригинальной и нормализованной)
        print("\n--- Original table (first 5 rows) ---")
        print(csv.head().to_string())
        print("\n--- Normalized table (first 5 rows) ---")
        print(df_norm.head().to_string())
        print("\n--- Column types (original) ---")
        print(csv.dtypes.to_string())
        print("\n--- Column types (normalized) ---")
        print(df_norm.dtypes.to_string())

        # Проверяем совпадение
        match = evaluate_code(code, df_norm, targetValue)
        print(f"Match: {match}")

        if match:
            correct_answers += 1
        results.append(match)

    # Статистика
    print("\n" + "="*80)
    print("СТАТИСТИКА")
    print("="*80)
    print(f"Всего запусков:                 {total}")
    print(f"С синтаксической ошибкой:       {syntax_errors}")
    print(f"Без синтаксической ошибки:      {total - syntax_errors}")
    print(f"Правильных ответов:             {correct_answers}")
    print(f"Точность (от всех запусков):    {correct_answers / total * 100:.1f}%")
    print(f"Точность (только без ошибок):   {correct_answers / (total - syntax_errors) * 100:.1f}%" if syntax_errors < total else "Точность (только без ошибок): N/A")
    print("="*80)

    return results


if __name__ == "__main__":
    results = asyncio.run(main())
    print(f"\nFinal results: {results}")