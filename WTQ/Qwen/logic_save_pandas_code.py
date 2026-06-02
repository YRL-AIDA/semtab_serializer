import pandas as pd
import numpy as np
import asyncio
import json
import logging
import os
from datetime import datetime
from tqdm.asyncio import tqdm
from logic_pandas_code import get_pandas
from check_result import evaluate_code
from config import generate_prompt
from utils.normalize import convert_dataset_types, convert_type, find_word
from utils.utils import serialize_table_to_tapex_format
from check_result import extract_code_from_response
from utils.type_check import analyze_dataset_parallel

# Создаём папку для результатов
RESULTS_DIR = 'train_results/logic_train'
os.makedirs(RESULTS_DIR, exist_ok=True)


# Настройка логирования в файл
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(RESULTS_DIR, f'execution_log_{timestamp}.log')
results_filename = os.path.join(RESULTS_DIR, f'results_{timestamp}.json')
checkpoint_filename = os.path.join(RESULTS_DIR, f'checkpoint_{timestamp}.json')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

train = pd.read_csv('../../datasets/WikiTableQuestions/training.tsv', sep='\t')
with open('train_results/generated_train/checkpoint_20260524_154618.json', 'r') as f:
    first_train_false_results = json.load(f)
with open('train_results/corrected_train/results_20260526_140242.json', 'r') as f:
    second_train_false_results = json.load(f)

def save_checkpoint(results, filename):
    """Сохраняет промежуточные результаты"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Checkpoint saved: {len(results)} results")


def load_checkpoint(filename):
    """Загружает сохранённые результаты"""
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def safe_convert_type(value):
    """Безопасная обёртка для convert_type"""
    try:
        return convert_type(value)
    except Exception as e:
        logger.warning(f"convert_type failed for '{value}': {e}")
        return value


def run_code_and_get_result(code_str, df):
    """Выполняет код на нормализованной таблице и возвращает (результат, ошибка)"""
    try:
        df_norm = convert_dataset_types(df)
    except Exception as e:
        return None, f"convert_dataset_types error: {e}"

    clean_code = extract_code_from_response(code_str)
    if clean_code is None:
        return None, "extract_code_from_response returned None"

    try:
        result = eval(clean_code, {
            'df': df_norm,
            'pd': pd,
            'np': np,
            'convert_type': convert_type,
            'find_word': find_word
        })
        return result, None
    except Exception as e:
        return None, str(e)


async def process_single_row(i, train,errors,temperature):
    """Обработка одной строки"""
    try:
        # 1. Чтение TSV с обработкой ошибок
        file_path = train.iloc[i].context
        tsv_path = file_path.replace('.csv', '.tsv').replace('/csv/', '/tsv/')
        full_path = '../../datasets/WikiTableQuestions/' + tsv_path



        pandas_code = errors['code']
        error = errors['error']

        try:
            df = pd.read_csv(full_path, sep='\t')
        except Exception as e:
            logger.error(f"Failed to read {full_path}: {e}")
            return {
                'id': i,
                'table': file_path,
                'code': None,
                'result': None,
                'error': f"CSV/TSV read error: {e}",
                'is_correct': False,
                'target_value': train.iloc[i].targetValue
            }

        targetValue = train.iloc[i].targetValue
        question = train.iloc[i].utterance

        # 2. Нормализация и анализ типов с защитой
        try:
            norm_df = convert_dataset_types(df)
        except Exception as e:
            logger.error(f"convert_dataset_types failed for row {i}: {e}")
            norm_df = df.copy()

        try:
            ser_df = serialize_table_to_tapex_format(norm_df)
        except Exception as e:
            logger.error(f"serialize_table_to_tapex_format failed for row {i}: {e}")
            ser_df = str(norm_df.head())

        try:
            df_types = analyze_dataset_parallel(df)
            columns_types = {col: types[0] for col, types in df_types.items()}
        except Exception as e:
            logger.warning(f"analyze_dataset_parallel failed for row {i}: {e}")
            columns_types = {}

        # 3. Получение кода от модели
        try:
            code = await get_pandas(question, ser_df, columns_types, pandas_code, error, temperature = temperature)
        except Exception as e:
            logger.error(f"get_pandas failed for row {i}: {e}")
            return {
                'id': i,
                'table': file_path,
                'code': None,
                'result': None,
                'error': f"get_pandas error: {e}",
                'is_correct': False,
                'target_value': targetValue,
                'temperature': temperature
            }

        # 4. Выполнение кода
        result, error = run_code_and_get_result(code, df)

        # 5. Сравнение с ожидаемым значением
        try:
            norm_target = safe_convert_type(targetValue)
            is_correct = (result == norm_target) if error is None else False
        except Exception as e:
            logger.warning(f"Comparison failed for row {i}: {e}")
            is_correct = False

        return {
            'id': i,
            'table': file_path,
            'code': code,
            'result': str(result) if result is not None else None,
            'error': error,
            'is_correct': bool(is_correct),
            'target_value': targetValue
        }

    except Exception as e:
        logger.exception(f"Unexpected error in process_single_row for id {i}: {e}")
        return {
            'id': i,
            'table': train.iloc[i].context if i < len(train) else 'unknown',
            'code': None,
            'result': None,
            'error': f"Unexpected: {e}",
            'is_correct': False,
            'target_value': train.iloc[i].targetValue if i < len(train) else None
        }


async def main():
    # 1. Строим словарь некорректных результатов (id -> данные)
    all_train_false_results = {}
    ids = []

    for item in first_train_false_results:
        if item.get('error') is None and not item.get('is_correct', False):
            all_train_false_results[item['id']] = item
            ids.append(item['id'])

    for item in second_train_false_results:
        if item.get('error') is None and not item.get('is_correct', False):
            all_train_false_results[item['id']] = item
            ids.append(item['id'])

    print(f"Найдено некорректных строк: {len(all_train_false_results)}")
    # 2. Загружаем уже обработанные ID из чекпоинта
    existing_results = load_checkpoint(checkpoint_filename)
    processed_ids = {r['id'] for r in existing_results}

    remaining_ids = [i for i in ids if i not in processed_ids]

    logger.info(f"Всего: {len(ids)}, Уже обработано: {len(processed_ids)}, Осталось: {len(remaining_ids)}")

    results = existing_results.copy()

    # 3. Обработка оставшихся строк
    if remaining_ids:
        with tqdm(total=len(remaining_ids), desc="Повторная обработка") as pbar:
            for idx, row_id in enumerate(remaining_ids, 1):
                # Инициализация для каждой строки
                temperature = 0.3
                attempt = 1
                error_info = all_train_false_results[row_id]

                result = await process_single_row(row_id, train, error_info, temperature)

                # Пытаемся улучшить результат, увеличивая температуру (если ответ неверный)
                while not result['is_correct'] and attempt < 4:
                    temperature += 0.23333
                    temperature = min(temperature, 1.0)
                    attempt += 1
                    result = await process_single_row(row_id, train, error_info, temperature)

                results.append(result)

                if idx % 100 == 0:
                    save_checkpoint(results, checkpoint_filename)

                pbar.update(1)

    # 4. Сохраняем финальные результаты
    save_checkpoint(results, results_filename)
    total_correct = sum(1 for r in results if r['is_correct'])

    logger.info(f"Итоговый файл: {results_filename}")
    logger.info(f"Лог: {log_filename}")

    return results, total_correct


if __name__ == "__main__":
    print(len(second_train_false_results))

    result, count = asyncio.run(main())

    # Вывод результатов в консоль
    print("\n" + "=" * 50)
    for item in result:
        print(f"id: {item['id']}, correct: {item['is_correct']}, error: {item['error']}")

    print(f"\nTotal correct: {count} out of {len(result)}")
    print(f"Accuracy: {count / len(result) * 100:.2f}%")
    print(f"Results saved in folder: {RESULTS_DIR}/")

