import pandas as pd
import numpy as np
import asyncio
import json
import logging
import os
from datetime import datetime

from tqdm.asyncio import tqdm
from WTQ.Qwen.correct_pandas_code import get_pandas
from utils.normalize import convert_dataset_types, convert_type, find_word
from utils.utils import serialize_table_to_tapex_format
from WTQ.Qwen.check_result import extract_code_from_response
from utils.type_check import analyze_dataset_parallel

# Создаём папку для результатов
RESULTS_DIR = '/home/master/PycharmProjects/semtab_serializer/WTQ/Qwen/train_results/corrected_train'
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

#train = pd.read_csv('../../datasets/WikiTableQuestions/training.tsv', sep='\t')
train = pd.read_csv('/home/master/PycharmProjects/semtab_serializer/datasets/WikiTableQuestions/training.tsv', sep='\t')
#with open('train_results/checkpoint_20260524_154618.json', 'r') as f:
with open('train_results/results_20260602_151233_corrected.json', 'r') as f:
    errors = json.load(f)

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


async def process_single_row(i, train, error_item, temperature, sql_code=None):
    """Обработка одной строки с возможностью использовать корректный SQL-запрос"""
    try:
        # 1. Путь к таблице
        file_path = train.iloc[i].context
        tsv_path = file_path.replace('.csv', '.tsv').replace('/csv/', '/tsv/')
        full_path = '/home/master/PycharmProjects/semtab_serializer/datasets/WikiTableQuestions/' + tsv_path

        # 2. Чтение таблицы
        try:
            table_df = pd.read_csv(full_path, sep='\t')
        except Exception as e:
            logger.error(f"Failed to read {full_path}: {e}")
            return {
                'id': i,
                'table': file_path,
                'code': None,
                'result': None,
                'error': f"CSV/TSV read error: {e}",
                'is_correct': False,
                'target_value': train.iloc[i].targetValue,
                'temperature': temperature,
                'old_pandas_code': error_item.get('code'),
                'nt_id': train.iloc[i]['id'],
                'sql_used': sql_code
            }

        targetValue = train.iloc[i].targetValue
        question = train.iloc[i].utterance
        pandas_code = error_item['code']
        old_error = error_item['error']

        # Добавляем SQL-подсказку в вопрос, если она есть
        if sql_code:
            question = f"{question}\n\n[Correct SQL query for reference: {sql_code}]"

        # 3. Нормализация и анализ таблицы
        try:
            norm_df = convert_dataset_types(table_df)
        except Exception as e:
            logger.error(f"convert_dataset_types failed for row {i}: {e}")
            norm_df = table_df.copy()

        try:
            ser_df = serialize_table_to_tapex_format(norm_df)
        except Exception as e:
            logger.error(f"serialize_table_to_tapex_format failed for row {i}: {e}")
            ser_df = str(norm_df.head())

        try:
            df_types = analyze_dataset_parallel(table_df)
            columns_types = {col: types[0] for col, types in df_types.items()}
        except Exception as e:
            logger.warning(f"analyze_dataset_parallel failed for row {i}: {e}")
            columns_types = {}

        # 4. Получение исправленного кода от модели
        try:
            # Здесь предполагается, что get_pandas принимает параметры:
            # question, table, columns_types, old_code, old_error, temperature
            # Параметр sql_code не передаётся отдельно, т.к. мы добавили его в question
            code = await get_pandas(
                question=question,
                ser_tbl=ser_df,
                tbl_types=columns_types,
                pandas_code=pandas_code,
                error=old_error,
                temperature=temperature,
                sql=sql_code
            )
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
                'temperature': temperature,
                'old_pandas_code': pandas_code,
                'nt_id': train.iloc[i]['id'],
                'sql_used': sql_code
            }

        # 5. Выполнение кода
        result, exec_error = run_code_and_get_result(code, table_df)

        # Приведение результата к строковому формату (как в вашем оригинале)
        if exec_error is None and result is not None:
            if isinstance(result, list):
                result = '|'.join(str(x) for x in result)
            elif isinstance(result, pd.Series):
                if len(result) == 1:
                    result = str(result.iloc[0])
                else:
                    result = '|'.join(str(x) for x in result.tolist())

        # 6. Сравнение с ожидаемым ответом
        try:
            norm_target = safe_convert_type(targetValue)
            is_correct = (result == norm_target) if exec_error is None else False
        except Exception as e:
            logger.warning(f"Comparison failed for row {i}: {e}")
            is_correct = False

        # 7. Возврат результата
        return {
            'id': i,
            'table': file_path,
            'code': code,
            'result': str(result) if result is not None else None,
            'error': exec_error,
            'is_correct': bool(is_correct),
            'target_value': targetValue,
            'temperature': temperature,
            'old_pandas_code': pandas_code,
            'nt_id': train.iloc[i]['id'],
            'sql_used': sql_code
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
            'target_value': train.iloc[i].targetValue if i < len(train) else None,
            'temperature': temperature,
            'old_pandas_code': error_item.get('code') if 'error_item' in locals() else None,
            'nt_id': train.iloc[i]['id'] if i < len(train) else None,
            'sql_used': sql_code
        }


async def main(selected_ids=None, wrong_dict=None):
    if selected_ids is None:
        # Если не переданы, берём все id из errors (у которых есть ошибка)
        ids = [err['id'] for err in errors if err['error'] is not None]
    else:
        ids = selected_ids

    # Загружаем уже обработанные ID из чекпоинта
    existing_results = load_checkpoint(checkpoint_filename)
    processed_ids = {r['id'] for r in existing_results}

    remaining_ids = [i for i in ids if i not in processed_ids]

    logger.info(f"Total: {len(ids)}, Already processed: {len(processed_ids)}, Remaining: {len(remaining_ids)}")
    logger.info(f"Results directory: {os.path.abspath(RESULTS_DIR)}")

    results = existing_results.copy()

    if remaining_ids:
        with tqdm(total=len(remaining_ids), desc="Processing WikiTableQuestions") as pbar:
            for idx, i in enumerate(remaining_ids, 1):
                # Находим элемент errors по id i
                error_item = next((e for e in errors if e['id'] == i), None)
                if not error_item:
                    continue
                temperature = 0.3
                attempt = 1
                # Передаём sql_code в process_single_row через wrong_dict
                sql_code = wrong_dict.get(i) if wrong_dict else None
                result = await process_single_row(i, train, error_item, temperature, sql_code)
                while result['error'] is not None and attempt < 4:
                    temperature += 0.23333
                    temperature = min(temperature, 1)
                    attempt += 1
                    result = await process_single_row(i, train, error_item, temperature, sql_code)
                results.append(result)

                if idx % 100 == 0:
                    save_checkpoint(results, checkpoint_filename)

                pbar.update(1)

    save_checkpoint(results, results_filename)
    total_correct = sum(1 for r in results if r['is_correct'])
    logger.info(f"Final results saved to {results_filename}")
    logger.info(f"Logs saved to {log_filename}")
    return results, total_correct

if __name__ == "__main__":
    # Загружаем SQL запросы из squall.json
    with open('/home/master/PycharmProjects/semtab_serializer/datasets/wtq_sql/squall.json', 'r') as f:
        sql_data = json.load(f)

    # Создаём словарь: nt -> sql_code
    sql_dict = {}
    for item in sql_data:
        nt = item['nt']
        sql_tokens = item['sql']
        sql_code = ' '.join([t[1] for t in sql_tokens])
        sql_dict[nt] = sql_code

    # Обогащаем errors: добавляем nt_id и sql_code
    for err in errors:
        if 'nt_id' not in err:
            err['nt_id'] = train.iloc[err['id']]['id']
        err['sql_code'] = err.get('sql')  # берём SQL из самого JSON

    # Оставляем только записи с ошибкой (error не None) и у которых есть sql_code
    wrong_items = [err for err in errors if err['error'] is not None and err.get('sql_code')]

    # Создаём wrong_ids (список числовых id) и wrong_dict (id -> sql_code)
    wrong_ids = [err['id'] for err in wrong_items]
    wrong_dict = {err['id']: err['sql_code'] for err in wrong_items}

    print(f"Всего ошибок с SQL: {len(wrong_ids)}")

    # Запускаем обработку только этих id
    result, count = asyncio.run(main(selected_ids=wrong_ids, wrong_dict=wrong_dict))

    # Вывод результатов
    print("\n" + "=" * 50)
    for item in result:
        print(f"id: {item['id']}, correct: {item['is_correct']}, error: {item['error']}")

    print(f"\nTotal correct: {count} out of {len(result)}")
    print(f"Accuracy: {count / len(result) * 100:.2f}%")
    print(f"Results saved in folder: {RESULTS_DIR}/")

