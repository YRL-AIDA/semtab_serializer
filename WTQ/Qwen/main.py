import pandas as pd
import numpy as np
import asyncio
import json
from get_pandas_code import get_pandas
from check_result import evaluate_code
from config import system_prompt
from utils.normalize import convert_dataset_types,convert_type
from utils.utils import serialize_table_to_tapex_format
from check_result import extract_code_from_response
from utils.type_check import analyze_dataset_parallel

train = pd.read_csv('../../datasets/WikiTableQuestions/training.tsv', sep='\t')


def run_code_and_get_result(code_str, df):
    """Выполняет код на нормализованной таблице и возвращает (результат, ошибка)"""
    df_norm = convert_dataset_types(df)
    clean_code = extract_code_from_response(code_str)
    try:
        result = eval(clean_code, {'df': df_norm, 'pd': pd, 'np': np,'convert_type':convert_type})
        return result, None
    except Exception as e:
        return None, str(e)


async def main():
    json_result = []

    for i in range(30,40):
        df = pd.read_csv('../../datasets/WikiTableQuestions/' + train.iloc[i].context)
        targetValue = train.iloc[i].targetValue
        question = train.iloc[i].utterance

        norm_df = convert_dataset_types(df)
        ser_df = serialize_table_to_tapex_format(norm_df)
        df_types = analyze_dataset_parallel(df)
        columns_types = {col: types[0] for col, types in df_types.items()}

        code = await get_pandas(question, ser_df, columns_types)
        result = run_code_and_get_result(code, df)
        error = None
        if result[0] is not None:
            result = result[0]
        else:
            error = result[1]
            result = None

        json_result.append({
            'id': i,
            'table': train.iloc[i].context,
            'code': code,
            'result': result,
            'error': error
        })

        # print(system_prompt)
        # print('table ',ser_df)
        # # print('coll types ',columns_types)
        # print('question ',question)
        # print(run_code_and_get_result(input(),df))
        # print(targetValue)
        # print()
        # print()
        # print()
        # print()
        # print()
        # print()
    return json_result


if __name__ == "__main__":
    result = asyncio.run(main())  # ← сохраняем результат
    for i in result:
        print(i)
