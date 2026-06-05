import json
import pandas
import pandas as pd
from datasets import Dataset

import pandas as pd
import numpy as np
import asyncio
import json
import logging
import os
from datetime import datetime
from tqdm.asyncio import tqdm
from utils.normalize import convert_dataset_types, convert_type, find_word
from utils.utils import serialize_table_to_tapex_format
from utils.type_check import analyze_dataset_parallel
from WTQ.Qwen.check_result import extract_code_from_response

with open('generated_train/results_20260524_154618.json') as f:
    general = pandas.json_normalize(json.load(f))

with open('corrected_train/results_20260526_140242.json') as f:
    corrected = pandas.json_normalize(json.load(f))

with open('logic_train/results_20260526_120435.json') as f:
    logic = pandas.json_normalize(json.load(f))

with open('sql_to_pandas.json') as f:
    sql_to_pandas = pandas.json_normalize(json.load(f))
#
# general_correct = 0
# for i in general:
#     general_correct += i['is_correct']
#
# corrected_correct = 0
# for i in corrected:
#     corrected_correct += i['is_correct']
#
# logic_correct = 0
# logic_temperature = 0
# for i in logic:
#     logic_correct += i['is_correct']
#
#
# print(general_correct)
# print(corrected_correct)
# print(logic_correct)
#
# print((general_correct + corrected_correct + logic_correct)/len(general))
#




# print(corrected.columns)
# #print(corrected[corrected['is_correct'] == True].groupby('temperature').size())
#
# print(general.is_correct.mean())
# print(corrected.is_correct.mean())
# print(logic.is_correct.mean())
#
# print('mean',(general.is_correct.sum() + corrected.is_correct.sum() + logic.is_correct.sum())/len(general))
#
# correct_df = general[general['is_correct'] == True]
# correct_df = pd.concat([correct_df, corrected[corrected['is_correct'] == True]], ignore_index= True, join = 'inner')
# correct_df = pd.concat([correct_df, logic[logic['is_correct'] == True]], ignore_index= True, join = 'inner')
#
# print('correct_df',len(correct_df))
# print(correct_df)
#
# dataset = Dataset.from_pandas(correct_df)
# dataset.save_to_disk('wtq_train_incorrect_pandas')

pd.set_option('display.max_columns', None)      # все колонки
pd.set_option('display.max_colwidth', None)     # полный текст (без ограничений)
pd.set_option('display.width', None)            # ширина вывода без переносов
pd.set_option('display.max_rows', None)

#2594
df_code = (sql_to_pandas[sql_to_pandas.id == 2594])

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

train = pd.read_csv('../../../datasets/WikiTableQuestions/training.tsv', sep='\t')

# print(df)
# print(df_code)
# result = sorted((run_code_and_get_result(df_code.code.iloc[0],df))[0])
# target = (df_code.targetValue.iloc[0])  # "Peru|Ecuador"
# correct = sorted(target.split('|'))  # ['Ecuador', 'Peru']
#
# print(result,correct)
# print(result==correct)
# a = 0
# count = 0
# print(train.head())
#
# for i in range(len(sql_to_pandas)):
#     row = sql_to_pandas.iloc[i]
#     id = train[train.id == row.nt_id]
#     file_path = id.context
#     tsv_path = file_path.iloc[0].replace('.csv', '.tsv')
#
#     full_path = '/home/master/PycharmProjects/semtab_serializer/datasets/WikiTableQuestions/' + tsv_path
#
#     try:
#         df = pd.read_csv(full_path, sep='\t', on_bad_lines='skip')
#     except Exception as e:
#         print(f"Ошибка чтения файла {full_path}: {e}")
#         continue
#
#     code_string = row['code']
#     result = run_code_and_get_result(code_string, df)
#
#     if result[0] is None:
#         continue
#     else:
#         target = id.targetValue.iloc[0]
#         result_value = result[0]
#
#         if isinstance(result_value, (list, tuple)):
#             # Преобразуем все элементы в строки перед сортировкой
#             result_sorted = sorted([str(x) for x in result_value])
#         else:
#             result_sorted = [str(result_value)]
#
#
#         correct = sorted(target.split('|'))
#         a += 1
#         if not bool(row.is_correct) and result_sorted == correct:
#             # sql_to_pandas.at[i, 'is_correct'] = True
#             count += 1
#
# # with open('sql_to_pandas.json', 'w', encoding='utf-8') as f:
# #     json.dump(sql_to_pandas.to_dict('records'), f, ensure_ascii=False, indent=2)
#
# print('count', count, a)

#true 750
#false 2817

#false 2670
#true 897

# import json
#
# with open('final_merged_results.json') as f1, open('thith.json') as f2:
#     first = json.load(f1)
#     second = {item['nt_id']: item for item in json.load(f2) if 'nt_id' in item}
#
# updated_count = 0
# for item in first:
#     nt_id = item.get('nt_id')
#     if nt_id and nt_id in second:
#         second_item = second[nt_id]
#         if not item.get('is_correct', False) and second_item.get('is_correct', False):
#             # Обновляем только нужные поля
#             item['id'] = second_item.get('id', item['id'])
#             item['table'] = second_item.get('table', item['table'])
#             item['code'] = second_item.get('code', item['code'])
#             item['result'] = second_item.get('result')
#             item['error'] = second_item.get('error')
#             item['is_correct'] = second_item['is_correct']
#             item['target_value'] = second_item.get('targetValue', item.get('target_value'))
#
#             updated_count += 1
#             print(f"Обновлена запись {nt_id}")
#
# with open('fouth.json', 'w') as f:
#     json.dump(first, f, indent=2)
#
# print(f"Обновлено записей: {updated_count}")

# import json
#
# with open('final_data.json', 'r') as f:
#     data = json.load(f)
#
# # Поля для удаления
# fields_to_remove = ['old_pandas_code', 'temperature']
#
# for item in data:
#     for field in fields_to_remove:
#         if field in item:
#             del item[field]
#
# with open('fina_final.json', 'w') as f:
#     json.dump(data, f, indent=2)


# import json
# import pandas as pd
#
# # Загружаем first_updated.json
# with open('final_merged_results.json', 'r') as f:
#     data = json.load(f)
#
# df_first = pd.DataFrame(data)
#
# # Загружаем squall.json
# with open('../../../datasets/wtq_sql/squall.json') as f:
#     sql_data = json.load(f)
#     sql = pd.json_normalize(sql_data)
#
# # Создаём sql_code
# sql['sql_code'] = sql['sql'].apply(lambda tokens: ' '.join([t[1] for t in tokens]))
#
# # Делаем left join по полю nt (из sql) и nt_id (из df_first)
# df_result = df_first.merge(sql[['nt', 'sql_code']], left_on='nt_id', right_on='nt', how='left')
#
# # Удаляем дублирующуюся колонку 'nt' (если не нужна)
# df_result = df_result.drop('nt', axis=1)
#
# # Сохраняем результат
# with open('final_data.json', 'w') as f:
#     json.dump(df_result.to_dict('records'), f, indent=2)
#
# print(f"Результат: {len(df_result)} записей")
# print(f"Колонки: {df_result.columns.tolist()}")


import pandas as pd
import json

# Загружаем train

train = pd.read_csv('../../../datasets/WikiTableQuestions/training.tsv', sep='\t')

# Загружаем first_updated_with_sql.json (результат предыдущего шага)
with open('fina_final.json', 'r') as f:
    data = json.load(f)

df_final = pd.DataFrame(data)

# Делаем left join по полям nt_id (из df_final) и id (из train)
df_final = df_final.merge(train[['id', 'utterance']], left_on='nt_id', right_on='id', how='left')

# Переименовываем utterance в query
df_final = df_final.rename(columns={'utterance': 'query'})
print(df_final.head())
# Удаляем дублирующуюся колонку 'id' из train (если не нужна)
df_final = df_final.drop('id_y', axis=1)

# Сохраняем результат
with open('final_result.json', 'w') as f:
    json.dump(df_final.to_dict('records'), f, indent=2)

print(f"Результат: {len(df_final)} записей")
print(f"Колонки: {df_final.columns.tolist()}")