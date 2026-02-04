
# Добавляем корневую директорию проекта в sys.path
import argparse
from functools import partial
from datasets import load_from_disk
from openai import OpenAI
import sys
import os
import json
import pandas as pd
from io import StringIO
from tqdm import tqdm
import re
import time
import traceback
import shutil
# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.abspath('/media/research/yrl_aida_users/poddubny/poddubnyy/postgraduate/semtab_serializer/tests')))

from utils.utils import load_config

system_prompt = '''You are a Python expert specializing in pandas. Your task is to translate the
given natural language statement into a single-line pandas expression. This
expression must be valid and executable to verify the truth of the statement
using the provided table. Consider the following:
1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Ensure the output is concise, correct, and when run outputs either True or
False, and strictly in the following Json Format with a single key "PANDA":
"PANDA": "<your Pandas code>"
'''
system_correcting_prompt = '''
You are a Python expert specializing in pandas. Your task is to correct a pandas code that translates a given natural language statement into a pandas expression. The input data, code, along with the specific error it contains, is provided.
Your corrected pandas_code must be valid and executable by running the code
snippet str(bool(eval(pandas_code))) ensuring it accurately evaluates the truth
of the statement using the provided table with no errors.
Make sure the pandas_code is of type boolean. Consider the following:
1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Ensure the output is concise, correct, and when run outputs either True or
False, and strictly in the following Json Format with a single key "CORRECT PANDA": 
"CORRECT PANDA": "<your Pandas code>"
'''


def send_message(message,max_tokens=500,top_p=0.9,temperature=0.5,server_url="http://127.0.0.1:8800/v1",api_key="dummy",
                 model_name='deepseek-ai/deepseek-coder-7b-instruct-v1.5',system_prompt='', stop = ["Observation:","\n\n\n\n","\n \n \n"]):
    client = OpenAI(base_url=server_url, api_key=api_key)
    model_input = [
        { 'role': 'system', 'content': system_prompt},
        { 'role': 'user', 'content': message}
    ]
    try:
        print(f"Generating content with model: {model_name}",)
        
        response = client.chat.completions.create(
            model=model_name,
            messages=model_input,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            stop = stop
        )
        
        return True, response.choices[0].message.content

    except Exception as e:
        print("Failed to call LLM: " + str(e))
        time.sleep(6)
        if hasattr(e, 'response'):
            error_info = e.response.json()  
            code_value = error_info['error']['code']
        else:
            code_value = "context_length_exceeded"
        print("Retrying ...")
        return False, None

def parse_panda_code(input_string):
    """
    Парсит строку и извлекает код, который находится внутри конструкции "PANDA": <код>
    Поддерживает различные форматы: JSON, простой текст, Markdown
    
    Args:
        input_string (str): Входная строка для парсинга
        
    Returns:
        str: Извлеченный код или пустая строка, если код не найден
    """
    # Сначала попробуем найти JSON объект с PANDA
    json_pattern = r'\{[^{}]*(?:CORRECT PANDA|PANDA)":\s*(.+?)(?:\n|$)?\}'
    json_match = re.search(json_pattern, input_string, re.DOTALL)
    code = None
    pattern = r'"(?:CORRECT PANDA|PANDA)":\s*(.+?)(?:\n|$)'
    if json_match:
        code = json_match.group(1).strip()
    else:
        match = re.search(pattern, input_string, re.DOTALL)
        if match:
        # Извлекаем код и убираем лишние пробелы по краям
            code = match.group(1).strip()
        # Заменяем одинарные кавычки внутри строки для корректного парсинга JSON

    # Если JSON не найден или не распарсился, используем старый метод
    # Паттерн для поиска кода после "PANDA": 
    # Ищет "PANDA": за которым следует пробел, затем код до конца строки или до следующего символа
    if code != None:
    # Убираем возможные кавычки вокруг кода
        if code.startswith('"') and code.endswith('"'):
            code = code[1:-1]
        elif code.startswith("'") and code.endswith("'"):
            code = code[1:-1]
            
        return code
    
    return ""

def correct_code(df, input_data, code_str, error_str, iter_id=0, system_correcting_prompt='', 
                 stop=["Observation:", "\n\n\n\n", "\n \n \n"],
                 max_tokens=500, top_p=0.9, temperature=0.5, 
                 server_url="http://127.0.0.1:8800/v1", api_key="dummy",
                 model_name='deepseek-ai/deepseek-coder-7b-instruct-v1.5', max_iter=5,add_df_info=False):
    """
    Рекурсивно исправляет код с использованием LLM
    """
    if iter_id < max_iter:
        if add_df_info :
            df_info = df.dtypes.to_string()
            df_info = f'DATAFRAME TABLE TYPES: {df_info}\n'
        else:
            df_info = ''
        # Формируем запрос на исправление
        success, response = send_message(
            f'INPUT DATA: {input_data}\n{df_info} CODE: {code_str}\nERROR: {error_str}',
            stop=stop,
            max_tokens=max_tokens,
            top_p=top_p,
            temperature=temperature,
            server_url=server_url,
            api_key=api_key,
            model_name=model_name,
            system_prompt=system_correcting_prompt
        )
        
        if success:
            code = parse_panda_code(response)
            if not code:  # Если не удалось извлечь код
                print(f"Iteration {iter_id}: Failed to parse code from response",f"text: {response}")
                return correct_code(df, input_data, code_str, error_str, iter_id=iter_id+1,
                                   max_tokens=max_tokens, top_p=top_p, temperature=temperature,
                                   server_url=server_url, api_key=api_key, model_name=model_name,
                                   system_correcting_prompt=system_correcting_prompt, max_iter=max_iter,add_df_info=add_df_info)
            
            try:
                # Пробуем выполнить исправленный код
                pandas_eval = str(bool(eval(code)))
                print(f"Iteration {iter_id}: Code corrected successfully")
                return True, code, response
            except Exception as e:
                print(f"Iteration {iter_id}: Code execution failed with error: {e}")
                # Рекурсивно пытаемся исправить новый код
                return correct_code(df, input_data, code, f'{type(e).__name__}: {e}', iter_id=iter_id+1,
                                   max_tokens=max_tokens, top_p=top_p, temperature=temperature,
                                   server_url=server_url, api_key=api_key, model_name=model_name,
                                   system_correcting_prompt=system_correcting_prompt, max_iter=max_iter,add_df_info=add_df_info)
        else:
            print(f'Iteration {iter_id}: LLM call failed')
            # Если не удалось вызвать LLM, пробуем снова с теми же данными
            return correct_code(df, input_data, code_str, error_str, iter_id=iter_id+1,
                               max_tokens=max_tokens, top_p=top_p, temperature=temperature,
                               server_url=server_url, api_key=api_key, model_name=model_name,
                               system_correcting_prompt=system_correcting_prompt, max_iter=max_iter,add_df_info=add_df_info)
    else:
        print(f'Max iterations ({max_iter}) reached without successful correction')
        return False, code_str, None


def dataset_processing(entry,serialized_table_field=None,system_correcting_prompt='',system_prompt='',add_df_info=False,**kwargs):
    df = pd.read_csv(StringIO(entry['table_text']), delimiter='#')
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except ValueError:
            # Если возникает ошибка, оставляем столбец как есть
            continue
    #query = entry[f"{query_field}_query"]
    #query = entry['statement']+ ' ' + serialize_table(df,serialization_type='space')     
    query = entry['statement']+ ' ' + entry[serialized_table_field]
    success,response = send_message(query, system_prompt=system_prompt)

    entry[f'{serialized_table_field}_answ'] = 'None'
    entry[f'{serialized_table_field}_label'] = 'None'
    entry[f'{serialized_table_field}_answ_correct'] = 'None'

    
    if success:
        entry[f'{serialized_table_field}_answ'] = response
        try:
            code = parse_panda_code(response)
            try:
            
                pandas_eval = str(bool(eval(code)))
                entry[f'{serialized_table_field}_label'] = str(pandas_eval)
            except Exception as e:
                print('EEEERRRRRRR', entry['id'])
                print(e)
                success_correcting, new_code, correcting_response = correct_code(df,query,code,f'{type(e).__name__}: {e}',
                                             system_correcting_prompt=system_correcting_prompt,add_df_info=add_df_info)
                if success_correcting:
                    pandas_eval = str(bool(eval(new_code)))
                    entry[f'{serialized_table_field}_label'] = str(pandas_eval)
                    entry[f'{serialized_table_field}_answ_correct'] = correcting_response
        except Exception as e:
            print (e)
        
    return entry




def main():
    parser = argparse.ArgumentParser(description='Скрипт с настраиваемыми параметрами')
    parser.add_argument('--inputdata', type=str, default='./answer_gen_nlsep_none_filtered_new_dataset')
    parser.add_argument('--outputdata', type=str, default='./answer_gen_nlsep_none_filtered_new_dataset_failran_clear')
    parser.add_argument('--num_proc', type=int, default=16, 
                      help='Количество процессов')
    parser.add_argument('--conf-file', type=str, default='./convert_conf.yaml')
    #parser.add_argument('--add-df-info', type=int, default=0, help='Добавлять ли доп инфо о типах и столбцах df')
    
    args = parser.parse_args()
    
    inputdata = args.inputdata
    outputdata = args.outputdata
    NUM_PROC = args.num_proc
    #add_df_info = bool(args.add_df_info)
    conf_file = args.conf_file
    print(f"inputdata: {inputdata}")
    print(f"outputdata: {outputdata}")
    print(f"num-proc: {NUM_PROC}")
    print(f"conf-file: {conf_file}")
    # Ваш основной код здесь
    #dataset = dataset.filter(lambda x: True if x[f'{query_field}_answ']!='None' else False,num_proc=17)
    #dataset = dataset.filter(lambda x: True if x[f'{query_field}_label']=='None' else False,num_proc=17)
    config_data = load_config(conf_file)
    dataset = load_from_disk(inputdata)
    
    for config_name in config_data.keys():
        config = config_data[config_name]
        print(config_name)
        print(config) 
        dataset2 = dataset.map(partial(dataset_processing,serialized_table_field=config_name,
                                  system_correcting_prompt=system_correcting_prompt,
                        system_prompt=system_prompt,**config),num_proc=NUM_PROC)
        #if inputdata == outputdata:
         #   shutil.rmtree(inputdata)
        dataset2.save_to_disk(outputdata)
        dataset = dataset2

if __name__ == "__main__":
    #python abation_experiments.py --inputdata tab_fact_test_semtab__html_ablation --outputdata tab_fact_test_semtab_html_ablation_correcring_first --conf-file semtab_html_config_answer.yaml --num_proc 16 > tab_fact_test_html_ablation_log2.txt
    main()          