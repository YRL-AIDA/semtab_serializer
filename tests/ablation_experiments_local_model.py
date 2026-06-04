
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
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import PeftModel
# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.abspath('/media/research/yrl_aida_users/poddubny/poddubnyy/postgraduate/semtab_serializer/tests')))

from utils.utils import load_config


system_prompt = '''
### Instruction:
You are a Python expert specializing in pandas. Your task is to translate the
given natural language query into a single-line pandas expression. This
expression must be valid and executable to verify the truth of the statement
using the provided table. Consider the following:
1. The table schema is represented in XML format.
2. The table is represented as a pandas DataFrame named df.
3. Do not include explanations, comments, or multiline outputs.
4. Ensure the output is concise, correct, and when run outputs either True or
False, and strictly in the following Json Format with a single key "PANDA":
"PANDA": "<your Pandas code>"

### Table schema
{table}
### Query
{query}
### Response:\n
'''

system_correcting_prompt = '''
You are a Python expert specializing in pandas. Your task is to correct a pandas code that translates a given natural language query into a pandas expression. The input table schema,query, code, along with the specific error it contains, is provided.
Your corrected pandas_code must be valid and executable by running the code
snippet str(bool(eval(pandas_code))) ensuring it accurately evaluates the truth
of the statement using the provided table with no errors.
Make sure the pandas_code is of type boolean. Consider the following:
1. The table schema is represented in XML format.
2. The table is represented as a pandas DataFrame named df.
3. Do not include explanations, comments, or multiline outputs.
4. Ensure the output is concise, correct, and when run outputs either True or
False, and strictly in the following Json Format with a single key "CORRECT PANDA": 
"CORRECT PANDA": "<your Pandas code>"

### Table schema
{table}
### Query
{query}
### CODE
{code_str}
### ERROR
{error_str}
### Response:\n
'''


def send_message(message,max_tokens=500,top_p=0.9,temperature=0.5,server_url="http://127.0.0.1:9092/v1",api_key="dummy",
                 model_name='deepseek-ai/deepseek-coder-7b-instruct-v1.5', stop = ["Observation:","\n\n\n\n","\n \n \n"]):
    client = OpenAI(base_url=server_url, api_key=api_key)
    model_input = [
        #{ 'role': 'system', 'content': system_prompt},
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
        print(message)
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

def correct_code(df, table,query, code_str, error_str, iter_id=0, system_correcting_prompt='', 
                 stop=["Observation:", "\n\n\n\n", "\n \n \n"],
                 max_tokens=500, top_p=0.9, temperature=0.5, 
                 server_url="http://127.0.0.1:9092/v1", api_key="dummy",
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
        prompt = system_correcting_prompt.format(table=table, query=query,code_str=code_str,error_str=error_str).lstrip() 
        success, response = send_message(
            prompt,
            stop=stop,
            max_tokens=max_tokens,
            top_p=top_p,
            temperature=temperature,
            server_url=server_url,
            api_key=api_key,
            model_name=model_name)
        
        if success:
            code = parse_panda_code(response)
            if not code:  # Если не удалось извлечь код
                print(f"Iteration {iter_id}: Failed to parse code from response",f"text: {response}")
                return correct_code(df, table,query, code_str, error_str, iter_id=iter_id+1,
                                   max_tokens=max_tokens, top_p=top_p, temperature=temperature,
                                   server_url=server_url, api_key=api_key, model_name=model_name,
                                   system_correcting_prompt=system_correcting_prompt, max_iter=max_iter,add_df_info=add_df_info)
            
            try:
                # Пробуем выполнить исправленный код
                pandas_eval = str(bool(eval(code)))
                print(f"Iteration {iter_id}: Code corrected successfully")
                return True, code, response,None
            except Exception as e:
                print(f"Iteration {iter_id}: Code execution failed with error: {e}")
                # Рекурсивно пытаемся исправить новый код
                return correct_code(df, table,query, code, f'{type(e).__name__}: {e}', iter_id=iter_id+1,
                                   max_tokens=max_tokens, top_p=top_p, temperature=temperature,
                                   server_url=server_url, api_key=api_key, model_name=model_name,
                                   system_correcting_prompt=system_correcting_prompt, max_iter=max_iter,add_df_info=add_df_info)
        else:
            print(f'Iteration {iter_id}: LLM call failed')
            # Если не удалось вызвать LLM, пробуем снова с теми же данными
            return correct_code(df, table,query, code_str, error_str, iter_id=iter_id+1,
                               max_tokens=max_tokens, top_p=top_p, temperature=temperature,
                               server_url=server_url, api_key=api_key, model_name=model_name,
                               system_correcting_prompt=system_correcting_prompt, max_iter=max_iter,add_df_info=add_df_info)
    else:
        print(f'Max iterations ({max_iter}) reached without successful correction')
        return False, code_str, None,error_str


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
    query = system_prompt.format(table=entry[serialized_table_field], query=entry['statement']).lstrip() 
    success,response = send_message(query,model_name=kwargs.get('model_name','deepseek-ai/deepseek-coder-7b-instruct-v1.5'),
                                    server_url=kwargs.get('server_url',"http://127.0.0.1:9092/v1"),max_tokens = None)
                                    

    entry[f'{serialized_table_field}_answ'] = 'None'
    entry[f'{serialized_table_field}_label'] = 'None'
    entry[f'{serialized_table_field}_answ_correct'] = 'None'
    entry[f'{serialized_table_field}_last_err'] = 'None'

    
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
                success_correcting, new_code, correcting_response,last_err = correct_code(df,entry[serialized_table_field],
                                                                                 entry['statement'],
                                                                                 code,f'{type(e).__name__}: {e}',
                                             system_correcting_prompt=system_correcting_prompt,add_df_info=add_df_info,
                                            model_name=kwargs.get('model_name','deepseek-ai/deepseek-coder-7b-instruct-v1.5'),
                                    server_url=kwargs.get('server_url',"http://127.0.0.1:9092/v1"))
                if success_correcting:
                    pandas_eval = str(bool(eval(new_code)))
                    entry[f'{serialized_table_field}_label'] = str(pandas_eval)
                    entry[f'{serialized_table_field}_answ_correct'] = correcting_response
                else:
                    entry[f'{serialized_table_field}_last_err'] = str(last_err)
                
                
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

    config_data = load_config(conf_file)
    dataset = load_from_disk(inputdata)
    previous_checkpoint_dir = None

    for config_name in config_data.keys():
        config = config_data[config_name]
        print(config_name)
        print(config) 
        dataset = dataset.map(partial(dataset_processing,serialized_table_field=config_name,
                                  system_correcting_prompt=system_correcting_prompt,
                        system_prompt=system_prompt,**config),num_proc=NUM_PROC)
        #if inputdata == outputdata:
         #   shutil.rmtree(inputdata)
        current_checkpoint_dir = f"{outputdata}_temp_ckpt_{config_name}"
        dataset.save_to_disk(current_checkpoint_dir)
        if previous_checkpoint_dir and os.path.exists(previous_checkpoint_dir):
            print(f"Removing previous checkpoint: {previous_checkpoint_dir}")
            shutil.rmtree(previous_checkpoint_dir)
            
        # 4. Обновляем ссылку на предыдущий чекпоинт для следующей итерации
        previous_checkpoint_dir = current_checkpoint_dir
    if previous_checkpoint_dir and os.path.exists(previous_checkpoint_dir):
        # Если целевая папка уже существует, удаляем ее во избежание конфликта
        if os.path.exists(outputdata):
            shutil.rmtree(outputdata)
            
        os.rename(previous_checkpoint_dir, outputdata)
        print(f"\nSuccess! Final dataset saved to: {outputdata}")

if __name__ == "__main__":
    #python ablation_experiments_local_model.py --inputdata tab_fact_test_semtab_xml_ablation --outputdata tab_fact_test_semtab_xml_ablation_lora --conf-file semtab_xml_config_answer_lora.yaml --num_proc 16 &> tab_fact_test_xml_ablation_lora_log.txt
    main()     

#    features: ['id', 'table_csv', 'table_text', 'label', 'statement', 'table_caption', 'semtab_xml_attributes_semantic_datatype_exampples_description_top1_tresh50', 'semtab_xml_attributes_datatype_exampples_description_top1_tresh50', 'semtab_xml_attributes_exampples_description_top1_tresh50', 'semtab_xml_attributes_description_top1_tresh50', 'semtab_xml_attributes_top1_tresh50', 'semtab_xml_elements_semantic_datatype_exampples_description_top1_tresh50', 'semtab_xml_elements_datatype_exampples_description_top1_tresh50', 'semtab_xml_elements_exampples_description_top1_tresh50', 'semtab_xml_elements_description_top1_tresh50', 'semtab_xml_elements_top1_tresh50'],