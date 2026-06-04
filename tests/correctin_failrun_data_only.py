
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
# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.abspath('/media/research/yrl_aida_users/poddubny/poddubnyy/postgraduate/semtab_serializer/tests')))

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
You are a Python expert specializing in pandas. Your task is to correct a pandas code that translates 
a given natural language statement into a pandas
expression. The input data, code, along with the specific error it contains, is provided.
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
    json_pattern = r'\{[^{}]*PANDA":\s*(.+?)(?:\n|$)?\}'
    json_match = re.search(json_pattern, input_string, re.DOTALL)
    code = None
    pattern = r'"PANDA":\s*(.+?)(?:\n|$)'
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

def correct_code(df,input_data,code_str,error_str,iter_id=0,system_correcting_prompt='',stop= ["Observation:","\n\n\n\n","\n \n \n"],
                max_tokens=500,top_p=0.9,temperature=0.5,server_url="http://127.0.0.1:8800/v1",api_key="dummy",
                model_name='deepseek-ai/deepseek-coder-7b-instruct-v1.5',max_iter=5):
    if iter_id<max_iter:
        success,response = send_message(f'INPUT DATA: {input_data}\n CODE: {code_str}\n ERROR: {error_str}',stop= stop,
                    max_tokens=max_tokens,top_p=top_p,temperature=temperature,server_url=server_url,api_key=api_key,
                    model_name=model_name, system_prompt=system_correcting_prompt)
        if success:
            correct_code = parse_panda_code(response)
            try:
                pandas_eval = str(bool(eval(correct_code)))
                return True,correct_code, response
            except Exception as e:
                print(e)
                return correct_code(df,correct_code,str(e),iter_id=iter_id+1,max_tokens=max_tokens,top_p=top_p,
                             temperature=temperature, server_url=server_url,api_key=api_key,model_name=model_name, 
                             system_correcting_prompt=system_correcting_prompt,max_iter=max_iter)
                
        else:
            print('success correcting error')
            return correct_code(df,correct_code,str(e),iter_id=iter_id+1,max_tokens=max_tokens,top_p=top_p,
                             temperature=temperature, server_url=server_url,api_key=api_key,model_name=model_name, 
                             system_correcting_prompt=system_correcting_prompt,max_iter=max_iter)
    else:
        print(f'end of iter {iter_id}, max iter is {max_iter}')
        return False, code_str, None

system_correcting_prompt

def dataset_processing(entry,query_field=None,system_correcting_prompt='',system_prompt=''):
    df = pd.read_csv(StringIO(entry['table_text']), delimiter='#')
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except ValueError:
            # Если возникает ошибка, оставляем столбец как есть
            continue
    
    entry[f'{query_field}_answ_correct'] = 'None'
    entry[f'{query_field}_label_correct'] = 'None'
    try:
        code = parse_panda_code(entry[f'{query_field}_answ'])
        try:
            pandas_eval = str(bool(eval(code)))
        except Exception as e:
            print('EEEERRRRRRR', entry['id'])
            print(e)
            success_correcting, new_code, correcting_response = correct_code(df,entry[f"{query_field}_query"],
                                         code,str(e),
                                         system_correcting_prompt=system_correcting_prompt)
            if success_correcting:
                pandas_eval = str(bool(eval(new_code)))
                entry[f'{query_field}_label_correct'] = pandas_eval
                entry[f'{query_field}_answ_correct'] = correcting_response
    except Exception as e:
        print (e)
        
    return entry




def main():
    parser = argparse.ArgumentParser(description='Скрипт с настраиваемыми параметрами')
    parser.add_argument('--inputdata', type=str, default='./answer_gen_nlsep_none_filtered_new_dataset')
    parser.add_argument('--outputdata', type=str, default='./answer_gen_nlsep_none_filtered_new_dataset_failran_clear')
    parser.add_argument('--num_proc', type=int, default=16, 
                      help='Количество процессов')
    parser.add_argument('--query-field', type=str, default='nlsep', 
                       choices=['semtab', 'nlsep','proto'], help='Тип query field')
    
    args = parser.parse_args()
    
    inputdata = args.inputdata
    outputdata = args.outputdata
    query_field = args.query_field
    NUM_PROC = args.num_proc 
    
    print(f"inputdata: {inputdata}")
    print(f"outputdata: {outputdata}")
    print(f"query_field: {query_field}")
    print(f"num-proc: {NUM_PROC}")
    # Ваш основной код здесь
    dataset = load_from_disk(inputdata)
    dataset = dataset.filter(lambda x: True if x[f'{query_field}_answ']!='None' else False,num_proc=17)
    dataset = dataset.filter(lambda x: True if x[f'{query_field}_label']=='None' else False,num_proc=17)
    dataset = dataset.map(partial(dataset_processing,query_field=query_field,
                                  system_correcting_prompt=system_correcting_prompt,
                                           system_prompt=system_prompt),num_proc=NUM_PROC)
    dataset.save_to_disk(outputdata)


if __name__ == "__main__":
    main()          