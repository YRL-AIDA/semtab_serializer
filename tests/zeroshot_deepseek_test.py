from datasets import load_from_disk
from openai import OpenAI
import sys
import os
import json
import pandas as pd
from io import StringIO
from tqdm import tqdm
import time
from functools import partial
import re
import argparse

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.abspath('/media/research/yrl_aida_users/poddubny/poddubnyy/postgraduate/semtab_serializer/tests')))
query_field = 'semtab' #'nlsep' #'semtab'
NUM_PROC = 2
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

def send_message(message,max_tokens=1000,top_p=0.9,temperature=0.5,server_url="http://127.0.0.1:8800/v1",api_key="dummy",
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
        time.sleep(3)
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
def dataset_processing(entry,query_field=None):
    df = pd.read_csv(StringIO(entry['table_text']), delimiter='#')
    success,response_sep = send_message(entry[f"{query_field}_query"],
                                       system_prompt=system_prompt)

    entry[f'{query_field}_answ'] = 'None'
    entry[f'{query_field}_label'] = 'None'
    
    if success:
        entry[f'{query_field}_answ'] = response_sep
        try:
            pandas_eval = str(bool(eval(parse_panda_code(response_sep))))
            entry[f'{query_field}_label'] = str(pandas_eval)
        except Exception as e:
            print('EEEERRRRRRR', entry['id'])
            print(e)
    return entry
def main():
    parser = argparse.ArgumentParser(description='Скрипт с настраиваемыми параметрами')
    parser.add_argument('--query-field', type=str, default='semtab', 
                       choices=['semtab', 'nlsep','proto'], help='Тип query field')
    parser.add_argument('--input_data', type=str, default='none_filtered_new_dataset2', 
                        help='путь к набору данных для обработки')
    parser.add_argument('--output_data', type=str, default='none_filtered_new_dataset2_new', 
                        help='путь к выходному набору данных')
    parser.add_argument('--num-proc', type=int, default=2, 
                       help='Количество процессов')
    
    args = parser.parse_args()
    in_dataset = args.input_data
    out_dataset = args.output_data
    query_field = args.query_field
    NUM_PROC = args.num_proc
    
    print(f"query_field: {query_field}")
    print(f"NUM_PROC: {NUM_PROC}")
    print(f"in_dataset: {in_dataset}")
    print(f"out_dataset: {out_dataset}")

    
    # Ваш основной код здесь
    dataset = load_from_disk(in_dataset)
    dataset2 = dataset.map(partial(dataset_processing,query_field=query_field),num_proc=NUM_PROC)
    dataset2.save_to_disk(out_dataset)

if __name__ == "__main__":
    main()          
