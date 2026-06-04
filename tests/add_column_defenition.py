import sys
import os
from datasets import load_from_disk

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.abspath('/media/research/yrl_aida_users/poddubny/poddubnyy/postgraduate/semtab_serializer/tests')))
from datasets import Dataset
import json
import pandas as pd
from io import StringIO
from tqdm import tqdm
from utils.utils import serialize_table, load_config
from utils.doduo.doduo import Doduo
from utils.cossim.cossim import Qwen3EmbeddingMatcher
import argparse
from functools import partial
import torch
system_prompt = '''You're an expert linguist. You are provided with a table schema with a description in XML format.
Your task is to analyze the tabular context and, based on the information received, give a brief but semantically succinct definition for each HEADER, 
its format features (how to interpret signs, abbreviations, or complex strings, such as "A - B"). 
If there are units of measurement in the EXAMPLES, then include them in the answer. 

The output must be in json in the dictionary format {<HEADER Name> : <header description>}.

######################
-Examples-
######################
Example 1:
<TABLE DESCRIPTION="physical characteristics of solar system planets">
  <HEADER NAME="planet" EXAMPLES="[&quot;earth&quot;, &quot;mars&quot;, &quot;jupiter&quot;]" />
  <HEADER NAME="type" EXAMPLES="[&quot;terrestrial&quot;, &quot;terrestrial&quot;, &quot;gas giant&quot;]" />
  <HEADER NAME="moons" EXAMPLES="[1, 2, 95]" />
  <HEADER NAME="diameter_km" EXAMPLES="[12756, 6792, 142984]" />
  <HEADER NAME="gravity_ms2" EXAMPLES="[9.8, 3.7, 24.8]" />
</TABLE>
######################
Output:
{
"planet": "The proper name of a celestial body in the Solar system, presented in text format.",
"type": "Classification of a planet by its physical and chemical composition (for example, 'terrestrial' — rocky or 'gas giant' — gas giant).",
"moons": "Quantitative indicator indicating the total number of confirmed natural satellites of the planet (integer).",
"diameter_km""The equatorial diameter of a celestial body, expressed in numerical terms. Unit of measurement: kilometers (km).",
"gravity_ms2": "Acceleration of gravity on the surface of the planet. Unit of measurement: meters per second squared ($$m/s^2$$)."
}
######################
Example 2:
<TABLE DESCRIPTION="famous classic literature books and authors">
  <HEADER NAME="title" EXAMPLES="[&quot;1984&quot;, &quot;pride and prejudice&quot;, &quot;the great gatsby&quot;]" />
  <HEADER NAME="author" EXAMPLES="[&quot;george orwell&quot;, &quot;jane austen&quot;, &quot;f. scott fitzgerald&quot;]" />
</TABLE>
######################
Output:
{
  "title": "The official name of a literary work, written in text form.",
  "author": "The first and last name of the author (creator) of the work, presented as a string."
}
######################
Example 3:
<TABLE DESCRIPTION="high-end smartphones specifications 2023-2024">
  <HEADER NAME="model" EXAMPLES="[&quot;iphone 15 pro&quot;, &quot;samsung galaxy s24&quot;, &quot;google pixel 8&quot;]" />
  <HEADER NAME="brand" EXAMPLES="[&quot;apple&quot;, &quot;samsung&quot;, &quot;google&quot;]" />
  <HEADER NAME="screen_size" EXAMPLES="[6.1, 6.2, 6.7]" />
  <HEADER NAME="ram_gb" EXAMPLES="[8, 8, 12]" />
  <HEADER NAME="battery_mah" EXAMPLES="[3274, 4000, 5050]" />
</TABLE>
######################
Output:
{
  "model": "The commercial name of a specific device, including the series name and serial number.",
"brand": "The name of the manufacturing company (brand) responsible for the release of the device.",
"screen_size": "The diagonal size of the device's display, represented as a decimal. Unit of measurement: inches.",
"ram_gb": "The amount of RAM available for the system and applications. Unit of measurement: gigabytes (GB).",
"battery_mah": "The electrical capacity of the device's battery, which determines the battery life. Unit of measurement: milliampere-hours (mAh)."
}

'''
prompt = '''
######################
-Real Data-
######################
{input_text}
######################
Output:
'''
from openai import OpenAI
import base64
from typing import List,Dict,Any,Callable
import inspect
import re
def remove_think_tags(text: str) -> str:
    """Удаляет тег <think>...</think> из ответов моделей reasoning."""
    try:
        match = re.search(r"<\/think>", text)
        if match:
            return text[match.end():].strip()
    except Exception as e:
        print(f"Error removing <think> tags: {e}")
    return text.strip()
    
def get_kwargs(kwargs: Dict[str,Any],func: Callable) -> Dict[str,Any]:
    '''
        Вытаскивает аргументы из kwargs по сигнатуре функции func.
    '''
    sig = inspect.signature(func)
    return {key:value for key,value in kwargs.items() if key in sig.parameters}


class ModelMessageDict(dict):
    '''
        Класс - словарь для удобого форматирования запроса к модели.
        Формирует словарь для передачи в клиента openia и в модель
    '''
    def __init__(self,role:str='user'):
        super().__init__()
        self['role'] = role
        self['content'] =[]
    def add_text_content(self,content:str):
        self['content'].append({'type':'text',
                                       'text':content})

    def add_img_content(self, source:str = 'image_url',path_to_img : str = None, url: str = None):
        match source:
            case 'image_url':
                if path_to_img != None:
                    with open(path_to_img, "rb") as f:
                        base64_image = base64.b64encode(f.read()).decode()
                    self['content'].append({'type':'image_url',
                                       'image_url':{'url':f"data:image/jpeg;base64,{base64_image}"}})
                elif url != None:
                    self['content'].append({'type':'image_url',
                                       'image_url':{'url':url}})

def send_messasge(messages:List[ModelMessageDict],base_url:str = "http://127.0.0.1:8880/v1", api_key:str = 'EMPTY',
                  model_name:str = 'Qwen/Qwen3-VL-30B-A3B-Thinking',**kwargs) -> List[str]:
    '''
        Функция для отправки сообщениий в удаленную модель
    '''
    client = OpenAI( api_key=api_key,base_url=base_url,**get_kwargs(kwargs,OpenAI))
    
    try:
        print(f"Generating content with model: {model_name}",)
        
        response = client.chat.completions.create(
                                        messages=messages,
                                        model=model_name,
                                        **get_kwargs(kwargs,client.chat.completions.create)
                                        )
        
        cleaned_responses = []
        for answ in response.choices:
            text = answ.message.content
            if text:
                # Вырезаем блок <think>...</think> вместе с содержимым и переносами строк
                # Флаг re.DOTALL нужен, чтобы точка (.) захватывала символы новой строки (\n)
                text = remove_think_tags(text)
                cleaned_responses.append(text.strip())
            else:
                cleaned_responses.append("")
                
        return True, cleaned_responses

    except Exception as e:
        print("Failed to call LLM: " + str(e))
        if hasattr(e, 'response'):
            error_info = e.response.json()  
            code_value = error_info['error']['code']
            print(code_value)
        else:
            code_value = "context_length_exceeded"
            print(code_value)
        return False, None   

def extract_json_from_text(text):
    # Ищем первую открывающую фигурную (объект) или квадратную (массив) скобку
    match = re.search(r'\{|$$', text)
    if not match:
        return None

    # Отрезаем весь текст до начала JSON
    json_start = match.start()
    potential_json = text[json_start:]

    decoder = json.JSONDecoder()
    try:
        # raw_decode читает JSON и возвращает сам объект и индекс, где он закончился
        obj, index = decoder.raw_decode(potential_json)
        return obj
    except json.JSONDecodeError as e:
        print(f"Ошибка парсинга JSON: {e}")
        return None


def add_to_dataset_column_def(data):
    
    user = ModelMessageDict(role='user')
    user.add_text_content(system_prompt+prompt.format(input_text=data['semtab_xml_attributes_exampples_top1_tresh50']))
    
    try: 
        sucs,res = send_messasge([user],model_name='Qwen/Qwen3-30B-A3B-Thinking-2507',base_url = "http://127.0.0.1:9124/v1")
        
        data['column_defenition'] = res[0] if sucs else 'None'
    #try:
     #   data['semtab_query'] = data['statement'] + ' ' + serialize_table(df,model=model, description=data['table_caption'],basedir='../utils/doduo/')
    except Exception as e:
        print ('ERRORRRRRRRRRRRRRRRRRRRRRRRRR', e)
        data['column_defenition'] = 'None'
    return data



def main():
    parser = argparse.ArgumentParser(description='Скрипт с настраиваемыми параметрами')
    parser.add_argument('--inputdata', type=str, default='./tab_fact_test_unique_tables')
    parser.add_argument('--outputdata', type=str, default='./tab_fact_test_unique_tables_def')
    parser.add_argument('--num_proc', type=int, default=1, 
                      help='Количество процессов')
    
    args = parser.parse_args()
    
    inputdata = args.inputdata
    outputdata = args.outputdata
    num_proc = args.num_proc 
    dataset = load_from_disk(inputdata)
    dataset2 = dataset.map(add_to_dataset_column_def,num_proc=num_proc)
    dataset2.save_to_disk(outputdata)
    


if __name__ == "__main__":
    main()          