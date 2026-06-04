import yaml
import json
import base64
import inspect
from typing import List, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from openai import OpenAI
from datasets import load_from_disk, Dataset
from tqdm import tqdm

# ==========================================
# 1. Исправленные классы и функции обертки
# ==========================================

def get_kwargs(kwargs: Dict[str, Any], func: Callable) -> Dict[str, Any]:
    '''
    Вытаскивает аргументы из kwargs по сигнатуре функции func.
    '''
    sig = inspect.signature(func)
    return {key: value for key, value in kwargs.items() if key in sig.parameters}

class ModelMessageDict(dict):
    '''
    Класс - словарь для удобного форматирования запроса к модели.
    Формирует словарь для передачи в клиента openai и в модель.
    '''
    def __init__(self, role: str = 'user'):
        super().__init__()
        self['role'] = role
        self['content'] = ''

    def add_text_content(self, content: str):
        self['content'] += content

    def add_img_content(self, source: str = 'image_url', path_to_img: str = None, url: str = None):
        match source:
            case 'image_url':
                if path_to_img is not None:
                    with open(path_to_img, "rb") as f:
                        base64_image = base64.b64encode(f.read()).decode()
                    self['content'].append({
                        'type': 'image_url',
                        'image_url': {'url': f"data:image/jpeg;base64,{base64_image}"}
                    })
                elif url is not None:
                    self['content'].append({
                        'type': 'image_url',
                        'image_url': {'url': url}
                    })

def send_message(messages: List[ModelMessageDict], base_url: str = "http://127.0.0.1:8880/v1", 
                 api_key: str = 'EMPTY', model_name: str = 'Qwen/Qwen3-VL-32B-Thinking', **kwargs) -> tuple[bool, List[str]]:
    '''
    Функция для отправки сообщений в удаленную модель.
    '''
    client = OpenAI(api_key=api_key, base_url=base_url, **get_kwargs(kwargs, OpenAI))
    try:
        response = client.chat.completions.create(
            messages=messages,
            model=model_name,
            **get_kwargs(kwargs, client.chat.completions.create)
        )
        return True, [answ.message.content for answ in response.choices]

    except Exception as e:
        print("Failed to call LLM: " + str(e))
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_info = e.response.json()
                code_value = error_info.get('error', {}).get('code', 'unknown_error')
                print(f"API Error Code: {code_value}")
            except Exception:
                print("Could not parse error response.")
        else:
            print("context_length_exceeded or connection error")
        return False, []

# ==========================================
# 2. Логика обработки и промпт
# ==========================================

PROMPT_TEMPLATE = """You are an expert Data Scientist and a specialist in Error Analysis for LLM-generated code. 
Your task is to analyze a failed Table Fact Verification attempt. 

A previous model generated a one-line Python Pandas code to verify a `Statement` against a `Table`. However, the execution of this code produced an `Actual Result` that does not match the `Ground Truth Label`. 

Your goal is to identify and describe the exact reason WHY the code failed to produce the correct answer.
Think about which one-line pandas code would give the correct answer. 
Think about how the correct code differs from the incorrect code given to you. Provide a brief description of why the given code produces an incorrect result. This description may include errors in logic, syntax, and errors in referencing table entities.
### CONSTRAINTS (CRITICAL):
1. You MUST output ONLY a valid JSON object. Do not add any conversational text, greetings, or explanations outside the JSON.
2. DO NOT wrap the JSON in markdown formatting (like ```json ... ```). Output the raw JSON string directly.
4. DO NOT give recommendations or step-by-step instructions on how to fix it.
5. The "error_description" must be concise (1-3 sentences maximum).

### INPUT DATA:
<table_caption>
{table_caption}
</table_caption>

<table_text>
{table_text}
</table_text>

<statement>
{statement}
</statement>

<pandas_code>
{pandas_code}
</pandas_code>

<execution_result>
{execution_result}
</execution_result>

<ground_truth_label>
{label}
</ground_truth_label>

### REQUIRED JSON FORMAT:
{{
  "reasoning": "Step-by-step analysis of the table data, the statement, and why the pandas code failed. Think through the problem here.",
  "error_category": "Choose exactly one: [Column Mismatch, Row Filtering Error, Aggregation/Math Error, Logic/Comparison Error, Data Type Error, Syntax/Execution Error]",
  "error_description": "Brief explanation (max 3 sentences) of why the code logic does not correctly verify the statement."
}}"""

def remove_think_tags(text: str) -> str:
    """Удаляет тег <think>...</think> из ответов моделей reasoning."""
    try:
        match = re.search(r"<\/think>", text)
        if match:
            return text[match.end():].strip()
    except Exception as e:
        print(f"Error removing <think> tags: {e}")
    return text.strip()
    
def process_row(row: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Обрабатывает одну строку датасета: формирует промпт, вызывает модель, записывает ответ.
    """
    # Формируем промпт
    prompt = PROMPT_TEMPLATE.format(
        table_caption=row.get('table_caption', ''),
        table_text=row.get('table_text', ''),
        schema=row.get('semtab_xml_attributes_semantic_datatype_exampples_description_top1_tresh50', ''),
        statement=row.get('statement', ''),
        pandas_code=row.get('pandas_code', ''),
        execution_result=row.get('semtab_xml_attributes_semantic_datatype_exampples_description_top1_tresh50_label', ''),
        label=row.get('label', '')
    )
    
    # Создаем сообщение
    msg = ModelMessageDict(role='user')
    msg.add_text_content(prompt)
    
    # Отправляем запрос
    api_cfg = config['api']
    success, result = send_message(
        messages=[msg],
        base_url=api_cfg['base_url'],
        api_key=api_cfg['api_key'],
        model_name=api_cfg['model_name'],
        temperature=api_cfg.get('temperature', 0.2),
        
    )
    
    # Сохраняем результат в строку датасета
    if success and len(result) > 0:
        row['llm_analysis_raw'] = result[0]
    else:
        row['llm_analysis_raw'] = None
        
    return row

# ==========================================
# 3. Основной цикл выполнения
# ==========================================
    
def main():
    # 1. Загрузка конфигурации
    with open('study_err_conf.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
        
    print(f"Loaded config. Using model: {config['api']['model_name']}")
    
    # 2. Загрузка датасета
    # Если датасет локальный, можно использовать load_from_disk или передать путь к CSV/JSON
    # В данном примере предполагается, что он загружается из локальной директории или хаба
    dataset = load_from_disk(config['data']['input_path']) 
    #dataset= dataset.select(range(5))
    print(f"Dataset loaded. Total rows: {len(dataset)}")
    
    processed_data = []
    max_workers = config['processing']['max_workers']
    
    # 3. Многопоточная обработка
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Запускаем задачи в пуле потоков
        futures = {executor.submit(process_row, row, config): row for row in dataset}
        
        # tqdm для отображения прогресса по мере завершения потоков
        for future in tqdm(as_completed(futures), total=len(dataset), desc="Processing instances"):
            try:
                result_row = future.result()
                processed_data.append(result_row)
            except Exception as exc:
                print(f"Row generated an exception: {exc}")
                
    # 4. Преобразование обратно в HF Dataset и сохранение
    print("Processing complete. Saving results...")
    result_dataset = Dataset.from_list(processed_data)
    
    # Сохраняем в JSON или Parquet
    output_path = config['data']['output_path_json']
    result_dataset.to_json(output_path, force_ascii=False)
    print('json save')
    result_dataset.save_to_disk(config['data']['output_path'])
    print(f"Done! Saved to {output_path}")

if __name__ == "__main__":
    main()
