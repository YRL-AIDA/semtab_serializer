from trl import SFTTrainer
from peft import LoraConfig,TaskType 
import wandb

# Start a new wandb run to track this script
from transformers import TrainerCallback
import time
from functools import partial

import torch
from typing import Any, Dict, List, Union, Optional
from transformers import DataCollatorForLanguageModeling
import random
import numpy as np
from dataclasses import dataclass, field
import transformers
from datasets import load_from_disk

IGNORE_INDEX = -100
EOT_TOKEN = "<|EOT|>"

def set_global_seed(seed: int = 42):
    """
    Фиксирует seed для обеспечения полной воспроизводимости экспериментов.
    """
    # 1. Фиксация в стандартном Python
    random.seed(seed)
    
    # 2. Фиксация в NumPy
    np.random.seed(seed)
    
    # 3. Фиксация в PyTorch (CPU)
    torch.manual_seed(seed)
    
    # 4. Фиксация в PyTorch (GPU)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed) # Важно, если используется несколько видеокарт (Multi-GPU)
    
    # 5. Настройки CuDNN (GPU backend)
    # Детерминированные алгоритмы (может незначительно снизить скорость обучения)
    #torch.backends.cudnn.deterministic = True 
    # Отключение автоматического поиска оптимальных алгоритмов свертки
    #torch.backends.cudnn.benchmark = False 
    
    # 6. Фиксация в Hugging Face Transformers
    transformers.set_seed(seed)

def build_instruction_prompt(table: str,query: str):
    return """
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
### Response:
""".format(instruction.strip()).lstrip()

@dataclass
class ModelArguments:
    model_name_or_path: Optional[str] = field(default="deepseek-ai/deepseek-coder-6.7b-instruct")
    run_name: Optional[str] = field(default="test_run")
    lora_rank: int = field(default=16 )
    lora_dropout: float = field(default=0.05)
    

@dataclass
class DataArguments:
    data_path: str = field(default=None, metadata={"help": "Path to the training data."})
    table_col_name: str = field(default=None, metadata={"help": "Name column with table serialization."})


@dataclass
class TrainingArguments(transformers.TrainingArguments):
    cache_dir: Optional[str] = field(default=None)
    optim: str = field(default="adamw_torch")
    model_max_length: int = field(
        default=512,
        metadata={"help": "Maximum sequence length. Sequences will be right padded (and possibly truncated)."},
    )
    seed: int = field(default=42, metadata={"help": "Random seed for initialization."})
    


class CustomCompletionOnlyCollator(DataCollatorForLanguageModeling):
    def __init__(self, response_template: str, tokenizer, *args, **kwargs):
        # Обязательно выключаем Masked Language Modeling (mlm=False), так как у нас Causal LM
        super().__init__(tokenizer=tokenizer, mlm=False, *args, **kwargs)
        # Токенизируем шаблон ответа (без спецтокенов, чтобы избежать конфликтов с BOS)
        self.response_template_ids = tokenizer.encode(response_template, add_special_tokens=False)

    def torch_call(self, examples: List[Union[List[int], Any, Dict[str, Any]]]) -> Dict[str, Any]:
        # Стандартный коллатор соберет батч и создаст input_ids и labels
        batch = super().torch_call(examples)
        labels = batch["labels"].clone()

        for i in range(len(labels)):
            label_list = labels[i].tolist()
            template_len = len(self.response_template_ids)
            match_idx = -1
            
            # Ищем подпоследовательность токенов шаблона ответа
            for j in range(len(label_list) - template_len + 1):
                if label_list[j : j + template_len] == self.response_template_ids:
                    match_idx = j + template_len
                    break
            
            if match_idx != -1:
                # Маскируем всё ДО конца шаблона (заменяем на -100)
                labels[i, :match_idx] = -100
            else:
                # Если шаблон почему-то не найден в тексте, маскируем всё, 
                # чтобы модель не училась на неверно размеченных данных
                labels[i, :] = -100
                
        batch["labels"] = labels
        return batch


class WandbLoggingCallback(TrainerCallback):
    # 2. Вызывается в начале обучения
    def on_train_begin(self, args, state, control, **kwargs):
        
        if state.is_world_process_zero:
            print("Обучение начинается")
            # Здесь логируются train_loss, learning_rate и т.д.
            run.log({"start_train": time.time()})
    

    
    # 6. Вызывается после каждого логирования (по logging_steps)
    def on_log(self, args, state, control, logs=None, **kwargs):
        if state.is_world_process_zero:
            # Здесь логируются train_loss, learning_rate и т.д.
            run.log({'train_time':time.time(),**logs})
            print(f"Логирование на шаге {state.global_step}: {logs}")
        # Здесь можно отправить метрики в wandb
    
    # 7. Вызывается после каждой валидации (по eval_steps)
    def on_evaluate(self, args, state, control, metrics=None, **kwargs):
        if state.is_world_process_zero:
            # Здесь логируются train_loss, learning_rate и т.д.
            run.log({'eval_time':time.time(),**metrics})
            print(f"Валидация на шаге {state.global_step}: {metrics}")
        # Здесь можно обработать метрики валидации
    
    # 10. Вызывается в конце обучения
    def on_train_end(self, args, state, control, **kwargs):
        if state.is_world_process_zero:
            # Здесь логируются train_loss, learning_rate и т.д.
            run.log({"End_train": time.time()})
            print("Обучение закончено")
    # 11. Вызывается в конце (для очистки ресурсов)
   


# 1. Твоя функция подготовки промпта (немного адаптирован под батчи)
def formatting_prompts_func(example,table_col_name=''):
    output_texts = []
    # Важно: example содержит списки, так как SFTTrainer передает батчи
    for i in range(len(example['statement'])):
        prompt = build_instruction_prompt(example[table_col_name][i], example['statement'][i])
        response = f'"PANDA": {example["pandas_code"][i]}\n{EOT_TOKEN}'
        output_texts.append(prompt + response)
    return output_texts



run = None

def main():
    parser = transformers.HfArgumentParser((ModelArguments, DataArguments, TrainingArguments))
    model_args, data_args, training_args = parser.parse_args_into_dataclasses()
    global run
    run = wandb.init(
    # Set the wandb entity where your project will be logged (generally your team name).
    entity="ivan",
    # Set the wandb project where this run will be logged.
    project=model_args.run_name,
    # Track hyperparameters and run metadata.
    config={
        **model_args,
        **data_args,
        **data_args
    },
)
    if training_args.local_rank == 0:
        print('='*100)
        print(training_args)
    
    tokenizer = transformers.AutoTokenizer.from_pretrained(
        model_args.model_name_or_path,
        model_max_length=training_args.model_max_length,
        padding_side="right",
        use_fast=True,
        trust_remote_code=True
    )

    print("PAD Token:", tokenizer.pad_token, tokenizer.pad_token_id)
    print("BOS Token", tokenizer.bos_token, tokenizer.bos_token_id)
    print("EOS Token", tokenizer.eos_token, tokenizer.eos_token_id)

    if training_args.local_rank == 0:
        print("Load tokenizer from {} over.".format(model_args.model_name_or_path))

    model = transformers.AutoModelForCausalLM.from_pretrained(
        model_args.model_name_or_path,
        torch_dtype=torch.bfloat16
    )
    peft_config = LoraConfig(
                        task_type=TaskType.CAUSAL_LM, # Тип задачи
                        r=model_args.lora_rank,                         # Ранг матрицы (низкая размерность)
                        lora_alpha=2*model_args.lora_rank,                # Масштабирующий коэффициент (обычно 2x от r)
                        lora_dropout=model_args.lora_dropout,            # Dropout для регуляризации
                        bias="none",                  # Обычно bias не обучают
                        target_modules=[              # Куда встраиваем матрицы
                            "q_proj", 
                            "k_proj", 
                            "v_proj", 
                            "o_proj", 
                            "gate_proj", 
                            "up_proj", 
                            "down_proj"
                        ],
                    )
    formatting_prompts_func_loc = partial(formatting_prompts_func,table_col_name=data_args.table_col_name)
    if training_args.local_rank == 0:
        print("Load model from {} over.".format(model_args.model_name_or_path))


    #raw_train_datasets = load_dataset(
    #    'json',
    #    data_files=data_args.data_path,
    #    split="train",
    #    cache_dir=training_args.cache_dir
    #)
    dataset = load_from_disk(data_path)
    raw_train_datasets = dataset.get('train',None)
    raw_eval_dataset = dataset.get('val',None)
# 2. Магия маскирования промпта (заменяет твой сложный preprocess)
# Модель не будет учиться генерировать инструкцию, только то, что после "### Response:\n"
    response_template = "### Response:\n"
    collator = CustomCompletionOnlyCollator(
        response_template=response_template, 
        tokenizer=tokenizer
    )
    
    
    # 3. Конфиг LoRA (передаем напрямую в Trainer)
    
    # 4. Инициализация SFTTrainer
    trainer = SFTTrainer(
        model=model, # Передаешь чистую загруженную модель (БЕЗ get_peft_model)
        args=training_args, # Твои аргументы с deepspeed="config.json" работают здесь идеально!
        train_dataset=raw_train_dataset, # Передаешь СЫРОЙ датасет, без .map()
        eval_dataset = raw_eval_dataset,
        formatting_func=formatting_prompts_func_loc, # Функция, которая склеивает вопрос и ответ
        data_collator=collator, # Тот самый умный коллатор
        max_seq_length=training_args.model_max_length, # SFTTrainer сам обрежет длинные тексты
        peft_config=peft_config, # SFTTrainer сам применит LoRA
        callbacks=[WandbLoggingCallback()]
    )
    
    # 5. Запуск
    trainer.train()
    trainer.save_state()
    trainer.save_model(training_args.output_dir)
    run.finish()


if __name__ == '__main__':
    main()