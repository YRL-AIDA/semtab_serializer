import torch
from typing import Any, Dict, List, Union
from transformers import DataCollatorForLanguageModeling
from transformers import TrainerCallback
import time
from trl import SFTTrainer
from peft import LoraConfig,TaskType 
import wandb

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


# Start a new wandb run to track this script



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
                        r=16,                         # Ранг матрицы (низкая размерность)
                        lora_alpha=32,                # Масштабирующий коэффициент (обычно 2x от r)
                        lora_dropout=0.05,            # Dropout для регуляризации
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
    raw_val_dataset = dataset.get('val',None)
# 2. Магия маскирования промпта (заменяет твой сложный preprocess)
# Модель не будет учиться генерировать инструкцию, только то, что после "### Response:\n"
    response_template = "### Response:\n"
    collator = CustomCompletionOnlyCollator(
        response_template=response_template, 
        tokenizer=tokenizer
    )
    
    
    # 3. Конфиг LoRA (передаем напрямую в Trainer)
    peft_config = LoraConfig(
                        task_type=TaskType.CAUSAL_LM, # Тип задачи
                        r=16,                         # Ранг матрицы (низкая размерность)
                        lora_alpha=32,                # Масштабирующий коэффициент (обычно 2x от r)
                        lora_dropout=0.05,            # Dropout для регуляризации
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
    # 4. Инициализация SFTTrainer
    trainer = SFTTrainer(
        model=model, # Передаешь чистую загруженную модель (БЕЗ get_peft_model)
        args=training_args, # Твои аргументы с deepspeed="config.json" работают здесь идеально!
        train_dataset=raw_train_dataset, # Передаешь СЫРОЙ датасет, без .map()
        eval_dataset = raw_eval_dataset,
        formatting_func=formatting_prompts_func, # Функция, которая склеивает вопрос и ответ
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