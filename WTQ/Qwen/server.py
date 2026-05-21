import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig

# Заменили путь на локальный Qwen 7B
model_name = "D:/models/Qwen2.5-Coder-7B-Instruct"
print("Загрузка модели Qwen 7B в 4-бит на GPU...")

# Инициализируем модель с 4-битным квантованием под RTX 4060
tokenizer = AutoTokenizer.from_pretrained(model_name)

quantization_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.bfloat16,
    bnb_4bit_use_double_quant=True
)

model = AutoModelForCausalLM.from_pretrained(
    model_name, 
    quantization_config=quantization_config,
    device_map="cuda:0" # Для квантованных моделей заменяет (.to("cuda"))
)

app = FastAPI()

class ChatRequest(BaseModel):
    model: str
    messages: list
    temperature: float = 0.5
    max_tokens: int = 500

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatRequest):
    try:
        user_message = request.messages[-1]["content"]
        
        text = tokenizer.apply_chat_template([{"role": "user", "content": user_message}], tokenize=False, add_generation_prompt=True)
        
        # Явно отправляем входные токены на видеокарту ("cuda")
        model_inputs = tokenizer([text], return_tensors="pt", truncation=True, max_length=2048).to("cuda")

        if request.temperature <= 0.0:
            gen_kwargs = {"do_sample": False}
        else:
            gen_kwargs = {"do_sample": True, "temperature": request.temperature}

        # Отключаем подсчет градиентов для экономии памяти при генерации
        with torch.no_grad():
            generated_ids = model.generate(
                **model_inputs, 
                max_new_tokens=request.max_tokens, 
                pad_token_id=tokenizer.eos_token_id,
                **gen_kwargs
            )
        
        generated_ids = [output_ids[len(input_ids):] for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)]
        response_text = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]

        return {
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": response_text
                }
            }]
        }
    except Exception as e:
        print(f"\n[ВНУТРЕННЯЯ ОШИБКА СЕРВЕРА]: {e}\n")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9092)