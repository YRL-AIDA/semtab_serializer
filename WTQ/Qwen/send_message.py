from openai import OpenAI, AsyncOpenAI
import base64
from typing import List, Dict, Any, Callable
import inspect
def get_kwargs(kwargs: Dict[str, Any], func: Callable) -> Dict[str, Any]:
    """Вытаскивает аргументы из kwargs по сигнатуре функции func."""
    sig = inspect.signature(func)
    return {key: value for key, value in kwargs.items() if key in sig.parameters}

async def send_message_async(messages, base_url: str = "http://192.168.19.127:9886/v1",
                              api_key: str = 'EMPTY',
                              model_name: str = 'Qwen/Qwen3-4B-Instruct-2507',
                              **kwargs):
    """Асинхронная версия отправки сообщений."""
    client = AsyncOpenAI(api_key=api_key, base_url=base_url, **get_kwargs(kwargs, AsyncOpenAI))
    try:
        print(f"Generating content with model: {model_name}")
        response = await client.chat.completions.create(
            messages=messages,
            model=model_name,
            **get_kwargs(kwargs, client.chat.completions.create)
        )
        return True, [answ.message.content for answ in response.choices]
    except Exception as e:
        print("Failed to call LLM: " + str(e))
        return False, None

class ModelMessageDict(dict):
    """Класс - словарь для удобного форматирования запроса к модели."""

    def __init__(self, role: str = 'user'):
        super().__init__()
        self['role'] = role
        self['content'] = ''

    def add_text_content(self, content: str):
        self['content']+= content

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