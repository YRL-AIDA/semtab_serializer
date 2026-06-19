from config import system_prompt
import pandas as pd
from send_message import send_message_async, ModelMessageDict
import asyncio
from config import system_prompt
from utils.type_check import analyze_dataset_parallel


async def get_pandas(question: str, ser_tbl: str, tbl_types: dict, max_rows=20):

    system_message = ModelMessageDict(role = 'system')
    system_message.add_text_content(system_prompt)

    user_message = ModelMessageDict(role='user')
    user_message.add_text_content(
        f"QUESTION: {question}\n"
        f"AVAILABLE COLUMNS: {', '.join(list(tbl_types.keys()))}\n"
        #f"COLUMN TYPES:\n{tbl_types}\n"
        f"TABLE DATA:\n{ser_tbl}"
    )

    success, responses = await send_message_async(
        messages=[system_message, user_message
        ],
        base_url="http://127.0.0.1:9123/v1",
        api_key='EMPTY',
        model_name='Qwen/Qwen3-Coder-Next',
        temperature=0.3,
    )

    if not success:
        return None, f"LLM error: {responses}"

    return responses[0]