import pandas as pd
from send_message import send_message_async, ModelMessageDict
import asyncio
from config import sql_pandas_logic_prompt
from utils.type_check import analyze_dataset_parallel


async def get_pandas(question: str, ser_tbl: str, tbl_types: dict,
                     pandas_code: str, error: str, temperature: float,
                     max_rows=50, sql=None):   # добавили sql

    system_message = ModelMessageDict(role='system')
    system_message.add_text_content(sql_pandas_logic_prompt)

    user_message = ModelMessageDict(role='user')
    content = (
        f"QUESTION: {question}\n"
        f"AVAILABLE COLUMNS: {', '.join(list(tbl_types.keys()))}\n"
        f"TABLE DATA:\n{ser_tbl}\n"
        f"PANDAS CODE (incorrect):\n{pandas_code}\n"
        f"PREVIOUS ERROR:\n{error}\n"
        f"CORRECT SQL QUERY (for reference): \n{sql}\n"
    )

    success, responses = await send_message_async(
        messages=[system_message, user_message
        ],
        base_url="http://127.0.0.1:9123/v1",
        api_key='EMPTY',
        model_name='Qwen/Qwen3-Coder-Next',
        temperature=temperature,
    )

    if not success:
        return None, f"LLM error: {responses}"

    return responses[0]