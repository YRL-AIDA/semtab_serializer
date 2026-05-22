from config import system_prompt
import pandas as pd
from send_message import send_message_async, ModelMessageDict
import asyncio


async def get_pandas(question, tbl, max_rows=20):
    tbl_input = tbl.head(max_rows) if len(tbl) > max_rows else tbl

    # Получаем типы данных колонок
    column_types = {col: str(dtype) for col, dtype in tbl_input.dtypes.items()}
    types_info = "\n".join([f"  - {col}: {dtype}" for col, dtype in column_types.items()])

    # Форматируем system_prompt с таблицей, типами и вопросом
    formatted_system_prompt = system_prompt.format(
        table=tbl_input.to_string(index=False),
        column_types=types_info,
        query=question
    )

    # Формируем сообщение пользователя
    user_msg = ModelMessageDict(role='user')
    user_msg.add_text_content(
        f"QUESTION: {question}\n"
        f"AVAILABLE COLUMNS: {', '.join(tbl_input.columns)}\n"
        f"COLUMN TYPES:\n{types_info}\n"
        f"TABLE DATA:\n{tbl_input.to_string(index=False)}"
    )

    success, responses = await send_message_async(
        messages=[
            {"role": "system", "content": formatted_system_prompt},
            user_msg
        ],
        base_url="http://192.168.19.127:9886/v1",
        api_key='EMPTY',
        model_name='Qwen/Qwen3-4B-Instruct-2507',
        temperature=0,
    )

    if not success:
        return None, f"LLM error: {responses}"

    return responses[0]