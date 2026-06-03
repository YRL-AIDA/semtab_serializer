from WTQ.Qwen.send_message import send_message_async, ModelMessageDict
from WTQ.Qwen.config import sql_pandas_correct_prompt


async def get_pandas(question: str, ser_tbl: str, tbl_types: dict,pandas_code:str, error:str, temperature: float,max_rows=20, sql = None):

    system_message = ModelMessageDict(role = 'system')
    system_message.add_text_content(sql_pandas_correct_prompt)

    user_message = ModelMessageDict(role='user')
    # В get_pandas, формируя user_message:
    user_message.add_text_content(
        f"QUESTION: {question}\n"
        f"AVAILABLE COLUMNS: {', '.join(list(tbl_types.keys()))}\n"
        f"TABLE DATA:\n{ser_tbl}\n"
        f"CORRECT SQL: {sql}\n"  # ← добавлено
        f"PREVIOUS PANDAS CODE (INCORRECT): {pandas_code}\n"  # ← добавлено
        f"PREVIOUS ERROR: {error}\n"  # ← добавлено
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