from WTQ.Qwen.send_message import send_message_async, ModelMessageDict
from WTQ.Qwen.config import generate_prompt,correct_prompt,logic_prompt


async def get_pandas(question: str, ser_tbl: str, tbl_types: dict, pandas_code: str, temperature: float, mode: str, error: str = None, max_rows=20):

    if mode == 'correct':
        prompt = correct_prompt
        user_message = ModelMessageDict(role='user')
        user_message.add_text_content(
            f"QUESTION: {question}\n"
            f"AVAILABLE COLUMNS: {', '.join(list(tbl_types.keys()))}\n"
            f"TABLE DATA:\n{ser_tbl}\n"
            f"PANDAS CODE: \n{pandas_code}\n"
            f"ERROR: \n{error}\n"
        )
    elif mode == 'logic':
        prompt = logic_prompt
        user_message = ModelMessageDict(role='user')
        user_message.add_text_content(
            f"QUESTION: {question}\n"
            f"AVAILABLE COLUMNS: {', '.join(list(tbl_types.keys()))}\n"
            f"TABLE DATA:\n{ser_tbl}\n"
            f"PANDAS CODE: \n{pandas_code}\n"
            f"ERROR: \n{error}\n"
        )
    elif mode == 'system':
        prompt = generate_prompt

        user_message = ModelMessageDict(role='user')
        user_message.add_text_content(
            f"QUESTION: {question}\n"
            f"AVAILABLE COLUMNS: {', '.join(list(tbl_types.keys()))}\n"
            f"TABLE DATA:\n{ser_tbl}\n"
        )
    else:
        return None, f"incorrect mode"

    system_message = ModelMessageDict(role='system')
    system_message.add_text_content(prompt)

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

    return responses[0],None