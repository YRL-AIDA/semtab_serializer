import pandas as pd
from send_message import send_message_async, ModelMessageDict
import asyncio
from config import sql_pandas_logic_prompt
from utils.type_check import analyze_dataset_parallel
import re

async def get_pandas(question, ser_tbl, tbl_types, pandas_code, error, temperature, max_rows=50, sql=None, df_columns=None):
    system_message = ModelMessageDict(role='system')
    system_message.add_text_content(sql_pandas_logic_prompt)

    if df_columns is None:
        df_columns = list(tbl_types.keys())   # берём колонки из tbl_types

    col_mapping_str = ""
    if df_columns:
        mapping = {f"c{i+1}": col for i, col in enumerate(df_columns)}  # ← убрали лишнюю 'c'
        if sql:  # добавляем суффиксы из SQL
            sql_cols = set(re.findall(r'\bc\d+(?:_\w+)?\b', sql))
            for sc in sql_cols:
                if sc not in mapping:
                    num = int(re.search(r'c(\d+)', sc).group(1))
                    if 1 <= num <= len(df_columns):
                        mapping[sc] = df_columns[num-1]
        col_mapping_str = "COLUMN MAPPING (SQL → table):\n" + "\n".join(
            f"{k} → {v}" for k, v in sorted(mapping.items())
        ) + "\n\n"

    user_message = ModelMessageDict(role='user')
    user_message.add_text_content(
        f"QUESTION: {question}\n"
        f"AVAILABLE COLUMNS: {', '.join(list(tbl_types.keys()))}\n"
        f"{col_mapping_str}"
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