from __future__ import annotations

import json
import re
from typing import Any, Optional


def _strip_markdown_fence(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json|python)?\s*", "", text, flags=re.I)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def extract_code_from_response(response: Any) -> Optional[str]:
    """
    Надёжно извлекает pandas-код из ответа модели.

    Поддерживает:
    - {"PANDA": "..."}
    - markdown fenced JSON
    - старый fallback через regex
    - уже готовую строку кода
    """
    if response is None:
        return None

    if isinstance(response, dict):
        value = response.get("PANDA") or response.get("panda")
        return None if value is None else str(value)

    text = str(response).strip()
    if not text:
        return None

    clean_text = _strip_markdown_fence(text)

    # 1. Нормальный путь: модель вернула валидный JSON.
    try:
        obj = json.loads(clean_text)
        if isinstance(obj, dict):
            value = obj.get("PANDA") or obj.get("panda")
            if value is not None:
                return str(value)
    except Exception:
        pass

    # 2. Если вокруг JSON есть лишний текст, пытаемся вырезать JSON-объект.
    start = clean_text.find("{")
    end = clean_text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = clean_text[start:end + 1]
        try:
            obj = json.loads(candidate)
            if isinstance(obj, dict):
                value = obj.get("PANDA") or obj.get("panda")
                if value is not None:
                    return str(value)
        except Exception:
            pass

    # 3. Fallback для старых ответов. Умеет escaped quotes внутри строки.
    match = re.search(r'"PANDA"\s*:\s*"((?:\\.|[^"\\])*)"', clean_text, flags=re.S)
    if match:
        code = match.group(1)
        try:
            return json.loads(f'"{code}"')
        except Exception:
            return code.replace('\\"', '"').replace("\\'", "'")

    # 4. Последний fallback: считаем, что это уже pandas expression.
    return clean_text
