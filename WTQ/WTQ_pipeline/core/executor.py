from __future__ import annotations

import ast
from typing import Any, Optional

import numpy as np
import pandas as pd

from utils.normalize import convert_dataset_types, convert_type, find_word
from .response_parser import extract_code_from_response


FORBIDDEN_NAMES = {
    "open", "exec", "eval", "compile", "input", "__import__",
    "globals", "locals", "vars", "dir", "getattr", "setattr", "delattr",
}
FORBIDDEN_ROOT_ATTRS = {"os", "subprocess", "pathlib", "shutil", "socket", "requests", "sys"}


def validate_code_ast(code: str) -> Optional[str]:
    """Минимальная защита перед eval pandas expression."""
    try:
        tree = ast.parse(code, mode="eval")
    except SyntaxError as e:
        return f"syntax error: {e}"

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.Lambda, ast.FunctionDef, ast.ClassDef)):
            return f"forbidden syntax: {type(node).__name__}"

        if isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            return f"forbidden name: {node.id}"

        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in FORBIDDEN_NAMES:
                return f"forbidden call: {node.func.id}"

        if isinstance(node, ast.Attribute):
            root = node.value
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name) and root.id in FORBIDDEN_ROOT_ATTRS:
                return f"forbidden module access: {root.id}.{node.attr}"

    return None


def execute_code(code_str: Any, df: pd.DataFrame, *, check_ast: bool = False) -> tuple[Any, Optional[str]]:
    """Выполняет pandas-код на нормализованной таблице."""
    try:
        df_norm = convert_dataset_types(df)
    except Exception as e:
        return None, f"convert_dataset_types error: {e}"

    clean_code = extract_code_from_response(code_str)
    if clean_code is None:
        return None, "extract_code_from_response returned None"

    if check_ast:
        ast_error = validate_code_ast(clean_code)
        if ast_error:
            return None, ast_error

    try:
        safe_globals = {
            "__builtins__": {},

            "df": df_norm,
            "pd": pd,
            "np": np,
            "convert_type": convert_type,
            "find_word": find_word,

            "int": int,
            "str": str,
            "float": float,
            "bool": bool,
            "len": len,
            "sum": sum,
            "max": max,
            "min": min,
            "abs": abs,
            "round": round,
            "isinstance": isinstance,
            "any": any,
            "all": all,
            "sorted": sorted,
            "list": list,
            "set": set,
            "tuple": tuple,
            "range": range,
            "enumerate": enumerate,
            "zip": zip,
            "map": map,
            "filter": filter,
        }

        result = eval(clean_code, safe_globals, {})
        return result, None

    except Exception as e:
        return None, str(e)
