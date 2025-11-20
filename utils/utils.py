from pandas import DataFrame
from typing import List, Tuple, Dict,Any
from doduo.doduo import Doduo
import argparse

def get_item(arr: List, idx: int) -> Any:
    try:
        return arr[idx]
    except IndexError:
        return None


def make_semantic_columns_name(table: DataFrame, model: str = "doduo--viznet", top_k: int = 1, device: str = 'cpu',
                               basedir: str = './doduo', threshold: float = 0.5) -> List[Tuple[str, Dict[str, float]]]:
    proj, model_type = model.split("--")
    if proj == "doduo":
        model = Doduo(argparse.Namespace(**{'model': model_type, 'device': device}), basedir=basedir)
        columns_annotations = model.annotate_columns(table, top_k=top_k, threshold=threshold)
        semantic_columns_name = []
        for col_id, col_name in enumerate(table.columns):
            sem_col_types = get_item(columns_annotations, col_id)
            sem_col_types = sem_col_types if sem_col_types is not None else [(None, None)]
            semantic_columns_name.append((col_name, {col_types[0]: col_types[1] for col_types in sem_col_types}))

    return semantic_columns_name