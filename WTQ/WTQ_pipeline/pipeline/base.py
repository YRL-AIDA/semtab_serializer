from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Awaitable, Optional

import pandas as pd

from core.comparison import compare_answers, normalize_result_for_storage
from core.dataset_loader import load_table, prepare_table_payload
from core.executor import execute_code
from core.result import PipelineResult


@dataclass
class StageContext:
    dataset_root: str | Path
    sql_dict: dict[str, str]
    max_attempts: int = 4
    start_temperature: float = 0.3
    temperature_step: float = 0.23333


class BaseStage:
    stage_name = "base"

    def __init__(self, context: StageContext):
        self.context = context

    async def run(self, result: PipelineResult) -> PipelineResult:
        raise NotImplementedError

    def _temperature(self, attempt: int) -> float:
        return min(
            self.context.start_temperature + (attempt - 1) * self.context.temperature_step,
            1.0,
        )

    def _load_payload(self, result: PipelineResult) -> tuple[pd.DataFrame, str, dict[str, Any]]:
        df = load_table(result.table, self.context.dataset_root)
        _, ser_tbl, tbl_types = prepare_table_payload(df)
        return df, ser_tbl, tbl_types

    def _execute_and_update(self, result: PipelineResult, *, stage: str, attempt: int,
                            temperature: float, code: Optional[str], df: pd.DataFrame) -> PipelineResult:
        raw_result, error = execute_code(code, df)
        normalized = normalize_result_for_storage(raw_result) if error is None else None
        is_correct = compare_answers(raw_result, result.target_value) if error is None else False
        result.update_from_attempt(
            stage=stage,
            attempt=attempt,
            temperature=temperature,
            code=code,
            result=raw_result,
            normalized_result=normalized,
            error=error,
            is_correct=is_correct,
        )
        return result
