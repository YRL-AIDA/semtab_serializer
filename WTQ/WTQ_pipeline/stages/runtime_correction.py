from __future__ import annotations

from typing import Callable, Awaitable, Optional

from core.result import PipelineResult
from pipeline.base import BaseStage


class RuntimeCorrectionStage(BaseStage):
    stage_name = "runtime_correction"

    def __init__(self, context, correct_pandas_func: Callable[..., Awaitable[str]]):
        super().__init__(context)
        self.correct_pandas_func = correct_pandas_func

    async def run(self, result: PipelineResult) -> PipelineResult:
        if result.error is None:
            return result

        df, ser_tbl, tbl_types = self._load_payload(result)
        sql_code: Optional[str] = result.sql_code if result.used_sql else None
        old_code = result.code
        old_error = result.error

        for attempt in range(1, self.context.max_attempts + 1):
            temperature = self._temperature(attempt)
            try:
                code = await self.correct_pandas_func(
                    question=result.question,
                    ser_tbl=ser_tbl,
                    tbl_types=tbl_types,
                    pandas_code=old_code,
                    error=old_error,
                    temperature=temperature,
                    sql=sql_code,
                )
            except Exception as e:
                result.update_from_attempt(
                    stage=self.stage_name + ("_sql" if result.used_sql else ""),
                    attempt=attempt,
                    temperature=temperature,
                    code=None,
                    result=None,
                    normalized_result=None,
                    error=f"correct_pandas error: {e}",
                    is_correct=False,
                )
                continue

            self._execute_and_update(
                result,
                stage=self.stage_name + ("_sql" if result.used_sql else ""),
                attempt=attempt,
                temperature=temperature,
                code=code,
                df=df,
            )

            if result.error is None:
                break

            old_code = result.code
            old_error = result.error

        return result
