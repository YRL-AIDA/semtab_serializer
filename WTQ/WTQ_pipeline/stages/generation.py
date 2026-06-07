from __future__ import annotations

from typing import Callable, Awaitable, Optional

from core.result import PipelineResult
from pipeline.base import BaseStage


class GenerationStage(BaseStage):
    stage_name = "generate"

    def __init__(self, context, get_pandas_func: Callable[..., Awaitable[str]]):
        super().__init__(context)
        self.get_pandas_func = get_pandas_func

    async def run(self, result: PipelineResult) -> PipelineResult:
        df, ser_tbl, tbl_types = self._load_payload(result)
        sql_code: Optional[str] = result.sql_code if result.used_sql else None
        temperature = self.context.start_temperature

        try:
            code = await self.get_pandas_func(
                question=result.question,
                ser_tbl=ser_tbl,
                tbl_types=tbl_types,
                sql=sql_code,
            )
        except Exception as e:
            result.update_from_attempt(
                stage=self.stage_name + ("_sql" if result.used_sql else ""),
                attempt=1,
                temperature=temperature,
                code=None,
                result=None,
                normalized_result=None,
                error=f"get_pandas error: {e}",
                is_correct=False,
            )
            return result

        return self._execute_and_update(
            result,
            stage=self.stage_name + ("_sql" if result.used_sql else ""),
            attempt=1,
            temperature=temperature,
            code=code,
            df=df,
        )
