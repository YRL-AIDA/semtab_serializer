from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Optional

from tqdm.asyncio import tqdm

from core.checkpoint import save_json, load_json
from core.dataset_loader import load_training, load_sql_dict
from core.final_validation import validate_results_file
from core.result import PipelineResult
from pipeline.base import StageContext
from stages.generation import GenerationStage
from stages.runtime_correction import RuntimeCorrectionStage
from stages.logic_correction import LogicCorrectionStage


class PipelineRunner:
    """
    Единый двухпроходный WTQ pipeline:
    1) generate -> runtime correction -> logic correction без SQL
    2) для неправильных: generate -> runtime correction -> logic correction с SQL
    """

    def __init__(
        self,
        *,
        training_path: str | Path,
        dataset_root: str | Path,
        squall_path: Optional[str | Path],
        output_dir: str | Path,
        generate_func,
        correct_func,
        logic_func,
        max_attempts: int = 4,
    ):
        self.training_path = Path(training_path)
        self.dataset_root = Path(dataset_root)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.train = load_training(self.training_path)
        self.sql_dict = load_sql_dict(squall_path) if squall_path else {}

        self.context = StageContext(
            dataset_root=self.dataset_root,
            sql_dict=self.sql_dict,
            max_attempts=max_attempts,
        )
        self.generate_stage = GenerationStage(self.context, generate_func)
        self.runtime_stage = RuntimeCorrectionStage(self.context, correct_func)
        self.logic_stage = LogicCorrectionStage(self.context, logic_func)

    def _make_initial_result(self, row_index: int, *, use_sql: bool) -> PipelineResult:
        row = self.train.iloc[row_index]
        result = PipelineResult.from_train_row(row_index, row)
        result.used_sql = use_sql
        if use_sql:
            result.sql_code = self.sql_dict.get(result.nt_id)
        return result

    async def run_one_pass_for_id(self, row_index: int, *, use_sql: bool) -> PipelineResult:
        result = self._make_initial_result(row_index, use_sql=use_sql)
        result = await self.generate_stage.run(result)
        result = await self.runtime_stage.run(result)
        result = await self.logic_stage.run(result)
        return result

    async def run_ids(self, ids: list[int], *, use_sql: bool, desc: str) -> list[PipelineResult]:
        results: list[PipelineResult] = []
        for row_id in tqdm(ids, desc=desc):
            result = await self.run_one_pass_for_id(int(row_id), use_sql=use_sql)
            results.append(result)
        return results

    async def run(self, selected_ids: Optional[list[int]] = None, *, validate_final: bool = True) -> dict[str, Any]:
        ids = selected_ids if selected_ids is not None else list(range(len(self.train)))

        pass1 = await self.run_ids(ids, use_sql=False, desc="PASS 1 without SQL")
        pass1_path = self.output_dir / "pass1_results.json"
        save_json([r.to_dict() for r in pass1], pass1_path)

        wrong_ids = [r.id for r in pass1 if not r.is_correct]
        pass2 = await self.run_ids(wrong_ids, use_sql=True, desc="PASS 2 with SQL") if wrong_ids else []
        pass2_path = self.output_dir / "pass2_sql_results.json"
        save_json([r.to_dict() for r in pass2], pass2_path)

        final_by_id = {r.id: r for r in pass1}
        for r in pass2:
            # SQL-проход заменяет первый только если стал правильным.
            # Если не стал — оставляем pass1 как лучший baseline.
            if r.is_correct:
                final_by_id[r.id] = r

        final_results = [final_by_id[i].to_dict() for i in sorted(final_by_id)]
        final_path = self.output_dir / "results.json"
        save_json(final_results, final_path)

        stats = {
            "total": len(final_results),
            "pass1_correct": sum(1 for r in pass1 if r.is_correct),
            "pass1_wrong": len(wrong_ids),
            "pass2_fixed": sum(1 for r in pass2 if r.is_correct),
            "final_correct": sum(1 for r in final_results if r.get("is_correct")),
            "results_path": str(final_path),
            "pass1_path": str(pass1_path),
            "pass2_path": str(pass2_path),
        }

        if validate_final:
            validated_path = self.output_dir / "validated_results.json"
            validation_stats = validate_results_file(
                input_json=final_path,
                output_json=validated_path,
                dataset_root=self.dataset_root,
            )
            stats["validated_results_path"] = str(validated_path)
            stats["validation"] = validation_stats

        save_json(stats, self.output_dir / "summary.json")
        return stats
