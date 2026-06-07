from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from .checkpoint import load_json, save_json
from .comparison import normalize_result_for_storage
from .dataset_loader import load_table
from .executor import execute_code
from .result import PipelineResult


def validate_single_record(record: dict[str, Any], dataset_root: str | Path, *, check_ast: bool = True) -> dict[str, Any]:
    result = PipelineResult.from_dict(record)

    if not result.code:
        result.final_execution_ok = False
        result.final_execution_error = "empty code"
        result.final_answer = None
        result.final_result_type = None
        return result.to_dict()

    try:
        df = load_table(result.table, dataset_root)
    except Exception as e:
        result.final_execution_ok = False
        result.final_execution_error = f"table read error: {e}"
        result.final_answer = None
        result.final_result_type = None
        return result.to_dict()

    raw_answer, error = execute_code(result.code, df, check_ast=check_ast)
    result.final_execution_ok = error is None
    result.final_execution_error = error

    if error is None:
        result.final_answer = normalize_result_for_storage(raw_answer)
        result.final_result_type = type(raw_answer).__name__
    else:
        result.final_answer = None
        result.final_result_type = None

    return result.to_dict()


def validate_results_file(input_json: str | Path, output_json: str | Path, dataset_root: str | Path,
                          *, check_ast: bool = True) -> dict[str, Any]:
    records = load_json(input_json, default=[])
    validated = [validate_single_record(r, dataset_root, check_ast=check_ast) for r in records]
    save_json(validated, output_json)

    stats = build_validation_stats(validated)
    stats_path = Path(output_json).with_suffix(".stats.json")
    save_json(stats, stats_path)
    return stats


def build_validation_stats(records: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(records)
    executable = sum(1 for r in records if r.get("final_execution_ok") is True)
    failed = total - executable
    correct = sum(1 for r in records if r.get("is_correct") is True)

    correct_and_executable = sum(
        1 for r in records
        if r.get("is_correct") is True
        and r.get("final_execution_ok") is True
    )

    correct_but_not_executable = sum(
        1 for r in records
        if r.get("is_correct") is True
        and r.get("final_execution_ok") is False
    )

    wrong_but_executable = sum(
        1 for r in records
        if r.get("is_correct") is False
        and r.get("final_execution_ok") is True
    )

    stage_counter = Counter(r.get("stage") for r in records)
    error_counter = Counter(
        (r.get("final_execution_error") or "OK")[:200]
        for r in records
    )
    result_types = Counter(r.get("final_result_type") for r in records)

    return {
        "total": total,
        "is_correct_count": correct,
        "is_correct_accuracy_percent": (correct / total * 100) if total else 0,

        "correct_and_executable": correct_and_executable,
        "correct_but_not_executable": correct_but_not_executable,
        "wrong_but_executable": wrong_but_executable,

        "final_executable_count": executable,
        "final_execution_failed_count": failed,
        "final_executable_percent": (executable / total * 100) if total else 0,

        "by_stage": dict(stage_counter),
        "by_final_result_type": dict(result_types),
        "top_final_execution_errors": dict(error_counter.most_common(30)),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Validate executability of final WTQ pandas results")
    parser.add_argument("--input", required=True, help="Path to input results.json")
    parser.add_argument("--output", required=True, help="Path to output validated_results.json")
    parser.add_argument("--dataset-root", required=True, help="Path to datasets/WikiTableQuestions")
    parser.add_argument("--no-ast", action="store_true", help="Disable AST safety validation")
    args = parser.parse_args()

    stats = validate_results_file(
        input_json=args.input,
        output_json=args.output,
        dataset_root=args.dataset_root,
        check_ast=not args.no_ast,
    )
    print(stats)
