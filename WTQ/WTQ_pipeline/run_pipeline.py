from __future__ import annotations

import argparse
import asyncio

# ВАЖНО: эти импорты рассчитаны на запуск из папки WTQ/Qwen
# или на корректно настроенный PYTHONPATH проекта.
from get_pandas_code import get_pandas as generate_pandas
from correct_pandas_code import get_pandas as correct_pandas
from logic_pandas_code import get_pandas as logic_pandas
from pipeline.runner import PipelineRunner


def parse_ids(value: str | None):
    if not value:
        return None
    return [int(x.strip()) for x in value.split(",") if x.strip()]


async def main():
    parser = argparse.ArgumentParser(description="Unified WTQ two-pass pandas pipeline")
    parser.add_argument("--training", required=True, help="Path to WikiTableQuestions/training.tsv")
    parser.add_argument("--dataset-root", required=True, help="Path to datasets/WikiTableQuestions")
    parser.add_argument("--squall", required=False, help="Path to wtq_sql/squall.json")
    parser.add_argument("--output-dir", required=True, help="Directory for pipeline outputs")
    parser.add_argument("--ids", required=False, help="Comma-separated row ids for debug, e.g. 0,1,2")
    parser.add_argument("--no-final-validation", action="store_true")
    args = parser.parse_args()

    runner = PipelineRunner(
        training_path=args.training,
        dataset_root=args.dataset_root,
        squall_path=args.squall,
        output_dir=args.output_dir,
        generate_func=generate_pandas,
        correct_func=correct_pandas,
        logic_func=logic_pandas,
    )

    stats = await runner.run(
        selected_ids=parse_ids(args.ids),
        validate_final=not args.no_final_validation,
    )
    print(stats)


if __name__ == "__main__":
    asyncio.run(main())
