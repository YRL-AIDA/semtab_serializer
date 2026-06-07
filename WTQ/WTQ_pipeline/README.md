# WTQ unified pipeline refactor

Каркас объединяет три этапа в один двухпроходный pipeline:

1. PASS 1 без SQL:
   - generation
   - runtime correction
   - logic correction
2. PASS 2 с SQL только для неправильных после PASS 1:
   - generation + SQL
   - runtime correction + SQL
   - logic correction + SQL
3. Финальная проверка исполняемости `FinalValidationStage`.

## Куда класть

Папки `core`, `pipeline`, `stages` и файл `run_pipeline.py` лучше положить рядом с текущими файлами:

```text
WTQ/Qwen/
  get_pandas_code.py
  correct_pandas_code.py
  logic_pandas_code.py
  core/
  pipeline/
  stages/
  run_pipeline.py
```

## Запуск на 3 строках для проверки

```bash
python run_pipeline.py \
  --training ../../datasets/WikiTableQuestions/training.tsv \
  --dataset-root ../../datasets/WikiTableQuestions \
  --squall ../../datasets/wtq_sql/squall.json \
  --output-dir train_results/unified_pipeline_debug \
  --ids 0,1,2
```

## Запуск полного пайплайна

```bash
python run_pipeline.py \
  --training ../../datasets/WikiTableQuestions/training.tsv \
  --dataset-root ../../datasets/WikiTableQuestions \
  --squall ../../datasets/wtq_sql/squall.json \
  --output-dir train_results/unified_pipeline
```

## Только финальная проверка уже готового results.json

```bash
python -m core.final_validation \
  --input train_results/unified_pipeline/results.json \
  --output train_results/unified_pipeline/validated_results.json \
  --dataset-root ../../datasets/WikiTableQuestions
```
