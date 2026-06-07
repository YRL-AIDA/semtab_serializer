from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional


@dataclass
class PipelineResult:
    """Единая запись результата для одного вопроса WTQ."""

    id: int
    nt_id: str
    question: str
    table: str
    target_value: Any

    code: Optional[str] = None
    result: Optional[str] = None
    raw_result_type: Optional[str] = None
    error: Optional[str] = None
    is_correct: bool = False

    used_sql: bool = False
    sql_code: Optional[str] = None

    stage: str = "init"
    attempt: int = 0
    temperature: float = 0.3

    final_execution_ok: Optional[bool] = None
    final_execution_error: Optional[str] = None
    final_answer: Optional[str] = None
    final_result_type: Optional[str] = None

    history: list[dict[str, Any]] = field(default_factory=list)

    def add_history(self, *, stage: str, attempt: int, temperature: float,
                    code: Optional[str], result: Any, error: Optional[str],
                    is_correct: bool) -> None:
        self.history.append({
            "stage": stage,
            "attempt": attempt,
            "temperature": temperature,
            "code": code,
            "result": None if result is None else str(result),
            "raw_result_type": None if result is None else type(result).__name__,
            "error": error,
            "is_correct": bool(is_correct),
        })

    def update_from_attempt(self, *, stage: str, attempt: int, temperature: float,
                            code: Optional[str], result: Any, normalized_result: Optional[str],
                            error: Optional[str], is_correct: bool) -> None:
        self.stage = stage
        self.attempt = attempt
        self.temperature = temperature
        self.code = code
        self.result = normalized_result
        self.raw_result_type = None if result is None else type(result).__name__
        self.error = error
        self.is_correct = bool(is_correct)
        self.add_history(
            stage=stage,
            attempt=attempt,
            temperature=temperature,
            code=code,
            result=result,
            error=error,
            is_correct=is_correct,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_train_row(cls, row_index: int, row: Any) -> "PipelineResult":
        return cls(
            id=int(row_index),
            nt_id=str(row["id"]),
            question=str(row["utterance"]),
            table=str(row["context"]),
            target_value=row["targetValue"],
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineResult":
        """Мягкая загрузка из старых JSON с разными именами полей."""
        return cls(
            id=int(data.get("id", data.get("id_x"))),
            nt_id=str(data.get("nt_id", data.get("id_y", ""))),
            question=str(data.get("question", data.get("query", ""))),
            table=str(data.get("table") or data.get("context") or ""),
            target_value=str(data.get("target_value", data.get("targetValue", ""))),
            code=data.get("code"),
            result=data.get("result"),
            raw_result_type=data.get("raw_result_type"),
            error=data.get("error"),
            is_correct=bool(data.get("is_correct", False)),
            used_sql=bool(
                data.get("used_sql", False)
                or data.get("sql_code")
                or data.get("sql")
                or data.get("sql_used")
            ),
            sql_code=data.get("sql_code") or data.get("sql") or data.get("sql_used"),
            stage=data.get("stage", "loaded"),
            attempt=int(data.get("attempt", 0) or 0),
            temperature=float(data.get("temperature", 0.3) or 0.3),
            final_execution_ok=data.get("final_execution_ok"),
            final_execution_error=data.get("final_execution_error"),
            final_answer=data.get("final_answer"),
            final_result_type=data.get("final_result_type"),
            history=data.get("history") or [],
        )
