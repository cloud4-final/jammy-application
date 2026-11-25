"""Instruction payload validation matching the published OpenAPI contract."""

import re
from decimal import Decimal
from typing import Annotated

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    field_validator,
    model_validator,
)

KW = re.compile(r"(0|[1-9][0-9]{0,10})\.[0-9]{3}")
RFC3339 = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:[Zz]|[+-][0-9]{2}:[0-9]{2})"
)


def require_timestamp(value: object) -> str:
    if not isinstance(value, str) or not RFC3339.fullmatch(value):
        raise ValueError("Expected an RFC 3339 timestamp")
    return value


class Instruction(BaseModel):
    instruction_id: str
    target_reduction_kw: Decimal
    valid_from: Annotated[AwareDatetime, BeforeValidator(require_timestamp)]
    valid_until: Annotated[AwareDatetime, BeforeValidator(require_timestamp)]
    routing_keys: list[str] | None = None

    @field_validator("target_reduction_kw", mode="before")
    @classmethod
    def require_positive_kw(cls, value: object) -> Decimal:
        if not isinstance(value, str) or not KW.fullmatch(value) or value == "0.000":
            raise ValueError("Expected a positive decimal string with three fractional digits")
        return Decimal(value)

    @model_validator(mode="after")
    def require_valid_interval(self) -> Instruction:
        if self.valid_until <= self.valid_from:
            raise ValueError("valid_until must be after valid_from")
        if "routing_keys" in self.model_fields_set and self.routing_keys is None:
            raise ValueError("routing_keys must be a list when provided")
        return self
