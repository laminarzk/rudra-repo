"""Default, stateless DataFrame transformer driven by the recipe.

Applies column mapping/renaming, derived columns, fail-closed PII handling and
the final projection. Used when a recipe does not name a specialised
transformer. Contains no I/O.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.spark_emr.export_extract_spark_job.util.constants import (
    PII_ACTION_HASH_SHA256,
    PII_ACTION_MASK,
    PII_ACTION_REDACT,
    PII_ACTIONS,
    PII_DEFAULT_KEEP_LAST,
    PII_DEFAULT_MASK_CHAR,
)
from dataoutbound.spark_emr.export_extract_spark_job.util.file_utils import (
    as_list,
    get_recipe_value,
    validate_identifier,
)
from dataoutbound.util.exceptions import ValidationError

logger = get_logger(__name__)

_SHA256_BITS = 256


class DefaultTransformer:
    """Recipe-driven column mapping, derivation and PII transformer.

    Args:
        recipe: Parsed recipe mapping for the dataset being extracted.
        config: Optional job configuration (reserved for transformer overrides).
    """

    def __init__(
        self,
        recipe: Optional[Mapping[str, Any]] = None,
        config: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self._recipe: Mapping[str, Any] = recipe if isinstance(recipe, Mapping) else {}
        self._config: Mapping[str, Any] = config if isinstance(config, Mapping) else {}
        self._pii_rules = _normalise_pii_rules(self._recipe.get("pii_rules"))

    @property
    def pii_rule_count(self) -> int:
        """Number of validated PII rules that will be applied."""
        return len(self._pii_rules)

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply the recipe transformations in a deterministic order.

        Returns:
            The transformed DataFrame (a pure function of the input).
        """
        if df is None:
            raise ValidationError("A DataFrame is required for transformation")

        working = self._apply_column_mappings(df)
        working = self._apply_derived_columns(working)
        working = self._apply_pii_rules(working)
        working = self._apply_projection(working)

        logger.info(
            "Transformation complete",
            extra={
                "pii_rules": self.pii_rule_count,
                "columns": len(working.columns),
            },
        )
        return working

    def _apply_column_mappings(self, df: DataFrame) -> DataFrame:
        """Rename columns according to the recipe's ``column_mappings``."""
        mappings = get_recipe_value(self._recipe, "column_mappings")
        if not isinstance(mappings, Mapping):
            return df

        working = df
        for source, target in mappings.items():
            if source not in working.columns:
                logger.warning(
                    "Column mapping source not found", extra={"column": str(source)}
                )
                continue
            working = working.withColumnRenamed(
                str(source), validate_identifier(target, "column")
            )
        return working

    def _apply_derived_columns(self, df: DataFrame) -> DataFrame:
        """Add or replace columns defined by recipe SQL expressions."""
        derived = get_recipe_value(self._recipe, "derived_columns")
        if not isinstance(derived, Mapping):
            return df

        working = df
        for name, expression in derived.items():
            column_name = validate_identifier(name, "derived column")
            if not isinstance(expression, str) or not expression.strip():
                raise ValidationError(
                    f"Derived column '{column_name}' requires a SQL expression"
                )
            working = working.withColumn(column_name, F.expr(expression.strip()))
        return working

    def _apply_pii_rules(self, df: DataFrame) -> DataFrame:
        """Apply REDACT/HASH_SHA256/MASK to the configured PII fields."""
        working = df
        for rule in self._pii_rules:
            field_name = rule["field_name"]
            if field_name not in working.columns:
                logger.warning(
                    "PII field not present in dataset", extra={"field": field_name}
                )
                continue
            working = working.withColumn(field_name, _pii_expression(field_name, rule))
        return working

    def _apply_projection(self, df: DataFrame) -> DataFrame:
        """Select the recipe's output columns when it declares them."""
        projection_config = get_recipe_value(self._recipe, "columns")
        if isinstance(projection_config, Mapping):
            requested = as_list(list(projection_config.keys()))
        else:
            requested = as_list(
                get_recipe_value(self._recipe, "output_columns") or projection_config
            )
        if not requested:
            return df

        existing = [column for column in requested if column in df.columns]
        missing = [column for column in requested if column not in df.columns]
        if missing:
            logger.warning("Projection columns missing", extra={"missing": missing})
        if not existing:
            raise ValidationError("Recipe projection removed every column")
        return df.select(*existing)


def _pii_expression(field_name: str, rule: Mapping[str, Any]) -> Column:
    """Build the column expression for a single PII rule."""
    action = rule["action"]
    string_value = F.col(field_name).cast("string")

    if action == PII_ACTION_REDACT:
        return F.lit(None).cast("string")

    if action == PII_ACTION_HASH_SHA256:
        return F.sha2(string_value, _SHA256_BITS)

    if action == PII_ACTION_MASK:
        keep_last = _positive_int(rule.get("keep_last"), PII_DEFAULT_KEEP_LAST)
        mask_char = str(rule.get("mask_char") or PII_DEFAULT_MASK_CHAR)
        length = F.length(string_value)
        tail_length = F.least(length, F.lit(keep_last))
        tail = F.substring(string_value, length - tail_length + F.lit(1), tail_length)
        mask_length = F.greatest(length - F.lit(keep_last), F.lit(0))
        masked = F.repeat(F.lit(mask_char), mask_length)
        return F.concat(masked, tail)

    raise ValidationError(f"Unsupported PII action: {action}")


def _positive_int(value: Any, default: int) -> int:
    """Coerce an optional positive integer, falling back to ``default``."""
    if value is None:
        return int(default)
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"keep_last must be an integer: {value!r}") from exc
    if resolved <= 0:
        return int(default)
    return resolved


def _normalise_pii_rules(raw_rules: Any) -> List[Dict[str, Any]]:
    """Validate and normalise the recipe's PII rule list."""
    if raw_rules is None:
        return []
    if not isinstance(raw_rules, Sequence) or isinstance(raw_rules, str):
        raise ValidationError("pii_rules must be a list of rule objects")

    normalised: List[Dict[str, Any]] = []
    for rule in raw_rules:
        if not isinstance(rule, Mapping):
            raise ValidationError("Each PII rule must be an object")
        field_name = validate_identifier(rule.get("field_name"), "PII field_name")
        raw_action = rule.get("action")
        if not isinstance(raw_action, str) or not raw_action.strip():
            raise ValidationError("Each PII rule requires an action")
        action = raw_action.strip().upper()
        if action not in PII_ACTIONS:
            raise ValidationError(f"Unsupported PII action: {action}")
        normalised.append(
            {
                "field_name": field_name,
                "action": action,
                "mask_char": str(rule.get("mask_char") or PII_DEFAULT_MASK_CHAR),
                "keep_last": rule.get("keep_last", PII_DEFAULT_KEEP_LAST),
            }
        )
    return normalised
