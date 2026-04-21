"""ValidationRule dataclass and parsing utilities."""

from dataclasses import dataclass, field
from typing import List, Optional


SUPPORTED_RULE_TYPES = {"not_null", "regex"}


@dataclass
class ValidationRule:
    """Represents a single validation rule configuration."""

    rule_type: str
    column: str
    pattern: Optional[str] = None
    rule_name: Optional[str] = field(default=None)

    def __post_init__(self):
        if self.rule_name is None:
            self.rule_name = f"{self.rule_type}_{self.column}"

    @classmethod
    def from_dict(cls, d: dict) -> "ValidationRule":
        """Parse a rule dictionary into a ValidationRule instance.

        Raises ValueError for missing required fields or unknown rule types.
        """
        if "rule_type" not in d:
            raise ValueError("Missing required field: 'rule_type'")
        if "column" not in d:
            raise ValueError("Missing required field: 'column'")

        rule_type = d["rule_type"]
        if rule_type not in SUPPORTED_RULE_TYPES:
            raise ValueError(
                f"Unknown rule type: '{rule_type}'. "
                f"Supported types: {sorted(SUPPORTED_RULE_TYPES)}"
            )

        return cls(
            rule_type=rule_type,
            column=d["column"],
            pattern=d.get("pattern"),
            rule_name=d.get("rule_name"),
        )


def parse_rules(rules_list: List[dict]) -> List[ValidationRule]:
    """Convert a list of rule dicts into ValidationRule instances."""
    return [ValidationRule.from_dict(d) for d in rules_list]
