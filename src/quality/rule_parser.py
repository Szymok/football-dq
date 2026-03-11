"""YAML Rule Engine Parser."""

import yaml
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field

class RuleParams(BaseModel):
    min: Optional[float] = None
    max: Optional[float] = None
    # Add other dynamic rule params here as needed

class RuleDef(BaseModel):
    name: str
    dimension: str
    description: str
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    check: str
    params: Optional[Dict[str, Any]] = None
    severity: str

class RuleBook(BaseModel):
    rules: List[RuleDef]

def parse_rules(yaml_path: str) -> RuleBook:
    """Read and validate a ruleset YAML into strict Pydantic models."""
    with open(yaml_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    return RuleBook.model_validate(data)
