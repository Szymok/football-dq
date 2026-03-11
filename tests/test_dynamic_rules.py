import pytest
import os
import yaml
from pathlib import Path
from src.quality.rule_parser import parse_rules

@pytest.fixture
def mock_yaml_file(tmp_path):
    rules = {
        "rules": [
            {
                "name": "test_xg_range",
                "dimension": "validity",
                "description": "xG range test",
                "column": "xg",
                "check": "between",
                "params": {"min": 0.0, "max": 7.0},
                "severity": "warning"
            },
            {
                "name": "test_not_null",
                "dimension": "completeness",
                "description": "Ensure no nulls",
                "column": "minutes",
                "check": "not_null",
                "severity": "critical"
            }
        ]
    }
    file_path = tmp_path / "test_rules.yaml"
    with open(file_path, "w") as f:
        yaml.dump(rules, f)
    return str(file_path)


def test_parse_rules(mock_yaml_file):
    rulebook = parse_rules(mock_yaml_file)
    assert len(rulebook.rules) == 2
    
    rule1 = rulebook.rules[0]
    assert rule1.name == "test_xg_range"
    assert rule1.check == "between"
    assert rule1.params["min"] == 0.0
    
    rule2 = rulebook.rules[1]
    assert rule2.column == "minutes"
    assert rule2.check == "not_null"
