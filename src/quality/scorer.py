"""DQ Scorer – oblicza zagregowany wynik jakości danych (0–100)."""

import logging
from src.storage.models import DQCheckResult

logger = logging.getLogger(__name__)

# Wagi per wymiar DQ (sumują się do 1.0)
DIMENSION_WEIGHTS = {
    "completeness": 0.25,
    "validity":     0.15,
    "uniqueness":   0.15,
    "timeliness":   0.10,
    "consistency":  0.20,
    "accuracy":     0.15,
}


def calculate_dq_score(results: list[DQCheckResult]) -> dict:
    """
    Oblicza DQ Score na podstawie wyników sprawdzeń.

    Zwraca:
    {
        "overall_score": float (0-100),
        "dimension_scores": { "completeness": float, ... },
        "total_checks": int,
        "passed_checks": int,
        "failed_checks": int,
    }
    """
    if not results:
        return {
            "overall_score": 0.0,
            "dimension_scores": {},
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0,
        }

    # Grupuj per wymiar
    dim_results: dict[str, list[bool]] = {}
    for r in results:
        dim = r.dimension
        if dim not in dim_results:
            dim_results[dim] = []
        dim_results[dim].append(r.passed)

    # Oblicz score per wymiar (% passed)
    dim_scores = {}
    for dim, outcomes in dim_results.items():
        passed = sum(1 for o in outcomes if o)
        dim_scores[dim] = round((passed / len(outcomes)) * 100, 1)

    # Ważony overall score
    overall = 0.0
    total_weight = 0.0
    for dim, score in dim_scores.items():
        weight = DIMENSION_WEIGHTS.get(dim, 0.1)
        overall += score * weight
        total_weight += weight

    if total_weight > 0:
        overall = round(overall / total_weight, 1)

    total_checks = len(results)
    passed_checks = sum(1 for r in results if r.passed)

    return {
        "overall_score": overall,
        "dimension_scores": dim_scores,
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": total_checks - passed_checks,
    }


def print_scorecard(score: dict):
    """Wyswietla DQ Scorecard w terminalu (ASCII-safe for Windows)."""
    import sys
    import io
    # Force UTF-8 output on Windows
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print()
    print("+==================================================+")
    print("|           DATA QUALITY SCORECARD                  |")
    print("+==================================================+")
    print(f"|  Overall DQ Score:  {score['overall_score']:>5.1f} / 100               |")
    print(f"|  Checks: {score['passed_checks']} passed, {score['failed_checks']} failed"
          f" (total: {score['total_checks']})" + " " * max(0, 14 - len(str(score['total_checks']))) + "|")
    print("+--------------------------------------------------+")

    for dim, ds in sorted(score["dimension_scores"].items()):
        bar_len = int(ds / 100 * 20)
        bar = "#" * bar_len + "." * (20 - bar_len)
        icon = "OK" if ds >= 80 else "!!" if ds >= 50 else "XX"
        line = f"|  {icon} {dim:<16s} {bar} {ds:>5.1f}%"
        # Pad to fixed width
        padding = max(0, 51 - len(line))
        line = line + " " * padding + "|"
        print(line)

    print("+==================================================+")
    print()
