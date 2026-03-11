@echo off
echo Running CI/CD Pipeline Checks
echo ====================================

echo 1. Running Ruff Linter...
python -m ruff check src/
if errorlevel 1 goto error

echo 2. Running Pytest...
python -m pytest tests/
if errorlevel 1 goto error

echo All checks passed!
exit /b 0

:error
echo CI Checks Failed!
exit /b 1
