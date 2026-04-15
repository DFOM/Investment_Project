@echo off
setlocal

REM Run from the script directory (project root).
cd /d "%~dp0"

REM Prefer project virtual environment if it exists.
if exist ".venv\Scripts\python.exe" (
    call ".venv\Scripts\activate.bat"
    python daily_valuation.py
    exit /b %ERRORLEVEL%
)

REM Fallback to system Python when .venv is not found.
python daily_valuation.py
exit /b %ERRORLEVEL%
