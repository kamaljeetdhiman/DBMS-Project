@echo off
setlocal
title TimescaleDB vs PostgreSQL 16 - Project Dashboard (Group G13)

echo ==================================================================
echo   DR. B. R. AMBEDKAR NATIONAL INSTITUTE OF TECHNOLOGY JALANDHAR
echo   Department of Computer Science and Engineering
echo   DBMS Project: TimescaleDB vs. PostgreSQL 16 IoT Benchmark
echo   Group G13: Khushpreet, Krish, Kamaljeet, Jitendra
echo ==================================================================
echo.

cd /d "%~dp0"

:: Check for python on PATH
where python >nul 2>&1
if %ERRORLEVEL% equ 0 (
    echo [OK] Python detected. Starting local dashboard server...
    python app.py
    goto end
)

:: Check for py launcher on PATH
where py >nul 2>&1
if %ERRORLEVEL% equ 0 (
    echo [OK] Python launcher detected. Starting local dashboard server...
    py app.py
    goto end
)

:: Fallback if Python is not installed
echo [INFO] Python not found on PATH.
echo [INFO] Launching dashboard directly in your default browser...
start "" index.html

:end
pause
