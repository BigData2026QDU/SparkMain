@echo off
setlocal enabledelayedexpansion

REM ################################################################################
REM Big Data Analysis Pipeline Main Script (Windows CMD Version)
REM Function: Automate data cleaning to Spark analysis workflow
REM ################################################################################

REM ################################################################################
REM Configuration
REM ################################################################################
set PYTHON_CMD=python

REM Directory definitions
set DATASET_DIR=dataset
set TRUNCATED_DIR=truncatedDataset
set CLEANED_DIR=cleanedDataset
set CLEANPY_DIR=cleanPy
set JOB_SQL_DIR=jobSQL
set OUTPUT_DIR=output

REM ################################################################################
REM Main Process
REM ################################################################################

echo.
echo ================================================================================
echo                    Big Data Analysis Pipeline Started (SparkSQL)
echo ================================================================================
echo.

REM Environment check
echo [CHECK] Checking required commands...
call :check_command %PYTHON_CMD%
call :check_command spark-submit
echo [SUCCESS] All commands check passed

echo [CHECK] Checking required directories...
call :check_directory %DATASET_DIR%
call :check_directory %JOB_SQL_DIR%
echo [SUCCESS] All directories check passed

REM ################################################################################
REM Step 1: Check dataset
REM ################################################################################
call :print_step 1 "Check dataset"

if exist "%DATASET_DIR%\ratings.csv" (
    echo [INFO] Dataset exists, skipping download
) else (
    echo [INFO] Dataset not found, downloading...
    %PYTHON_CMD% %DATASET_DIR%\download_movielens.py
    if !ERRORLEVEL! NEQ 0 (
        echo [ERROR] Dataset download failed
        exit /b 1
    )
)

REM ################################################################################
REM Step 2: Run data cleaning scripts
REM ################################################################################
call :print_step 2 "Run data cleaning scripts"

echo [INFO] Cleaning output directory...
if exist "%OUTPUT_DIR%" rd /s /q "%OUTPUT_DIR%"
mkdir "%OUTPUT_DIR%"

set py_count=0
for %%f in (%CLEANPY_DIR%\*.py) do (
    echo [INFO] Executing: %%f
    %PYTHON_CMD% "%%f"
    if !ERRORLEVEL! NEQ 0 (
        echo [ERROR] %%f execution failed
        exit /b 1
    )
    echo [SUCCESS] %%f executed successfully
    set /a py_count+=1
)

if %py_count% EQU 0 (
    echo [WARNING] No Python scripts found in cleanPy
)

REM ################################################################################
REM Step 3: Run Spark analysis tasks
REM ################################################################################
call :print_step 3 "Run Spark analysis tasks"

set py_count=0
for %%f in (%JOB_SQL_DIR%\*.py) do (
    echo [INFO] Executing: %%f
    spark-submit --master local[*] "%%f"
    if !ERRORLEVEL! NEQ 0 (
        echo [ERROR] %%f execution failed
        exit /b 1
    )
    echo [SUCCESS] %%f executed successfully
    set /a py_count+=1
)

if %py_count% EQU 0 (
    echo [WARNING] No Python scripts found in jobSQL
)

REM ################################################################################
REM Step 4: Verify output
REM ################################################################################
call :print_step 4 "Verify output results"

if exist "%OUTPUT_DIR%" (
    echo [INFO] Output directory contents:
    dir /b "%OUTPUT_DIR%"
) else (
    echo [WARNING] Output directory not found
)

REM ################################################################################
REM Step 5: End
REM ################################################################################
call :print_step 5 "Pipeline completed"

if exist "print_end.sh" (
    bash print_end.sh
) else (
    echo [WARNING] print_end.sh not found
    echo.
    echo END
    echo.
)

echo.
echo [COMPLETED] Pipeline execution completed successfully!
exit /b 0

REM ################################################################################
REM Utility Functions
REM ################################################################################

:print_separator
echo ================================================================================
goto :eof

:print_step
echo.
call :print_separator
echo Step %~1: %~2
call :print_separator
goto :eof

:check_command
where %~1 >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Command not found: %~1
    exit /b 1
)
goto :eof

:check_directory
if not exist "%~1" (
    echo [ERROR] Directory not found: %~1
    exit /b 1
)
goto :eof
