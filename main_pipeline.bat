@echo off
setlocal enabledelayedexpansion

REM ################################################################################
REM Big Data Analysis Pipeline Main Script (Windows CMD Version)
REM Function: Automate data cleaning to Hive analysis workflow
REM ################################################################################

REM ################################################################################
REM Configuration
REM ################################################################################
set HIVE_DB=bigdata_ana
set HDFS_BASE_PATH=/user/hive/bigdata_ana
set PYTHON_CMD=python

REM Directory definitions
set DATASET_DIR=dataset
set TRUNCATED_DIR=truncatedDataset
set CLEANED_DIR=cleanedDataset
set CLEANPY_DIR=cleanPy
set INITIALIZE_SQL_DIR=initializeSQL
set PREPARE_DATA_DIR=prepareData
set JOB_SQL_DIR=jobSQL

REM ################################################################################
REM Main Process
REM ################################################################################

echo.
echo ================================================================================
echo                    Big Data Analysis Pipeline Started
echo ================================================================================
echo.

REM Environment check
echo [CHECK] Checking required commands...
call :check_command hive
call :check_command hdfs
call :check_command %PYTHON_CMD%
echo [SUCCESS] All commands check passed

echo [CHECK] Checking required directories...
call :check_directory %DATASET_DIR%
call :check_directory %CLEANPY_DIR%
call :check_directory %INITIALIZE_SQL_DIR%
call :check_directory %JOB_SQL_DIR%
echo [SUCCESS] All directories check passed

REM ################################################################################
REM Step 1: Check/Create Hive Database
REM ################################################################################
call :print_step 1 "Check/Create Hive Database"

hive -e "SHOW DATABASES;" | findstr /x "%HIVE_DB%" >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [INFO] Database '%HIVE_DB%' exists
) else (
    echo [INFO] Database '%HIVE_DB%' does not exist, creating...
    hive -e "CREATE DATABASE %HIVE_DB%;"
    if !ERRORLEVEL! NEQ 0 (
        echo [ERROR] Database creation failed
        exit /b 1
    )
    echo [SUCCESS] Database '%HIVE_DB%' created
)

REM ################################################################################
REM Step 2: Run truncate_file.py
REM ################################################################################
call :print_step 2 "Run truncate_file.py"

echo [INFO] Executing truncate_file.py...
%PYTHON_CMD% truncate_file.py
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] truncate_file.py execution failed
    exit /b 1
)
echo [SUCCESS] truncate_file.py executed successfully

REM ################################################################################
REM Step 3: Run all Python scripts in cleanPy
REM ################################################################################
call :print_step 3 "Run cleaning scripts"

echo [INFO] Cleaning cleanedDataset directory...
if exist "%CLEANED_DIR%" rd /s /q "%CLEANED_DIR%"
mkdir "%CLEANED_DIR%"

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
    echo [WARNING] No Python scripts found
)

REM ################################################################################
REM Step 4: Clean HDFS and upload files
REM ################################################################################
call :print_step 4 "Upload to HDFS"

echo [INFO] Cleaning HDFS path: %HDFS_BASE_PATH%
hdfs dfs -rm -r -f %HDFS_BASE_PATH%/* 2>nul

echo [INFO] Creating HDFS directory...
hdfs dfs -mkdir -p %HDFS_BASE_PATH%

set csv_count=0
for %%f in (%CLEANED_DIR%\*.csv) do (
    set csv_file=%%~nxf
    echo [INFO] Uploading: !csv_file!
    hdfs dfs -put "%%f" %HDFS_BASE_PATH%/

    hdfs dfs -test -e %HDFS_BASE_PATH%/!csv_file!
    if !ERRORLEVEL! EQU 0 (
        echo [SUCCESS] !csv_file! uploaded
        set /a csv_count+=1
    ) else (
        echo [ERROR] !csv_file! upload failed
        exit /b 1
    )
)

if %csv_count% EQU 0 (
    echo [WARNING] No CSV files found
)

echo [INFO] Total CSV files uploaded: %csv_count%
echo [SUCCESS] All files uploaded to HDFS

REM ################################################################################
REM Step 5: Check Hive tables
REM ################################################################################
call :print_step 5 "Check Hive tables"

set need_initialize=false

for %%f in (%CLEANED_DIR%\*.csv) do (
    set table_name=%%~nf
    echo [CHECK] Checking table: !table_name!

    hive -e "USE %HIVE_DB%; SHOW TABLES;" | findstr /x "!table_name!" >nul 2>&1
    if !ERRORLEVEL! EQU 0 (
        echo [INFO] Table exists
    ) else (
        echo [INFO] Table does not exist
        set need_initialize=true
    )
)

if "%need_initialize%"=="true" (
    echo [INFO] Executing initialization SQL...

    for %%s in (%INITIALIZE_SQL_DIR%\*.sql) do (
        echo [INFO] Executing: %%s
        hive -f "%%s"
        if !ERRORLEVEL! NEQ 0 (
            echo [ERROR] %%s failed
            exit /b 1
        )
        echo [SUCCESS] %%s executed
    )
) else (
    echo [INFO] All tables exist, skipping
)

REM ################################################################################
REM Step 6: Run prepareData SQL
REM ################################################################################
call :print_step 6 "Prepare data"

if exist "%PREPARE_DATA_DIR%" (
    set sql_count=0
    for %%s in (%PREPARE_DATA_DIR%\*.sql) do (
        echo [INFO] Executing: %%s
        hive -f "%%s"
        if !ERRORLEVEL! NEQ 0 (
            echo [ERROR] %%s failed
            exit /b 1
        )
        echo [SUCCESS] %%s executed
        set /a sql_count+=1
    )

    if !sql_count! EQU 0 (
        echo [INFO] No SQL files found
    )
) else (
    echo [INFO] Directory not found, skipping
)

REM ################################################################################
REM Step 7: Run jobSQL
REM ################################################################################
call :print_step 7 "Run analysis tasks"

set sql_count=0
for %%s in (%JOB_SQL_DIR%\*.sql) do (
    echo [INFO] Executing: %%s
    hive -f "%%s"
    if !ERRORLEVEL! NEQ 0 (
        echo [ERROR] %%s failed
        exit /b 1
    )
    echo [SUCCESS] %%s executed
    set /a sql_count+=1
)

if %sql_count% EQU 0 (
    echo [WARNING] No SQL files found
)

REM ################################################################################
REM Step 8: End
REM ################################################################################
call :print_step 8 "Pipeline completed"

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