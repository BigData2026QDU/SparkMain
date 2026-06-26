@echo off
setlocal enabledelayedexpansion

set SPARK_SUBMIT_CMD=spark-submit
set MVN_CMD=mvn
set PYTHON_CMD=python

set DB_NAME=bigdata_ana
set DATASET_DIR=dataset
set CLEANED_DIR=cleanedDataset
set CLEANPY_DIR=cleanPy
set PREPARE_DATA_DIR=prepareData
set JOB_SQL_DIR=jobSQL
set WAREHOUSE_DIR=spark-warehouse
set OUTPUT_DIR=output\bigdata_ana
set JAR_PATH=SparkMain\target\spark-streaming-kafka-1.0.0.jar

echo.
echo ================================================================================
echo SparkMain Spark SQL pipeline
echo ================================================================================

call :print_step 1 "Check local tools and directories"
call :check_command %SPARK_SUBMIT_CMD%
call :check_command %MVN_CMD%
call :check_command %PYTHON_CMD%
call :check_directory %DATASET_DIR%
call :check_directory %CLEANPY_DIR%
call :check_directory %PREPARE_DATA_DIR%
call :check_directory %JOB_SQL_DIR%

call :print_step 2 "Truncate large source files"
%PYTHON_CMD% truncate_file.py
if %ERRORLEVEL% NEQ 0 exit /b 1

call :print_step 3 "Run cleaning scripts"
if exist "%CLEANED_DIR%" rd /s /q "%CLEANED_DIR%"
mkdir "%CLEANED_DIR%"
for %%f in (%CLEANPY_DIR%\*.py) do (
    echo [INFO] Running cleaner: %%f
    %PYTHON_CMD% "%%f"
    if !ERRORLEVEL! NEQ 0 exit /b 1
)

call :print_step 4 "Build Spark Scala pipeline"
%MVN_CMD% -B -f SparkMain\pom.xml -DskipTests package
if %ERRORLEVEL% NEQ 0 exit /b 1

call :print_step 5 "Run Spark SQL analysis"
%SPARK_SUBMIT_CMD% ^
    --class org.example.pipeline.SparkSqlPipeline ^
    --master local[*] ^
    "%JAR_PATH%" ^
    --database "%DB_NAME%" ^
    --dataset-dir "%DATASET_DIR%" ^
    --cleaned-dir "%CLEANED_DIR%" ^
    --prepare-dir "%PREPARE_DATA_DIR%" ^
    --job-dir "%JOB_SQL_DIR%" ^
    --warehouse-dir "%WAREHOUSE_DIR%" ^
    --output-dir "%OUTPUT_DIR%"
if %ERRORLEVEL% NEQ 0 exit /b 1

call :print_step 6 "Pipeline completed"
if exist "print_end.sh" (
    bash print_end.sh
) else (
    echo.
    echo END
)

exit /b 0

:print_step
echo.
echo ================================================================================
echo Step %~1: %~2
echo ================================================================================
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
