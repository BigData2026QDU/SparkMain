@echo off
setlocal enabledelayedexpansion

set SPARK_SUBMIT_CMD=spark-submit
set MVN_CMD=mvn
set PYTHON_CMD=python

set DB_NAME=bigdata_ana_test
set DATASET_DIR=dataset_test
set CLEANED_DIR=cleanedDataset_test
set CLEANPY_DIR=cleanPy_test
set PREPARE_DATA_DIR=prepareData_test
set JOB_SQL_DIR=jobSQL_test
set WAREHOUSE_DIR=spark-warehouse-test
set OUTPUT_DIR=output\bigdata_ana_test
set JAR_PATH=SparkMain\target\spark-streaming-kafka-1.0.0.jar

echo.
echo ================================================================================
echo SparkMain lightweight Spark SQL pipeline test
echo ================================================================================

call :print_step 1 "Check local tools and directories"
call :check_command %SPARK_SUBMIT_CMD%
call :check_command %MVN_CMD%
call :check_command %PYTHON_CMD%
call :check_directory %DATASET_DIR%
call :check_directory %CLEANPY_DIR%
call :check_directory %PREPARE_DATA_DIR%
call :check_directory %JOB_SQL_DIR%

call :print_step 2 "Prepare cleaned test data"
if exist "%CLEANED_DIR%" rd /s /q "%CLEANED_DIR%"
mkdir "%CLEANED_DIR%"
for %%f in (%CLEANPY_DIR%\*.py) do (
    echo [INFO] Running cleaner: %%f
    %PYTHON_CMD% "%%f"
    if !ERRORLEVEL! NEQ 0 exit /b 1
)

call :print_step 3 "Build Spark Scala pipeline"
%MVN_CMD% -B -f SparkMain\pom.xml -DskipTests package
if %ERRORLEVEL% NEQ 0 exit /b 1

call :print_step 4 "Run Spark SQL analysis"
if exist "%WAREHOUSE_DIR%" rd /s /q "%WAREHOUSE_DIR%"
if exist "%OUTPUT_DIR%" rd /s /q "%OUTPUT_DIR%"
%SPARK_SUBMIT_CMD% ^
    --class org.example.pipeline.SparkSqlPipeline ^
    --master local[2] ^
    "%JAR_PATH%" ^
    --database "%DB_NAME%" ^
    --dataset-dir "%DATASET_DIR%" ^
    --cleaned-dir "%CLEANED_DIR%" ^
    --prepare-dir "%PREPARE_DATA_DIR%" ^
    --job-dir "%JOB_SQL_DIR%" ^
    --warehouse-dir "%WAREHOUSE_DIR%" ^
    --output-dir "%OUTPUT_DIR%" ^
    --validate
if %ERRORLEVEL% NEQ 0 exit /b 1

echo.
echo [SUCCESS] Lightweight Spark SQL pipeline test completed
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
