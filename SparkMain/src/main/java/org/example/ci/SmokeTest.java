package org.example.ci;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CI 烟雾测试 —— Spark SQL 管线端到端验证。
 *
 * <p>通过 spark-submit 执行，不依赖 PySpark，符合 SPARK.md 规范。</p>
 */
public final class SmokeTest {

    private static final Set<String> OUTPUT_TABLES = Set.of(
            "task1_movie_stats",
            "task2_movie_ranking",
            "task3_genre_stats",
            "lb_funnel_overall",
            "lb_time_hourly_behavior",
            "lb_category_efficiency",
            "lb_item_efficiency",
            "lb_user_segment_summary"
    );

    private final Path repoRoot;
    private SparkSession spark;

    public SmokeTest(Path repoRoot) {
        this.repoRoot = repoRoot;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: SmokeTest <repo-root>");
            System.exit(2);
        }
        Path repoRoot = Paths.get(args[0]).toAbsolutePath().normalize();
        if (!Files.isDirectory(repoRoot)) {
            System.err.println("Invalid repo root: " + repoRoot);
            System.exit(2);
        }
        new SmokeTest(repoRoot).run();
    }

    void run() {
        initSpark();
        try {
            verifyDatasetSize();
            runPythonCleaners();
            loadCsvData();
            execSqlDir("prepareData_test");
            execSqlDir("jobSQL_test");
            verifyOutputTables();
            System.out.println("\n[SUCCESS] All Spark SQL smoke tests passed!");
        } finally {
            spark.stop();
        }
    }

    // ---- Spark 初始化 --------------------------------------------------------

    private void initSpark() {
        spark = SparkSession.builder()
                .appName("LuckyAnJun-SmokeTest")
                .master("local[2]")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.warehouse.dir",
                        repoRoot.resolve("spark-warehouse-ci").toString())
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.sql("CREATE DATABASE IF NOT EXISTS bigdata_ana_test");
        spark.sql("USE bigdata_ana_test");
        System.out.println("[INFO] Spark session started (local[2], Hive enabled)");
    }

    // ---- 数据集大小校验 -------------------------------------------------------

    private void verifyDatasetSize() throws IOException {
        Path dir = repoRoot.resolve("dataset_test");
        List<Path> files;
        try (Stream<Path> s = Files.list(dir)) {
            files = s.filter(p -> p.toString().endsWith(".csv"))
                    .sorted()
                    .collect(Collectors.toList());
        }
        if (files.isEmpty()) {
            throw new RuntimeException("dataset_test has no CSV files");
        }
        long totalBytes = 0;
        for (Path f : files) {
            totalBytes += Files.size(f);
        }
        System.out.printf("dataset_test files: %s%n",
                files.stream().map(p -> p.getFileName().toString()).collect(Collectors.toList()));
        System.out.printf("dataset_test total bytes: %d%n", totalBytes);
        if (totalBytes > 64 * 1024) {
            throw new RuntimeException("dataset_test must stay below 64 KiB for CI");
        }
        System.out.println("[PASS] Dataset size check passed");
    }

    // ---- Python 清洗脚本 ------------------------------------------------------

    private void runPythonCleaners() throws IOException {
        Path cleanerDir = repoRoot.resolve("cleanPy_test");
        List<Path> scripts;
        try (Stream<Path> s = Files.list(cleanerDir)) {
            scripts = s.filter(p -> p.toString().endsWith(".py"))
                    .sorted()
                    .collect(Collectors.toList());
        }
        if (scripts.isEmpty()) {
            System.out.println("[INFO] No cleaner scripts found, skipping");
            return;
        }
        Path cleanedDir = repoRoot.resolve("cleanedDataset_test");
        deleteRecursive(cleanedDir);
        Files.createDirectories(cleanedDir);

        for (Path script : scripts) {
            System.out.printf("[INFO] Running cleaner: %s%n", script.getFileName());
            execProcess(repoRoot.toFile(), "python", script.toString());
        }
    }

    // ---- CSV 数据加载 ---------------------------------------------------------

    private void loadCsvData() {
        Path datasetDir = repoRoot.resolve("dataset_test");
        Path cleanedDir = repoRoot.resolve("cleanedDataset_test");

        // MovieLens 测试数据
        StructType moviesSchema = new StructType()
                .add("movieId", DataTypes.IntegerType)
                .add("title", DataTypes.StringType)
                .add("genres", DataTypes.StringType);
        readCsv(datasetDir.resolve("movies.csv"), moviesSchema).createOrReplaceTempView("movies");

        StructType ratingsSchema = new StructType()
                .add("userId", DataTypes.IntegerType)
                .add("movieId", DataTypes.IntegerType)
                .add("rating", DataTypes.DoubleType)
                .add("timestamp", DataTypes.LongType);
        readCsv(datasetDir.resolve("ratings.csv"), ratingsSchema).createOrReplaceTempView("ratings");

        StructType tagsSchema = new StructType()
                .add("userId", DataTypes.IntegerType)
                .add("movieId", DataTypes.IntegerType)
                .add("tag", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType);
        readCsv(datasetDir.resolve("tags.csv"), tagsSchema).createOrReplaceTempView("tags");

        StructType linksSchema = new StructType()
                .add("movieId", DataTypes.IntegerType)
                .add("imdbId", DataTypes.StringType)
                .add("tmdbId", DataTypes.IntegerType);
        readCsv(datasetDir.resolve("links.csv"), linksSchema).createOrReplaceTempView("links");

        // 清洗后的 user_behavior 数据
        Path ub = cleanedDir.resolve("user_behavior.csv");
        if (!Files.isRegularFile(ub)) {
            throw new RuntimeException("Missing cleaned user_behavior: " + ub);
        }
        spark.read().option("header", "true").option("inferSchema", "true")
                .csv(ub.toString())
                .createOrReplaceTempView("user_behavior");

        System.out.println("[INFO] All CSV data loaded as temp views");
    }

    private Dataset<Row> readCsv(Path path, StructType schema) {
        if (!Files.isRegularFile(path)) {
            throw new RuntimeException("Missing test CSV: " + path);
        }
        return spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(path.toString());
    }

    // ---- SQL 执行 ------------------------------------------------------------

    private void execSqlDir(String dirName) {
        Path sqlDir = repoRoot.resolve(dirName);
        if (!Files.isDirectory(sqlDir)) {
            System.out.printf("[WARN] SQL directory not found: %s%n", dirName);
            return;
        }
        System.out.printf("%n[STEP] Running %s SQL...%n", dirName);
        List<Path> sqlFiles;
        try (Stream<Path> s = Files.list(sqlDir)) {
            sqlFiles = s.filter(p -> p.toString().endsWith(".sql"))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list " + dirName, e);
        }
        for (Path sqlFile : sqlFiles) {
            execSqlFile(sqlFile);
        }
    }

    private void execSqlFile(Path sqlFile) {
        System.out.printf("[INFO] Executing: %s%n", sqlFile.getFileName());
        String text;
        try {
            text = Files.readString(sqlFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read SQL file: " + sqlFile, e);
        }
        for (String stmt : splitStatements(text)) {
            String trimmed = stmt.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String upper = trimmed.toUpperCase();
            if (upper.startsWith("SET ") || upper.equals("USE BIGDATA_ANA_TEST")) {
                continue; // CI 已配置
            }
            try {
                spark.sql(trimmed);
            } catch (Exception e) {
                // DROP TABLE IF EXISTS 在表不存在时抛异常，忽略
                if (upper.startsWith("DROP ")) {
                    System.out.printf("  [OK] (table may not exist)%n");
                } else {
                    System.out.printf("  [WARN] %s: %s%n",
                            sqlFile.getFileName(),
                            e.getMessage().substring(0, Math.min(120, e.getMessage().length())));
                }
            }
        }
    }

    // ---- 结果验证 ------------------------------------------------------------

    private void verifyOutputTables() {
        System.out.println("\n[VERIFY] Checking output tables...");
        Dataset<Row> tables = spark.sql("SHOW TABLES");
        Set<String> existing = new HashSet<>();
        for (Row row : tables.collectAsList()) {
            existing.add(row.getString(1));
        }

        boolean allPass = true;
        for (String tableName : OUTPUT_TABLES) {
            if (existing.contains(tableName)) {
                long count = (long) spark.sql("SELECT COUNT(*) FROM " + tableName)
                        .collectAsList().get(0).get(0);
                if (count > 0) {
                    System.out.printf("[PASS] Table %s: %d rows%n", tableName, count);
                } else {
                    System.out.printf("[FAIL] Table %s: 0 rows%n", tableName);
                    allPass = false;
                }
            } else {
                System.out.printf("[FAIL] Table %s: not found%n", tableName);
                allPass = false;
            }
        }
        if (!allPass) {
            throw new RuntimeException("Some expected output tables are missing or empty");
        }
    }

    // ---- 工具方法 ------------------------------------------------------------

    /** 按分号拆分 SQL 语句，跳过注释行。 */
    private static List<String> splitStatements(String sqlText) {
        List<String> result = new ArrayList<>();
        StringBuilder buf = new StringBuilder();
        for (String line : sqlText.split("\n")) {
            String stripped = line.strip();
            if (stripped.startsWith("--")) {
                continue;
            }
            buf.append(line).append('\n');
            if (stripped.endsWith(";")) {
                String stmt = buf.toString().strip();
                if (stmt.endsWith(";")) {
                    stmt = stmt.substring(0, stmt.length() - 1);
                }
                result.add(stmt);
                buf.setLength(0);
            }
        }
        String remaining = buf.toString().strip();
        if (!remaining.isEmpty()) {
            result.add(remaining);
        }
        return result;
    }

    private static void deleteRecursive(Path dir) throws IOException {
        if (Files.isDirectory(dir)) {
            try (Stream<Path> s = Files.walk(dir)) {
                s.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.delete(p); } catch (IOException ignored) {}
                });
            }
        }
    }

    private static void execProcess(File workDir, String... cmd) {
        try {
            Process proc = new ProcessBuilder(cmd)
                    .directory(workDir)
                    .inheritIO()
                    .start();
            int code = proc.waitFor();
            if (code != 0) {
                throw new RuntimeException("Process failed with code " + code + ": " + String.join(" ", cmd));
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Process error: " + String.join(" ", cmd), e);
        }
    }
}
