package org.example.benchmark;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 快速性能对比测试
 * 使用官方文档示例：https://dev.mysql.com/doc/refman/9.5/en/lateral-derived-tables.html
 * 
 * 场景：销售人员表 + 销售记录表，找出每个销售人员的最大销售额及对应客户
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QuickBenchmarkTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:9.0"))
            .withDatabaseName("benchmark")
            .withUsername("bench")
            .withPassword("bench")
            .withCommand("--character-set-server=utf8mb4", "--innodb-buffer-pool-size=256M");

    static Connection connection;

    // 测试参数
    static final int SALESPERSON_COUNT = 500;   // 销售人员数量
    static final int SALES_COUNT = 50_000;      // 销售记录数量
    static final int WARMUP_RUNS = 3;
    static final int BENCHMARK_RUNS = 10;

    @BeforeAll
    static void setup() throws Exception {
        connection = DriverManager.getConnection(
                mysql.getJdbcUrl(),
                mysql.getUsername(),
                mysql.getPassword()
        );

        System.out.println("\n====== 初始化测试数据 ======");
        System.out.println("MySQL版本: " + getMySQLVersion());
        System.out.println("销售人员数: " + SALESPERSON_COUNT);
        System.out.println("销售记录数: " + SALES_COUNT);

        setupTestData();
    }

    private static String getMySQLVersion() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT VERSION()")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static void setupTestData() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS all_sales");
            stmt.execute("DROP TABLE IF EXISTS salesperson");

            // 官方文档示例表结构
            stmt.execute("""
                CREATE TABLE salesperson (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                ) ENGINE=InnoDB
                """);

            stmt.execute("""
                CREATE TABLE all_sales (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    salesperson_id INT NOT NULL,
                    customer_name VARCHAR(100) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    sale_date DATE NOT NULL,
                    INDEX idx_salesperson (salesperson_id),
                    INDEX idx_salesperson_amount (salesperson_id, amount DESC)
                ) ENGINE=InnoDB
                """);

            // 插入销售人员
            String insertSalesperson = "INSERT INTO salesperson (name) VALUES (?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSalesperson)) {
                for (int i = 1; i <= SALESPERSON_COUNT; i++) {
                    pstmt.setString(1, "Salesperson_" + i);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            // 插入销售记录
            String insertSales = "INSERT INTO all_sales (salesperson_id, customer_name, amount, sale_date) VALUES (?, ?, ?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSales)) {
                connection.setAutoCommit(false);
                for (int i = 0; i < SALES_COUNT; i++) {
                    int salespersonId = (i % SALESPERSON_COUNT) + 1;
                    pstmt.setInt(1, salespersonId);
                    pstmt.setString(2, "Customer_" + (int)(Math.random() * 10000));
                    pstmt.setDouble(3, Math.random() * 50000);
                    pstmt.setDate(4, Date.valueOf("2024-01-01"));
                    pstmt.addBatch();
                    if (i % 5000 == 0) {
                        pstmt.executeBatch();
                    }
                }
                pstmt.executeBatch();
                connection.commit();
                connection.setAutoCommit(true);
            }

            stmt.execute("ANALYZE TABLE salesperson");
            stmt.execute("ANALYZE TABLE all_sales");
            System.out.println("数据准备完成！\n");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (connection != null) connection.close();
    }

    // ======================== LATERAL 查询（官方推荐） ========================
    static final String LATERAL_SQL = """
        SELECT
          salesperson.name,
          max_sale.amount,
          max_sale.customer_name
        FROM
          salesperson,
          LATERAL
          (SELECT amount, customer_name
            FROM all_sales
            WHERE all_sales.salesperson_id = salesperson.id
            ORDER BY amount DESC LIMIT 1)
          AS max_sale
        """;

    // ======================== 窗口函数查询 ========================
    static final String WINDOW_SQL = """
        SELECT 
            s.name,
            ranked.amount,
            ranked.customer_name
        FROM salesperson s
        JOIN (
            SELECT 
                salesperson_id,
                amount,
                customer_name,
                ROW_NUMBER() OVER (PARTITION BY salesperson_id ORDER BY amount DESC) AS rn
            FROM all_sales
        ) ranked ON s.id = ranked.salesperson_id AND ranked.rn = 1
        """;

    // ======================== 相关子查询（官方文档反例：低效） ========================
    static final String CORRELATED_SUBQUERY_SQL = """
        SELECT
          salesperson.name,
          (SELECT MAX(amount) AS amount
            FROM all_sales
            WHERE all_sales.salesperson_id = salesperson.id)
          AS amount,
          (SELECT customer_name
            FROM all_sales
            WHERE all_sales.salesperson_id = salesperson.id
            AND all_sales.amount =
                 (SELECT MAX(amount) AS amount
                   FROM all_sales
                   WHERE all_sales.salesperson_id = salesperson.id))
          AS customer_name
        FROM salesperson
        """;

    private long runQuery(String sql) throws SQLException {
        long start = System.nanoTime();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
        }
        return System.nanoTime() - start;
    }

    private BenchmarkResult benchmark(String name, String sql) throws SQLException {
        // 预热
        for (int i = 0; i < WARMUP_RUNS; i++) {
            runQuery(sql);
        }

        // 正式测试
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < BENCHMARK_RUNS; i++) {
            times.add(runQuery(sql));
        }

        double avgMs = times.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
        double minMs = times.stream().mapToLong(Long::longValue).min().orElse(0) / 1_000_000.0;
        double maxMs = times.stream().mapToLong(Long::longValue).max().orElse(0) / 1_000_000.0;

        return new BenchmarkResult(name, avgMs, minMs, maxMs);
    }

    @Test
    @Order(1)
    void testLateral() throws SQLException {
        System.out.println("====== LATERAL 派生表（官方推荐）======");
        BenchmarkResult result = benchmark("LATERAL", LATERAL_SQL);
        result.print();
        printExplain("LATERAL", LATERAL_SQL);
    }

    @Test
    @Order(2)
    void testWindowFunction() throws SQLException {
        System.out.println("\n====== 窗口函数 ROW_NUMBER() ======");
        BenchmarkResult result = benchmark("ROW_NUMBER", WINDOW_SQL);
        result.print();
        printExplain("ROW_NUMBER", WINDOW_SQL);
    }

    @Test
    @Order(3)
    void testCorrelatedSubquery() throws SQLException {
        System.out.println("\n====== 相关子查询（官方反例：低效）======");
        BenchmarkResult result = benchmark("CORRELATED_SUBQUERY", CORRELATED_SUBQUERY_SQL);
        result.print();
        printExplain("CORRELATED_SUBQUERY", CORRELATED_SUBQUERY_SQL);
    }

    @Test
    @Order(4)
    void printComparison() throws SQLException {
        System.out.println("\n");
        System.out.println("+" + "=".repeat(70) + "+");
        System.out.println("|" + centerText("性 能 对 比 汇 总", 70) + "|");
        System.out.println("|" + centerText("官方示例: 每个销售人员的最大销售额及客户", 70) + "|");
        System.out.println("+" + "=".repeat(70) + "+");

        BenchmarkResult lateral = benchmark("LATERAL", LATERAL_SQL);
        BenchmarkResult window = benchmark("ROW_NUMBER", WINDOW_SQL);
        BenchmarkResult correlated = benchmark("CORRELATED_SUBQUERY", CORRELATED_SUBQUERY_SQL);

        System.out.printf("| %-25s 平均: %8.2f ms  范围: [%.2f - %.2f] ms |%n",
                "LATERAL (官方推荐)", lateral.avgMs, lateral.minMs, lateral.maxMs);
        System.out.printf("| %-25s 平均: %8.2f ms  范围: [%.2f - %.2f] ms |%n",
                "ROW_NUMBER (窗口函数)", window.avgMs, window.minMs, window.maxMs);
        System.out.printf("| %-25s 平均: %8.2f ms  范围: [%.2f - %.2f] ms |%n",
                "CORRELATED (官方反例)", correlated.avgMs, correlated.minMs, correlated.maxMs);

        System.out.println("+" + "-".repeat(70) + "+");

        // 找出最快的
        String winner;
        double improvement;
        if (lateral.avgMs <= window.avgMs) {
            winner = "LATERAL";
            improvement = (window.avgMs / lateral.avgMs - 1) * 100;
            System.out.printf("| 🏆 LATERAL 比 ROW_NUMBER 快 %.1f%%%n", improvement);
        } else {
            winner = "ROW_NUMBER";
            improvement = (lateral.avgMs / window.avgMs - 1) * 100;
            System.out.printf("| 🏆 ROW_NUMBER 比 LATERAL 快 %.1f%%%n", improvement);
        }

        double vsCorrelated = (correlated.avgMs / Math.min(lateral.avgMs, window.avgMs) - 1) * 100;
        System.out.printf("| 📉 相关子查询比最优方案慢 %.1f%%（官方不推荐）%n", vsCorrelated);
        System.out.println("+" + "=".repeat(70) + "+");
    }

    private String centerText(String text, int width) {
        int padding = (width - text.length()) / 2;
        return " ".repeat(Math.max(0, padding)) + text + " ".repeat(Math.max(0, width - padding - text.length()));
    }

    private void printExplain(String name, String sql) throws SQLException {
        System.out.println("\n[" + name + " 执行计划]");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("EXPLAIN " + sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                sb.append("  type: ").append(rs.getString("type"));
                sb.append(" | key: ").append(rs.getString("key"));
                sb.append(" | rows: ").append(rs.getString("rows"));
                System.out.println(sb);
            }
        }
    }

    record BenchmarkResult(String name, double avgMs, double minMs, double maxMs) {
        void print() {
            System.out.printf("平均耗时: %.2f ms | 最小: %.2f ms | 最大: %.2f ms%n", avgMs, minMs, maxMs);
        }
    }
}
