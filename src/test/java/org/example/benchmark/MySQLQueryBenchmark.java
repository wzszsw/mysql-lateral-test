package org.example.benchmark;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * MySQL查询性能基准测试
 * 使用官方文档示例：https://dev.mysql.com/doc/refman/9.5/en/lateral-derived-tables.html
 * 
 * 场景：销售人员表 + 销售记录表，找出每个销售人员的最大销售额及对应客户
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(0)  // 禁用fork，在同一JVM运行
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
public class MySQLQueryBenchmark {

    private static MySQLContainer<?> mysql;
    private Connection connection;

    // 测试参数
    @Param({"100", "500", "1000"})
    private int salespersonCount;  // 销售人员数量

    @Param({"10000", "50000"})
    private int salesCount;  // 销售记录数量

    @Setup(Level.Trial)
    public void setupContainer() throws Exception {
        if (mysql == null || !mysql.isRunning()) {
            mysql = new MySQLContainer<>(DockerImageName.parse("mysql:9.0"))
                    .withDatabaseName("benchmark")
                    .withUsername("bench")
                    .withPassword("bench")
                    .withCommand(
                            "--character-set-server=utf8mb4",
                            "--innodb-buffer-pool-size=512M"
                    );
            mysql.start();
            System.out.println("MySQL容器启动成功: " + mysql.getJdbcUrl());
        }

        connection = DriverManager.getConnection(
                mysql.getJdbcUrl(),
                mysql.getUsername(),
                mysql.getPassword()
        );

        setupTestData();
    }

    /**
     * 创建官方文档示例的表结构
     */
    private void setupTestData() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // 删除旧表
            stmt.execute("DROP TABLE IF EXISTS all_sales");
            stmt.execute("DROP TABLE IF EXISTS salesperson");

            // 创建销售人员表
            stmt.execute("""
                CREATE TABLE salesperson (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                ) ENGINE=InnoDB
                """);

            // 创建销售记录表
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
                for (int i = 1; i <= salespersonCount; i++) {
                    pstmt.setString(1, "Salesperson_" + i);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            // 插入销售记录
            String insertSales = "INSERT INTO all_sales (salesperson_id, customer_name, amount, sale_date) VALUES (?, ?, ?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSales)) {
                connection.setAutoCommit(false);
                for (int i = 0; i < salesCount; i++) {
                    int salespersonId = (i % salespersonCount) + 1;
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

            System.out.printf("数据准备完成: %d个销售人员, %d条销售记录%n", salespersonCount, salesCount);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * 方法1: LATERAL派生表（官方推荐方式）
     * 高效：一次查询获取最大销售额和客户名
     */
    @Benchmark
    public int lateralQuery() throws SQLException {
        // 官方文档示例：https://dev.mysql.com/doc/refman/9.5/en/lateral-derived-tables.html
        String sql = """
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

        int count = 0;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                count++;
            }
        }
        return count;
    }

    /**
     * 方法2: 窗口函数 ROW_NUMBER()
     */
    @Benchmark
    public int windowFunctionQuery() throws SQLException {
        String sql = """
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

        int count = 0;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                count++;
            }
        }
        return count;
    }

    /**
     * 方法3: 低效的相关子查询（官方文档中的反例）
     * 计算两次最大值，效率低下
     */
    @Benchmark
    public int correlatedSubqueryQuery() throws SQLException {
        // 官方文档中的低效示例
        String sql = """
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

        int count = 0;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                count++;
            }
        }
        return count;
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(MySQLQueryBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.TEXT)
                .build();

        new Runner(opt).run();

        if (mysql != null) {
            mysql.stop();
        }
    }
}
