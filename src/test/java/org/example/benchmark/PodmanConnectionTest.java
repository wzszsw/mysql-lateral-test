package org.example.benchmark;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 测试Testcontainers与Podman的连接
 * 优先验证基础设施是否正常工作
 */
@Testcontainers
public class PodmanConnectionTest {

    // 使用MySQL 9.0（目前最新稳定版）
    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:9.0"))
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test")
            .withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_unicode_ci");

    @Test
    void testConnectionToPodman() throws Exception {
        assertTrue(mysql.isRunning(), "MySQL容器应该正在运行");
        
        System.out.println("=== Podman + Testcontainers 连接成功 ===");
        System.out.println("容器ID: " + mysql.getContainerId());
        System.out.println("JDBC URL: " + mysql.getJdbcUrl());
        System.out.println("Host: " + mysql.getHost());
        System.out.println("Port: " + mysql.getMappedPort(3306));
        
        // 验证数据库连接
        try (Connection conn = DriverManager.getConnection(
                mysql.getJdbcUrl(),
                mysql.getUsername(),
                mysql.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT VERSION()")) {
            
            assertTrue(rs.next());
            String version = rs.getString(1);
            System.out.println("MySQL版本: " + version);
            assertTrue(version.startsWith("9."), "应该是MySQL 9.x版本");
        }
    }

    @Test
    void testLateralSupport() throws Exception {
        // 验证MySQL 8.0.14+支持LATERAL派生表
        try (Connection conn = DriverManager.getConnection(
                mysql.getJdbcUrl(),
                mysql.getUsername(),
                mysql.getPassword());
             Statement stmt = conn.createStatement()) {
            
            // 创建测试表
            stmt.execute("CREATE TABLE IF NOT EXISTS test_lateral (id INT, value INT)");
            stmt.execute("INSERT INTO test_lateral VALUES (1, 100), (1, 200), (2, 300)");
            
            // 测试LATERAL语法
            String lateralQuery = """
                SELECT t.id, l.top_value
                FROM (SELECT DISTINCT id FROM test_lateral) t,
                LATERAL (
                    SELECT value AS top_value 
                    FROM test_lateral 
                    WHERE test_lateral.id = t.id 
                    ORDER BY value DESC 
                    LIMIT 1
                ) l
                """;
            
            try (ResultSet rs = stmt.executeQuery(lateralQuery)) {
                assertTrue(rs.next(), "LATERAL查询应该返回结果");
                System.out.println("LATERAL语法测试通过！");
            }
        }
    }
}

