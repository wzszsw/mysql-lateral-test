# MySQL LATERAL vs 窗口函数 性能基准测试

使用 Testcontainers + Podman + JMH 对比 MySQL 9.0 中不同 Top-N 查询方式的性能。

## 📊 测试结果

基于 [MySQL 官方文档示例](https://dev.mysql.com/doc/refman/9.5/en/lateral-derived-tables.html)：查询每个销售人员的最大销售额及对应客户。

### 性能对比表 (ms/op，越小越好)

| 销售记录数 | 销售人员数 | 关联子查询 | LATERAL | 窗口函数 | 🏆 最优 |
|-----------|-----------|-----------|---------|---------|--------|
| 10,000 | 100 | **6.50** | 9.40 | 7.12 | 关联子查询 |
| 10,000 | 500 | 10.51 | 10.47 | **7.45** | 窗口函数 |
| 10,000 | 1,000 | 15.04 | 12.25 | **8.20** | 窗口函数 |
| 50,000 | 100 | **25.44** | 44.70 | 33.89 | 关联子查询 |
| 50,000 | 500 | **30.13** | 45.60 | 33.46 | 关联子查询 |
| 50,000 | 1,000 | **35.17** | 47.55 | 34.74 | 关联子查询 |

### 📈 可视化

将 `jmh-result.json` 上传到 [jmh.morethan.io](https://jmh.morethan.io/) 查看交互式图表。

### 🔍 结果分析

**出乎意料的发现：**

1. **关联子查询表现最佳** - 与官方文档描述的"低效"相反，在本测试中关联子查询实际上性能最优
2. **窗口函数稳定性好** - 随数据量增长，性能下降最平缓
3. **LATERAL 表现不佳** - 在大数据量场景下耗时最长

**可能原因：**

- MySQL 9.0 优化器对关联子查询做了深度优化
- 测试场景为 Top-1（`LIMIT 1`），优化器可以提前终止扫描
- 复合索引 `(salesperson_id, amount DESC)` 对关联子查询帮助最大
- LATERAL 需要对每个分组执行依赖派生表，开销较大

### 测试环境

- **MySQL**: 9.0.1 (via Testcontainers)
- **JDK**: OpenJDK 21.0.9
- **Container Runtime**: Podman
- **JMH**: 1.37

## 🚀 快速开始

### 前提条件

- Java 21+
- Maven 3.6+
- Podman (已启动 `podman machine start`)

### 运行测试

```bash
# Windows
run.bat

# Linux/macOS/Git Bash
./run.sh
```

### 测试输出

- `jmh-result.json` - JMH 结果文件，可上传到 [jmh.morethan.io](https://jmh.morethan.io/) 可视化

## 📁 项目结构

```
├── pom.xml
├── run.bat / run.sh                    # 启动脚本
├── jmh-result.json                     # JMH 测试结果
└── src/test/
    ├── java/org/example/benchmark/
    │   ├── MySQLQueryBenchmark.java    # JMH 基准测试
    │   ├── QuickBenchmarkTest.java     # 快速对比测试
    │   └── PodmanConnectionTest.java   # 连接验证测试
    └── resources/
        └── testcontainers.properties   # Podman 连接配置
```

## 📝 SQL 查询对比

### 1. LATERAL 派生表（官方推荐）

```sql
SELECT
  salesperson.name,
  max_sale.amount,
  max_sale.customer_name
FROM
  salesperson,
  LATERAL (
    SELECT amount, customer_name
    FROM all_sales
    WHERE all_sales.salesperson_id = salesperson.id
    ORDER BY amount DESC LIMIT 1
  ) AS max_sale
```

### 2. 窗口函数 ROW_NUMBER()

```sql
SELECT s.name, ranked.amount, ranked.customer_name
FROM salesperson s
JOIN (
    SELECT 
        salesperson_id, amount, customer_name,
        ROW_NUMBER() OVER (PARTITION BY salesperson_id ORDER BY amount DESC) AS rn
    FROM all_sales
) ranked ON s.id = ranked.salesperson_id AND ranked.rn = 1
```

### 3. 关联子查询（官方反例，但实测最快）

```sql
SELECT
  salesperson.name,
  (SELECT MAX(amount) FROM all_sales WHERE salesperson_id = salesperson.id) AS amount,
  (SELECT customer_name FROM all_sales 
   WHERE salesperson_id = salesperson.id 
   AND amount = (SELECT MAX(amount) FROM all_sales WHERE salesperson_id = salesperson.id)
  ) AS customer_name
FROM salesperson
```

## 🎯 结论

| 场景 | 推荐方案 |
|------|---------|
| Top-1 查询 + 有索引 | 关联子查询 |
| Top-N 查询 (N>1) | 窗口函数 |
| 需要缓存中间结果 | LATERAL |
| 代码可读性优先 | LATERAL 或 窗口函数 |

> ⚠️ **注意**: 性能取决于具体场景，建议在实际环境中进行测试。

