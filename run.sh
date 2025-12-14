#!/bin/bash

# MySQL LATERAL vs 窗口函数 JMH 基准测试

echo "======================================"
echo "  MySQL JMH 基准测试"
echo "  LATERAL vs ROW_NUMBER vs 子查询"
echo "======================================"

mvn test-compile exec:java
