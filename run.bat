@echo off
chcp 65001 >nul 2>&1

echo ======================================
echo   MySQL JMH 基准测试
echo   LATERAL vs ROW_NUMBER vs 子查询
echo ======================================

call mvn test-compile exec:java
