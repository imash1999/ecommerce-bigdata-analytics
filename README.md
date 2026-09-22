E-Commerce Big Data Analytics Platform

项目简介

处理与分析电商实时及定时数据的工程项目。平台收集用户行为事件，提供实时数据监控，并完成 RFM 客户分群分析。

核心组件

Generator (Python)
持续生成模拟用户事件与交易数据的服务。

Apache Kafka
用于接收与缓冲数据流的消息代理。

Apache Flink
实时流处理服务，直接计算来自 Kafka 的核心业务指标。

MinIO (Data Lake)
对象存储服务，作为 S3 兼容的数据湖存储原始与中间数据。

Grafana
展示实时销售额与用户活跃度的监控仪表盘。

PostgreSQL
存储历史交易数据与最终分析结果的数据集市。

Apache Airflow 与 PySpark
工作流调度器与批处理引擎，每日定时触发从 MinIO 到 PostgreSQL 的 RFM 客户分群计算。

Telegram Alerts
任务异常监控通知系统，在 Airflow Task 执行失败或重试时向 Telegram Bot 实时发送告警。

部署与运行

先决条件

首次构建和运行 Docker 容器之前，需要将所需的 JAR 库下载到 `scripts/spark/` 文件夹：

# 1. AWS / S3 (MinIO) connector
curl -fSL [https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar) -O
curl -fSL [https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar](https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar) -O

# 2. PostgreSQL JDBC driver
curl -fSL [https://jdbc.postgresql.org/download/postgresql-42.6.0.jar](https://jdbc.postgresql.org/download/postgresql-42.6.0.jar) -O

克隆与启动

git clone https://github.com/imash1999/ecommerce-bigdata-analytics.git
cd ecommerce-bigdata-analytics
docker compose up -d --build

强制刷新 Airflow DAG

若 Web 界面未显示 DAG：

docker exec -it airflow_webserver airflow dags reserialize

手动触发任务

docker exec -it airflow_webserver airflow dags trigger rfm_segmentation_daily

访问入口

Airflow: http://localhost:8085
Grafana: http://localhost:3000
Flink: http://localhost:8081
MinIO Console: http://localhost:9001
