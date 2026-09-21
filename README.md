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

Grafana
展示实时销售额与用户活跃度的监控仪表盘。

PostgreSQL
存储历史交易数据与最终分析结果的数据集市。

Apache Airflow 与 PySpark
工作流调度器与批处理引擎，每小时定时触发一次全量订单数据的 RFM 计算。

部署与运行

克隆与启动

git clone https://github.com/imash1999/ecommerce-bigdata-analytics.git
cd ecommerce-bigdata-analytics
docker compose up -d --build

强制刷新 Airflow DAG

若 Web 界面未显示 DAG：

docker exec -it airflow_webserver airflow dags reserialize

手动触发任务

docker exec -it airflow_webserver airflow dags trigger rfm_segmentation_dag

访问入口

Airflow: http://localhost:8085
Grafana: http://localhost:3000
Flink: http://localhost:8081
