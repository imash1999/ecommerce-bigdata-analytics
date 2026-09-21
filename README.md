E-Commerce Big Data Analytics Platform

这是一个用于实时和批处理电商平台用户行为数据的全栈分析系统。系统负责收集用户行为事件，在流处理中实时聚合核心指标，并通过离线批处理定期进行 RFM 客户分群分析。

系统架构

数据处理流水线包含以下核心组件：

1. Data Generator (Python): 模拟生成用户行为事件流（商品浏览、加购、下单），并将数据发送至 Kafka 消息队列。
2. Stream Processing (Apache Flink): 实时消费 Kafka 中的数据，按 1 分钟滑动窗口进行聚合计算（统计销售额、订单量、浏览量及加购量），并将结果写入 PostgreSQL。
3. Batch Processing (PySpark): 离线批处理模块，定期执行 RFM（Recency, Frequency, Monetary）分析，对客户群体进行分类并将分群结果存入 PostgreSQL。
4. Data Storage (PostgreSQL): 作为统一的数据仓库，存储聚合后的数据集（realtime_metrics 和 user_rfm_segments）。
5. Visualization (Grafana): 实时展示销售趋势看板以及 RFM 客户分群比例图表。

部署与运行指南

1. 环境准备

运行本项目需要提前安装以下依赖：

* Docker 及 Docker Compose
* Python 3.10+
* psql 命令行客户端（可选，用于数据库查询验证）

2. 启动基础架构

在后台启动所有 Docker 容器服务：

docker compose up -d


检查容器运行状态：

docker compose ps


3. 初始化数据库

在 PostgreSQL 中创建所需的表结构（若启动时未自动创建）：

docker compose exec postgres psql -U postgres -d ecommerce_analytics -c "
CREATE TABLE IF NOT EXISTS realtime_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_views INT DEFAULT 0,
    total_cart_adds INT DEFAULT 0,
    total_buys INT DEFAULT 0,
    total_revenue NUMERIC(10,2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_rfm_segments (
    user_id INT PRIMARY KEY,
    recency INT,
    frequency INT,
    monetary NUMERIC(10,2),
    r_score INT,
    f_score INT,
    m_score INT,
    segment VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"

4. 启动数据生成与处理程序

启动用户行为模拟生成器：

python scripts/generator/main.py


打开新终端，启动 Flink 实时流处理程序以生成实时数据看板：

python scripts/flink/process_stream.py


执行 Spark 脚本计算 RFM 客户分群数据：

python scripts/spark/rfm_analysis.py

Grafana 看板配置

1. 访问 Grafana Web 界面：http://localhost:3000（默认账号密码：admin / admin）。
2. 添加 PostgreSQL 数据源：
* Host: postgres:5432
* Database: ecommerce_analytics
* User: postgres
* Password: postgres
* TLS/SSL Mode: disable


3. 创建仪表盘并添加面板：
* Realtime Revenue & Orders Trend: 选择 Time series 图表，查询 realtime_metrics 表。
* RFM Segments Distribution: 选择 Pie chart 图表，查询 user_rfm_segments 表。

