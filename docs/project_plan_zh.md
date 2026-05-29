# 项目说明：NYC Taxi Snowflake 数据仓库

## 项目定位

这是一个面向数据仓库、Analytics Engineer、Data Engineer 岗位的端到端作品集项目。核心目标不是简单做报表，而是展示你能把公开数据从原始文件变成可测试、可调度、可解释的分析型数据产品。

## 业务问题

项目围绕纽约黄色出租车运营数据，回答这些问题：

- 哪些上车区域贡献最高收入和订单量？
- 机场相关行程在不同时段的需求如何变化？
- 深夜高需求区域在哪里？
- 不同行政区的平均车费、小费率、行程时长有什么差异？

## 技术亮点

- 使用 Airflow 编排公开数据下载、Snowflake 装载、dbt 转换和测试。
- 使用 Snowflake internal stage 与 `COPY INTO` 装载 parquet 数据。
- 使用 raw、staging、marts 三层建模方式。
- raw 层保留 `VARIANT` 原始记录，staging 层做类型转换和基础清洗，marts 层产出事实表、维表和聚合表。
- 使用 dbt schema tests 检查主键、非空、枚举值和维表关系。
- 提供 demo SQL，便于在面试中现场解释业务指标。

## 2-3 周推进计划

### 第 1 周：跑通链路

- 创建 Snowflake trial account。
- 配置 `.env` 和 Snowflake connection。
- 启动 Airflow。
- 成功跑通 `nyc_taxi_warehouse` DAG。
- 在 Snowflake 中确认 raw、transform、marts 三层表已生成。

### 第 2 周：完善建模和质量

- 增加更多 dbt tests，例如异常金额、异常距离、异常时长检查。
- 将 `fct_taxi_trips` 改造成 incremental model。
- 增加 dbt docs，并截图放进 README。
- 增加更多业务指标，例如机场行程占比、现金/信用卡支付差异、区域收入排名。

### 第 3 周：作品集打磨

- 增加 Streamlit 或 BI dashboard。
- 补充 README 中的运行截图、Snowflake lineage 截图、Airflow DAG 截图。
- 写一段项目复盘：问题、设计选择、数据质量、成本优化、可扩展方向。
- 发布到 GitHub，并在简历中写成一条完整项目经历。

## 简历写法

Built an end-to-end NYC Taxi analytics warehouse on Snowflake using Airflow and dbt. Orchestrated public parquet ingestion into Snowflake raw tables, modeled staging and mart layers with dbt, implemented data quality tests, and produced BI-ready revenue and demand analytics by borough, zone, hour, and airport corridor.

