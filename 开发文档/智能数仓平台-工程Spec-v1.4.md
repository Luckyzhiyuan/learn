# 智能数仓平台 —— 完整工程 Spec

> 版本：v1.4（工程可实现版，补充治理闭环模块）
> 日期：2026-10-12
> 知识库配套：`智能数仓平台-知识库增强补充设计.md`、`04-知识库体系设计与执行规划.md`
> 前置文档：`智能数仓平台-功能开发文档.md`（v1.2，F1-F13 完整版）
> 技术栈：Python 3.11 + FastAPI + SQLAlchemy 2.0 / Pydantic v2 + Celery + Redis + MySQL 8.0 + Elasticsearch + Spark 3.X + 千问 Qwen 3.8
> 目标读者：后端开发、前端开发、测试、架构师

---

## 目录

1. [工程结构与模块划分](#1-工程结构与模块划分)
2. [数据库 DDL 全集（MySQL 8.0）](#2-数据库-ddl-全集mysql-80)
3. [REST API 规范与出入参定义](#3-rest-api-规范与出入参定义)
4. [千问 Qwen 3.8 LLM 网关交互协议](#4-千问-qwen-38-llm-网关交互协议)
5. [知识库（RAG）增强 NL2SQL](#5-知识库rag增强-nl2sql)
6. [Celery 异步任务设计](#6-celery-异步任务设计)
7. [错误码与异常规范](#7-错误码与异常规范)
8. [缺失模块补充工程 Spec](#8-缺失模块补充工程-spec数据集成--数据安全--数据资产--数据标准--数据开发--数据分析--数据服务--生命周期)
9. [目录与表统计](#9-目录与表统计)

---

## 1. 工程结构与模块划分

### 1.1 后端目录结构（FastAPI）

```
smart-dw-platform/
├── app/
│   ├── main.py                  # FastAPI 入口，注册路由/中间件
│   ├── config.py                # Pydantic Settings 配置加载
│   ├── deps.py                  # 依赖注入（DB session、当前用户、权限）
│   ├── core/
│   │   ├── security.py          # JWT/Token 认证、密码
│   │   ├── exceptions.py        # 统一异常类
│   │   └── response.py          # 统一返回包装 {code,message,data}
│   ├── db/
│   │   ├── base.py              # Declarative Base
│   │   ├── session.py           # 引擎与会话
│   │   └── models/              # SQLAlchemy ORM 模型（对应 §2 DDL）
│   ├── routers/                 # 路由层（按模块拆分）
│   │   ├── metadata.py          # 元数据
│   │   ├── modeling.py          # 智能建模
│   │   ├── sql_assistant.py     # SQL 助手
│   │   ├── quality.py           # 数据质量
│   │   ├── search.py            # 检索/问答/NL2SQL
│   │   ├── lineage.py           # 血缘
│   │   ├── schedule.py          # 调度
│   │   ├── notify.py            # 平台内通知
│   │   ├── kb.py                # 知识库管理（§5）
│   │   ├── ds.py                 # 数据集成（§8.1）
│   │   ├── sec.py                # 数据安全（§8.2）
│   │   ├── assets.py             # 数据资产（§8.3）
│   │   ├── std.py                # 数据标准（§8.4）
│   │   ├── dev.py                # 数据开发（§8.6）
│   │   ├── ana.py                # 数据分析（§8.7）
│   │   └── svc.py                # 数据服务（§8.8）
│   │   └── llm.py               # LLM 网关配置
│   ├── schemas/                 # Pydantic 请求/响应模型（对应 §3）
│   ├── services/                # 业务逻辑层
│   │   ├── metadata_ingest.py   # 元数据采集（文件+直连双模式）
│   │   ├── lineage_builder.py   # 血缘自建（SQL 解析）
│   │   ├── sql_parser.py        # SQL 解析（表/字段/特征提取）
│   │   ├── optimizer.py         # SQL 优化规则引擎
│   │   ├── evaluator.py         # 优化效果评估
│   │   ├── quality_service.py   # 质量校验
│   │   ├── llm_gateway.py       # 千问 LLM 网关封装（§4）
│   │   ├── kb_service.py        # 知识库 CRUD + 多路召回检索（§5）
│   │   ├── rag.py               # RAG 检索融合 + 提示词组装（§5）
│   │   ├── nl2sql_service.py    # NL2SQL 编排（schema+Kb注入+校验+降级）
│   │   ├── ds_service.py        # 数据集成（§8.1）
│   │   ├── sec_service.py       # 脱敏/权限/审计（§8.2）
│   │   ├── asset_service.py     # 资产/标签/收藏（§8.3）
│   │   ├── std_service.py       # 标准/落标（§8.4）
│   │   ├── dev_service.py       # 脚本/发布（§8.6）
│   │   ├── ana_service.py       # 看板/图表/推荐（§8.7）
│   │   └── svc_service.py       # 数据API/监控（§8.8）
│   ├── tasks/                   # Celery 任务
│   │   ├── metadata_sync.py
│   │   ├── quality_run.py
│   │   ├── lineage_build.py
│   │   └── embedding_index.py   # 知识库向量重建（§5）
│   └── utils/
├── tests/
├── alembic/                     # 数据库迁移
├── docker/                      # 部署编排
├── pyproject.toml / requirements.txt
└── .env.example
```

### 1.2 配置项（.env / config.py）

```env
# 数据库
DB_URL=mysql+pymysql://user:pass@host:3306/smart_dw?charset=utf8mb4
# Redis（Celery broker + 结果缓存）
REDIS_URL=redis://localhost:6379/0
# ES
ES_URL=http://localhost:9200
ES_INDEX_META=meta_index
# Spark
SPARK_MASTER=yarn
SPARK_SUBMIT_CONF=...
# 千问 LLM（见 §4）
QWEN_API_URL=https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions
QWEN_MODEL=qwen3-8b        # 平台提供的模型名
QWEN_API_KEY=sk-xxxx        # 仅存储于服务端，前端不回传
QWEN_TIMEOUT=30
QWEN_MAX_RETRY=2
QWEN_FALLBACK_ENABLED=true  # LLM 不可用时是否降级为规则生成
```

---

## 2. 数据库 DDL 全集（MySQL 8.0）

> 以下 DDL 可直接在 MySQL 8.0 执行。统一字符集 utf8mb4，引擎 InnoDB。
> 表前缀：`meta_` 元数据 / `quality_` 质量 / `task_` 调度 / `qa_` 检索问答 / `notify_` 通知 / `sys_` 系统。

### 2.1 meta_database（库目录）

```sql
CREATE TABLE meta_database (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  db_name     VARCHAR(128)  NOT NULL COMMENT '库名',
  layer       VARCHAR(32)   NOT NULL DEFAULT '' COMMENT '分层 dim/dwd/ads/etl/tmp/rpt',
  owner       VARCHAR(64)   NOT NULL DEFAULT '' COMMENT '负责人工号',
  location    VARCHAR(512)  NOT NULL DEFAULT '' COMMENT 'HDFS 路径',
  remark      VARCHAR(255)  NOT NULL DEFAULT '' COMMENT '说明',
  created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_db_name (db_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数仓库目录';
```

### 2.2 meta_table（表）

```sql
CREATE TABLE meta_table (
  id                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  db_id             BIGINT UNSIGNED  NOT NULL COMMENT '所属库 FK→meta_database.id',
  table_name        VARCHAR(128)     NOT NULL COMMENT '表名',
  comment           VARCHAR(512)     NOT NULL DEFAULT '' COMMENT '表注释',
  table_type        VARCHAR(32)      NOT NULL DEFAULT 'MANAGED' COMMENT 'MANAGED/EXTERNAL',
  storage_format    VARCHAR(32)      NOT NULL DEFAULT 'ORC' COMMENT '存储格式',
  compress          VARCHAR(32)      NOT NULL DEFAULT 'SNAPPY' COMMENT '压缩格式',
  is_partitioned    TINYINT          NOT NULL DEFAULT 0 COMMENT '是否分区表 0/1',
  row_count         BIGINT           NOT NULL DEFAULT 0 COMMENT '行数',
  table_size_bytes  BIGINT           NOT NULL DEFAULT 0 COMMENT '表大小(字节)',
  file_count        BIGINT           NOT NULL DEFAULT 0 COMMENT '文件数',
  little_file_count BIGINT           NOT NULL DEFAULT 0 COMMENT '小文件数',
  owner             VARCHAR(64)      NOT NULL DEFAULT '' COMMENT '所有者',
  location          VARCHAR(512)     NOT NULL DEFAULT '' COMMENT 'HDFS 路径',
  source_mode       VARCHAR(16)      NOT NULL DEFAULT 'file' COMMENT '采集来源 file/hive',
  created_at        DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_db_table (db_id, table_name),
  KEY idx_owner (owner)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数仓表';
```

### 2.3 meta_column（字段）

```sql
CREATE TABLE meta_column (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  table_id     BIGINT UNSIGNED NOT NULL COMMENT '所属表 FK→meta_table.id',
  column_name  VARCHAR(128)    NOT NULL COMMENT '字段名',
  column_type  VARCHAR(128)    NOT NULL DEFAULT '' COMMENT '字段类型',
  comment      VARCHAR(512)    NOT NULL DEFAULT '' COMMENT '字段注释',
  is_partition TINYINT         NOT NULL DEFAULT 0 COMMENT '是否分区字段 0/1',
  is_primary   TINYINT         NOT NULL DEFAULT 0 COMMENT '是否主键 0/1',
  ordinal      INT             NOT NULL DEFAULT 0 COMMENT '字段顺序',
  created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_table_column (table_id, column_name),
  KEY idx_comment (comment(64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='表字段';
```

### 2.4 meta_partition（分区）

```sql
CREATE TABLE meta_partition (
  id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  table_id        BIGINT UNSIGNED NOT NULL COMMENT '所属表 FK→meta_table.id',
  partition_key   VARCHAR(64)     NOT NULL COMMENT '分区键 day/month/...',
  partition_value VARCHAR(64)     NOT NULL COMMENT '分区值',
  size_bytes      BIGINT          NOT NULL DEFAULT 0 COMMENT '分区大小',
  row_count       BIGINT          NOT NULL DEFAULT 0 COMMENT '分区行数',
  file_count      BIGINT          NOT NULL DEFAULT 0 COMMENT '分区文件数',
  updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_table_partition (table_id, partition_key, partition_value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='表分区';
```

### 2.5 meta_lineage（血缘 - 平台自建表级）

```sql
CREATE TABLE meta_lineage (
  id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  src_table_id    BIGINT UNSIGNED NOT NULL COMMENT '上游表 FK→meta_table.id',
  dst_table_id    BIGINT UNSIGNED NOT NULL COMMENT '下游表 FK→meta_table.id',
  edge_tag        VARCHAR(32)     NOT NULL DEFAULT 'sql_parse' COMMENT '边类型',
  task_id         BIGINT          NULL COMMENT '生成/使用任务ID(可空)',
  project_id      BIGINT          NULL COMMENT '项目ID',
  job_type        VARCHAR(32)     NOT NULL DEFAULT '' COMMENT '任务类型',
  owner           VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '负责人',
  schedule_status VARCHAR(32)     NOT NULL DEFAULT '' COMMENT '调度状态',
  schedule_time   DATETIME        NULL COMMENT '最后执行时间',
  source          VARCHAR(16)     NOT NULL DEFAULT 'offline' COMMENT '来源 offline回填/incremental增量',
  field_level     JSON            NULL COMMENT '字段级血缘明细(预留,本期不填)',
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_edge (src_table_id, dst_table_id, task_id),
  KEY idx_dst (dst_table_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='表级血缘';
```

### 2.6 meta_snapshot（元数据快照）

```sql
CREATE TABLE meta_snapshot (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  table_id      BIGINT UNSIGNED NOT NULL COMMENT 'FK→meta_table.id',
  snapshot_time DATETIME        NOT NULL COMMENT '快照时间',
  snapshot_json JSON            NOT NULL COMMENT '字段/分区快照',
  diff_json     JSON            NULL COMMENT '相较上次的变更',
  KEY idx_table_time (table_id, snapshot_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='元数据快照';
```

### 2.7 quality_rule（质量规则）

```sql
CREATE TABLE quality_rule (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  rule_name    VARCHAR(128)  NOT NULL COMMENT '规则名',
  rule_type    VARCHAR(32)   NOT NULL COMMENT '非空/唯一/波动/值域/枚举/自定义SQL',
  table_id     BIGINT UNSIGNED NOT NULL COMMENT '目标表 FK→meta_table.id',
  column_id    BIGINT UNSIGNED NULL COMMENT '目标字段(可选)',
  params       JSON          NULL COMMENT '规则参数',
  sql          TEXT          NULL COMMENT '自定义SQL',
  threshold    DECIMAL(20,4) NULL COMMENT '阈值',
  enabled      TINYINT       NOT NULL DEFAULT 1 COMMENT '是否启用 0/1',
  created_by   VARCHAR(64)   NOT NULL DEFAULT '' COMMENT '创建人工号',
  updated_by   VARCHAR(64)   NOT NULL DEFAULT '' COMMENT '更新人工号',
  created_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_table (table_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='质量规则';
```

### 2.8 quality_rule_result（质量执行结果）

```sql
CREATE TABLE quality_rule_result (
  id                    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  rule_id               BIGINT UNSIGNED NOT NULL COMMENT 'FK→quality_rule.id',
  execute_time          DATETIME        NOT NULL COMMENT '执行时间',
  execute_instance_id   VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '执行实例ID',
  pass                  TINYINT         NOT NULL DEFAULT 1 COMMENT '是否通过 0/1',
  actual_value          DECIMAL(20,4)   NULL COMMENT '实际值',
  detail                TEXT            NULL COMMENT '详情/失败原因',
  notified              TINYINT         NOT NULL DEFAULT 0 COMMENT '是否已发通知',
  KEY idx_rule_time (rule_id, execute_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='质量执行结果';
```

### 2.9 task_instance（任务实例）

```sql
CREATE TABLE task_instance (
  id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  task_id        BIGINT         NOT NULL COMMENT '外部平台任务ID',
  instance_id    VARCHAR(64)    NOT NULL COMMENT '平台实例ID',
  status         VARCHAR(32)    NOT NULL DEFAULT 'SUBMITTED' COMMENT 'SUBMITTED/RUNNING/SUCCESS/FAILED/KILLED',
  app_id         VARCHAR(128)   NOT NULL DEFAULT '' COMMENT 'Spark Application ID',
  history_url    VARCHAR(512)   NOT NULL DEFAULT '' COMMENT 'Spark History URL',
  start_time     DATETIME       NULL,
  end_time       DATETIME       NULL,
  log_text       MEDIUMTEXT     NULL COMMENT '执行日志(截取)',
  created_at     DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_task_status (task_id, status),
  KEY idx_instance (instance_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务实例';
```

### 2.10 qa_conversation（NL2SQL / 口径问答会话 - 新增）

```sql
CREATE TABLE qa_conversation (
  id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_id        VARCHAR(64)    NOT NULL COMMENT '用户工号',
  query_type     VARCHAR(16)    NOT NULL COMMENT 'nl2sql/qa',
  question       TEXT           NOT NULL COMMENT '用户原始问题',
  generated_sql  TEXT           NULL COMMENT '生成的候选SQL',
  confirmed      TINYINT        NOT NULL DEFAULT 0 COMMENT '用户是否确认 0/1',
  executed       TINYINT        NOT NULL DEFAULT 0 COMMENT '是否已执行',
  llm_provider   VARCHAR(16)    NOT NULL DEFAULT 'qwen3-8b' COMMENT '使用的LLM模型',
  fallback_used  TINYINT        NOT NULL DEFAULT 0 COMMENT '是否走降级规则生成',
  result_summary TEXT           NULL COMMENT '执行结果摘要',
  feedback       TINYINT        NULL COMMENT '用户反馈 1赞/-1踩/0无',
  created_at     DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_user_time (user_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='检索问答会话';
```

### 2.11 notify_message（平台内通知 - 新增）

```sql
CREATE TABLE notify_message (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_id     VARCHAR(64)    NOT NULL COMMENT '接收人工号',
  title       VARCHAR(255)   NOT NULL COMMENT '标题',
  content     TEXT           NOT NULL COMMENT '内容',
  biz_type    VARCHAR(32)    NOT NULL DEFAULT 'quality' COMMENT 'quality/schedule/system',
  ref_id      BIGINT         NULL COMMENT '关联业务ID(如质量结果ID)',
  read_flag   TINYINT        NOT NULL DEFAULT 0 COMMENT '是否已读 0/1',
  created_at  DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_user_read (user_id, read_flag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='平台内通知';
```

### 2.12 llm_config（LLM 网关配置 - 新增）

```sql
CREATE TABLE llm_config (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  provider     VARCHAR(32)  NOT NULL DEFAULT 'qwen' COMMENT 'provider',
  model_name   VARCHAR(64)  NOT NULL DEFAULT 'qwen3-8b' COMMENT '模型名',
  api_url      VARCHAR(512) NOT NULL COMMENT 'Endpoint',
  api_key_enc  VARCHAR(512) NOT NULL COMMENT '加密后的API Key（AES-GCM，密钥存服务端环境变量）',
  enabled      TINYINT      NOT NULL DEFAULT 1 COMMENT '是否启用',
  fallback     TINYINT      NOT NULL DEFAULT 1 COMMENT '是否允许降级到规则生成',
  temperature  DECIMAL(3,2) NOT NULL DEFAULT 0.00 COMMENT '生成温度',
  max_tokens   INT          NOT NULL DEFAULT 2048,
  timeout_sec  INT          NOT NULL DEFAULT 30,
  updated_by   VARCHAR(64)  NOT NULL DEFAULT '',
  updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='LLM网关配置';
```

### 2.13 sys_user（用户 - 最小化）

```sql
CREATE TABLE sys_user (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  employee_no   VARCHAR(64)  NOT NULL COMMENT '工号',
  user_name     VARCHAR(128) NOT NULL COMMENT '姓名',
  role          VARCHAR(32)  NOT NULL DEFAULT 'dev' COMMENT 'admin/dev/analyst/viewer',
  created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_emp (employee_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户';
```

### 2.14 ES 索引（检索）

```json
PUT /meta_index
{
  "mappings": {
    "properties": {
      "entity_type": { "type": "keyword" },   // table/column/database
      "name":        { "type": "text", "analyzer": "ik_max_word" },
      "name_ngram":  { "type": "text", "analyzer": "ngram_analyzer" },  // 前缀模糊
      "comment":     { "type": "text", "analyzer": "ik_max_word" },
      "db_name":     { "type": "keyword" },
      "table_id":    { "type": "long" },
      "column_id":   { "type": "long" },
      "boost":       { "type": "float" }       // 权重：表名>字段名>注释
    }
  },
  "settings": {
    "analysis": {
      "analyzer": {
        "ngram_analyzer": {
          "tokenizer": "ngram_tokenizer"
        }
      },
      "tokenizer": {
        "ngram_tokenizer": {
          "type": "ngram",
          "min_gram": 2,
          "max_gram": 4,
          "token_chars": ["letter", "digit"]
        }
      }
    }
  }
}
```

---

## 3. REST API 规范与出入参定义

### 3.1 通用约定

```json
// 统一返回
{
  "code": 0,
  "message": "success",
  "data": { }
}
```

- 前缀：`/api/v1`
- 认证：请求头 `Authorization: Bearer <token>`；由安全模块解析当前用户 `employee_no`。
- 分页：`page`(默认1)、`page_size`(默认20，上限100)，返回 `{ list, total, page, page_size }`。
- 全部 Pydantic v2 模型校验。

### 3.2 元数据模块

#### GET /api/v1/databases

请求参数：`parent_id`(可选) / `keyword`(可选，模糊匹配库名)

响应 `data`：

```json
{
  "list": [
    { "id": 1, "db_name": "mid_hotel", "layer": "dwd",
      "owner": "1207799", "location": "/user/hotel/mid", "remark": "明细层" }
  ],
  "total": 1, "page": 1, "page_size": 20
}
```

#### GET /api/v1/databases/{db}/tables

请求参数：`keyword` / `page` / `page_size`

响应 `data.item`：

```json
{
  "id": 101, "table_name": "hotel_flow_newubt_di",
  "comment": "ubt日志离线表", "db_name": "mid_hotel",
  "table_type": "MANAGED", "storage_format": "ORC", "compress": "SNAPPY",
  "is_partitioned": 1, "row_count": 12000000,
  "table_size_bytes": 684258068, "file_count": 512, "little_file_count": 4,
  "owner": "1207799"
}
```

#### GET /api/v1/tables/{id}

响应 `data`（表详情聚合）：

```json
{
  "id": 101,
  "db_name": "mid_hotel", "table_name": "hotel_flow_newubt_di", "comment": "ubt日志离线表",
  "table_type": "MANAGED", "storage_format": "ORC", "compress": "SNAPPY",
  "is_partitioned": 1, "row_count": 12000000, "table_size_bytes": 684258068,
  "file_count": 512, "little_file_count": 4, "owner": "1207799",
  "location": "viewfs://...", "source_mode": "hive",
  "columns": [
    { "id": 1001, "column_name": "userkey", "column_type": "string",
      "comment": "统一用户标识", "is_partition": 0, "ordinal": 1 }
  ],
  "partitions": [
    { "id": 5001, "partition_key": "dt", "partition_value": "20261008",
      "size_bytes": 1024, "row_count": 100, "file_count": 2 }
  ],
  "created_at": "2026-10-01 00:00:00"
}
```

#### GET /api/v1/tables/{id}/lineage

响应 `data`：

```json
{
  "nodes": [
    { "id": 101, "label": "mid_hotel.hotel_flow_newubt_di", "type": "table" },
    { "id": 55, "label": "applydata_hotel.base_mvt2ubt", "type": "table" }
  ],
  "edges": [
    { "src_id": 55, "dst_id": 101,
      "task_id": 72367, "project_id": 100, "job_type": "spark_sql",
      "owner": "1207799", "schedule_status": "SUCCESS", "schedule_time": "2026-10-08 00:30:00" }
  ]
}
```

#### POST /api/v1/metadata/sync

请求 `data`：

```json
{ "mode": "all", "scope": ["mid_hotel"], "source": "hive" }
```

嵌套说明：`source` 取值 `file`（本地建表文件）/ `hive`（Metastore直连）。`mode=all|incremental` 异步触发，返回 `data.job_id`（Celery任务ID）。

响应 `data`：

```json
{ "job_id": "c1b2a3d4-...", "status": "SUBMITTED" }
```

### 3.3 智能建模模块

#### POST /api/v1/modeling/validate

请求 `data`：

```json
{
  "db_name": "mid_hotel",
  "table_name": "hotel_mem_user_info_di",
  "table_comment": "国内酒店用户信息表",
  "columns": [
    { "name": "id", "type": "bigint", "comment": "主键id", "is_primary": true },
    { "name": "user_name", "type": "string", "comment": "用户名称" }
  ],
  "partition_by": "day",
  "storage_format": "ORC",
  "compress": "SNAPPY"
}
```

响应 `data`（规范校验结果）：

```json
{
  "valid": false,
  "violations": [
    { "rule": "命名规范", "level": "error",
      "message": "表名 hotel_mem_user_info_di 缺少项目前缀(国内酒店应含 hotel)" }
  ],
  "warnings": [
    { "rule": "注释规范", "message": "字段 business_time 缺少 COMMENT" }
  ]
}
```

#### POST /api/v1/modeling/generate

请求同上；响应 `data`：

```json
{
  "sql": "CREATE TABLE IF NOT EXISTS mid_hotel.hotel_mem_user_info_di (...) PARTITIONED BY (day STRING COMMENT '日期yyyyMMdd') STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');"
}
```

### 3.4 SQL 智能开发助手模块

#### POST /api/v1/sql/parse

请求 `data`：`{ "sql": "SELECT ..." }`

响应 `data`：

```json
{
  "tables": [
    { "db": "mid_hotel", "table": "hotel_flow_newubt_di", "table_id": 101,
      "is_partitioned": 1, "size_bytes": 684258068, "storage_format": "ORC" }
  ],
  "columns": ["userkey", "memberid"],
  "features": {
    "select_star": false, "partition_filter": true,
    "partition_func_on_key": false, "has_join": true,
    "join_tables": [103]  // 小表id，用于判断广播
  }
}
```

#### POST /api/v1/sql/optimize

请求 `data`：`{ "sql": "SELECT ...", "task_id": 72367 }`

响应 `data`：

```json
{
  "diagnosis": [
    { "issue": "CTE表重复扫描", "severity": "high",
      "detail": "Stage-1/2/3 重复读取 applydata_hotel.base_mvt2ubt" }
  ],
  "suggestions": [
    { "rule": "cache_cte", "level": "must_do",
      "message": "将大表 CTE 物化缓存，避免重复扫描" }
  ]
}
```

#### POST /api/v1/sql/rewrite

响应 `data`：

```json
{
  "original_sql": "SELECT ...",
  "rewritten_sql": "SELECT ... /* 优化后 */",
  "diff": "--- original +++ rewritten @@ ...",   // unified diff
  "changes": [
    { "rule": "column_pruning", "desc": "裁剪未使用字段" }
  ]
}
```

#### POST /api/v1/sql/evaluate

响应 `data`（五维评估）：

```json
{
  "before": { "duration_sec": 1500, "input_bytes": 2861000000000,
              "shuffle_read_bytes": 500000000000, "stage_count": 8, "task_count": 111623 },
  "after":  { "duration_sec": 600,  "input_bytes": 700000000000,
              "shuffle_read_bytes": 120000000000, "stage_count": 5, "task_count": 40000 },
  "report": {
    "dims": [
      { "dim": "执行性能", "before": "25分钟", "after": "10分钟", "improve": "60%" },
      { "dim": "资源消耗", "before": "2.6TiB", "after": "0.7TiB", "improve": "73%" },
      { "dim": "稳定性", "before": "111623", "after": "40000", "improve": "64%" },
      { "dim": "成本效率", "before": "120", "after": "50", "improve": "58%" },
      { "dim": "可维护性", "before": "8", "after": "9", "improve": "12%" }
    ],
    "verdict": "优秀", "upgrade": true
  }
}
```

### 3.5 数据质量模块

#### CRUD /api/v1/quality/rules

POST 创建规则请求 `data`：

```json
{
  "rule_name": "订单表行数波动监测",
  "rule_type": "波动",
  "table_id": 101,
  "column_id": null,
  "params": { "period": "day", "compare": "环比", "window": 7 },
  "sql": null,
  "threshold": 0.2,
  "enabled": true
}
```

响应 `data`：`{ "id": 1, "rule_name": "...", ... }`

GET 列表响应 `data`：`{ "list": [规则对象], "total": n, "page": 1, "page_size": 20 }`

#### POST /api/v1/quality/rules/{id}/run

响应 `data`：

```json
{ "job_id": "c1b2a3d4-...", "rule_id": 1, "status": "SUBMITTED" }
```

#### GET /api/v1/quality/reports

请求参数：`table_id` / `rule_id` / `start_time` / `end_time` / `page`

响应 `data`：

```json
{
  "list": [
    { "id": 100, "rule_id": 1, "rule_name": "行数波动监测", "table_name": "hotel_flow_newubt_di",
      "execute_time": "2026-10-08 01:00:00", "pass": false,
      "actual_value": 0.35, "detail": "当日行数较7日均值波动+35%，超阈值20%",
      "notified": true }
  ],
  "total": 1, "page": 1, "page_size": 20
}
```

### 3.6 检索 / 问答 / NL2SQL 模块

#### POST /api/v1/search

请求 `data`：`{ "keyword": "ubt日志", "entity_type": "table,column", "page": 1, "page_size": 20 }`

响应 `data`：

```json
{
  "list": [
    { "entity_type": "table", "name": "hotel_flow_newubt_di",
      "comment": "ubt日志离线表", "db_name": "mid_hotel",
      "table_id": 101, "score": 0.95 }
  ],
  "suggest": {
    "related_tables": ["hotel_flow_newubt_gnhotel_di"],
    "related_fields": ["userkey", "platid"],
    "common_sql": "SELECT COUNT(*) FROM mid_hotel.hotel_flow_newubt_di WHERE dt='20261008'"
  },
  "total": 1, "page": 1, "page_size": 20
}
```

#### POST /api/v1/nl2sql/generate

请求 `data`：

```json
{
  "question": "统计昨天国内酒店项目的UV",
  "db_hint": "mid_hotel",
  "n": 1
}
```

响应 `data`（候选 SQL + 元数据溯源）：

```json
{
  "candidates": [
    {
      "sql": "SELECT COUNT(DISTINCT userkey) AS uv\nFROM mid_hotel.hotel_flow_newubt_gnhotel_di\nWHERE dt = DATE_FORMAT(DATE_SUB(CURRENT_DATE, 1), 'yyyyMMdd')",
      "sql_id": "f3a2...",
      "confidence": 0.87,
      "tables_used": [{ "id": 102, "table": "mid_hotel.hotel_flow_newubt_gnhotel_di" }],
      "is_read_only": true
    }
  ],
  "llm_used": "qwen3-8b", "fallback_used": false
}
```

> ⚠️ 关键安全点：`is_read_only` 由后端只读校验器（正则 + 语法解析）生成，前端不可信。非只读候选直接过滤。

#### POST /api/v1/nl2sql/execute

请求 `data`：

```json
{ "sql_id": "f3a2...", "sql": "SELECT ...", "limit": 100 }
```

后端执行前再次做只读校验（非 SELECT 拒绝）。响应 `data`：

```json
{
  "result": {
    "columns": ["uv"],
    "rows": [["123456"]],
    "row_count": 1,
    "cost_seconds": 2.1
  },
  "query_key": "qk-xxx"
}
```

#### POST /api/v1/nl2sql/{id}/feedback

请求 `data`：`{ "feedback": 1 }`  // 1赞 / -1踩，用于闭环改进。

#### POST /api/v1/qa/ask

请求 `data`：`{ "question": "GMV 是怎么算的？" }`

响应 `data`：

```json
{
  "answer": "GMV 口径：国内酒店各端已支付订单金额之和，来源表 mid_hotel.dw_hotel_order_wide_da...",
  "sources": [
    { "type": "table", "id": 101, "name": "dw_hotel_order_wide_da", "reason": "口径定义来源表" },
    { "type": "doc", "id": 5, "name": "酒店GMV口径说明", "url": "https://toca.17u.cn/wiki?fid=xxx" }
  ],
  "llm_used": "qwen3-8b"
}
```

### 3.7 调度模块

#### GET /api/v1/tasks

请求参数：`keyword` / `status` / `page`

响应 `data`：`{ "list": [{ "task_id": 72367, "task_name": "...", "status": "SUCCESS", "owner": "1207799", ... }], "total": n, "page": 1, "page_size": 20 }`

#### GET /api/v1/tasks/{id}/instances

响应 `data`：

```json
{
  "list": [
    { "instance_id": "72367_2026100800", "status": "SUCCESS",
      "app_id": "application_1700000000000_0001",
      "history_url": "http://sparksql.bds.17usoft.com/history/application_.../",
      "start_time": "2026-10-08 00:30:00", "end_time": "2026-10-08 00:55:00" }
  ]
}
```

#### GET /api/v1/tasks/{id}/logs

响应 `data`：`{ "instance_id": "...", "log": "Starting Job - 0 ...", "history_url": "..." }`

### 3.8 通知模块（平台内）

#### GET /api/v1/notify/messages

请求参数：`read_flag` / `page`

响应 `data`：

```json
{
  "list": [
    { "id": 1, "title": "质量校验失败", "content": "hotel_flow_newubt_di 行数波动+35%",
      "biz_type": "quality", "ref_id": 100, "read_flag": 0, "created_at": "2026-10-08 01:00:00" }
  ],
  "unread_count": 3, "total": 1, "page": 1, "page_size": 20
}
```

#### PUT /api/v1/notify/messages/{id}/read

请求 `data`：`{}`；响应 `data`：`{ "id": 1, "read_flag": 1 }`

### 3.9 LLM 网关配置模块（新增）

#### GET /api/v1/llm/config

响应 `data`（API Key 脱敏返回）：

```json
{
  "provider": "qwen", "model_name": "qwen3-8b",
  "api_url": "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
  "api_key_masked": "sk-****abc", "enabled": true,
  "fallback": true, "temperature": 0.0, "max_tokens": 2048, "timeout_sec": 30
}
```

#### PUT /api/v1/llm/config

请求 `data`：

```json
{
  "provider": "qwen", "model_name": "qwen3-8b",
  "api_url": "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
  "api_key": "sk-xxxx",   // 明文仅在校验后加密入库，不落日志
  "enabled": true, "fallback": true, "temperature": 0.0,
  "max_tokens": 2048, "timeout_sec": 30
}
```

响应 `data`：`{ "ok": true, "test_result": { "latency_ms": 850, "status": "success" } }`（保存前会做一次连通性测试）。

---

## 4. 千问 Qwen 3.8 LLM 网关交互协议

### 4.1 网关职责（app/services/llm_gateway.py）

- 统一封装千问 HTTP API（OpenAI 兼容格式）。
- 管理配置（生效模型、Endpoint、Key 加密存储、超时/重试）。
- 注入 Schema 约束、few-shot 提示词。
- 结果解析为结构化 JSON（候选SQL + 溯源 + 置信度）。
- 异常处理与降级（LLM 不可用 → 规则/模板生成）。

### 4.2 请求协议（OpenAI 兼容 chat/completions）

```
POST {QWEN_API_URL}
Authorization: Bearer {QWEN_API_KEY}
Content-Type: application/json
```

请求体：

```json
{
  "model": "qwen3-8b",
  "temperature": 0.0,
  "max_tokens": 2048,
  "messages": [
    { "role": "system",
      "content": "你是数仓SQL专家。根据给定的表结构，把用户中文问题转成Hive/Spark SQL。严格只输出JSON，不要多余文字。" },
    { "role": "user",
      "content": "【表结构】\nmid_hotel.hotel_flow_newubt_gnhotel_di(国内酒店): dt(string,分区,yyyyMMdd), userkey(string,统一用户标识), platid(int, 平台码)\n\n【规则】\n1. 只允许SELECT，禁止DELETE/UPDATE/INSERT/DDL。\n2. 分区字段必须过滤并裁剪。\n3. 输出格式: {\"sql\": \"...\", \"tables_used\": [...], \"confidence\": 0.0-1.0}\n\n【问题】统计昨天国内酒店项目的UV" }
  ]
}
```

### 4.3 响应解析

正常响应 `data`：

```json
{
  "sql": "SELECT COUNT(DISTINCT userkey) AS uv\nFROM mid_hotel.hotel_flow_newubt_gnhotel_di\nWHERE dt = DATE_FORMAT(DATE_SUB(CURRENT_DATE, 1), 'yyyyMMdd')",
  "tables_used": ["mid_hotel.hotel_flow_newubt_gnhotel_di"],
  "confidence": 0.87
}
```

### 4.4 只读安全校验（执行前强制）

后端的只读校验器 `is_read_only(sql)` 逻辑：

```text
1. 剥离注释/字符串后，用正则匹配危险关键字：
   INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|LOAD|MERGE
2. 用 SQL 解析器（如 sqlglot）解析，确认顶级语句均为 SELECT / WITH...SELECT。
3. 校验引用的表均在元数据库中（防越权访问未授权表，可选）。
4. 全通过 → is_read_only=true；否则拒绝执行并返回错误码 40012。
```

### 4.5 降级策略（fallback）

| 触发条件 | 行为 |
|----------|------|
| 网络超时 / 连接失败 | 重试 2 次（指数退避）→ 仍失败则走规则生成 |
| API 返回 429 | 等待重试，最多 2 次 |
| 返回 JSON 解析失败 | 重试一次，采用 `output_text` 兜底解析 |
| Key 无效 / 401 | 不再重试，直接降级 + 平台内通知管理员 |
| `fallback=false` 配置 | 直接返回错误，不降级 |

降级生成器（规则/模板）：
```text
输入问题 → 关键词匹配表名/字段/维度（来自 meta_index 检索）→
根据匹配结果套用聚合模板（COUNT/COUNT DISTINCT/SUM/AVG + GROUP BY）→
自动补齐分区过滤。
```

### 4.6 API Key 安全存储

- 明文 Key 仅在 `PUT /llm/config` 请求体中出现一次。
- 入库前用 **AES-256-GCM** 加密（加密密钥从服务端环境变量 `LLM_KEY_ENC_MASTER` 读取）。
- 日志中强制脱敏：`sk-` 之后仅保留末 3 位。
- 读取配置接口仅返回脱敏值（`api_key_masked`）。
- `.env` / 配置文件禁止提交到 Git，提供 `.env.example` 占位。

### 4.7 LLM 网关 Pydantic 模型（schemas/llm.py）

```python
class LLMConfig(BaseModel):
    provider: str = "qwen"
    model_name: str = "qwen3-8b"
    api_url: HttpUrl
    api_key: SecretStr = Field(..., description="仅写入时使用，读取接口返回脱敏值")
    enabled: bool = True
    fallback: bool = True
    temperature: float = Field(0.0, ge=0.0, le=1.0)
    max_tokens: int = Field(2048, ge=1, le=8192)
    timeout_sec: int = Field(30, ge=1, le=120)

class LLMGenerateRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=500)
    db_hint: str | None = None
    n: int = Field(1, ge=1, le=3)

class NL2SQLCandidate(BaseModel):
    sql: str
    sql_id: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    tables_used: list[str] = []
    is_read_only: bool = False
```

---

## 5. 知识库（RAG）增强 NL2SQL

> 本章为增强版（v1.3）：在原有"向量增强"基础上，引入**知识四层架构（L1-L4）**、**防腐烂生命周期**、**ACL 出域合规**、**混合检索**与**评测机制**。对标配套文档《04-知识库体系设计与执行规划.md》。
> 核心纠正：**NL2SQL 的第一上下文不是向量库，而是结构化语义层（L1）**。表/字段/指标是**事实**，应走 SQL 精确查询（Schema Linking）；文档是**解释**，才走向量模糊召回。把文档塞给 NL2SQL 当主上下文，是引入噪声、拖慢响应、降低准确率的头号错误。

### 5.0 知识四层架构（L1-L4，职责不重叠）

```text
L4 运行时上下文（不入库向量，仅会话内短暂保留，Redis TTL 1h）
    对话历史、用户偏好、临时澄清
    ↑
L3 经验知识层（向量 + 结构化标签）          ← few-shot 样例来源
    SQL 样例、改写案例、历史问答对(高赞)、事故复盘、取数套路 FAQ
    ↑
L2 文档知识层（向量为主）                    ← 口径补充上下文
    口径说明、建模规范、Wiki/Word、数据字典、变更说明、故障手册
    ↑
L1 结构化事实层（MySQL，不走向量）           ← NL2SQL 主上下文
    meta_*(元数据)、semantic_*(语义层)、血缘(meta_lineage)、质量(quality_*)、指标
```

| 层 | 存储 | 检索方式 | 是否被 NL2SQL 直接使用 | 说明 |
|----|------|---------|----------------------|------|
| L1 | MySQL | SQL 精确查询 / join | ✅ **主上下文** | 权威、可控、可解释 |
| L2 | MySQL(chunk) + ES | 混合检索（BM25 + kNN） | ✅ 补充上下文（口径、业务规则） | 必须带溯源 |
| L3 | MySQL(chunk) + ES | 混合检索 + 标签过滤 | ✅ few-shot 样例来源 | 采纳后沉淀，正向飞轮 |
| L4 | Redis / 内存 | 直取 | ✅ 多轮澄清 | TTL 短，不落库 |

> 一句话：**能用 SQL 查到的，绝不放进向量库**。向量库只放"无法结构化"的文本。

### 5.1 为什么需要知识库

仅靠表结构（Schema）生成 SQL，LLM 常因「同一含义多种表达」而选错字段/表（例如「昨日 GMV」可能对应多张候选表）。通过**检索增强生成（RAG）**，把**业务口径、字段同义词、历史查询 SQL、表间关联规则**等额外知识在生成前注入提示词，可显著提升选表、选字段、写过滤条件的准确率。

### 5.2 知识分类体系（kb_type + domain）

#### 5.2.1 kb_type 枚举（升级自原 glossary/alias/sql_example/mapping/rule）

| 码 | 类型 | 内容示例 | 来源 | 向量化 | 优先级 |
|----|------|---------|------|--------|--------|
| `METRIC` | 指标口径 | "间夜量=SUM(入住天数)，口径排除取消订单" | 人工注册/文档抽取 | ✅ | P0 |
| `SEMANTIC` | 表/字段业务含义 | "validdays=里程有效期天数" | F0 自举+人工校正 | ⚠️ 已在 L1，只存索引指针 | P0 |
| `TERM` | 业务术语/黑话 | "ADR、OCC、RevPAR、间夜" | 人工/术语表 | ✅ | P0 |
| `FAQ` | 取数 FAQ | "怎么查上月各酒店 GMV？" | 沉淀 | ✅ | P1 |
| `SQLCASE` | SQL 样例/改写案例 | 优化前后 SQL+说明 | F3 沉淀 | ✅ | P1 |
| `RULE` | 规范与规则 | 分层规范、SQL MUST DO/MUST NOT、质量规则 | 配置表同步 | ✅ | P1 |
| `INCIDENT` | 事故复盘 | "dwd 层延迟 3h，根因…" | 人工录入 | ✅ | P2 |
| `GLOSSARY` | 主题域/资产说明 | 域划分、责任人、SLA、平台码字典 | 人工 | ✅ | P2 |

> **SEMANTIC 特殊处理（重要）**：字段语义已在 `semantic_column`（L1），**不重复切分入库**，向量库只建一条轻量索引指针（指向 col_id），避免"两处维护、必然不一致"。这是很多项目踩的坑。

#### 5.2.2 主题域（domain）

按分层 `dim / dwd / ads / etl / tmp / rpt` 为**层级**标签，另加业务域标签（酒店、里程、订单、会员…）。检索时先按域收敛、再语义召回。`kb_document` 增加 `domain` 字段。

### 5.3 数据模型（MySQL 新增/增强）

> 在原有 `kb_document / kb_alias / kb_sql_example` 基础上增强并新增 6 张表。DDL 可直接在 MySQL 8.0 执行，统一 utf8mb4 + InnoDB。

#### 5.3.1 kb_document（知识条目主表，增强版）

```sql
CREATE TABLE kb_document (
  id                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  kb_type           VARCHAR(32)  NOT NULL COMMENT '标准枚举: METRIC/SEMANTIC/TERM/FAQ/SQLCASE/RULE/INCIDENT/GLOSSARY',
  domain            VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '主题域/分层标签(含兼容旧值)',
  title             VARCHAR(255) NOT NULL COMMENT '标题',
  content           TEXT         NOT NULL COMMENT '正文/说明',
  tags              VARCHAR(255) NOT NULL DEFAULT '' COMMENT '逗号分隔标签，用于检索',
  source            VARCHAR(512) NOT NULL DEFAULT '' COMMENT '来源(如维基链接)',
  related_ids       JSON         NULL COMMENT '关联表/字段ID 列表',
  owner             VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '负责人工号，无owner不允许发布',
  status            VARCHAR(16)  NOT NULL DEFAULT 'draft' COMMENT 'draft/published/stale/archived',
  version           INT          NOT NULL DEFAULT 1 COMMENT '版本号',
  superseded_by     BIGINT       NULL COMMENT '被替代的新知识ID',
  last_verified_at  DATETIME     NULL COMMENT '最后复核时间',
  cited_count       INT          NOT NULL DEFAULT 0 COMMENT '被问答引用次数',
  acl_level         VARCHAR(32)  NOT NULL DEFAULT 'L1_INTERNAL' COMMENT 'L0_PUBLIC/L1_INTERNAL/L2_CONFIDENTIAL/L3_RESTRICTED',
  enabled           TINYINT      NOT NULL DEFAULT 1,
  created_by        VARCHAR(64)  NOT NULL DEFAULT '',
  created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_type_enabled (kb_type, enabled),
  KEY idx_status_owner (status, owner),
  KEY idx_acllvl (acl_level),
  KEY idx_tags (tags(128))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='NL2SQL知识库文档';
```

#### 5.3.2 kb_alias（业务词 → 标准字段/表达式 同义词）

```sql
CREATE TABLE kb_alias (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  kb_type     VARCHAR(32)  NOT NULL COMMENT 'alias 同义词',
  term        VARCHAR(128) NOT NULL COMMENT '用户常用词/业务词，如"UV"、"去重人数"',
  standard    VARCHAR(128) NOT NULL COMMENT '标准字段/表/表达式，如 userkey、COUNT(DISTINCT userkey)',
  score       DECIMAL(5,4) NOT NULL DEFAULT 1.0000 COMMENT '匹配权重',
  remark      VARCHAR(255) NOT NULL DEFAULT '' COMMENT '说明',
  enabled     TINYINT      NOT NULL DEFAULT 1,
  created_by  VARCHAR(64)  NOT NULL DEFAULT '',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_term_type (term, kb_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='业务词→标准字段/表达式同义词';
```

#### 5.3.3 kb_sql_example（历史查询 / 示例 SQL，few-shot 来源）

```sql
CREATE TABLE kb_sql_example (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  question     VARCHAR(500) NOT NULL COMMENT '自然语言问题',
  sql          TEXT         NOT NULL COMMENT '对应SQL',
  tables_used  VARCHAR(512) NOT NULL DEFAULT '' COMMENT '用到的表，逗号分隔',
  tags         VARCHAR(255) NOT NULL DEFAULT '' COMMENT '标签',
  good_feedback INT         NOT NULL DEFAULT 0 COMMENT '被点赞次数',
  owner        VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '负责人工号',
  status       VARCHAR(16)  NOT NULL DEFAULT 'published' COMMENT 'draft/published/stale/archived',
  acl_level    VARCHAR(32)  NOT NULL DEFAULT 'L1_INTERNAL' COMMENT '出域合规级别',
  enabled      TINYINT      NOT NULL DEFAULT 1,
  created_by   VARCHAR(64)  NOT NULL DEFAULT '',
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_tags (tags(128)),
  KEY idx_question (question(128))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='历史/示例SQL（few-shot 来源）';
```

#### 5.3.4 kb_chunk（知识分片，与 ES 索引一一对应）

```sql
CREATE TABLE kb_chunk (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  doc_id      BIGINT UNSIGNED NOT NULL COMMENT 'FK→kb_document.id',
  seq         INT             NOT NULL DEFAULT 0 COMMENT '分片序号',
  content     TEXT            NOT NULL COMMENT '分片文本',
  title_path  VARCHAR(512)    NOT NULL DEFAULT '' COMMENT '标题路径前缀【域>文档>章节】',
  token_count INT             NOT NULL DEFAULT 0,
  es_doc_id   VARCHAR(128)    NOT NULL DEFAULT '' COMMENT 'ES中的文档ID',
  acl_level   VARCHAR(32)     NOT NULL DEFAULT 'L1_INTERNAL',
  status      VARCHAR(16)     NOT NULL DEFAULT 'published',
  created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_doc (doc_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识分片';
```

#### 5.3.5 kb_embedding（向量本地兜底存储，ES 不可用时启用）

```sql
CREATE TABLE kb_embedding (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  chunk_id     BIGINT UNSIGNED NOT NULL COMMENT 'FK→kb_chunk.id',
  model        VARCHAR(64)     NOT NULL COMMENT 'embedding模型',
  dim          INT             NOT NULL DEFAULT 1024 COMMENT '向量维度',
  vector_blob  BLOB            NOT NULL COMMENT '序列化向量',
  updated_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_chunk_model (chunk_id, model)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识向量本地兜底(ES不可用/涉密知识本地embedding)';
```

#### 5.3.6 kb_feedback（检索反馈）

```sql
CREATE TABLE kb_feedback (
  id        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  chunk_id  BIGINT UNSIGNED NOT NULL,
  qa_id     BIGINT UNSIGNED NULL,
  user      VARCHAR(64)     NOT NULL COMMENT '反馈人工号',
  useful    TINYINT         NOT NULL COMMENT '1有用/0无用',
  comment   VARCHAR(512)    NOT NULL DEFAULT '',
  handled   TINYINT         NOT NULL DEFAULT 0 COMMENT '是否已处理',
  created_at DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识检索反馈(降权/纠错依据)';
```

#### 5.3.7 kb_retrieval_log（检索日志，评测与调优数据源）

```sql
CREATE TABLE kb_retrieval_log (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  qa_id        BIGINT UNSIGNED NULL,
  user         VARCHAR(64)    NOT NULL,
  query        TEXT           NOT NULL COMMENT '原问题',
  hit_chunk_ids JSON          NULL COMMENT '命中的chunk_id列表',
  scores       JSON           NULL COMMENT '各命中分数',
  latency_ms   INT            NOT NULL DEFAULT 0,
  top1_score   DECIMAL(5,4)   NULL,
  created_at   DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识检索日志';
```

#### 5.3.8 kb_review_task（防腐烂执行载体 - 复核待办）

```sql
CREATE TABLE kb_review_task (
  id        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  doc_id    BIGINT UNSIGNED NOT NULL COMMENT 'FK→kb_document.id',
  reason    VARCHAR(32)     NOT NULL COMMENT 'meta_changed/ttl_expired/negative_fb',
  assignee  VARCHAR(64)     NOT NULL COMMENT '负责人工号',
  status    VARCHAR(16)     NOT NULL DEFAULT 'pending' COMMENT 'pending/done',
  created_at DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  done_at   DATETIME        NULL,
  KEY idx_assignee_status (assignee, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识复核待办';
```

#### 5.3.9 kb_source（知识来源登记）

```sql
CREATE TABLE kb_source (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(128) NOT NULL,
  type        VARCHAR(16)  NOT NULL COMMENT 'local_dir/db/manual/api',
  config_json JSON         NULL,
  schedule    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '采集调度cron',
  last_sync_at DATETIME    NULL,
  enabled     TINYINT      NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识来源登记';
```

> **与已有表联动**：`kb_term` 并入 `kb_alias`（无需新建）；`kb_document.related_ids` 关联 `meta_table` / `semantic_metric`，供 §5.5 元数据变更联动失效。业务术语与 `semantic_term` **建议合并避免双份**。

#### 5.3.10 ES 索引 kb_index

```json
PUT /kb_index
{
  "mappings": {
    "properties": {
      "content":    { "type": "text", "analyzer": "ik_max_word" },
      "title_path": { "type": "text", "analyzer": "ik_smart" },
      "embedding":  { "type": "dense_vector", "dims": 1024, "index": true, "similarity": "cosine" },
      "kb_type":    { "type": "keyword" },
      "domain":     { "type": "keyword" },
      "acl_level":  { "type": "keyword" },
      "status":     { "type": "keyword" },
      "doc_id":     { "type": "keyword" },
      "owner":      { "type": "keyword" }
    }
  }
}
```

检索 = `bool` 过滤（acl_level/domain/status）+ `knn` 语义 + `match` 关键词，用 RRF 融合。

### 5.4 混合检索架构（意图分流 + 权限前置 + 多路召回 + 强制溯源）

```text
用户问句
  ↓
[1] 意图识别（规则 + 千问轻量分类）
      ├─ 取数类 → L1 Schema Linking（SQL 查 meta/semantic）+ L3 few-shot 召回
      ├─ 口径类 → L2/L3 混合检索
      └─ 排障类 → L2(INCIDENT) + L1(质量/血缘)
  ↓
[2] 权限前置过滤（按 user.role 过滤 acl_level，在检索阶段执行）   ← 关键，非召回后
  ↓
[3] 混合召回
      ├── 向量语义: ES kNN（dense_vector）
      ├── 关键词: ES BM25（中文 IK 分词）
      └── 结构化过滤: domain / layer / kb_type / verified / status
  ↓
[4] 融合重排: RRF 倒数排名融合（或可选 rerank 模型）
  ↓
[5] Top-K（K=5）拼装上下文，每条附 [doc_id, chunk_id, version, 来源链接]
  ↓
[6] 调用千问生成候选 SQL（见第4章），强制引用标注 [1][2]
  ↓
[7] 输出 + 溯源面板（可点开原文、可反馈、可一键纠错）
```

**硬性规则**

- **无溯源不输出**：答案中每个事实性断言必须带引用；无法引用时回答"知识库未覆盖，建议咨询 owner"。
- **Top-K 上限 5**：上下文越长越容易漂移，宁缺毋滥。
- **权限过滤在检索层**：分片写入时打 `acl_level`，检索按用户角色过滤，**不在召回后处理**（召回后再过滤，被过滤内容已进 LLM 上下文，可能出域）。
- **缓存**：问答对缓存（Redis，TTL 1h），降低千问调用成本。

#### 5.4.1 切分策略（显著影响召回质量）

| 知识类型 | 切分方式 | chunk 大小 |
|---------|---------|-----------|
| METRIC / TERM / SEMANTIC | **不切分**，一条 = 一个 chunk | 100-300 字 |
| 文档（md/docx） | 按**标题层级**切（H2/H3 为界），不足再按段落合并 | 300-600 字 + 15% 重叠 |
| SQLCASE | 一条 = 一个 chunk（含 SQL + 说明） | — |
| **标题路径前缀（必做）** | 每个 chunk 文本前拼 `【域 > 文档 > 章节】` | — |

> 标题路径前缀让 ES 关键词命中"章节归属"，成本几乎为零但能显著提升召回准确率。

#### 5.4.2 Embedding 模型与 ACL 路由

| 用途 | 主选 | 降级/不出域备选 | 注意 |
|------|------|----------------|------|
| 中文知识向量化 | 千问 DashScope `text-embedding-v3/v4`（1024 维） | 本地 Ollama `bge-large-zh`（1024 维） | **维度必须一致**，换模型 = 全量重建索引 |
| 出域判断 | ⚠️ 调千问 embedding = 知识文本出域 | 涉密知识走本地模型 | 见 §5.6 |

### 5.5 防腐烂生命周期（知识库活下去的保障）

> 核心判断：知识库最大的敌人不是"内容少"，是"腐烂"。三个月没人维护的口径文档，比没有文档更危险——它会让 AI 自信给出错误答案。**鲜度机制和 owner 机制必须与入库功能同期做**。

```text
采集(Ingest) → 结构化(Structure) → 校验(Verify) → 发布(Publish)
→ 消费(Retrieve) → 反馈(Feedback) → 鲜度衰减(Decay) → 归档/失效(Archive)
```

| 环节 | 机制 | 落地 |
|------|------|------|
| **Owner** | 每条知识必须有 owner | 表知识 owner = 表负责人；指标 owner = 提出人。**无 owner 不允许发布** |
| **校验** | LLM 生成内容一律 `verified=false`，人工确认后才 `verified=true` | 与 F0.3 校正界面共用 |
| **版本** | `version` + `superseded_by`，历史版本保留可回溯 | 口径变更可 diff |
| **鲜度** | `last_verified_at`；超 **90 天**未复核 → 转 `stale`，检索降权（不删除） | 看板暴露"逾期知识数" |
| **失效触发** | **元数据变更自动触发关联知识失效**：字段被删除/改类型 → 关联 METRIC/SEMANTIC 自动转 `stale` 并通知 owner | 复用 F1.5 快照 diff，低成本高价值联动 |
| **引用计数** | `cited_count`（被问答引用次数） | 高引用 + 高采纳 = 优质；零引用 = 考虑归档 |
| **反馈闭环** | 问答页"有用/无用"按钮 → 无用则降权并生成待办给 owner | 对应 PRD 反馈埋点 |

**防腐烂执行载体**：`kb_review_task` 表 + 定期 Celery 任务（`kb_freshness_check` / `kb_meta_link_expire`）。

### 5.6 ACL 分级与出域合规（知识库特有新增风险）

> 原 §4.5 只定义"元数据出域白名单"，但知识库会引入新风险：**文档内容出域**。一份《酒店定价策略》embedding 时被送到千问 API——这不是元数据，属于业务敏感信息。

**知识分级（`acl_level` 字段）**

| 级别 | 内容 | 可否送外部 embedding | 可否送外部 LLM |
|------|------|--------------------|---------------|
| `L0_PUBLIC` | 通用数仓概念、SQL 语法、公开规范 | ✅ | ✅ |
| `L1_INTERNAL` | 表/字段注释、指标定义、内部 FAQ | ✅（建议） | ✅ |
| `L2_CONFIDENTIAL` | 含具体商户/价格/成本/经营策略 | ❌ **本地 embedding** | ❌ 仅送标题与摘要 |
| `L3_RESTRICTED` | 涉个人信息、合规敏感 | ❌ 不入库向量 | ❌ 禁止调用 |

**落地**

- 知识入库强制打级别（默认 `L1`，创建者可选升级）。
- Model Router 增加 `embed()` 方法，内部按级别路由：L0/L1 → 千问 embedding；L2 → 本地 Ollama `bge-large-zh`；L3 → 拒绝入库向量，仅存结构化字段。
- `llm_call_log` 扩展 `kb_doc_id`、`acl_level`、`payload_hash`，审计可追溯到具体知识条目。

### 5.7 提示词组装（知识注入）

在 `llm_gateway.generate()` 中，把检索到的知识按三类注入：

```text
【知识库-口径】
- 口径：UV = 去重用户数，使用 userkey 字段 COUNT(DISTINCT userkey)
- 平台码：10056=艺龙App, 10113=艺龙酒店App ...

【知识库-同义词】
- 用户说"UV" → userkey
- 用户说"昨日"/"昨天" → dt = DATE_SUB(CURRENT_DATE,1)

【知识库-示例SQL】
- 类似问题: ''统计昨天国内酒店UV'' →
  SELECT COUNT(DISTINCT userkey) FROM mid_hotel.hotel_flow_newubt_gnhotel_di WHERE dt=...

【表结构】
...（原 Schema 注入，来自 L1 结构化语义层）
```

### 5.8 知识库管理 API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /api/v1/kb/sources | 注册知识来源 |
| POST | /api/v1/kb/ingest | 触发采集（全量/增量），返回任务 ID |
| POST | /api/v1/kb/documents | 新增知识库文档，**必填 owner** |
| GET  | /api/v1/kb/documents | 分页查询（按 type/tag/domain/关键词） |
| GET  | /api/v1/kb/documents?status=stale&owner=me | 我的待复核知识 |
| PUT  | /api/v1/kb/documents/{id} | 更新 |
| POST | /api/v1/kb/documents/{id}/verify | 复核通过（version+1，刷新 last_verified_at） |
| POST | /api/v1/kb/documents/{id}/archive | 归档 |
| DELETE | /api/v1/kb/documents/{id} | 删除（软删 enabled=0） |
| POST | /api/v1/kb/aliases | 新增同义词 |
| GET  | /api/v1/kb/aliases | 查询同义词 |
| POST | /api/v1/kb/sql-examples | 新增示例SQL |
| POST | /api/v1/kb/sql-examples/{id}/import | 从优质会话一键沉淀为示例（见5.9） |
| POST | /api/v1/kb/retrieve | 供 LLM 调用的结构化召回（内部） |
| GET  | /api/v1/kb/search?q=&type=&domain=&k=5 | 混合检索（人用） |
| POST | /api/v1/kb/feedback | 有用/无用反馈 |
| GET  | /api/v1/kb/freshness | 鲜度看板：stale 数、超90天占比、无 owner 数 |
| GET  | /api/v1/kb/stats | 知识量、类型分布、引用 Top10、零引用清单 |
| GET  | /api/v1/kb/metrics | 检索评测指标（Recall@5/MRR/有引用率） |
| POST | /api/v1/kb/embedding/rebuild | 触发向量重新索引 |

### 5.9 知识闭环（人工 + 自动沉淀）

1. **人工维护**：分析师/数仓在知识库管理页录入口径、同义词、示例SQL（必填 owner）。
2. **自动沉淀**：用户对 NL2SQL 结果**点赞(feedback=1)** 的问答对，一键「沉淀为知识」入 `kb_sql_example`，不断扩充 few-shot 库。采纳结果 → 生成 FAQ/SQLCASE 草稿，owner 确认后发布（正向飞轮）。
3. **质量标记**：踩(feedback=-1) 的问答对标记候选，供人工修正口径。
4. **版本与回滚**：`version` + `superseded_by`，历史版本保留可供 diff。

### 5.10 知识来源与采集策略

| 来源 | 采集方式 | 优先级 | 备注 |
|------|---------|--------|------|
| ① **平台内生（首选）** | `meta_*` / `semantic_*` / `quality_*` / `llm_call_log` 视图化 | P0 | 零人工，永远与真实元数据同步，不腐烂 |
| ② **指标口径注册** | F0.4 人工录入界面 | P0 | 当前最缺，从 0 到 1 |
| ③ **本地文档目录** | 扫描 `D:\dw\docs` 的 .md/.docx/.txt/.pdf | P1 | 建议现在就建，形成沉淀习惯 |
| ④ **建表 SQL 注释** | 解析 `verify.sql` 等 COMMENT | P1 | 与"模式一采集"复用解析器 |
| ⑤ **对话沉淀** | NL2SQL/问答被采纳结果 → FAQ/SQLCASE | P1 | 飞轮起点，"一键沉淀为知识" |
| ⑥ **外部系统（后续）** | Wiki/Confluence API、Git 仓库 SQL | P2 | 预留 connector 接口 |

> **冷启动建议**：先只做 ①②⑤。①零成本、②补齐最大缺口、⑤让知识自然增长。③④等在闭环跑通后再接——否则会浪费大量时间且污染检索质量。

### 5.11 评测方法（知识库必须能量化）

| 指标 | 定义 | 目标 | 如何测 |
|------|------|------|--------|
| **检索 Recall@5** | 标准问句的正确答案切片出现在 Top5 的比例 | ≥85% | 建 50 条评测集（问句 + 期望 doc_id），跑批自动算 |
| **MRR** | 首个正确结果的排名倒数均值 | ≥0.7 | 同上 |
| **答案有引用率** | 生成答案中带 `[n]` 引用的比例 | **100%**（硬门槛） | 评测集全量跑，任一条无引用即失败 |
| **引用准确率** | 引用指向的切片确实支持该断言 | ≥90% | 人工抽检 20 条 |
| **知识采纳率** | 用户点"有用"的比例 | ≥60% | 线上埋点 |
| **鲜度达标率** | 超 90 天未复核知识占比 | ≤20% | `kb_freshness` 看板 |
| **覆盖率** | 有口径知识的核心表占比 | ≥80%（Top-N 表） | 关联 `meta_table` 统计 |

> **评测集是前置任务，不是后置任务**。50 条问句 + 期望答案切片，在 KB-2 阶段就要建好——否则无法判断"换切分/换 embedding 是变好还是变坏"。

### 5.12 执行规划（四阶段，与现有迭代对齐）

| 阶段 | 内容 | 对齐迭代 | 估时 |
|------|------|---------|------|
| **KB-1 底座复用** | 不新建系统，把 `meta_*` + `semantic_*` 通过视图/服务暴露为可检索知识（L1） | 迭代1（与 F0 同期） | 1-2d |
| **KB-2 知识库骨架** | `kb_*` 表 + ES 索引 + 手工录入界面 + owner/版本/状态 | 迭代1末/迭代2初 | 3d |
| **KB-3 检索与问答** | 混合检索 + 权限过滤 + 融合重排 + 溯源 UI + 反馈 + retrieval_log | 迭代2-3 | 3-4d |
| **KB-4 防腐与运营** | 鲜度衰减、元数据变更联动失效、复核待办、鲜度看板、一键沉淀 | 迭代3末 | 2d |

**任务卡（1-3 天粒度）**

| 卡号 | 任务 | 验收标准 | 估时 |
|------|------|---------|------|
| TK1.1 | 语义层知识服务化：`/kb/search` 先只查 L1 | 问"里程有效期字段"能返回 `validdays` 及注释 | 1d |
| TK1.2 | 建 kb_source/kb_doc/kb_chunk/kb_term 表 + 迁移 | 表结构就位，与 semantic_metric 外键联通 | 1d |
| TK1.3 | 知识手工录入界面（类型/域/owner/级别/内容） | 无 owner 无法提交；提交后状态 draft | 1.5d |
| TK1.4 | 指标口径批量录入（≥10 个核心指标） | 指标知识 ≥10 条且 owner 齐全 | 1d |
| TK2.1 | ES kb_index 建索引（ik + dense_vector） | 索引创建成功，可写入可查询 | 1d |
| TK2.2 | 切分器（标题层级 + 标题路径前缀 + 重叠） | 一篇文档切成合理 chunk，前缀正确 | 1d |
| TK2.3 | Embedding 接入（千问 v3 主 + Ollama bge 降级），按 acl 路由 | L2 知识走本地，`llm_call_log` 有记录 | 1.5d |
| TK2.4 | 混合检索（BM25 + kNN + 结构化过滤 + RRF） | 单接口返回 Top5，含分数与来源 | 1.5d |
| TK3.1 | 接入口径问答：检索 → 拼上下文 → 千问 → 强制引用 | **有引用率 100%**，答案可点开原文 | 2d |
| TK3.2 | 检索权限前置过滤（按角色过滤 acl_level） | 低权限用户检索不到 L2/L3 切片 | 1d |
| TK3.3 | 反馈与降权："无用" → 记 kb_feedback → 生成复核待办 | 反馈可追溯，待办可见 | 1d |
| TK3.4 | 评测集 50 条 + 自动跑批脚本 | Recall@5、MRR、有引用率一键可算 | 1.5d |
| TK4.1 | 鲜度机制：90 天转 stale + 检索降权 + 看板 | stale 知识检索排后，看板可见 | 1d |
| TK4.2 | 元数据变更联动失效（改类型 → 关联知识转 stale + 通知 owner） | 改一次字段类型，关联知识自动转 stale | 1.5d |
| TK4.3 | "一键沉淀为知识"：采纳问答 → FAQ/SQLCASE | 点一下生成草稿，owner 确认后发布 | 1d |

> 合计约 18-20 工作日。TK1.x（4.5d）建议**并入迭代1**，与 F0 注释自举打包做——共用同一套"人工校正/录入"界面，省一半工作量。

### 5.13 所需新增组件

- `app/services/kb_service.py`：知识库 CRUD + 检索（多路召回、重排、权限过滤）。
- `app/services/rag.py`：Embedding 与检索融合、提示词组装、RF RRF。
- `app/routers/kb.py`：知识库管理路由。
- `app/tasks/embedding_index.py`：异步向量重建（Celery，含 acl 路由）。
- `app/tasks/kb_freshness_check.py`：鲜度检查（90 天转 stale + 生成复核待办）。
- `app/tasks/kb_meta_link_expire.py`：元数据变更联动失效。
- 向量库：复用 ES `meta_index`，知识库额外建 `kb_index`（§5.3.10）。

### 5.14 知识库 RAG Pydantic 模型

```python
from typing import Literal
from pydantic import BaseModel, Field

KBTYPE = Literal['METRIC','SEMANTIC','TERM','FAQ','SQLCASE','RULE','INCIDENT','GLOSSARY']
ACLLEVEL = Literal['L0_PUBLIC','L1_INTERNAL','L2_CONFIDENTIAL','L3_RESTRICTED']

class KBDocumentCreate(BaseModel):
    kb_type: KBTYPE
    domain: str = ""
    title: str = Field(..., max_length=255)
    content: str = Field(..., max_length=10000)
    tags: str = ""
    source: str = ""
    related_ids: list[int] = []
    owner: str = Field(..., min_length=1, description="必填，无 owner 禁止发布")   # 必填
    acl_level: ACLLEVEL = 'L1_INTERNAL'
    status: Literal['draft','published'] = 'draft'

class RAGContext(BaseModel):
    glossaries: list[str] = []
    aliases: list[dict] = []       # {term, standard}
    sql_examples: list[dict] = []  # {question, sql}

class NL2SQLGenerateRequest(BaseModel):
    question: str
    db_hint: str | None = None
    n: int = 1
    use_kb: bool = True            # 是否启用知识库检索(可开关)
```

### 5.15 检索数据来源与维护建议（优先级）

| 优先级 | 来源 | 沉淀为 | 价值 |
|--------|------|--------|------|
| ⭐⭐⭐ | 已有的**口径文档**(维基/手册) | METRIC/GLOSSARY | 最高，直接解决口径不一致 |
| ⭐⭐⭐ | 用户**高频历史查询**与点赞 SQL | SQLCASE | 精准匹配用户真实问法 |
| ⭐⭐ | 表/字段**中文注释**批量抽取为同义词 | alias / TERM | 低成本、覆盖广 |
| ⭐⭐ | **业务词字典**(平台码、项目码、日期词) | TERM/GLOSSARY | 过滤条件准确率提升 |
| ⭐ | 血缘关系(常用 JOIN 表对) | RULE/mapping | 推荐联表查询 |

### 5.16 冷启动策略与三个最容易踩的坑

**冷启动策略**：当前只有 2 张表、零指标文档。**不要全量灌文档**。先做 **30-50 条高价值知识**跑通"入库→检索→问答→溯源→反馈"闭环，验证有效再规模化。

| 坑 | 表现 | 规避 |
|----|------|------|
| **知识重复源** | 字段注释在 semantic_column 和 kb_doc 各存一份，改一处忘另一处 | SEMANTIC 类知识不复制内容，只存指针（见 §5.2.1） |
| **全量灌文档** | 冷启动把 500 份历史文档全塞进去，检索全是噪声，Recall 反而更低 | 先 50 条高价值知识跑通闭环，评测通过再扩量 |
| **只建不管** | 上线时 100 条，半年后 105 条且一半过期 | §5.5 生命周期 + §5.12 KB-4 任务不得裁剪 |

### 5.17 待拍板（2 项）

| # | 问题 | 选项 A | 选项 B | 建议 |
|---|------|--------|--------|------|
| **C6** | 本地 embedding 是否部署 | 部署 Ollama + bge-large-zh（L2 可本地向量化） | 不部署，L2 以上不向量化（仅 BM25） | **先不部署**（MVP 阶段 L2 极少），出现真实涉密知识再补 |
| **C7** | 向量存储 | 复用 ES 8 kNN（推荐） | 引入 Milvus / PG+pgvector | **ES 8 kNN**，零新增组件；ES 是 7.x → 先 BM25 + MySQL 内存向量兜底 |

---

## 6. Celery 异步任务设计

| 任务 | 队列 | 说明 |
|------|------|------|
| `metadata_sync` | metadata | 元数据同步（文件/hive 双模式，支持增量） |
| `lineage_build` | lineage | 血缘自建（离线回填/增量解析） |
| `quality_run` | quality | 质量规则执行（提交 Spark，写入结果，超限发通知） |
| `nl2sql_feedback` | search | 用户反馈异步落库 |
| `embedding_index` | kb | 知识库向量索引重建（RAG，按 acl 路由本地/千问 embedding） |
| `kb_ingest` | kb | 知识采集（目录扫描/DB/manual） |
| `kb_freshness_check` | kb | 鲜度检查：90 天转 stale + 生成复核待办 |
| `kb_meta_link_expire` | kb | 元数据变更联动失效（字段删/改 → 关联知识转 stale + 通知 owner） |

任务定义（tasks/quality_run.py 示例）：

```python
@celery_app.task(name="quality_run", bind=True, max_retries=2)
def quality_run(self, rule_id: int):
    rule = get_rule(rule_id)
    if not rule.enabled:
        return {"status": "SKIPPED"}
    try:
        instance = submit_spark_read_only(rule.to_sql())
        result = wait_for(instance)
        save_result(rule_id, result)
        if not result.pass:
            send_notify(rule, result)      # 平台内通知
        return {"status": "DONE", "pass": result.pass}
    except SparkSubmitError as e:
        raise self.retry(exc=e, countdown=30)
```

---

## 7. 错误码与异常规范

| code | 含义 | HTTP 状态 |
|------|------|-----------|
| 0 | 成功 | 200 |
| 40000 | 参数校验失败（Pydantic） | 400 |
| 40001 | 资源不存在 | 404 |
| 40002 | 未认证 / Token 失效 | 401 |
| 40003 | 无权限 | 403 |
| 40010 | SQL 语法解析失败 | 400 |
| 40011 | NL2SQL 生成失败（LLM 不可用且降级失败） | 502 |
| 40012 | SQL 非只读，已拦截 | 403 |
| 40013 | 千问 API 调用异常 | 502 |
| 40014 | 知识库 RAG 检索失败（不影响主流程，可忽略） | 200+警告 |
| 40015 | 知识缺 owner，禁止发布 | 400 |
| 40016 | 知识 ACL 级别不允许向量化(L3) | 400 |
| 40017 | 知识已 stale/archived，不可检索 | 403 |
| 40018 | 检索权限不足，命中内容被 ACL 过滤 | 403 |
| 50000 | 服务器内部错误 | 500 |

异常统一由 `core/exceptions.py` 抛出，`main.py` 全局 handler 捕获并包装为 `{code,message,data}`。

---

## 8. 缺失模块补充工程 Spec（数据集成 / 数据安全 / 数据资产 / 数据标准 / 数据开发 / 数据分析 / 数据服务 / 生命周期）

> 本节补充工程 Spec 的治理闭环与开发/分析/服务模块（对应功能文档 F7/F8/F9/F10/F11/F12/F13，另含生命周期）。工程规划、任务卡见《智能数仓平台-市场调研与差距分析.md》。

### 8.1 数据集成模块（F7）

#### 8.1.1 数据源管理表（MySQL 新增）

```sql
CREATE TABLE ds_source (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  source_name   VARCHAR(128) NOT NULL COMMENT '数据源名称',
  source_type   VARCHAR(32)  NOT NULL COMMENT 'mysql/hive/file/kafka',
  host          VARCHAR(255) NOT NULL DEFAULT '' COMMENT '地址',
  port          INT          NOT NULL DEFAULT 0,
  database_name VARCHAR(128) NOT NULL DEFAULT '' COMMENT '库名/路径',
  username_enc  VARCHAR(255) NOT NULL DEFAULT '' COMMENT '加密用户名',
  password_enc  VARCHAR(512) NOT NULL DEFAULT '' COMMENT '加密密码',
  extra_config  JSON         NULL COMMENT '扩展配置(如Kerberos/分区)',
  status        VARCHAR(16)  NOT NULL DEFAULT 'enabled' COMMENT 'enabled/disabled',
  last_test_at  DATETIME     NULL COMMENT '最后连通性测试时间',
  created_by    VARCHAR(64)  NOT NULL DEFAULT '',
  created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_name (source_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据源';
```

#### 8.1.2 同步任务表

```sql
CREATE TABLE ds_sync_task (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  task_name    VARCHAR(128) NOT NULL,
  source_id    BIGINT UNSIGNED NOT NULL COMMENT 'FK→ds_source.id',
  source_table VARCHAR(128) NOT NULL COMMENT '源表',
  target_table VARCHAR(128) NOT NULL COMMENT '目标表',
  sync_mode    VARCHAR(16)  NOT NULL DEFAULT 'full' COMMENT 'full/incremental',
  increment_col VARCHAR(64) NOT NULL DEFAULT '' COMMENT '增量字段',
  field_mapping JSON        NULL COMMENT '字段映射',
  schedule     VARCHAR(64)  NOT NULL DEFAULT '' COMMENT 'cron',
  enabled      TINYINT      NOT NULL DEFAULT 1,
  created_by   VARCHAR(64)  NOT NULL DEFAULT '',
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_source (source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同步任务';
```

#### 8.1.3 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| CRUD | /api/v1/ds/sources | 数据源管理 |
| POST | /api/v1/ds/sources/{id}/test | 连通性测试 |
| CRUD | /api/v1/ds/sync-tasks | 同步任务管理 |
| POST | /api/v1/ds/sync-tasks/{id}/run | 手动执行同步 |
| GET | /api/v1/ds/sync-tasks/{id}/instances | 同步实例/日志 |

> 密码用 AES-256-GCM 加密（密钥存服务端环境变量），同 llm_config API Key 规范。员工加解密在前端回传。

---

### 8.2 数据安全模块（F8）

#### 8.2.1 敏感类型与脱敏规则表

```sql
CREATE TABLE sec_sensitive_rule (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  rule_name    VARCHAR(64)  NOT NULL COMMENT '规则名',
  field_pattern VARCHAR(255) NOT NULL DEFAULT '' COMMENT '字段名正则',
  data_pattern VARCHAR(255) NOT NULL DEFAULT '' COMMENT '数据正则',
  sensitive_type VARCHAR(32) NOT NULL COMMENT 'mobile/idcard/email/custom',
  mask_type    VARCHAR(32)  NOT NULL DEFAULT 'mask' COMMENT 'mask/hash/truncate',
  created_by   VARCHAR(64)  NOT NULL DEFAULT '',
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='敏感识别与脱敏规则';
```

#### 8.2.2 审计日志表

```sql
CREATE TABLE sec_audit_log (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_no     VARCHAR(64)  NOT NULL COMMENT '操作用户工号',
  action      VARCHAR(32)  NOT NULL COMMENT 'query/export/ddl/perm_change',
  target      VARCHAR(255) NOT NULL DEFAULT '' COMMENT '操作对象(表/SQL)',
  detail      TEXT         NULL COMMENT '详情',
  ip          VARCHAR(64)  NOT NULL DEFAULT '',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_user_time (user_no, created_at),
  KEY idx_action (action)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='安全审计日志';
```

#### 8.2.3 权限申请审批表（工单）

```sql
CREATE TABLE sec_access_request (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_no     VARCHAR(64)  NOT NULL COMMENT '申请人',
  table_id    BIGINT UNSIGNED NOT NULL COMMENT '申请访问的表',
  cols        JSON         NULL COMMENT '申请字段(空=全部)',
  reason      VARCHAR(512) NOT NULL DEFAULT '',
  status      VARCHAR(16)  NOT NULL DEFAULT 'pending' COMMENT 'pending/approved/rejected',
  approver    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '审批人',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  approved_at DATETIME     NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据访问申请审批';
```

#### 8.2.4 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| CRUD | /api/v1/sec/rules | 敏感/脱敏规则管理 |
| POST | /api/v1/sec/detect | 字段敏感识别 |
| GET | /api/v1/sec/audit | 审计日志检索 |
| POST | /api/v1/sec/access-requests | 数据访问申请 |
| POST | /api/v1/sec/access-requests/{id}/approve | 审批 |

#### 8.2.5 脱敏执行策略

- **检索期过滤**：敏感字段按用户角色在检索阶段即过滤，不到达 LLM 上下文（对齐出域红线）。
- **查询脱敏**：SELECT 敏感字段时按规则自动 masked 返回。
- **导出脱敏**：导出 Excel/CSV 同样脱敏。

---

### 8.3 数据资产模块（F9）

#### 8.3.1 资产标签表

```sql
CREATE TABLE asset_tag (
  id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  tag_name   VARCHAR(64)  NOT NULL,
  tag_type   VARCHAR(32)  NOT NULL DEFAULT 'biz' COMMENT 'biz/importance/timeliness',
  created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tag (tag_name, tag_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资产标签';
```

#### 8.3.2 资产-标签关联 / 收藏

```sql
CREATE TABLE asset_tag_rel (
  id      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  asset_type VARCHAR(16)  NOT NULL COMMENT 'table/field/metric',
  asset_id   BIGINT UNSIGNED NOT NULL,
  tag_id     BIGINT UNSIGNED NOT NULL,
  UNIQUE KEY uk_rel (asset_type, asset_id, tag_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资产标签关联';

CREATE TABLE asset_favorite (
  id        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_no   VARCHAR(64)  NOT NULL,
  asset_type VARCHAR(16) NOT NULL,
  asset_id  BIGINT UNSIGNED NOT NULL,
  created_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_fav (user_no, asset_type, asset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资产收藏';
```

#### 8.3.3 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/v1/assets | 资产目录/检索（聚合血缘+质量+owner） |
| CRUD | /api/v1/assets/tags | 标签管理 |
| POST | /api/v1/assets/{type}/{id}/tags | 打标签 |
| POST | /api/v1/assets/{type}/{id}/favorite | 收藏/取消 |
| GET | /api/v1/assets/stats | 资产统计 |

---

### 8.4 数据标准模块（F11）

#### 8.4.1 数据标准表

```sql
CREATE TABLE std_standard (
  id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  std_name   VARCHAR(128) NOT NULL COMMENT '标准名',
  std_type   VARCHAR(32)  NOT NULL COMMENT 'column/enum/dict',
  data_type  VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '标准类型',
  value_range VARCHAR(255) NOT NULL DEFAULT '' COMMENT '值域',
  format     VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '格式',
  owner      VARCHAR(64)  NOT NULL DEFAULT '',
  domain     VARCHAR(64)  NOT NULL DEFAULT '',
  created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_std (std_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据标准';
```

#### 8.4.2 落标映射表

```sql
CREATE TABLE std_mapping (
  id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  table_id   BIGINT UNSIGNED NOT NULL COMMENT '资产表',
  column_id  BIGINT UNSIGNED NOT NULL COMMENT '资产字段',
  std_id     BIGINT UNSIGNED NOT NULL COMMENT 'FK→std_standard.id',
  match_status VARCHAR(16) NOT NULL DEFAULT 'matched' COMMENT 'matched/mismatched',
  created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_map (column_id, std_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='落标映射';
```

#### 8.4.3 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| CRUD | /api/v1/std/standards | 数据标准管理 |
| POST | /api/v1/std/mapping | 建立落标映射 |
| GET | /api/v1/std/mapping?status=mismatched | 落标评估 |
| GET | /api/v1/std/dictionary | 数据字典/术语表 |

> 术语表与知识库 RAG 打通：`std_standard` 的枚举/术语，同步为 `kb_document(kb_type=TERM)` / `kb_alias`，共用一套源头避免重复维护。

---

### 8.5 生命周期管理

#### 8.5.1 生命周期策略表

```sql
CREATE TABLE asset_life_cycle (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  table_id    BIGINT UNSIGNED NOT NULL,
  retain_days INT          NOT NULL DEFAULT 1095 COMMENT '保留天数',
  archive_to  VARCHAR(255) NOT NULL DEFAULT '' COMMENT '归档位置',
  policy      VARCHAR(32)  NOT NULL DEFAULT 'retain' COMMENT 'retain/archive/purge',
  enabled     TINYINT      NOT NULL DEFAULT 1,
  created_by  VARCHAR(64)  NOT NULL DEFAULT '',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='生命周期策略';
```

#### 8.5.2 接口与 Celery

| 方法 | 路径 | 说明 |
|------|------|------|
| CRUD | /api/v1/assets/lifecycle | 生命周期策略 |
| - | Celery `lifecycle_check` | 每日扫描过期分区，触发归档/清理（对齐 archive_ 规范） |

---

> 本节补充 5 大治理闭环 + 数据开发/分析/服务模块，新增 18 张表。全库核心表合计 40 张（见 §9.1 表清单）。

---

### 8.6 数据开发模块（F10，On line IDE）

#### 8.6.1 设计思路

在线数据开发环境是 SQL 智能助手（F3）的"安身之所"，核心是：**编辑器 + 脚本任务 + 发布流程**。复用 F3 的解析/优化/改写能力，为分析师和开发提供统一编码入口。`Monaco Editor` 作为前端编辑器，后端提供裸 SQL 解析、方言校验与任务生命周期管理。

#### 8.6.2 脚本任务表（MySQL 新增）

```sql
CREATE TABLE dev_script (
  id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  script_name    VARCHAR(128) NOT NULL COMMENT '脚本名',
  engine         VARCHAR(32)  NOT NULL DEFAULT 'hive' COMMENT 'hive/spark_sql/python',
  content        LONGTEXT     NOT NULL COMMENT '脚本内容(SQL/Python)',
  version        INT          NOT NULL DEFAULT 1 COMMENT '版本号',
  status         VARCHAR(16)  NOT NULL DEFAULT 'draft' COMMENT 'draft/developed/published/archived',
  db_hint        VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '默认库',
  tags           VARCHAR(255) NOT NULL DEFAULT '' COMMENT '标签',
  owner          VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '负责人工号',
  created_by     VARCHAR(64)  NOT NULL DEFAULT '',
  created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_owner (owner),
  KEY idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据开发脚本';
```

#### 8.6.3 脚本版本历史表

```sql
CREATE TABLE dev_script_version (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  script_id   BIGINT UNSIGNED NOT NULL COMMENT 'FK→dev_script.id',
  version     INT          NOT NULL COMMENT '版本号',
  content     LONGTEXT     NOT NULL,
  changed_by  VARCHAR(64)  NOT NULL DEFAULT '',
  change_note VARCHAR(255) NOT NULL DEFAULT '' COMMENT '变更说明',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_script_ver (script_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='脚本版本历史';
```

#### 8.6.4 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| CRUD | /api/v1/dev/scripts | 脚本管理（新建/保存/列表/详情） |
| GET | /api/v1/dev/scripts/{id}/versions | 版本历史 |
| POST | /api/v1/dev/scripts/{id}/run | 手动运行（前台测试） |
| GET | /api/v1/dev/scripts/{id}/logs | 调试日志 |
| POST | /api/v1/dev/scripts/{id}/publish | 发布上线（对接 F6 调度） |
| POST | /api/v1/dev/scripts/{id}/validate | 语法/元数据校验（复用 F3.5） |

#### 8.6.5 与 F3 复用关系

- `/dev/scripts/{id}/validate` → 复用 `sql_parser` + 元数据白名单校验。
- `/dev/scripts/{id}/run` → 复用只读校验器（非 SELECT 拦截）。
- 编辑器前端联想 → 复用 `meta_index` 检索（表/字段补全）。

---

### 8.7 数据分析模块（F12）

#### 8.7.1 设计思路

面向分析师的"查询→可视化"闭环。核心：**SQL/NL2SQL 结果 → 图表自动推荐 → 轻量看板**。前端用 ECharts，后端只做图表类型建议与数据聚合组装，不引入重型 BI。

#### 8.7.2 看板表（MySQL 新增）

```sql
CREATE TABLE ana_dashboard (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  dash_name   VARCHAR(128) NOT NULL COMMENT '看板名',
  owner       VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '负责人',
  layout      JSON         NOT NULL COMMENT '看板布局(图表坐标)',
  status      VARCHAR(16)  NOT NULL DEFAULT 'active' COMMENT 'active/archived',
  created_by  VARCHAR(64)  NOT NULL DEFAULT '',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_owner (owner)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='自助分析看板';
```

```sql
CREATE TABLE ana_chart (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  dash_id      BIGINT UNSIGNED NOT NULL COMMENT 'FK→ana_dashboard.id',
  chart_name   VARCHAR(128) NOT NULL COMMENT '图表名',
  chart_type   VARCHAR(32)  NOT NULL DEFAULT 'auto' COMMENT 'auto/bar/line/pie/table',
  ds_query     TEXT         NOT NULL COMMENT '数据源SQL',
  query_type   VARCHAR(16)  NOT NULL DEFAULT 'sql' COMMENT 'sql/nl2sql',
  dimension    VARCHAR(255) NOT NULL DEFAULT '' COMMENT '维度字段',
  metric       VARCHAR(255) NOT NULL DEFAULT '' COMMENT '指标字段',
  options      JSON         NULL COMMENT '图表配置(过滤/排序)',
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_dash (dash_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='看板图表';
```

#### 8.7.3 图表类型自动推荐逻辑

| 输入特征 | 推荐图表 |
|---------|---------|
| 1 维度 + 1 指标，维度为离散（如平台/城市） | bar（柱状） |
| 1 维度为时间 + 1 指标 | line（折线） |
| 1 维度 + 多指标 | bar 分组 或 line 多线 |
| 2 维度 | pie（饼图）或 heatmap |
| 无维度（仅聚合指标） | table + 卡片 |

#### 8.7.4 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/v1/ana/chart/recommend | 图表类型推荐（输入SQL/维度/指标） |
| CRUD | /api/v1/ana/dashboards | 看板管理 |
| CRUD | /api/v1/ana/charts | 图表管理（挂看板） |
| POST | /api/v1/ana/charts/{id}/query | 图表查询（执行底层SQL） |
| GET | /api/v1/ana/dashboards/{id}/render | 看板渲染（并行查询所有图表） |
| GET | /api/v1/ana/export | 结果导出（Excel/CSV，受 F8 脱敏约束） |

> 导出与查询统一过只读校验 + 脱敏（见 F8.2.5）。

---

### 8.8 数据服务模块（F13）

#### 8.8.1 设计思路

将治理后的表/指标封装为可调用的数据 API，供外部系统程序化取数。试点期为核心较弱模块，**提供最小可用闭环**：API 发布 → 访问控制 → 调用监控。按 F13 属 P2，此详设满足"预留接口、可扩展"目标。

#### 8.8.2 API 服务表（MySQL 新增）

```sql
CREATE TABLE svc_api (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  api_name     VARCHAR(128) NOT NULL COMMENT 'API名',
  api_path     VARCHAR(255) NOT NULL COMMENT '访问路径(唯一)',
  api_method   VARCHAR(8)   NOT NULL DEFAULT 'GET' COMMENT 'GET/POST',
  source_type  VARCHAR(16)  NOT NULL DEFAULT 'table' COMMENT 'table/metric/custom_sql',
  source_db    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '源库',
  source_table VARCHAR(128) NOT NULL DEFAULT '' COMMENT '源表',
  source_sql   TEXT         NULL COMMENT '自定义SQL(仅SELECT)',
  params       JSON         NULL COMMENT '入参定义(如日期/维度)',
  owner        VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '负责人',
  status       VARCHAR(16)  NOT NULL DEFAULT 'draft' COMMENT 'draft/published/offline',
  version      INT          NOT NULL DEFAULT 1,
  rate_limit   INT          NOT NULL DEFAULT 100 COMMENT '限流(QPS)',
  acl_level    VARCHAR(32)  NOT NULL DEFAULT 'L1_INTERNAL',
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_path (api_path)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据API服务';
```

#### 8.8.3 调用监控表

```sql
CREATE TABLE svc_call_log (
  id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  api_id     BIGINT UNSIGNED NOT NULL COMMENT 'FK→svc_api.id',
  caller     VARCHAR(128) NOT NULL DEFAULT '' COMMENT '调用方(appKey/账号)',
  status     SMALLINT     NOT NULL DEFAULT 200 COMMENT 'HTTP状态',
  latency_ms INT          NOT NULL DEFAULT 0,
  row_count  INT          NOT NULL DEFAULT 0 COMMENT '返回行数',
  created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_api_time (api_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='API调用监控';
```

#### 8.8.4 AppKey 访问控制表

```sql
CREATE TABLE svc_appkey (
  id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  app_name   VARCHAR(128) NOT NULL,
  app_key    VARCHAR(128) NOT NULL COMMENT '调用方标识',
  app_secret_enc VARCHAR(512) NOT NULL DEFAULT '' COMMENT '加密密钥',
  api_ids    JSON         NULL COMMENT '授权API列表',
  enabled    TINYINT      NOT NULL DEFAULT 1,
  created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='API调用方AppKey';
```

#### 8.8.5 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| CRUD | /api/v1/svc/apis | API 发布管理 |
| POST | /api/v1/svc/apis/{id}/publish | 发布/下线 |
| CRUD | /api/v1/svc/appkeys | AppKey 管理 |
| GET | /api/v1/svc/apis/{id}/calls | 调用监控（量/成功率/耗时） |
| 运行时 | /api/v1/svc/run/{api_path} | 供外部调用的数据API（限流+鉴权+脱敏） |

#### 8.8.6 运行时安全

- **鉴权**：请求带 `app_key + 签名`，校验通过才能调用。
- **限流**：按 `rate_limit` 做 QPS 限流 + 熔断。
- **只读**：`source_sql` 必须过只读校验器（非 SELECT 拒绝）。
- **脱敏**：返回字段按 `acl_level` 与用户权限脱敏（复用 F8）。

---

---

## 9. 目录与表统计

| 版本 | 变化 |
|------|------|
| v1.4 | 新增第8章：数据集成/安全/资产/标准/开发/分析/服务/生命周期；新增表 |

### 9.1 全库表清单（本章节涉及）

| 所属模块 | 表 | 章节 |
|---------|-----|------|
| 元数据 | meta_database / meta_table / meta_column / meta_partition / meta_lineage / meta_snapshot | §2 |
| 质量 | quality_rule / quality_rule_result | §2 |
| 调度 | task_instance / task_log | §2 |
| 问答 | qa_conversation | §2 |
| 通知 | notify_message | §2 |
| LLM | llm_config | §2 |
| 用户 | sys_user | §2 |
| 知识库 | kb_document / kb_alias / kb_sql_example / kb_chunk / kb_embedding / kb_feedback / kb_retrieval_log / kb_review_task / kb_source | §5 |
| 数据集成 | ds_source / ds_sync_task | §8.1 |
| 数据安全 | sec_sensitive_rule / sec_audit_log / sec_access_request | §8.2 |
| 数据资产 | asset_tag / asset_tag_rel / asset_favorite / asset_life_cycle | §8.3/§8.5 |
| 数据标准 | std_standard / std_mapping | §8.4 |
| 数据开发 | dev_script / dev_script_version | §8.6 |
| 数据分析 | ana_dashboard / ana_chart | §8.7 |
| 数据服务 | svc_api / svc_call_log / svc_appkey | §8.8 |

> 全库核心表合计 **40 张**（基础 13 张 + 知识库 9 张 + 治理闭环 18 张：集成 2 + 安全 3 + 资产 3 + 生命周期 1 + 标准 2 + 开发 2 + 分析 2 + 服务 3）。

---

> 本文档为可直接开发的完整工程 Spec。数据库 DDL 可直接执行；API 出入参已定义到 Pydantic 模型；千问网关协议含安全校验与降级策略；知识库（RAG）用于增强 NL2SQL 生成质量；第8章补齐治理闭环模块（数据集成/安全/资产/标准/开发/分析/服务/生命周期）。