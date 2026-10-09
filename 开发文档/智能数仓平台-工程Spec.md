# 智能数仓平台 —— 完整工程 Spec

> 版本：v1.1（工程可实现版）
> 日期：2026-10-08
> 前置文档：`智能数仓平台-功能开发文档.md`（v1.0 决策确认稿）
> 技术栈：Python 3.11 + FastAPI + SQLAlchemy 2.0 / Pydantic v2 + Celery + Redis + MySQL 8.0 + Elasticsearch + Spark 3.X + 千问 Qwen 3.8
> 目标读者：后端开发、前端开发、测试、架构师

---

## 目录

1. [工程结构与模块划分](#1-工程结构与模块划分)
2. [数据库 DDL 全集（MySQL 8.0）](#2-数据库-ddl-全集mysql-80)
3. [REST API 规范与出入参定义](#3-rest-api-规范与出入参定义)
4. [千问 Qwen 3.8 LLM 网关交互协议](#4-千问-qwen-38-llm-网关交互协议)
5. [Celery 异步任务设计](#5-celery-异步任务设计)
6. [错误码与异常规范](#6-错误码与异常规范)

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
│   │   └── nl2sql_service.py    # NL2SQL 编排（schema注入+校验+降级）
│   ├── tasks/                   # Celery 任务
│   │   ├── metadata_sync.py
│   │   ├── quality_run.py
│   │   └── lineage_build.py
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

## 5. Celery 异步任务设计

| 任务 | 队列 | 说明 |
|------|------|------|
| `metadata_sync` | metadata | 元数据同步（文件/hive 双模式，支持增量） |
| `lineage_build` | lineage | 血缘自建（离线回填/增量解析） |
| `quality_run` | quality | 质量规则执行（提交 Spark，写入结果，超限发通知） |
| `nl2sql_feedback` | search | 用户反馈异步落库 |

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

## 6. 错误码与异常规范

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
| 50000 | 服务器内部错误 | 500 |

异常统一由 `core/exceptions.py` 抛出，`main.py` 全局 handler 捕获并包装为 `{code,message,data}`。

---

> 本文档为可直接开发的完整工程 Spec。数据库 DDL 可直接执行；API 出入参已定义到 Pydantic 模型；千问网关协议含安全校验与降级策略。