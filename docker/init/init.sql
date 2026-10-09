-- =============================================================
-- 智能数仓平台 · MySQL 8.0 数据库 DDL (对应工程Spec §2)
-- 统一 utf8mb4 / InnoDB。
-- 该文件由 db/init.sql / docker-entrypoint-initdb.d 引用，首次建库自动执行。
-- =============================================================

SET NAMES utf8mb4;

-- ---------- 2.1 库目录 ----------
CREATE TABLE IF NOT EXISTS meta_database (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  db_name     VARCHAR(128)  NOT NULL,
  layer       VARCHAR(32)   NOT NULL DEFAULT '' ,
  owner       VARCHAR(64)   NOT NULL DEFAULT '' ,
  location    VARCHAR(512)  NOT NULL DEFAULT '' ,
  remark      VARCHAR(255)  NOT NULL DEFAULT '' ,
  created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_db_name (db_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.2 表 ----------
CREATE TABLE IF NOT EXISTS meta_table (
  id                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  db_id             BIGINT UNSIGNED  NOT NULL,
  table_name        VARCHAR(128)     NOT NULL,
  comment           VARCHAR(512)     NOT NULL DEFAULT '' ,
  table_type        VARCHAR(32)      NOT NULL DEFAULT 'MANAGED',
  storage_format    VARCHAR(32)      NOT NULL DEFAULT 'ORC',
  compress          VARCHAR(32)      NOT NULL DEFAULT 'SNAPPY',
  is_partitioned    TINYINT          NOT NULL DEFAULT 0,
  row_count         BIGINT           NOT NULL DEFAULT 0,
  table_size_bytes  BIGINT           NOT NULL DEFAULT 0,
  file_count        BIGINT           NOT NULL DEFAULT 0,
  little_file_count BIGINT           NOT NULL DEFAULT 0,
  owner             VARCHAR(64)      NOT NULL DEFAULT '' ,
  location          VARCHAR(512)     NOT NULL DEFAULT '' ,
  source_mode       VARCHAR(16)      NOT NULL DEFAULT 'file',
  created_at        DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_db_table (db_id, table_name),
  KEY idx_owner (owner)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.3 字段 ----------
CREATE TABLE IF NOT EXISTS meta_column (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  table_id     BIGINT UNSIGNED NOT NULL,
  column_name  VARCHAR(128)    NOT NULL,
  column_type  VARCHAR(128)    NOT NULL DEFAULT '' ,
  comment      VARCHAR(512)    NOT NULL DEFAULT '' ,
  is_partition TINYINT         NOT NULL DEFAULT 0,
  is_primary   TINYINT         NOT NULL DEFAULT 0,
  ordinal      INT             NOT NULL DEFAULT 0,
  created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_table_column (table_id, column_name),
  KEY idx_comment (comment(64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.4 分区 ----------
CREATE TABLE IF NOT EXISTS meta_partition (
  id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  table_id        BIGINT UNSIGNED NOT NULL,
  partition_key   VARCHAR(64)     NOT NULL,
  partition_value VARCHAR(64)     NOT NULL,
  size_bytes      BIGINT          NOT NULL DEFAULT 0,
  row_count       BIGINT          NOT NULL DEFAULT 0,
  file_count      BIGINT          NOT NULL DEFAULT 0,
  updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_table_partition (table_id, partition_key, partition_value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.5 血缘 ----------
CREATE TABLE IF NOT EXISTS meta_lineage (
  id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  src_table_id    BIGINT UNSIGNED NOT NULL,
  dst_table_id    BIGINT UNSIGNED NOT NULL,
  edge_tag        VARCHAR(32)     NOT NULL DEFAULT 'sql_parse',
  task_id         BIGINT          NULL,
  project_id      BIGINT          NULL,
  job_type        VARCHAR(32)     NOT NULL DEFAULT '' ,
  owner           VARCHAR(64)     NOT NULL DEFAULT '' ,
  schedule_status VARCHAR(32)     NOT NULL DEFAULT '' ,
  schedule_time   DATETIME        NULL,
  source          VARCHAR(16)     NOT NULL DEFAULT 'offline',
  field_level     JSON            NULL,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_edge (src_table_id, dst_table_id, task_id),
  KEY idx_dst (dst_table_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.6 快照 ----------
CREATE TABLE IF NOT EXISTS meta_snapshot (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  table_id      BIGINT UNSIGNED NOT NULL,
  snapshot_time DATETIME        NOT NULL,
  snapshot_json JSON            NOT NULL,
  diff_json     JSON            NULL,
  KEY idx_table_time (table_id, snapshot_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.7 质量规则 ----------
CREATE TABLE IF NOT EXISTS quality_rule (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  rule_name    VARCHAR(128)  NOT NULL,
  rule_type    VARCHAR(32)   NOT NULL,
  table_id     BIGINT UNSIGNED NOT NULL,
  column_id    BIGINT UNSIGNED NULL,
  params       JSON          NULL,
  sql          TEXT          NULL,
  threshold    DECIMAL(20,4) NULL,
  enabled      TINYINT       NOT NULL DEFAULT 1,
  created_by   VARCHAR(64)   NOT NULL DEFAULT '' ,
  updated_by   VARCHAR(64)   NOT NULL DEFAULT '' ,
  created_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_table (table_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.8 质量结果 ----------
CREATE TABLE IF NOT EXISTS quality_rule_result (
  id                    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  rule_id               BIGINT UNSIGNED NOT NULL,
  execute_time          DATETIME        NOT NULL,
  execute_instance_id   VARCHAR(64)     NOT NULL DEFAULT '' ,
  pass                  TINYINT         NOT NULL DEFAULT 1,
  actual_value          DECIMAL(20,4)   NULL,
  detail                TEXT            NULL,
  notified              TINYINT         NOT NULL DEFAULT 0,
  KEY idx_rule_time (rule_id, execute_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.9 任务实例 ----------
CREATE TABLE IF NOT EXISTS task_instance (
  id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  task_id        BIGINT         NOT NULL,
  instance_id    VARCHAR(64)    NOT NULL,
  status         VARCHAR(32)    NOT NULL DEFAULT 'SUBMITTED',
  app_id         VARCHAR(128)   NOT NULL DEFAULT '' ,
  history_url    VARCHAR(512)   NOT NULL DEFAULT '' ,
  start_time     DATETIME       NULL,
  end_time       DATETIME       NULL,
  log_text       MEDIUMTEXT     NULL,
  created_at     DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_task_status (task_id, status),
  KEY idx_instance (instance_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.10 问答会话 ----------
CREATE TABLE IF NOT EXISTS qa_conversation (
  id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_id        VARCHAR(64)    NOT NULL,
  query_type     VARCHAR(16)    NOT NULL,
  question       TEXT           NOT NULL,
  generated_sql  TEXT           NULL,
  confirmed      TINYINT        NOT NULL DEFAULT 0,
  executed       TINYINT        NOT NULL DEFAULT 0,
  llm_provider   VARCHAR(16)    NOT NULL DEFAULT 'qwen3-8b',
  fallback_used  TINYINT        NOT NULL DEFAULT 0,
  result_summary TEXT           NULL,
  feedback       TINYINT        NULL,
  created_at     DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_user_time (user_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.11 通知 ----------
CREATE TABLE IF NOT EXISTS notify_message (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_id     VARCHAR(64)    NOT NULL,
  title       VARCHAR(255)   NOT NULL,
  content     TEXT           NOT NULL,
  biz_type    VARCHAR(32)    NOT NULL DEFAULT 'quality',
  ref_id      BIGINT         NULL,
  read_flag   TINYINT        NOT NULL DEFAULT 0,
  created_at  DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_user_read (user_id, read_flag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.12 LLM 配置 ----------
CREATE TABLE IF NOT EXISTS llm_config (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  provider     VARCHAR(32)  NOT NULL DEFAULT 'qwen',
  model_name   VARCHAR(64)  NOT NULL DEFAULT 'qwen3-8b',
  api_url      VARCHAR(512) NOT NULL,
  api_key_enc  VARCHAR(512) NOT NULL,
  enabled      TINYINT      NOT NULL DEFAULT 1,
  fallback     TINYINT      NOT NULL DEFAULT 1,
  temperature  DECIMAL(3,2) NOT NULL DEFAULT 0.00,
  max_tokens   INT          NOT NULL DEFAULT 2048,
  timeout_sec  INT          NOT NULL DEFAULT 30,
  updated_by   VARCHAR(64)  NOT NULL DEFAULT '' ,
  updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 2.13 用户 ----------
CREATE TABLE IF NOT EXISTS sys_user (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  employee_no   VARCHAR(64)  NOT NULL,
  user_name     VARCHAR(128) NOT NULL,
  role          VARCHAR(32)  NOT NULL DEFAULT 'dev',
  created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_emp (employee_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 3. 调度任务表（Spec §8.3 task_instance/task_log 补充） ----------
CREATE TABLE IF NOT EXISTS task_info (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  task_name     VARCHAR(255) NOT NULL,
  sql           MEDIUMTEXT   NULL,
  owner         VARCHAR(64)  NOT NULL DEFAULT '' ,
  schedule_cron VARCHAR(64)  NOT NULL DEFAULT '' ,
  status        VARCHAR(32)  NOT NULL DEFAULT 'CREATED',
  created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- 初始化内置用户 ----------
INSERT INTO sys_user (employee_no, user_name, role) VALUES
  ('1207799', '张三', 'admin'),
  ('1001001', '李四', 'dev'),
  ('2002002', '王五', 'analyst')
ON DUPLICATE KEY UPDATE user_name = VALUES(user_name);