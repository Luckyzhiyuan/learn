"""智能建模：分层规范校验 + 建表 SQL 生成（Spec §7.2 / F2）。

内置酒旅数仓规范：分层 lib 映射、命名词根、分区、ORC+SNAPPY、字段注释。
"""
import re


LAYER_PREFIX = {
    "dim": "dim", "dwd": "dwd", "ads": "ads", "etl": "etl",
    "tmp": "tmp", "rpt": "rpt",
}
KNOWN_TOPICS = ["hotel", "mem", "order", "flow", "ubt", "pay", "user"]

PARTITION_KEYS = ("day", "month", "year", "hour", "area")
PARTITION_COMMENT = {"day": "日期yyyyMMdd", "month": "月份yyyyMM", "year": "年份yyy",
                     "hour": "小时HH", "area": "区域"}


def _snake_words(name: str) -> list[str]:
    parts = re.split(r"[_\s]+", name.strip().lower())
    return [p for p in parts if p]


def validate(db_name: str, table_comment: str, columns: list[dict],
             partition_by: str = "day", storage_format: str = "ORC") -> dict:
    violations, warnings = [], []
    layer = db_name.split("_")[0] if db_name else ""

    # 1. 分层
    allowed_layers = set(LAYER_PREFIX.values())
    if layer and layer not in allowed_layers:
        violations.append({"rule": "分层规范", "level": "error",
                           "message": f"库名 {db_name} 分层 {layer} 非法，应为 {sorted(allowed_layers)}"})
    # 2. 存储格式必须 ORC
    if storage_format.upper() != "ORC":
        violations.append({"rule": "存储规范", "level": "error",
                           "message": "存储格式必须为 ORC"})
    # 3. 分区合法
    if partition_by not in PARTITION_KEYS:
        warnings.append({"rule": "分区规范", "message": f"分区字段 {partition_by} 非标准粒度，建议 {PARTITION_KEYS}"})
    # 4. 字段注释规范
    for col in columns:
        if not col.get("comment"):
            warnings.append({"rule": "注释规范", "message": f"字段 {col.get('name')} 缺少 COMMENT"})
    # 5. 命名：主题+实体 词根（表名应含主题 topic）
    if table_comment:
        topic_present = any(t in table_comment for t in KNOWN_TOPICS)
        if not topic_present and layer:
            warnings.append({"rule": "命名规范", "message": "表注释未匹配已知主题词根(hotel/mem/order/flow/ubt/pay/user)"})

    valid = not any(v["level"] == "error" for v in violations)
    return {"valid": valid, "violations": violations, "warnings": warnings}


def _partition_comment(key: str) -> str:
    return PARTITION_COMMENT.get(key, key)


def generate(db_name: str, table_name: str, table_comment: str,
             columns: list[dict], partition_by: str = "day",
             storage_format: str = "ORC", compress: str = "SNAPPY") -> str:
    def to_hive_type(t: str) -> str:
        m = re.search(r"(string|bigint|int|double|decimal\(\d+,\d+\)|date|timestamp)", t, re.I)
        return (m.group(1) if m else "string").lower()

    col_lines = []
    pk_line = None
    for c in columns:
        ctype = to_hive_type(c.get("type", "string"))
        comment = c.get("comment", "")
        cname = c.get("name")
        col_lines.append(f"  {cname} {ctype}" + (f" COMMENT '{comment}'" if comment else ""))
        if c.get("is_primary"):
            pk_line = cname
    cols_sql = ",\n".join(col_lines)
    part_line = f"  {partition_by} STRING COMMENT '{_partition_comment(partition_by)}'"

    sql = (f"CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (\n"
           f"{cols_sql}\n"
           f")\nCOMMENT '{table_comment or ''}'"
           f"\nPARTITIONED BY (\n{part_line}\n)"
           f"\nSTORED AS {storage_format.upper()}"
           f"\nTBLPROPERTIES ({_compress(compress)})")
    return sql


def _compress(c: str) -> str:
    return f"'orc.compress'='{c.upper()}'" if c.upper() != "NONE" else "'orc.compress'='ZLIB'"