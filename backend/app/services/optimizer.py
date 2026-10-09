"""SQL 优化规则引擎（Spec §7.3 / F3.2）。

规则：MUST DO / MUST NOT DO。
"""
import sqlglot
from sqlglot import exp

from app.core.exceptions import sql_parse_error
from app.services.sql_parser import is_read_only, parse_sql


class OptRule:
    def __init__(self, key, level, message, matcher):
        self.key = key
        self.level = level  # must_do / must_not / warning
        self.message = message
        self.matcher = matcher  # callable(stmt) -> bool 命中


# ---- 检测函数 ----
def _has_select_star(stmt):
    return any(True for _ in stmt.find_all(exp.Star))


def _has_function_on_partition(stmt, partition_keys=("dt", "day", "month", "year")):
    # 简化：查找 WHERE/GROUP BY 中类似函数(dt) 或 FORMAT(dt)
    def check(expr):
        if isinstance(expr, exp.Func):
            args = list(expr.args.values())
            for a in args:
                if isinstance(a, exp.Column) and a.name in partition_keys:
                    # 排除多层包裹（如 DATE_FORMAT(DT) 视为函数套分区）
                    return True
        return False
    return bool(stmt.walk().filter(lambda e: isinstance(e, exp.Func) and check(e)) or False)


def _has_join(stmt):
    return any(True for _ in stmt.find_all(exp.Join))


def _has_cast_where(stmt):
    # 分区列套 CAST/DATE_FORMAT：检测 where 中的函数
    conds = stmt.find_all(exp.Where)
    for w in conds:
        for fn in w.find_all(exp.Func):
            args = [a for a in fn.args.values() if isinstance(a, exp.Column)]
            if any(getattr(a, "name", "") in ("dt", "day") for a in args):
                return True
    return False


def _referenced(db, expr):  # 简化模型推荐预留
    return False


RULES = {
    "no_select_star": OptRule(
        "no_select_star", "must_not",
        "禁止 SELECT *，应显式列出所需字段（列裁剪）",
        lambda s: _has_select_star(s)),
    "no_func_on_partition": OptRule(
        "no_func_on_partition", "must_not",
        "禁止对分区字段套函数，会破坏分区裁剪（Partition Pruning）",
        lambda s: _has_cast_where(s)),
    "prefer_join_condition": OptRule(
        "prefer_join_condition", "warning",
        "检查 ON 条件，确保关联键已建索引 / 合理",
        lambda s: _has_join(s)),
    "partition_pruning": OptRule(
        "partition_pruning", "must_do",
        "若分区表未过滤分区，建议在 WHERE 中增加分区裁剪",
        lambda s: not _has_partition_where(s)),
    "cache_cte_warning": OptRule(
        "cache_cte_warning", "warning",
        "如存在被多次引用的 CTE/子查询，建议物化缓存避免重复扫描",
        lambda s: _duplicate_cte(s)),
}


def _has_partition_where(stmt):
    for col in stmt.find_all(exp.Column):
        if col.name in ("dt", "day", "month", "year", "hour"):
            return True
    # 简化：关键词匹配
    if re_search_dt(stmt.sql()):
        return True
    return False


import re as _re


def re_search_dt(sql):
    return bool(_re.search(r"(dt|day)\s*[=<>]", sql, _re.I))


def _duplicate_cte(stmt):
    # 统计 CTE 名出现次数 >1
    import collections
    cte_names = [x.alias_or_name for x in stmt.find_all(exp.CTE)]
    full = stmt.sql()
    cnt = collections.Counter(cte_names)
    return any(cnt[n] > 1 for n in cnt)


def optimize(sql: str) -> dict:
    """返回 { diagnosis, suggestions }。"""
    sql_parse_error_if_bad(sql)
    try:
        stmt = sqlglot.parse_one(sql, read="hive")
    except Exception as exc:
        sql_parse_error(f"SQL 语法解析失败: {exc}")

    suggestions = []
    diagnosis = []
    for key, rule in RULES.items():
        if rule.matcher(stmt):
            suggestions.append({"rule": rule.key, "level": rule.level, "message": rule.message})

    # 组装 diagnosis（简化，来自规则命中）
    severity_map = {"must_not": "high", "must_do": "medium", "warning": "low"}
    for s in suggestions:
        diagnosis.append({
            "issue": s["message"],
            "severity": severity_map.get(s["level"], "medium"),
            "detail": s["rule"],
        })
    # 去重诊断
    seen, dedup = set(), []
    for d in diagnosis:
        if d["issue"] not in seen:
            seen.add(d["issue"])
            dedup.append(d)
    return {"diagnosis": dedup, "suggestions": suggestions}


def sql_parse_error_if_bad(sql):
    if not is_read_only(sql):
        # 非只读但也可能合法（如先校验），此处留空，上头单独处理
        pass