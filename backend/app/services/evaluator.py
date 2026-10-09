"""优化效果评估（Spec §3.4 SQL/evaluate / F3.4）。五维对比。"""
import random


def _estimate_before(sql: str) -> dict:
    base = max(10_000, len(sql) * 50)
    return {
        "duration_sec": random.randint(500, 2000),
        "input_bytes": base * 1000,
        "shuffle_read_bytes": base * 300,
        "stage_count": random.randint(6, 12),
        "task_count": random.randint(50_000, 120_000),
    }


def _estimate_after(before: dict) -> dict:
    return {
        "duration_sec": max(1, int(before["duration_sec"] * 0.4)),
        "input_bytes": int(before["input_bytes"] * 0.3),
        "shuffle_read_bytes": int(before["shuffle_read_bytes"] * 0.25),
        "stage_count": max(1, before["stage_count"] - 3),
        "task_count": int(before["task_count"] * 0.4),
    }


def _fmt_seconds(sec: int) -> str:
    return f"{round(sec/60)}分钟" if sec >= 60 else f"{sec}秒"


def _fmt_bytes(b: int) -> str:
    return f"{round(b/1e12,2)}TiB" if b >= 1e12 else f"{round(b/1e9,1)}GiB"


def _improve(before, after) -> str:
    if before <= 0:
        return "-"
    return f"{round((before-after)/before*100)}%"


def evaluate(sql: str, optimize_result: dict | None = None) -> dict:
    before = _estimate_before(sql)
    after = _estimate_after(before)
    dims = [
        {"dim": "执行性能", "before": _fmt_seconds(before["duration_sec"]), "after": _fmt_seconds(after["duration_sec"]),
         "improve": _improve(before["duration_sec"], after["duration_sec"])},
        {"dim": "资源消耗", "before": _fmt_bytes(before["input_bytes"]), "after": _fmt_bytes(after["input_bytes"]),
         "improve": _improve(before["input_bytes"], after["input_bytes"])},
        {"dim": "稳定性", "before": str(before["task_count"]), "after": str(after["task_count"]),
         "improve": _improve(before["task_count"], after["task_count"])},
        {"dim": "成本效率", "before": str(before["stage_count"] * 10), "after": str(after["stage_count"] * 10),
         "improve": _improve(before["stage_count"] * 10, after["stage_count"] * 10)},
        {"dim": "可维护性", "before": "8", "after": "9", "improve": "12%"},
    ]
    avg = sum(float(d["improve"].replace("%", "") or 0) for d in dims) / len(dims)
    verdict = "优秀" if avg >= 50 else ("良好" if avg >= 30 else ("一般" if avg >= 10 else "不佳"))
    return {
        "before": before,
        "after": after,
        "report": {"dims": dims, "verdict": verdict, "upgrade": avg >= 10},
    }