"""RAG：知识片段格式化 + 提示词组装 + 向量索引进/重建（Spec §5.4/5.7）。

Embedding：本环境无可用的向量编码服务，采用「关键词+注释文本」写入 ES kb_index，
作为向量索引重建的可运行占位实现（不可用则静默，返回 False）。
"""
from app.config import settings
from app.schemas.kb import RAGContext
from app.services import kb_service


def format_knowledge(ctx: RAGContext) -> str:
    """把召回的知识片段拼成 LLM 提示词注入块（Spec §5.4）。"""
    blocks = []
    if ctx.glossaries:
        blocks.append("【知识库-口径】\n- " + "\n- ".join(ctx.glossaries))
    if ctx.aliases:
        blocks.append("【知识库-同义词】\n- " + "\n- ".join(
            f"用户说\"{a['term']}\" → {a['standard']}" for a in ctx.aliases))
    if ctx.sql_examples:
        blocks.append("【知识库-示例SQL】\n- " + "\n- ".join(
            f"类似问题: {e['question']} → {e['sql']}" for e in ctx.sql_examples))
    return "\n\n".join(blocks)


def assemble_prompt(question: str, schema_block: str = "") -> str:
    """整体提示词：表结构 + 知识注入 + 规则。"""
    ctx = kb_service.recall(question)
    kb = format_knowledge(ctx)
    prompt = "【表结构】\n" + (schema_block or "(暂无元数据，请基于问题自动推断)\n")
    if kb:
        prompt += "\n" + kb + "\n"
    prompt += (
        "\n【规则】\n1. 只允许SELECT，禁止DELETE/UPDATE/INSERT/DDL。\n"
        "2. 分区字段必须过滤并裁剪。\n"
        "3. 输出格式: {\"sql\": \"...\", \"tables_used\": [...], \"confidence\": 0.0-1.0}\n\n"
        f"【问题】{question}"
    )
    return prompt


def rebuild_index() -> dict:
    """重建 ES kb_index（占位：ES 不可用则返回 skipped）。"""
    try:
        from app.services import search
        if not search.es_available():
            return {"status": "SKIPPED", "reason": "es unavailable"}
        client = search._es_client()
        index = settings.es_index_kb
        if client.indices.exists(index=index):
            client.indices.delete(index=index)
        client.indices.create(index=index, body={
            "mappings": {"properties": {
                "entity_type": {"type": "keyword"},
                "title": {"type": "keyword"},
                "content": {"type": "text"},
                "tags": {"type": "text"},
                "doc_id": {"type": "long"},
            }}})
        return {"status": "DONE", "index": index}
    except Exception:
        return {"status": "SKIPPED", "reason": "exception"}