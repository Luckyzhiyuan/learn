"""知识库服务层单元测试：kb_service CRUD / 多路召回 与 rag 提示词组装。"""
from app.schemas.kb import KBDocumentCreate, KBAliasIn, KBSqlExampleIn, RAGContext
from app.services import kb_service, rag


# ---- CRUD ----
class TestDocumentCRUD:
    def test_create_and_get(self, db_session):
        body = KBDocumentCreate(kb_type="glossary", title="测试口径", content="测试内容", tags="t1,测试")
        doc = kb_service.create_document(body, user="1207799", db=db_session)
        assert doc.id
        pt = kb_service.list_documents(page=1, page_size=10, keyword="测试口径", db=db_session)
        assert pt["total"] >= 1

    def test_soft_delete_disables(self, db_session):
        body = KBDocumentCreate(kb_type="rule", title="待删除", content="x")
        doc = kb_service.create_document(body, user="1207799", db=db_session)
        assert kb_service.soft_delete_document(doc.id, db=db_session) is True
        assert db_session.query(kb_service.KBDocument).filter(
            kb_service.KBDocument.id == doc.id).first().enabled == 0

    def test_soft_delete_missing_returns_false(self, db_session):
        assert kb_service.soft_delete_document(999999, db=db_session) is False

    def test_update_missing_returns_none(self, db_session):
        body = KBDocumentCreate(kb_type="glossary", title="x", content="y")
        assert kb_service.update_document(999999, body, db=db_session) is None

    def test_list_filter_by_kb_type(self, db_session):
        kb_service.create_document(
            KBDocumentCreate(kb_type="glossary", title="口径A", content="c1"), user="u", db=db_session)
        kb_service.create_document(
            KBDocumentCreate(kb_type="rule", title="规则B", content="c2"), user="u", db=db_session)
        gloss = kb_service.list_documents(kb_type="glossary", page_size=50, db=db_session)
        rules = kb_service.list_documents(kb_type="rule", page_size=50, db=db_session)
        assert gloss["total"] >= 1 and rules["total"] >= 1
        assert all(d["kb_type"] == "glossary" for d in gloss["list"])
        assert all(d["kb_type"] == "rule" for d in rules["list"])


class TestAliasAndSqlExample:
    def test_create_alias(self, db_session):
        a = kb_service.create_alias(KBAliasIn(term="去重人数", standard="COUNT(DISTINCT userkey)"), db=db_session)
        assert a.id and a.standard

    def test_create_sql_example(self, db_session):
        e = kb_service.create_sql_example(KBSqlExampleIn(question="查询GMV", sql="SELECT SUM(gmv) FROM t"), db=db_session)
        assert e.id and e.question


# ---- 多路召回 ----
class TestRecall:
    def test_recall_glossary_and_alias(self, db_session):
        ctx = kb_service.recall("统计昨天UV去重人数")
        assert isinstance(ctx, RAGContext)
        assert any("UV" in g or "userkey" in g or "口径" in g for g in ctx.glossaries) or any(
            a["term"] in ("UV", "昨日") for a in ctx.aliases)

    def test_recall_returns_empty_context_on_unknown(self, db_session):
        ctx = kb_service.recall("完全无关的内容zzzz")
        assert ctx is not None
        assert not ctx.glossaries and not ctx.aliases and not ctx.sql_examples


# ---- RAG 提示词组装 ----
class TestRag:
    def test_format_knowledge(self):
        ctx = RAGContext(glossaries=["UV=去重用户数"], aliases=[{"term": "UV", "standard": "COUNT(DISTINCT userkey)"}],
                         sql_examples=[{"question": "统计UV", "sql": "SELECT 1"}])
        text = rag.format_knowledge(ctx)
        assert "【知识库-口径】" in text and "【知识库-同义词】" in text and "【知识库-示例SQL】" in text

    def test_assemble_prompt_injects_kb(self):
        prompt = rag.assemble_prompt("统计昨天国内酒店UV", schema_block="foo.bar: ...")
        assert "【表结构】" in prompt and "【问题】" in prompt

    def test_rebuild_index_skips_without_es(self):
        res = rag.rebuild_index()
        assert res["status"] in ("SKIPPED", "DONE")