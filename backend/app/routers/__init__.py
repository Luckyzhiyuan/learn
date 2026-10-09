"""路由层。"""

from app.routers import (
    metadata, sql_assistant, quality, search, lineage,
    schedule, notify, llm, auth,
)

Routers = [
    auth.router, metadata.router, sql_assistant.router, quality.router,
    search.router, lineage.router, schedule.router,
    notify.router, llm.router,
]