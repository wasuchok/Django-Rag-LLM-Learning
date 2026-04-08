from django.urls import path
from .views import (
    analyze_mt_job_card_problem_view,
    chat_with_local_model,
    feedback_summary_view,
    health_check,
    import_mt_job_card_view,
    sync_mt_job_card_view,
    system_health_view,
    knowledge_list_create,
    knowledge_detail,
    get_chat_history,
)

urlpatterns = [
    path("chat/", chat_with_local_model, name="chat-with-local-model"),
    path("chat/<str:conversation_id>/history/", get_chat_history, name="chat-history"),

    path("knowledge/", knowledge_list_create, name="knowledge-list-create"),
    path("knowledge/<int:document_id>/", knowledge_detail, name="knowledge-detail"),
    path(
        "knowledge/import/mt-job-cards/",
        import_mt_job_card_view,
        name="knowledge-import-mt-job-cards",
    ),
    path(
        "knowledge/sync/mt-job-cards/",
        sync_mt_job_card_view,
        name="knowledge-sync-mt-job-cards",
    ),
    path(
        "analytics/mt-job-cards/problem-stats/",
        analyze_mt_job_card_problem_view,
        name="analytics-mt-job-cards-problem-stats",
    ),

    path("health/", health_check, name="health-check"),
    path("system-health/", system_health_view, name="system-health"),
    path("feedback/summary/", feedback_summary_view, name="feedback-summary"),
]
