from django.urls import path
from .views import (
    chat_with_local_model,
    health_check,
    knowledge_list_create,
    knowledge_detail,
    get_chat_history,
)

urlpatterns = [
    path("chat/", chat_with_local_model, name="chat-with-local-model"),
    path("chat/<str:conversation_id>/history/", get_chat_history, name="chat-history"),

    path("knowledge/", knowledge_list_create, name="knowledge-list-create"),
    path("knowledge/<int:document_id>/", knowledge_detail, name="knowledge-detail"),

    path("health/", health_check, name="health-check"),
]