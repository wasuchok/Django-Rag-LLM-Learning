from django.contrib import admin
from .models import ChatMessage, ChatMessageFeedback

admin.site.register(ChatMessage)


@admin.register(ChatMessageFeedback)
class ChatMessageFeedbackAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "conversation_id",
        "user",
        "message_id",
        "feedback_label",
        "updated_at",
    )
    list_filter = ("value", "updated_at", "created_at")
    search_fields = (
        "conversation_id",
        "chainlit_feedback_id",
        "chainlit_step_id",
        "message__content",
        "user__username",
    )
    readonly_fields = ("created_at", "updated_at")

    @admin.display(description="feedback")
    def feedback_label(self, obj: ChatMessageFeedback) -> str:
        return "ตรง" if obj.value == ChatMessageFeedback.VALUE_CORRECT else "ไม่ตรง"
