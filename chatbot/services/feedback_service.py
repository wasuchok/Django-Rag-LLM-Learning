from __future__ import annotations

import uuid
from typing import Any, Dict

from django.db import transaction
from django.db.models import Count, Q

from ..models import ChatMessage, ChatMessageFeedback
from .conversation_management_service import parse_chat_message_id_from_step_id


def _normalize_feedback_value(value: int) -> int:
    return (
        ChatMessageFeedback.VALUE_CORRECT
        if int(value) == ChatMessageFeedback.VALUE_CORRECT
        else ChatMessageFeedback.VALUE_INCORRECT
    )


def _get_feedback_queryset_for_message(message: ChatMessage):
    if message.user_id is None:
        return ChatMessageFeedback.objects.filter(
            message=message,
            user__isnull=True,
        )

    return ChatMessageFeedback.objects.filter(
        message=message,
        user_id=message.user_id,
    )


def _get_message_for_feedback(step_id: str, thread_id: str | None = None) -> ChatMessage | None:
    normalized_step_id = (step_id or "").strip()
    if not normalized_step_id:
        return None

    queryset = ChatMessage.objects.select_related("user").all()
    if thread_id:
        queryset = queryset.filter(conversation_id=thread_id)

    message_id = parse_chat_message_id_from_step_id(normalized_step_id)
    if message_id is not None:
        return queryset.filter(id=message_id).first()

    return queryset.filter(chainlit_step_id=normalized_step_id).first()


def upsert_message_feedback(
    *,
    step_id: str,
    value: int,
    thread_id: str | None = None,
    feedback_id: str | None = None,
    comment: str | None = None,
) -> str:
    message = _get_message_for_feedback(step_id, thread_id=thread_id)
    if not message:
        raise ChatMessage.DoesNotExist("ไม่พบข้อความที่ต้องการบันทึก feedback")

    normalized_feedback_id = (feedback_id or "").strip() or str(uuid.uuid4())
    normalized_comment = (comment or "").strip() or None
    normalized_value = _normalize_feedback_value(value)

    with transaction.atomic():
        feedback = (
            ChatMessageFeedback.objects.select_for_update()
            .filter(chainlit_feedback_id=normalized_feedback_id)
            .first()
        )

        if not feedback:
            feedback = _get_feedback_queryset_for_message(message).select_for_update().first()

        if feedback:
            if not (feedback_id or "").strip():
                normalized_feedback_id = (
                    feedback.chainlit_feedback_id or normalized_feedback_id
                )
            feedback.message = message
            feedback.user_id = message.user_id
            feedback.conversation_id = message.conversation_id
            feedback.chainlit_step_id = (step_id or "").strip()
            feedback.chainlit_feedback_id = normalized_feedback_id
            feedback.value = normalized_value
            feedback.comment = normalized_comment
            feedback.save()
        else:
            feedback = ChatMessageFeedback.objects.create(
                user_id=message.user_id,
                message=message,
                conversation_id=message.conversation_id,
                chainlit_step_id=(step_id or "").strip(),
                chainlit_feedback_id=normalized_feedback_id,
                value=normalized_value,
                comment=normalized_comment,
            )

    return feedback.chainlit_feedback_id or normalized_feedback_id


def delete_message_feedback(feedback_id: str) -> bool:
    normalized_feedback_id = (feedback_id or "").strip()
    if not normalized_feedback_id:
        return False

    deleted_count, _ = ChatMessageFeedback.objects.filter(
        chainlit_feedback_id=normalized_feedback_id
    ).delete()
    return deleted_count > 0


def build_feedback_summary(limit: int = 20) -> Dict[str, Any]:
    normalized_limit = max(1, int(limit))
    queryset = ChatMessageFeedback.objects.select_related("user", "message").order_by(
        "-updated_at",
        "-id",
    )

    totals = queryset.aggregate(
        total=Count("id"),
        positive=Count(
            "id",
            filter=Q(value=ChatMessageFeedback.VALUE_CORRECT),
        ),
        negative=Count(
            "id",
            filter=Q(value=ChatMessageFeedback.VALUE_INCORRECT),
        ),
    )
    total = int(totals.get("total") or 0)
    positive = int(totals.get("positive") or 0)
    negative = int(totals.get("negative") or 0)
    positive_rate = round((positive / total) * 100, 2) if total else 0.0

    recent = []
    for feedback in queryset[:normalized_limit]:
        recent.append(
            {
                "id": feedback.id,
                "chainlit_feedback_id": feedback.chainlit_feedback_id,
                "conversation_id": feedback.conversation_id,
                "message_id": feedback.message_id,
                "chainlit_step_id": feedback.chainlit_step_id,
                "value": feedback.value,
                "label": "correct"
                if feedback.value == ChatMessageFeedback.VALUE_CORRECT
                else "incorrect",
                "comment": feedback.comment,
                "user_username": (
                    feedback.user.get_username() if feedback.user_id is not None else None
                ),
                "message_preview": (feedback.message.content or "")[:160],
                "created_at": feedback.created_at.isoformat(),
                "updated_at": feedback.updated_at.isoformat(),
            }
        )

    return {
        "total": total,
        "positive": positive,
        "negative": negative,
        "positive_rate": positive_rate,
        "recent": recent,
    }
