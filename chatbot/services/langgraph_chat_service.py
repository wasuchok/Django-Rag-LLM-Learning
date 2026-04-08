from __future__ import annotations

import asyncio
import logging
import time
from functools import lru_cache
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional, TypedDict

from asgiref.sync import sync_to_async
from django.conf import settings
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
    HumanMessage,
    SystemMessage,
)
from langchain_ollama import ChatOllama
from langgraph.graph import END, START, StateGraph

from .ollama_service import (
    DEFAULT_RESPONSE_LANGUAGE,
    build_generation_error_reply,
    build_messages,
    build_missing_knowledge_result,
    get_generation_num_predict,
    has_grounded_knowledge,
    is_bad_reply,
    prepare_reply_generation,
    should_block_for_missing_knowledge,
)

logger = logging.getLogger(__name__)


class ChatPlanState(TypedDict, total=False):
    conversation_id: str
    user_message: str
    user_id: Optional[int]
    exclude_message_id: Optional[int]
    before_message_id: Optional[int]
    prepared: Dict[str, object]
    response_language: str
    route: Literal["analytics", "missing_knowledge", "llm_generate"]
    reply: str
    sources: List[Dict[str, object]]
    num_predict: int
    ollama_messages: List[Dict[str, str]]
    langchain_messages: List[BaseMessage]


def _message_content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue

            if isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))

        return "".join(parts)

    if content is None:
        return ""

    return str(content)


def _build_langchain_messages(messages: List[Dict[str, str]]) -> List[BaseMessage]:
    converted: List[BaseMessage] = []

    for message in messages:
        role = (message.get("role") or "").strip().lower()
        content = message.get("content") or ""

        if role == "system":
            converted.append(SystemMessage(content=content))
        elif role == "assistant":
            converted.append(AIMessage(content=content))
        else:
            converted.append(HumanMessage(content=content))

    return converted


@lru_cache(maxsize=8)
def get_langchain_chat_model(num_predict: int) -> ChatOllama:
    return ChatOllama(
        model=settings.OLLAMA_MODEL,
        base_url=settings.OLLAMA_BASE_URL,
        temperature=settings.OLLAMA_TEMPERATURE,
        num_predict=num_predict,
        keep_alive=settings.OLLAMA_KEEP_ALIVE,
        reasoning=settings.OLLAMA_THINK,
        validate_model_on_init=False,
        sync_client_kwargs={"timeout": 120.0},
        async_client_kwargs={"timeout": 120.0},
    )


def _prepare_node(state: ChatPlanState) -> ChatPlanState:
    prepared = prepare_reply_generation(
        state["conversation_id"],
        state["user_message"],
        user_id=state.get("user_id"),
        exclude_message_id=state.get("exclude_message_id"),
        before_message_id=state.get("before_message_id"),
    )

    return {
        "prepared": prepared,
        "response_language": str(
            prepared.get("response_language") or DEFAULT_RESPONSE_LANGUAGE
        ),
    }


def _route_after_prepare(state: ChatPlanState) -> str:
    prepared = state["prepared"]
    analytics_reply = str(prepared.get("analytics_reply") or "").strip()

    if analytics_reply:
        return "analytics"

    if should_block_for_missing_knowledge(prepared):
        return "missing_knowledge"

    return "llm_generate"


def _analytics_node(state: ChatPlanState) -> ChatPlanState:
    prepared = state["prepared"]
    return {
        "route": "analytics",
        "reply": str(prepared.get("analytics_reply") or "").strip(),
        "sources": list(prepared.get("sources") or []),
    }


def _missing_knowledge_node(state: ChatPlanState) -> ChatPlanState:
    prepared = state["prepared"]
    result = build_missing_knowledge_result(prepared)
    return {
        "route": "missing_knowledge",
        "reply": str(result.get("reply") or ""),
        "sources": list(result.get("sources") or []),
    }


def _ready_for_generation_node(state: ChatPlanState) -> ChatPlanState:
    prepared = state["prepared"]
    raw_messages = build_messages(
        prepared["history"],
        state["user_message"],
        strict=has_grounded_knowledge(prepared),
        knowledge_text=str(prepared.get("knowledge_text") or ""),
        response_language=str(
            prepared.get("response_language") or DEFAULT_RESPONSE_LANGUAGE
        ),
        structured_answer_mode=bool(prepared.get("structured_answer_mode")),
    )
    num_predict = get_generation_num_predict(state["user_message"], prepared)

    return {
        "route": "llm_generate",
        "sources": list(prepared.get("sources") or []),
        "num_predict": num_predict,
        "ollama_messages": raw_messages,
        "langchain_messages": _build_langchain_messages(raw_messages),
    }


def _build_chat_plan_graph():
    graph = StateGraph(ChatPlanState)
    graph.add_node("prepare", _prepare_node)
    graph.add_node("analytics", _analytics_node)
    graph.add_node("missing_knowledge", _missing_knowledge_node)
    graph.add_node("llm_generate", _ready_for_generation_node)
    graph.add_edge(START, "prepare")
    graph.add_conditional_edges(
        "prepare",
        _route_after_prepare,
        {
            "analytics": "analytics",
            "missing_knowledge": "missing_knowledge",
            "llm_generate": "llm_generate",
        },
    )
    graph.add_edge("analytics", END)
    graph.add_edge("missing_knowledge", END)
    graph.add_edge("llm_generate", END)
    return graph.compile(name="chat_reply_orchestrator")


CHAT_PLAN_GRAPH = _build_chat_plan_graph()


def plan_reply_with_langgraph(
    conversation_id: str,
    user_message: str,
    *,
    user_id: Optional[int] = None,
    exclude_message_id: Optional[int] = None,
    before_message_id: Optional[int] = None,
) -> ChatPlanState:
    state: ChatPlanState = {
        "conversation_id": conversation_id,
        "user_message": user_message,
        "user_id": user_id,
        "exclude_message_id": exclude_message_id,
        "before_message_id": before_message_id,
    }
    return CHAT_PLAN_GRAPH.invoke(state)


def _extract_ai_text(message: AIMessage | AIMessageChunk) -> str:
    return _message_content_to_text(getattr(message, "content", "")).strip()


def _get_done_reason(message: AIMessage | AIMessageChunk) -> str:
    response_metadata = getattr(message, "response_metadata", {}) or {}
    return str(response_metadata.get("done_reason") or "").strip()


def _invoke_langchain_model(messages: List[BaseMessage], num_predict: int) -> AIMessage:
    llm = get_langchain_chat_model(num_predict)
    return llm.invoke(messages)


def _invoke_with_retry(messages: List[BaseMessage], num_predict: int) -> AIMessage:
    response = _invoke_langchain_model(messages, num_predict)
    reply = _extract_ai_text(response)

    if not reply and _get_done_reason(response) == "load":
        time.sleep(1)
        response = _invoke_langchain_model(messages, max(num_predict, 1536))
        reply = _extract_ai_text(response)

    if _get_done_reason(response) == "length":
        time.sleep(1)
        response = _invoke_langchain_model(messages, max(num_predict * 2, 2048))
        reply = _extract_ai_text(response)

    if is_bad_reply(reply):
        time.sleep(1)
        retry_response = _invoke_langchain_model(messages, max(num_predict, 1536))
        retry_reply = _extract_ai_text(retry_response)
        if retry_reply and not is_bad_reply(retry_reply):
            return retry_response

    return response


def generate_reply_with_langgraph(
    conversation_id: str,
    user_message: str,
    *,
    user_id: Optional[int] = None,
    exclude_message_id: Optional[int] = None,
    before_message_id: Optional[int] = None,
) -> Dict[str, object]:
    planned = plan_reply_with_langgraph(
        conversation_id,
        user_message,
        user_id=user_id,
        exclude_message_id=exclude_message_id,
        before_message_id=before_message_id,
    )

    route = planned["route"]
    if route != "llm_generate":
        return {
            "reply": planned["reply"],
            "sources": planned.get("sources", []),
        }

    response_language = str(
        planned.get("response_language") or DEFAULT_RESPONSE_LANGUAGE
    )
    response = _invoke_with_retry(
        planned["langchain_messages"],
        int(planned.get("num_predict") or settings.OLLAMA_NUM_PREDICT),
    )
    reply = _extract_ai_text(response)

    if reply and not is_bad_reply(reply):
        return {
            "reply": reply,
            "sources": planned.get("sources", []),
        }

    return {
        "reply": build_generation_error_reply(response_language),
        "sources": planned.get("sources", []),
    }


async def _astream_with_retry(
    messages: List[BaseMessage],
    on_token: Callable[[str], Awaitable[None]],
    *,
    num_predict: int,
) -> str:
    llm = get_langchain_chat_model(num_predict)

    async def run_once() -> str:
        reply_parts: list[str] = []
        async for chunk in llm.astream(messages):
            token = _message_content_to_text(getattr(chunk, "content", ""))
            if not token:
                continue

            reply_parts.append(token)
            await on_token(token)

        return "".join(reply_parts).strip()

    reply = await run_once()
    if reply:
        return reply

    await asyncio.sleep(1)
    return await run_once()


async def stream_reply_with_langgraph(
    conversation_id: str,
    user_message: str,
    on_token: Callable[[str], Awaitable[None]],
    *,
    user_id: Optional[int] = None,
    exclude_message_id: Optional[int] = None,
    before_message_id: Optional[int] = None,
) -> Dict[str, object]:
    planned = await sync_to_async(
        plan_reply_with_langgraph,
        thread_sensitive=True,
    )(
        conversation_id,
        user_message,
        user_id=user_id,
        exclude_message_id=exclude_message_id,
        before_message_id=before_message_id,
    )

    route = planned["route"]
    if route != "llm_generate":
        reply = str(planned.get("reply") or "")
        if route == "analytics" and reply:
            await on_token(reply)

        return {
            "reply": reply,
            "sources": planned.get("sources", []),
        }

    response_language = str(
        planned.get("response_language") or DEFAULT_RESPONSE_LANGUAGE
    )
    reply = await _astream_with_retry(
        planned["langchain_messages"],
        on_token,
        num_predict=int(planned.get("num_predict") or settings.OLLAMA_NUM_PREDICT),
    )

    if reply and not is_bad_reply(reply):
        return {
            "reply": reply,
            "sources": planned.get("sources", []),
        }

    logger.warning("LangChain streaming returned an empty or invalid reply")
    return {
        "reply": build_generation_error_reply(response_language),
        "sources": planned.get("sources", []),
    }
