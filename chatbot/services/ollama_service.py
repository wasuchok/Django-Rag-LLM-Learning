import re
import time
import requests
from typing import List, Dict
from ..models import ChatMessage

from .rag_service import search_knowledge

OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen2.5:14b"


def has_cjk(text: str) -> bool:
    # จีน ญี่ปุ่น เกาหลี
    return bool(re.search(r"[\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]", text))


def too_much_latin(text: str) -> bool:
    latin_chars = re.findall(r"[A-Za-z]", text)
    thai_chars = re.findall(r"[\u0E00-\u0E7F]", text)

    return len(latin_chars) > 40 and len(thai_chars) < 10


def is_bad_reply(text: str) -> bool:
    if not text or not text.strip():
        return True
    if has_cjk(text):
        return True
    if too_much_latin(text):
        return True
    return False


def get_conversation_history(conversation_id: str, limit: int = 6) -> List[Dict[str, str]]:
    rows = (
        ChatMessage.objects
        .filter(conversation_id=conversation_id)
        .order_by("-created_at")[:limit]
    )

    rows = list(rows)[::-1]

    history = []
    for row in rows:
        if row.role not in ["user", "assistant", "system"]:
            continue

        content = (row.content or "").strip()
        if not content:
            continue

        # ข้ามข้อความ debug / error
        if content.startswith("ไม่พบข้อความตอบกลับ"):
            continue
        if content.startswith("Ollama error:"):
            continue

        # ข้าม assistant reply ที่ปนจีน/เพี้ยน
        if row.role == "assistant" and is_bad_reply(content):
            continue

        history.append({
            "role": row.role,
            "content": content
        })

    return history


def build_messages(history: List[Dict[str, str]], user_message : str, strict : bool = False, knowledge_text : str = "") -> List[Dict[str, str]]:
    system_prompt = """
    คุณคือผู้ช่วย AI ภาษาไทย

    กฎที่ต้องทำตาม:
    1. ตอบเป็นภาษาไทยเท่านั้น
    2. ถ้ามีข้อมูลอ้างอิง ให้ตอบจากข้อมูลอ้างอิงก่อน
    3. ถ้าไม่มีข้อมูลอ้างอิงพอ ให้บอกว่าไม่มีข้อมูลเพียงพอ
    4. ห้ามแต่งข้อมูลเพิ่มเอง
    5. ตอบสั้น กระชับ เข้าใจง่าย
    """

    if strict:
        system_prompt += """
ข้อบังคับเพิ่มเติม:
- ห้ามตอบเป็นภาษาจีนหรือภาษาอังกฤษ
- คำตอบสุดท้ายต้องเป็นภาษาไทย
"""

    messages = [{"role": "system", "content": system_prompt}]

    if knowledge_text:
        messages.append({
            "role": "system",
            "content": f"ข้อมูลอ้างอิง:\n{knowledge_text}"
        })

    messages.extend(history)
    messages.append({"role": "user", "content": user_message})
    return messages


def call_ollama(messages: List[Dict[str, str]]) -> dict:
    payload = {
        "model": OLLAMA_MODEL,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": 0.2
        }
    }

    print("PAYLOAD =", payload)

    response = requests.post(OLLAMA_URL, json=payload, timeout=120)
    print("STATUS =", response.status_code)
    print("RAW =", response.text)

    response.raise_for_status()
    return response.json()


def extract_reply(data: dict) -> str:
    if data.get("error"):
        return f"Ollama error: {data['error']}"

    message_obj = data.get("message")
    if isinstance(message_obj, dict):
        content = message_obj.get("content", "")
        if content and content.strip():
            return content.strip()

    if data.get("response"):
        return data["response"].strip()

    return ""


def generate_reply_with_history(conversation_id: str, user_message: str) -> str:
    history = get_conversation_history(conversation_id, limit=6)

    knowledge_items = search_knowledge(user_message, top_k=5, max_distance=1.2)
    knowledge_text = build_knowledge_context(knowledge_items)
    source_items = clean_sources(knowledge_items)

    messages = build_messages(
        history, 
        user_message, 
        strict=False, 
        knowledge_text=knowledge_text
    )

    data = call_ollama(messages)
    reply = extract_reply(data)

    if not reply and data.get("done_reason") == "load":
        time.sleep(1)
        data = call_ollama(messages)
        reply = extract_reply(data)

    if is_bad_reply(reply):
        strict_messages = build_messages(
            history, 
            user_message, 
            strict=True, 
            knowledge_text=knowledge_text
        )

        data = call_ollama(strict_messages)
        retry_reply = extract_reply(data)

        if retry_reply and not is_bad_reply(retry_reply):
            return {
                "reply" : retry_reply,
                "sources" : source_items
            }
        
    if reply and not is_bad_reply(reply):
        return {
            "reply" : reply,
            "sources" : source_items
        }
    
    return {
        "reply" : "ขออภัยครับ ลองพิมพ์ใหม่อีกครั้งได้เลย",
        "sources" : source_items
    }

def build_knowledge_context(knowledge_items : List[Dict]) -> str:
    if not knowledge_items:
        return ""
    
    parts = []
    for item in knowledge_items:
        content = item.get("content", "").strip()
        metadata = item.get("metadata", {})
        title = metadata.get("title", "ไม่ระบุชื่อ")

        parts.append(f"[แหล่งข้อมูล : {title}]\n{content}")

    return "\n\n".join(parts)

def clean_sources(knowledge_items : List[Dict]) -> List[Dict]:
    cleaned = []

    for item in knowledge_items:
        metadata = item.get("metadata", {}) or {}

        cleaned.append({
            "title" : metadata.get("title"),
            "source" : metadata.get("source"),
            "chunk_index" : metadata.get("chunk_index"),
            "document_id" : metadata.get("document_id"),
            "distance" : item.get("distance")
        })

    return cleaned