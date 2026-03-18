import requests

from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status

from .models import ChatMessage, KnowledgeDocument
from .services.ollama_service import generate_reply_with_history
from .services.rag_service import index_document, delete_document_from_index

@api_view(["POST"])
def chat_with_local_model(request):
    conversation_id = request.data.get("conversation_id", "").strip()
    message = request.data.get("message", "").strip()

    if not conversation_id:
        return Response(
            {"error": "conversation_id is required"},
            status=status.HTTP_400_BAD_REQUEST
        )

    if not message:
        return Response(
            {"error": "message is required"},
            status=status.HTTP_400_BAD_REQUEST
        )

    try:
        result = generate_reply_with_history(conversation_id, message)
        reply = result["reply"]
        sources = result["sources"]

        user_msg = ChatMessage.objects.create(
            conversation_id=conversation_id,
            role="user",
            content=message,
            model_name="qwen2.5:14b"
        )

        assistant_msg = ChatMessage.objects.create(
            conversation_id = conversation_id,
            role = "assistant",
            content = reply,
            model_name = "qwen2.5:14b"
        )

        return Response({
            "conversation_id": conversation_id,
            "reply": reply,
            "sources": sources,
            "saved": {
                "user_message_id": user_msg.id,
                "assistant_message_id": assistant_msg.id
            }
        })
    

    except requests.exceptions.RequestException as e:
        return Response(
            {"error": f"cannot connect to local model: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

    except Exception as e:
        return Response(
            {"error": f"unexpected error: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    
@api_view(["POST", "GET"])
def knowledge_list_create(request):
    if request.method == "POST":
        title = request.data.get("title", "").strip()
        content = request.data.get("content", "").strip()
        source = request.data.get("source", "").strip()

        if not title:
            return Response(
                {"error" : "title is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if not content:
            return Response(
                {"error" : "content is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            doc = KnowledgeDocument.objects.create(
                title=title,
                content=content,
                source=source or None
            )

            index_document(doc)

            return Response({
                "message" : "knowledge added successfully",
                "document_id" : doc.id,
                "title" : doc.title
            }, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            return Response(
                {"error" : f"unexpected error: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
    documents = KnowledgeDocument.objects.all().order_by("-created_at")

    data = [
        {
            "id" : doc.id,
            "title" : doc.title,
            "source" : doc.source,
            "created_at" : doc.created_at,
            "content_preview" : doc.content[:120]
        }
        for doc in documents
    ]

    return Response({
        "count" : len(data),
        "results" : data
    })

@api_view(["GET", "PUT", "DELETE"])
def knowledge_detail(request, document_id):
    try:
        doc = KnowledgeDocument.objects.get(id=document_id)
    except KnowledgeDocument.DoesNotExist:
        return Response(
            {"error" : "Knowledge document not found"},
            status=status.HTTP_404_NOT_FOUND
        )
    
    if request.method == "GET":
        return Response({
            "id" : doc.id,
            "title" : doc.title,
            "content" : doc.content,
            "source" : doc.source,
            "created_at" : doc.created_at
        })
    
    if request.method == "PUT":
        title = request.data.get("title", "").strip()
        content = request.data.get("content", "").strip()
        source = request.data.get("source", "").strip()

        if not title:
            return Response(
                {"error" : "title is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if not content:
            return Response(
                {"error" : "content is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            delete_document_from_index(doc.id)

            doc.title = title
            doc.content = content
            doc.source = source or None
            doc.save()

            index_document(doc)

            return Response({
                "message" : "knowledge document updated successfully",
                "document_id" : doc.id,
                "title" : doc.title,
                "source" : doc.source
            })
        
        except Exception as e:
            return Response(
                {"error" : f"unexpected error while updating knowledge: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
    try:
        delete_document_from_index(doc.id)
        doc.delete()

        return Response({
            "message" : "knowledge document deleted successfully",
            "document_id" : document_id
        })
    
    except Exception as e:
        return Response(
            {"error" : f"unexpected error while deleting knowledge: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

@api_view(["GET"])
def get_chat_history(request, conversation_id):
    messages = (
        ChatMessage.objects
        .filter(conversation_id=conversation_id)
        .order_by("created_at")
    )

    data = [
        {
            "id": msg.id,
            "role": msg.role,
            "content": msg.content,
            "model_name": msg.model_name,
            "created_at": msg.created_at,
        }
        for msg in messages
    ]

    return Response({
        "conversation_id" : conversation_id,
        "messages" : data
    })

@api_view(["GET"])
def health_check(request):
    return Response({
        "status": "ok",
        "service": "django-chatbot-api"
    })

@api_view(["POST"])
def add_knowledge(request):
    title = request.data.get("title", "").strip()
    content = request.data.get("content", "").strip()
    source = request.data.get("source", "").strip()

    if not title:
        return Response({"error": "title is required"}, status=status.HTTP_400_BAD_REQUEST)

    if not content:
        return Response({"error": "content is required"}, status=status.HTTP_400_BAD_REQUEST)

    try:
        doc = KnowledgeDocument.objects.create(
            title=title,
            content=content,
            source=source or None
        )

        index_document(doc)

        return Response({
            "message": "knowledge added successfully",
            "document_id": doc.id,
            "title": doc.title
        })

    except Exception as e:
        return Response(
            {"error": f"unexpected error: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    
@api_view(["GET"])
def get_chat_history(request, conversation_id):
    messages = (
        ChatMessage.objects
        .filter(conversation_id=conversation_id)
        .order_by("created_at")
    )

    data = [
        {
            "id": msg.id,
            "role": msg.role,
            "content": msg.content,
            "model_name": msg.model_name,
            "created_at": msg.created_at,
        }
        for msg in messages
    ]

    return Response({
        "conversation_id": conversation_id,
        "messages": data
    })