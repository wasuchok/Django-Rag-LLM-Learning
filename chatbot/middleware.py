from django.conf import settings
from django.http import HttpResponse
from django.utils.cache import patch_vary_headers


class SimpleCORSMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        origin = request.headers.get("Origin")
        is_preflight = request.method == "OPTIONS" and origin

        if is_preflight:
            response = HttpResponse(status=204)
        else:
            response = self.get_response(request)

        self._apply_cors_headers(request, response, origin)
        return response

    def _apply_cors_headers(self, request, response, origin):
        if not origin:
            return

        if not self._is_allowed_origin(origin):
            return

        allow_all = getattr(settings, "CORS_ALLOW_ALL_ORIGINS", False)
        allow_credentials = getattr(settings, "CORS_ALLOW_CREDENTIALS", False)

        if allow_all and not allow_credentials:
            response["Access-Control-Allow-Origin"] = "*"
        else:
            response["Access-Control-Allow-Origin"] = origin
            patch_vary_headers(response, ("Origin",))

        if allow_credentials:
            response["Access-Control-Allow-Credentials"] = "true"

        response["Access-Control-Allow-Methods"] = ", ".join(
            getattr(settings, "CORS_ALLOW_METHODS", [])
        )
        response["Access-Control-Allow-Headers"] = ", ".join(
            getattr(settings, "CORS_ALLOW_HEADERS", [])
        )
        response["Access-Control-Max-Age"] = str(
            getattr(settings, "CORS_PREFLIGHT_MAX_AGE", 86400)
        )

        expose_headers = getattr(settings, "CORS_EXPOSE_HEADERS", [])
        if expose_headers:
            response["Access-Control-Expose-Headers"] = ", ".join(expose_headers)

    def _is_allowed_origin(self, origin: str) -> bool:
        if getattr(settings, "CORS_ALLOW_ALL_ORIGINS", False):
            return True

        allowed_origins = getattr(settings, "CORS_ALLOWED_ORIGINS", [])
        return origin in allowed_origins
