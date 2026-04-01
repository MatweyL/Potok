
# service/ports/inbound/http/middleware/auth_middleware.py

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse

from service.domain.use_cases.external.auth.refresh_token import RefreshTokenUCRq
from service.ports.common.logs import logger

# Роуты которые НЕ требуют авторизации
PUBLIC_PATHS = {"/login", "/auth/login", "/auth/logout", "/", "/static"}


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # пропускаем публичные пути и статику
        if any(request.url.path == p for p in PUBLIC_PATHS):
            logger.info(f"pass route: {request.url.path}")

            return await call_next(request)
        logger.info(f"auth check route: {request.url.path}")

        token = request.cookies.get("access_token")
        # access валиден — пускаем
        if token:
            try:
                request.app.state.token_service.decode_token(token)
                logger.info(f"token is valid")

                return await call_next(request)
            except ValueError as e:
                logger.info(f"token not valid: {e}")
                pass

        # access протух — пробуем refresh
        refresh = request.cookies.get("refresh_token")
        logger.info(f"try refresh")

        if refresh:
            rs = await request.app.state.auth_facade.refresh_token(
                RefreshTokenUCRq(refresh_token=refresh)
            )
            logger.info(f"refresh response: {rs.success=}, {rs.error=}")
            if rs.success:
                response = await call_next(request)
                response.set_cookie(
                    key="access_token", value=rs.access_token,
                    httponly=True, secure=True, samesite="strict", max_age=60 * 15,
                )
                response.set_cookie(
                    key="refresh_token", value=rs.refresh_token,
                    httponly=True, secure=True, samesite="strict", max_age=60 * 60 * 24 * 30,
                )
                return response
        else:
            logger.info("no refresh, redirect to login")
        return RedirectResponse(url="/login", status_code=302)