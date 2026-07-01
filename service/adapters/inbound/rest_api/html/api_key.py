# service/ports/inbound/http/routers/html_api_key_router.py

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

from starlette.templating import Jinja2Templates

from service.domain.use_cases.external.api_token import (
    CreateApiTokenUCRq,
    RevokeApiTokenUCRq,
    DeleteApiTokenUCRq,
    GetApiTokensUCRq,
)
from service.ports.common.path_utils import get_project_root

router = APIRouter(tags=["API Keys Admin"])
api_key_router = router
templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))

# ── Request schemas ───────────────────────────────────────────────────────────

class CreateApiTokenBody(BaseModel):
    name: str
    user_id: int
    expires_at: Optional[datetime] = None


# ── HTML страница управления ключами ──────────────────────────────────────────

@router.get("/api-keys", response_class=HTMLResponse)
async def api_keys_page(request: Request):
    """Страница управления API-ключами для администратора."""
    rs = await request.app.state.api_token_facade.get_api_tokens(
        GetApiTokensUCRq()
    )
    return templates.TemplateResponse(
        request=request,
        name="api_keys.html",
        context={"tokens": rs.tokens},
    )


# ── REST эндпоинты для UI (вызываются из JS на странице) ─────────────────────

@router.post("/api-keys")
async def create_api_key(request: Request, body: CreateApiTokenBody):
    """
    Создаёт новый API-ключ.
    Сырой ключ возвращается только один раз — показать пользователю и не хранить.
    """
    rs = await request.app.state.api_token_facade.create_api_token(
        CreateApiTokenUCRq(
            name=body.name,
            user_id=body.user_id,
            expires_at=body.expires_at,
        )
    )
    if not rs.success or not rs.api_token_create:
        raise HTTPException(status_code=400, detail=rs.error or "Failed to create API key")

    return {
        "id":         rs.api_token_create.token.id,
        "name":       rs.api_token_create.token.name,
        "key_prefix": rs.api_token_create.token.key_prefix,
        "raw_key":    rs.api_token_create.raw_key,   # показать один раз
        "created_at": rs.api_token_create.token.created_at,
        "expires_at": rs.api_token_create.token.expires_at,
    }


@router.get("/api-keys/list")
async def list_api_keys(request: Request, user_id: Optional[int] = None):
    """Список всех API-ключей (без сырых ключей и хэшей)."""
    rs = await request.app.state.api_token_facade.get_api_tokens(
        GetApiTokensUCRq(user_id=user_id)
    )
    return {"tokens": rs.tokens}


@router.post("/api-keys/{token_id}/revoke")
async def revoke_api_key(request: Request, token_id: int):
    """Деактивирует ключ. Ключ остаётся в БД для аудита, но перестаёт работать."""
    rs = await request.app.state.api_token_facade.revoke_api_token(
        RevokeApiTokenUCRq(token_id=token_id)
    )
    if not rs.success:
        raise HTTPException(status_code=404, detail=rs.error or "API token not found")
    return {"token_id": token_id, "is_active": False}


@router.delete("/api-keys/{token_id}")
async def delete_api_key(request: Request, token_id: int):
    """Полностью удаляет ключ из БД."""
    rs = await request.app.state.api_token_facade.delete_api_token(
        DeleteApiTokenUCRq(token_id=token_id)
    )
    if not rs.success:
        raise HTTPException(status_code=404, detail=rs.error or "API token not found")
    return {"token_id": token_id, "deleted": True}
