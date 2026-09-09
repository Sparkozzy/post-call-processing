from typing import Dict, Any
from fastapi import APIRouter, HTTPException, Query, Header, Request, status, BackgroundTasks
from src.database.supabase_client import get_client_config, get_supabase_tenant
from src.workflows.webhook_ligacao import run_webhook_ligacao_workflow

router = APIRouter(prefix="/webhook", tags=["Webhook Post Call"])


@router.post("/post-call/{client_id}")
async def post_call_webhook_endpoint(
    client_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    token: str = Query(..., description="Token de autenticação mindflow_api_token do cliente"),
    x_mindflow_token: str = Header(None, alias="X-MindFlow-Token")
):
    """
    Endpoint único de webhook pós-chamada.
    Valida token contra o client_configurations no Supabase Master e executa o processamento assíncronamente.
    """
    token_to_validate = token or x_mindflow_token
    if not token_to_validate:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de autenticação não fornecido."
        )

    # Validate Client and Token against Supabase Master
    client_config = get_client_config(client_id, token_to_validate)
    if not client_config:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Cliente '{client_id}' não encontrado ou token de segurança inválido."
        )

    # Read webhook body JSON
    try:
        payload: Dict[str, Any] = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payload JSON inválido."
        )

    # Create tenant Supabase client
    tenant_db = get_supabase_tenant(client_config)

    # Dispatch main workflow asynchronously
    background_tasks.add_task(run_webhook_ligacao_workflow, tenant_db, client_config, payload)

    return {
        "status": "success",
        "client_id": client_id,
        "message": "Webhook recebido com sucesso. Processamento pós-ligação iniciado em background."
    }

