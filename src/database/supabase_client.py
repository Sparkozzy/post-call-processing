from functools import lru_cache
from typing import Dict, Any, Optional
from supabase import create_client, Client
from src.config import settings


@lru_cache()
def get_supabase_master() -> Client:
    """Retorna cliente Supabase Master singleton."""
    return create_client(settings.SUPABASE_MASTER_URL, settings.SUPABASE_MASTER_SERVICE_KEY)


def get_client_config(client_id: str, token: str) -> Optional[Dict[str, Any]]:
    """
    Busca e valida as configurações do cliente na tabela client_configurations do Supabase Master.
    Valida se o token fornecido corresponde ao mindflow_api_token.
    """
    master = get_supabase_master()
    response = master.table("client_configurations") \
        .select("*") \
        .eq("client_id", client_id) \
        .execute()

    if not response.data:
        return None

    client_data = response.data[0]
    expected_token = client_data.get("mindflow_api_token")

    if expected_token and expected_token != token:
        return None

    return client_data


def get_supabase_tenant(client_config: Dict[str, Any]) -> Client:
    """Cria uma instância do cliente Supabase isolado do tenant com a chave service_role."""
    url = client_config["supabase_url"]
    key = client_config["supabase_service_key"]
    return create_client(url, key)
