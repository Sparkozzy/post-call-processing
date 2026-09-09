from typing import Dict, Any
from supabase import Client
import httpx
from src.edw.tracker import EDWTracker
from src.workflows.call_analysis import run_call_analysis_workflow
from src.workflows.retentativa import run_retentativa_workflow


async def run_webhook_ligacao_workflow(
    tenant_db: Client,
    client_config: Dict[str, Any],
    payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Workflow 1: Ponto de entrada mestre pós-chamada.
    Ingere dados brutos na tabela Retell_calls_Mindflow e efetua o roteamento inicial.
    """
    tracker = EDWTracker(tenant_db, "post_call_webhook_ligacao")
    execution_id = tracker.start_execution(payload)

    try:
        call_obj = payload.get("call", {})
        event_type = payload.get("event")
        call_id = call_obj.get("call_id")
        to_number = call_obj.get("to_number")
        from_number = call_obj.get("from_number")
        agent_id = call_obj.get("agent_id")
        agent_name = call_obj.get("agent_name")
        agent_version = call_obj.get("agent_version")
        disconnection_reason = call_obj.get("disconnection_reason")
        transcript = call_obj.get("transcript")
        recording_url = call_obj.get("recording_url")
        call_status = call_obj.get("call_status")

        dynamic_vars = call_obj.get("retell_llm_dynamic_variables", {})
        nome = dynamic_vars.get("nome") or dynamic_vars.get("customer_name") or ""
        email = dynamic_vars.get("email") or ""

        # Step 1: Raw Ingestion into Retell_calls_Mindflow
        step1_id = tracker.start_step("raw_ingestion", input_data={"call_id": call_id, "event": event_type})

        row_data = {
            "call_id": call_id,
            "to_number": to_number,
            "from_number": from_number,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "agent_version": agent_version,
            "disconnection_reason": disconnection_reason,
            "transcript": transcript,
            "recording_url": recording_url,
            "status": call_status,
            "Nome": nome,
            "Email": email,
            "data": payload.get("timestamp") or str(call_obj.get("start_timestamp", ""))
        }

        # Check if call_id exists to update or insert
        existing = tenant_db.table("Retell_calls_Mindflow") \
            .select("id") \
            .eq("call_id", call_id) \
            .execute()

        if existing.data:
            tenant_db.table("Retell_calls_Mindflow").update(row_data).eq("call_id", call_id).execute()
        else:
            tenant_db.table("Retell_calls_Mindflow").insert(row_data).execute()

        tracker.finish_step_success(step1_id, {"status": "ingested", "call_id": call_id})

        # Step 2: Route Decision (Call_predict vs AI Agent)
        call_predict_enabled = bool(
            client_config.get("call_predict_enabled") or client_config.get("Call_predict")
        )
        step2_id = tracker.start_step("route_decision", input_data={"call_predict_enabled": call_predict_enabled, "event": event_type})

        routed_action = None

        if call_predict_enabled:
            routed_action = "call_predict_microservice"
            tracker.finish_step_success(step2_id, {"routed_to": routed_action})
            
            # Envia diretamente ao microsserviço call_predict via API
            call_predict_url = client_config.get("call_predict_url") or "http://call-predict:8000/webhook/predict"
            try:
                async with httpx.AsyncClient() as http_client:
                    cp_resp = await http_client.post(call_predict_url, json=payload, timeout=15.0)
                    cp_result = cp_resp.json() if cp_resp.status_code == 200 else {"error": cp_resp.text}
            except Exception as cp_err:
                cp_result = {"error": str(cp_err)}

            tracker.finish_execution_success({"status": "completed", "route": routed_action, "call_predict_result": cp_result})
            return {"status": "completed", "execution_id": execution_id, "route": routed_action}
        else:
            routed_action = "post_call_analysis_ai"
            tracker.finish_step_success(step2_id, {"routed_to": routed_action})
            analysis_res = await run_call_analysis_workflow(tenant_db, client_config, payload, execution_id)
            tracker.finish_execution_success({"status": "completed", "route": routed_action, "result": analysis_res})
            return {"status": "completed", "execution_id": execution_id, "route": routed_action}

    except Exception as e:
        error_msg = str(e)
        tracker.finish_execution_failed(error_msg)
        raise e

