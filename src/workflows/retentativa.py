from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any
import httpx
from supabase import Client
from src.config import settings
from src.edw.tracker import EDWTracker


async def run_retentativa_workflow(
    tenant_db: Client,
    client_config: Dict[str, Any],
    payload: Dict[str, Any],
    parent_execution_id: str
) -> Dict[str, Any]:
    """
    Workflow 3: post_call_retentativa
    Executa agendamento e rediscagem de chamadas através da API interna do microsserviço pre_call_processing.
    """
    tracker = EDWTracker(tenant_db, "post_call_retentativa", trigger_event_id=parent_execution_id)
    tracker.start_execution(payload)

    try:
        call_obj = payload.get("call", {})
        dynamic_vars = call_obj.get("retell_llm_dynamic_variables", {})

        to_number = call_obj.get("to_number")
        from_number = call_obj.get("from_number")
        client_id = client_config.get("client_id")
        customer_name = dynamic_vars.get("nome") or dynamic_vars.get("customer_name") or "Cliente"
        customer_email = dynamic_vars.get("email") or ""
        delay_minutes = payload.get("delay_minutes", 120)

        # Step 1: Check Meeting Scheduled (exclusively agendamentos table)
        step1_id = tracker.start_step("check_meeting_scheduled", input_data={"to_number": to_number})

        agendamento_check = tenant_db.table("agendamentos") \
            .select("id") \
            .eq("numero", to_number) \
            .eq("status", "agendado") \
            .execute()

        meeting_scheduled = bool(agendamento_check.data)

        if meeting_scheduled:
            tracker.finish_step_success(step1_id, {"meeting_scheduled": True, "action": "aborted"})
            out = {"status": "aborted_meeting_already_scheduled"}
            tracker.finish_execution_success(out)
            return out

        tracker.finish_step_success(step1_id, {"meeting_scheduled": False})

        # Step 2: Check Hourly Call Limit
        step2_id = tracker.start_step("check_hourly_limit", input_data={"to_number": to_number})
        
        recent_calls_res = tenant_db.table("Retell_calls_Mindflow") \
            .select("created_at") \
            .eq("to_number", to_number) \
            .execute()

        one_hour_ago = datetime.now(ZoneInfo("UTC")) - timedelta(hours=1)
        calls_last_hour = 0
        if recent_calls_res.data:
            for row in recent_calls_res.data:
                c_time_str = row.get("created_at")
                if c_time_str:
                    try:
                        c_time = datetime.fromisoformat(c_time_str.replace("Z", "+00:00"))
                        if c_time >= one_hour_ago:
                            calls_last_hour += 1
                    except Exception:
                        pass

        hourly_limit = int(client_config.get("hourly_call_limit") or 3)
        if calls_last_hour >= hourly_limit:
            tracker.finish_step_success(step2_id, {"calls_last_hour": calls_last_hour, "hourly_limit": hourly_limit, "action": "postponed"})
            out = {"status": "postponed_hourly_limit_reached", "calls_last_hour": calls_last_hour}
            tracker.finish_execution_success(out)
            return out

        tracker.finish_step_success(step2_id, {"calls_last_hour": calls_last_hour, "hourly_limit": hourly_limit})

        # Step 3: Check BRT Business Hours Window & Schedule Wait
        step3_id = tracker.start_step("calculate_wait_window", input_data={"delay_minutes": delay_minutes})
        now_brt = datetime.now(ZoneInfo("America/Sao_Paulo"))

        start_str = str(client_config.get("business_hours_start") or "09:00:00")
        end_str = str(client_config.get("business_hours_end") or "18:00:00")

        start_hour = int(start_str.split(":")[0])
        end_hour = int(end_str.split(":")[0])

        target_time_brt = now_brt + timedelta(minutes=delay_minutes)
        if target_time_brt.hour < start_hour or target_time_brt.hour >= end_hour or target_time_brt.weekday() >= 5:
            # Ajusta para próximo dia útil às 09:00
            target_time_brt = target_time_brt.replace(hour=start_hour, minute=0, second=0)
            if target_time_brt <= now_brt:
                target_time_brt += timedelta(days=1)
            while target_time_brt.weekday() >= 5:
                target_time_brt += timedelta(days=1)

        tracker.finish_step_success(step3_id, {
            "now_brt": now_brt.isoformat(),
            "target_time_brt": target_time_brt.isoformat()
        })

        # Step 4: Build Payload for pre_call_processing Microservice
        step4_id = tracker.start_step("build_pre_call_payload")
        agent_decision = payload.get("agent_decision", {})
        contexto_text = agent_decision.get("context") or "Retentativa automática recomendada após análise de chamada anterior."
        
        pre_call_payload = {
            "client_id": client_id,
            "numero": to_number,
            "nome": customer_name,
            "email": customer_email,
            "contexto": contexto_text,
            "scheduled_time": target_time_brt.isoformat()
        }
        tracker.finish_step_success(step4_id, {"pre_call_payload": pre_call_payload})

        # Step 5: Dispatch to Internal pre_call_processing API
        step5_id = tracker.start_step("dispatch_pre_call_service", input_data={"to_number": to_number})
        pre_call_url = client_config.get("pre_call_processing_url") or "http://pre-call-processing:8000/trigger"

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(pre_call_url, json=pre_call_payload, timeout=15.0)

            if resp.status_code not in (200, 201, 202):
                err_msg = f"pre_call_processing API HTTP {resp.status_code}: {resp.text}"
                tracker.finish_step_failed(step5_id, err_msg)
                # Não interrompe exceção para garantir finalização auditável
                dispatch_status = "error_pre_call_dispatch"
            else:
                dispatch_status = "dispatched_successfully"
                tracker.finish_step_success(step5_id, {"pre_call_response": resp.json() if resp.text else {}})
        except Exception as dispatch_err:
            dispatch_status = f"dispatch_exception: {str(dispatch_err)}"
            tracker.finish_step_failed(step5_id, dispatch_status)

        # Step 6: Finalize EDW (No insertion into Retell_calls_Mindflow or Retell_Leads_Midflow)
        step6_id = tracker.start_step("finalize_edw_record")
        tracker.finish_step_success(step6_id, {"status": "edw_completed", "dispatch_status": dispatch_status})

        output_res = {
            "status": "completed",
            "dispatch_status": dispatch_status,
            "scheduled_time": target_time_brt.isoformat()
        }
        tracker.finish_execution_success(output_res)
        return output_res

    except Exception as e:
        error_msg = str(e)
        tracker.finish_execution_failed(error_msg)
        raise e

