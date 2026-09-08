from datetime import datetime
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
    Workflow 3: Executa re-discagem ativa para leads que não atenderam ou caíram em caixa postal.
    """
    tracker = EDWTracker(tenant_db, "post_call_retentativa", trigger_event_id=parent_execution_id)
    tracker.start_execution(payload)

    try:
        call_obj = payload.get("call", {})
        dynamic_vars = call_obj.get("retell_llm_dynamic_variables", {})

        to_number = call_obj.get("to_number")
        from_number = call_obj.get("from_number")
        original_agent_id = call_obj.get("agent_id")
        customer_name = dynamic_vars.get("nome") or dynamic_vars.get("customer_name") or "Cliente"
        customer_email = dynamic_vars.get("email") or ""

        # Step 1: Check Meeting Scheduled
        step1_id = tracker.start_step("check_meeting_scheduled", input_data={"to_number": to_number, "email": customer_email})

        # Check agendamentos table
        agendamento_check = tenant_db.table("agendamentos") \
            .select("id") \
            .eq("numero", to_number) \
            .eq("status", "agendado") \
            .execute()

        # Check Retell_Leads_Midflow table for Reuniao_marcada
        lead_check = tenant_db.table("Retell_Leads_Midflow") \
            .select("Reuniao_marcada") \
            .eq("Numero", to_number) \
            .execute()

        meeting_scheduled = bool(agendamento_check.data) or (
            bool(lead_check.data) and bool(lead_check.data[0].get("Reuniao_marcada"))
        )

        if meeting_scheduled:
            tracker.finish_step_success(step1_id, {"meeting_scheduled": True, "action": "aborted"})
            out = {"status": "aborted_meeting_already_scheduled"}
            tracker.finish_execution_success(out)
            return out

        tracker.finish_step_success(step1_id, {"meeting_scheduled": False})

        # Step 2: Check Max Retry Attempts
        step2_id = tracker.start_step("check_max_attempts", input_data={"to_number": to_number})
        max_attempts = int(client_config.get("max_retry_attempts") or 15)

        calls_res = tenant_db.table("Retell_calls_Mindflow") \
            .select("id") \
            .eq("to_number", to_number) \
            .execute()

        total_attempts = len(calls_res.data) if calls_res.data else 0

        if total_attempts >= max_attempts:
            tracker.finish_step_success(step2_id, {"total_attempts": total_attempts, "max_attempts": max_attempts, "action": "aborted"})
            out = {"status": "aborted_max_retry_attempts_reached", "total_attempts": total_attempts}
            tracker.finish_execution_success(out)
            return out

        tracker.finish_step_success(step2_id, {"total_attempts": total_attempts, "max_attempts": max_attempts})

        # Step 3: Check BRT Business Hours Window
        step3_id = tracker.start_step("check_business_hours")
        now_brt = datetime.now(ZoneInfo("America/Sao_Paulo"))

        start_str = str(client_config.get("business_hours_start") or "09:00:00")
        end_str = str(client_config.get("business_hours_end") or "19:00:00")

        start_hour = int(start_str.split(":")[0])
        end_hour = int(end_str.split(":")[0])

        current_hour = now_brt.hour
        is_in_business_hours = (start_hour <= current_hour < end_hour)

        if not is_in_business_hours:
            tracker.finish_step_success(step3_id, {"is_in_business_hours": False, "now_brt": now_brt.isoformat(), "action": "postponed"})
            out = {"status": "postponed_outside_business_hours", "current_hour": current_hour}
            tracker.finish_execution_success(out)
            return out

        tracker.finish_step_success(step3_id, {"is_in_business_hours": True, "now_brt": now_brt.isoformat()})

        # Step 4: Build Enriched Context
        step4_id = tracker.start_step("build_context", input_data={"to_number": to_number})

        previous_calls = tenant_db.table("Retell_calls_Mindflow") \
            .select("call_summary, created_at") \
            .eq("to_number", to_number) \
            .order("created_at", desc=True) \
            .limit(5) \
            .execute()

        summaries = []
        if previous_calls.data:
            for row in previous_calls.data:
                if row.get("call_summary"):
                    summaries.append(row["call_summary"])

        summary_text = " | ".join(summaries) if summaries else "Sem interações com resposta anterior."
        enriched_context = (
            f"Você já tentou contato com esta pessoa {total_attempts} vezes sem sucesso. "
            f"Resumo das tentativas/interações anteriores: {summary_text}. "
            f"Não mencione esse histórico proativamente no início da chamada, a menos que o cliente pergunte."
        )
        tracker.finish_step_success(step4_id, {"total_attempts": total_attempts, "context_length": len(enriched_context)})

        # Step 5: Dispatch Retell Call API
        step5_id = tracker.start_step("dispatch_retell_call", input_data={"agent_id": original_agent_id, "to_number": to_number})

        retell_payload = {
            "from_number": from_number,
            "to_number": to_number,
            "override_agent_id": original_agent_id,
            "retell_llm_dynamic_variables": {
                "customer_name": customer_name,
                "nome": customer_name,
                "email": customer_email,
                "contexto": enriched_context,
                "numero_do_lead": to_number
            }
        }

        api_key = settings.RETELL_API_KEY
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post("https://api.retellai.com/v2/create-phone-call", json=retell_payload, headers=headers, timeout=15.0)

        if resp.status_code not in (200, 201):
            err_msg = f"Retell API HTTP {resp.status_code}: {resp.text}"
            tracker.finish_step_failed(step5_id, err_msg)
            raise RuntimeError(err_msg)

        retell_res = resp.json()
        new_call_id = retell_res.get("call_id")
        tracker.finish_step_success(step5_id, {"new_call_id": new_call_id})

        # Step 6: Upsert Lead Attempt Count
        step6_id = tracker.start_step("upsert_lead_attempts", input_data={"to_number": to_number})

        lead_exist = tenant_db.table("Retell_Leads_Midflow") \
            .select("tentativas") \
            .eq("Numero", to_number) \
            .execute()

        if lead_exist.data:
            current_tent = int(lead_exist.data[0].get("tentativas") or 0)
            tenant_db.table("Retell_Leads_Midflow") \
                .update({"tentativas": str(current_tent + 1), "Data_horario_ligação": now_brt.isoformat()}) \
                .eq("Numero", to_number) \
                .execute()
        else:
            tenant_db.table("Retell_Leads_Midflow") \
                .insert({
                    "Numero": to_number,
                    "Nome": customer_name,
                    "email_lead": customer_email,
                    "tentativas": "1",
                    "Data_horario_ligação": now_brt.isoformat()
                }) \
                .execute()

        tracker.finish_step_success(step6_id, {"updated_attempts": total_attempts + 1})

        output_res = {"status": "retry_call_dispatched", "new_call_id": new_call_id}
        tracker.finish_execution_success(output_res)
        return output_res

    except Exception as e:
        error_msg = str(e)
        tracker.finish_execution_failed(error_msg)
        raise e
