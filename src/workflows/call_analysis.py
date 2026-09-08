from typing import Dict, Any
from supabase import Client
from src.edw.tracker import EDWTracker


async def run_call_analysis_workflow(
    tenant_db: Client,
    client_config: Dict[str, Any],
    payload: Dict[str, Any],
    parent_execution_id: str
) -> Dict[str, Any]:
    """
    Workflow 2: Extrai métricas, transcrição e atualiza os dados consolidados da chamada na tabela Retell_calls_Mindflow.
    """
    tracker = EDWTracker(tenant_db, "post_call_analysis", trigger_event_id=parent_execution_id)
    tracker.start_execution(payload)

    try:
        # Step 1: Extract Call Metrics
        step1_id = tracker.start_step("extract_metrics", input_data=payload)
        call_obj = payload.get("call", {})
        call_id = call_obj.get("call_id")
        transcript = call_obj.get("transcript")
        call_analysis = call_obj.get("call_analysis", {})
        call_summary = call_analysis.get("call_summary")
        user_sentiment = call_analysis.get("user_sentiment")

        duracao = call_obj.get("duration_ms", 0) / 1000.0 if call_obj.get("duration_ms") else None
        eleven_labs_cost = call_obj.get("e2e_latency", {}).get("eleven_labs_cost")
        llm_cost = call_obj.get("llm_cost")
        combined_cost = call_obj.get("combined_cost")

        metrics_extracted = {
            "call_id": call_id,
            "transcript_snippet": (transcript[:100] + "...") if transcript else None,
            "call_summary": call_summary,
            "user_sentiment": user_sentiment,
            "duracao_seconds": duracao,
            "combined_cost": combined_cost
        }
        tracker.finish_step_success(step1_id, metrics_extracted)

        # Step 2: Update Database Record
        step2_id = tracker.start_step("update_db_record", input_data={"call_id": call_id})
        update_data = {
            "transcript": transcript,
            "call_summary": call_summary,
            "LLM_token_usage": str(call_obj.get("llm_token_usage", "")),
        }
        if duracao is not None:
            update_data["Duracao"] = str(duracao)
        if combined_cost is not None:
            update_data["combined_cost"] = str(combined_cost)
        if llm_cost is not None:
            update_data["LLM_cost"] = str(llm_cost)
        if eleven_labs_cost is not None:
            update_data["eleven_labs_cost"] = str(eleven_labs_cost)

        tenant_db.table("Retell_calls_Mindflow") \
            .update(update_data) \
            .eq("call_id", call_id) \
            .execute()

        tracker.finish_step_success(step2_id, {"status": "updated", "call_id": call_id})

        output_result = {"status": "analysis_completed", "call_id": call_id}
        tracker.finish_execution_success(output_result)
        return output_result

    except Exception as e:
        error_msg = str(e)
        tracker.finish_execution_failed(error_msg)
        raise e
