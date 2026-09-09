from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List
import json
import httpx
from supabase import Client
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_community.vectorstores import SupabaseVectorStore
from langchain_core.prompts import PromptTemplate
from src.edw.tracker import EDWTracker
from src.database.supabase_client import get_supabase_master
from src.workflows.retentativa import run_retentativa_workflow


async def run_call_analysis_workflow(
    tenant_db: Client,
    client_config: Dict[str, Any],
    payload: Dict[str, Any],
    parent_execution_id: str
) -> Dict[str, Any]:
    """
    Workflow 2: post_call_analysis_ai
    Agente autônomo de IA que analisa a transcrição, consulta RAG no Supabase Master,
    conta tentativas únicas por call_id e decide se e quando ligar novamente.
    """
    tracker = EDWTracker(tenant_db, "post_call_analysis_ai", trigger_event_id=parent_execution_id)
    tracker.start_execution(payload)

    try:
        call_obj = payload.get("call", {})
        call_id = call_obj.get("call_id")
        to_number = call_obj.get("to_number")
        transcript = call_obj.get("transcript") or ""
        dynamic_vars = call_obj.get("retell_llm_dynamic_variables", {})
        customer_name = dynamic_vars.get("nome") or dynamic_vars.get("customer_name") or ""
        customer_email = dynamic_vars.get("email") or ""

        # Step 1: History Check (Distinct call_ids & Hourly Rate Limit)
        step1_id = tracker.start_step("history_check", input_data={"to_number": to_number})
        
        history_res = tenant_db.table("Retell_calls_Mindflow") \
            .select("call_id, created_at") \
            .eq("to_number", to_number) \
            .execute()
        
        seen_call_ids = set()
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        calls_last_hour = 0

        if history_res.data:
            for row in history_res.data:
                c_id = row.get("call_id")
                if c_id:
                    seen_call_ids.add(c_id)
                
                created_at_str = row.get("created_at")
                if created_at_str:
                    try:
                        c_time = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                        if c_time >= one_hour_ago:
                            calls_last_hour += 1
                    except Exception:
                        pass

        total_unique_attempts = len(seen_call_ids)
        tracker.finish_step_success(step1_id, {
            "total_unique_attempts": total_unique_attempts,
            "calls_last_hour": calls_last_hour
        })

        # Step 2: Vector RAG in Supabase Master (Ryan Project)
        step2_id = tracker.start_step("vector_rag_master", input_data={"transcript_length": len(transcript)})
        master_db = get_supabase_master()
        
        rag_examples = []
        try:
            embeddings = OpenAIEmbeddings()
            vector_store = SupabaseVectorStore(
                client=master_db,
                table_name="documents_fil",
                query_name="match_documents_fil",
                embedding=embeddings
            )
            docs = vector_store.similarity_search(transcript, k=2)
            for doc in docs:
                rag_examples.append({
                    "page_content": doc.page_content,
                    "metadata": doc.metadata
                })
        except Exception as rag_err:
            # Fallback seguro caso embeddings/RPC falhe
            rag_examples = [{"note": f"Erro na consulta RAG Master: {str(rag_err)}"}]

        tracker.finish_step_success(step2_id, {"rag_examples_found": len(rag_examples)})

        # Step 3: AI Agent Decision Evaluation
        step3_id = tracker.start_step("agent_evaluation", input_data={"transcript": transcript[:100]})
        
        system_prompt_text = """
==<role>Estrategista de Conversão MindFlow. Analise a intencionalidade comercial desta ligação com precisão.</role>
Analise a transcrição abaixo e os exemplos históricos RAG para decidir se deve ligar novamente ("Ligar?") e em quantos minutos ("min").

Retorne APENAS um JSON válido sem markdown no formato:
{{
  "pense": "explicação curta da decisão",
  "Ligar?": true ou false,
  "min": número em minutos (0 se Ligar? = false),
  "alerta_sdr": "mensagem direta SDR",
  "context": "resumo para CRM",
  "causa_raiz": "Humano | Caixa Postal | URA | Queda | Falha",
  "nivel_interesse": "Quente | Morno | Frio | Nulo",
  "drop_state": "Abertura | Discovery | Pitch | Close | Nulo"
}}

Exemplos RAG:
{rag_examples}

Transcrição da ligação:
{transcript}
"""
        prompt = PromptTemplate(
            template=system_prompt_text,
            input_variables=["rag_examples", "transcript"]
        )
        
        llm = ChatOpenAI(model="gpt-4.1-nano", temperature=0.2)
        chain = prompt | llm
        
        llm_response = await chain.ainvoke({
            "rag_examples": json.dumps(rag_examples, ensure_ascii=False),
            "transcript": transcript
        })
        
        content = llm_response.content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[-1].rsplit("\n", 1)[0].replace("json", "").strip()

        try:
            agent_decision = json.loads(content)
        except Exception:
            agent_decision = {
                "pense": "Fallback por falha de parser",
                "Ligar?": True,
                "min": 120,
                "alerta_sdr": "⚪ *NULO* | Analisar manualmente",
                "context": "Transcrição processada com fallback",
                "causa_raiz": "Falha",
                "nivel_interesse": "Nulo",
                "drop_state": "Nulo"
            }

        tracker.finish_step_success(step3_id, agent_decision)

        # Step 4: Update Database Record (Metrics & Summary)
        step4_id = tracker.start_step("update_call_record", input_data={"call_id": call_id})
        update_data = {
            "transcript": transcript,
            "call_summary": agent_decision.get("context"),
            "LLM_token_usage": str(call_obj.get("llm_token_usage", "")),
        }
        if call_obj.get("duration_ms"):
            update_data["Duracao"] = str(call_obj.get("duration_ms") / 1000.0)
        if call_obj.get("combined_cost"):
            update_data["combined_cost"] = str(call_obj.get("combined_cost"))

        tenant_db.table("Retell_calls_Mindflow") \
            .update(update_data) \
            .eq("call_id", call_id) \
            .execute()
        tracker.finish_step_success(step4_id, {"status": "updated"})

        # Step 5: Decision Routing
        step5_id = tracker.start_step("decision_routing", input_data={"Ligar?": agent_decision.get("Ligar?"), "min": agent_decision.get("min")})
        
        should_call = bool(agent_decision.get("Ligar?"))
        delay_minutes = int(agent_decision.get("min") or 0)
        call_predict_enabled = bool(client_config.get("call_predict_enabled") or client_config.get("Call_predict"))

        routing_result = None

        if should_call:
            if call_predict_enabled and calls_last_hour < 5:
                routing_result = "forwarded_to_call_predict"
                call_predict_url = client_config.get("call_predict_url") or "http://call-predict:8000/webhook/predict"
                try:
                    async with httpx.AsyncClient() as http_client:
                        await http_client.post(call_predict_url, json=payload, timeout=15.0)
                except Exception:
                    pass
            else:
                routing_result = "forwarded_to_retentativa"
                retry_payload = {**payload, "delay_minutes": delay_minutes, "agent_decision": agent_decision}
                await run_retentativa_workflow(tenant_db, client_config, retry_payload, tracker.execution_id)
        else:
            routing_result = "finished_no_retrial"

        tracker.finish_step_success(step5_id, {"routing_result": routing_result})
        output_res = {"status": "completed", "agent_decision": agent_decision, "routing_result": routing_result}
        tracker.finish_execution_success(output_res)
        return output_res

    except Exception as e:
        error_msg = str(e)
        tracker.finish_execution_failed(error_msg)
        raise e

