from datetime import datetime, timezone
from typing import Dict, Any, Optional
import uuid
from supabase import Client


class EDWTracker:
    def __init__(self, supabase: Client, workflow_name: str, trigger_event_id: Optional[str] = None):
        self.supabase = supabase
        self.workflow_name = workflow_name
        self.trigger_event_id = trigger_event_id
        self.execution_id: Optional[str] = None

    def start_execution(self, input_data: Dict[str, Any]) -> str:
        """Insere o registro mestre de execução na tabela workflow_executions."""
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "workflow_name": self.workflow_name,
            "status": "RUNNING",
            "input_data": input_data,
            "started_at": now,
            "created_at": now,
            "updated_at": now,
        }
        if self.trigger_event_id:
            payload["trigger_event_id"] = self.trigger_event_id

        res = self.supabase.table("workflow_executions").insert(payload).execute()
        self.execution_id = res.data[0]["id"]
        return self.execution_id

    def finish_execution_success(self, output_data: Dict[str, Any]):
        """Atualiza o registro mestre para SUCCESS."""
        now = datetime.now(timezone.utc).isoformat()
        self.supabase.table("workflow_executions").update({
            "status": "SUCCESS",
            "output_data": output_data,
            "completed_at": now,
            "updated_at": now
        }).eq("id", self.execution_id).execute()

    def finish_execution_failed(self, error_details: str):
        """Atualiza o registro mestre para FAILED."""
        now = datetime.now(timezone.utc).isoformat()
        self.supabase.table("workflow_executions").update({
            "status": "FAILED",
            "error_details": error_details,
            "completed_at": now,
            "updated_at": now
        }).eq("id", self.execution_id).execute()

    def start_step(self, step_description: str, input_data: Optional[Dict[str, Any]] = None, attempt: int = 1) -> str:
        """Cria o registro do nó em workflow_step_executions."""
        step_name = f"{self.workflow_name}_{step_description}"
        now = datetime.now(timezone.utc).isoformat()
        res = self.supabase.table("workflow_step_executions").insert({
            "execution_id": self.execution_id,
            "step_name": step_name,
            "status": "RUNNING",
            "attempt": attempt,
            "input_data": input_data or {},
            "started_at": now,
            "created_at": now
        }).execute()
        return res.data[0]["id"]

    def finish_step_success(self, step_id: str, output_data: Optional[Dict[str, Any]] = None):
        """Finaliza o nó com status SUCCESS."""
        now = datetime.now(timezone.utc).isoformat()
        self.supabase.table("workflow_step_executions").update({
            "status": "SUCCESS",
            "output_data": output_data or {},
            "completed_at": now
        }).eq("id", step_id).execute()

    def finish_step_failed(self, step_id: str, error_details: str):
        """Finaliza o nó com status FAILED."""
        now = datetime.now(timezone.utc).isoformat()
        self.supabase.table("workflow_step_executions").update({
            "status": "FAILED",
            "error_details": error_details,
            "completed_at": now
        }).eq("id", step_id).execute()
