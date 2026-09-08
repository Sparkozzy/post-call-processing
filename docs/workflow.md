# Especificação de Workflow: `post_call_processing`

## 🎯 Objetivo

O microsserviço **`post_call_processing`** é um serviço centralizado em Python (FastAPI + Supabase + EDW) responsável por receber eventos HTTP POST de pós-chamada da **Retell AI**, persistir histórico de chamadas em tempo real, executar análises de ligação e aplicar a política dinâmica de retentativas de discagem para múltiplos clientes de forma totalmente configurável via banco de dados.

---

## 🔐 Autenticação & Validação de Tenant

- **Endpoint:** `POST /webhook/retell/{client_id}?token={mindflow_api_token}`
- **Mecanismo:** Valida se o `client_id` existe na tabela `client_configurations` (Supabase Master) e se o `mindflow_api_token` informado bate com o cadastrado para aquele cliente.
- **Isolamento:** Após a autenticação, o serviço conecta dinamicamente ao Supabase do cliente usando as credenciais privadas retornadas (`supabase_url` e `supabase_service_key`).

---

## ⚡ 1. Workflow: `post_call_webhook_ligacao`

### Passos (Nodes):
1. **`post_call_webhook_ligacao_auth`**: Valida `client_id` e `token` na tabela `client_configurations`.
2. **`post_call_webhook_ligacao_init_edw`**: Insere o registro mestre de execução na tabela `workflow_executions` (`workflow_name = 'post_call_webhook_ligacao'`).
3. **`post_call_webhook_ligacao_raw_ingestion`**: Insere/atualiza o evento e metadados brutos da ligação na tabela `Retell_calls_Mindflow` do cliente.
4. **`post_call_webhook_ligacao_evaluate`**:
   - Se `event == 'call_analyzed'` E `user_sentiment != 'negative'` E possui transcrição válida: direciona para o workflow `post_call_analysis`.
   - Se `event == 'call_ended'` com erro (`dial_failed`, `dial_busy`) OU `disconnection_reason == 'voicemail_reached'` OU ausência de transcrição: direciona para o workflow `post_call_retentativa`.

---

## 📊 2. Workflow: `post_call_analysis`

### Passos (Nodes):
1. **`post_call_analysis_init_edw`**: Insere o registro mestre de execução (`workflow_name = 'post_call_analysis'`, `trigger_event_id = execution_id_pai`).
2. **`post_call_analysis_extract`**: Extrai duração, custos (ElevenLabs, LLM, combinado), transcrição completa e resumo.
3. **`post_call_analysis_update_db`**: Atualiza a linha correspondente em `Retell_calls_Mindflow` do cliente com os dados consolidados.

---

## 🔄 3. Workflow: `post_call_retentativa`

### Passos (Nodes):
1. **`post_call_retentativa_init_edw`**: Insere o registro mestre de execução (`workflow_name = 'post_call_retentativa'`, `trigger_event_id = execution_id_pai`).
2. **`post_call_retentativa_check_scheduled`**: Consulta a tabela `agendamentos` e `Retell_Leads_Midflow` do cliente. Se o lead já possui reunião agendada (`Reuniao_marcada` preenchida), aborta a retentativa com status `SUCCESS` e resultado `aborted_meeting_scheduled`.
3. **`post_call_retentativa_check_max_attempts`**: Consulta a contagem total de ligações para aquele número na `Retell_calls_Mindflow`. Se `tentativas >= max_retry_attempts` (configurado em `client_configurations`), aborta com resultado `aborted_max_attempts_reached`.
4. **`post_call_retentativa_check_business_hours`**: Avalia o horário atual no fuso `America/Sao_Paulo`. Se estiver fora da janela comercial (`business_hours_start` a `business_hours_end`), calcula o tempo de espera até a reabertura do horário comercial.
5. **`post_call_retentativa_build_context`**:
   - Recupera os resumos das últimas chamadas do lead em `Retell_calls_Mindflow`.
   - Monta o contexto enriquecido: *"Você já tentou contato com esta pessoa X vezes e não obteve sucesso. Resumo das conversas anteriores: {resumo_historico}."*
6. **`post_call_retentativa_dispatch_retell`**: Faz o disparo da chamada ativas via API Retell (`POST /v2/create-phone-call`) utilizando o `agent_id` e `from_number` originais da chamada.
7. **`post_call_retentativa_upsert_lead`**: Incrementa a contagem de tentativas em `Retell_Leads_Midflow`.
