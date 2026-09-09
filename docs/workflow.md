# Especificação de Workflow: `post_call_processing`

## 🎯 Objetivo

O microsserviço **`post_call_processing`** é um serviço centralizado em Python (FastAPI + Supabase + EDW) responsável por receber eventos HTTP POST de pós-chamada, persistir histórico de chamadas em tempo real, executar análises inteligentes de ligação via IA (LangChain / RAG / OpenAI) e aplicar a política dinâmica de retentativas de discagem para múltiplos clientes de forma totalmente configurável via banco de dados.

---

## 🔐 Autenticação & Validação de Tenant

- **Endpoint:** `POST /webhook/post-call/{client_id}?token={mindflow_api_token}`
- **Mecanismo:** Valida se o `client_id` existe na tabela `client_configurations` (Supabase Master) e se o `mindflow_api_token` informado bate com o cadastrado para aquele cliente.
- **Isolamento:** Após a autenticação, o serviço conecta dinamicamente ao Supabase do cliente usando as credenciais privadas retornadas (`supabase_url` e `supabase_service_key`).
- **Configuração de Clientes (`client_configurations`):** Inclui a coluna `call_predict_enabled` (BOOLEAN, ou `Call_predict`), que define se o tenant utiliza o motor de IA Preditiva para retentativas.

---

## ⚡ 1. Workflow: `post_call_webhook_ligacao` (Ingestão & Roteamento Inicial)

### Passos (Nodes):
1. **`post_call_webhook_ligacao_auth`**: Valida `client_id` e `token` na tabela `client_configurations` (Supabase Master).
2. **`post_call_webhook_ligacao_init_edw`**: Insere o registro mestre de execução na tabela `workflow_executions` (`workflow_name = 'post_call_webhook_ligacao'`) no Supabase do cliente.
3. **`post_call_webhook_ligacao_raw_ingestion`**: Insere/atualiza o evento e metadados brutos da ligação na tabela `Retell_calls_Mindflow` do cliente.
4. **`post_call_webhook_ligacao_async_dispatch`**:
   > ⚠️ **CRÍTICO:** O processamento pós-ligação DEVE ocorrer de forma estritamente **assíncrona** (Background Tasks / Worker Queues), retornando HTTP `200 OK` imediatamente ao gateway para liberar a requisição e evitar timeouts no webhook.
   - Dispara em background o pipeline de **Processamento de IA e Roteamento**.
5. **`post_call_webhook_ligacao_route_decision`**:
   - Consulta a flag `call_predict_enabled` (ou `Call_predict`) em `client_configurations` (Supabase Master).
   - **Se `Call_predict == True`**:
     - Ao invés de seguir o fluxo tradicional de espera estática (ex: 5 horas / `min`), o lead é **encaminhado diretamente para o microsserviço `call_predict`** para cálculo preditivo do melhor momento de chamada.
     - O lead só é roteado para a `post_call_retentativa` convencional caso **não tenha sido atingido o limite máximo de ligações na última hora**.
   - **Se `Call_predict == False` (ou não configurado)**:
     - Encaminha para o workflow `post_call_analysis_ai` (Agente de IA).

---

## 🧠 2. Workflow: `post_call_analysis_ai` (Agente de Decisão por IA & RAG)

Este é o fluxo principal de inteligência pós-chamada. Um **Agente de IA Autônomo** (LangChain / OpenAI) alimentado por prompt estruturado e exemplos históricos (RAG) analisa a transcrição e decide se deve ou não ligar novamente para o lead e em quantos minutos.

### Entradas do Fluxo:
- `Email_Lead`, `transcrição`, `numero`, `nome`, `prompt`

### Passos (Nodes):
1. **`post_call_analysis_init_edw`**: Insere registro mestre de execução (`workflow_name = 'post_call_analysis_ai'`, `trigger_event_id = execution_id_pai`).
2. **`post_call_analysis_history_check`**:
   - Consulta chamadas passadas na tabela `Retell_calls_Mindflow` do cliente filtrando por `numero`.
   - **Regra de Contagem de Ligações:** A contagem de tentativas passadas DEVE ser realizada agrupando/contando o número de `call_id`s **únicos** (`count(DISTINCT call_id)` onde `call_id` não é nulo). Nem todo registro na tabela é uma ligação única (devido a múltiplos eventos da Retell), portanto somente `call_id`s distintos representam chamadas reais.
   - **Filtro de Limite na Última Hora:** Verifica o número de ligações efetuadas para aquele `numero` na última 1 hora.
3. **`post_call_analysis_vector_rag`**:
   - Busca exemplos de poucas demonstrações (few-shot RAG) no Supabase Vector Store (`documents_fil` via `OpenAIEmbeddings`) com base na transcrição atual.
   - ⚠️ **REGRA OBRIGATÓRIA DE RAG:** A consulta de RAG na tabela `documents_fil` DEVE ser realizada SEMPRE no **Supabase Master (projeto principal "Ryan")**, utilizando a chave de conexão master, e NUNCA no banco isolado do cliente.
4. **`post_call_analysis_agent_evaluation`**:
   - **Agente de Decisão por IA:** Executa o modelo LLM (`gpt-4.1-nano` / LangChain LLM Chain) utilizando o prompt mestre e os exemplos obtidos via RAG para decidir autônoma e fundamentadamente:
     - **`Ligar?`** (`boolean`): Se deve realizar nova tentativa de ligação.
     - **`min`** (`number`): Quando (em quantos minutos) a próxima tentativa deve ocorrer.
     - `causa_raiz` (`Humano`, `Caixa Postal`, `URA`, `Queda`, `Falha`)
     - `nivel_interesse` (`Quente`, `Morno`, `Frio`, `Nulo`)
     - `alerta_sdr` (`string`)
     - `context` (`string`)
     - `drop_state` (`string`)
   - Utiliza Output Parser com Auto-Fixing (JSON Estruturado) para garantir retorno estritamente válido.
5. **`post_call_analysis_decision_routing`**:
   - Se `Ligar? == true`:
     - Se `Call_predict == True` E o limite de ligações na última hora não foi excedido: encaminha diretamente para o microsserviço `call_predict`.
     - Caso contrário: encaminha para o workflow `post_call_retentativa` agendando os `min` minutos informados pelo Agente (respeitando a janela comercial 09:00 - 18:00, Seg-Sex).
   - Se `Ligar? == false` (ex.: número errado, reunião agendada, objeção definitiva), encerra o ciclo de retentativas para o lead (`min = 0`).

---

## 🔄 3. Workflow: `post_call_retentativa` (Execução de Agendamento e Rediscagem)

> ⚠️ **DESCONTINUAÇÃO DA TABELA RETELL_LEADS:** A tabela `retell_leads` / `Retell_Leads_Midflow` foi oficialmente descontinuada e NÃO deve ser consultada nem atualizada em nenhuma etapa deste ou de qualquer outro fluxo. Toda a checagem de status de agendamento é feita na tabela `agendamentos` e o histórico em `Retell_calls_Mindflow`.

### Passos (Nodes):
1. **`post_call_retentativa_init_edw`**: Insere o registro mestre de execução (`workflow_name = 'post_call_retentativa'`).
2. **`post_call_retentativa_check_scheduled`**: Consulta exclusivamente a tabela `agendamentos` (Supabase do cliente). Se o lead já tiver reunião marcada (`status == 'agendado'`) ou `Ligar? == false`, aborta o disparo da retentativa com status `SUCCESS` e resultado `aborted_meeting_scheduled`.
3. **`post_call_retentativa_check_hourly_limit`**: Verifica se o limite de ligações na última hora para aquele número foi atingido. Se sim, aguarda a liberação da janela de taxa antes de agendar.
4. **`post_call_retentativa_wait`**: Aguarda os `min` minutos decididos pelo Agente de IA (ou o tempo do `call_predict`), ajustados para a janela comercial válida (09:00–18:00, Seg–Sex).
5. **`post_call_retentativa_build_payload`**: Formata o payload de requisição para a API do microsserviço de disparo `pre_call_processing` (`client_id`, `numero`, `nome`, `email`, `prompt`, `contexto`).
6. **`post_call_retentativa_dispatch_pre_call`**:
   - Efetua a chamada POST HTTP para a API interna do microsserviço **`pre_call_processing`**.
   - ⚠️ **REGRA DE DISPARO:** Todas as chamadas telefônicas de retentativa DEVEM ser disparadas através do serviço `pre_call_processing`, e NUNCA chamando a API externa da Retell AI diretamente.
7. **`post_call_retentativa_finalize_edw`**:
   - Atualiza o registro no EDW (`workflow_executions`) definindo o status como `SUCCESS` e salvando o `output_data`.
   - ⚠️ **REGRA ESTRITA DE INGESTÃO:** Este passo NUNCA escreve nem altera linhas na tabela `Retell_calls_Mindflow`. Os únicos registros permitidos nessa tabela são aqueles inseridos/atualizados via evento de webhook recebido da Retell AI no workflow `post_call_webhook_ligacao_raw_ingestion`.
