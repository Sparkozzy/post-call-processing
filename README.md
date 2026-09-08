# 📞 post_call_processing

Microsserviço em Python (FastAPI + Supabase Multi-Tenant + EDW) responsável pelo recebimento centralizado de eventos pós-chamada da Retell AI, persistência em tempo real, análise de chamadas e retentativas dinâmicas de discagem.

---

## 🚀 Como Executar Localmente

### 1. Requisitos
- Python 3.10+
- `.env` baseado em `.env.example`

### 2. Instalação de Dependências
```bash
pip install -e .
```

### 3. Rodar o Servidor Dev
```bash
python src/main.py
```
Servidor acessível em: `http://localhost:8000`

---

## 📌 Endpoint Principal

```http
POST /webhook/retell/{client_id}?token={mindflow_api_token}
```
