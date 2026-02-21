import os
from fastapi import FastAPI, Request, HTTPException
import httpx
import json

from sheets import SheetsRepo
from parser import parse_message

app = FastAPI()

# =========================
# Variáveis de ambiente
# =========================
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "")
WHATSAPP_VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "verify-token")

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
MASTER_CLIENTS_SHEET_ID = os.getenv("MASTER_CLIENTS_SHEET_ID", "")

repo = None
if GOOGLE_SERVICE_ACCOUNT_JSON:
    repo = SheetsRepo(GOOGLE_SERVICE_ACCOUNT_JSON)


# =========================
# Função para enviar msg
# =========================
async def send_whatsapp_text(to_phone: str, text: str):
    if not WHATSAPP_TOKEN or not WHATSAPP_PHONE_NUMBER_ID:
        raise RuntimeError("WHATSAPP_TOKEN ou WHATSAPP_PHONE_NUMBER_ID não configurados")

    url = f"https://graph.facebook.com/v19.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"

@app.post("/webhook")
async def receive_webhook(request: Request):
    if repo is None:
        raise HTTPException(status_code=500, detail="Google Sheets não configurado")

    payload = await request.json()

    # LOG 1: mostra o tipo de evento e um pedaço do payload
    print("WEBHOOK RECEBIDO:", json.dumps(payload)[:800])

    from_phone, text = extract_message(payload)

    # LOG 2: mostra o que seu extractor pegou
    print("EXTRACT:", from_phone, text)...
    {"messaging_product": "whatsapp",
        "to": to_phone,
        "type": "text",
        "text": {
            "body": text
        }
    }

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }

    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(url, json=payload, headers=headers)

    if response.status_code >= 300:
        raise RuntimeError(f"Erro ao enviar mensagem: {response.text}")


# =========================
# Verificação do Webhook
# =========================
@app.get("/webhook")
async def verify_webhook(request: Request):
    params = request.query_params

    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode == "subscribe" and token == WHATSAPP_VERIFY_TOKEN:
        return int(challenge)

    raise HTTPException(status_code=403, detail="Falha na verificação")


# =========================
# Receber mensagens
# =========================
def extract_message(payload: dict):
    try:
        entry = payload["entry"][0]
        changes = entry["changes"][0]
        value = changes["value"]
        message = value["messages"][0]

        from_phone = message["from"]
        msg_type = message["type"]

        if msg_type != "text":
            return from_phone, None

        text = message["text"]["body"]
        return from_phone, text

    except Exception:
        return None, None


@app.post("/webhook")
async def receive_webhook(request: Request):
    if repo is None:
        raise HTTPException(status_code=500, detail="Google Sheets não configurado")

    payload = await request.json()
    from_phone, text = extract_message(payload)

    if not from_phone or not text:
        return {"ok": True}

    phone_e164 = f"+{from_phone}" if not from_phone.startswith("+") else from_phone

    # Descobrir planilha do cliente
    client_sheet_id = repo.resolve_client_sheet_id(
        MASTER_CLIENTS_SHEET_ID,
        phone_e164
    )

    if not client_sheet_id:
        await send_whatsapp_text(from_phone, "Seu número não está cadastrado no sistema.")
        return {"ok": True}

    accounts = repo.get_active_accounts(client_sheet_id)
    categories_map = repo.get_categories_map(client_sheet_id)

    kind, data = parse_message(
        text,
        categories_map=categories_map,
        accounts=accounts
    )

    # =========================
    # Comandos
    # =========================
    if kind == "COMMAND":
        if data["cmd"] == "saldo":
            saldos = repo.get_sum_by_account(client_sheet_id)
            resposta = ["💰 Saldo por conta:"]

            for conta in accounts:
                valor = saldos.get(conta, 0.0)
                resposta.append(f"- {conta}: R$ {valor:.2f}")

            msg = "\n".join(resposta)
            await send_whatsapp_text(from_phone, msg)
            return {"ok": True}

        await send_whatsapp_text(from_phone, "Comando reconhecido, mas ainda não implementado.")
        return {"ok": True}

    # =========================
    # Falta informação
    # =========================
    if kind == "NEED_INFO":
        await send_whatsapp_text(from_phone, f"Faltou informação: {data.get('hint')}")
        return {"ok": True}

    # =========================
    # Transação válida
    # =========================
    if kind == "TX":
        repo.append_transaction(client_sheet_id, data)

        if data["tipo"] == "DESPESA":
            msg = f"✅ Gasto registrado: {data['categoria']} - R$ {data['valor']:.2f} ({data['conta_origem']})"
        elif data["tipo"] == "RECEITA":
            msg = f"✅ Receita registrada: R$ {data['valor']:.2f} ({data['conta_origem']})"
        else:
            msg = f"🔁 Transferência: R$ {data['valor']:.2f} {data['conta_origem']} → {data['conta_destino']}"

        await send_whatsapp_text(from_phone, msg)
        return {"ok": True}

    # =========================
    # Fallback
    # =========================
    await send_whatsapp_text(
        from_phone,
        "Não entendi 😕\nExemplo: padaria 18,90 caixa\nOu: transferir 300 pix -> caixa"
    )
    return {"ok": True}
