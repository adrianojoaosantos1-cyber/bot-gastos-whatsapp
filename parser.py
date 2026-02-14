import re
import uuid
from datetime import datetime
from typing import Dict, Any, Tuple, Optional


def normalize(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("r$", "")
    text = re.sub(r"\s+", " ", text)
    return text


def parse_value(token: str) -> Optional[float]:
    token = token.replace(".", "").replace(",", ".")
    try:
        return float(token)
    except ValueError:
        return None


def parse_message(
    raw: str,
    categories_map: Dict[str, str],
    accounts: list[str],
) -> Tuple[str, Dict[str, Any]]:

    msg = normalize(raw)
    tokens = msg.split()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tx_id = str(uuid.uuid4())

    # -----------------------
    # Comando SALDO
    # -----------------------
    if msg == "saldo":
        return "COMMAND", {"cmd": "saldo"}

    # -----------------------
    # TRANSFERÊNCIA
    # -----------------------
    if msg.startswith("transferir"):
        try:
            valor = next(parse_value(t) for t in tokens if parse_value(t) is not None)
            origem = next(t for t in tokens if t in accounts)
            destino = tokens[-1]

            if destino not in accounts:
                return "NEED_INFO", {"hint": "Conta destino inválida"}

            return "TX", {
                "id": tx_id,
                "data_hora": now,
                "tipo": "TRANSFERENCIA",
                "categoria": "",
                "conta_origem": origem,
                "conta_destino": destino,
                "valor": valor,
                "descricao": "",
                "raw_msg": raw,
                "status": "OK",
            }

        except StopIteration:
            return "NEED_INFO", {"hint": "Ex: transferir 300 pix -> caixa"}

    # -----------------------
    # RECEITA
    # -----------------------
    if msg.startswith(("receita", "entrada", "recebi")):
        try:
            valor = next(parse_value(t) for t in tokens if parse_value(t) is not None)
            conta = next(t for t in tokens if t in accounts)

            return "TX", {
                "id": tx_id,
                "data_hora": now,
                "tipo": "RECEITA",
                "categoria": "outros",
                "conta_origem": conta,
                "conta_destino": "",
                "valor": valor,
                "descricao": "",
                "raw_msg": raw,
                "status": "OK",
            }

        except StopIteration:
            return "NEED_INFO", {"hint": "Ex: receita 1500 caixa"}

    # -----------------------
    # DESPESA (padrão)
    # -----------------------
    try:
        categoria_token = tokens[0]
        categoria = categories_map.get(categoria_token)

        if not categoria:
            return "NEED_INFO", {"hint": "Categoria não reconhecida"}

        valor = next(parse_value(t) for t in tokens if parse_value(t) is not None)
        conta = next(t for t in tokens if t in accounts)

        return "TX", {
            "id": tx_id,
            "data_hora": now,
            "tipo": "DESPESA",
            "categoria": categoria,
            "conta_origem": conta,
            "conta_destino": "",
            "valor": valor,
            "descricao": "",
            "raw_msg": raw,
            "status": "OK",
        }

    except StopIteration:
        return "NEED_INFO", {"hint": "Ex: padaria 18,90 caixa"}
