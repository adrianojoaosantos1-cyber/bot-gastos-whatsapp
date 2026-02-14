import json
from typing import Any, Dict, List, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class SheetsRepo:
    def __init__(self, service_account_json: str):
        info = json.loads(service_account_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        self.svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def _get_values(self, sheet_id: str, range_a1: str) -> List[List[Any]]:
        resp = (
            self.svc.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=range_a1)
            .execute()
        )
        return resp.get("values", [])

    def _append_values(self, sheet_id: str, range_a1: str, rows: List[List[Any]]) -> None:
        (
            self.svc.spreadsheets()
            .values()
            .append(
                spreadsheetId=sheet_id,
                range=range_a1,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": rows},
            )
            .execute()
        )

    def resolve_client_sheet_id(self, master_sheet_id: str, phone_e164: str) -> Optional[str]:
        values = self._get_values(master_sheet_id, "Clientes!A2:D")
        phone_e164 = phone_e164.strip()

        for row in values:
            if len(row) < 4:
                continue

            telefone = (row[0] or "").strip()
            sheet_id = (row[1] or "").strip()
            status = (row[3] or "").strip().upper()

            if telefone == phone_e164 and status == "ATIVO":
                return sheet_id

        return None

    def get_active_accounts(self, client_sheet_id: str) -> List[str]:
        values = self._get_values(client_sheet_id, "Contas!A2:D")
        accounts: List[str] = []

        for row in values:
            if len(row) < 4:
                continue
            conta = (row[0] or "").strip().lower()
            ativo = (row[3] or "").strip().upper()
            if conta and ativo == "SIM":
                accounts.append(conta)

        return accounts

    def get_categories_map(self, client_sheet_id: str) -> Dict[str, str]:
        values = self._get_values(client_sheet_id, "Categorias!A2:C")
        m: Dict[str, str] = {}

        for row in values:
            if len(row) < 3:
                continue

            categoria = (row[0] or "").strip().lower()
            apelidos = (row[1] or "").strip().lower() if len(row) > 1 else ""
            ativo = (row[2] or "").strip().upper()

            if not categoria or ativo != "SIM":
                continue

            m[categoria] = categoria
            for a in [x.strip() for x in apelidos.split(",") if x.strip()]:
                m[a] = categoria

        return m

    def append_transaction(self, client_sheet_id: str, tx: Dict[str, Any]) -> None:
        row = [
            tx.get("id", ""),
            tx.get("data_hora", ""),
            tx.get("tipo", ""),
            tx.get("categoria", ""),
            tx.get("conta_origem", ""),
            tx.get("conta_destino", ""),
            tx.get("valor", ""),
            tx.get("descricao", ""),
            tx.get("raw_msg", ""),
            tx.get("status", "OK"),
        ]
        self._append_values(client_sheet_id, "Lancamentos!A1", [row])

    def get_sum_by_account(self, client_sheet_id: str) -> Dict[str, float]:
        values = self._get_values(client_sheet_id, "Lancamentos!A2:J")
        bal: Dict[str, float] = {}

        for row in values:
            if len(row) < 7:
                continue

            tipo = (row[2] or "").strip().upper()
            conta_origem = (row[4] or "").strip().lower()
            conta_dest = (row[5] or "").strip().lower()

            try:
                valor = float(str(row[6]).replace(",", "."))
            except Exception:
                continue

            if tipo == "DESPESA" and conta_origem:
                bal[conta_origem] = bal.get(conta_origem, 0.0) - valor
            elif tipo == "RECEITA" and conta_origem:
                bal[conta_origem] = bal.get(conta_origem, 0.0) + valor
            elif tipo == "TRANSFERENCIA":
                if conta_origem:
                    bal[conta_origem] = bal.get(conta_origem, 0.0) - valor
                if conta_dest:
                    bal[conta_dest] = bal.get(conta_dest, 0.0) + valor

        return bal
