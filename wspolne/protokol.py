# -*- coding: utf-8 -*-
"""Nazwy kolejek, koperty wiadomości i podpisy HMAC."""

from __future__ import annotations

import hashlib
import hmac
import json
import uuid


WYMIANA_TOPIC = "gra.topic"

TEMAT_HEARTBEAT = "system.heartbeat"
TEMAT_KOORDYNATOR = "system.koordynator"
TEMAT_REJESTR = "globalne.rejestr"
TEMAT_GRACZE = "globalne.gracze"
TEMAT_WYDARZENIE = "globalne.wydarzenie"


def kolejka_rpc(id_serwera: str) -> str:
    """Nazwa kolejki RPC danego serwera krainy."""
    return f"serwer.{id_serwera}.rpc"


def temat_krainy(id_krainy: str) -> str:
    """Klucz routingu zdarzeń widocznych dla graczy w krainie."""
    return f"kraina.{id_krainy}.zdarzenie"



TYPY_MIEDZYSERWEROWE = {
    "puls",
    "ra.prosba",
    "ra.zgoda",
    "2pc.przygotuj",
    "2pc.glos",
    "2pc.zatwierdz",
    "2pc.przerwij",
    "2pc.potwierdzenie",
    "elekcja.wybory",
    "elekcja.ok",
    "elekcja.koordynator",
    "rejestr.aktualizacja",
    "rejestr.migawka",
    "gracz.aktualizacja",
    "wydarzenie.globalne",
}




def zbuduj(
    typ: str,
    nadawca: str,
    lamport: int,
    dane: dict | None = None,
    id_wiadomosci: str | None = None,
) -> dict:
    """Buduje kopertę wiadomości. ``id_wiadomosci`` można podać jawnie,
    aby ponowić wysyłkę tej samej logicznej wiadomości (idempotencja)."""
    return {
        "id": id_wiadomosci or uuid.uuid4().hex,
        "typ": typ,
        "nadawca": nadawca,
        "lamport": int(lamport),
        "dane": dane if dane is not None else {},
    }


def serializuj(wiadomosc: dict) -> bytes:
    return json.dumps(wiadomosc, ensure_ascii=False).encode("utf-8")




def _kanonicznie(wiadomosc: dict) -> bytes:
    """Deterministyczna (kanoniczna) postać rdzenia koperty do podpisu."""
    rdzen = {k: wiadomosc[k] for k in ("id", "typ", "nadawca", "lamport", "dane")}
    return json.dumps(
        rdzen, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def podpisz(wiadomosc: dict, sekret: str) -> dict:
    """Dokłada do koperty podpis HMAC-SHA256 liczony ze wspólnego sekretu serwerów."""
    wiadomosc["podpis"] = hmac.new(
        sekret.encode("utf-8"), _kanonicznie(wiadomosc), hashlib.sha256
    ).hexdigest()
    return wiadomosc


def sprawdz_podpis(wiadomosc: dict, sekret: str) -> bool:
    """Weryfikuje podpis koperty; brak lub zły podpis => wiadomość odrzucana."""
    podpis = wiadomosc.get("podpis")
    if not isinstance(podpis, str):
        return False
    oczekiwany = hmac.new(
        sekret.encode("utf-8"), _kanonicznie(wiadomosc), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(podpis, oczekiwany)
