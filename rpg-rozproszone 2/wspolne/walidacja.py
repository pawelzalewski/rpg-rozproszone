# -*- coding: utf-8 -*-
"""Prosta walidacja wiadomości z RabbitMQ."""

from __future__ import annotations

import json
import re

MAKS_ROZMIAR = 64 * 1024  # maksymalny rozmiar wiadomości w bajtach
MAKS_NAPIS = 300  # maksymalna długość pojedynczego pola tekstowego
MAKS_LAMPORT = 10**12

WZORZEC_ID = re.compile(r"^[0-9a-fA-F\-]{8,64}$")
WZORZEC_NADAWCY = re.compile(r"^(serwer|klient):[A-Za-z0-9_\-]{1,32}$")
WZORZEC_NAZWY_GRACZA = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]{1,19}$")
WZORZEC_TOKENU = re.compile(r"^[0-9a-f]{32}$")


class BladWalidacji(ValueError):
    """Wiadomość nie przeszła walidacji i została odrzucona."""


# Schemat pól ``dane`` dla każdego dozwolonego typu wiadomości:
#   pole -> (typ_python, czy_wymagane)
# Wartość None oznacza: ``dane`` musi być słownikiem, ale treść jest dowolna
# (dotyczy wyłącznie wiadomości tworzonych przez serwer dla klienta).
SCHEMATY: dict[str, dict | None] = {
    # komendy klienta (autoryzacja tokenem sesji)
    "cmd.zaloguj": {"nazwa": (str, True)},
    "cmd.patrz": {"token": (str, True)},
    "cmd.idz": {"token": (str, True), "cel": (str, True)},
    "cmd.podnies": {"token": (str, True), "przedmiot": (str, True)},
    "cmd.uzyj": {"token": (str, True), "przedmiot": (str, True)},
    "cmd.upusc": {"token": (str, True), "przedmiot": (str, True)},
    "cmd.spij": {"token": (str, True)},
    "cmd.atakuj": {"token": (str, True), "cel": (str, True)},
    "cmd.mapa": {"token": (str, True)},
    "cmd.powiedz": {"token": (str, True), "tekst": (str, True)},
    "cmd.wyloguj": {"token": (str, True)},
    # odpowiedzi RPC i zdarzenia dla klientów (tworzone przez serwer)
    "odpowiedz": None,
    "zdarzenie": None,
    # wykrywanie awarii (heartbeat)
    "puls": {"numer": (int, True)},
    # wzajemne wykluczanie Ricarta-Agrawali
    "ra.prosba": {"zasob": (str, True), "znacznik": (list, True)},
    "ra.zgoda": {"zasob": (str, True)},
    # dwufazowy commit (migracja postaci)
    "2pc.przygotuj": {"tx": (str, True), "postac": (dict, True), "token": (str, True)},
    "2pc.glos": {"tx": (str, True), "glos": (str, True), "powod": (str, False)},
    "2pc.zatwierdz": {"tx": (str, True)},
    "2pc.przerwij": {"tx": (str, True)},
    "2pc.potwierdzenie": {"tx": (str, True)},
    # elekcja lidera (algorytm Bully)
    "elekcja.wybory": {"numer": (int, True)},
    "elekcja.ok": {"numer": (int, True)},
    "elekcja.koordynator": {"numer": (int, True)},
    # replikacja rejestru przedmiotów
    "rejestr.aktualizacja": {"przedmiot": (str, True), "wpis": (dict, True)},
    "rejestr.migawka": {"wpisy": (dict, True)},
    # replikowana obecność graczy (globalna unikalność nazw + mapa)
    "gracz.aktualizacja": {
        "nazwa": (str, True),
        "kraina": (str, True),
        "status": (str, True),
    },
    # wydarzenia globalne od lidera
    "wydarzenie.globalne": {
        "id_wydarzenia": (str, True),
        "tresc": (str, True),
        "efekt": (dict, True),
    },
}


def waliduj_nazwe_gracza(nazwa) -> bool:
    return isinstance(nazwa, str) and bool(WZORZEC_NAZWY_GRACZA.match(nazwa))


def _waliduj_dane(typ: str, dane: dict) -> None:
    schemat = SCHEMATY[typ]
    if schemat is None:
        return  # treść dowolna (wiadomości serwera do klienta)
    for pole in dane:
        if pole not in schemat:
            raise BladWalidacji(f"nieznane pole '{pole}' w danych typu {typ}")
    for pole, (typ_py, wymagane) in schemat.items():
        if pole not in dane:
            if wymagane:
                raise BladWalidacji(f"brak wymaganego pola '{pole}' ({typ})")
            continue
        wartosc = dane[pole]
        if not isinstance(wartosc, typ_py) or isinstance(wartosc, bool):
            raise BladWalidacji(f"pole '{pole}' ma zły typ ({typ})")
        if isinstance(wartosc, str) and len(wartosc) > MAKS_NAPIS:
            raise BladWalidacji(f"pole '{pole}' jest za długie ({typ})")
    # znacznik żądania Ricarta-Agrawali to para (lamport, id_serwera)
    if typ == "ra.prosba":
        z = dane["znacznik"]
        if (
            len(z) != 2
            or not isinstance(z[0], int)
            or isinstance(z[0], bool)
            or not isinstance(z[1], str)
            or len(z[1]) > 32
        ):
            raise BladWalidacji("zły znacznik żądania ra.prosba")


def parsuj(cialo: bytes) -> dict:
    """Parsuje i waliduje surową wiadomość z RabbitMQ.

    Zwraca zwalidowaną kopertę albo rzuca :class:`BladWalidacji`.
    """
    if not isinstance(cialo, (bytes, bytearray)) or len(cialo) > MAKS_ROZMIAR:
        raise BladWalidacji("wiadomość pusta lub zbyt duża")
    try:
        wiadomosc = json.loads(cialo.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as blad:
        raise BladWalidacji(f"niepoprawny JSON: {blad}") from blad
    if not isinstance(wiadomosc, dict):
        raise BladWalidacji("koperta nie jest obiektem JSON")

    klucze = set(wiadomosc)
    if not {"id", "typ", "nadawca", "lamport", "dane"} <= klucze:
        raise BladWalidacji("koperta nie ma kompletu pól")
    if klucze - {"id", "typ", "nadawca", "lamport", "dane", "podpis"}:
        raise BladWalidacji("koperta zawiera nadmiarowe pola")

    if not isinstance(wiadomosc["id"], str) or not WZORZEC_ID.match(wiadomosc["id"]):
        raise BladWalidacji("złe pole 'id'")
    if wiadomosc["typ"] not in SCHEMATY:
        raise BladWalidacji(f"nieznany typ wiadomości: {wiadomosc['typ']!r}")
    if not isinstance(wiadomosc["nadawca"], str) or not WZORZEC_NADAWCY.match(
        wiadomosc["nadawca"]
    ):
        raise BladWalidacji("złe pole 'nadawca'")
    lamport = wiadomosc["lamport"]
    if (
        not isinstance(lamport, int)
        or isinstance(lamport, bool)
        or not (0 <= lamport <= MAKS_LAMPORT)
    ):
        raise BladWalidacji("złe pole 'lamport'")
    if not isinstance(wiadomosc["dane"], dict):
        raise BladWalidacji("pole 'dane' nie jest słownikiem")

    _waliduj_dane(wiadomosc["typ"], wiadomosc["dane"])
    return wiadomosc
