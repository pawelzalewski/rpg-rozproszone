# -*- coding: utf-8 -*-
"""Prosty klient RPC używany przez terminal."""

from __future__ import annotations

import time
import uuid

import pika

from wspolne import protokol, walidacja
from wspolne.zegar import ZegarLamporta


class KlientGry:
    def __init__(self, host: str = "localhost", na_zdarzenie=None) -> None:
        self.host = host
        self.na_zdarzenie = na_zdarzenie or (lambda wiadomosc: None)
        self.zegar = ZegarLamporta()
        self.token: str | None = None
        self.kraina: str | None = None
        self.nazwa: str | None = None
        self._odpowiedzi: dict[str, dict] = {}
        self._wiazania: list[str] = []
        self.polaczenie = None
        self.kanal = None


    def polacz(self) -> None:
        parametry = pika.ConnectionParameters(
            host=self.host,
            credentials=pika.PlainCredentials("guest", "guest"),
            heartbeat=30,
        )
        self.polaczenie = pika.BlockingConnection(parametry)
        self.kanal = self.polaczenie.channel()
        self.kanal.exchange_declare(
            exchange=protokol.WYMIANA_TOPIC, exchange_type="topic"
        )
        self._kolejka_odpowiedzi = self.kanal.queue_declare(
            queue="", exclusive=True
        ).method.queue
        self._kolejka_zdarzen = self.kanal.queue_declare(
            queue="", exclusive=True
        ).method.queue
        self.kanal.basic_consume(
            queue=self._kolejka_odpowiedzi,
            on_message_callback=self._na_odpowiedz,
            auto_ack=True,
        )
        self.kanal.basic_consume(
            queue=self._kolejka_zdarzen,
            on_message_callback=self._na_zdarzenie_surowe,
            auto_ack=True,
        )

    def zamknij(self) -> None:
        try:
            if self.polaczenie and self.polaczenie.is_open:
                self.polaczenie.close()
        except Exception:
            pass


    def _na_odpowiedz(self, kanal, metoda, wlasciwosci, cialo) -> None:
        try:
            wiadomosc = walidacja.parsuj(cialo)
        except walidacja.BladWalidacji:
            return  # zniekształcona odpowiedź — ignorujemy
        self.zegar.scal(wiadomosc["lamport"])
        if wlasciwosci.correlation_id:
            self._odpowiedzi[wlasciwosci.correlation_id] = wiadomosc["dane"]

    def _na_zdarzenie_surowe(self, kanal, metoda, wlasciwosci, cialo) -> None:
        try:
            wiadomosc = walidacja.parsuj(cialo)
        except walidacja.BladWalidacji:
            return
        self.zegar.scal(wiadomosc["lamport"])
        self.na_zdarzenie(wiadomosc)

    def przetworz(self, czas: float = 0.1) -> None:
        """Pompuje pętlę zdarzeń pika (odbiór zdarzeń i odpowiedzi)."""
        self.polaczenie.process_data_events(time_limit=czas)


    def komenda(
        self,
        typ: str,
        dane: dict | None = None,
        limit_czasu: float = 8.0,
        id_wiadomosci: str | None = None,
    ) -> dict:
        """Wysyła komendę i czeka na odpowiedź; raz ponawia z tym samym id."""
        dane = dict(dane or {})
        if self.token and typ != "cmd.zaloguj":
            dane.setdefault("token", self.token)
        id_w = id_wiadomosci or uuid.uuid4().hex
        wiadomosc = protokol.zbuduj(
            typ, f"klient:{self.nazwa or 'nowy'}", self.zegar.tik(), dane, id_w
        )
        cialo = protokol.serializuj(wiadomosc)
        wlasciwosci = pika.BasicProperties(
            reply_to=self._kolejka_odpowiedzi, correlation_id=id_w
        )
        for _proba in range(2):
            self.kanal.basic_publish(
                exchange="",
                routing_key=protokol.kolejka_rpc(self.kraina),
                body=cialo,
                properties=wlasciwosci,
            )
            koniec = time.monotonic() + limit_czasu
            while time.monotonic() < koniec:
                self.przetworz(0.1)
                if id_w in self._odpowiedzi:
                    return self._odpowiedzi.pop(id_w)
            # brak odpowiedzi — ponawiamy z TYM SAMYM id (idempotencja)
        return {
            "status": "blad",
            "komunikat": "brak odpowiedzi serwera (kraina może nie żyć)",
        }


    def zaloguj(self, nazwa: str, kraina: str) -> dict:
        self.kraina = kraina
        odpowiedz = self.komenda("cmd.zaloguj", {"nazwa": nazwa})
        if odpowiedz.get("status") == "ok":
            self.token = odpowiedz["token"]
            self.nazwa = nazwa
            self._przepnij_krainy(kraina)
        return odpowiedz

    def patrz(self) -> dict:
        return self.komenda("cmd.patrz")

    def mapa(self) -> dict:
        return self.komenda("cmd.mapa")

    def powiedz(self, tekst: str) -> dict:
        return self.komenda("cmd.powiedz", {"tekst": tekst})

    def idz(self, cel: str) -> dict:
        odpowiedz = self.komenda("cmd.idz", {"cel": cel}, limit_czasu=15.0)
        if odpowiedz.get("status") == "ok":
            self._przepnij_krainy(odpowiedz["kraina"])
        return odpowiedz

    def podnies(self, przedmiot: str, id_wiadomosci: str | None = None) -> dict:
        return self.komenda(
            "cmd.podnies", {"przedmiot": przedmiot}, id_wiadomosci=id_wiadomosci
        )

    def uzyj(self, przedmiot: str) -> dict:
        return self.komenda("cmd.uzyj", {"przedmiot": przedmiot})

    def upusc(self, przedmiot: str) -> dict:
        return self.komenda("cmd.upusc", {"przedmiot": przedmiot})

    def spij(self) -> dict:
        return self.komenda("cmd.spij")

    def atakuj(self, cel: str) -> dict:
        return self.komenda("cmd.atakuj", {"cel": cel})

    def wyloguj(self) -> dict:
        return self.komenda("cmd.wyloguj")


    def _przepnij_krainy(self, kraina: str) -> None:
        """Po zalogowaniu/migracji słuchamy zdarzeń aktualnej krainy."""
        for klucz in self._wiazania:
            self.kanal.queue_unbind(
                queue=self._kolejka_zdarzen,
                exchange=protokol.WYMIANA_TOPIC,
                routing_key=klucz,
            )
        self.kraina = kraina
        self._wiazania = [f"kraina.{kraina}.#", protokol.TEMAT_WYDARZENIE]
        for klucz in self._wiazania:
            self.kanal.queue_bind(
                queue=self._kolejka_zdarzen,
                exchange=protokol.WYMIANA_TOPIC,
                routing_key=klucz,
            )
