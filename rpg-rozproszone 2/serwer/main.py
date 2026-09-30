# -*- coding: utf-8 -*-
"""Serwer jednej krainy gry."""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
import uuid
from pathlib import Path

import pika
import pika.exceptions

from serwer import walka as modul_walki
from serwer.dwa_pc import Migracje2PC
from serwer.elekcja import ElekcjaBully
from serwer.puls import Puls
from serwer.rejestr import RejestrPrzedmiotow
from serwer.ricart_agrawala import RicartAgrawala
from serwer.sesje import MenedzerSesji
from serwer.stan import Postac, Potwor, StanKrainy
from wspolne import protokol, walidacja
from wspolne.idempotencja import BRAK, PamiecIdempotencji
from wspolne.zegar import ZegarLamporta

KATALOG_PROJEKTU = Path(__file__).resolve().parent.parent
KATALOG_KONFIG = KATALOG_PROJEKTU / "config"

WEDROWNE_POTWORY = [
    {"nazwa": "Zbłąkany Rozbójnik", "hp": 35, "atak": 7},
    {"nazwa": "Głodny Ogr", "hp": 55, "atak": 10},
    {"nazwa": "Cień Wędrowca", "hp": 45, "atak": 9},
]

TEKSTY_WYDARZEN_GLOBALNYCH = [
    "Zaćmienie księżyca! Kojący chłód nocy leczy rany wszystkich wędrowców.",
    "Deszcz spadających gwiazd rozświetla niebo nad wszystkimi krainami.",
    "Prastara pieśń niesie się echem przez świat, krzepiąc serca bohaterów.",
]


def wczytaj_konfiguracje() -> dict:
    """Wczytuje konfiguracje wszystkich krain (struktura świata)."""
    krainy = {}
    for plik in sorted(KATALOG_KONFIG.glob("*.json")):
        with open(plik, encoding="utf-8") as f:
            konfig = json.load(f)
        krainy[konfig["id"]] = konfig
    return krainy


class SerwerKrainy:
    def __init__(self, id_krainy: str, host_rabbit: str) -> None:
        self.wszystkie = wczytaj_konfiguracje()
        if id_krainy not in self.wszystkie:
            raise SystemExit(f"Nieznana kraina: {id_krainy!r}")
        self.id = id_krainy
        self.konfig = self.wszystkie[id_krainy]
        self.nadawca = f"serwer:{self.id}"
        self.host_rabbit = host_rabbit
        self.sekret = os.environ.get("SEKRET_SERWEROW", "tajny-sekret-krain")
        self.log = logging.getLogger(self.id)

        self.zegar = ZegarLamporta()
        self.stan = StanKrainy(self.konfig)
        self.rejestr = RejestrPrzedmiotow(self.wszystkie.values())
        self.sesje = MenedzerSesji()
        self.idem_komend = PamiecIdempotencji()
        self.idem_serwerowa = PamiecIdempotencji()
        self.gracze_globalnie: dict[str, dict] = {}
        self.zwyciezca: str | None = None
        self.straznicy_przedmiotow: dict[str, str] = {}
        self.odblokowane_przedmioty: set[str] = set()
        self.inni = [k for k in self.wszystkie if k != self.id]

        self.polaczenie: pika.BlockingConnection | None = None
        self.kanal = None
        self._licznik_tykow = 0

        self.puls = Puls(
            self.id,
            publikuj=self._publikuj_puls,
            zaplanuj=self._zaplanuj,
            na_smierc=self._na_smierc_wezla,
            na_ozycie=self._na_ozycie_wezla,
            log=self.log,
        )
        self.ra = RicartAgrawala(
            self.id,
            self.inni,
            self.zegar,
            wyslij=self._wyslij_rpc,
            czy_zywy=self.puls.czy_zywy,
            log=self.log,
        )
        self.elekcja = ElekcjaBully(
            self.id,
            numery={k: v["numer"] for k, v in self.wszystkie.items()},
            wyslij=self._wyslij_rpc,
            oglos=self._oglos_koordynatora,
            zaplanuj=self._zaplanuj,
            czy_zywy=self.puls.czy_zywy,
            na_zmiane_lidera=self._na_zmiane_lidera,
            log=self.log,
        )
        self.migracje = Migracje2PC(
            self.id,
            wyslij=self._wyslij_rpc,
            zaplanuj=self._zaplanuj,
            log=self.log,
            katalog_wal=str(KATALOG_PROJEKTU / "dane"),
            usun_postac=self._usun_postac_po_migracji,
            przyjmij_postac=self._przyjmij_postac,
            czy_przyjme=self._czy_przyjme_postac,
            zarezerwuj_nazwe=self.stan.zarezerwowane_nazwy.add,
            zwolnij_nazwe=self.stan.zarezerwowane_nazwy.discard,
        )

    # transport RabbitMQ

    def start(self) -> None:
        self._polacz_z_rabbit()
        kanal = self.kanal
        kanal.exchange_declare(exchange=protokol.WYMIANA_TOPIC, exchange_type="topic")
        kanal.queue_declare(queue=protokol.kolejka_rpc(self.id), auto_delete=True)
        kolejka_tematow = f"serwer.{self.id}.zdarzenia"
        kanal.queue_declare(queue=kolejka_tematow, exclusive=True)
        for klucz in ("system.#", "globalne.#"):
            kanal.queue_bind(
                queue=kolejka_tematow,
                exchange=protokol.WYMIANA_TOPIC,
                routing_key=klucz,
            )
        kanal.basic_consume(
            queue=protokol.kolejka_rpc(self.id),
            on_message_callback=self._na_rpc,
            auto_ack=True,
        )
        kanal.basic_consume(
            queue=kolejka_tematow, on_message_callback=self._na_temat, auto_ack=True
        )

        self.puls.start()
        self._zaplanuj(8.0, self._pierwsza_elekcja)
        self._zaplanuj(10.0, self._tyk_lidera)
        self._zaplanuj(15.0, self._tyk_regeneracji)
        self._zaplanuj(55.0, self._tyk_potworow)
        self.log.info(
            "Kraina '%s' (%s, numer Bully %d) nasłuchuje. Sąsiedzi: %s",
            self.konfig["nazwa"],
            self.id,
            self.konfig["numer"],
            self.konfig["sasiedzi"],
        )
        kanal.start_consuming()

    def _polacz_z_rabbit(self) -> None:
        parametry = pika.ConnectionParameters(
            host=self.host_rabbit,
            credentials=pika.PlainCredentials("guest", "guest"),
            heartbeat=30,
            blocked_connection_timeout=30,
        )
        for proba in range(60):
            try:
                self.polaczenie = pika.BlockingConnection(parametry)
                self.kanal = self.polaczenie.channel()
                return
            except pika.exceptions.AMQPConnectionError:
                self.log.info("RabbitMQ jeszcze nie odpowiada (próba %d)…", proba + 1)
                time.sleep(2)
        raise SystemExit("Nie udało się połączyć z RabbitMQ")

    def _zaplanuj(self, opoznienie: float, funkcja) -> None:
        self.polaczenie.call_later(opoznienie, funkcja)

    def _wyslij_rpc(self, cel_serwera: str, typ: str, dane: dict) -> None:
        """Podpisana wiadomość punkt-punkt do kolejki RPC innego serwera."""
        wiadomosc = protokol.zbuduj(typ, self.nadawca, self.zegar.tik(), dane)
        protokol.podpisz(wiadomosc, self.sekret)
        self.kanal.basic_publish(
            exchange="",
            routing_key=protokol.kolejka_rpc(cel_serwera),
            body=protokol.serializuj(wiadomosc),
        )

    def _publikuj(self, klucz: str, typ: str, dane: dict) -> dict:
        """Podpisana publikacja na wymianie tematycznej gra.topic."""
        wiadomosc = protokol.zbuduj(typ, self.nadawca, self.zegar.tik(), dane)
        protokol.podpisz(wiadomosc, self.sekret)
        self.kanal.basic_publish(
            exchange=protokol.WYMIANA_TOPIC,
            routing_key=klucz,
            body=protokol.serializuj(wiadomosc),
        )
        return wiadomosc

    def _zdarzenie_krainy(
        self, rodzaj: str, tresc: str, szczegoly: dict | None = None
    ) -> None:
        """Zdarzenie widoczne dla wszystkich graczy w tej krainie."""
        self._publikuj(
            protokol.temat_krainy(self.id),
            "zdarzenie",
            {
                "rodzaj": rodzaj,
                "tresc": tresc,
                "kraina": self.id,
                "szczegoly": szczegoly or {},
            },
        )

    def _publikuj_puls(self) -> None:
        self._publikuj(
            protokol.TEMAT_HEARTBEAT, "puls", {"numer": self.konfig["numer"]}
        )

    def _oglos_koordynatora(self, dane: dict) -> None:
        self._publikuj(protokol.TEMAT_KOORDYNATOR, "elekcja.koordynator", dane)

    # odbiór wiadomości

    def _na_rpc(self, kanal, metoda, wlasciwosci, cialo) -> None:
        """Kolejka RPC: komendy klientów + wiadomości algorytmów rozproszonych."""
        try:
            wiadomosc = walidacja.parsuj(cialo)
        except walidacja.BladWalidacji as blad:
            self.log.warning("Odrzucono zniekształconą wiadomość: %s", blad)
            return
        self.zegar.scal(wiadomosc["lamport"])
        typ = wiadomosc["typ"]

        if typ.startswith("cmd."):
            self._obsluz_komende(wiadomosc, wlasciwosci)
            return

        # wiadomości serwer->serwer muszą mieć poprawny podpis HMAC
        if not protokol.sprawdz_podpis(wiadomosc, self.sekret):
            self.log.warning(
                "Odrzucono niepodpisaną/nieautoryzowaną wiadomość typu %s od %s",
                typ,
                wiadomosc["nadawca"],
            )
            return
        if self.idem_serwerowa.widziano(wiadomosc["id"]):
            return  # duplikat — już obsłużona
        self.idem_serwerowa.zapamietaj(wiadomosc["id"])

        od = wiadomosc["nadawca"].split(":", 1)[1]
        dane = wiadomosc["dane"]
        if typ == "ra.prosba":
            self.ra.na_prosbe(od, dane["zasob"], dane["znacznik"])
        elif typ == "ra.zgoda":
            self.ra.na_zgode(od, dane["zasob"])
        elif typ.startswith("2pc."):
            self.migracje.na_wiadomosc(typ, od, dane)
        elif typ == "elekcja.wybory":
            self.elekcja.na_wybory(od)
        elif typ == "elekcja.ok":
            self.elekcja.na_ok(od)

    def _na_temat(self, kanal, metoda, wlasciwosci, cialo) -> None:
        """Tematy system.# i globalne.# (heartbeat, elekcja, replikacja)."""
        try:
            wiadomosc = walidacja.parsuj(cialo)
        except walidacja.BladWalidacji as blad:
            self.log.warning("Odrzucono zniekształconą publikację: %s", blad)
            return
        self.zegar.scal(wiadomosc["lamport"])
        if not protokol.sprawdz_podpis(wiadomosc, self.sekret):
            self.log.warning(
                "Odrzucono niepodpisaną publikację typu %s", wiadomosc["typ"]
            )
            return
        if self.idem_serwerowa.widziano(wiadomosc["id"]):
            return
        self.idem_serwerowa.zapamietaj(wiadomosc["id"])

        typ = wiadomosc["typ"]
        od = wiadomosc["nadawca"].split(":", 1)[1]
        dane = wiadomosc["dane"]
        if typ == "puls":
            if od != self.id:
                self.puls.na_puls(od)
        elif typ == "elekcja.koordynator":
            self.elekcja.na_koordynatora(od, dane["numer"])
        elif typ == "rejestr.aktualizacja":
            if od != self.id and self.rejestr.zastosuj(dane["przedmiot"], dane["wpis"]):
                self.log.info(
                    "REJESTR: replika zaktualizowana — '%s' teraz %s",
                    dane["przedmiot"],
                    dane["wpis"].get("polozenie"),
                )
        elif typ == "rejestr.migawka":
            if od != self.id:
                ile = self.rejestr.scal_migawke(dane["wpisy"])
                if ile:
                    self.log.info(
                        "REJESTR: migawka od lidera '%s' "
                        "zaktualizowała %d wpisów (anti-entropy)",
                        od,
                        ile,
                    )
        elif typ == "gracz.aktualizacja":
            self._zastosuj_aktualizacje_gracza(dane, wiadomosc["lamport"], od)
        elif typ == "wydarzenie.globalne":
            self._zastosuj_wydarzenie_globalne(dane)

    # komendy klientów (RPC z reply_to + correlation_id)

    def _obsluz_komende(self, wiadomosc: dict, wlasciwosci) -> None:
        id_wiadomosci = wiadomosc["id"]
        zapamietana = self.idem_komend.odpowiedz_dla(id_wiadomosci)
        if zapamietana is not BRAK:
            # idempotencja: duplikat komendy nie wykonuje skutków po raz drugi
            if zapamietana is not None:
                self.log.info(
                    "Duplikat komendy %s — odsyłam zapamiętaną odpowiedź",
                    id_wiadomosci[:8],
                )
                self._wyslij_odpowiedz(wlasciwosci, zapamietana)
            return
        self.idem_komend.zapamietaj(id_wiadomosci, None)  # "w trakcie"

        def odpowiedz(wynik: dict) -> None:
            self.idem_komend.zapamietaj(id_wiadomosci, wynik)
            self._wyslij_odpowiedz(wlasciwosci, wynik)

        typ = wiadomosc["typ"]
        dane = wiadomosc["dane"]
        try:
            if typ == "cmd.zaloguj":
                self._cmd_zaloguj(dane, odpowiedz)
                return
            postac, blad = self._autoryzuj(dane)
            if blad:
                odpowiedz(blad)
                return
            if self.zwyciezca and typ not in ("cmd.patrz", "cmd.mapa", "cmd.powiedz"):
                odpowiedz({"status": "blad",
                           "komunikat": f"gra zakończona — wygrał {self.zwyciezca}"})
                return
            if postac.spi_do > time.monotonic() and typ not in ("cmd.patrz", "cmd.mapa", "cmd.powiedz"):
                ile = int(postac.spi_do - time.monotonic()) + 1
                odpowiedz({"status": "blad",
                           "komunikat": f"śpisz jeszcze przez {ile}s"})
                return
            if typ == "cmd.patrz":
                self._cmd_patrz(postac, odpowiedz)
            elif typ == "cmd.idz":
                self._cmd_idz(postac, dane, odpowiedz)
            elif typ == "cmd.podnies":
                self._cmd_podnies(postac, dane, odpowiedz)
            elif typ == "cmd.uzyj":
                self._cmd_uzyj(postac, dane, odpowiedz)
            elif typ == "cmd.upusc":
                self._cmd_upusc(postac, dane, odpowiedz)
            elif typ == "cmd.spij":
                self._cmd_spij(postac, odpowiedz)
            elif typ == "cmd.atakuj":
                self._cmd_atakuj(postac, dane, odpowiedz)
            elif typ == "cmd.mapa":
                self._cmd_mapa(postac, odpowiedz)
            elif typ == "cmd.powiedz":
                self._cmd_powiedz(postac, dane, odpowiedz)
            elif typ == "cmd.wyloguj":
                self._cmd_wyloguj(postac, dane, odpowiedz)
        except Exception:
            self.log.exception("Błąd obsługi komendy %s", typ)
            odpowiedz({"status": "blad", "komunikat": "wewnętrzny błąd serwera"})

    def _wyslij_odpowiedz(self, wlasciwosci, dane_odpowiedzi: dict) -> None:
        if not wlasciwosci or not wlasciwosci.reply_to:
            return
        wiadomosc = protokol.zbuduj(
            "odpowiedz", self.nadawca, self.zegar.tik(), dane_odpowiedzi
        )
        self.kanal.basic_publish(
            exchange="",
            routing_key=wlasciwosci.reply_to,
            body=protokol.serializuj(wiadomosc),
            properties=pika.BasicProperties(correlation_id=wlasciwosci.correlation_id),
        )

    def _autoryzuj(self, dane: dict):
        """Token sesji -> postać. Chroni przed podszywaniem się pod graczy."""
        id_postaci = self.sesje.postac_dla(dane.get("token"))
        postac = self.stan.postacie.get(id_postaci) if id_postaci else None
        if postac is None:
            return None, {
                "status": "blad",
                "komunikat": "nieautoryzowany — nieprawidłowy token "
                "sesji (zaloguj się w tej krainie)",
            }
        if postac.w_podrozy:
            return None, {
                "status": "blad",
                "komunikat": "postać jest w podróży między krainami",
            }
        return postac, None


    def _zastosuj_aktualizacje_gracza(self, dane: dict, lamport: int, od: str) -> None:
        nazwa = dane["nazwa"].strip()
        klucz = nazwa.lower()
        wersja = (int(lamport), od)
        lokalna = self.gracze_globalnie.get(klucz)
        if lokalna and tuple(lokalna.get("wersja", (0, ""))) >= wersja:
            return
        status = dane.get("status", "nieaktywny")
        self.gracze_globalnie[klucz] = {
            "nazwa": nazwa,
            "kraina": dane.get("kraina", ""),
            "status": status,
            "wersja": wersja,
        }

    def _oglos_obecnosc_gracza(self, nazwa: str, kraina: str, status: str) -> None:
        dane = {"nazwa": nazwa, "kraina": kraina, "status": status}
        self._zastosuj_aktualizacje_gracza(dane, self.zegar.wartosc + 1, self.id)
        self._publikuj(protokol.TEMAT_GRACZE, "gracz.aktualizacja", dane)

    def _nazwa_globalnie_zajeta(self, nazwa: str) -> bool:
        if self.stan.nazwa_zajeta(nazwa):
            return True
        wpis = self.gracze_globalnie.get(nazwa.strip().lower())
        return bool(wpis and wpis.get("status") == "aktywny")

    def _aktywni_gracze_globalnie(self) -> list[dict]:
        gracze = {
            klucz: {"nazwa": wpis["nazwa"], "kraina": wpis["kraina"]}
            for klucz, wpis in self.gracze_globalnie.items()
            if wpis.get("status") == "aktywny"
        }
        # Lokalny stan jest źródłem prawdy dla graczy z tej krainy, nawet jeśli
        # aktualizacja obecności jeszcze nie wróciła z brokera.
        for p in self.stan.postacie.values():
            if not p.w_podrozy:
                gracze[p.nazwa.lower()] = {"nazwa": p.nazwa, "kraina": self.id}
        return sorted(gracze.values(), key=lambda g: g["nazwa"].lower())


    def _cmd_zaloguj(self, dane: dict, odpowiedz) -> None:
        nazwa = dane["nazwa"].strip()
        if not walidacja.waliduj_nazwe_gracza(nazwa):
            odpowiedz(
                {
                    "status": "blad",
                    "komunikat": "nazwa: 2-20 znaków, litery/cyfry/-/_, "
                    "zaczyna się literą",
                }
            )
            return
        if self._nazwa_globalnie_zajeta(nazwa):
            odpowiedz(
                {
                    "status": "blad",
                    "komunikat": f"nazwa '{nazwa}' jest już zajęta na świecie gry",
                }
            )
            return
        postac = Postac(id=uuid.uuid4().hex[:12], nazwa=nazwa)
        self.stan.postacie[postac.id] = postac
        token = self.sesje.utworz(postac.id)
        self._zdarzenie_krainy(
            "wejscie", f"{postac.nazwa} przybywa do krainy {self.konfig['nazwa']}."
        )
        self._oglos_obecnosc_gracza(postac.nazwa, self.id, "aktywny")
        self.log.info("Zalogowano '%s' (id %s)", nazwa, postac.id)
        odpowiedz(
            {
                "status": "ok",
                "token": token,
                "postac": postac.do_slownika(),
                "kraina": self.id,
                "komunikat": f"Witaj w krainie {self.konfig['nazwa']}!",
            }
        )


    def _atak_efektywny(self, postac: Postac) -> int:
        bonus = 0
        for pid in postac.ekwipunek:
            info = self.rejestr.info(pid)
            if info:
                bonus += info["bonus_ataku"]
        return postac.atak_bazowy + bonus

    def _obrona_efektywna(self, postac: Postac) -> int:
        bonus = 0
        for pid in postac.ekwipunek:
            info = self.rejestr.info(pid)
            if info:
                bonus += int(info.get("bonus_obrony", 0))
        return bonus

    def _zajete_rece(
        self, postac: Postac, dodatkowy_przedmiot: str | None = None
    ) -> int:
        przedmioty = list(postac.ekwipunek)
        if dodatkowy_przedmiot:
            przedmioty.append(dodatkowy_przedmiot)
        suma = 0
        for pid in przedmioty:
            info = self.rejestr.info(pid)
            if info:
                suma += max(0, int(info.get("rece", 0)))
        return suma

    def _czy_moze_uniesc(self, postac: Postac, pid: str) -> tuple[bool, str]:
        info = self.rejestr.info(pid)
        if not info:
            return False, "nieznany przedmiot"
        rece = max(0, int(info.get("rece", 0)))
        if rece == 0:
            return True, ""
        po_podniesieniu = self._zajete_rece(postac, pid)
        if po_podniesieniu > 2:
            trzymane = [self.rejestr.info(x) for x in postac.ekwipunek]
            nazwy = ", ".join(i["nazwa"] for i in trzymane if i) or "nic"
            return False, (
                f"brakuje wolnej ręki: {info['nazwa']} zajmuje {rece} "
                f"rękę/ręce, a trzymasz już: {nazwy}. Dwuręczna broń "
                "blokuje obie ręce; jednoręczna broń może iść z "
                "drugą jednoręczną bronią albo tarczą"
            )
        return True, ""

    def _opis_bonusow(self, info: dict) -> str:
        czesci = []
        if info.get("bonus_ataku", 0):
            czesci.append(f"+{info['bonus_ataku']} ATK")
        if info.get("bonus_obrony", 0):
            czesci.append(f"+{info['bonus_obrony']} DEF")
        if info.get("leczenie", 0):
            czesci.append(f"leczy {info['leczenie']} HP")
        rece = int(info.get("rece", 0))
        if rece:
            czesci.append(f"{rece}R")
        else:
            czesci.append("0R")
        return ", ".join(czesci)

    def _cmd_patrz(self, postac: Postac, odpowiedz) -> None:
        przedmioty = []
        for pid in self.rejestr.w_krainie(self.id):
            info = self.rejestr.info(pid)
            if not info:
                continue
            przedmioty.append(
                {
                    "id": pid,
                    "nazwa": info["nazwa"],
                    "opis": info["opis"],
                    "typ": info.get("typ", "artefakt"),
                    "podtyp": info.get("podtyp", "inne"),
                    "rece": int(info.get("rece", 0)),
                    "bonus_ataku": info["bonus_ataku"],
                    "bonus_obrony": int(info.get("bonus_obrony", 0)),
                    "leczenie": int(info.get("leczenie", 0)),
                    "zuzywalny": bool(info.get("zuzywalny", False)),
                    "statystyki": self._opis_bonusow(info),
                }
            )
        odpowiedz(
            {
                "status": "ok",
                "kraina": {
                    "id": self.id,
                    "nazwa": self.konfig["nazwa"],
                    "opis": self.konfig["opis"],
                },
                "sasiedzi": [
                    {
                        "id": s,
                        "nazwa": self.wszystkie[s]["nazwa"],
                        "dostepna": self.puls.czy_zywy(s),
                    }
                    for s in self.konfig["sasiedzi"]
                ],
                "koordynator": self.elekcja.lider,
                "gracze": [
                    {
                        "nazwa": p.nazwa,
                        "hp": p.hp,
                        "hp_maks": p.hp_maks,
                        "atak": self._atak_efektywny(p),
                        "obrona": self._obrona_efektywna(p),
                    }
                    for p in self.stan.postacie.values()
                    if not p.w_podrozy
                ],
                "potwory": [
                    {
                        "id": m.id,
                        "nazwa": m.nazwa,
                        "hp": m.hp,
                        "hp_maks": m.hp_maks,
                        "atak": m.atak,
                        "boss": m.boss,
                        "final_boss": m.final_boss,
                    }
                    for m in self.stan.potwory.values()
                ],
                "przedmioty": przedmioty,
                "ty": {
                    "nazwa": postac.nazwa,
                    "hp": postac.hp,
                    "hp_maks": postac.hp_maks,
                    "atak": self._atak_efektywny(postac),
                    "obrona": self._obrona_efektywna(postac),
                    "rece_zajete": self._zajete_rece(postac),
                    "rece_maks": 2,
                    "spi": max(0, int(postac.spi_do - time.monotonic())),
                    "ekwipunek": [
                        {
                            "id": pid,
                            "nazwa": (self.rejestr.info(pid) or {}).get("nazwa", pid),
                            "typ": (self.rejestr.info(pid) or {}).get(
                                "typ", "artefakt"
                            ),
                            "podtyp": (self.rejestr.info(pid) or {}).get(
                                "podtyp", "inne"
                            ),
                            "rece": int((self.rejestr.info(pid) or {}).get("rece", 0)),
                            "bonus_ataku": (self.rejestr.info(pid) or {}).get(
                                "bonus_ataku", 0
                            ),
                            "bonus_obrony": int(
                                (self.rejestr.info(pid) or {}).get("bonus_obrony", 0)
                            ),
                            "leczenie": int(
                                (self.rejestr.info(pid) or {}).get("leczenie", 0)
                            ),
                            "zuzywalny": bool(
                                (self.rejestr.info(pid) or {}).get("zuzywalny", False)
                            ),
                            "statystyki": self._opis_bonusow(
                                self.rejestr.info(pid) or {}
                            ),
                        }
                        for pid in postac.ekwipunek
                    ],
                },
            }
        )


    def _rozwiaz_kraine(self, nazwa_lub_id: str) -> str | None:
        szukana = nazwa_lub_id.strip().lower()
        for id_krainy, konfig in self.wszystkie.items():
            if szukana == id_krainy or szukana == konfig["nazwa"].lower():
                return id_krainy
        return None

    def _cmd_idz(self, postac: Postac, dane: dict, odpowiedz) -> None:
        cel = self._rozwiaz_kraine(dane["cel"])
        if cel is None or cel == self.id or cel not in self.konfig["sasiedzi"]:
            odpowiedz(
                {
                    "status": "blad",
                    "komunikat": f"stąd nie ma przejścia do "
                    f"'{dane['cel']}' (sąsiedzi: "
                    f"{', '.join(self.konfig['sasiedzi'])})",
                }
            )
            return
        if not self.puls.czy_zywy(cel):
            odpowiedz(
                {
                    "status": "blad",
                    "komunikat": f"kraina '{cel}' jest odcięta "
                    f"(serwer nie odpowiada) — spróbuj później",
                }
            )
            return

        nazwa_celu = self.wszystkie[cel]["nazwa"]

        def sukces() -> None:
            odpowiedz(
                {
                    "status": "ok",
                    "kraina": cel,
                    "komunikat": f"Bezpiecznie docierasz do krainy {nazwa_celu}.",
                }
            )

        def porazka(powod: str) -> None:
            odpowiedz({"status": "blad", "komunikat": f"migracja przerwana: {powod}"})

        # atomowa migracja: dwufazowy commit (serwer/dwa_pc.py)
        self.migracje.rozpocznij(postac, dane["token"], cel, sukces, porazka)

    def _usun_postac_po_migracji(self, postac: Postac, token: str, cel: str) -> None:
        """Zaczep koordynatora 2PC: faza DONE — postać zniknęła z tej krainy."""
        self.stan.postacie.pop(postac.id, None)
        self.sesje.usun_token(token)
        self._zdarzenie_krainy(
            "wyjscie",
            f"{postac.nazwa} odchodzi do krainy {self.wszystkie[cel]['nazwa']}.",
        )

    def _czy_przyjme_postac(self, dane_postaci: dict):
        """Zaczep uczestnika 2PC: walidacja przed głosem w fazie PREPARE."""
        try:
            postac = Postac.ze_slownika(dane_postaci)
        except (KeyError, TypeError, ValueError):
            return False, "zniekształcone dane postaci"
        if not walidacja.waliduj_nazwe_gracza(postac.nazwa):
            return False, "niepoprawna nazwa postaci"
        if self.stan.nazwa_zajeta(postac.nazwa) or postac.id in self.stan.postacie:
            return False, f"nazwa '{postac.nazwa}' zajęta w krainie docelowej"
        return True, ""

    def _przyjmij_postac(self, dane_postaci: dict, token: str, od: str) -> None:
        """Zaczep uczestnika 2PC: faza COMMIT — postać pojawia się u nas."""
        postac = Postac.ze_slownika(dane_postaci)
        self.stan.postacie[postac.id] = postac
        self.sesje.przypisz(token, postac.id)
        self._oglos_obecnosc_gracza(postac.nazwa, self.id, "aktywny")
        self._zdarzenie_krainy(
            "wejscie",
            f"{postac.nazwa} przybywa z krainy {self.wszystkie[od]['nazwa']}.",
        )
        self.log.info("Postać '%s' przybyła w migracji 2PC z '%s'", postac.nazwa, od)


    def _straznik_przedmiotu(self, pid: str, info: dict) -> Potwor | None:
        if pid in self.odblokowane_przedmioty:
            return None
        if info.get("typ") == "mikstura" or not info.get("strzezony", True):
            return None
        zywi = [p for p in self.stan.potwory.values() if not p.final_boss]
        if not zywi:
            return None
        if pid not in self.straznicy_przedmiotow:
            moc = int(info.get("bonus_ataku", 0)) + int(info.get("bonus_obrony", 0))
            zywi.sort(key=lambda m: (m.atak + m.hp_maks // 10, m.id))
            indeks = min(len(zywi) - 1, max(0, moc // 6))
            self.straznicy_przedmiotow[pid] = zywi[indeks].id
        return self.stan.potwory.get(self.straznicy_przedmiotow[pid])

    def _cmd_podnies(self, postac: Postac, dane: dict, odpowiedz) -> None:
        pid = self.rejestr.znajdz_w_krainie(self.id, dane["przedmiot"])
        if pid is None:
            odpowiedz(
                {
                    "status": "blad",
                    "komunikat": f"nie ma tu przedmiotu '{dane['przedmiot']}'",
                }
            )
            return
        id_postaci = postac.id

        def sekcja_krytyczna() -> None:
            # Jesteśmy w rozproszonej sekcji krytycznej zasobu przedmiot:<pid>
            # — żaden inny serwer nie modyfikuje teraz tego wpisu rejestru.
            # Warunek sprawdzamy PONOWNIE: między żądaniem a wejściem ktoś
            # mógł podnieść przedmiot pierwszy.
            aktualna = self.stan.postacie.get(id_postaci)
            if (
                aktualna is None
                or aktualna.w_podrozy
                or self.rejestr.gdzie(pid) != ("kraina", self.id)
            ):
                odpowiedz(
                    {
                        "status": "blad",
                        "komunikat": "ktoś był szybszy — przedmiot zniknął sprzed nosa",
                    }
                )
                return
            info_przed = self.rejestr.info(pid) or {}
            straznik = self._straznik_przedmiotu(pid, info_przed)
            if straznik:
                odpowiedz({
                    "status": "blad",
                    "komunikat": f"{info_przed.get('nazwa', pid)} jest strzeżony przez "
                                 f"{straznik.nazwa}. Najpierw pokonaj strażnika."
                })
                return
            moze, powod = self._czy_moze_uniesc(aktualna, pid)
            if not moze:
                odpowiedz({"status": "blad", "komunikat": powod})
                return
            aktualna.ekwipunek.append(pid)
            wpis = self.rejestr.ustaw(
                pid, {"typ": "gracz", "id": aktualna.id}, self.zegar.tik(), self.id
            )
            # replikacja: pozostałe serwery scalają nowszą wersję wpisu
            self._publikuj(
                protokol.TEMAT_REJESTR,
                "rejestr.aktualizacja",
                {"przedmiot": pid, "wpis": wpis},
            )
            info = self.rejestr.info(pid)
            if not info:
                odpowiedz(
                    {"status": "blad", "komunikat": "przedmiot zniknął z rejestru"}
                )
                return
            self._zdarzenie_krainy(
                "podniesiono",
                f"{aktualna.nazwa} podnosi przedmiot {info['nazwa']}.",
                {"przedmiot": pid, "gracz": aktualna.nazwa},
            )
            odpowiedz(
                {
                    "status": "ok",
                    "komunikat": f"Podnosisz: {info['nazwa']} "
                    f"({self._opis_bonusow(info)}).",
                    "przedmiot": {
                        "id": pid,
                        "nazwa": info["nazwa"],
                        "typ": info.get("typ", "artefakt"),
                        "podtyp": info.get("podtyp", "inne"),
                        "rece": int(info.get("rece", 0)),
                        "bonus_ataku": info["bonus_ataku"],
                        "bonus_obrony": int(info.get("bonus_obrony", 0)),
                        "leczenie": int(info.get("leczenie", 0)),
                        "zuzywalny": bool(info.get("zuzywalny", False)),
                    },
                }
            )

        # wzajemne wykluczanie: serwer/ricart_agrawala.py
        self.ra.zajmij(f"przedmiot:{pid}", sekcja_krytyczna)


    def _cmd_uzyj(self, postac: Postac, dane: dict, odpowiedz) -> None:
        szukany = dane["przedmiot"].strip().lower()
        pid = None
        for kandydat in postac.ekwipunek:
            info = self.rejestr.info(kandydat)
            if info and szukany in (kandydat.lower(), info["nazwa"].lower()):
                pid = kandydat
                break
        if pid is None:
            odpowiedz(
                {
                    "status": "blad",
                    "komunikat": f"nie masz przedmiotu '{dane['przedmiot']}'",
                }
            )
            return

        info = self.rejestr.info(pid)
        if not info:
            odpowiedz({"status": "blad", "komunikat": "przedmiot zniknął z rejestru"})
            return
        leczenie = int(info.get("leczenie", 0))
        if not info.get("zuzywalny") or leczenie <= 0:
            odpowiedz(
                {
                    "status": "blad",
                    "komunikat": f"{info['nazwa']} nie jest miksturą do użycia",
                }
            )
            return
        if postac.hp >= postac.hp_maks:
            odpowiedz(
                {
                    "status": "blad",
                    "komunikat": "masz pełne HP — szkoda marnować miksturę",
                }
            )
            return

        hp_przed = postac.hp
        postac.hp = min(postac.hp_maks, postac.hp + leczenie)
        wyleczono = postac.hp - hp_przed
        postac.ekwipunek.remove(pid)
        wpis = self.rejestr.ustaw(
            pid, {"typ": "zuzyty", "id": postac.id}, self.zegar.tik(), self.id
        )
        self._publikuj(
            protokol.TEMAT_REJESTR,
            "rejestr.aktualizacja",
            {"przedmiot": pid, "wpis": wpis},
        )
        self._zdarzenie_krainy(
            "leczenie",
            f"{postac.nazwa} wypija {info['nazwa']} i odzyskuje {wyleczono} HP.",
            {"gracz": postac.nazwa, "przedmiot": pid, "leczenie": wyleczono},
        )
        odpowiedz(
            {
                "status": "ok",
                "komunikat": f"Wypijasz {info['nazwa']} i odzyskujesz "
                f"{wyleczono} HP ({postac.hp}/{postac.hp_maks}).",
                "hp": postac.hp,
                "hp_maks": postac.hp_maks,
                "przedmiot": pid,
            }
        )

    def _cmd_upusc(self, postac: Postac, dane: dict, odpowiedz) -> None:
        szukany = dane["przedmiot"].strip().lower()
        pid = None
        for kandydat in postac.ekwipunek:
            info = self.rejestr.info(kandydat)
            if info and szukany in (kandydat.lower(), info["nazwa"].lower()):
                pid = kandydat
                break
        if pid is None:
            odpowiedz({"status": "blad", "komunikat": "nie masz takiego przedmiotu"})
            return
        postac.ekwipunek.remove(pid)
        wpis = self.rejestr.ustaw(pid, {"typ": "kraina", "id": self.id},
                                  self.zegar.tik(), self.id)
        self._publikuj(protokol.TEMAT_REJESTR, "rejestr.aktualizacja",
                       {"przedmiot": pid, "wpis": wpis})
        nazwa = (self.rejestr.info(pid) or {}).get("nazwa", pid)
        self._zdarzenie_krainy("upuszczono", f"{postac.nazwa} upuszcza {nazwa}.")
        odpowiedz({"status": "ok", "komunikat": f"Upuszczasz: {nazwa}."})

    def _cmd_spij(self, postac: Postac, odpowiedz) -> None:
        postac.spi_do = time.monotonic() + 60.0
        self._zdarzenie_krainy("sen", f"{postac.nazwa} rozkłada posłanie i zasypia.")
        self._zaplanuj(60.0, lambda: self._obudz(postac.id))
        odpowiedz({"status": "ok", "komunikat": "Zasypiasz na minutę. Po przebudzeniu odzyskasz 10 HP."})

    def _obudz(self, id_postaci: str) -> None:
        postac = self.stan.postacie.get(id_postaci)
        if not postac:
            return
        postac.spi_do = 0.0
        przed = postac.hp
        postac.hp = min(postac.hp_maks, postac.hp + 10)
        self._zdarzenie_krainy("leczenie",
                               f"{postac.nazwa} budzi się wypoczęty (+{postac.hp - przed} HP).")

    def _upusc_ekwipunek(self, postac: Postac) -> list[str]:
        """Przedmioty zmarłego/wylogowanego wracają na ziemię tej krainy.

        Modyfikacja bez RA jest tu bezpieczna: wpisy 'u gracza X' zmienia
        wyłącznie serwer-gospodarz gracza X, więc nie ma współbieżnego
        pisarza; replikację zapewnia rozgłoszenie nowszej wersji wpisu.
        """
        upuszczone = []
        for pid in postac.ekwipunek:
            wpis = self.rejestr.ustaw(
                pid, {"typ": "kraina", "id": self.id}, self.zegar.tik(), self.id
            )
            self._publikuj(
                protokol.TEMAT_REJESTR,
                "rejestr.aktualizacja",
                {"przedmiot": pid, "wpis": wpis},
            )
            upuszczone.append((self.rejestr.info(pid) or {}).get("nazwa", pid))
        postac.ekwipunek.clear()
        return upuszczone


    def _cmd_mapa(self, postac: Postac, odpowiedz) -> None:
        odpowiedz(
            {
                "status": "ok",
                "aktualna": self.id,
                "krainy": [
                    {"id": kid, "nazwa": k["nazwa"], "sasiedzi": list(k["sasiedzi"])}
                    for kid, k in sorted(
                        self.wszystkie.items(), key=lambda para: para[1]["numer"]
                    )
                ],
                "gracze": self._aktywni_gracze_globalnie(),
                "ty": postac.nazwa,
            }
        )

    def _cmd_powiedz(self, postac: Postac, dane: dict, odpowiedz) -> None:
        tekst = dane["tekst"].strip()
        if not tekst:
            odpowiedz(
                {"status": "blad", "komunikat": "nie możesz wysłać pustej wiadomości"}
            )
            return
        self._zdarzenie_krainy(
            "czat",
            f"💬 {postac.nazwa}: {tekst}",
            {"gracz": postac.nazwa, "tekst": tekst},
        )
        odpowiedz({"status": "ok", "komunikat": "wysłano wiadomość do krainy"})


    def _cmd_atakuj(self, postac: Postac, dane: dict, odpowiedz) -> None:
        cel_gracz = self.stan.znajdz_postac(dane["cel"])
        if cel_gracz is not None and cel_gracz.w_podrozy:
            cel_gracz = None  # postać w podróży (2PC) jest nieosiągalna
        if cel_gracz is not None and cel_gracz.id == postac.id:
            odpowiedz(
                {"status": "blad", "komunikat": "nie możesz atakować samego siebie"}
            )
            return
        potwor = None if cel_gracz else self.stan.znajdz_potwora(dane["cel"])
        if potwor and potwor.final_boss:
            wymagany = potwor.wymagany_przedmiot
            if wymagany and wymagany not in postac.ekwipunek:
                nazwa = (self.rejestr.info(wymagany) or {}).get("nazwa", wymagany)
                odpowiedz({"status": "blad",
                           "komunikat": f"Droga do finałowego bossa jest zamknięta. "
                                        f"Potrzebujesz: {nazwa}."})
                return
        if cel_gracz is None and potwor is None:
            odpowiedz(
                {"status": "blad", "komunikat": f"nie ma tu celu '{dane['cel']}'"}
            )
            return

        if cel_gracz is not None:
            cel_id, cel_nazwa, cel_typ = cel_gracz.id, cel_gracz.nazwa, "gracz"
            cel_hp = cel_gracz.hp
            cel_atak = self._atak_efektywny(cel_gracz)
            cel_obrona = self._obrona_efektywna(cel_gracz)
        else:
            cel_id, cel_nazwa, cel_typ = potwor.id, potwor.nazwa, "potwor"
            cel_hp, cel_atak, cel_obrona = potwor.hp, potwor.atak, 0

        # Jedyny autorytet: serwer-gospodarz. Ziarno zawiera znacznik
        # Lamporta, więc wynik jest deterministyczny i jednokrotny;
        # wszyscy obserwatorzy dostają identyczny komunikat.
        znacznik = self.zegar.tik()
        ziarno = f"walka:{self.id}:{znacznik}:{postac.id}:{cel_id}"
        atak_atakujacego = self._atak_efektywny(postac)
        obrona_atakujacego = self._obrona_efektywna(postac)
        wynik = modul_walki.rozstrzygnij(
            ziarno,
            atak_atakujacego,
            cel_atak,
            postac.hp,
            cel_hp,
            obrona_atakujacego=obrona_atakujacego,
            obrona_celu=cel_obrona,
        )

        postac.hp = wynik["hp_atakujacego_po"]
        if cel_gracz is not None:
            cel_gracz.hp = wynik["hp_celu_po"]
        else:
            potwor.hp = wynik["hp_celu_po"]

        czesci = []
        if wynik["trafienie"]:
            czesci.append(
                f"{postac.nazwa} trafia {cel_nazwa} za {wynik['obrazenia']} obrażeń"
            )
        else:
            czesci.append(f"{postac.nazwa} chybia, atakując {cel_nazwa}")
        if wynik["kontra"]:
            czesci.append(f"{cel_nazwa} kontratakuje za {wynik['kontra']}")
        opis_walki = "; ".join(czesci) + "."

        szczegoly = {
            **wynik,
            "znacznik_walki": znacznik,
            "atakujacy": postac.nazwa,
            "cel": cel_nazwa,
            "cel_typ": cel_typ,
            "atak_atakujacego": atak_atakujacego,
            "obrona_atakujacego": obrona_atakujacego,
            "atak_celu": cel_atak,
            "obrona_celu": cel_obrona,
        }
        self._zdarzenie_krainy("walka", opis_walki, szczegoly)

        # zgony rozstrzyga ten sam, jedyny autorytet
        if wynik["hp_celu_po"] == 0:
            if cel_gracz is not None:
                upuszczone = self._upusc_ekwipunek(cel_gracz)
                cel_gracz.hp = cel_gracz.hp_maks
                tekst = (
                    f"{cel_nazwa} pada z ręki {postac.nazwa} i odradza "
                    f"się przy życiu {cel_gracz.hp_maks} HP."
                )
                if upuszczone:
                    tekst += " Na ziemię upada: " + ", ".join(upuszczone) + "."
                self._zdarzenie_krainy("smierc", tekst)
            else:
                id_potwora = potwor.id
                final = potwor.final_boss
                for pid, straznik_id in list(self.straznicy_przedmiotow.items()):
                    if straznik_id == id_potwora:
                        self.odblokowane_przedmioty.add(pid)
                del self.stan.potwory[id_potwora]
                loot = self._wypusc_loot(potwor, postac)
                if not final:
                    self._zaplanuj(45.0, lambda: self._odrodz_potwora(id_potwora))
                tekst = f"{postac.nazwa} pokonuje potwora {cel_nazwa}!"
                if loot:
                    tekst += " Łup: " + ", ".join(loot) + "."
                self._zdarzenie_krainy("smierc", tekst)
                if final:
                    self._oglos_zwyciestwo(postac.nazwa, cel_nazwa)
        if wynik["hp_atakujacego_po"] == 0:
            upuszczone = self._upusc_ekwipunek(postac)
            postac.hp = postac.hp_maks
            tekst = (
                f"{postac.nazwa} ginie od kontrataku {cel_nazwa} "
                f"i odradza się przy {postac.hp_maks} HP."
            )
            if upuszczone:
                tekst += " Na ziemię upada: " + ", ".join(upuszczone) + "."
            self._zdarzenie_krainy("smierc", tekst)

        odpowiedz(
            {
                "status": "ok",
                "komunikat": opis_walki,
                "walka": szczegoly,
                "twoje_hp": postac.hp,
            }
        )

    def _wypusc_loot(self, potwor: Potwor, zabojca: Postac) -> list[str]:
        nazwy = []
        for pid in potwor.loot:
            info = self.rejestr.info(pid)
            if not info:
                continue
            polozenie = self.rejestr.gdzie(pid)
            if polozenie and polozenie[0] == "gracz":
                continue
            wpis = self.rejestr.ustaw(pid, {"typ": "kraina", "id": self.id},
                                      self.zegar.tik(), self.id)
            self._publikuj(protokol.TEMAT_REJESTR, "rejestr.aktualizacja",
                           {"przedmiot": pid, "wpis": wpis})
            nazwy.append(info["nazwa"])
        return nazwy

    def _oglos_zwyciestwo(self, gracz: str, boss: str) -> None:
        if self.zwyciezca:
            return
        self.zwyciezca = gracz
        self._publikuj(
            protokol.TEMAT_WYDARZENIE,
            "wydarzenie.globalne",
            {
                "id_wydarzenia": uuid.uuid4().hex,
                "tresc": f"{gracz} pokonał {boss} i wygrał grę!",
                "efekt": {"typ": "zwyciestwo", "gracz": gracz, "boss": boss},
            },
        )

    def _odrodz_potwora(self, id_potwora: str) -> None:
        self.stan.odrodz_potwora(id_potwora)
        potwor = self.stan.potwory.get(id_potwora)
        if potwor:
            self._zdarzenie_krainy(
                "wejscie", f"{potwor.nazwa} znów czai się w krainie…"
            )


    def _cmd_wyloguj(self, postac: Postac, dane: dict, odpowiedz) -> None:
        upuszczone = self._upusc_ekwipunek(postac)
        self.stan.postacie.pop(postac.id, None)
        self.sesje.usun_postac(postac.id)
        self._oglos_obecnosc_gracza(postac.nazwa, self.id, "nieaktywny")
        tekst = f"{postac.nazwa} opuszcza świat gry."
        if upuszczone:
            tekst += " Na ziemię upada: " + ", ".join(upuszczone) + "."
        self._zdarzenie_krainy("wyjscie", tekst)
        odpowiedz({"status": "ok", "komunikat": "Żegnaj, wędrowcze!"})

    # obowiązki lidera i reakcja na awarie

    def _pierwsza_elekcja(self) -> None:
        if self.elekcja.lider is None:
            self.elekcja.rozpocznij()

    def _tyk_regeneracji(self) -> None:
        try:
            for postac in self.stan.postacie.values():
                if postac.hp < postac.hp_maks:
                    postac.hp = min(postac.hp_maks, postac.hp + 5)
        finally:
            self._zaplanuj(15.0, self._tyk_regeneracji)

    def _tyk_potworow(self) -> None:
        try:
            zwykle = [p for p in self.stan.potwory.values() if not p.boss]
            if len(zwykle) < 3:
                szablon = random.choice(WEDROWNE_POTWORY)
                pid = f"wedrowny-{uuid.uuid4().hex[:6]}"
                self.stan.potwory[pid] = Potwor(
                    id=pid,
                    nazwa=szablon["nazwa"],
                    hp=szablon["hp"],
                    hp_maks=szablon["hp"],
                    atak=szablon["atak"],
                )
                self._zdarzenie_krainy("wejscie",
                                       f"{szablon['nazwa']} pojawia się w krainie.")
        finally:
            self._zaplanuj(55.0, self._tyk_potworow)

    def _tyk_lidera(self) -> None:
        """Okresowe obowiązki koordynatora świata (zwycięzcy elekcji)."""
        try:
            if self.elekcja.czy_jestem_liderem():
                # anti-entropy: pełna migawka rejestru dla replik
                self._publikuj(
                    protokol.TEMAT_REJESTR,
                    "rejestr.migawka",
                    {"wpisy": self.rejestr.migawka()},
                )
                self._licznik_tykow += 1
                if self._licznik_tykow % 3 == 0:
                    indeks = (self._licznik_tykow // 3) % len(
                        TEKSTY_WYDARZEN_GLOBALNYCH
                    )
                    self._publikuj(
                        protokol.TEMAT_WYDARZENIE,
                        "wydarzenie.globalne",
                        {
                            "id_wydarzenia": uuid.uuid4().hex,
                            "tresc": TEKSTY_WYDARZEN_GLOBALNYCH[indeks],
                            "efekt": {"typ": "uzdrowienie", "ilosc": 5},
                        },
                    )
        finally:
            self._zaplanuj(10.0, self._tyk_lidera)

    def _zastosuj_wydarzenie_globalne(self, dane: dict) -> None:
        """Każdy serwer stosuje efekt wydarzenia do SWOICH graczy — wszyscy
        widzą to samo wydarzenie z tym samym identyfikatorem."""
        efekt = dane.get("efekt", {})
        if efekt.get("typ") == "zwyciestwo":
            self.zwyciezca = str(efekt.get("gracz", "nieznany"))
            return
        if efekt.get("typ") == "uzdrowienie":
            try:
                ilosc = max(0, min(50, int(efekt.get("ilosc", 0))))
            except (TypeError, ValueError):
                return
            for postac in self.stan.postacie.values():
                postac.hp = min(postac.hp_maks, postac.hp + ilosc)
            self.log.info(
                "WYDARZENIE GLOBALNE [%s]: %s (+%d HP dla %d graczy)",
                dane["id_wydarzenia"][:8],
                dane["tresc"],
                ilosc,
                len(self.stan.postacie),
            )

    def _na_zmiane_lidera(self, id_lidera: str) -> None:
        if id_lidera == self.id:
            self.log.info(">>> Zostaję koordynatorem świata <<<")

    def _na_smierc_wezla(self, id_serwera: str) -> None:
        self.ra.na_smierc_wezla(id_serwera)
        self.migracje.na_smierc_wezla(id_serwera)
        self.elekcja.na_smierc_wezla(id_serwera)

    def _na_ozycie_wezla(self, id_serwera: str) -> None:
        if self.elekcja.czy_jestem_liderem():
            # przyspieszona synchronizacja wracającego węzła (anti-entropy)
            self._publikuj(
                protokol.TEMAT_REJESTR,
                "rejestr.migawka",
                {"wpisy": self.rejestr.migawka()},
            )


def main() -> None:
    id_krainy = os.environ.get("KRAINA")
    if not id_krainy:
        print(
            "Ustaw zmienną środowiskową KRAINA (bor|turnie|mokradla)", file=sys.stderr
        )
        raise SystemExit(2)
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{id_krainy}] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    serwer = SerwerKrainy(id_krainy, os.environ.get("RABBIT_HOST", "localhost"))
    try:
        serwer.start()
    except KeyboardInterrupt:
        logging.getLogger(id_krainy).info("Zamykanie serwera krainy…")


if __name__ == "__main__":
    main()
