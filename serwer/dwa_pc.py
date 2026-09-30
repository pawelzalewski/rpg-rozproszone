# -*- coding: utf-8 -*-
"""Dwufazowy commit przy przechodzeniu między krainami."""

from __future__ import annotations

import os
import time

from wspolne.idempotencja import PamiecIdempotencji

LIMIT_GLOSU = 6.0  # koordynator: czekanie na głos uczestnika
LIMIT_ACK = 5.0  # koordynator: czekanie na potwierdzenie COMMIT
LIMIT_DECYZJI = 25.0  # uczestnik: czekanie na decyzję po głosie "tak"
PROBY_ZATWIERDZENIA = 3


class Migracje2PC:
    def __init__(
        self,
        moj_id: str,
        wyslij,
        zaplanuj,
        log,
        katalog_wal,
        usun_postac,
        przyjmij_postac,
        czy_przyjme,
        zarezerwuj_nazwe,
        zwolnij_nazwe,
    ) -> None:
        self._moj_id = moj_id
        self._wyslij = wyslij  # (id_serwera, typ, dane) -> None
        self._zaplanuj = zaplanuj
        self._log = log
        # zaczepy do stanu gry (główny moduł serwera):
        self._usun_postac = usun_postac  # (postac, token, cel) -> None
        self._przyjmij_postac = przyjmij_postac  # (dane_postaci, token, od) -> None
        self._czy_przyjme = czy_przyjme  # (dane_postaci) -> (bool, powod)
        self._zarezerwuj_nazwe = zarezerwuj_nazwe
        self._zwolnij_nazwe = zwolnij_nazwe
        # rola koordynatora: tx -> opis transakcji
        self._koordynowane: dict[str, dict] = {}
        # rola uczestnika: tx -> ładunek fazy PREPARE
        self._przygotowane: dict[str, dict] = {}
        # idempotencja decyzji: tx -> "commit"/"abort" (ponowione zatwierdź
        # dostaje ponowne potwierdzenie zamiast drugiego wykonania)
        self._zakonczone = PamiecIdempotencji(1000)

        os.makedirs(katalog_wal, exist_ok=True)
        self._sciezka_wal = os.path.join(katalog_wal, f"dziennik_2pc_{moj_id}.log")

    def _wal(self, tekst: str) -> None:
        """Dziennik z wyprzedzeniem (write-ahead log) decyzji 2PC."""
        linia = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {tekst}"
        with open(self._sciezka_wal, "a", encoding="utf-8") as plik:
            plik.write(linia + "\n")
        self._log.info("2PC/WAL: %s", tekst)


    def rozpocznij(
        self, postac, token: str, kraina_cel: str, na_sukces, na_blad
    ) -> None:
        import uuid

        tx = uuid.uuid4().hex
        postac.w_podrozy = True  # blokada: od teraz postać nie działa w A
        self._koordynowane[tx] = {
            "postac": postac,
            "token": token,
            "cel": kraina_cel,
            "etap": "przygotowanie",
            "na_sukces": na_sukces,
            "na_blad": na_blad,
        }
        self._wal(f"START tx={tx} postac={postac.nazwa}({postac.id}) cel={kraina_cel}")
        self._wyslij(
            kraina_cel,
            "2pc.przygotuj",
            {"tx": tx, "postac": postac.do_slownika(), "token": token},
        )
        self._zaplanuj(LIMIT_GLOSU, lambda: self._limit_glosu(tx))

    def _limit_glosu(self, tx: str) -> None:
        t = self._koordynowane.get(tx)
        if t and t["etap"] == "przygotowanie":
            self._przerwij(tx, "kraina docelowa nie odpowiedziała w terminie")

    def na_glos(self, od: str, dane: dict) -> None:
        tx = dane["tx"]
        t = self._koordynowane.get(tx)
        if not t or t["etap"] != "przygotowanie" or od != t["cel"]:
            return
        if dane["glos"] == "tak":
            t["etap"] = "zatwierdzanie"
            self._wal(f"COMMIT tx={tx}")  # punkt bez odwrotu
            self._wyslij_zatwierdz(tx, proba=1)
        else:
            self._przerwij(tx, dane.get("powod", "kraina docelowa odmówiła"))

    def _wyslij_zatwierdz(self, tx: str, proba: int) -> None:
        t = self._koordynowane.get(tx)
        if not t or t["etap"] != "zatwierdzanie":
            return
        self._wyslij(t["cel"], "2pc.zatwierdz", {"tx": tx})
        if proba < PROBY_ZATWIERDZENIA:
            self._zaplanuj(LIMIT_ACK, lambda: self._wyslij_zatwierdz(tx, proba + 1))
        else:
            self._zaplanuj(LIMIT_ACK, lambda: self._poddaj_zatwierdzanie(tx))

    def _poddaj_zatwierdzanie(self, tx: str) -> None:
        t = self._koordynowane.get(tx)
        if t and t["etap"] == "zatwierdzanie":
            # decyzja COMMIT zapadła, ale uczestnik milczy (padł tuż po głosie).
            # Postać pozostaje zablokowana — bezpieczeństwo ponad żywotność:
            # nigdy nie dopuścimy do dwóch aktywnych kopii.
            self._log.error(
                "2PC: brak potwierdzenia COMMIT tx=%s — postać pozostaje zablokowana",
                tx,
            )
            t["na_blad"](
                "kraina docelowa padła w trakcie przenosin; "
                "postać zawieszona do wyjaśnienia"
            )

    def na_potwierdzenie(self, od: str, dane: dict) -> None:
        tx = dane["tx"]
        t = self._koordynowane.pop(tx, None)
        if not t:
            return
        self._wal(f"DONE tx={tx}")
        # dopiero teraz, po pewności że B ma postać, usuwamy ją u siebie
        self._usun_postac(t["postac"], t["token"], t["cel"])
        t["na_sukces"]()

    def _przerwij(self, tx: str, powod: str) -> None:
        t = self._koordynowane.pop(tx, None)
        if not t:
            return
        self._wal(f"ABORT tx={tx} ({powod})")
        t["postac"].w_podrozy = False  # postać zostaje bezpiecznie w A
        self._wyslij(t["cel"], "2pc.przerwij", {"tx": tx})
        t["na_blad"](powod)


    def na_przygotuj(self, od: str, dane: dict) -> None:
        tx = dane["tx"]
        if tx in self._przygotowane:
            self._wyslij(od, "2pc.glos", {"tx": tx, "glos": "tak"})  # duplikat
            return
        decyzja = self._zakonczone.odpowiedz_dla(tx)
        if decyzja == "commit":
            self._wyslij(od, "2pc.potwierdzenie", {"tx": tx})
            return
        if decyzja == "abort":
            return
        ok, powod = self._czy_przyjme(dane["postac"])
        if not ok:
            self._wal(f"VOTE-NO tx={tx} ({powod})")
            self._wyslij(od, "2pc.glos", {"tx": tx, "glos": "nie", "powod": powod})
            return
        nazwa = str(dane["postac"].get("nazwa", ""))[:32]
        self._zarezerwuj_nazwe(nazwa)
        self._przygotowane[tx] = {
            "postac": dane["postac"],
            "token": dane["token"],
            "od": od,
            "nazwa": nazwa,
        }
        self._wal(f"PREPARED tx={tx} postac={nazwa} od={od}")
        self._wyslij(od, "2pc.glos", {"tx": tx, "glos": "tak"})
        self._zaplanuj(LIMIT_DECYZJI, lambda: self._samoprzerwanie(tx))

    def na_zatwierdz(self, od: str, dane: dict) -> None:
        tx = dane["tx"]
        p = self._przygotowane.pop(tx, None)
        if p is None:
            if self._zakonczone.odpowiedz_dla(tx) == "commit":
                # ponowione zatwierdzenie — potwierdzamy idempotentnie
                self._wyslij(od, "2pc.potwierdzenie", {"tx": tx})
            return
        self._zwolnij_nazwe(p["nazwa"])
        self._przyjmij_postac(p["postac"], p["token"], p["od"])
        self._wal(f"COMMITTED tx={tx}")
        self._zakonczone.zapamietaj(tx, "commit")
        self._wyslij(od, "2pc.potwierdzenie", {"tx": tx})

    def na_przerwij(self, od: str, dane: dict) -> None:
        tx = dane["tx"]
        p = self._przygotowane.pop(tx, None)
        if p is not None:
            self._zwolnij_nazwe(p["nazwa"])
            self._wal(f"ABORTED tx={tx}")
            self._zakonczone.zapamietaj(tx, "abort")

    def _samoprzerwanie(self, tx: str) -> None:
        p = self._przygotowane.pop(tx, None)
        if p is not None:
            self._zwolnij_nazwe(p["nazwa"])
            self._wal(f"TIMEOUT-ABORT tx={tx} (koordynator milczy)")
            self._zakonczone.zapamietaj(tx, "abort")


    def na_wiadomosc(self, typ: str, od: str, dane: dict) -> None:
        """Dyspozytor wiadomości 2PC z kolejki RPC."""
        obsluga = {
            "2pc.przygotuj": self.na_przygotuj,
            "2pc.glos": self.na_glos,
            "2pc.zatwierdz": self.na_zatwierdz,
            "2pc.przerwij": self.na_przerwij,
            "2pc.potwierdzenie": self.na_potwierdzenie,
        }.get(typ)
        if obsluga:
            obsluga(od, dane)

    def na_smierc_wezla(self, id_serwera: str) -> None:
        """Kraina docelowa padła w fazie przygotowania — przerywamy od razu."""
        for tx, t in list(self._koordynowane.items()):
            if t["cel"] == id_serwera and t["etap"] == "przygotowanie":
                self._przerwij(tx, f"kraina '{id_serwera}' padła w trakcie migracji")
