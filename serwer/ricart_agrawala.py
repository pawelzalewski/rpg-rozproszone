# -*- coding: utf-8 -*-
"""Ricart-Agrawala dla podnoszenia unikalnych przedmiotów."""

from __future__ import annotations

from collections import deque


class RicartAgrawala:
    def __init__(self, moj_id: str, inni: list, zegar, wyslij, czy_zywy, log) -> None:
        self._moj_id = moj_id
        self._inni = list(inni)
        self._zegar = zegar
        self._wyslij = wyslij        # (id_serwera, typ, dane) -> None
        self._czy_zywy = czy_zywy    # (id_serwera) -> bool
        self._log = log
        # stan per zasób:
        #   znacznik   — (lamport, id) naszego aktywnego żądania albo None
        #   czekam_na  — zbiór węzłów, od których czekamy na zgodę (None = nie ubiegamy się)
        #   w_sekcji   — czy właśnie wykonujemy sekcję krytyczną
        #   odroczone  — węzły, którym odpowiemy zgodą dopiero po wyjściu z sekcji
        #   sekcje     — kolejka lokalnych funkcji czekających na ten zasób
        self._stan: dict[str, dict] = {}

    def _zasob(self, zasob: str) -> dict:
        if zasob not in self._stan:
            self._stan[zasob] = {"znacznik": None, "czekam_na": None,
                                 "w_sekcji": False, "odroczone": [],
                                 "sekcje": deque()}
        return self._stan[zasob]


    def zajmij(self, zasob: str, sekcja_krytyczna) -> None:
        """Wykonaj ``sekcja_krytyczna()`` w rozproszonej sekcji krytycznej.

        Wywołanie jest asynchroniczne — funkcja zostanie uruchomiona, gdy
        zbierzemy zgody wszystkich żywych węzłów. Kolejne lokalne żądania
        tego samego zasobu ustawiają się w kolejce.
        """
        s = self._zasob(zasob)
        s["sekcje"].append(sekcja_krytyczna)
        if s["w_sekcji"] or s["czekam_na"] is not None:
            return  # już się ubiegamy — obsłużymy z kolejki przy zwolnieniu
        self._ubiegaj_sie(zasob)


    def _ubiegaj_sie(self, zasob: str) -> None:
        s = self._zasob(zasob)
        znacznik = (self._zegar.tik(), self._moj_id)
        s["znacznik"] = znacznik
        zywi = [inny for inny in self._inni if self._czy_zywy(inny)]
        s["czekam_na"] = set(zywi)
        self._log.info("RA: żądam sekcji '%s' ze znacznikiem %s, pytam %s",
                       zasob, znacznik, zywi or "(nikogo — jestem sam)")
        for inny in zywi:
            self._wyslij(inny, "ra.prosba",
                         {"zasob": zasob, "znacznik": list(znacznik)})
        if not s["czekam_na"]:
            self._wejdz(zasob)

    def na_prosbe(self, od: str, zasob: str, znacznik) -> None:
        """Odebrano ra.prosba innego serwera."""
        s = self._zasob(zasob)
        ich = (int(znacznik[0]), str(znacznik[1]))
        ubiegamy_sie = s["czekam_na"] is not None
        if s["w_sekcji"] or (ubiegamy_sie and s["znacznik"] < ich):
            # my mamy pierwszeństwo — zgodę odraczamy do wyjścia z sekcji
            s["odroczone"].append(od)
            self._log.info("RA: odraczam zgodę dla '%s' na '%s' "
                           "(nasz znacznik %s < %s)", od, zasob, s["znacznik"], ich)
        else:
            self._wyslij(od, "ra.zgoda", {"zasob": zasob})

    def na_zgode(self, od: str, zasob: str) -> None:
        """Odebrano ra.zgoda innego serwera."""
        s = self._zasob(zasob)
        if s["czekam_na"] is None or od not in s["czekam_na"]:
            return  # spóźniona / zduplikowana zgoda
        s["czekam_na"].discard(od)
        if not s["czekam_na"]:
            self._wejdz(zasob)

    def _wejdz(self, zasob: str) -> None:
        s = self._zasob(zasob)
        s["czekam_na"] = None
        s["w_sekcji"] = True
        self._log.info("RA: wchodzę do sekcji krytycznej '%s'", zasob)
        sekcja = s["sekcje"].popleft()
        try:
            sekcja()
        finally:
            self._zwolnij(zasob)

    def _zwolnij(self, zasob: str) -> None:
        s = self._zasob(zasob)
        s["w_sekcji"] = False
        s["znacznik"] = None
        for od in s["odroczone"]:
            self._wyslij(od, "ra.zgoda", {"zasob": zasob})
        if s["odroczone"]:
            self._log.info("RA: zwalniam '%s', wysyłam odroczone zgody: %s",
                           zasob, s["odroczone"])
        s["odroczone"].clear()
        if s["sekcje"]:
            self._ubiegaj_sie(zasob)   # kolejne lokalne żądanie z kolejki
        else:
            del self._stan[zasob]


    def na_smierc_wezla(self, id_serwera: str) -> None:
        """Martwy węzeł nie odpowie — przestajemy czekać na jego zgody."""
        for zasob in list(self._stan):
            s = self._stan[zasob]
            if s["czekam_na"] is not None and id_serwera in s["czekam_na"]:
                self._log.warning("RA: węzeł '%s' padł — nie czekam na jego "
                                  "zgodę dla '%s'", id_serwera, zasob)
                s["czekam_na"].discard(id_serwera)
                if not s["czekam_na"]:
                    self._wejdz(zasob)
