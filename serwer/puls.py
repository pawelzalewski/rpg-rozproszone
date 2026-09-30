# -*- coding: utf-8 -*-
"""Heartbeat między serwerami."""

from __future__ import annotations

import time


class Puls:
    def __init__(self, moj_id: str, publikuj, zaplanuj, na_smierc, na_ozycie,
                 log, interwal: float = 2.0, limit: float = 7.0) -> None:
        self._moj_id = moj_id
        self._publikuj = publikuj      # () -> None: wyślij własny puls
        self._zaplanuj = zaplanuj      # (opóźnienie, funkcja) -> None
        self._na_smierc = na_smierc    # (id_serwera) -> None
        self._na_ozycie = na_ozycie    # (id_serwera) -> None
        self._log = log
        self._interwal = interwal
        self._limit = limit
        self._ostatni_puls: dict[str, float] = {}
        self._zywi: set[str] = set()

    def start(self) -> None:
        self._nadawaj()
        self._sprawdzaj()

    def _nadawaj(self) -> None:
        self._publikuj()
        self._zaplanuj(self._interwal, self._nadawaj)

    def _sprawdzaj(self) -> None:
        teraz = time.monotonic()
        for id_serwera, ostatnio in list(self._ostatni_puls.items()):
            if id_serwera in self._zywi and teraz - ostatnio > self._limit:
                self._zywi.discard(id_serwera)
                self._log.warning("PULS: serwer '%s' nie odpowiada od %.1fs — "
                                  "uznaję za martwy", id_serwera, teraz - ostatnio)
                self._na_smierc(id_serwera)
        self._zaplanuj(1.0, self._sprawdzaj)

    def na_puls(self, id_serwera: str) -> None:
        """Odebrano puls innego serwera."""
        self._ostatni_puls[id_serwera] = time.monotonic()
        if id_serwera not in self._zywi:
            self._zywi.add(id_serwera)
            self._log.info("PULS: serwer '%s' jest żywy", id_serwera)
            self._na_ozycie(id_serwera)

    def czy_zywy(self, id_serwera: str) -> bool:
        return id_serwera in self._zywi

    def zywi(self) -> set[str]:
        return set(self._zywi)
