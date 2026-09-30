# -*- coding: utf-8 -*-
"""Elekcja lidera algorytmem Bully."""

from __future__ import annotations

CZAS_NA_OK = 3.0            # ile czekamy na elekcja.ok od wyższych numerów
CZAS_NA_KOORDYNATORA = 6.0  # ile czekamy na ogłoszenie zwycięzcy


class ElekcjaBully:
    def __init__(self, moj_id: str, numery: dict, wyslij, oglos, zaplanuj,
                 czy_zywy, na_zmiane_lidera, log) -> None:
        self._moj_id = moj_id
        self._numery = dict(numery)          # id_serwera -> numer Bully
        self._moj_numer = self._numery[moj_id]
        self._wyslij = wyslij                # (id, typ, dane) -> None
        self._oglos = oglos                  # (dane) -> None: temat system.koordynator
        self._zaplanuj = zaplanuj
        self._czy_zywy = czy_zywy
        self._na_zmiane_lidera = na_zmiane_lidera  # (id_lidera) -> None
        self._log = log
        self.lider: str | None = None
        self._trwa = False
        self._dostalem_ok = False
        self._runda = 0                      # unieważnia stare timery

    def czy_jestem_liderem(self) -> bool:
        return self.lider == self._moj_id


    def rozpocznij(self) -> None:
        if self._trwa:
            return
        self._trwa = True
        self._dostalem_ok = False
        self._runda += 1
        runda = self._runda
        wyzsi = [id_s for id_s, numer in self._numery.items()
                 if numer > self._moj_numer and self._czy_zywy(id_s)]
        self._log.info("ELEKCJA: rozpoczynam wybory (mój numer %d), "
                       "wyżsi żywi kandydaci: %s", self._moj_numer, wyzsi or "brak")
        if not wyzsi:
            self._oglos_siebie()
            return
        for id_s in wyzsi:
            self._wyslij(id_s, "elekcja.wybory", {"numer": self._moj_numer})
        self._zaplanuj(CZAS_NA_OK, lambda: self._po_czekaniu_na_ok(runda))

    def _po_czekaniu_na_ok(self, runda: int) -> None:
        if runda != self._runda or not self._trwa:
            return
        if not self._dostalem_ok:
            self._oglos_siebie()   # nikt wyższy nie żyje — wygrywam
        else:
            self._zaplanuj(CZAS_NA_KOORDYNATORA,
                           lambda: self._po_czekaniu_na_koordynatora(runda))

    def _po_czekaniu_na_koordynatora(self, runda: int) -> None:
        if runda != self._runda or not self._trwa:
            return
        # zwycięzca miał się ogłosić i tego nie zrobił (padł?) — ponawiamy
        self._log.warning("ELEKCJA: koordynator się nie ogłosił — ponawiam wybory")
        self._trwa = False
        self.rozpocznij()

    def _oglos_siebie(self) -> None:
        self._trwa = False
        self._log.info("ELEKCJA: ogłaszam się koordynatorem świata (numer %d)",
                       self._moj_numer)
        # ogłoszenie wraca do nas tematem system.koordynator i tam ustawiamy lidera
        self._oglos({"numer": self._moj_numer})


    def na_wybory(self, od: str) -> None:
        """Węzeł o niższym numerze rozpoczął wybory — ucinamy go odpowiedzią OK."""
        self._wyslij(od, "elekcja.ok", {"numer": self._moj_numer})
        if self.czy_jestem_liderem():
            self._oglos({"numer": self._moj_numer})  # przypomnij, kto rządzi
        elif not self._trwa:
            self.rozpocznij()

    def na_ok(self, od: str) -> None:
        self._log.info("ELEKCJA: '%s' ma wyższy numer — czekam na wynik", od)
        self._dostalem_ok = True

    def na_koordynatora(self, od: str, numer: int) -> None:
        """Ogłoszenie zwycięzcy (temat system.koordynator)."""
        if numer < self._moj_numer:
            # uzurpacja: my mamy wyższy numer, więc to my powinniśmy rządzić
            self._log.info("ELEKCJA: '%s' (numer %d) ogłosił się koordynatorem, "
                           "ale mój numer %d jest wyższy — przejmuję",
                           od, numer, self._moj_numer)
            self._trwa = False
            self.rozpocznij()
            return
        poprzedni = self.lider
        self.lider = od
        self._trwa = False
        self._runda += 1   # unieważnij oczekujące timery elekcji
        if poprzedni != od:
            self._log.info("ELEKCJA: koordynatorem świata jest '%s' (numer %d)",
                           od, numer)
            self._na_zmiane_lidera(od)

    def na_smierc_wezla(self, id_serwera: str) -> None:
        if id_serwera == self.lider:
            self._log.warning("ELEKCJA: koordynator '%s' padł — nowe wybory",
                              id_serwera)
            self.lider = None
            self._trwa = False
            self.rozpocznij()
