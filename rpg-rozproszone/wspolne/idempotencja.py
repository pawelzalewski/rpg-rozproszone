# -*- coding: utf-8 -*-
"""Pamięć ostatnich wiadomości do obsługi duplikatów."""

from __future__ import annotations

from collections import OrderedDict

#: wartownik "identyfikatora nigdy nie widziano" (odróżnialny od None,
#: bo None oznacza "widziano, odpowiedź jeszcze nie powstała")
BRAK = object()


class PamiecIdempotencji:
    def __init__(self, pojemnosc: int = 5000) -> None:
        self._pojemnosc = pojemnosc
        self._mapa: OrderedDict[str, object] = OrderedDict()

    def widziano(self, id_wiadomosci: str) -> bool:
        return id_wiadomosci in self._mapa

    def zapamietaj(self, id_wiadomosci: str, odpowiedz=None) -> None:
        self._mapa[id_wiadomosci] = odpowiedz
        self._mapa.move_to_end(id_wiadomosci)
        while len(self._mapa) > self._pojemnosc:
            self._mapa.popitem(last=False)

    def odpowiedz_dla(self, id_wiadomosci: str):
        """Zwraca zapamiętaną odpowiedź, None (w trakcie) albo BRAK (nowa)."""
        return self._mapa.get(id_wiadomosci, BRAK)
