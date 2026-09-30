# -*- coding: utf-8 -*-
"""Zegar logiczny Lamporta."""

from __future__ import annotations

import threading


class ZegarLamporta:
    def __init__(self) -> None:
        self._wartosc = 0
        self._blokada = threading.Lock()

    def tik(self) -> int:
        """Zdarzenie lokalne / wysłanie wiadomości: L = L + 1."""
        with self._blokada:
            self._wartosc += 1
            return self._wartosc

    def scal(self, otrzymany: int) -> int:
        """Odbiór wiadomości ze znacznikiem: L = max(L, T) + 1."""
        with self._blokada:
            self._wartosc = max(self._wartosc, int(otrzymany)) + 1
            return self._wartosc

    @property
    def wartosc(self) -> int:
        with self._blokada:
            return self._wartosc
