# -*- coding: utf-8 -*-
"""Tokeny sesji graczy."""

from __future__ import annotations

import secrets

from wspolne.walidacja import WZORZEC_TOKENU


class MenedzerSesji:
    def __init__(self) -> None:
        self._token_na_postac: dict[str, str] = {}

    def utworz(self, id_postaci: str) -> str:
        token = secrets.token_hex(16)
        self._token_na_postac[token] = id_postaci
        return token

    def przypisz(self, token: str, id_postaci: str) -> None:
        """Rejestruje istniejący token (postać przybyła w migracji 2PC)."""
        if isinstance(token, str) and WZORZEC_TOKENU.match(token):
            self._token_na_postac[token] = id_postaci

    def postac_dla(self, token) -> str | None:
        if not isinstance(token, str) or not WZORZEC_TOKENU.match(token):
            return None
        return self._token_na_postac.get(token)

    def usun_token(self, token: str) -> None:
        self._token_na_postac.pop(token, None)

    def usun_postac(self, id_postaci: str) -> None:
        for token, pid in list(self._token_na_postac.items()):
            if pid == id_postaci:
                del self._token_na_postac[token]
