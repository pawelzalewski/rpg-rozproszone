# -*- coding: utf-8 -*-
"""Replikowany rejestr przedmiotów."""

from __future__ import annotations

import copy


class RejestrPrzedmiotow:
    def __init__(self, konfiguracje) -> None:
        self._wpisy: dict[str, dict] = {}
        for konfig in konfiguracje:
            for przedmiot in konfig.get("przedmioty", []):
                self._wpisy[przedmiot["id"]] = {
                    "nazwa": przedmiot["nazwa"],
                    "opis": przedmiot.get("opis", ""),
                    "typ": przedmiot.get("typ", "artefakt"),
                    "podtyp": przedmiot.get("podtyp", "inne"),
                    "rece": int(przedmiot.get("rece", 0)),
                    "bonus_ataku": int(przedmiot.get("bonus_ataku", 0)),
                    "bonus_obrony": int(przedmiot.get("bonus_obrony", 0)),
                    "leczenie": int(przedmiot.get("leczenie", 0)),
                    "zuzywalny": bool(przedmiot.get("zuzywalny", False)),
                    "strzezony": bool(przedmiot.get("strzezony", True)),
                    "polozenie": (
                        {"typ": "ukryty", "id": konfig["id"]}
                        if przedmiot.get("ukryty", False)
                        else {"typ": "kraina", "id": konfig["id"]}
                    ),
                    "wersja": [0, ""],
                }


    def info(self, id_przedmiotu: str) -> dict | None:
        wpis = self._wpisy.get(id_przedmiotu)
        return copy.deepcopy(wpis) if wpis else None

    def gdzie(self, id_przedmiotu: str):
        wpis = self._wpisy.get(id_przedmiotu)
        if not wpis:
            return None
        return (wpis["polozenie"]["typ"], wpis["polozenie"]["id"])

    def w_krainie(self, id_krainy: str) -> list[str]:
        """Identyfikatory przedmiotów leżących na ziemi w danej krainie."""
        return sorted(
            pid
            for pid, wpis in self._wpisy.items()
            if wpis["polozenie"] == {"typ": "kraina", "id": id_krainy}
        )

    def znajdz_w_krainie(self, id_krainy: str, szukany: str) -> str | None:
        """Dopasowanie po id albo nazwie (bez rozróżniania wielkości liter)."""
        szukany = szukany.strip().lower()
        for pid in self.w_krainie(id_krainy):
            if szukany in (pid, self._wpisy[pid]["nazwa"].lower()):
                return pid
        return None


    def ustaw(
        self, id_przedmiotu: str, polozenie: dict, lamport: int, id_serwera: str
    ) -> dict:
        """Ustawia nowe położenie przedmiotu i podbija wersję.

        Zwraca kopię wpisu do rozgłoszenia (``rejestr.aktualizacja``).
        """
        wpis = self._wpisy[id_przedmiotu]
        wpis["polozenie"] = dict(polozenie)
        wpis["wersja"] = [int(lamport), id_serwera]
        return copy.deepcopy(wpis)


    def zastosuj(self, id_przedmiotu: str, wpis_zdalny: dict) -> bool:
        """Scala zdalną aktualizację: przyjmujemy tylko nowszą wersję.

        Nieznane przedmioty są odrzucane (sanityzacja — nie da się
        "doreplikować" przedmiotu spoza konfiguracji świata). Z wpisu
        zdalnego przejmujemy wyłącznie położenie i wersję; metadane
        (nazwa, bonus) pochodzą zawsze z lokalnej konfiguracji.
        """
        lokalny = self._wpisy.get(id_przedmiotu)
        if lokalny is None:
            return False
        try:
            wersja_zdalna = (
                int(wpis_zdalny["wersja"][0]),
                str(wpis_zdalny["wersja"][1]),
            )
            polozenie = wpis_zdalny["polozenie"]
            typ = str(polozenie["typ"])
            cel = str(polozenie["id"])
        except (KeyError, IndexError, TypeError, ValueError):
            return False
        if typ not in ("kraina", "gracz", "zuzyty", "ukryty"):
            return False
        wersja_lokalna = (int(lokalny["wersja"][0]), str(lokalny["wersja"][1]))
        if wersja_zdalna <= wersja_lokalna:
            return False
        lokalny["polozenie"] = {"typ": typ, "id": cel[:64]}
        lokalny["wersja"] = [wersja_zdalna[0], wersja_zdalna[1]]
        return True

    def migawka(self) -> dict:
        """Pełny zrzut rejestru do okresowej synchronizacji anti-entropy."""
        return copy.deepcopy(self._wpisy)

    def scal_migawke(self, wpisy: dict) -> int:
        """Scala migawkę od lidera; zwraca liczbę zaktualizowanych wpisów."""
        ile = 0
        if not isinstance(wpisy, dict):
            return 0
        for pid, wpis in wpisy.items():
            if isinstance(wpis, dict) and self.zastosuj(str(pid), wpis):
                ile += 1
        return ile
