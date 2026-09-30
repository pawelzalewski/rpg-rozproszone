# -*- coding: utf-8 -*-
"""Lokalny stan krainy."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Postac:
    id: str
    nazwa: str
    hp: int = 100
    hp_maks: int = 100
    atak_bazowy: int = 10
    ekwipunek: list = field(default_factory=list)
    w_podrozy: bool = False
    spi_do: float = 0.0

    def do_slownika(self) -> dict:
        """Postać w formie przenośnej (ładunek fazy PREPARE w 2PC)."""
        return {
            "id": self.id, "nazwa": self.nazwa,
            "hp": self.hp, "hp_maks": self.hp_maks,
            "atak_bazowy": self.atak_bazowy,
            "ekwipunek": list(self.ekwipunek),
        }

    @staticmethod
    def ze_slownika(dane: dict) -> "Postac":
        return Postac(
            id=str(dane["id"])[:64],
            nazwa=str(dane["nazwa"])[:32],
            hp=int(dane["hp"]),
            hp_maks=int(dane["hp_maks"]),
            atak_bazowy=int(dane["atak_bazowy"]),
            ekwipunek=[str(p)[:64] for p in list(dane.get("ekwipunek", []))[:32]],
        )


@dataclass
class Potwor:
    id: str
    nazwa: str
    hp: int
    hp_maks: int
    atak: int
    boss: bool = False
    final_boss: bool = False
    wymagany_przedmiot: str | None = None
    loot: list = field(default_factory=list)


class StanKrainy:
    def __init__(self, konfig: dict) -> None:
        self.postacie: dict[str, Postac] = {}
        #: nazwy zarezerwowane przez fazę PREPARE trwających migracji 2PC —
        #: między głosem "tak" a decyzją nikt nie może zająć tej nazwy
        self.zarezerwowane_nazwy: set[str] = set()
        self._szablony_potworow = {p["id"]: p for p in konfig.get("potwory", [])}
        self.potwory: dict[str, Potwor] = {}
        for szablon in self._szablony_potworow.values():
            self.odrodz_potwora(szablon["id"])

    def znajdz_postac(self, nazwa: str) -> Postac | None:
        szukana = nazwa.strip().lower()
        for postac in self.postacie.values():
            if postac.nazwa.lower() == szukana:
                return postac
        return None

    def nazwa_zajeta(self, nazwa: str) -> bool:
        n = nazwa.strip().lower()
        return (self.znajdz_postac(nazwa) is not None
                or n in {z.lower() for z in self.zarezerwowane_nazwy})

    def znajdz_potwora(self, identyfikator: str) -> Potwor | None:
        szukany = identyfikator.strip().lower()
        for potwor in self.potwory.values():
            if szukany in (potwor.id, potwor.nazwa.lower()):
                return potwor
        return None

    def odrodz_potwora(self, id_potwora: str) -> None:
        szablon = self._szablony_potworow.get(id_potwora)
        if szablon and id_potwora not in self.potwory:
            self.potwory[id_potwora] = Potwor(
                id=szablon["id"],
                nazwa=szablon["nazwa"],
                hp=szablon["hp"],
                hp_maks=szablon["hp"],
                atak=szablon["atak"],
                boss=bool(szablon.get("boss", False)),
                final_boss=bool(szablon.get("final_boss", False)),
                wymagany_przedmiot=szablon.get("wymagany_przedmiot"),
                loot=list(szablon.get("loot", [])),
            )
