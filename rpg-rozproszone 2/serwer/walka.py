# -*- coding: utf-8 -*-
"""Deterministyczne rozstrzyganie walki."""

from __future__ import annotations

import hashlib
import random


def rozstrzygnij(
    ziarno: str,
    atak_atakujacego: int,
    atak_celu: int,
    hp_atakujacego: int,
    hp_celu: int,
    obrona_atakujacego: int = 0,
    obrona_celu: int = 0,
) -> dict:
    """Czysta funkcja: to samo ziarno i parametry => ten sam wynik.

    Obrona zmniejsza otrzymywane obrażenia, ale nie wpływa na samo losowanie —
    obserwator nadal może odtworzyć wynik z opublikowanego ziarna i statystyk.
    """
    liczba = int(hashlib.sha256(ziarno.encode("utf-8")).hexdigest(), 16)
    rng = random.Random(liczba)

    trafienie = rng.random() < 0.9
    obrazenia_surowe = (
        rng.randint(max(1, atak_atakujacego - 2), atak_atakujacego + 4)
        if trafienie
        else 0
    )
    obrazenia = max(0, obrazenia_surowe - max(0, int(obrona_celu)))
    hp_celu_po = max(0, hp_celu - obrazenia)

    # kontra: cel oddaje cios, o ile przeżył
    kontra_surowa = 0
    kontra = 0
    if hp_celu_po > 0 and atak_celu > 0:
        kontra_surowa = rng.randint(max(0, atak_celu - 2), atak_celu + 2)
        kontra = max(0, kontra_surowa - max(0, int(obrona_atakujacego)))
    hp_atakujacego_po = max(0, hp_atakujacego - kontra)

    return {
        "ziarno": ziarno,
        "trafienie": trafienie,
        "obrazenia": obrazenia,
        "obrazenia_surowe": obrazenia_surowe,
        "kontra": kontra,
        "kontra_surowa": kontra_surowa,
        "hp_celu_po": hp_celu_po,
        "hp_atakujacego_po": hp_atakujacego_po,
    }
