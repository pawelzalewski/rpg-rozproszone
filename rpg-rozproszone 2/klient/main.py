# -*- coding: utf-8 -*-
"""Terminalowy klient gry."""

from __future__ import annotations

import argparse
import queue
import sys
import termios
import threading
import tty

from rich.console import Console

# starsze konsole Windows (cp1250) nie znają emoji — wymuszamy UTF-8
for _strumien in (sys.stdout, sys.stderr):
    if hasattr(_strumien, "reconfigure"):
        _strumien.reconfigure(encoding="utf-8", errors="replace")
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from klient.api import KlientGry

konsola = Console(highlight=False)

STYLE_ZDARZEN = {
    "wejscie": "green",
    "wyjscie": "yellow",
    "podniesiono": "cyan",
    "upuszczono": "yellow",
    "sen": "blue",
    "leczenie": "green",
    "czat": "bright_blue",
    "walka": "red",
    "smierc": "bold red",
}

# Sterowanie promptem i kursorem. Nie używamy tu Console.input, bo przy dwóch
# wątkach potrafiło ono wypisywać prompt dopiero przy następnym outputcie.
BLOKADA_TERMINALA = threading.RLock()
AKTYWNY_KLIENT: KlientGry | None = None
PROMPT_AKTYWNY = False
AKTUALNY_BUFOR = ""
ORYGINALNE_USTAWIENIA_TERMINALA = None
ANSI_MRUGAJACY_BLOK = "\033[1 q"
ANSI_KURSOR_DOMYSLNY = "\033[0 q"
ANSI_CZYSZCZENIE_LINII = "\r\033[K"

POMOC = """\
[bold cyan]ls[/bold cyan] / [bold]patrz[/bold]             — rozejrzyj się po krainie
[bold cyan]cd <kraina>[/bold cyan] / [bold]idz[/bold]      — przejdź do sąsiedniej krainy (migracja 2PC)
[bold cyan]yy <przedmiot>[/bold cyan] / [bold]podnies[/bold] — podnieś przedmiot (wzajemne wykluczanie R-A)
[bold cyan]u <mikstura>[/bold cyan] / [bold]uzyj[/bold]     — wypij miksturę leczenia z ekwipunku
[bold cyan]drop <przedmiot>[/bold cyan]           — zdejmij/upść przedmiot
[bold cyan]sleep[/bold cyan] / [bold]zz[/bold]              — śpij minutę i odzyskaj 10 HP
[bold cyan]x <cel>[/bold cyan] / [bold]atakuj[/bold]       — zaatakuj gracza albo potwora
[bold cyan]m[/bold cyan] / [bold]mapa[/bold]               — pokaż mapę krain i położenie graczy
[bold cyan]say <tekst>[/bold cyan] / [bold]: <tekst>[/bold] — wiadomość widoczna w aktualnej krainie
[bold cyan]pwd[/bold cyan]                       — pokaż aktualną krainę
[bold cyan]h[/bold cyan] / [bold]?[/bold] / [bold]pomoc[/bold]        — ta ściąga
[bold cyan]:q[/bold cyan] / [bold]q[/bold] / [bold]wyjdz[/bold]       — wyloguj się i zakończ\
"""

SZYBKI_START = """\
[bold]Jak wpisywać cele?[/bold]
W tabeli przy potworach i przedmiotach widzisz identyfikatory do komend.
Jeśli potwór nazywa się [bold]Lodowy Golem[/bold], atakujesz go jako:
[cyan]x lodowy-golem[/cyan]

Przykłady:
[cyan]ls[/cyan]                  — pokaż lokację
[cyan]yy topor-lodu[/cyan]       — podnieś Topór Lodu
[cyan]u mikstura-leczenia[/cyan] — wypij miksturę z ekwipunku
[cyan]x lodowy-golem[/cyan]      — zaatakuj Lodowego Golema
[cyan]drop tarcza-debowa[/cyan]  — upuść tarczę i zwolnij rękę
[cyan]sleep[/cyan]               — odpocznij minutę
[cyan]cd mokradla[/cyan]         — przejdź do Mokradeł
[cyan]x Bolek[/cyan]             — zaatakuj gracza Bolek, jeśli stoi w tej samej krainie
[cyan]say Cześć drużyno![/cyan]  — napisz do graczy w tej krainie

Ekwipunek ma limit [bold]2 rąk[/bold]: broń dwuręczna zajmuje obie, a jednoręczna
może iść z drugą jednoręczną bronią albo tarczą. Artefakty i mikstury mają [bold]0R[/bold].
Mikstury najpierw podnieś przez [cyan]yy <id>[/cyan], potem wypij przez [cyan]u <id>[/cyan].\
"""


def prompt_gry(klient: KlientGry) -> str:
    nazwa = klient.nazwa or "gracz"
    kraina = klient.kraina or "?"
    return f"\033[1;32m{nazwa}\033[0m@\033[1;36m{kraina}\033[0m$ "


def ustaw_mrugajacy_kursor() -> None:
    """Prosi terminal o klasyczny mrugający blokowy kursor (xterm/Unix-like)."""
    sys.stdout.write(ANSI_MRUGAJACY_BLOK)
    sys.stdout.flush()


def przywroc_kursor() -> None:
    sys.stdout.write(ANSI_KURSOR_DOMYSLNY)
    sys.stdout.flush()


def odswiez_prompt() -> None:
    if PROMPT_AKTYWNY and AKTYWNY_KLIENT is not None:
        sys.stdout.write(
            ANSI_CZYSZCZENIE_LINII + prompt_gry(AKTYWNY_KLIENT) + AKTUALNY_BUFOR
        )
        sys.stdout.flush()


def drukuj_zdarzenie_bez_klejenia(linia: Text) -> None:
    """Drukuje asynchroniczne zdarzenie nad promptem i rysuje prompt ponownie."""
    with BLOKADA_TERMINALA:
        if PROMPT_AKTYWNY:
            sys.stdout.write(ANSI_CZYSZCZENIE_LINII)
            sys.stdout.flush()
        konsola.print(linia)
        odswiez_prompt()


def pokaz_zdarzenie(wiadomosc: dict) -> None:
    """Zdarzenie z wymiany tematycznej — kolor wg rodzaju + znacznik Lamporta."""
    dane = wiadomosc.get("dane", {})
    if wiadomosc.get("typ") == "wydarzenie.globalne" and dane.get("efekt", {}).get("typ") == "zwyciestwo":
        with BLOKADA_TERMINALA:
            if PROMPT_AKTYWNY:
                sys.stdout.write(ANSI_CZYSZCZENIE_LINII)
                sys.stdout.flush()
            zwyciezca = dane.get("efekt", {}).get("gracz", "nieznany")
            boss = dane.get("efekt", {}).get("boss", "boss")
            konsola.print(Panel(
                f"[bold green]{zwyciezca}[/bold green] pokonał [bold red]{boss}[/bold red]\n\nKoniec gry!",
                title="ZWYCIĘSTWO",
                border_style="green",
            ))
            odswiez_prompt()
        return
    linia = Text()
    linia.append(f"[L={wiadomosc.get('lamport', '?')}] ", style="dim")
    if wiadomosc.get("typ") == "wydarzenie.globalne":
        linia.append("🌒 " + str(dane.get("tresc", "")), style="magenta")
    else:
        rodzaj = str(dane.get("rodzaj", ""))
        styl = STYLE_ZDARZEN.get(rodzaj, "white")
        linia.append(str(dane.get("tresc", "")), style=styl)
    drukuj_zdarzenie_bez_klejenia(linia)


def pokaz_mape(widok: dict) -> None:
    aktualna = widok.get("aktualna")
    gracze_po_krainach: dict[str, list[str]] = {}
    for gracz in widok.get("gracze", []):
        gracze_po_krainach.setdefault(gracz["kraina"], []).append(gracz["nazwa"])

    tabela = Table(show_header=True, header_style="bold cyan")
    tabela.add_column("Kraina")
    tabela.add_column("Połączenia")
    tabela.add_column("Gracze")
    for kraina in widok.get("krainy", []):
        kid = kraina["id"]
        nazwa = (
            f"➡ {kraina['nazwa']} ({kid})"
            if kid == aktualna
            else f"{kraina['nazwa']} ({kid})"
        )
        gracze = ", ".join(sorted(gracze_po_krainach.get(kid, []))) or "—"
        tabela.add_row(nazwa, ", ".join(kraina.get("sasiedzi", [])), gracze)
    konsola.print(
        Panel(tabela, title="Mapa świata i położenie graczy", border_style="cyan")
    )


def pokaz_widok(widok: dict) -> None:
    """Renderuje odpowiedź komendy 'patrz'."""
    kraina = widok["kraina"]
    konsola.print(
        Panel(kraina["opis"], title=f"⚔  {kraina['nazwa']}", border_style="green")
    )

    sasiedzi = ", ".join(
        f"{s['nazwa']} ({s['id']})" + ("" if s["dostepna"] else " [red]— ODCIĘTA[/red]")
        for s in widok["sasiedzi"]
    )
    koordynator = widok.get("koordynator") or "(trwa elekcja)"
    konsola.print(f"[dim]Wyjścia:[/dim] {sasiedzi}")
    konsola.print(f"[dim]Koordynator świata:[/dim] {koordynator}")

    tabela = Table(show_header=True, header_style="bold")
    tabela.add_column("Kto/co")
    tabela.add_column("Szczegóły")
    twoja_nazwa = widok.get("ty", {}).get("nazwa")
    for gracz in widok["gracze"]:
        akcja = "to Ty" if gracz["nazwa"] == twoja_nazwa else f"x {gracz['nazwa']}"
        tabela.add_row(
            f"🧝 {gracz['nazwa']}",
            f"HP {gracz['hp']}/{gracz['hp_maks']}, "
            f"ATK {gracz.get('atak', '?')}, DEF {gracz.get('obrona', 0)} — {akcja}",
        )
    for potwor in widok["potwory"]:
        tabela.add_row(
            f"👹 {potwor['nazwa']}" + (" [BOSS]" if potwor.get("boss") else ""),
            f"HP {potwor['hp']}/{potwor['hp_maks']}, ATK {potwor['atak']} — "
            f"x {potwor['id']}",
        )
    for przedmiot in widok["przedmioty"]:
        tabela.add_row(
            f"🗡  {przedmiot['nazwa']}",
            f"{przedmiot.get('statystyki', '')}; {przedmiot.get('typ', 'przedmiot')} — "
            f"yy {przedmiot['id']}",
        )
    konsola.print(tabela)

    ty = widok["ty"]

    def opis_przedmiotu(p: dict) -> str:
        statystyki = p.get("statystyki") or f"+{p.get('bonus_ataku', 0)} ATK"
        akcja = f", użyj: u {p['id']}" if p.get("zuzywalny") else ""
        return f"{p['nazwa']} ({statystyki}{akcja})"

    ekwipunek = ", ".join(opis_przedmiotu(p) for p in ty["ekwipunek"]) or "(pusto)"
    sen = f", śpisz jeszcze {ty.get('spi')}s" if ty.get("spi", 0) else ""
    konsola.print(
        f"[bold]{ty['nazwa']}[/bold]: HP {ty['hp']}/{ty['hp_maks']}, "
        f"ATK {ty['atak']}, DEF {ty.get('obrona', 0)}, "
        f"ręce {ty.get('rece_zajete', 0)}/{ty.get('rece_maks', 2)}{sen}, "
        f"ekwipunek: {ekwipunek}"
    )


def obsluz_linie(klient: KlientGry, linia: str) -> bool:
    """Wykonuje komendę gracza; zwraca False, gdy chcemy zakończyć grę."""
    czesci = linia.strip().split(maxsplit=1)
    if not czesci:
        return True
    komenda = czesci[0].lower()
    argument = czesci[1].strip() if len(czesci) > 1 else ""

    if komenda in ("pomoc", "?", "h", "help"):
        konsola.print(Panel(POMOC, title="Komendy", border_style="blue"))
    elif komenda in ("instrukcja", "intro"):
        konsola.print(Panel(SZYBKI_START, title="Szybki start", border_style="yellow"))
    elif komenda in ("patrz", "p", "ls", "l"):
        odpowiedz = klient.patrz()
        if odpowiedz.get("status") == "ok":
            pokaz_widok(odpowiedz)
        else:
            konsola.print(f"[red]{odpowiedz.get('komunikat')}[/red]")
    elif komenda in ("mapa", "map", "m"):
        odpowiedz = klient.mapa()
        if odpowiedz.get("status") == "ok":
            pokaz_mape(odpowiedz)
        else:
            konsola.print(f"[red]{odpowiedz.get('komunikat')}[/red]")
    elif komenda == "pwd":
        konsola.print(f"[cyan]{klient.kraina}[/cyan]")
    elif komenda in ("idz", "cd") and argument:
        konsola.print("[dim]Migracja 2PC w toku…[/dim]")
        odpowiedz = klient.idz(argument)
        kolor = "green" if odpowiedz.get("status") == "ok" else "red"
        konsola.print(f"[{kolor}]{odpowiedz.get('komunikat')}[/{kolor}]")
    elif komenda in ("podnies", "yy", "y", "get") and argument:
        odpowiedz = klient.podnies(argument)
        kolor = "cyan" if odpowiedz.get("status") == "ok" else "red"
        konsola.print(f"[{kolor}]{odpowiedz.get('komunikat')}[/{kolor}]")
    elif komenda in ("uzyj", "u", "drink", "quaff") and argument:
        odpowiedz = klient.uzyj(argument)
        kolor = "green" if odpowiedz.get("status") == "ok" else "yellow"
        konsola.print(f"[{kolor}]{odpowiedz.get('komunikat')}[/{kolor}]")
    elif komenda in ("upusc", "drop", "d") and argument:
        odpowiedz = klient.upusc(argument)
        kolor = "yellow" if odpowiedz.get("status") == "ok" else "red"
        konsola.print(f"[{kolor}]{odpowiedz.get('komunikat')}[/{kolor}]")
    elif komenda in ("spij", "sleep", "zz"):
        odpowiedz = klient.spij()
        kolor = "blue" if odpowiedz.get("status") == "ok" else "red"
        konsola.print(f"[{kolor}]{odpowiedz.get('komunikat')}[/{kolor}]")
    elif komenda in ("powiedz", "say", "s", ":") and argument:
        odpowiedz = klient.powiedz(argument)
        if odpowiedz.get("status") != "ok":
            konsola.print(f"[yellow]{odpowiedz.get('komunikat')}[/yellow]")
    elif komenda.startswith(":") and len(komenda) > 1:
        tekst = linia.strip()[1:].strip()
        odpowiedz = klient.powiedz(tekst)
        if odpowiedz.get("status") != "ok":
            konsola.print(f"[yellow]{odpowiedz.get('komunikat')}[/yellow]")
    elif komenda in ("atakuj", "x") and argument:
        odpowiedz = klient.atakuj(argument)
        if odpowiedz.get("status") != "ok":
            konsola.print(f"[yellow]{odpowiedz.get('komunikat')}[/yellow]")
        # Sukcesu nie wypisujemy drugi raz: identyczny wynik walki przychodzi
        # jako zdarzenie z tematu krainy i jest już pokazany z [L=...].
    elif komenda in ("wyjdz", "koniec", "quit", "exit", "q", ":q"):
        klient.wyloguj()
        return False
    else:
        konsola.print("[yellow]Nie rozumiem — wpisz 'h' albo 'pomoc'.[/yellow]")
    return True


def main() -> None:
    global AKTYWNY_KLIENT, PROMPT_AKTYWNY

    parser = argparse.ArgumentParser(description="Klient gry RPG")
    parser.add_argument(
        "--host", default="localhost", help="adres RabbitMQ (domyślnie localhost)"
    )
    parser.add_argument(
        "--kraina",
        default="bor",
        choices=["bor", "turnie", "mokradla", "pustynia", "cytadela"],
    )
    parser.add_argument("--nazwa", default=None, help="imię postaci")
    argumenty = parser.parse_args()

    konsola.print(
        Panel(
            "[bold]Kroniki Pięciu Krain[/bold]\n"
            "sieciowa gra RPG na systemy rozproszone\n"
            "[dim]Dawid Surmacz, Kamil Synowiec, Paweł Zalewski[/dim]",
            border_style="magenta",
        )
    )

    nazwa = argumenty.nazwa
    while not nazwa:
        nazwa = konsola.input("[bold]Jak zwą Cię w tych stronach?[/bold] ").strip()

    klient = KlientGry(host=argumenty.host, na_zdarzenie=pokaz_zdarzenie)
    AKTYWNY_KLIENT = klient
    try:
        klient.polacz()
    except Exception as blad:
        konsola.print(
            f"[red]Nie mogę połączyć się z RabbitMQ ({argumenty.host}): "
            f"{blad}[/red]\nCzy serwery działają? (docker compose up)"
        )
        raise SystemExit(1)

    odpowiedz = klient.zaloguj(nazwa, argumenty.kraina)
    if odpowiedz.get("status") != "ok":
        konsola.print(f"[red]Logowanie nieudane: {odpowiedz.get('komunikat')}[/red]")
        raise SystemExit(1)
    token_skrot = (klient.token or "????????")[:8]
    konsola.print(
        f"[green]{odpowiedz['komunikat']}[/green] "
        f"[dim](token sesji: {token_skrot}…)[/dim]"
    )
    konsola.print(Panel(SZYBKI_START, title="Szybki start", border_style="yellow"))
    konsola.print(Panel(POMOC, title="Komendy", border_style="blue"))
    mapa_startowa = klient.mapa()
    if mapa_startowa.get("status") == "ok":
        pokaz_mape(mapa_startowa)

    ustaw_mrugajacy_kursor()

    # wątek klawiatury -> kolejka; wątek główny: pika + wykonywanie komend.
    # Wątek inputu rysuje prompt ręcznie ANSI i czeka z kolejnym promptem,
    # aż główny wątek skończy obsługiwać poprzednią komendę.
    linie: queue.Queue[str | None] = queue.Queue()
    gotowe_na_prompt = threading.Event()
    gotowe_na_prompt.set()

    def czytaj_klawiature() -> None:
        global PROMPT_AKTYWNY, AKTUALNY_BUFOR, ORYGINALNE_USTAWIENIA_TERMINALA
        stare_ustawienia = None
        if sys.stdin.isatty():
            stare_ustawienia = termios.tcgetattr(sys.stdin.fileno())
            ORYGINALNE_USTAWIENIA_TERMINALA = stare_ustawienia
            tty.setcbreak(sys.stdin.fileno())
        try:
            while True:
                gotowe_na_prompt.wait()
                gotowe_na_prompt.clear()
                with BLOKADA_TERMINALA:
                    AKTUALNY_BUFOR = ""
                    PROMPT_AKTYWNY = True
                    odswiez_prompt()
                while True:
                    znak = sys.stdin.read(1)
                    if znak == "":
                        linie.put(None)
                        return
                    if znak in ("\n", "\r"):
                        with BLOKADA_TERMINALA:
                            linia = AKTUALNY_BUFOR
                            AKTUALNY_BUFOR = ""
                            PROMPT_AKTYWNY = False
                            sys.stdout.write("\n")
                            sys.stdout.flush()
                        linie.put(linia)
                        break
                    if znak == "\x03":  # Ctrl+C
                        linie.put(None)
                        return
                    if znak == "\x04":  # Ctrl+D
                        linie.put(None)
                        return
                    if znak in ("\x7f", "\b"):
                        with BLOKADA_TERMINALA:
                            AKTUALNY_BUFOR = AKTUALNY_BUFOR[:-1]
                            odswiez_prompt()
                        continue
                    if znak == "\x1b":
                        # Ignorujemy sekwencje specjalne (strzałki itd.).
                        continue
                    if znak.isprintable():
                        with BLOKADA_TERMINALA:
                            AKTUALNY_BUFOR += znak
                            sys.stdout.write(znak)
                            sys.stdout.flush()
        except (EOFError, KeyboardInterrupt):
            with BLOKADA_TERMINALA:
                PROMPT_AKTYWNY = False
            linie.put(None)
        finally:
            if stare_ustawienia is not None:
                termios.tcsetattr(
                    sys.stdin.fileno(), termios.TCSADRAIN, stare_ustawienia
                )

    watek = threading.Thread(target=czytaj_klawiature, daemon=True)
    watek.start()

    try:
        gramy = True
        while gramy:
            klient.przetworz(0.15)
            try:
                linia = linie.get_nowait()
            except queue.Empty:
                continue
            if linia is None:
                klient.wyloguj()
                break
            gramy = obsluz_linie(klient, linia)
            if gramy:
                gotowe_na_prompt.set()
    except KeyboardInterrupt:
        klient.wyloguj()
    finally:
        with BLOKADA_TERMINALA:
            PROMPT_AKTYWNY = False
            sys.stdout.write(ANSI_CZYSZCZENIE_LINII)
            sys.stdout.flush()
        if ORYGINALNE_USTAWIENIA_TERMINALA is not None and sys.stdin.isatty():
            termios.tcsetattr(
                sys.stdin.fileno(), termios.TCSADRAIN, ORYGINALNE_USTAWIENIA_TERMINALA
            )
        przywroc_kursor()
        klient.zamknij()
        konsola.print("[magenta]Do zobaczenia w Pięciu Krainach![/magenta]")


if __name__ == "__main__":
    main()
