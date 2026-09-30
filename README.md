# Kroniki Pięciu Krain

Prosta sieciowa gra RPG.

Autorzy: Dawid Surmacz, Kamil Synowiec, Paweł Zalewski.

## O projekcie

Świat gry składa się z pięciu krain. Każda kraina działa jako osobny serwer:

- `bor` — Bór Cieni,
- `turnie` — Zamarznięte Turnie,
- `mokradla` — Mokradła Zapomnienia,
- `pustynia` — Szkarłatna Pustynia,
- `cytadela` — Czarna Cytadela.

Serwery komunikują się przez RabbitMQ. Gracz korzysta z klienta terminalowego.

## Co jest zaimplementowane

- wielu graczy,
- pięć komunikujących się serwerów,
- przechodzenie między krainami,
- walka z potworami, bossami i innymi graczami,
- nagrody wypadające z potworów,
- finałowy boss i ekran zwycięstwa,
- przedmioty, mikstury, ekwipunek i limit dwóch rąk,
- sen, regeneracja HP i okresowe pojawianie się nowych potworów,
- lokalny czat w krainie,
- mapa pokazująca położenie graczy,
- RabbitMQ jako broker wiadomości,
- zegary Lamporta,
- Ricart-Agrawala przy podnoszeniu unikalnych przedmiotów,
- 2PC przy przenoszeniu postaci między krainami,
- elekcja lidera Bully,
- heartbeat między serwerami,
- HMAC dla wiadomości między serwerami,
- idempotencja komend.

## Uruchomienie od zera

Wymagania:

- Docker,
- Docker Compose,
- Python 3.11+.

Wejdź do katalogu projektu:

```bash
cd /Users/pawel/projekty/rpg-rozproszone
```

Postaw RabbitMQ i serwery krain:

```bash
docker compose up --build -d
```

Utwórz środowisko Pythona dla klienta:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Uruchom klienta:

```bash
python -m klient.main --nazwa imie
```

Drugi gracz w drugim terminalu:

```bash
cd /Users/pawel/projekty/rpg-rozproszone
source .venv/bin/activate
python -m klient.main --nazwa imie --kraina turnie
```

Panel RabbitMQ:

```text
http://localhost:15672
login: guest
hasło: guest
```

Logi serwera:

```bash
docker compose logs -f bor
```

Zatrzymanie gry:

```bash
docker compose down --remove-orphans
```

Pełny reset świata:

```bash
docker compose down --remove-orphans
docker compose up --build -d
```

## Komendy w grze

| Komenda | Działanie |
|---|---|
| `ls` | pokaż aktualną krainę |
| `cd <kraina>` | przejdź do krainy |
| `yy <przedmiot>` | podnieś przedmiot |
| `u <mikstura>` | użyj mikstury |
| `drop <przedmiot>` | upuść przedmiot i zwolnij rękę |
| `sleep` albo `zz` | śpij minutę i odzyskaj HP |
| `x <cel>` | zaatakuj cel |
| `m` | pokaż mapę i graczy |
| `say <tekst>` | napisz wiadomość w krainie |
| `: <tekst>` | krótsza forma czatu |
| `h` | pomoc |
| `q` albo `:q` | wyjście |

Przykład:

```text
ls
x wilk-boru
yy miecz-cieni
yy tarcza-debowa
drop tarcza-debowa
cd pustynia
m
say Jestem na pustyni
x piaskowy-smok
yy pieczec-smoka
cd cytadela
x krol-licz
:q
```

Cel gry: zdobyć `pieczec-smoka` po pokonaniu Piaskowego Smoka, wejść do Cytadeli
i jako pierwszy zabić `krol-licz`. Po śmierci finałowego bossa wszyscy gracze
widzą ekran zwycięstwa.

## Wiadomość do profesora