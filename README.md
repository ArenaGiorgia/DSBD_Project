
# Sistema Distribuito per il Monitoraggio Voli

> **Corso:** Distributed Systems and Big Data (2025-2026)  
> **Studenti:** Arena Giorgia, Tornabene Alessio  
> **Università degli Studi di Catania**

---

## Panoramica del Progetto

**Il seguente progetto** è un'architettura a microservizi progettata per il monitoraggio in tempo reale del traffico aereo, integrata con le API di OpenSky Network. Il sistema permette agli utenti di registrare interessi su specifici aeroporti e ricevere notifiche e-mail istantanee (Event-Driven) quando il numero di voli supera soglie personalizzate (`high_value` / `low_value`).

Il progetto non si limita alla funzionalità, ma implementa pattern avanzati di **Ingegneria dei Sistemi Distribuiti** per garantire resilienza, scalabilità e consistenza dei dati.

---

## Caratteristiche Architetturali (Key Features)

Il sistema è progettato utilizzando tecnologie eterogenee per risolvere problemi specifici:

* **Robustezza & Fault Tolerance:**
    * **Circuit Breaker Custom:** Protegge il sistema dai fallimenti dell'API OpenSky (Logica a stati: *Closed* -> *Open* dopo 4 errori -> *Half-Open* dopo 60s).
    * **Active Waiting:** Gestione intelligente dello startup dei container per risolvere le dipendenze temporali da Kafka.
    * **Graceful Degradation:** Generazione di **Dati Mock** in caso di indisponibilità della rete esterna per garantire la continuità del servizio.

* **Comunicazione Eterogenea:**
    * **Kafka (Pub/Sub):** Disaccoppiamento asincrono per l'ingestione dati ad alto throughput.
    * **gRPC (Protobuf):** Comunicazione sincrona interna ad alta efficienza tra *Data Collector* e *User Manager*.
    * **REST/HTTP:** Interfaccia pubblica universale gestita tramite **API Gateway (NGINX)**.

* **Integrità dei Dati:**
    * **Semantica At-Least-Once Stretto:** Configurazione Kafka Producer con `acks='all'` e `flush()` manuale prima del commit.
    * **Deduplica Applicativa:** Implementazione di *Idempotenza* nel Notifier tramite hash MD5 per simulare una semantica *Exactly-Once* ed evitare spam.
    * **Polyglot Persistence:** Uso ibrido di **PostgreSQL** (Dati Relazionali/Utenti - CP) e **MongoDB** (Dati Telemetria/Voli - AP).

---

##  Tech Stack

* **Linguaggio:** Python 3.9+ (Flask)
* **Containerizzazione:** Docker & Docker Compose
* **Broker:** Apache Kafka
* **Gateway:** NGINX (Reverse Proxy su porta 80)
* **Database:** PostgreSQL, MongoDB
* **Protocolli:** HTTP/1.1, gRPC, TCP (Kafka Protocol)

---

## Prerequisiti

* **Docker Desktop** installato e attivo.
* **Postman** (o cURL) per testare le API.

---

##  Istruzioni per l'Avvio (Quick Start)

### 1. Avvio dell'Infrastruttura
Dalla root del progetto, esegui:

```bash
docker-compose up --build -d

```
### 2. Settaggio file .env
All' interno del progetto settare un file .env con le seguenti variabili:

```bash
OPENSKY_CLIENT_ID= TUO_CLIENTID
OPENSKY_CLIENT_SECRET= TUO_SECRETCLIENT
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=users_db
GMAIL_USER= TUA_GMAIL
GMAIL_PASSWORD= TUA_PASSGMAIL
```
NB. 
**1. Attivare la Verifica in due passaggi (2FA)**
Per poter usare le "Password per le App", Google richiede che il tuo account sia protetto dalla doppia verifica.
Vai su Gestisci il tuo Account Google (clicca sulla tua foto in alto a destra su Gmail).
Vai nella scheda Sicurezza (a sinistra).
Cerca la sezione "Come accedi a Google".
Assicurati che "Verifica in due passaggi" sia su ON. (Se è OFF, attivala seguendo le istruzioni col cellulare).
**2. Generare la "Password per le App"**
Questa è la procedura che genera quel codice GMAIL_PASSWORD.
Sempre nella scheda Sicurezza, cerca la voce "Password per le app".
Nota: A volte Google nasconde questa voce. Se non la trovi, usa la barra di ricerca in alto "Cerca nell'account Google" e scrivi "Password per le app".
Ti chiederà di ri-inserire la tua password normale per sicurezza.
Nel menu a tendina "Seleziona app", scegli Posta.
Nel menu "Seleziona dispositivo", scegli Altro (nome personalizzato) e scrivi un nome a piacere, ad esempio DockerNotifier.
Clicca su Genera.
Google ti mostrerà un codice di 16 lettere dentro un riquadro giallo (es. turLV zdyW ...).

###  Nota Importante sul Bootstrap

Immediatamente dopo l'avvio, i servizi Python (`alert_system`, `notifier`) entreranno in uno stato di attesa.

> **Attendere circa 20-30 secondi:** Questo tempo è necessario affinché il cluster Kafka elegga il controller e inizializzi i topic. I log mostreranno il messaggio `In attesa di Kafka...` finché la connessione non sarà stabilita.

Per monitorare lo stato di avvio:

```bash
docker-compose logs -f

```

---

## Guida API 
Tutte le richieste devono essere inviate alla **Porta 80** (NGINX).

### 1. Registrazione Utente

Crea un nuovo utente su PostgreSQL.

* **Method:** `POST`
* **URL:** `http://localhost/users`
* **Headers:** `Request-ID: <uuid-univoco>` (Obbligatorio per la cache idempotente)
* **Body:**
```json
{
  "email": "mario.rossi@example.com",
  "password": "passwordSicura123",
  "nome": "Mario",
  "cognome": "Rossi"
}

```



### 2. Aggiunta Interesse (Monitoraggio)

Registra un aeroporto e le soglie di allarme. Innesca una verifica gRPC interna e l'acquisizione dati immediata.

* **Method:** `POST`
* **URL:** `http://localhost/interests`
* **Body:**
```json
{
  "email": "mario.rossi@example.com",
  "airport": "LIRF",
  "high_value": 100,
  "low_value": 20
}

```


> **Nota:** Se `high_value` <= `low_value`, l'API restituirà errore 400.



### 3. Consultazione Dati Volo

Restituisce l'ultimo dato acquisito (Reale o Mock).

* **Method:** `GET`
* **URL:** `http://localhost/flights/last?airport=LIRF`

---

## Scenari di Test

Per verificare il funzionamento completo della pipeline e la logica di alerting, consigliamo di seguire questi scenari:

### Scenario A: Nessun Allarme (Valore nel Range)

1. Imposta soglie ampie (es. `low: 0`, `high: 1000`) per un aeroporto attivo (es. LIRF).
2. **Risultato atteso:** Il dato viene salvato, ma **nessuna email** viene inviata.

### Scenario B: Allarme Soglia Alta (High Value)

1. Imposta una soglia `high_value` molto bassa (es. `10`) rispetto al traffico reale.
2. **Risultato atteso:**
* Log Alert System: `Allarme rilevato: Valore ALTO superato`.
* **Email ricevuta:** Oggetto "Allerta Voli: LIRF".



### Scenario C: Allarme Soglia Bassa (Low Value)

1. Imposta una soglia `low_value` molto alta (es. `500`).
2. **Risultato atteso:**
* Log Alert System: `Allarme rilevato: Valore BASSO superato`.
* **Email ricevuta:** Oggetto "Allerta Voli: LIRF".



### Scenario D: Fault Tolerance (Simulazione Guasti)

1. **Stop API Esterna:** Se OpenSky non risponde, il **Circuit Breaker** si apre. Le API `GET /flights` restituiranno dati Mock e il sistema continuerà a funzionare.
2. **Crash Notifier:** Se il container `notifier` viene arrestato durante l'elaborazione, al riavvio riprocesserà i messaggi grazie al commit manuale, ma la **Deduplica** eviterà l'invio di doppie email.

---

##  Struttura del Repository

* `/user_manager`: Servizio gestione utenti (Flask + Postgres + gRPC Server).
* `/data_collector`: Servizio ingestione dati (Flask + Mongo + Circuit Breaker + Kafka Producer).
* `/alert_system`: Processore eventi (Kafka Consumer/Producer).
* `/notifier`: Servizio invio email (Kafka Consumer + SMTP + Deduplica).
* `/nginx`: Configurazione API Gateway.
* `/protos`: Definizioni gRPC (`user.proto`).
* `docker-compose.yml`: Orchestrazione dell'intero stack.

---

> **Nota per la correzione:** Il codice sorgente completo e la relazione tecnica dettagliata sono disponibili in questo repository.






