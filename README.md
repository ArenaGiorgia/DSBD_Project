#  Sistema Distribuito di Monitoraggio Voli (DSBD Project #1)

**Corso:** Sistemi Distribuiti e Big Data (2025/2026)

**Progetto:** Architettura a microservizi per la gestione utenti e il monitoraggio del traffico aereo in tempo reale.

![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![PostgreSQL](https://img.shields.io/badge/postgresql-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white)
![gRPC](https://img.shields.io/badge/gRPC-%23244c5a.svg?style=for-the-badge&logo=grpc&logoColor=white)

---

##  Descrizione del Progetto

Il sistema è un'applicazione distribuita che integra dati provenienti da **OpenSky Network** per tracciare voli aerei di interesse per gli utenti registrati. L'architettura è basata su **Microservizi** containerizzati, garantendo scalabilità, isolamento e una gestione efficiente dei dati eterogenei (Polyglot Persistence).

###  Funzionalità Chiave
* **Gestione Utenti:** Registrazione sicura con politica *"At-Most-Once"*.
* **Monitoraggio Voli:** Download ciclico dei dati di volo in background senza bloccare le API REST.
* **Analisi Statistica:** Calcolo in tempo reale di medie e ultimi avvistamenti su MongoDB.
* **Robustezza:** Sistema di *Fallback* automatico che genera dati simulati (Mock Data) in caso di indisponibilità delle API esterne.

---

## Architettura del Sistema

Il progetto è composto da 4 container orchestrati via Docker Compose:

| Servizio | Tecnologia | Porta Host | Descrizione |
| :--- | :--- | :--- | :--- |
| **User Manager** | Python (Flask + gRPC) | `5000` | Gestisce l'anagrafica utenti e valida le richieste interne. |
| **Data Collector** | Python (Flask + gRPC) | `5001` | Raccoglie dati da OpenSky, gestisce i job periodici e le statistiche. |
| **User DB** | PostgreSQL 15 | `5432` | Database relazionale per garantire l'integrità dei dati utente. |
| **Data DB** | MongoDB 6.0 | `27017` | Database NoSQL per l'archiviazione flessibile dei log di volo. |

### Flussi di Comunicazione
* **REST (HTTP):** Interfaccia pubblica verso i client (es. Postman).
* **gRPC:** Comunicazione interna ad alta efficienza tra microservizi (Verifica Utente su porta 50051 / Cancellazione Dati su porta 50052).
* **OAuth2:** Autenticazione sicura verso le API di OpenSky Network.

---

##  Guida all'Installazione e Deploy (Docker)

Questo è il metodo consigliato per avviare l'intero sistema in pochi secondi.

### Prerequisiti
* [Docker Desktop](https://www.docker.com/products/docker-desktop) installato e in esecuzione.

### 1. Clona la Repository
```bash
git clone https://github.com/ArenaGiorgia/DB_Project.git
cd DB_Project
````

### 2\. Configurazione Credenziali (Opzionale)

Per abilitare il download dei dati reali, modifica il file `docker-compose.yml` nella sezione `data-collector` inserendo le tue credenziali OpenSky.
*Nota: Se non configuri queste variabili, il sistema funzionerà comunque utilizzando dati simulati (Mock Data).*

```yaml
# In docker-compose.yml
environment:
  - OPENSKY_CLIENT_ID=tuo_username
  - OPENSKY_CLIENT_SECRET=tua_password
```

### 3\. Build & Avvio

Esegui il comando per costruire le immagini e avviare l'intero stack:

```bash
docker-compose up --build
```

### 4\. Arresto e Pulizia

Per fermare i container e rimuovere i volumi (reset completo dei database):

```bash
docker-compose down -v
```

-----

##  Sviluppo Locale (Senza Docker)

Se si desidera eseguire o testare i singoli script Python (es. test unitari o debugging) al di fuori dei container Docker, è necessario configurare un ambiente virtuale locale e installare le dipendenze dai file `requirements.txt`.

> **Nota:** La cartella `.venv` NON è inclusa nel repository (è nel `.gitignore`) e va creata localmente.

### 1\. Creazione Virtual Environment

Apri il terminale nella root del progetto:

```bash
# Windows
python -m venv .venv

# Mac/Linux
python3 -m venv .venv
```

### 2\. Attivazione

```bash
# Windows (PowerShell)
.\.venv\Scripts\activate

# Mac/Linux
source .venv/bin/activate
```


### 3\. Installazione Dipendenze (Requirements)

Installa le librerie necessarie per entrambi i microservizi utilizzando i file di specifica:

```bash
pip install -r user_manager/requirements.txt
pip install -r data_collector/requirements.txt
```

-----

##  API Reference

Il sistema espone le seguenti API su `localhost`.

### User Manager (Porta 5000)

| Metodo | Endpoint | Body (JSON) | Descrizione |
| :--- | :--- | :--- | :--- |
| `POST` | `/users` | `{"email": "...", "nome": "..."}` | Registra un nuovo utente. |
| `DELETE` | `/users/<email>` | - | Cancella un utente e i suoi dati a cascata. |

###  Data Collector (Porta 5001)

| Metodo | Endpoint | Body / Query | Descrizione |
| :--- | :--- | :--- | :--- |
| `POST` | `/interests` | `{"email": "...", "airport": "LIRF"}` | Aggiunge un aeroporto da monitorare (Verifica gRPC). |
| `GET` | `/flights/last` | `?airport=LIRF` | Restituisce l'ultimo volo registrato. |
| `GET` | `/flights/average`| `?airport=LIRF&days=7` | Calcola la media voli giornaliera. |
| `GET` | `/flights/my-interests`| `?email=...` | Restituisce tutti i voli per un utente (Join applicativa). |

-----

###  Autori

Arena Giorgia

Tornabene Alessio

