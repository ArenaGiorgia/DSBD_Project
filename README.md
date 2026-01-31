
# Sistema di Monitoraggio dei Voli (HW3)

> 
> **Corso:** Distributed Systems and Big Data (2025-2026)
> **Studenti:** Arena Giorgia, Tornabene Alessio
> **Università:** Università degli Studi di Catania 
> 
> 

---

## Panoramica del Progetto

Questo progetto rappresenta l'evoluzione **Cloud-Native** del sistema di monitoraggio voli. L'architettura è stata migrata da un approccio imperativo (Docker Compose) a un cluster **Kubernetes** orchestrato tramite **Kind**.

Il sistema non si limita a gestire microservizi, ma implementa pattern avanzati di ingegneria distribuita:

 
**Orchestrazione Dichiarativa:** Separazione netta tra carichi *Stateless* (Deployments) e *Stateful* (StatefulSets).
**Osservabilità White-Box:** Monitoraggio attivo tramite **Prometheus** con strumentazione diretta del codice Python .
**Persistenza Resiliente:** Utilizzo di **PVC (Persistent Volume Claims)** per garantire *Zero Data Loss* anche in caso di crash dei Pod.



---

## Architettura e Key Features

### 1. Infrastruttura Iper-Convergente (Kind)

Il cluster è configurato in modalità **Single-Node Hybrid** (Control Plane + Worker) per simulare un ambiente Cloud completo su risorse locali, mappando le porte del container Docker direttamente sull'host per esporre Ingress e Prometheus .

### 2. Gestione Avanzata dello Stato


**StatefulSets (Kafka, Mongo, Postgres):** Garantiscono identità di rete stabile (es. `kafka-0`) e ordine di avvio sequenziale, fondamentali per il cluster di messaggistica e i database .
**Deployments (App Logic):** I microservizi (`user-manager`, `notifier`, ecc.) sono trattati come entità effimere ("Cattle"), scalabili orizzontalmente senza perdita di stato .


### 3. Ottimizzazione Storage Kafka

Per evitare la saturazione del disco in un ambiente vincolato, Kafka è configurato con una **Retention Policy basata sulla dimensione** (`100MB`). Questo trasforma il broker in un buffer circolare robusto che elimina automaticamente i vecchi log prima di riempire il PVC .

---

## Tech Stack

**Orchestrator:** Kubernetes (v1.27+) su Kind.
**Monitoring:** Prometheus (Pull-based Model, Service Discovery).
**Broker:** Apache Kafka (StatefulSet).
**Database:** PostgreSQL (Relazionale), MongoDB (NoSQL).
**Networking:** NGINX Ingress Controller.
**Linguaggio:** Python 3.9+ (Flask, gRPC, Prometheus Client).



---

## Prerequisiti

Assicurarsi di avere installato:

* **Docker Desktop** (Attivo).
* **Kind** (Kubernetes in Docker).
* **Kubectl** (CLI Kubernetes).

---

## Guida al Deployment (Step-by-Step)

Seguire rigorosamente l'ordine dei comandi per rispettare le dipendenze di avvio.

### 1. Avvio del Cluster e Ingress

Spostarsi nella directory `k8s` e creare il cluster con la configurazione custom (Port Mapping):

```bash
# Creazione Cluster
kind create cluster --config kind-config.yaml --name homework3

# Installazione NGINX Ingress Controller
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

```



### 2. Build & Load delle Immagini

Poiché il cluster è locale, le immagini devono essere costruite e caricate manualmente nei nodi Kind.
*Tornare alla root del progetto (`cd ..`) ed eseguire:*

```bash
# USER MANAGER
docker build -t user-manager:v1 ./user_manager
kind load docker-image user-manager:v1 --name homework3

# DATA COLLECTOR
docker build -t data-collector:v1 ./data_collector
kind load docker-image data-collector:v1 --name homework3

# ALERT SYSTEM
docker build -t alert-system:v1 ./alert_system
kind load docker-image alert-system:v1 --name homework3

# NOTIFIER
docker build -t notifier:v1 ./notifier
kind load docker-image notifier:v1 --name homework3

```



### 3. Applicazione dei Manifesti (Deployment)

Tornare nella cartella `k8s` e applicare i file in questo ordine preciso:

```bash
# 1. Configurazioni Base (Namespace e Credenziali)
kubectl apply -f namespace.yaml
kubectl apply -f secrets.yaml

# 2. Infrastruttura Dati (Attendere qualche secondo dopo questo step)
kubectl apply -f database.yaml
kubectl apply -f kafka.yaml

# 3. Microservizi Applicativi
kubectl apply -f deployments.yaml

# 4. Networking e Sicurezza
kubectl apply -f network-policy.yaml
kubectl apply -f ingress.yaml

# 5. Sistema di Monitoraggio
kubectl apply -f prometheus.yaml

```



---

##  Testing e Validazione

### Verifica dello Stato

Controllare che tutti i pod passino dallo stato `ContainerCreating` a `Running`:

```bash
kubectl get pods -n homework3 -w

```



### Accesso ai Log

Per debuggare i singoli microservizi in tempo reale:

```bash
kubectl logs -l app=data-collector -n homework3 -f
kubectl logs -l app=alert-system -n homework3 -f
kubectl logs -l app=notifier -n homework3 -f

```



---

##  Monitoraggio (Prometheus)

Accedere alla dashboard di monitoraggio su: **http://localhost:9090**.
Utilizzare le seguenti query **PromQL** per validare il sistema:

1. **Carico Database (Throughput):**
`rate(mongo_total_query_total[5m])`


*Mostra i "secondi di lavoro DB al secondo"*.


2. **Resilienza (Circuit Breaker):**
`rate(opensky_requests_total{status="error"}[5m])`


*Se > 0, indica che il sistema sta bloccando chiamate verso OpenSky fallite*.


3. **Latenza User Experience:**
`last_db_query_seconds`


*Analisi puntuale delle query SQL (Login/Registrazione)*.


4. **Totale Notifiche Inviate:**
`emails_total{status="success"}`


*Contatore cumulativo delle email spedite*.



---

## Pulizia e Reset (Clean Slate)

Per eliminare il cluster o resettare i dati persistenti, usare i seguenti comandi.

### Eliminazione Cluster Completo

```bash
kind delete cluster --name homework3

```



### Reset dei Dati Persistenti (Senza eliminare il cluster)

Se si vuole ripartire da zero (database vuoti) mantenendo i servizi attivi, è necessario cancellare i PVC e rimuovere i *finalizers* che potrebbero bloccare l'eliminazione .

```bash
# MONGO DB (Voli)
kubectl delete pvc mongo-storage-mongo-0 -n homework3
kubectl patch pvc mongo-storage-mongo-0 -n homework3 -p '{"metadata":{"finalizers":null}}'

# POSTGRES (Utenti)
kubectl delete pvc postgres-storage-postgres-0 -n homework3
kubectl patch pvc postgres-storage-postgres-0 -n homework3 -p '{"metadata":{"finalizers":null}}'

# KAFKA (Log Messaggi)
kubectl delete pvc kafka-data-kafka-0 -n homework3
kubectl patch pvc kafka-data-kafka-0 -n homework3 -p '{"metadata":{"finalizers":null}}'

# PROMETHEUS (Metriche)
kubectl delete pvc prometheus-storage-prometheus-server-0 -n homework3
kubectl patch pvc prometheus-storage-prometheus-server-0 -n homework3 -p '{"metadata":{"finalizers":null}}'

```
