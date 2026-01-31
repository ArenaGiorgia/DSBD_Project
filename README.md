# Cloud-Native Flight Monitoring System (Kubernetes Edition)

> **Corso:** Distributed Systems and Big Data (2025-2026)
> 
> **Studenti:** Arena Giorgia, Tornabene Alessio
> 
> **Università degli Studi di Catania**

---

## ☁️ Panoramica del Progetto (HW3)

**Il progetto evolve l'architettura a microservizi** precedente trasformandola in un sistema **Cloud-Native** pienamente orchestrato su **Kubernetes**.
Il sistema monitora il traffico aereo in tempo reale (OpenSky API) e notifica gli utenti via email al superamento di soglie critiche.

A differenza della versione basata su Docker Compose, questa iterazione si focalizza su:
* **Orchestrazione Dichiarativa:** Gestione dello stato desiderato tramite manifest YAML.
* **Osservabilità White-Box:** Monitoraggio metrico profondo tramite **Prometheus**.
* **Resilienza & Self-Healing:** Recupero automatico dai guasti e persistenza dei dati garantita.
* **Ottimizzazione Risorse:** Gestione QoS (Quality of Service) in ambienti vincolati.

---

## 🏗️ Caratteristiche Architetturali (Key Features)

### 1. Architettura Kubernetes Avanzata
* **Stateful vs Stateless:**
    * **Deployments:** I microservizi applicativi (`User Manager`, `Data Collector`, `Notifier`) sono trattati come entità effimere ("Cattle"), scalabili orizzontalmente.
    * **StatefulSets:** I database (`Mongo`, `Postgres`) e il broker (`Kafka`) mantengono un'identità di rete stabile (es. `kafka-0`) e storage ordinato.
* **Zero Data Loss:** Utilizzo di **Persistent Volume Claims (PVC)** per disaccoppiare il ciclo di vita dei dati da quello dei Pod. Se un DB crasha, il nuovo Pod si riaggancia allo stesso disco.
* **Single-Node Cluster Topology:** Configurazione custom di **Kind** che unifica Control Plane e Worker node, mappando le porte host per simulare un ambiente di produzione locale.

### 2. Osservabilità (White-Box Monitoring)
Il sistema non è una "scatola nera". Grazie all'integrazione nativa con **Prometheus** e alla strumentazione del codice Python (`prometheus_client`), esponiamo metriche di business in tempo reale:
* **Throughput:** Analisi del carico di lavoro (Email/sec, Query/sec).
* **Latenza:** Misurazione puntuale dei tempi di risposta DB e API Esterne.
* **Resilienza:** Monitoraggio dello stato del **Circuit Breaker** (Open/Closed).

### 3. Efficienza e Scalabilità
* **Resource Quotas:** Limiti di CPU e RAM (Requests/Limits) per prevenire il "Noisy Neighbor problem" e gestire memory leaks.
* **Smart Kafka Retention:** Policy calcolata a **100MB** per trasformare il broker in un buffer circolare e prevenire la saturazione del disco.

---

##
