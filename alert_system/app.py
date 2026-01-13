import os
import json
import time
import hashlib
from pymongo import MongoClient, errors
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# --- CONFIGURAZIONE ---
mongo_host = os.getenv("MONGODB_HOST", "mongodb")
MONGO_URL = os.getenv("MONGO_URL", f"mongodb://{mongo_host}:27017/")
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-service:9092')
TOPIC_1 = os.getenv('KAFKA_TOPIC', 'to-alert-system')
TOPIC_2 = 'to-notifier'

db = None
producer = None


def init_mongo_connection():
    global db
    print(f"Connessione a MongoDB su {MONGO_URL} in corso..")
    while True:
        try:
            client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db = client["flight_db"]
            print("MongoDB Connesso.")
            return
        except errors.ServerSelectionTimeoutError:
            print("MongoDB non pronto. Riprovo tra 5s..")
            time.sleep(5)


def wait_for_kafka():
    print(f"In attesa di Kafka su {KAFKA_BOOTSTRAP_SERVERS}...")
    configurazione_temp = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    while True:
        try:
            producer_temp = Producer(configurazione_temp)
            producer_temp.list_topics(timeout=5.0)
            print("Kafka è PRONTO! Connessione stabilita")
            return
        except Exception as e:
            print(f"Kafka non ancora pronto ({e}). Riprovo tra 5s..")
            time.sleep(5)


def delivery_report(err, msg):
    if err:
        print(f"Errore invio Kafka: {err}")


def generate_deduplication_id(email, airport, timestamp, value):
    id_string = f"{email}-{airport}-{timestamp}-{value}"
    return hashlib.md5(id_string.encode()).hexdigest()


def check_thresholds_and_alert(flight_data):
    if db is None or producer is None:
        print("ERRORE CRITICO: DB o Producer non pronti.")
        return

    airport = flight_data.get('airport')
    current_count = flight_data.get('flights_count', 0)
    source_timestamp = flight_data.get('timestamp', int(time.time()))

    # 1. CERCO NEL DATABASE
    try:
        # Recupero la lista grezza dal database
        interessi = list(db.interests.find({"airport": airport}))

        # --- STAMPA DI DEBUG FONDAMENTALE ---
        print(f"DEBUG DB: Per l'aeroporto '{airport}' ho trovato {len(interessi)} utenti interessati.")
        if len(interessi) > 0:
            print(f"DEBUG DATA: Primo record trovato: {interessi[0]}")
        # ------------------------------------

    except Exception as e:
        print(f"Errore lettura Mongo: {e}")
        return

    alerts_generated = 0

    for regola in interessi:
        # 2. ADATTO LA CHIAVE (Il Data Collector usa 'user', l'Alert System voleva 'email')
        # Li provo entrambi per sicurezza
        email = regola.get('user') or regola.get('email')

        if not email:
            print(f"DEBUG SKIP: Trovato record senza email/user valido: {regola}")
            continue

        high = regola.get('high_value')
        low = regola.get('low_value')

        condition = None
        triggered_threshold = None

        # 3. CONTROLLO MATEMATICO
        # Converto in int per sicurezza
        if high is not None and current_count > int(high):
            condition = "Valore ALTO superato"
            triggered_threshold = high
        elif low is not None and current_count < int(low):
            condition = "Valore BASSO superato"
            triggered_threshold = low

        if condition:
            alert_id = generate_deduplication_id(email, airport, source_timestamp, current_count)

            # Preparo il messaggio per il Notifier
            alert_message = {
                "alert_id": alert_id,
                "email": email,
                "subject": f"ALERT {airport}: {condition}",
                "body": f"Allarme per {airport}.\nVoli attuali: {current_count}\nSoglia: {triggered_threshold}",
                "airport": airport,
                "condition": condition,
                "current_value": current_count,
                "threshold": triggered_threshold,
                "timestamp": int(time.time())
            }

            print(f"!!! ALLARME SCATTATO per {email}: {condition} !!!")

            producer.produce(TOPIC_2, json.dumps(alert_message).encode('utf-8'), callback=delivery_report)
            alerts_generated += 1

    if alerts_generated > 0:
        producer.flush()
        print(f"-> Inviati {alerts_generated} messaggi al Notifier.")


def main():
    global producer
    init_mongo_connection()
    wait_for_kafka()

    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'acks': 'all', 'linger.ms': 0}
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'alert_system_group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False
    }

    producer = Producer(producer_conf)
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_1])

    print("Alert System OPERATIVO! In attesa di dati...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Kafka Error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                print(f"Ricevuto da Kafka: {data.get('airport')} - Voli: {data.get('flights_count')}")
                check_thresholds_and_alert(data)
                consumer.commit(message=msg, asynchronous=False)
            except Exception as e:
                print(f"Errore processing: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()