import os
import json
import time
import hashlib
from pymongo import MongoClient
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

MONGO_URL = os.getenv("MONGO_URL", "mongodb://data-db:27017/")
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'

try:
    client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
    db = client["flight_db"]
    print("Connesso a MongoDB.")
except Exception as e:
    print(f"Errore connessione Mongo: {e}")


consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'alert_system_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}


producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',  # Aspetta che TUTTI i broker abbiano salvato il dato
    'retries': 5,  # Riprova molte volte in caso di errore di rete
    'linger.ms': 0  # Invia SUBITO, niente batching per ridurre latenza
}
producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)
TOPIC_1 = 'to-alert-system'
TOPIC_2 = 'to-notifier'
consumer.subscribe([TOPIC_1])


def delivery_report(err, msg):
    if err:
        print(f"Errore durante l'invio: {err}")

#dato che se il sistema va in crash rilegge i messaggi,generiamo un id univoco al notifier per evitare i duplicati
def generate_deduplication_id(email, airport, timestamp, value):
    """
    Usa il timestamp ORIGINALE del pacchetto dati.
    In caso di retry, il pacchetto è lo stesso -> timestamp uguale -> ID uguale.
    """
    id = f"{email}-{airport}-{timestamp}-{value}"
    return hashlib.md5(id.encode()).hexdigest()


def check_thresholds_and_alert(flight_data):
    airport = flight_data.get('airport')
    current_count = flight_data.get('flights_count', 0)

    #Usiamo il timestamp scritto dal Data Collector.
    # Se il Data Collector non lo manda, usiamo 0 (o gestiamo l'errore), ma NON time.time().
    source_timestamp = flight_data.get('timestamp')

    if source_timestamp is None:
        print("ERRORE: Timestamp mancante nei dati volo!")
        return  # O gestisci come preferisci, ma non generare ID a caso

    # Lettura da Mongo
    utenti_interessati = list(db.interests.find({"airport": airport}))

    alerts_generated = 0

    for utente in utenti_interessati:
        email = utente.get('user')
        high = utente.get('high_value')
        low = utente.get('low_value')

        condition = None
        triggered_threshold = None

        if high is not None and current_count > high:
            condition = "HIGH_VALUE_EXCEEDED"
            triggered_threshold = high
        elif low is not None and current_count < low:
            condition = "LOW_VALUE_EXCEEDED"
            triggered_threshold = low

        if condition:
            alert_id = generate_deduplication_id(email, airport, source_timestamp, current_count)

            alert_message = {
                "alert_id": alert_id,
                "email": email,
                "airport": airport,
                "condition": condition,
                "current_value": current_count,
                "threshold": triggered_threshold,
                "timestamp": int(time.time())
            }

            # Invio al Notifier
            producer.produce(
                TOPIC_2,
                json.dumps(alert_message).encode('utf-8'),
                callback=delivery_report
            )
            alerts_generated += 1

    # ROBUSTEZZA STEP 1:
    # Se abbiamo generato allarmi, forziamo l'invio fisico
    # Se il producer fallisce qui, il codice si ferma o lancia eccezione,
    # quindi NON committeremo l'offset di lettura. Il messaggio verrà riletto. Corretto.
    if alerts_generated > 0:
        producer.flush()


def main():

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    time.sleep(2)
                    continue
                elif msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            try:

                data = json.loads(msg.value().decode('utf-8'))
                check_thresholds_and_alert(data)

                # 2. COMMIT SINCRONO
                # Blocchiamo tutto finché Kafka non dice "Offset Salvato".
                # Se il container viene ucciso QUI, l'offset non è salvato.
                # Al riavvio rileggeremo il messaggio -> Genereremo lo stesso Alert ID -> Il Notifier lo scarterà.
                consumer.commit(message=msg, asynchronous=False)

            except Exception as e:
                print(f"Errore processamento: {e}")
                # In caso di errore logico grave, NON committiamo.
                # Il messaggio verrà ripresentato all'infinito finché non si risolve il bug (o manual fix).
                # È il comportamento corretto per "Non perdere dati".

    except KeyboardInterrupt:
        print("Stop manuale.")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    time.sleep(5)
    main()