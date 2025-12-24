import os
import json
import time
import hashlib
from pymongo import MongoClient, errors
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

MONGO_URL = os.getenv("MONGO_URL", "mongodb://data-db:27017/")
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_1 = 'to-alert-system' #da consumare l'alert systerm prodotto dal datacollector
TOPIC_2 = 'to-notifier'     #da produrre l'alert system e consumarlo il notifier

#Variabili globali inizializzate a None per il main
db = None
producer = None


#Verifico la connessione con il database mongo e voglio che sia attivo
def init_mongo_connection():
    global db
    print("Connessione a MongoDB in corso..")
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
        except Exception as e:
            print(f"Errore Mongo: {e}. Riprovo tra 5s..")
            time.sleep(5)

#aspetto che kafka sia pronto e dopo creo il pub/sub
def wait_for_kafka():
    print("In attesa di Kafka...")

    #Configurazione temporanea solo per testare la connessione
    configurazione_temp = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    while True:
        try:
            #Proviamo a chiedere la lista dei topic per vedere se è vivo
            producer_temp = Producer(configurazione_temp)
            producer_temp.list_topics(timeout=5.0)
            print("Kafka è PRONTO! Connessione stabilita")
            return
        except Exception as e:
            print(f"Kafka non ancora pronto ({e}). Riprovo tra altri 5 secondi..")
            time.sleep(5)

def delivery_report(err, msg):
    if err:
        print(f"Errore durante l'invio: {err}")


#dato che se il sistema va in crash rilegge i messaggi,generiamo un id univoco al notifier per evitare i duplicati
def generate_deduplication_id(email, airport, timestamp, value):
    id = f"{email}-{airport}-{timestamp}-{value}"  #Usa il timestamp originale del pacchetto dati.
    return hashlib.md5(id.encode()).hexdigest()


def check_thresholds_and_alert(flight_data):
    if db is None or producer is None:
        print("errore: il database o il Producer non sono inizializzati. Riprova")
        return

    airport = flight_data.get('airport')
    current_count = flight_data.get('flights_count', 0)

    #Usiamo il timestamp scritto dal Data Collector.
    # Se il Data Collector non lo manda, usiamo 0 (o gestiamo l'errore), ma NON time.time().
    source_timestamp = flight_data.get('timestamp')
    data_source = flight_data.get('source', 'Sconosciuta')

    if source_timestamp is None:
        print("errore: Timestamp mancante nei dati di volo!")
        return

    # Lettura dal database di Mongo
    try:
        utenti_interessati = list(db.interests.find({"airport": airport}))
    except Exception as e:
        print(f"Errore lettura Mongo: {e}")
        return

    alerts_generated = 0

    for utente in utenti_interessati:
        email = utente.get('user')
        high = utente.get('high_value')
        low = utente.get('low_value')

        condition = None
        triggered_threshold = None

        if high is not None and current_count > high:
            condition = "Valore ALTO superato"
            triggered_threshold = high
        elif low is not None and current_count < low:
            condition = "Valore BASSO superato"
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
                "timestamp": int(time.time()),
                "source": data_source
            }

            print(f"Allarme rilevato per {email}: {condition}")

            # Invio al Notifier
            producer.produce(
                TOPIC_2,
                json.dumps(alert_message).encode('utf-8'),
                callback=delivery_report
            )
            alerts_generated += 1


    # Se abbiamo generato allarmi, forziamo l'invio fisico
    if alerts_generated > 0:
        producer.flush()

def main():
    global producer

    #Prima di fare qualsiasi cosa con Kafka o Mongo, aspettiamo che siano pronti.
    init_mongo_connection()
    wait_for_kafka()

    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'alert_system_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'acks': 'all',  #Aspetta che tutti i broker (eventualmente replicati) abbiano ottenuto il dato
        'retries': 5,  # Riprova 5 volte in caso di errore di rete
        'linger.ms': 0  #riduciamo la latenza e inviamo subito
    }

    producer = Producer(producer_conf)
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_1])

    print("Alert System OPERATIVO!")

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
                print(f"Ricevuto da Kafka: {data.get('airport')} - Voli: {data.get('flights_count')}")
                check_thresholds_and_alert(data)

                #commit sincrono, kafka deve salvare l offeset se no lo blocchiamo
                consumer.commit(message=msg, asynchronous=False)

            except Exception as e:
                print(f"Errore di processamento: {e}")

    except KeyboardInterrupt:
        print("Stop manuale.")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()