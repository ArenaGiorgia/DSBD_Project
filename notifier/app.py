import os
import json
import time
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import deque
from confluent_kafka import Consumer, KafkaException, KafkaError

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_1 = 'to-notifier'

# configurazione SMTP
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD')

#memoria deduplicata,controlla l'ID. Se ha già inviato questo allarme specifico, lo ignora
#per evitare spam se il sistema si riavvia
PROCESSED_IDS = deque(maxlen=10000)


#aspetto che kafka sia pronto e dopo creo il sub
def wait_for_kafka():
    print("Notifier in attesa di Kafka..")

    #Configurazione temporanea solo per testare la connessione
    configurazione_temp = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'group.id': 'temp_check'}

    while True:
        try:
            #Proviamo a chiedere la lista dei topic per vedere se è vivo
            consumer_temp = Consumer(configurazione_temp)
            consumer_temp.list_topics(timeout=5.0)
            consumer_temp.close()
            print("Kafka è PRONTO! Connessione stabilita.")
            return
        except Exception as e:
            print(f"Kafka non ancora pronto ({e}). Riprovo tra 5 secondi..")
            time.sleep(5)


def send_email(alert_data):

    email = alert_data.get('email')
    airport = alert_data.get('airport')
    condition = alert_data.get('condition')
    val = alert_data.get('current_value')
    threshold = alert_data.get('threshold')

    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("Credenziali email mancanti.", file=sys.stderr)
        return False

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = email
    msg['Subject'] = f"Allerta Voli: {airport}"

    body = f"""Ciao {email},

Il sistema di monitoraggio ha rilevato una condizione critica per l'aeroporto: {airport}.

- Condizione: {condition}
- Valore Attuale Voli: {val}
- Soglia Impostata: {threshold}

Ti auguriamo buone vacanze e un grande in bocca al lupo per l'esame di Distributed Systems!
    """

    msg.attach(MIMEText(body, 'plain'))

    try:
        # Connessione sicura a Gmail
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)

        print(f"Invio con successo a {email}")
        return True

    except smtplib.SMTPAuthenticationError:
        print("Password o Email sbagliata", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Errore invio: {e}", file=sys.stderr)
        return False


def main():
    print(f"Notifier Avviato. Mittente: {SENDER_EMAIL}")

    #aspettiamo che sia pronto kafka.
    wait_for_kafka()

    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'notifier_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_1])
    print(">>> Consumer Notifier connesso e sottoscritto al topic.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    time.sleep(2)
                    continue
                else:
                    print(f"Kafka errore: {msg.error()}", file=sys.stderr)
                    continue

            try:
                data = json.loads(msg.value().decode('utf-8'))

                #controlliamo se abbiamo ricevuto lo stesso id di alert
                alert_id = data.get('alert_id')
                if alert_id in PROCESSED_IDS:
                    print(f"Email {alert_id} già inviata.")
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                if send_email(data):
                    # Se l'invio ha successo, memorizziamo l'id e committiamo
                    PROCESSED_IDS.append(alert_id)
                    consumer.commit(message=msg, asynchronous=False)
                else:

                    #non committiamo, facciamo retry .
                    print(f"Invio fallito, riproverò al prossimo ciclo.")

            except json.JSONDecodeError:
                consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print("Stop manuale.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()