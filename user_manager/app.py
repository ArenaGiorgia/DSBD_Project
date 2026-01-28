import threading
import grpc
from concurrent import futures
from flask import Flask, request, jsonify, abort
import hashlib
import user_pb2
import user_pb2_grpc
from database_postgres import database_postgres
from cache import Cache
import json
import time
import os
from prometheus_client import start_http_server, Counter, Gauge

global_cache = Cache(ttl_seconds=300)
app = Flask(__name__)

# --- CONFIGURAZIONE METRICHE HOMEWORK (AGGIUNTE) ---
NODE_NAME = os.getenv("MY_NODE_NAME", "hmw3")
SERVICE_NAME = "user-manager"

# 1. Metrica COUNTER (Numero totale di richieste)
REQUEST_COUNT = Counter(
    'user_manager_requests_total',
    'Totale richieste ricevute dal servizio User Manager',
    ['service', 'node', 'method', 'endpoint', 'http_status']
)

# 2. Metrica GAUGE (Tempo di risposta dell'ultima richiesta)
LAST_REQUEST_LATENCY = Gauge(
    'user_manager_latency',
    'Tempo di esecuzione dell\'ultima richiesta in secondi',
    ['service', 'node', 'method', 'endpoint']
)


DATA_COLLECTOR_GRPC = "data-collector:50052"


# Definisco il Server gRPC (Risponde al Data Collector)
class UserManagerGRPC(user_pb2_grpc.UserManagerServicer):

    def CheckUser(self, request, context):

        client_id = request.client_id  # riferito al microservizio utilizzato (data-collector)
        message_id = request.message_id
        email = request.email

        print(f"Richiesta da parte di {client_id} con message_id: {message_id} e Email: {email}")

        # controllo la cache con la Politica At-Most-Once
        cache_response = global_cache.get_response(client_id, message_id)
        if cache_response:
            print(f"Mi hai mandato gia la stessa request, ti prendo il dato conservato nella mia cache.")

            return cache_response

        exists = False
        connection = None
        try:

            connection = database_postgres.get_connection()
            cursore = connection.cursor()

            try:
                cursore.execute("SELECT * FROM users WHERE email = %s", (email,))

                if cursore.fetchone():
                    exists = True
            finally:
                cursore.close()

        except Exception as e:
            print(f"Errore database: {e}")

        finally:
            # chiudiamo alla fine sempre la connessione
            if connection:
                connection.close()

        response = user_pb2.CheckUserResponse(exists=exists)

        # salvo il nuovo messaggio dentro la cache
        global_cache.save_response(client_id, message_id, response)

        return response


def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserManagerServicer_to_server(UserManagerGRPC(), server)
    port = 50051
    server.add_insecure_port(f"[::]:{port}")  # canale insicuro che prende qualsiasi host
    server.start()
    server.wait_for_termination()


@app.route('/users', methods=['POST'])
def register():
    # --- PROMETHEUS START TIMER ---
    start_time = time.time()

    # Recupero l'ID univoco dalla richiesta(Header)
    request_id = request.headers.get('Request-ID')

    if not request_id:
        # Metrica Errore 400
        REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'POST', '/users', 400).inc()
        return jsonify({"errore": "Manca l'header Request-ID per l' At-Most-Once"}), 400

    # utilizzo il DATA_COLLECTOR come client per identificare quel microservizio
    cache_resp = global_cache.get_response("DATA_COLLECTOR", request_id)
    if cache_resp:
        print(f"Mi hai mandato gia la stessa request, ti prendo il dato conservato nella mia cache.")

        # Metrica Cache Hit (Status originale)
        REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'POST', '/users', cache_resp['status']).inc()
        return jsonify(cache_resp['body']), cache_resp['status']

    data = request.json
    email = data.get('email')
    nome = data.get('nome')
    password = data.get('password')
    cognome = data.get('cognome')

    if not email or not password:
        # Metrica Errore 400
        REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'POST', '/users', 400).inc()
        return jsonify({"errore": "Email obbligatoria / password obbligatoria"}), 400

    connection = None
    try:
        connection = database_postgres.get_connection()
        cursor = connection.cursor()

        try:
            cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
            if cursor.fetchone():
                print(f"Utente con {email} e con ID {request_id} gia registrato / Vai in un eventuale login")

                # --- PROMETHEUS SUCCESS (User Exists) ---
                REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'POST', '/users', 200).inc()
                LAST_REQUEST_LATENCY.labels(SERVICE_NAME, NODE_NAME, 'POST', '/users').set(time.time() - start_time)

                return jsonify({
                    "messaggio": "Utente già registrato / Vai in un eventuale login",
                    "email": email,
                    "request_id": request_id
                }), 200

            pw_hash = hashlib.sha256(password.encode()).hexdigest()

            # Inserisco l'utente
            cursor.execute(
                "INSERT INTO users (email, password, nome, cognome) VALUES (%s, %s, %s, %s)",
                (email, pw_hash, nome, cognome)
            )

            connection.commit()
            print(f"Registrazione completata per l'utente con request_id: {request_id} ed email: {email}")

            response_body = {
                "messaggio": "Registrazione completata!",
                "email": email,
                "request_id": request_id
            }
            status_code = 201

            # Salvo in cache per futuri retry con lo stesso ID
            global_cache.save_response("DATA_COLLECTOR", request_id, {'body': response_body, 'status': status_code})

            # --- PROMETHEUS SUCCESS (Created) ---
            REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'POST', '/users', 201).inc()
            LAST_REQUEST_LATENCY.labels(SERVICE_NAME, NODE_NAME, 'POST', '/users').set(time.time() - start_time)

            return jsonify(response_body), status_code

        finally:

            cursor.close()

    except Exception as e:
        # Metrica Errore 500
        REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'POST', '/users', 500).inc()
        return jsonify({"errore": f"Errore server: {str(e)}"}), 500

    finally:
        if connection:
            connection.close()


@app.route('/users', methods=['DELETE'])
def delete_user():
    # --- PROMETHEUS START TIMER ---
    start_time = time.time()

    # Recupero l'ID per gestire la pulizia della cache
    request_id = request.headers.get('Request-ID')
    data = request.get_json() or {}
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        # Metrica Errore 400
        REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'DELETE', '/users', 400).inc()
        abort(400, description="Email e Password obbligatori per cancellare")

    pw_hash = hashlib.sha256(password.encode()).hexdigest()

    conn = None
    try:
        conn = database_postgres.get_connection()
        cur = conn.cursor()
        try:
            # cancello da POSTGRES
            cur.execute("DELETE FROM users WHERE email = %s AND password = %s", (email, pw_hash))
            conn.commit()

            if cur.rowcount > 0:

                if request_id:

                    esito = global_cache.remove_response("DATA_COLLECTOR", request_id)

                    if esito:
                        print(f"Cache pulita correttamente per l'ID {request_id}")
                    else:
                        print(f"Nessuna cache trovata per l'ID {request_id}")
                try:

                    # connettiamo alla porta 50052 configurata nel Data Collector
                    target_grpc = 'data-collector-service:50052'

                    # Configuro il canale per riprovare se il server è giù
                    service_config = {
                        "methodConfig": [
                            {
                                "name": [{"service": "DataCollector"}],
                                "timeout": "5s",  # Timeout totale
                                "retryPolicy": {
                                    "maxAttempts": 5,  # Riprovare massimo 5 volte
                                    "initialBackoff": "0.5s",  # Aspetta 0.5s al primo errore
                                    "maxBackoff": "2s",  # massima attesa
                                    "backoffMultiplier": 2,  # Raddoppia l'attesa ogni volta
                                    "retryableStatusCodes": ["UNAVAILABLE"]  # Riprova solo se il server va giù
                                }
                            }
                        ]
                    }

                    options = [
                        ('grpc.service_config', json.dumps(service_config))]  # da errore se non lo passo con json.dumps

                    with grpc.insecure_channel(target_grpc, options=options) as channel:

                        stub = user_pb2_grpc.DataCollectorStub(channel)
                        request_grpc = user_pb2.DeleteDataRequest(email=email)
                        response = stub.DeleteData(request_grpc)

                        print(f"Richiesta inviata per {email}. Successo: {response.success}")

                except grpc.RpcError as e:

                    print(f"Errore gRPC verso il Data Collector: {e}")

                except Exception as e:
                    print(f"Errore generico pulizia dei dati: {e}")

                # --- PROMETHEUS SUCCESS (Deleted) ---
                REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'DELETE', '/users', 200).inc()
                LAST_REQUEST_LATENCY.labels(SERVICE_NAME, NODE_NAME, 'DELETE', '/users').set(time.time() - start_time)

                return jsonify({"message": "Utente eliminato e dati puliti", "email": email}), 200
            else:

                print(f"Utente {email} non trovato nel DB o password errata.")

                # Metrica Errore 401
                REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'DELETE', '/users', 401).inc()
                return jsonify({"errore": "Utente non trovato o credenziali errate"}), 401

        finally:
            cur.close()

    except Exception as e:
        # Gestione errori generici del server
        print(f"[SERVER ERROR] {str(e)}")
        # Metrica Errore 500
        REQUEST_COUNT.labels(SERVICE_NAME, NODE_NAME, 'DELETE', '/users', 500).inc()
        return jsonify({"errore": f"Errore interno del server: {str(e)}"}), 500
    finally:
        if conn: conn.close()


if __name__ == '__main__':
    # --- AVVIO SERVER METRICHE PROMETHEUS (AGGIUNTO) ---
    # Fondamentale: apre la porta 8000 per rispondere a Prometheus
    start_http_server(8000)
    print("Server metriche Prometheus avviato su porta 8000")
    threading_grpc = threading.Thread(target=start_grpc_server, daemon=True)
    threading_grpc.start()
    app.run(host='0.0.0.0', port=5000, debug=False)