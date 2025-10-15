import time
import threading
import json
from datetime import datetime
from sseclient import SSEClient
from producer import send_data_Tokafka
from consumer import consum

stop_event = threading.Event()


def producer_thread(stop_event):
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    headers = {"User-Agent": "MyWikiKafkaBot/1.0 (ouissal@example.com)"}

    while not stop_event.is_set():
        try:
            client = SSEClient(url, headers=headers)
            print("Connexion au flux Wikipedia - data_pipeline.py")

            for event in client:
                if stop_event.is_set():
                    break
                if event.event == "message":
                    try:
                        data = json.loads(event.data)
                        ts = datetime.now()
                        wiki = data.get("wiki", "")
                        username = data.get("user", "")
                        title = data.get("title", "")
                        type_change = data.get("type", "")
                        namespace_id = data.get("namespace", "")
                        bot = data.get("bot", False)

                        message = f"{ts},{wiki},{username},{title},{type_change},{namespace_id},{bot}"
                        send_data_Tokafka(message)
                        time.sleep(0.5)

                    except Exception as e:
                        print(f"Erreur lors de l'envoi au producteur: {e}")

        except Exception as e:
            print(f"Error in producer thread: {e}")
            time.sleep(5)  # Reconnexion après 5 secondes si problème


def consumer_thread(stop_event):
    while not stop_event.is_set():
        try:
            consum()
        except Exception as e:
            print(f"Error in consumer thread: {e}")
        time.sleep(0.1)


# Création et démarrage des threads
producer = threading.Thread(target=producer_thread, args=(stop_event,))
consumer = threading.Thread(target=consumer_thread, args=(stop_event,))

producer.start()
consumer.start()

try:
    while True:
        time.sleep(0.1)
except KeyboardInterrupt:
    print("\nArrêt demandé par l'utilisateur. Arrêt des threads...")
    stop_event.set()

producer.join()
consumer.join()
print("Tous les threads sont arrêtés.")
