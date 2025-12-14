import time
import random
import uuid
import json
from confluent_kafka import Producer

# --------------------------
# Functie pentru incarcarea configuratiei Kafka
# --------------------------
def load_kafka_config(config_file="kafka.config"):
    """Incarca configuratia Kafka din fisierul specificat."""
    conf = {}
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    conf[key.strip()] = value.strip()
        conf['client.id'] = 'python-producer'
        return conf
    except FileNotFoundError:
        print(f"Eroare: Fisierul de configurare '{config_file}' nu a fost gasit.")
        print("Asigurati-va ca aveti un 'kafka.config' valid (pe care NU il urcati pe GitHub) si 'kafka.config.example' (pe care il urcati).")
        exit(1)

# --------------------------
# Functii utilitare
# --------------------------

def delivery_report(err, msg):
    """Callback apelat la livrarea mesajului."""
    if err is not None:
        print(f"Eroare la livrarea mesajului: {err}")
    # else:
    #     print(f"Mesaj livrat la topicul '{msg.topic()}'")

# Importam/Simulam generatorul de date
try:
    from data_generator import generate_support_request
except ImportError:
    # Fallback: Daca nu exista data_generator.py, folosim date simple de test
    print("ATENTIE: Modulul 'data_generator.py' nu a fost gasit. Se folosesc date de test simple.")
    def generate_support_request():
        return {
            "request_id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "customer_id": random.randint(1000, 9999),
            "text": random.choice([
                "Nu pot sa ma loghez, primesc eroarea 'Invalid Credentials'. Am incercat sa resetez parola dar nu merge.",
                "Am o intrebare despre factura mea din luna martie. Suma mi se pare prea mare. Puteti verifica?",
                "Contul meu pare sa fi fost accesat de pe o locatie necunoscuta. Va rog sa-l blocati imediat. Sunt ingrijorat de securitate.",
                "Aplicatia se blocheaza de fiecare data cand incerc sa incarc o poza. Folosesc Android 13.",
                "Vreau sa anulez abonamentul Premium incepand cu luna viitoare. Cum pot face asta in aplicatie?"
            ])
        }

# --------------------------
# Logica principala Producer
# --------------------------

if __name__ == "__main__":
    
    # 1. Incarcare configurare
    conf = load_kafka_config()
    topic = "date_hackathon" # Numele Topic-ului Kafka
    p = None

    try:
        p = Producer(conf)
        print(f"Producer Kafka conectat. Tinta: {topic}")
        print("Producer pornit. Trimite mesaje (Ctrl+C pentru a opri)...")

        while True:
            # 2. Generare date
            data = generate_support_request()
            json_data = json.dumps(data)
            
            # 3. Trimite mesajul catre Kafka
            p.poll(0) # Apel obligatoriu pentru a rula callback-urile
            
            try:
                p.produce(
                    topic, 
                    key=data['request_id'].encode('utf-8'), 
                    value=json_data.encode('utf-8'), 
                    callback=delivery_report
                )
                print(f"-> Trimis cerere: {data['request_id']} | Text: {data['text'][:50]}...")
            
            except BufferError:
                print(f"Buffer plin! Asteptare de 1 secunda...")
                time.sleep(1)

            # 4. Pauza
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\nOprire Producer...")
        
    except Exception as e:
        print(f"\nEroare neasteptata: {e}")

    finally:
        if p:
            # Așteaptă finalizarea tuturor livrărilor
            print("Asteptam livrarea mesajelor ramase...")
            p.flush()
            print("Producer oprit.")
