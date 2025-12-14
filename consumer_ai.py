import os
import json
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING
from google import genai
from google.genai.errors import APIError

# --------------------------
# Functii Utilitare
# --------------------------

def load_kafka_config(config_file="kafka.config"):
    """Incarca configuratia Kafka din fisierul specificat."""
    # Seteaza Consumer Group ID si offset-ul de pornire
    conf = {
        'group.id': 'gemini-ai-group',
        'auto.offset.reset': 'earliest'
    }
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    conf[key.strip()] = value.strip()
        return conf
    except FileNotFoundError:
        print(f"Eroare: Fisierul de configurare '{config_file}' nu a fost gasit.")
        print("Asigurati-va ca aveti un 'kafka.config' cu setarile reale in directorul local.")
        exit(1)

# --------------------------
# Interactiunea cu Gemini AI
# --------------------------

def process_with_gemini(support_text):
    """
    Trimite textul cererii de suport catre Gemini 2.5 Flash pentru clasificare si sumarizare.
    """
    # Verificare cruciala pentru cheia API
    if 'GEMINI_API_KEY' not in os.environ:
        print("\n!! EROARE: Variabila de mediu GEMINI_API_KEY nu este setata.")
        return None, "API Key lipsa"

    try:
        # Initializare client Gemini
        client = genai.Client()
        
        # Instructiuni pentru model pentru a asigura output-ul dorit
        system_instruction = (
            "You are an expert real-time support ticket analyst. "
            "Your task is to classify a support request into one category (e.g., Billing, Technical, Security, Account) and provide a concise, one-sentence summary of the issue. "
            "The response MUST be in JSON format."
        )

        # Prompt-ul care contine datele primite din Kafka
        prompt = (
            f"Analizeaza si proceseaza urmatoarea cerere de suport: '{support_text}'. "
            "Returneaza raspunsul in format JSON cu doua chei: 'category' si 'summary'."
        )

        # Configurarea cererii catre model (fortare JSON)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={
                "system_instruction": system_instruction,
                "response_mime_type": "application/json",
            }
        )
        
        # Parsarea raspunsului JSON
        try:
            gemini_output = json.loads(response.text)
            return gemini_output.get('category', 'N/A'), gemini_output.get('summary', 'N/A')
        except json.JSONDecodeError:
            print(f"Eroare la parsarea raspunsului JSON de la Gemini: {response.text}")
            return "JSON_ERROR", response.text

    except APIError as e:
        print(f"Eroare la apelul Gemini API: {e}")
        return "API_ERROR", str(e)
    except Exception as e:
        print(f"Eroare neasteptata in functia Gemini: {e}")
        return "UNKNOWN_ERROR", str(e)


# --------------------------
# Logica principala Consumer
# --------------------------

if __name__ == "__main__":
    
    # 1. Incarcare configurare
    conf = load_kafka_config()
    topic = "date_hackathon"
    
    c = None
    try:
        c = Consumer(conf)
        c.subscribe([topic])
        print(f"Consumer Kafka conectat. Asteapta mesaje pe topicul '{topic}'...")

        while True:
            msg = c.poll(1.0) # Polling cu timeout de 1 secunda

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(f"Eroare Consumer: {msg.error()}")
                    continue

            # 2. Procesare mesaj valid
            try:
                # Decodare mesaj din Kafka
                raw_data = json.loads(msg.value().decode('utf-8'))
                support_text = raw_data.get('text', 'Text lipsa')
                request_id = raw_data.get('request_id', 'Necunoscut')

                # 3. Apelare Gemini
                print(f"\n[ID: {request_id}] Primita cerere: {support_text[:70]}...")

                category, summary = process_with_gemini(support_text)
                
                # 4. Afisare Rezultate
                print("--------------------------------------------------")
                print(f"| 🏷️ CLASIFICARE (Gemini): {category}")
                print(f"| 📝 SUMAR EXECUTIV: {summary}")
                print("--------------------------------------------------")

            except json.JSONDecodeError:
                print("Eroare la decodarea JSON din Kafka.")
            except Exception as e:
                print(f"Eroare la procesarea mesajului: {e}")

    except KeyboardInterrupt:
        print("\nOprire Consumer...")
    
    except Exception as e:
        print(f"\nEroare neasteptata la initializare: {e}")

    finally:
        if c:
            c.close()
            print("Consumer oprit.")
