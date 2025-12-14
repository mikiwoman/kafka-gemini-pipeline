# ⚡ Real-Time Customer Support Pipeline with Kafka and Gemini AI

## Project Overview

We built an event-driven data pipeline to solve slow customer support response times. By leveraging **Apache Kafka** for scalable, real-time message transport and **Google Gemini 2.5 Flash** for instant intelligence, the system automatically processes and analyzes incoming support tickets.

The pipeline instantly **classifies** requests (e.g., Billing, Security, Technical) and provides a **concise, executive summary**. This automation reduces agent response time from minutes to mere seconds, drastically improving operational efficiency and accuracy.

## 🚀 Architecture and Components

The pipeline consists of three main components:

1.  **Kafka Producer (`producer.py`):** Simulates incoming support requests and publishes them as JSON messages to a Kafka topic.
2.  **Apache Kafka (Confluent Cloud):** Serves as the scalable message bus, ensuring durable and reliable transport of data events.
3.  **Gemini AI Consumer (`consumer_ai.py`):** Subscribes to the Kafka topic, extracts the text, calls the Gemini API for analysis, and prints the real-time classification and summary.

## 🛠️ Setup and Installation

### Prerequisites

You need the following accounts/tools:

1.  **Python 3.8+**
2.  **A Google AI Studio/Gemini API Key:** (Set as an environment variable).
3.  **A Confluent Cloud Account:** (To get Kafka bootstrap servers and API keys).

### Local Setup

1.  **Clone the Repository:**
    ```bash
    git clone [YOUR_REPO_LINK]
    cd [YOUR_REPO_NAME]
    ```

2.  **Install Dependencies:**
    ```bash
    pip install confluent-kafka google-genai
    ```

3.  **Configure Credentials:**
    * Rename the template: `cp kafka.config.example kafka.config`
    * **Edit `kafka.config`** and replace the placeholder values with your actual Confluent Cloud credentials. **(DO NOT commit this file!)**

4.  **Set Gemini API Key:**
    The consumer relies on an environment variable for security:
    ```bash
    export GEMINI_API_KEY="YOUR_ACTUAL_GEMINI_API_KEY"
    ```

## ▶️ How to Run the Project

The Producer and Consumer must be run simultaneously in two separate terminals.

### 1. Start the Consumer (Terminal 1)

This process starts listening for messages and initializes the Gemini connection.

```bash
python consumer_ai.py
2. Start the Producer (Terminal 2)
This process simulates a stream of support tickets being sent to Kafka.

Bash

python producer.py
You will immediately see the consumer_ai.py terminal pick up the messages, call the Gemini API, and display the instant classification and summary, demonstrating the real-time capability.
