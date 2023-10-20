# %%
import random
import logging
from faker import Faker
from datetime import datetime
from collections import Counter
from consts import *


# Initialize Faker
fake = Faker()
# User storage
global_users = []

# Logging Configuration
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def init_msk_producer():
    """
    Initiates AWS MSK Producer.
    :return: MSK Producer
    """
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'socket.timeout.ms': 100,
        'api.version.request': 'true',
        'broker.version.fallback': '0.9.0',
        'message.max.bytes': 1_000_000_000,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-512',
        'sasl.username': sasl_username,
        'sasl.password': sasl_password,
    })
    return producer


def get_next_event():
    """
    Gets the next event for mock customers.
    :return: Log of customer driven event
    """
    total_events = sum(Counter().values())
    if total_events == 0:
        return random.choice(["login", "logout", "search", "view_product", "add_to_cart", "checkout", "purchase"])
    frequencies = {
        "login": 0.05,
        "logout": 0.05,
        "search": 0.1,
        "view_product": 0.7,
        "add_to_cart": 0.1,
        "checkout": 0.05,
        "purchase": 0.05
    }
    for event, target_frequency in frequencies.items():
        actual_frequency = Counter()[event] / total_events
        if actual_frequency < target_frequency:
            return event
    return random.choice(["login", "logout", "search", "view_product", "add_to_cart", "checkout", "purchase"])


def generate_mock_data():
    """
    Generates mock data based on conditionals mocking real life.
    :return: Iceberg(Batch) and Elasticsearch(Speed) data
    """
    if random.random() < 0.01:  # Add new user at 1% rate
        user_data = {
            "table": "user",
            "data": {
                "user_id": str(fake.uuid4()),
                "user_name": fake.name(),
                "email": fake.email(),
                "age": random.randint(18, 80),
                "gender": random.choice(['Male', 'Female', 'Non-Binary']),
                "registered_on": datetime.utcnow().isoformat()
            }
        }
        global_users.append(user_data['data']['user_id'])
        send_msg(ICEBERG_TOPIC, user_data)

    # Randomly pick a user for the sale event
    user_id = random.choice(global_users) if global_users else str(fake.uuid4())
    
    # Iceberg Tables
    iceberg_data = []

    product = random.choice([
        "Lipstick",
        "Foundation",
        "Mascara",
        "Eyeliner",
        "Nail Polish",
        "BB Cream"
    ])

    # Price ranges based on product type
    price_ranges = {
        "Lipstick": (50, 50),
        "Foundation": (35, 35),
        "Mascara": (45, 45),
        "Eyeliner": (50, 50),
        "Nail Polish": (30, 30),
        "BB Cream": (15, 15)
    }

    if random.random() < 0.7:  # Generate a sale at 70% rate
        min_price, max_price = price_ranges.get(product, (10, 100))
        sale_data = {
            "table": "sales",
            "data": {
                "sale_id": str(fake.uuid4()),
                "product": product,
                "user_id": user_id,
                "price": round(random.uniform(min_price, max_price), 2),
                "quantity": random.randint(1, 3),
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        iceberg_data.append(sale_data)

    if random.random() < 0.1:  # Generate an inventory update at 10% rate
        inventory_data = {
            "table": "inventory",
            "data": {
                "inventory_id": str(fake.uuid4()),
                "product": product,
                "stock_count": random.randint(-5, 5),
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        iceberg_data.append(inventory_data)

    # Elasticsearch Data
    es_data = {}
    if random.random() < 0.25:  # Generate a logs at 25% rate
        event = get_next_event()  # Dynamic event generation based on frequency
        Counter()[event] += 1  # Increment event count

        # Conditional logic based on event types
        if event == "search":
            search_keywords = random.choice(["foundation", "eyeliner", "lipstick", "bb", "nail polish", "mascara"])
            es_data = {
                "search_keywords": search_keywords,
                "session_duration": 0,
                "ip_address": fake.ipv4(),
                "event": event,
                "user_agent": fake.user_agent(),
                "location": fake.location_on_land(),
                "session_actions": 1,
                "page_views": [fake.uri_path()],
                "timestamp": str(fake.date_time_this_year()),
                "user_id": user_id
            }

        elif event == "login":
            es_data = {
                "search_keywords": "",
                "session_duration": 0,
                "ip_address": fake.ipv4(),
                "event": event,
                "user_agent": fake.user_agent(),
                "location": fake.location_on_land(),
                "session_actions": 0,
                "page_views": [fake.uri_path()],
                "timestamp": str(fake.date_time_this_year()),
                "user_id": user_id
            }

        elif event == "logout":
            es_data = {
                "search_keywords": "",
                "session_duration": 0,
                "ip_address": fake.ipv4(),
                "event": event,
                "user_agent": fake.user_agent(),
                "location": fake.location_on_land(),
                "session_actions": 0,
                "page_views": [fake.uri_path()],
                "timestamp": str(fake.date_time_this_year()),
                "user_id": user_id
            }

        elif event == "view_product":
            es_data = {
                "search_keywords": "",
                "session_duration": random.randint(0, 360),
                "ip_address": fake.ipv4(),
                "event": event,
                "user_agent": fake.user_agent(),
                "location": fake.location_on_land(),
                "session_actions": random.randint(1, 50),
                "page_views": [fake.uri_path() for _ in range(random.randint(1, 10))],
                "timestamp": str(fake.date_time_this_year()),
                "user_id": user_id
            }

        elif event == "add_to_cart":
            es_data = {
                "search_keywords": "",
                "session_duration": random.randint(0, 360),
                "ip_address": fake.ipv4(),
                "event": event,
                "user_agent": fake.user_agent(),
                "location": fake.location_on_land(),
                "session_actions": random.randint(1, 50),
                "page_views": [fake.uri_path() for _ in range(random.randint(1, 10))],
                "timestamp": str(fake.date_time_this_year()),
                "user_id": user_id
            }

        elif event == "purchase":
            es_data = {
                "search_keywords": "",
                "session_duration": random.randint(0, 360),
                "ip_address": fake.ipv4(),
                "event": event,
                "user_agent": fake.user_agent(),
                "location": fake.location_on_land(),
                "session_actions": random.randint(1, 50),
                "page_views": [fake.uri_path() for _ in range(random.randint(1, 10))],
                "timestamp": str(fake.date_time_this_year()),
                "user_id": user_id
            }

        elif event == "checkout":
            es_data = {
                "search_keywords": "",
                "session_duration": random.randint(0, 360),
                "ip_address": fake.ipv4(),
                "event": event,
                "user_agent": fake.user_agent(),
                "location": fake.location_on_land(),
                "session_actions": random.randint(1, 50),
                "page_views": [fake.uri_path() for _ in range(random.randint(1, 10))],
                "timestamp": str(fake.date_time_this_year()),
                "user_id": user_id
            }
    
    return iceberg_data, es_data


def send_msg(topic, msg):
    """
    Produces to MSK topic.
    :param topic: Kafka topic
    :param msg: Message to send
    :return: None
    """
    try:
        msg_json_str = json.dumps(msg)
        producer.produce(
            topic,
            msg_json_str,
        )
        producer.flush()
    except Exception as ex:
        logging.error(f"Error : {ex}")


def handle_data(i):
    """
    Executes the data generation and sending process to MSK.
    :param i: Iteration number
    :return: None
    """
    try:
        iceberg_data, es_data = generate_mock_data()
        for data in iceberg_data:
            if data:  
                send_msg(ICEBERG_TOPIC, data)
        if es_data: 
            send_msg(ES_TOPIC, es_data)
    except Exception as e:
        logger.error(f"Exception occurred: {e}")