# %%
import os
import json
import boto3
from confluent_kafka import Producer


# Changeable
KAFKA_BROKER = os.getenv('KAFKA_BROKER', '')
ICEBERG_TOPIC = 'iceberg_events'
ES_TOPIC = 'es_events'

# Fixed (AWS SASL)
session = boto3.session.Session()
client = session.client(service_name='secretsmanager', region_name='ap-northeast-2')

get_secret_value_response = client.get_secret_value(SecretId=os.getenv('SECRET_ID', ''))
secret = get_secret_value_response['SecretString']
credentials = json.loads(secret)

sasl_username = credentials['username']
sasl_password = credentials['password']

# MSK Configuration
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