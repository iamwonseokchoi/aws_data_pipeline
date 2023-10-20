FROM python:3.8.18-slim 

WORKDIR /app 

COPY data/producerRequirements.txt producerRequirements.txt
RUN pip install --no-cache-dir -r producerRequirements.txt

RUN mkdir -p logs

COPY data/logs/app.log logs/

COPY data/src/producer.py producer.py
COPY data/src/consts.py consts.py
COPY data/src/functions.py functions.py

# ENVs: MSK Broker URL, AWS Secret ID

CMD ["python", "producer.py"]


