from kafka import KafkaProducer
import json, time, random
prod = KafkaProducer(bootstrap_servers=['localhost:9092'],
                     value_serializer=lambda v: json.dumps(v).encode())
i = 0
max_messages = 100
while i < max_messages:
    message = {'sensor': f's{random.randint(1, 5)}',
               'value': random.randint(15, 35)}
    print(f'Sending message {i} of {max_messages}: {message}')
    prod.send('temperatura', message)
    prod.flush()
    time.sleep(5)
    i += 1