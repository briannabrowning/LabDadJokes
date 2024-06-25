# Subscire to the jokes topic & consume the topic's message
from confluent_kafka import Consumer, KafkaError, KafkaException
from json.decoder import JSONDecodeError
import json
import sys

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':61941'

def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'joke_logger'}

    # Subscirbe consumer to conf
    consumer = Consumer(conf)
    
    try:
      consumer.subscribe([KAFKA_TOPIC])

      while True:
        msg = consumer.poll(timeout = 1.0)
        if msg is None: 
          continue
                
        if msg.error():
          if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
          elif msg.error():
            raise KafkaException(msg.error())
        else: 
          print(msg.value().decode())

        # Parse the received data and extracts the "joke" property as a string. 
        else:
          try:
            message_value = msg.value().decode('utf-8')    # string
            joke_data = json.loads(message_value)
            joke = joke_data['joke']
            print(f"Successfully received joke: {joke}")

          except json.JSONDecodeError as e:
            print(f"Invalid JSON format: {e}")
          except KeyError as e:
            print(f"Missing key in JSON: {e}")
s
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == '__main__':
    main()

